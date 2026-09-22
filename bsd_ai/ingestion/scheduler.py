"""
Orchestration Phase 1. Volontairement un simple polling loop (pas de queue
distribuée) : à ce stade le but est une boucle correcte et observable, pas une
architecture distribuée prématurée (cf. brief section 25 : "aucun modèle
complexe à ce stade" — le même principe de simplicité s'applique à l'infra).

RÉVISION 2026-09-20 — la v1 de ce fichier faisait du live continu (snapshot
detail+stats+incidents toutes les 30s pour chaque match live) sans tenir
compte du quota réel. Chiffres mesurés en usage réel :
  - le compte gratuit est plafonné à 7 500 requêtes/JOUR (doc conventions/
    #rate-limits), reset à minuit UTC ;
  - avec ne serait-ce que 15-20 matchs live simultanés (plausible sur 8-10
    grandes ligues un soir de championnat), du continu à 30s coûte
    plusieurs dizaines de MILLIERS de requêtes/jour rien que pour ce job.
Ce n'est pas un réglage fin à faire, c'est incompatible par construction.
Le brief lui-même (section 5) ne demandait QUE des points de contrôle
(T-24h, T-1h, kickoff, 15', 30', HT, 60', 75', FT) — pas du continu. Cette
révision revient à cet esprit :
  - poll_live reste fréquent (cheap, 1 seul call, donne minute/score/period
    pour TOUS les matchs live d'un coup) ;
  - snapshot_live_checkpoints ne fait un vrai appel détaillé (detail+stats+
    incidents, 2-3 requêtes) QUE quand la minute d'un match franchit un des
    paliers de LIVE_CHECKPOINT_MINUTES, et une seule fois par palier.
  - les odds passent par le feed BULK /api/v2/odds/ (1 appel couvre tous les
    matchs suivis) au lieu d'un appel par match.

Scope obligatoire : tous les jobs qui touchent des matchs individuels
(snapshot_live_checkpoints, settle_finished, snapshot_prematch_due) sont
filtrés sur settings.league_ids. poll_live reste un seul appel non filtré
(c'est gratuit de tout récupérer), mais seuls les matchs des ligues suivies
reçoivent ensuite des appels détaillés.

Cadence de chaque job :
- discover_upcoming            : toutes les 30 min, un call par ligue suivie.
- poll_live                    : toutes les 20s, 1 seul call (toutes ligues).
- snapshot_live_checkpoints    : toutes les 20s (même cadence que poll_live,
  quasi gratuit : ne déclenche un vrai call que sur franchissement de palier).
- poll_odds_bulk               : toutes les 3 min, 1 seul call (feed complet,
  filtré aux event_id suivis après réception).
- settle_finished              : toutes les 5 min, scope ligues suivies.
- snapshot_prematch_due        : toutes les 5 min, scope ligues suivies.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..bsd_client import BSDClient, BSDRateLimitError
from ..config import settings
from ..db.models import IngestionCursor, LIVE_CHECKPOINT_MINUTES, Match, MatchStatus
from . import discovery, snapshot

logger = logging.getLogger("bsd_ai.scheduler")


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _tracked_league_filter():
    """Condition SQLAlchemy réutilisable : restreint aux ligues suivies.
    Lève explicitement si BSD_LEAGUE_IDS est vide — on ne veut PAS d'un
    fallback silencieux vers "toutes les ligues", qui a été la cause du
    dépassement de quota (voir historique de ce fichier)."""
    if not settings.league_ids:
        raise RuntimeError(
            "BSD_LEAGUE_IDS est vide. Avec le quota gratuit (7 500 req/jour), "
            "suivre toutes les ligues n'est pas viable (voir docstring module). "
            "Renseigne BSD_LEAGUE_IDS dans .env avec 5-10 ligues avant de relancer."
        )
    return Match.league_id.in_(settings.league_ids)


def _run_batch(session: Session, matches: list[Match], fn: Callable[[Session, Match], object], label: str) -> int:
    """Applique `fn` à chaque match, avec un vrai circuit breaker sur le rate
    limit : dès qu'un appel BSD renvoie 429, on ARRÊTE la boucle immédiatement
    (les matchs restants seront de toute façon rejetés tant que
    client.blocked_until n'est pas passé — inutile de les taper un par un et
    de spammer les logs). Les autres erreurs sont loguées et n'interrompent
    que ce match."""
    processed = 0
    for match in matches:
        try:
            fn(session, match)
            processed += 1
        except BSDRateLimitError as exc:
            logger.warning(
                "%s: rate limit (%s) atteint après %d/%d matchs — arrêt du batch, "
                "reprise au plus tôt dans %.0fs",
                label, exc.code, processed, len(matches), exc.retry_after_seconds or 0,
            )
            session.rollback()
            break
        except Exception:
            logger.exception("%s: échec sur event=%s", label, match.event_id)
            session.rollback()
            continue
    session.commit()
    if processed or matches:
        logger.info("%s: %d/%d matchs traités", label, processed, len(matches))
    return processed


def discover_upcoming(session: Session, client: BSDClient) -> None:
    today = _now().date()
    date_from = today.isoformat()
    date_to = (today + dt.timedelta(days=7)).isoformat()
    try:
        n = discovery.discover_window(session, client, date_from, date_to)
        logger.info("discover_upcoming: %d matchs vus", n)
    except BSDRateLimitError as exc:
        logger.warning("discover_upcoming: rate limit (%s), reprise dans %.0fs", exc.code, exc.retry_after_seconds or 0)
        session.rollback()


def poll_live(session: Session, client: BSDClient) -> None:
    """1 seul appel, non filtré (gratuit de tout récupérer). Alimente
    current_minute/period/score sur TOUS les matchs vus — c'est ce qui permet
    à snapshot_live_checkpoints de détecter les franchissements de palier
    sans appel supplémentaire."""
    try:
        n = discovery.discover_live(session, client)
        logger.info("poll_live: %d matchs live", n)
    except BSDRateLimitError as exc:
        logger.warning("poll_live: rate limit (%s), reprise dans %.0fs", exc.code, exc.retry_after_seconds or 0)
        session.rollback()


def _next_checkpoint(current_minute: int, last_snapshot_minute: int | None) -> int | None:
    """Le plus grand palier <= current_minute pas encore capturé, ou None."""
    floor = last_snapshot_minute or 0
    reached = [c for c in LIVE_CHECKPOINT_MINUTES if floor < c <= current_minute]
    return max(reached) if reached else None


def snapshot_live_checkpoints(session: Session, client: BSDClient) -> None:
    """Ne fait un appel detail+stats+incidents que pour les matchs live (ligues
    suivies) dont la minute vient de franchir un palier (LIVE_CHECKPOINT_MINUTES)
    pas encore capturé. Coût par match sur un match complet : ~6 franchissements
    max, pas un par cycle de poll."""
    live_matches = session.scalars(
        select(Match).where(Match.status == MatchStatus.live, _tracked_league_filter())
    ).all()

    due = [
        m for m in live_matches
        if m.current_minute is not None and _next_checkpoint(m.current_minute, m.last_snapshot_minute) is not None
    ]

    def _snapshot_and_mark(s: Session, m: Match) -> None:
        checkpoint = _next_checkpoint(m.current_minute, m.last_snapshot_minute)
        snapshot.snapshot_live_state(s, client, m)
        m.last_snapshot_minute = checkpoint

    _run_batch(session, due, _snapshot_and_mark, "snapshot_live_checkpoints")


def _get_cursor(session: Session, key: str) -> str | None:
    row = session.get(IngestionCursor, key)
    return row.value if row else None


def _set_cursor(session: Session, key: str, value: str) -> None:
    row = session.get(IngestionCursor, key)
    if row is None:
        session.add(IngestionCursor(key=key, value=value, updated_at=_now()))
    else:
        row.value = value
        row.updated_at = _now()


ODDS_CURSOR_KEY = "odds_feed_updated_after"


def poll_odds_bulk(session: Session, client: BSDClient) -> None:
    """1 seul appel au feed bulk /api/v2/odds/, filtré côté client aux
    event_id qu'on suit réellement. Remplace l'ancien poll_odds_due qui
    faisait 1 requête PAR MATCH (invivable au-delà de quelques dizaines de
    matchs suivis — voir docstring module)."""
    if not settings.league_ids:
        logger.warning("poll_odds_bulk: BSD_LEAGUE_IDS vide, job ignoré")
        return

    tracked_event_ids = set(
        session.scalars(select(Match.event_id).where(_tracked_league_filter())).all()
    )
    if not tracked_event_ids:
        logger.info("poll_odds_bulk: aucun match suivi pour l'instant")
        return

    cursor = _get_cursor(session, ODDS_CURSOR_KEY)
    try:
        rows = list(client.odds_feed(updated_after=cursor))
    except BSDRateLimitError as exc:
        logger.warning("poll_odds_bulk: rate limit (%s), reprise dans %.0fs", exc.code, exc.retry_after_seconds or 0)
        return

    inserted, max_updated_at = snapshot.ingest_odds_feed_rows(session, rows, tracked_event_ids)
    if max_updated_at:
        _set_cursor(session, ODDS_CURSOR_KEY, max_updated_at.isoformat())
    session.commit()
    logger.info(
        "poll_odds_bulk: %d lignes vues, %d gardées (matchs suivis), curseur -> %s",
        len(rows), inserted, max_updated_at,
    )


def settle_finished(session: Session, client: BSDClient) -> None:
    """Prend un snapshot final pour les matchs qui viennent de passer
    `finished` et n'ont pas encore de RawSnapshot de type finished-equivalent
    récent. Détection simple : status finished + last_seen_at récent (mis à
    jour par discover_*), pour éviter de re-snapshoter indéfiniment un vieux
    match déjà traité — voir TODO Phase 1.1 pour un flag explicite settled_at."""
    recently_finished = session.scalars(
        select(Match).where(
            Match.status == MatchStatus.finished,
            Match.last_seen_at >= _now() - dt.timedelta(minutes=10),
            _tracked_league_filter(),
        )
    ).all()
    _run_batch(
        session, recently_finished,
        lambda s, m: snapshot.snapshot_finished(s, client, m),
        "settle_finished",
    )


def snapshot_prematch_due(session: Session, client: BSDClient) -> None:
    """Snapshot complet (lineups/h2h/prediction) pour les matchs dont le
    coup d'envoi est dans moins de 24h — UNE SEULE FOIS par match (voir
    Match.prematch_snapshotted_at : bug corrigé le 2026-09-20, ce job
    re-snapshotait tout indéfiniment tant qu'un match restait dans la
    fenêtre 24h). Approximation simple de T-24h/T-1h (brief section 5) —
    un scheduling plus fin (deux passages distincts) reste un TODO Phase 1.1."""
    now = _now()
    window_end = now + dt.timedelta(hours=24)
    candidates = session.scalars(
        select(Match).where(
            Match.status == MatchStatus.upcoming,
            Match.kickoff_at.is_not(None),
            Match.kickoff_at <= window_end,
            Match.prematch_snapshotted_at.is_(None),
            _tracked_league_filter(),
        )
    ).all()

    def _snapshot_and_mark(s: Session, m: Match) -> None:
        snapshot.snapshot_prematch_full(s, client, m)
        m.prematch_snapshotted_at = _now()

    _run_batch(session, candidates, _snapshot_and_mark, "snapshot_prematch_due")
