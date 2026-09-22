"""
Prend un "cliché" d'un match à l'instant présent : appelle les sous-ressources
BSD pertinentes et écrit une ligne neuve dans raw_snapshots pour chacune
(jamais d'update). Les odds sont en plus normalisées dans odds_snapshots.

Aucune fonction ici ne doit jamais lire une donnée "future" : chaque appel
n'utilise que ce que l'API renvoie à l'instant de l'appel, ce qui est
exactement la garantie anti-data-leakage demandée dans le brief (section 6).
"""
from __future__ import annotations

import datetime as dt
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..bsd_client import BSDClient, BSDPaidFeatureError
from ..db.models import Match, MatchStatus, OddsFeedRow, RawSnapshot, SnapshotKind

logger = logging.getLogger("bsd_ai.snapshot")


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _store_raw(session: Session, event_id: int, kind: SnapshotKind, payload: dict, minute: int | None) -> None:
    session.add(
        RawSnapshot(
            event_id=event_id,
            kind=kind,
            fetched_at=_now(),
            match_minute=minute,
            payload=payload,
        )
    )


def ingest_odds_feed_rows(session: Session, rows: list[dict], tracked_event_ids: set[int]) -> tuple[int, dt.datetime | None]:
    """
    Upsert des lignes du feed bulk /api/v2/odds/ (voir bsd_client.odds_feed).
    Ne garde que les event_id qu'on suit réellement (tracked_event_ids) : le
    feed sans filtre renvoie TOUTES les ligues, on ne veut pas polluer la
    base avec des matchs hors scope juste parce qu'un seul appel les a
    ramenés gratuitement.

    Retourne (nb de lignes insérées, updated_at max vu dans le batch complet
    AVANT filtrage — le curseur doit avancer sur tout ce qu'on a VU, pas
    seulement ce qu'on a gardé, sinon on rappellerait sans fin les lignes
    hors scope à chaque cycle)."""
    inserted = 0
    max_updated_at: dt.datetime | None = None
    fetched_at = _now()

    for row in rows:
        row_updated_at = _parse_dt(row.get("updated_at"))
        if row_updated_at and (max_updated_at is None or row_updated_at > max_updated_at):
            max_updated_at = row_updated_at

        event_id = row.get("event_id")
        if event_id not in tracked_event_ids:
            continue

        exists = session.execute(
            select(OddsFeedRow.id).where(
                OddsFeedRow.event_id == event_id,
                OddsFeedRow.market == row.get("market"),
                OddsFeedRow.outcome == row.get("outcome"),
                OddsFeedRow.bookmaker_slug == row.get("bookmaker_slug"),
                OddsFeedRow.updated_at == row_updated_at,
            )
        ).first()
        if exists:
            continue  # déjà vu cette observation exacte (peut arriver si le curseur chevauche)

        session.add(
            OddsFeedRow(
                event_id=event_id,
                market=row.get("market"),
                outcome=row.get("outcome"),
                bookmaker_slug=row.get("bookmaker_slug"),
                decimal_odds=row.get("decimal_odds"),
                previous_decimal_odds=row.get("previous_decimal_odds"),
                opening_decimal_odds=row.get("opening_decimal_odds"),
                movement=row.get("movement"),
                bookmaker_count=row.get("bookmaker_count"),
                opening_at=_parse_dt(row.get("opening_at")),
                updated_at=row_updated_at,
                fetched_at=fetched_at,
            )
        )
        inserted += 1

    return inserted, max_updated_at


def snapshot_odds(session: Session, client: BSDClient, match: Match) -> None:
    """LEGACY / ad-hoc uniquement : 1 requête par match. Ne plus utiliser dans
    le scheduler (voir scheduler.poll_odds_bulk) — gardé pour du debug manuel
    ou un backfill ponctuel sur un seul match."""
    try:
        data = client.event_odds(match.event_id)
    except KeyError:
        logger.info("pas d'odds pour event %s", match.event_id)
        return
    _store_raw(session, match.event_id, SnapshotKind.odds, data, minute=None)


def snapshot_live_state(session: Session, client: BSDClient, match: Match) -> int | None:
    """Snapshot detail + stats + incidents pendant un match live.
    Retourne la minute de match si trouvée, pour horodater les autres snapshots."""
    detail = client.event_detail(match.event_id)
    minute = detail.get("current_minute") or detail.get("minute")
    _store_raw(session, match.event_id, SnapshotKind.detail, detail, minute)

    try:
        stats = client.event_stats(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.stats, stats, minute)
    except KeyError:
        pass

    try:
        incidents = client.event_incidents(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.incidents, incidents, minute)
    except KeyError:
        pass

    return minute


def snapshot_prematch_full(session: Session, client: BSDClient, match: Match) -> None:
    """Snapshot complet pré-match : detail, lineups, h2h, prediction, odds.
    À appeler à T-24h / T-1h selon le brief (section 5)."""
    detail = client.event_detail(match.event_id)
    _store_raw(session, match.event_id, SnapshotKind.detail, detail, minute=None)

    try:
        lineups = client.event_lineups(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.lineups, lineups, minute=None)
    except KeyError:
        pass

    try:
        h2h = client.event_h2h(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.h2h, h2h, minute=None)
    except KeyError:
        pass

    try:
        prediction = client.event_prediction(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.prediction, prediction, minute=None)
    except (KeyError, BSDPaidFeatureError):
        pass
    # Odds : plus d'appel par-match ici. Le feed bulk (scheduler.poll_odds_bulk)
    # couvre ce match comme tous les autres suivis, en un seul appel partagé.


def snapshot_finished(session: Session, client: BSDClient, match: Match) -> None:
    """Snapshot final : stats + incidents + player-stats une fois le match
    terminé. Voir doc: le bag de stats peut encore bouger quelques heures
    après le coup de sifflet final le temps que la source amont se stabilise —
    prévoir un second passage différé (non fait ici, cf README Phase 1.1)."""
    detail = client.event_detail(match.event_id)
    _store_raw(session, match.event_id, SnapshotKind.detail, detail, minute=None)

    try:
        stats = client.event_stats(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.stats, stats, minute=None)
    except KeyError:
        pass

    try:
        incidents = client.event_incidents(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.incidents, incidents, minute=None)
    except KeyError:
        pass

    try:
        player_stats = client.event_player_stats(match.event_id)
        _store_raw(session, match.event_id, SnapshotKind.player_stats, player_stats, minute=None)
    except KeyError:
        pass
