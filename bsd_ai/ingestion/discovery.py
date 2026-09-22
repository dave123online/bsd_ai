"""Découverte des matchs à suivre : upsert dans `matches` depuis /events/.

Ne stocke aucun historique ici — c'est le rôle de raw_snapshots (snapshot.py).
Ce module maintient juste la liste "quels matchs existent et où en sont-ils"
pour que le scheduler sache quoi poller.
"""
from __future__ import annotations

import datetime as dt
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..bsd_client import BSDClient
from ..config import settings
from ..db.models import Match, MatchStatus

logger = logging.getLogger("bsd_ai.discovery")

# Faits vérifiés sur de vrais payloads (2026-09-20), PAS les valeurs
# documentées pour le paramètre de filtre `status` de /events/.
# Le champ `status` renvoyé dans les objets event n'utilise pas le même
# vocabulaire que le filtre de requête :
#   - liste /events/       -> "notstarted", "inprogress", "finished", ...
#   - /events/live/        -> "inprogress", "finished", ...
# alors que le paramètre de requête ?status= documenté attend
# "upcoming"/"live"/"finished"/... Les deux vocabulaires coexistent, on ne
# les mélange pas : on NORMALISE le champ de la réponse ici, et on continue
# à passer "upcoming"/"live"/etc. quand on construit une requête (bsd_client
# suit la doc du paramètre, inchangé).
RAW_STATUS_MAP: dict[str, MatchStatus] = {
    "notstarted": MatchStatus.upcoming,
    "not_started": MatchStatus.upcoming,
    "upcoming": MatchStatus.upcoming,
    "inprogress": MatchStatus.live,
    "in_progress": MatchStatus.live,
    "live": MatchStatus.live,
    "finished": MatchStatus.finished,
    "cancelled": MatchStatus.cancelled,
    "canceled": MatchStatus.cancelled,
    "postponed": MatchStatus.postponed,
    "unresolved": MatchStatus.unresolved,
}


def _normalize_status(raw: str | None) -> MatchStatus:
    if raw is None:
        return MatchStatus.unresolved
    mapped = RAW_STATUS_MAP.get(raw.lower())
    if mapped is None:
        logger.warning("statut BSD inconnu '%s', mappé sur unresolved — à investiguer", raw)
        return MatchStatus.unresolved
    return mapped


def _parse_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def upsert_match_from_event(session: Session, event: dict) -> Match:
    event_id = event["id"] if "id" in event else event["event_id"]
    match = session.get(Match, event_id)
    if match is None:
        match = Match(event_id=event_id)
        session.add(match)

    # home_team / away_team sont des strings (nom d'équipe), pas des objets
    # {id, name} — vérifié sur payload réel /events/ et /events/live/.
    match.league_id = event.get("league_id")
    match.season_id = event.get("season_id")
    match.home_team_id = event.get("home_team_id")
    match.away_team_id = event.get("away_team_id")
    match.home_team_name = event.get("home_team")
    match.away_team_name = event.get("away_team")
    match.status = _normalize_status(event.get("status"))
    # /events/ utilise "event_date", pas "kickoff_at" — vérifié sur payload réel.
    match.kickoff_at = _parse_dt(event.get("event_date"))
    match.has_xg = event.get("has_xg")

    # Ces champs sont DÉJÀ dans le payload compact de /events/ et /events/live/
    # (vérifié sur payloads réels) — les stocker ne coûte aucun appel
    # supplémentaire et pilote les checkpoints live (voir scheduler.py).
    match.current_minute = event.get("current_minute")
    match.period = event.get("period")
    match.home_score = event.get("home_score")
    match.away_score = event.get("away_score")

    match.last_seen_at = dt.datetime.now(dt.timezone.utc)
    return match


def discover_window(
    session: Session,
    client: BSDClient,
    date_from: str,
    date_to: str,
    league_ids: list[int] | None = None,
) -> int:
    """Récupère tous les matchs (tous statuts) sur une fenêtre de dates et les
    upsert. Retourne le nombre de matchs traités."""
    league_ids = league_ids if league_ids is not None else (settings.league_ids or [None])

    count = 0
    for league_id in league_ids:
        for event in client.list_events(league_id=league_id, date_from=date_from, date_to=date_to):
            upsert_match_from_event(session, event)
            count += 1
        session.commit()
    return count


def discover_live(session: Session, client: BSDClient) -> int:
    """Rafraîchit l'état des matchs actuellement live (statuts + scores).

    Important : /events/live/ liste aussi des matchs qui viennent de finir
    (status="finished" dans ce payload, vu sur données réelles). On NE force
    plus le statut à "live" ici — on garde le statut réel renvoyé, sinon un
    match fraîchement terminé resterait marqué "live" en base indéfiniment
    et ne serait jamais pris en charge par settle_finished."""
    count = 0
    for event in client.live_events():
        event_id = event.get("event_id") or event.get("id")
        if event_id is None:
            logger.warning("live event sans id, ignoré: %s", event)
            continue
        upsert_match_from_event(session, {**event, "id": event_id})
        count += 1
    session.commit()
    return count
