"""
Backfill historique : récupère les matchs déjà TERMINÉS sur une plage de
dates passées, pour construire un jeu d'entraînement immédiat au lieu
d'attendre que l'avenir se produise (voir échange du 2026-09-21 — le choix
initial de ne collecter que les matchs à venir était une erreur de
conception, cette phase aurait dû commencer par ça).

Coût : ~2 requêtes par match (stats + odds), UNE SEULE FOIS par match — pas
le pattern de polling répété du live, donc pas besoin du feed bulk ici. Le
detail complet (y compris head_to_head) est déjà dans la réponse de
/events/ elle-même (vérifié sur payload réel) : aucun appel /events/{id}/
séparé n'est nécessaire.

Fait vérifié le 2026-09-21 : les odds pré-match restent disponibles au moins
plusieurs semaines après le coup d'envoi (testé sur un match du 30/08/2026,
~3 semaines avant ce test) ; sur un match de 2023 en revanche elles sont
totalement nullifiées ("no further updates scheduled", valeurs null). La
limite exacte entre les deux n'est PAS connue — ne backfill que des périodes
récentes (quelques mois maximum) et vérifie toi-même sur un échantillon
avant de lancer une plage plus large.

Usage:
    python -m bsd_ai.backfill --date-from 2026-07-01 --date-to 2026-09-20
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from sqlalchemy.orm import Session

from .bsd_client import BSDClient, BSDRateLimitError
from .config import settings
from .db.models import Match, MatchStatus, SnapshotKind
from .db.session import get_session, init_db
from .ingestion.discovery import upsert_match_from_event
from .ingestion.snapshot import _store_raw  # réutilise le même stockage brut immuable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("bsd_ai.backfill")


def backfill_range(session: Session, client: BSDClient, date_from: str, date_to: str) -> dict[str, int]:
    if not settings.league_ids:
        raise RuntimeError("BSD_LEAGUE_IDS vide — voir .env.example, le scope est obligatoire.")

    stats = {"matches": 0, "stats_ok": 0, "odds_ok": 0, "errors": 0}

    for league_id in settings.league_ids:
        logger.info("backfill ligue %s : %s -> %s", league_id, date_from, date_to)
        try:
            events = list(client.list_events(
                league_id=league_id, date_from=date_from, date_to=date_to, status="finished",
            ))
        except BSDRateLimitError as exc:
            logger.warning("rate limit (%s) sur la liste ligue %s, arrêt du backfill", exc.code, league_id)
            break

        for event in events:
            event_id = event["id"]
            match = upsert_match_from_event(session, event)
            stats["matches"] += 1

            # Le payload de /events/ contient DÉJÀ le detail complet
            # (head_to_head, weather, highlights...) — pas d'appel séparé.
            _store_raw(session, event_id, SnapshotKind.detail, event, minute=event.get("current_minute"))
            if event.get("head_to_head"):
                _store_raw(session, event_id, SnapshotKind.h2h, event["head_to_head"], minute=None)

            try:
                stats_payload = client.event_stats(event_id)
                _store_raw(session, event_id, SnapshotKind.stats, stats_payload, minute=None)
                stats["stats_ok"] += 1
            except KeyError:
                pass  # pas de stats dispo pour ce match, on continue
            except BSDRateLimitError as exc:
                logger.warning("rate limit (%s) sur stats event=%s, arrêt du backfill", exc.code, event_id)
                session.commit()
                return stats

            try:
                odds_payload = client.event_odds(event_id)
                _store_raw(session, event_id, SnapshotKind.odds, odds_payload, minute=None)
                if any(v is not None for v in odds_payload.get("odds", {}).values()):
                    stats["odds_ok"] += 1
            except KeyError:
                pass
            except BSDRateLimitError as exc:
                logger.warning("rate limit (%s) sur odds event=%s, arrêt du backfill", exc.code, event_id)
                session.commit()
                return stats

            session.commit()

        logger.info("ligue %s terminée — cumul: %s", league_id, stats)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historique BSD AI")
    parser.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    init_db()
    session = get_session()
    with BSDClient() as client:
        result = backfill_range(session, client, args.date_from, args.date_to)
    logger.info("Backfill terminé: %s", result)


if __name__ == "__main__":
    main()
