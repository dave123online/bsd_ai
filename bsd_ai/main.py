"""Point d'entrée Phase 1 : boucle d'ingestion continue.

Usage:
    python -m bsd_ai.main
"""
from __future__ import annotations

import logging
import time

from .bsd_client import BSDClient
from .db.session import init_db, get_session
from .ingestion import scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("bsd_ai.main")

# (fonction, intervalle_secondes)
JOBS = [
    (scheduler.discover_upcoming, 30 * 60),
    (scheduler.poll_live, 20),
    (scheduler.snapshot_live_checkpoints, 20),
    (scheduler.poll_odds_bulk, 3 * 60),
    (scheduler.settle_finished, 5 * 60),
    (scheduler.snapshot_prematch_due, 5 * 60),
]


def run_forever() -> None:
    init_db()
    client = BSDClient()
    last_run = {job.__name__: 0.0 for job, _ in JOBS}

    logger.info("BSD AI — Phase 1 ingestion démarrée")
    try:
        while True:
            now = time.monotonic()
            for job, interval in JOBS:
                if now - last_run[job.__name__] >= interval:
                    session = get_session()
                    try:
                        job(session, client)
                    except Exception:
                        logger.exception("job %s a échoué", job.__name__)
                        session.rollback()
                    finally:
                        session.close()
                    last_run[job.__name__] = now
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("arrêt demandé")
    finally:
        client.close()


if __name__ == "__main__":
    run_forever()
