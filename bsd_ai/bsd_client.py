"""
Client HTTP pour l'API BSD (Bzzoiro Sports Data) v2.

Faits vérifiés dans la doc officielle au 2026-09-20 (docs/football/,
docs/conventions/, docs/football/odds-predictions/), à ne pas re-deviner :
- Base URL football : https://sports.bzzoiro.com/api/v2/
- Auth : header "Authorization: Token <key>" sur tous les endpoints
- Pagination : limit/offset, défaut 50, max 200, réponse {count,next,previous,results}
  SAUF /events/live/ qui renvoie {"count","events":[...]}
- Codes d'erreur : 401 token invalide, 402 add-on payant requis, 404
- 429 : DEUX limites indépendantes, distinguées par le champ `code` du corps
  JSON (voir docs/conventions/#rate-limits) :
    - "taster_exhausted" : quota compte de 7 500 req/jour épuisé (reset
      minuit UTC). Retry-After = secondes jusqu'à minuit UTC. Inutile de
      retenter avant.
    - "rate_limited" : burst 25 req/s (110 en rafale) par adresse IP sur les
      endpoints cache (/events/, /events/live/, /leagues/, etc.). Retry-After
      = 1s. Rien à voir avec le quota journalier.
- Odds : endpoint bulk /api/v2/odds/?updated_after=...&league_id=... couvre
  TOUS les matchs en un seul appel (voir odds_feed ci-dessous) — à préférer
  systématiquement à /events/{id}/odds/ (1 call par match) dès qu'on suit
  plus de quelques matchs, sous peine d'exploser le quota.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Iterator

import httpx

from .config import settings

logger = logging.getLogger("bsd_ai.client")


class BSDRateLimitError(Exception):
    """
    Levée sur 429, ou immédiatement si on sait déjà être bloqué (voir
    blocked_until). NE fait plus dormir le thread appelant : le Retry-After
    observé en réel peut être de plusieurs HEURES (jusqu'à minuit UTC pour
    taster_exhausted), donc un sleep() ici gèlerait tout le scheduler. C'est à
    l'appelant (scheduler.py) de décider quoi faire d'un job qui échoue
    (typiquement : arrêter la boucle en cours, pas continuer à taper l'API).
    """

    def __init__(self, path: str, retry_after_seconds: float | None = None, code: str | None = None) -> None:
        self.path = path
        self.retry_after_seconds = retry_after_seconds
        self.code = code  # "taster_exhausted" (quota jour) ou "rate_limited" (burst IP)
        super().__init__(f"rate limit ({code}) sur {path} (retry_after={retry_after_seconds}s)")


class BSDPaidFeatureError(Exception):
    """Levée sur 402 : endpoint nécessitant un add-on payant (ex: odds/comparison/)."""


class BSDClient:
    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        self._client = httpx.Client(
            base_url=base_url or settings.bsd_base_url,
            headers={"Authorization": f"Token {api_key or settings.bsd_api_key}"},
            timeout=15.0,
        )
        # Circuit breaker : dès qu'on connaît une fenêtre de blocage (via un
        # Retry-After reçu), on refuse tout nouvel appel réseau jusqu'à son
        # expiration, au lieu de retaper l'API (et de gaspiller le quota
        # restant, ou de se reprendre un 429 immédiatement).
        self._blocked_until: dt.datetime | None = None
        self._blocked_code: str | None = None

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "BSDClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def blocked_until(self) -> dt.datetime | None:
        return self._blocked_until

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        now = dt.datetime.now(dt.timezone.utc)
        if self._blocked_until is not None and now < self._blocked_until:
            raise BSDRateLimitError(path, (self._blocked_until - now).total_seconds(), self._blocked_code)

        resp = self._client.get(path, params=params)

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 5))
            body: dict[str, Any] = {}
            try:
                body = resp.json()
            except ValueError:
                pass
            code = body.get("code")  # "taster_exhausted" ou "rate_limited"
            self._blocked_until = now + dt.timedelta(seconds=retry_after)
            self._blocked_code = code
            logger.warning(
                "429 (%s) sur %s — plus aucun appel BSD avant %s (dans %.0fs). Corps: %s",
                code, path, self._blocked_until.isoformat(), retry_after, resp.text[:500],
            )
            raise BSDRateLimitError(path, retry_after, code)

        if resp.status_code == 402:
            raise BSDPaidFeatureError(
                f"{path} nécessite un add-on payant (Football Unlimited / Sports Addon): "
                f"{resp.text}"
            )

        if resp.status_code == 404:
            raise KeyError(f"404 BSD: {path}")

        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Itère sur tous les résultats d'un endpoint paginé (limit/offset)."""
        params = dict(params or {})
        params.setdefault("limit", 200)  # max autorisé par la doc
        offset = 0
        while True:
            params["offset"] = offset
            page = self._get(path, params=params)
            results = page.get("results", [])
            yield from results
            if not page.get("next") or not results:
                break
            offset += params["limit"]

    # ---- Coverage (pas d'auth requise) ----------------------------------
    def coverage(self, sport: str | None = None) -> dict[str, Any]:
        params = {"sport": sport} if sport else None
        return self._get("/coverage/", params=params)

    # ---- Events / matches -------------------------------------------------
    def list_events(
        self,
        league_id: int | None = None,
        season_id: int | None = None,
        team_id: int | None = None,
        status: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        stage: str | None = None,
        round_: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        params = {
            "league_id": league_id,
            "season_id": season_id,
            "team_id": team_id,
            "status": status,
            "date_from": date_from,
            "date_to": date_to,
            "stage": stage,
            "round": round_,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self._paginate("/events/", params=params)

    def live_events(
        self,
        league_id: int | None = None,
        season_id: int | None = None,
        team_id: int | None = None,
    ) -> list[dict[str, Any]]:
        params = {"league_id": league_id, "season_id": season_id, "team_id": team_id}
        params = {k: v for k, v in params.items() if v is not None}
        data = self._get("/events/live/", params=params)
        # Fait vérifié sur payload réel (2026-09-20) : {"count": N, "events": [...]},
        # PAS {"results": [...]} comme les autres endpoints paginés. On garde un
        # repli sur "results" seulement pour ne pas planter si BSD harmonise un jour.
        if isinstance(data, dict):
            return data.get("events", data.get("results", []))
        return data

    def event_detail(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/")

    def event_stats(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/stats/")

    def event_lineups(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/lineups/")

    def event_incidents(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/incidents/")

    def event_player_stats(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/player-stats/")

    def event_h2h(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/h2h/")

    def event_odds(self, event_id: int) -> dict[str, Any]:
        """Consensus pré-match (11 marchés) pour UN match. Coûte 1 requête —
        préférer odds_feed() dès qu'on suit plus de quelques matchs."""
        return self._get(f"/events/{event_id}/odds/")

    def event_prediction(self, event_id: int) -> dict[str, Any]:
        return self._get(f"/events/{event_id}/prediction/")

    # ---- Odds bulk (voir docs/football/odds-predictions/) -----------------
    def odds_feed(
        self,
        updated_after: str | None = None,
        league_id: int | None = None,
        event_id: int | None = None,
        market: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        GET /api/v2/odds/ — une ligne par event×market×outcome×bookmaker
        ("consensus" en free plan). Avec `updated_after`, ne renvoie que ce
        qui a été relu depuis ce timestamp : c'est LE mécanisme documenté pour
        suivre les odds de beaucoup de matchs sans exploser le quota (1 appel
        couvre tous les matchs, filtré ensuite côté client sur les event_id
        qu'on suit réellement — voir ingestion/scheduler.py:poll_odds_bulk).
        Sans aucun filtre, l'API renvoie par défaut les dernières 48h
        d'observations (comportement documenté, pas une supposition).
        """
        params = {
            "updated_after": updated_after,
            "league_id": league_id,
            "event_id": event_id,
            "market": market,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self._paginate("/odds/", params=params)
