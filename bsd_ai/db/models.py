"""
Schéma Phase 1.

Principe directeur (section 6 du brief) : ne jamais écraser une donnée.
Chaque appel API produit une ligne neuve dans `raw_snapshots`, timestampée
`fetched_at`. Le feature engineering (phases suivantes) reconstruira l'état
"tel que connu à l'instant T" en filtrant sur `fetched_at <= T`, jamais en
lisant un état mutable. `matches` reste une table d'état courant (pratique
pour scheduler/dashboard) mais n'est jamais la source de vérité pour un
backtest — elle ne fait qu'accélérer les requêtes "quel est le prochain match
à poller".
"""
from __future__ import annotations

import datetime as dt
import enum

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MatchStatus(str, enum.Enum):
    upcoming = "upcoming"
    live = "live"
    finished = "finished"
    cancelled = "cancelled"
    postponed = "postponed"
    unresolved = "unresolved"


class SnapshotKind(str, enum.Enum):
    """Un membre par sous-ressource BSD effectivement ingérée en Phase 1."""

    detail = "detail"
    stats = "stats"
    lineups = "lineups"
    incidents = "incidents"
    player_stats = "player_stats"
    h2h = "h2h"
    odds = "odds"
    prediction = "prediction"


# Points de contrôle du live (brief section 5 : 15',30',HT,60',75',FT).
# Vu les vrais chiffres de quota (7 500 req/jour, doc conventions/#rate-limits),
# du live continu (ce que faisait la v1 de ce fichier, toutes les 30s) est
# hors budget de plusieurs ordres de grandeur. On revient à l'esprit du brief :
# un snapshot détail+stats+incidents PAR POINT DE CONTRÔLE, pas en continu.
LIVE_CHECKPOINT_MINUTES: tuple[int, ...] = (15, 30, 45, 60, 75, 90)


class Match(Base):
    """État courant d'un match — table de pilotage, pas source de vérité backtest."""

    __tablename__ = "matches"

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # id BSD, pas d'auto-increment
    league_id: Mapped[int | None] = mapped_column(Integer, index=True)
    season_id: Mapped[int | None] = mapped_column(Integer, index=True)
    home_team_id: Mapped[int | None] = mapped_column(Integer)
    away_team_id: Mapped[int | None] = mapped_column(Integer)
    home_team_name: Mapped[str | None] = mapped_column(String)
    away_team_name: Mapped[str | None] = mapped_column(String)

    status: Mapped[str] = mapped_column(Enum(MatchStatus), index=True)
    kickoff_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    has_xg: Mapped[bool | None] = mapped_column()

    # État live tel que renvoyé PAR LE MÊME payload compact que le score/statut
    # (/events/ et /events/live/ le portent déjà — vérifié sur payloads réels
    # du 2026-09-20). Ça ne coûte AUCUN appel supplémentaire : c'est juste ce
    # qu'on stocke déjà en le lisant, pour piloter les checkpoints ci-dessous
    # sans avoir à interroger /events/{id}/ à chaque fois.
    current_minute: Mapped[int | None] = mapped_column(Integer)
    period: Mapped[str | None] = mapped_column(String)
    home_score: Mapped[int | None] = mapped_column(Integer)
    away_score: Mapped[int | None] = mapped_column(Integer)

    # Dernier point de contrôle (parmi LIVE_CHECKPOINT_MINUTES) pour lequel on
    # a déjà pris un snapshot detail+stats+incidents. Évite de re-snapshoter
    # le même checkpoint à chaque cycle de poll_live tant que la minute n'a
    # pas dépassé le suivant.
    last_snapshot_minute: Mapped[int | None] = mapped_column(Integer)

    # BUG corrigé le 2026-09-20 : sans ce flag, snapshot_prematch_due
    # re-snapshotait (detail+lineups+h2h+prediction, jusqu'à 4 appels) TOUS
    # les matchs à moins de 24h du coup d'envoi À CHAQUE CYCLE (5 min),
    # indéfiniment — observé en prod : 8623 raw_snapshots pour 436 matchs.
    # Avec ce flag, un seul passage complet par match tant qu'il est upcoming.
    prematch_snapshotted_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    last_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: dt.datetime.now(dt.timezone.utc)
    )

    __table_args__ = (Index("ix_matches_status_kickoff", "status", "kickoff_at"),)


class RawSnapshot(Base):
    """
    Copie brute et immuable d'une réponse BSD pour un event et une sous-ressource
    donnés, à un instant donné. C'est la source de vérité pour tout backtest :
    on ne modifie jamais une ligne existante, on en insère une nouvelle.
    """

    __tablename__ = "raw_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(BigInteger, index=True)
    kind: Mapped[str] = mapped_column(Enum(SnapshotKind), index=True)
    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), index=True)

    # Minute de match au moment du fetch, quand connue (utile pour retrouver
    # rapidement le snapshot "à la 30e minute" sans reparser le payload).
    # None pour les snapshots pré-match.
    match_minute: Mapped[int | None] = mapped_column(Integer)

    payload: Mapped[dict] = mapped_column(JSON)  # réponse JSON brute, telle quelle

    __table_args__ = (
        Index("ix_snapshots_event_kind_time", "event_id", "kind", "fetched_at"),
    )


class OddsFeedRow(Base):
    """
    Ligne du feed BULK `/api/v2/odds/` (doc football/odds-predictions/) :
    une ligne par event × market × outcome × bookmaker, "consensus" en free
    plan. On stocke CHAQUE observation (une ligne par updated_at) plutôt que
    d'écraser — même principe immuable que raw_snapshots, mais ce feed est
    déjà normalisé donc pas besoin de repasser par du JSON brut.

    Remplace l'ancien OddsSnapshot (v1 de ce fichier), qui appelait
    /events/{id}/odds/ UNE FOIS PAR MATCH — invivable avec le quota 7 500/jour
    dès qu'on suit plus de quelques dizaines de matchs. Le feed bulk avec
    `updated_after` couvre TOUS les matchs suivis en un seul appel.
    """

    __tablename__ = "odds_feed_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(BigInteger, index=True)
    market: Mapped[str] = mapped_column(String, index=True)
    outcome: Mapped[str] = mapped_column(String)
    bookmaker_slug: Mapped[str] = mapped_column(String)  # "consensus" en free plan

    decimal_odds: Mapped[float | None] = mapped_column(Float)
    previous_decimal_odds: Mapped[float | None] = mapped_column(Float)
    opening_decimal_odds: Mapped[float | None] = mapped_column(Float)
    movement: Mapped[str | None] = mapped_column(String)  # SHORTENING / DRIFTING / None
    bookmaker_count: Mapped[int | None] = mapped_column(Integer)

    opening_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), index=True)
    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True))  # quand NOUS l'avons ingérée

    __table_args__ = (
        UniqueConstraint(
            "event_id", "market", "outcome", "bookmaker_slug", "updated_at",
            name="uq_odds_row_observation",
        ),
        Index("ix_odds_feed_event_market", "event_id", "market", "updated_at"),
    )


class IngestionCursor(Base):
    """Petit key/value pour les curseurs de polling (ex: updated_after du feed
    odds bulk). Persisté en DB pour survivre à un redémarrage du process."""

    __tablename__ = "ingestion_cursors"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: dt.datetime.now(dt.timezone.utc)
    )
