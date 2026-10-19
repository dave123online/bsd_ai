"""
Phase 2, étape 1 : transforme les raw_snapshots (JSON brut, immuable) en un
dataset plat exploitable pour un baseline. Une ligne par match terminé, avec :
  - le résultat réel (label)
  - les odds pré-match (marché, pour calculer une probabilité implicite)
  - quelques features h2h (déjà présentes gratuitement dans le payload /events/)

Ne lit QUE des données antérieures au coup d'envoi (odds + h2h) pour prédire
un résultat POSTÉRIEUR (score final) — c'est la garantie anti-data-leakage
du brief (section 6) appliquée à l'extraction, pas seulement à l'ingestion.

Usage:
    python -m bsd_ai.analysis.extract_dataset --out dataset_1x2.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging

from sqlalchemy import select

from ..db.models import Match, MatchStatus, RawSnapshot, SnapshotKind
from ..db.session import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("bsd_ai.extract_dataset")


def _result_label(home_score: int, away_score: int) -> str:
    if home_score > away_score:
        return "H"
    if home_score < away_score:
        return "A"
    return "D"


def _h2h_features_excluding_self(h2h: dict, event_id: int, home_team_id: int | None) -> dict:
    """
    FUITE DE DONNÉES SUSPECTÉE ET CONTOURNÉE (2026-09-24) : les champs agrégés
    de `head_to_head` (total_matches, home_win_rate, avg_total_goals...) sont
    calculés par BSD au moment du fetch — et pour un match backfillé (déjà
    terminé), CE MATCH LUI-MÊME apparaît dans `recent_matches` (vérifié sur
    payload réel : recent_matches[0] avait le même event_id, la même date et
    le même score que le match qu'on essaie de prédire). Rien ne garantit que
    les agrégats top-level l'excluent — donc on ne leur fait PAS confiance :
    on recalcule nous-mêmes les stats h2h à partir de `recent_matches`, en
    filtrant explicitement ce match. Moins de matchs pris en compte (souvent
    seulement les ~10 de `recent_matches`, pas les `total_matches` complets),
    mais garanti sans la fuite.
    """
    recent = [m for m in h2h.get("recent_matches", []) if m.get("event_id") != event_id]
    if not recent:
        return {"h2h_n": 0, "h2h_home_win_rate": None, "h2h_away_win_rate": None, "h2h_avg_total_goals": None}

    home_wins = draws = away_wins = 0
    total_goals = 0
    for m in recent:
        # Le h2h liste les confrontations quel que soit qui jouait à domicile
        # ce jour-là — on ramène tout au repère "l'équipe qui est à domicile
        # DANS LE MATCH QU'ON PRÉDIT", via home_team_id.
        m_home_id = m.get("home_team_id")
        m_home_score, m_away_score = m.get("home_score"), m.get("away_score")
        if m_home_score is None or m_away_score is None:
            continue
        total_goals += m_home_score + m_away_score
        if m_home_score == m_away_score:
            draws += 1
        elif (m_home_score > m_away_score) == (m_home_id == home_team_id):
            home_wins += 1
        else:
            away_wins += 1

    n = home_wins + draws + away_wins
    if n == 0:
        return {"h2h_n": 0, "h2h_home_win_rate": None, "h2h_away_win_rate": None, "h2h_avg_total_goals": None}
    return {
        "h2h_n": n,
        "h2h_home_win_rate": home_wins / n,
        "h2h_away_win_rate": away_wins / n,
        "h2h_avg_total_goals": total_goals / n,
    }


FORM_WINDOW = 5  # nombre de matchs récents pris en compte
FORM_MIN_HISTORY = 2  # en dessous, on laisse les features à None plutôt que sur-interpréter 1 match


def _naive_utc(value):
    """
    BUG CONTOURNÉ (2026-09-28) : sous SQLite, un objet Match fraîchement
    ajouté à la session garde son kickoff_at timezone-aware original tant
    qu'il n'est pas rechargé depuis la base — mais tout objet relu (après
    expiration/commit) revient NAIF (SQLite n'a pas de type timestamp avec
    fuseau natif). Résultat observé : dans une même liste, certaines entrées
    étaient aware et d'autres naïves, faisant planter le tri/la comparaison
    (`TypeError: can't compare offset-naive and offset-aware datetimes`).
    On ne fait plus confiance au round-trip DB : tout est normalisé en naïf
    UTC ici, au moment de la comparaison, indépendamment de ce que renvoie
    le driver. (Sous Postgres ce bug n'existe pas — TIMESTAMPTZ est fiable —
    mais la normalisation reste inoffensive dans ce cas.)
    """
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


def _build_team_history(session) -> dict[int, list[dict]]:
    """
    Historique chronologique par équipe, construit sur TOUS les matchs
    terminés avec stats (1392 dans la base actuelle, pas seulement le
    sous-ensemble avec odds) — plus de matière pour la forme récente que si
    on se limitait aux ~640 matchs odds+résultat.

    Champs stats réels confirmés sur payload (struct.txt, 2026-09-28) :
    expected_goals, total_shots, shots_on_target, corner_kicks — pas de nom
    deviné.
    """
    finished = session.scalars(
        select(Match).where(
            Match.status == MatchStatus.finished,
            Match.home_score.is_not(None),
            Match.away_score.is_not(None),
            Match.kickoff_at.is_not(None),
        )
    ).all()

    history: dict[int, list[dict]] = {}
    for match in finished:
        stats_snap = session.scalars(
            select(RawSnapshot)
            .where(RawSnapshot.event_id == match.event_id, RawSnapshot.kind == SnapshotKind.stats)
            .order_by(RawSnapshot.fetched_at.desc())
            .limit(1)
        ).first()
        stats = stats_snap.payload.get("stats", {}) if stats_snap else {}
        home_s, away_s = stats.get("home", {}), stats.get("away", {})

        def _entry(team_id, goals_for, goals_against, own_stats, opp_stats):
            return {
                "kickoff_at": _naive_utc(match.kickoff_at),
                "goals_for": goals_for,
                "goals_against": goals_against,
                "xg_for": own_stats.get("expected_goals"),
                "xg_against": opp_stats.get("expected_goals"),
                "shots_for": own_stats.get("total_shots"),
                "shots_on_target_for": own_stats.get("shots_on_target"),
                "corners_for": own_stats.get("corner_kicks"),
            }

        if match.home_team_id is not None:
            history.setdefault(match.home_team_id, []).append(
                _entry(match.home_team_id, match.home_score, match.away_score, home_s, away_s)
            )
        if match.away_team_id is not None:
            history.setdefault(match.away_team_id, []).append(
                _entry(match.away_team_id, match.away_score, match.home_score, away_s, home_s)
            )

    for team_id in history:
        history[team_id].sort(key=lambda e: e["kickoff_at"])
    return history


def _recent_form(history_by_team: dict[int, list[dict]], team_id: int | None, before_kickoff, prefix: str) -> dict:
    """
    Moyenne sur les FORM_WINDOW derniers matchs strictement ANTÉRIEURS à
    before_kickoff — jamais le match courant ni un match futur, même si un
    autre match de cette équipe a été ingéré/backfillé après coup avec une
    date antérieure (le tri + le filtre strict `<` avant_kickoff garantissent
    ça indépendamment de l'ordre d'insertion en base).
    """
    empty = {f"{prefix}_n": 0, f"{prefix}_goals_for": None, f"{prefix}_goals_against": None,
             f"{prefix}_xg_for": None, f"{prefix}_xg_against": None, f"{prefix}_shots_for": None,
             f"{prefix}_corners_for": None}
    if team_id is None or before_kickoff is None:
        return empty
    before_kickoff = _naive_utc(before_kickoff)

    past = [e for e in history_by_team.get(team_id, []) if e["kickoff_at"] < before_kickoff]
    recent = past[-FORM_WINDOW:]
    if len(recent) < FORM_MIN_HISTORY:
        return empty

    def _avg(key):
        vals = [e[key] for e in recent if e.get(key) is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        f"{prefix}_n": len(recent),
        f"{prefix}_goals_for": _avg("goals_for"),
        f"{prefix}_goals_against": _avg("goals_against"),
        f"{prefix}_xg_for": _avg("xg_for"),
        f"{prefix}_xg_against": _avg("xg_against"),
        f"{prefix}_shots_for": _avg("shots_for"),
        f"{prefix}_corners_for": _avg("corners_for"),
    }


def extract(session) -> list[dict]:
    rows: list[dict] = []
    team_history = _build_team_history(session)

    finished = session.scalars(
        select(Match).where(
            Match.status == MatchStatus.finished,
            Match.home_score.is_not(None),
            Match.away_score.is_not(None),
        )
    ).all()

    for match in finished:
        # Odds : on prend le DERNIER snapshot odds vu pour ce match (en
        # backfill historique, il n'y en a qu'un — le figé pré-match). En
        # présence de plusieurs (ingestion continue), le dernier reste le
        # plus proche du coup d'envoi, donc toujours antérieur au résultat.
        odds_snap = session.scalars(
            select(RawSnapshot)
            .where(RawSnapshot.event_id == match.event_id, RawSnapshot.kind == SnapshotKind.odds)
            .order_by(RawSnapshot.fetched_at.desc())
            .limit(1)
        ).first()
        if odds_snap is None:
            continue
        odds = odds_snap.payload.get("odds", {})
        if odds.get("home_win") is None:
            continue  # odds jamais renseignées (event_odds a renvoyé null partout)

        # H2H : déjà dans le payload detail (voir backfill.py), pas d'appel séparé.
        h2h_snap = session.scalars(
            select(RawSnapshot)
            .where(RawSnapshot.event_id == match.event_id, RawSnapshot.kind == SnapshotKind.h2h)
            .order_by(RawSnapshot.fetched_at.desc())
            .limit(1)
        ).first()
        h2h = h2h_snap.payload if h2h_snap else {}

        total_goals = match.home_score + match.away_score
        h2h_features = _h2h_features_excluding_self(h2h, match.event_id, match.home_team_id)
        home_form = _recent_form(team_history, match.home_team_id, match.kickoff_at, "home_form")
        away_form = _recent_form(team_history, match.away_team_id, match.kickoff_at, "away_form")
        rows.append({
            "event_id": match.event_id,
            "league_id": match.league_id,
            "kickoff_at": match.kickoff_at.isoformat() if match.kickoff_at else None,
            "home_team": match.home_team_name,
            "away_team": match.away_team_name,
            "home_score": match.home_score,
            "away_score": match.away_score,
            "result": _result_label(match.home_score, match.away_score),
            "total_goals": total_goals,
            "over_25": int(total_goals > 2.5),
            "btts": int(match.home_score > 0 and match.away_score > 0),
            # Marché (feature ET benchmark — voir analysis/baseline_1x2.py)
            "odds_home_win": odds.get("home_win"),
            "odds_draw": odds.get("draw"),
            "odds_away_win": odds.get("away_win"),
            "odds_over_25": odds.get("over_25_goals"),
            "odds_under_25": odds.get("under_25_goals"),
            "odds_btts_yes": odds.get("btts_yes"),
            "odds_btts_no": odds.get("btts_no"),
            # H2H pré-match, recalculé sans le match courant (voir
            # _h2h_features_excluding_self — fuite de données contournée)
            "h2h_total_matches": h2h_features["h2h_n"],
            "h2h_home_win_rate": h2h_features["h2h_home_win_rate"],
            "h2h_away_win_rate": h2h_features["h2h_away_win_rate"],
            "h2h_avg_total_goals": h2h_features["h2h_avg_total_goals"],
            **home_form,
            **away_form,
        })

    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="dataset_1x2.csv")
    args = parser.parse_args()

    session = get_session()
    rows = extract(session)
    if not rows:
        logger.warning("Aucune ligne extraite — vérifie que le backfill a bien tourné et a des odds.")
        return

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    logger.info("%d lignes écrites dans %s", len(rows), args.out)


if __name__ == "__main__":
    main()
