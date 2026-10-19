"""
Phase 2, étape 2 : premier baseline sur le marché 1X2.

Deux "modèles" évalués et comparés, sur les mêmes métriques (brief section 14
et 19 — calibration avant tout, pas juste l'accuracy) :

1. MARCHÉ (benchmark, pas un modèle à nous) : probabilité implicite des odds,
   dévigorée (overround retiré). C'est la barre à battre — si notre propre
   modèle ne fait pas mieux que ça en calibration, il n'y a pas de EV réel.
2. BASELINE h2h : régression logistique multinomiale sur les seules features
   h2h pré-match (déjà gratuites, aucun call API supplémentaire).

Split TEMPOREL (pas aléatoire) — brief section 20 : on trie par date, les
20% de matchs les plus récents servent de test, jamais mélangés avec le train.
Avec seulement ~420 matchs c'est un signal, pas une preuve : à re-vérifier
quand le dataset aura grossi (retour des championnats après la trêve).

Usage:
    python -m bsd_ai.analysis.baseline_1x2 --data dataset_1x2.csv
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.preprocessing import StandardScaler

RESULT_CLASSES = ["H", "D", "A"]


def devig(row: pd.Series) -> pd.Series:
    """Retire l'overround du bookmaker : probabilité implicite normalisée à 1."""
    inv = np.array([1 / row["odds_home_win"], 1 / row["odds_draw"], 1 / row["odds_away_win"]])
    return pd.Series(inv / inv.sum(), index=[f"market_p_{c}" for c in RESULT_CLASSES])


def evaluate(y_true: np.ndarray, proba: np.ndarray, label: str) -> None:
    """log loss et Brier multiclasse (moyenne des Brier one-vs-rest par classe,
    voir brief section 14 — les deux sont demandés explicitement)."""
    y_true_idx = np.array([RESULT_CLASSES.index(y) for y in y_true])
    ll = log_loss(y_true_idx, proba, labels=list(range(len(RESULT_CLASSES))))

    briers = []
    for i, cls in enumerate(RESULT_CLASSES):
        y_bin = (y_true_idx == i).astype(int)
        briers.append(brier_score_loss(y_bin, proba[:, i]))
    brier = float(np.mean(briers))

    # Accuracy donnée en dernier et volontairement secondaire — le brief est
    # explicite (section 19) : ce n'est PAS la métrique qui compte le plus.
    pred_idx = proba.argmax(axis=1)
    acc = (pred_idx == y_true_idx).mean()

    print(f"\n--- {label} ---")
    print(f"  log loss : {ll:.4f}  (plus bas = mieux)")
    print(f"  brier    : {brier:.4f}  (plus bas = mieux)")
    print(f"  accuracy : {acc:.3f}  (secondaire, cf. brief section 19)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="dataset_1x2.csv")
    parser.add_argument("--test-fraction", type=float, default=0.2)
    args = parser.parse_args()

    df = pd.read_csv(args.data, parse_dates=["kickoff_at"])
    df = df.dropna(subset=["odds_home_win", "odds_draw", "odds_away_win"])
    df = df.sort_values("kickoff_at").reset_index(drop=True)

    n_test = max(1, int(len(df) * args.test_fraction))
    train, test = df.iloc[:-n_test], df.iloc[-n_test:]
    print(f"Dataset: {len(df)} matchs -> train={len(train)} / test={len(test)} (split temporel)")
    if len(test) < 20:
        print("ATTENTION: moins de 20 matchs en test — les métriques ci-dessous ne sont "
              "PAS fiables statistiquement, juste indicatives. Ne pas conclure dessus seul.")

    # --- 1. Marché dévigoré (benchmark, pas un modèle) ---
    market_test = test.apply(devig, axis=1)
    market_proba = market_test[[f"market_p_{c}" for c in RESULT_CLASSES]].to_numpy()
    evaluate(test["result"].to_numpy(), market_proba, "MARCHÉ (dévigoré) — benchmark")

    # --- 2. Baseline h2h seul (pour référence, cf. runs précédents) ---
    h2h_cols = ["h2h_home_win_rate", "h2h_away_win_rate", "h2h_avg_total_goals", "h2h_total_matches"]
    _fit_and_evaluate(train, test, h2h_cols, "BASELINE h2h seul (régression logistique)")

    # --- 3. Baseline h2h + forme récente (buts/xG/tirs/corners sur les 5
    # derniers matchs de chaque équipe, calculés en excluant tout match
    # postérieur — voir extract_dataset.py::_recent_form) ---
    form_cols = [
        "home_form_goals_for", "home_form_goals_against", "home_form_xg_for", "home_form_xg_against",
        "home_form_shots_for", "home_form_corners_for",
        "away_form_goals_for", "away_form_goals_against", "away_form_xg_for", "away_form_xg_against",
        "away_form_shots_for", "away_form_corners_for",
    ]
    _fit_and_evaluate(train, test, h2h_cols + form_cols, "BASELINE h2h + forme récente (régression logistique)")

    print(
        "\nLecture : si les baselines ne battent pas le marché dévigoré en log loss/brier, "
        "c'est attendu à ce stade (peu de matchs, features limitées) — le marché agrège "
        "bien plus d'information. Comparer surtout les DEUX baselines entre eux : si "
        "'h2h + forme' bat nettement 'h2h seul', la forme récente apporte un vrai signal "
        "et vaut la peine d'être approfondie (plus de matchs de fenêtre, plus de features "
        "stats). Si l'écart est nul ou négatif, c'est un signal à ne pas sur-interpréter "
        "vu la taille du test set actuel."
    )


def _fit_and_evaluate(train: pd.DataFrame, test: pd.DataFrame, feature_cols: list[str], label: str) -> None:
    train_feat = train.dropna(subset=feature_cols)
    test_feat = test.dropna(subset=feature_cols)
    if len(train_feat) < 30 or len(test_feat) == 0:
        print(f"\n--- {label} ---\nPas assez de matchs avec toutes les features renseignées "
              f"({len(train_feat)} train / {len(test_feat)} test) — features utilisées: {feature_cols}")
        return

    scaler = StandardScaler().fit(train_feat[feature_cols])
    X_train = scaler.transform(train_feat[feature_cols])
    X_test = scaler.transform(test_feat[feature_cols])

    clf = LogisticRegression(max_iter=1000)
    clf.fit(X_train, train_feat["result"])
    proba_raw = clf.predict_proba(X_test)
    proba = np.zeros((len(test_feat), 3))
    for i, cls in enumerate(clf.classes_):
        proba[:, RESULT_CLASSES.index(cls)] = proba_raw[:, i]

    evaluate(test_feat["result"].to_numpy(), proba, f"{label}  [n_train={len(train_feat)}, n_test={len(test_feat)}]")


if __name__ == "__main__":
    main()
