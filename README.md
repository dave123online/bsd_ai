# BSD AI — Phase 1 : pipeline d'ingestion

## Révision 2026-09-20 : architecture revue pour tenir dans le quota gratuit

La première version de ce pipeline faisait du polling live continu (toutes
les 30s pour chaque match live) sans tenir compte du quota réel de l'API.
En usage réel, ça a fait tomber le compte sur `taster_exhausted` dès le
premier cycle. Faits vérifiés (doc `conventions/#rate-limits`) :

- **Quota gratuit : 7 500 requêtes/jour**, reset à minuit UTC.
- Deux 429 différents, distingués par le champ `code` du corps JSON :
  `taster_exhausted` (quota jour épuisé, `Retry-After` = secondes jusqu'à
  minuit) et `rate_limited` (burst 25 req/s par IP, `Retry-After` = 1s).
- Il existe un **feed odds en masse** (`GET /api/v2/odds/?updated_after=...`)
  qui couvre tous les matchs suivis en 1 seul appel, au lieu d'1 appel par
  match sur `/events/{id}/odds/`.

**Décisions prises avec David (2026-09-20) :** scope limité à 5-10 grandes
ligues européennes, odds via le feed bulk.

**Redesign en conséquence :**

| Job | Avant (v1, cassé) | Maintenant |
|---|---|---|
| Live | detail+stats+incidents **toutes les 30s** pour chaque match live | **checkpoints** : un seul snapshot par palier franchi (15',30',HT,60',75',FT) — c'est ce que demandait le brief section 5 depuis le début |
| Odds | 1 appel **par match** dû | 1 appel **bulk** (`/api/v2/odds/?updated_after=...`) qui couvre tous les matchs suivis, avec curseur persisté (table `ingestion_cursors`) |
| Scope | Aucun — toutes les ligues retournées par `/events/live/` étaient traitées | **Obligatoire** : `BSD_LEAGUE_IDS` doit être renseigné, sinon le scheduler refuse de tourner (voir `scheduler._tracked_league_filter`) |

Nouveau budget estimé (8 ligues, calculs dans `scheduler.py`) : de l'ordre de
3 500-4 500 requêtes/jour, contre plusieurs dizaines de milliers avant — testé
fonctionnellement (pas juste relu) avec des scripts qui simulent les vrais
scénarios, voir historique de conversation pour le détail des assertions.

## Scope : ligues suivies (toutes confirmées sur l'API réelle)

`.env.example` contient les 8 league_id du top 5 européen + 3 autres, tous
vérifiés directement contre `/api/v2/leagues/?country=...` (pas devinés) :
`Premier League=1, Serie A=4, La Liga=3, Bundesliga=5, Ligue 1=6,
Liga Portugal Betclic=2, Trendyol Super Lig=11, Swiss Super League=15`.

Note au passage : `/leagues/` n'accepte PAS de paramètre `search` — seuls
`country`, `include_inactive`, `is_women`, `limit`, `offset` sont acceptés
(confirmé par le 400 auto-documenté de l'API). Un 401 HTML nginx sur cet
endpoint (au lieu du JSON d'erreur BSD) signifie que `$BSD_API_KEY` n'est
pas chargée dans l'environnement du shell, pas un problème d'auth réel.

## ⚠️ Point bloquant toujours ouvert : odds live

L'API REST ne sert les odds qu'en pré-match (confirmé par la doc odds &
predictions : *"all prices are... for now, pre-match only"*). Le feed bulk
n'y change rien — c'est une limite de l'API, pas de notre implémentation.
Pour la Phase 4 (repricing live vs marché), il faudra le WebSocket BSD
(addon payant) ou accepter un marché figé en live. Toujours pas tranché.

## Setup

```bash
cd bsd_ai
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# éditer .env : BSD_API_KEY, DATABASE_URL, BSD_LEAGUE_IDS (voir ci-dessus)
python -m bsd_ai.main
```

Sans Postgres sous la main pour tester vite : mets
`DATABASE_URL=sqlite:///./bsd_ai.db` dans `.env`. SQLite ne gère pas bien les
écritures concurrentes — à ne pas garder en prod avec le scheduler qui tourne
en continu.

## TODO Phase 1.1 (identifiés en écrivant le code, pas encore faits)

- Distinguer explicitement T-24h et T-1h (brief section 5) au lieu d'un seul
  snapshot "moins de 24h avant coup d'envoi".
- Second passage de `snapshot_finished` quelques heures après le coup de
  sifflet (la doc signale que le bag de stats est corrigé en amont pendant
  ce délai).
- Flag `settled_at` explicite sur `Match` plutôt que la fenêtre glissante de
  10 min sur `last_seen_at` pour décider qu'un match "vient de finir".
- Découverte automatique des league_id via `/leagues/` (pour l'instant :
  `BSD_LEAGUE_IDS` en dur dans `.env`).
- Alembic dès que le schéma doit évoluer avec des données déjà en base.
- Surveiller la consommation réelle de quota une fois en prod (les headers
  `RateLimit` / `RateLimit-Policy` documentés donnent le compte exact —
  pas encore exploités dans le client).

## Prochaine étape proposée

Une fois que le run tourne un cycle complet sans 429 chez toi (au moins un
match qui passe par pré-match → live avec quelques checkpoints → finished),
on attaque la Phase 2 (baselines) sur les données collectées.

