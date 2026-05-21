# POC Avantages sportifs — Sport Data Solution

Dispositif d’avantages pour encourager la pratique sportive des collaborateurs (prime mobilité active, journées bien-être, visibilité collective via Slack). Ce dépôt contient le **POC technique** : ingestion, CDC, notifications et contrôles qualité.

## Architecture

```text
Excel (RH + sportif)
       ↓
  generator ──► PostgreSQL (activities)
                       ↓ CDC (Debezium)
                 Redpanda (dbz.public.activities)
                    ↙         ↘
            slack/notify    (futur) Spark → Delta → Power BI
```

| Composant | Statut |
|-----------|--------|
| PostgreSQL + génération activités | ✅ |
| Debezium + Redpanda + Console | ✅ |
| Notifications Slack personnalisées | ✅ |
| Qualité des données (Soda) | ✅ |
| Data lake Spark + Delta + S3 | 🔜 |
| Power BI (KPI, impact financier) | 🔜 |

## Données

| Fichier | Usage |
|---------|--------|
| `data/Données+RH.xlsx` | Référentiel salariés (`ID salarié`, `Prénom`, `Nom`, moyen de déplacement, …) |
| `data/Données+Sportive.xlsx` | Pratique sportive par salarié |

Table **`activities`** : `id`, `employee_id`, `start_date`, `end_date`, `activity_type`, `distance_m`, `comment`.

## Prérequis

- Docker (Docker Desktop, WSL sous Windows)
- Fichiers Excel dans `data/`
- `.env` à partir de `.env.example` (`SLACK_WEBHOOK_URL`)

## Démarrage

```bash
cp .env.example .env
# Éditer .env : URL du webhook Slack

docker compose up -d --build
bash debezium/register-connector.sh
```

## Interfaces

| URL | Rôle |
|-----|------|
| [http://localhost:8080](http://localhost:8080) | Redpanda Console |
| http://localhost:8083 | API Debezium |
| `localhost:5432` | PostgreSQL (`sportsdb` / `appuser` / `apppass`) |

Topic CDC : **`dbz.public.activities`**

## Scénarios d’usage

### 1. Historique 12 mois (sans flood Slack)

```bash
docker compose stop slack
docker compose run --rm generator
# option : SLACK_ENABLED=false dans .env si le service slack tourne encore
```

Environ **2 000–3 300** activités (`HISTORY_DAYS=365`, 15–35 activités/salarié avec un sport).  
**Ne pas** relancer `slack` juste après le generator (sinon rattrapage des milliers d’événements sur le topic).

### 2. Contrôles qualité (Soda)

```bash
docker compose run --rm soda
```

Règles : distances non négatives, dates cohérentes, champs obligatoires, pas de doublon `id`, fenêtre 12 mois. Détail : [docs/qualite-donnees.md](docs/qualite-donnees.md).

### 3. Démo live Slack (3 activités, 1/min)

```bash
docker compose up -d slack
docker compose run --rm slack python demo_insert.py
```

Messages personnalisés (prénom/nom depuis le RH), ex. *« Bravo Prénom Nom ! Tu viens de courir 10,8 km en 46 min ! … »*

## Commandes utiles

```bash
docker exec -it pg_activities psql -U appuser -d sportsdb -c "SELECT COUNT(*) FROM activities;"
docker compose exec redpanda rpk topic list
docker compose up -d --build slack   # après modif de notify.py
```

## Structure du dépôt

```text
├── data/
├── postgres/init/
├── generator/
├── debezium/
├── quality/              # Soda
├── slack/
├── docs/qualite-donnees.md
├── docker-compose.yml
└── .env.example
```

## Arrêt

```bash
docker compose down          # conserve les données
docker compose down -v       # reset complet (volumes)
```

## Suite prévue

- Spark + **Delta Lake** (bronze / gold) et référentiels sur S3
- Contrôle Google Maps (cohérence déplacement domicile ↔ bureau)
- **Power BI** : éligibilité avantages, coût estimé, historique rejouable
- Monitoring des flux
