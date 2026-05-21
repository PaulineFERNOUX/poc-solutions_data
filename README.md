# POC Avantages sportifs — Sport Data Solution

**Contexte.** Dispositif d’avantages pour encourager la pratique sportive des collaborateurs (prime mobilité active, journées bien-être, visibilité sur Slack). Ce dépôt contient le **POC technique** : pipeline de données de bout en bout.

## Architecture

```text
Excel → generator → PostgreSQL (activities)
                          ↓ CDC (Debezium)
                    Redpanda (dbz.public.activities)
                          ↓
                    Slack (messages personnalisés)
                          ↓
                    S3 (à venir)
```

## Données

| Fichier | Usage |
|---------|--------|
| `data/Données+RH.xlsx` | Salariés (`ID salarié`, `Prénom`, `Nom`, …) |
| `data/Données+Sportive.xlsx` | Pratique sportive par salarié |

Les activités (simulation type Strava) sont dans la table `activities` : id, employee_id, dates, type, distance_m, commentaire.

## Prérequis

- Docker (Docker Desktop, WSL recommandé sous Windows)
- Fichiers Excel dans `data/` (non versionnés si données sensibles — à adapter selon votre politique)
- Fichier `.env` avec `SLACK_WEBHOOK_URL` (voir `.env.example`)

## Installation et démarrage

```bash
cp .env.example .env
# Éditer .env : coller l’URL du webhook Slack

docker compose up -d --build
bash debezium/register-connector.sh
```

## Interfaces

| URL / commande | Rôle |
|----------------|------|
| [http://localhost:8080](http://localhost:8080) | Redpanda Console (topics, messages) |
| http://localhost:8083 | API Debezium / Kafka Connect |
| `localhost:5432` | PostgreSQL — `sportsdb` / `appuser` / `apppass` |

Topic CDC : **`dbz.public.activities`**

## Démo live (Slack)

Le service `slack` écoute Redpanda et poste sur Slack uniquement les **nouveaux INSERT** (`op: c`), avec prénom/nom depuis le fichier RH.

Messages type : *« Bravo Prénom Nom ! Tu viens de courir 10,8 km en 46 min ! … »*

```bash
# Consumer Slack déjà lancé via docker compose up -d
docker compose run --rm slack python demo_insert.py
```

La démo insère **3 activités**, espacées d’**1 minute**.

## Autres commandes utiles

```bash
# Compter les activités en base
docker exec -it pg_activities psql -U appuser -d sportsdb -c "SELECT COUNT(*) FROM activities;"

# Lister les topics
docker compose exec redpanda rpk topic list

# Remplir la base en masse (simulation initiale)
docker compose run --rm generator

# Reconstruire le consumer Slack après modification de notify.py
docker compose up -d --build slack
```

## Arrêt

```bash
docker compose down          # conserve les données (volume pgdata)
docker compose down -v       # supprime volumes (reset complet)
```

## Structure du dépôt

```text
├── data/                    # Excel RH + sportif (local)
├── postgres/init/           # Schéma SQL
├── generator/               # Génération d’activités
├── debezium/                # Connecteur CDC
├── slack/                   # notify.py + demo_insert.py
├── docker-compose.yml
└── .env.example
```

## Suite prévue

- Archivage **S3** (boto3) sur les événements Redpanda
- **Power BI** : indicateurs et historique rejouable

## Dépannage Docker (WSL)

Si `docker compose up --build` échoue avec `docker-credential-desktop.exe: exec format error`, retirer `"credsStore"` dans `~/.docker/config.json` sous WSL, ou lancer Docker depuis PowerShell.
