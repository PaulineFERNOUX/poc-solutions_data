# POC Avantages sportifs

Pipeline de données pour le calcul et le pilotage des avantages salariés (prime sportive, jours de bien-être), de la source opérationnelle jusqu’à la restitution Power BI.

**Stack :** Docker (pipeline) + Power BI Desktop (visualisation). Détail BI : [`powerbi/README.md`](powerbi/README.md).

---

## Objectif

Ingérer des activités sportives et des référentiels RH, enrichir les données (trajet domicile–travail), calculer les KPI par salarié et par année, puis exposer le résultat à Power BI.

**Livrable analytique :** table `gold/benefits_employee_year` (1 ligne par salarié × année).

---

## Prérequis

- Docker Desktop
- Terminal (depuis la racine du projet)
- Fichiers sources dans `data/` :
  - `Données+RH.xlsx`
  - `Données+Sportive.xlsx`
- Fichier `.env` (copie de `.env.example`) avec au minimum `GOOGLE_MAPS_API_KEY` pour la couche Silver ; `SLACK_WEBHOOK_URL` pour les notifications (optionnel)

> Lancer Docker avec une liste explicite de services : `docker compose up -d postgres minio …`

---

## Architecture

```
Excel sport  →  generator   →  Postgres  →  Debezium  →  Kafka
                                    ↓
                          Spark bronze     →  MinIO bronze/
Excel RH/sport  →  Spark ref           →  MinIO ref/
                                    ↓
                          Spark silver     →  MinIO silver/
                                    ↓
                          Spark gold       →  MinIO gold/
                                    ↓
                          Export Parquet   →  Power BI
Postgres INSERT  →  Debezium  →  Kafka  →  Slack (notification)
```

| Couche | Contenu |
|--------|---------|
| Bronze | État courant des activités (CDC Debezium : create / update / delete) |
| Ref | Référentiels RH et sport (Excel) |
| Silver | Activités alignées sur le bronze, RH enrichi (distance, anomalie trajet) |
| Gold | KPI avantages par salarié et par année |
| Power BI | Lecture du Parquet exporté sur MinIO |

---

## Installation initiale

Exécuter depuis la **racine du projet**.

```bash
# Réinitialisation complète (optionnel)
docker compose down -v

# 1. Infrastructure
docker compose up -d --build postgres redpanda debezium console minio
docker compose run --rm minio-init
bash debezium/register-connector.sh

# 2. Jeu de données Postgres (une seule exécution)
docker compose run --rm generator

# 3. Pipeline Spark
docker compose build spark
docker compose run --rm spark --master 'local[*]' /app/jobs/bronze_kafka.py
docker compose run --rm spark --master 'local[*]' /app/jobs/ref_load.py
docker compose run --rm spark --master 'local[*]' /app/jobs/silver_enrichment.py
docker compose run --rm spark --master 'local[*]' /app/jobs/gold_benefits.py
docker compose run --rm spark --master 'local[*]' /app/jobs/export_powerbi_parquet.py
```

**Validation :** activités présentes en Postgres, événements CDC (`c`/`u`/`d`) visibles dans [Redpanda Console](http://localhost:8080), bucket `datalake` dans [MinIO](http://localhost:9001).

**Power BI :** [`powerbi/README.md`](powerbi/README.md) (`setup.ps1` + `load_benefits.py`).

**Qualité Postgres (optionnel) :** `docker compose run --rm soda`

---

## CDC Debezium (flux incrémental)

Debezium capture les changements Postgres (`INSERT`, `UPDATE`, `DELETE`) et les publie sur Kafka (`dbz.public.activities`). Le job `bronze_kafka.py` :

1. Lit **uniquement les nouveaux messages** Kafka (checkpoint offsets sur MinIO : `_checkpoints/bronze_kafka_offsets.json`)
2. Applique un **MERGE Delta** sur `bronze/activities` :
   - `c` / `u` / `r` (snapshot) → insert ou mise à jour par `id`
   - `d` → suppression physique de la ligne
3. Enregistre le checkpoint **après** merge réussi (rejeu idempotent)

La couche Silver lit l’état courant du bronze (plus de filtre `op = c`).

| Variable | Effet |
|----------|--------|
| `BRONZE_KAFKA_RESET=true` | Supprime checkpoint + table bronze, relit Kafka depuis le début |
| `BRONZE_KAFKA_CHECKPOINT` | URI du fichier checkpoint (défaut : `s3a://datalake/_checkpoints/...`) |

**Rejeu complet datalake :** `docker compose down -v`, puis enchaînement d’installation initiale.

**Nouvelles activités Postgres** (après `generator` ou inserts manuels) : relancer uniquement la chaîne Spark (Debezium doit rester actif).

---

## Utilisation courante

Après redémarrage machine ou `docker compose down` **sans** `-v` : ne **pas** relancer `generator`.

```bash
docker compose up -d postgres redpanda debezium console minio

docker compose build spark
docker compose run --rm spark --master 'local[*]' /app/jobs/bronze_kafka.py
docker compose run --rm spark --master 'local[*]' /app/jobs/ref_load.py
docker compose run --rm spark --master 'local[*]' /app/jobs/silver_enrichment.py
docker compose run --rm spark --master 'local[*]' /app/jobs/gold_benefits.py
docker compose run --rm spark --master 'local[*]' /app/jobs/export_powerbi_parquet.py
```

Actualiser ensuite le rapport Power BI (**Accueil → Actualiser**).

---

## Règles métier

Paramètres configurables : [`spark/config/rules.yml`](spark/config/rules.yml) (section `gold`).

| Indicateur | Règle |
|------------|--------|
| **Prime sportive** (5 % du salaire brut) | Sport déclaré, mode de déplacement éligible, absence d’anomalie de trajet |
| **Jours de bien-être** (5 jours) | Au moins 15 activités physiques sur l’année |

**Sources Excel :** `Salaire brut`, `Moyen de déplacement` (RH) ; `Pratique d'un sport` (sport).

**Contrôles qualité Gold :** unicité `(employee_id, year)` et absence de doublon sur le référentiel RH — le job `gold_benefits.py` échoue si ces règles ne sont pas respectées.

---

## Configuration

### Générateur d’activités

Variables dans `docker-compose.yml` (surcharge possible via `.env`) :

| Variable | Valeur par défaut |
|----------|-------------------|
| `GENERATOR_START_DATE` | `2025-01-01` |
| `GENERATOR_END_DATE` | date du jour (UTC) |
| `ACTIVITIES_PER_EMPLOYEE_MIN` / `MAX` | `0` / `50` |

Modification de la période ou du volume : vider la table (`TRUNCATE activities;`) ou `docker compose down -v`, puis relancer `generator` **une fois**.

---

## Slack (optionnel)

Notifications sur chaque **nouvelle activité** (`INSERT` Postgres → Debezium → Kafka). Le consumer écoute le topic `dbz.public.activities` et poste sur Slack via webhook.

**Prérequis :** `SLACK_WEBHOOK_URL` dans `.env` (voir `.env.example`).

```bash
# 1. Démarrer le consumer (profil slack — non lancé par défaut)
docker compose --profile slack up -d slack

# 2. Insérer des activités de démo (1 toutes les 60 s par défaut)
docker compose --profile slack run --rm slack python demo_insert.py
```

| Variable | Effet |
|----------|--------|
| `SLACK_ENABLED=false` | Écoute Kafka sans envoyer sur Slack (ex. chargement masse) |
| `DEMO_COUNT` | Nombre d’INSERT de démo (défaut : `3`) |
| `DEMO_INTERVAL_SEC` | Intervalle entre INSERT (défaut : `60`) |

Après un INSERT, relancer la chaîne Spark puis actualiser Power BI pour voir l’impact dans le reporting (voir [Utilisation courante](#utilisation-courante)).

**Arrêt :** `docker compose --profile slack stop slack`

---

## Documentation complémentaire

| Sujet | Fichier |
|-------|---------|
| Connexion Power BI | [`powerbi/README.md`](powerbi/README.md) |
| Contrôles Soda (Postgres) | [`quality/checks.yml`](quality/checks.yml) |

---

## Services locaux

| URL | Service | Identifiants |
|-----|---------|--------------|
| http://localhost:9001 | MinIO Console | `minioadmin` / `minioadmin` |
| http://localhost:8080 | Redpanda Console | — |
| http://localhost:8083 | Debezium | — |

```bash
docker compose down      # arrêt, données conservées
docker compose down -v   # suppression des volumes
```

**Debezium indisponible au démarrage :** attendre l’API (`curl http://localhost:8083/`) puis exécuter `bash debezium/register-connector.sh`.

---

## Jobs Spark

| Job | Description |
|-----|-------------|
| `bronze_kafka.py` | CDC Kafka → MERGE Delta bronze (incrémental + checkpoint) |
| `ref_load.py` | Chargement Excel → Delta ref |
| `silver_enrichment.py` | Enrichissement RH (Google Routes) |
| `gold_benefits.py` | Calcul des KPI + contrôles qualité |
| `export_powerbi_parquet.py` | Export Parquet vers MinIO |

Silver : seuls les modes **Marche/running** (15 km max) et **Vélo/Trottinette/Autres** (25 km max) sollicitent l’API Google Routes.
