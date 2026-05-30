# POC Avantages sportifs

Pipeline : **Excel + activités simulées → Postgres → Kafka → Spark (Delta sur MinIO) → Power BI**.

Commandes à lancer dans **WSL**. Power BI se configure sur **Windows** (voir [`powerbi/README.md`](powerbi/README.md)).

---

## Prérequis

- Docker Desktop (WSL2)
- Fichiers dans `data/` : `Données+RH.xlsx`, `Données+Sportive.xlsx`
- Copier `.env.example` → `.env` (Google Maps pour Silver, Slack optionnel)

> Toujours cibler une liste de services : `docker compose up -d postgres minio …`  
> Slack = profil `slack` (non démarré par défaut).

---

## Flux de données

```
Excel sport → generator → Postgres → Debezium → Kafka
                              ↓
                    Spark bronze  → MinIO bronze/
Excel RH/sport → Spark ref      → MinIO ref/
                              ↓
                    Spark silver  → MinIO silver/
                              ↓
                    Spark gold    → MinIO gold/ + export Parquet → Power BI
```

---

## Démarrage complet (première fois)

```bash
docker compose down -v   # optionnel : repartir de zéro

# 1. Infra
docker compose up -d --build postgres redpanda debezium console minio
docker compose run --rm minio-init
bash debezium/register-connector.sh

# 2. Activités Postgres (UNE SEULE FOIS — période 2025-01-01 → aujourd'hui)
docker compose run --rm generator

# 3. Datalake Spark
docker compose build spark
docker compose run --rm spark --master 'local[*]' /app/jobs/bronze_kafka.py
docker compose run --rm spark --master 'local[*]' /app/jobs/ref_load.py
docker compose run --rm spark --master 'local[*]' /app/jobs/silver_enrichment.py   # GOOGLE_MAPS_API_KEY dans .env
docker compose run --rm spark --master 'local[*]' /app/jobs/gold_benefits.py
docker compose run --rm spark --master 'local[*]' /app/jobs/export_powerbi_parquet.py

# 4. Power BI → voir powerbi/README.md
```

**Contrôles rapides :** activités en Postgres, topic Kafka dans [Redpanda](http://localhost:8080), bucket `datalake` dans [MinIO](http://localhost:9001).

Qualité Postgres (optionnel) : `docker compose run --rm soda`

---

## Relance au quotidien

Volumes intacts après un redémarrage PC → **ne pas** relancer `generator`.

```bash
docker compose up -d postgres redpanda debezium console minio

docker compose build spark
docker compose run --rm spark --master 'local[*]' /app/jobs/bronze_kafka.py
docker compose run --rm spark --master 'local[*]' /app/jobs/ref_load.py
docker compose run --rm spark --master 'local[*]' /app/jobs/silver_enrichment.py
docker compose run --rm spark --master 'local[*]' /app/jobs/gold_benefits.py
docker compose run --rm spark --master 'local[*]' /app/jobs/export_powerbi_parquet.py
```

Puis **Actualiser** le rapport Power BI.

---

## Règles métier (Gold)

Table `gold/benefits_employee_year` — 1 ligne par **salarié × année**.

| KPI | Règle |
|-----|--------|
| **Prime sportive** (5 %) | Sport déclaré + trajet sportif + pas d'anomalie distance |
| **5 jours bien-être** | ≥ 15 activités physiques sur l'année |

Sources Excel : `Salaire brut` (RH), `Pratique d'un sport` (sport), `Moyen de déplacement` (RH).

Paramètres : `spark/config/rules.yml` (section `gold`).

---

## Config generator

Dates aléatoires entre deux bornes (voir `docker-compose.yml`, surcharge via `.env`) :

| Variable | Défaut |
|----------|--------|
| `GENERATOR_START_DATE` | `2025-01-01` |
| `GENERATOR_END_DATE` | aujourd'hui (UTC) |
| `ACTIVITIES_PER_EMPLOYEE_MIN` / `MAX` | `0` / `50` |

Changer ces valeurs → `TRUNCATE activities;` ou `docker compose down -v`, puis relancer **generator une fois**.

---

## Slack (optionnel)

```bash
# .env : SLACK_WEBHOOK_URL=...
docker compose --profile slack up -d slack
docker compose run --rm slack python demo_insert.py
```

---

## Jobs Spark

| Script | Rôle |
|--------|------|
| `bronze_kafka.py` | Kafka → Delta bronze |
| `ref_load.py` | Excel RH + sport → Delta ref |
| `silver_enrichment.py` | Activités + RH enrichi (Google Routes) |
| `gold_benefits.py` | KPI avantages |
| `export_powerbi_parquet.py` | Export Parquet pour Power BI |


Silver : seuls **Marche/running** et **Vélo/Trottinette/Autres** appellent Google (15 km / 25 km max).

---

## Liens & commandes utiles

| URL | Service |
|-----|---------|
| http://localhost:9001 | MinIO (`minioadmin` / `minioadmin`) |
| http://localhost:8080 | Redpanda |
| http://localhost:8083 | Debezium |

```bash
docker compose down      # arrêt (données conservées)
docker compose down -v   # tout effacer
```

**Debezium ne démarre pas :** attendre l'API puis `bash debezium/register-connector.sh`.

**Power BI :** [`powerbi/README.md`](powerbi/README.md) · [`powerbi/load_benefits.py`](powerbi/load_benefits.py)
