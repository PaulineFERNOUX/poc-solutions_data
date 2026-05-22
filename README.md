# POC Avantages sportifs

Excel → PostgreSQL → Debezium → Redpanda → Slack.

**Prérequis :** Docker (WSL), fichiers Excel dans `data/`.

> Ne pas faire `docker compose up -d` seul (lance generator + slack → milliers de notifs).

---

## 1. Installation (sans Slack)

```bash
docker compose up -d --build postgres redpanda debezium console
bash debezium/register-connector.sh

docker compose run --rm generator
```

Optionnel — qualité des données :

```bash
docker compose run --rm soda
```

---

## 2. Démo Slack (3 messages)

```bash
cp .env.example .env
# Renseigner SLACK_WEBHOOK_URL dans .env

docker compose up -d slack
docker compose run --rm slack python demo_insert.py
```

---

## Si trop de notifs au lancement de slack

```bash
docker compose stop slack
docker compose exec redpanda rpk group delete slack-poc
docker compose up -d slack
docker compose run --rm slack python demo_insert.py
```

---

## Utile

| URL | Rôle |
|-----|------|
| http://localhost:8080 | Redpanda Console |
| localhost:5432 | Postgres `sportsdb` / `appuser` / `apppass` |

```bash
docker exec -it pg_activities psql -U appuser -d sportsdb -c "SELECT COUNT(*) FROM activities;"
docker compose down      # arrêt (garde les données)
docker compose down -v   # tout effacer
```
