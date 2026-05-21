#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

docker compose exec -T debezium curl -s -X POST \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  --data "$(cat debezium/register-connector.json)"

echo ""
echo "Connecteur enregistre. Verification :"
docker compose exec debezium curl -s http://localhost:8083/connectors/postgres-connector/status
