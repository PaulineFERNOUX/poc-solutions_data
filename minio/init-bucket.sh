#!/bin/sh
set -e

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
USER="${MINIO_ROOT_USER:-minioadmin}"
PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
BUCKET="${DATALAKE_BUCKET:-datalake}"

echo "MinIO init: waiting for ${ENDPOINT}..."
until mc alias set local "${ENDPOINT}" "${USER}" "${PASS}" 2>/dev/null; do
  sleep 2
done

echo "Creating bucket: ${BUCKET}"
mc mb --ignore-existing "local/${BUCKET}"

echo "Bucket ready: s3://${BUCKET}"
mc ls "local/"
