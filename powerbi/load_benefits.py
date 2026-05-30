"""
Charge la table Gold exportée (Parquet) depuis MinIO via l'API S3 (boto3).

Usage Power BI Desktop :
  Obtenir des données → Autre → Script Python → coller ce fichier (ou son contenu).
  Cocher la table ``df`` → Charger.

Prérequis : MinIO démarré + jobs gold_benefits.py et export_powerbi_parquet.py exécutés.
"""
import io
import os

import boto3
import pandas as pd

endpoint = os.environ.get("MINIO_ENDPOINT_POWERBI", "http://localhost:9000")
bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
prefix = os.environ.get(
    "POWERBI_PARQUET_PREFIX", "powerbi/benefits_employee_year_parquet/"
)
access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
)

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
keys = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

if not keys:
    raise FileNotFoundError(f"Aucun fichier Parquet sous s3://{bucket}/{prefix}")

obj = s3.get_object(Bucket=bucket, Key=keys[0])
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

df
