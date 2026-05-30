"""
Phase 3 : charge les référentiels RH et sportif (Excel) en tables Delta sur MinIO.
"""
import glob
import os
import re
import sys

import pandas as pd
from pyspark.sql import SparkSession

from commute_modes import clean_commute_mode

COL_ID = "ID salarié"
COL_MODE_RAW = "Moyen_de_déplacement"


def sanitize_column(name: str) -> str:
    """Delta interdit espaces et certains caractères dans les noms de colonnes."""
    s = str(name).strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[,;{}()\n\t=]", "", s)
    s = re.sub(r"['\"]", "", s)
    return s or "col"


def resolve_path(env_key: str, glob_pattern: str) -> str:
    path = os.environ.get(env_key, "").strip()
    if path and os.path.isfile(path):
        return path
    candidates = glob.glob(f"/app/data/{glob_pattern}") or glob.glob(
        f"data/{glob_pattern}"
    )
    if not candidates:
        raise FileNotFoundError(
            f"Fichier introuvable ({env_key} ou data/{glob_pattern}). "
            "Placez les Excel dans data/."
        )
    return candidates[0]


def excel_to_delta(spark, excel_path: str, delta_path: str, *, is_rh: bool = False) -> int:
    pdf = pd.read_excel(excel_path)
    if pdf.empty:
        raise ValueError(f"Fichier vide : {excel_path}")

    if COL_ID in pdf.columns:
        pdf = pdf.rename(columns={COL_ID: "employee_id"})
    elif "employee_id" not in pdf.columns:
        raise ValueError(
            f"Colonne {COL_ID!r} absente dans {excel_path} "
            f"(colonnes : {list(pdf.columns)})"
        )

    pdf = pdf.rename(columns={c: sanitize_column(c) for c in pdf.columns})

    pdf["employee_id"] = pd.to_numeric(pdf["employee_id"], errors="raise").astype(
        "int64"
    )

    if is_rh and COL_MODE_RAW in pdf.columns:
        pdf["commute_mode"] = pdf[COL_MODE_RAW].map(clean_commute_mode)

    df = spark.createDataFrame(pdf)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        delta_path
    )
    return df.count()


def main() -> None:
    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    rh_delta = f"s3a://{bucket}/ref/rh"
    sport_delta = f"s3a://{bucket}/ref/sport"

    spark = SparkSession.builder.appName("ref-load").getOrCreate()

    rh_file = resolve_path("RH_FILE", "*RH*.xlsx")
    sport_file = resolve_path("SPORT_FILE", "*Sportive*.xlsx")

    rh_count = excel_to_delta(spark, rh_file, rh_delta, is_rh=True)
    sport_count = excel_to_delta(spark, sport_file, sport_delta)

    spark.stop()

    print(f"Source RH      : {rh_file}")
    print(f"Source sport   : {sport_file}")
    print(f"OK: ref/rh      -> {rh_delta} ({rh_count} lignes)")
    print(f"OK: ref/sport   -> {sport_delta} ({sport_count} lignes)")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
