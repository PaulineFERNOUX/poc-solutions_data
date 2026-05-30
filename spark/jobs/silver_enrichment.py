"""
Silver : activites (etat courant bronze CDC) + RH enrichi pour la couche Gold.
"""
import os
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from commute_modes import OFFICE, clean_commute_mode, google_route
from routes_client import distance_km

COL_MODE_RAW = "Moyen_de_déplacement"
COL_MODE = "commute_mode"
COL_HOME = "Adresse_du_domicile"


def enrich_rh_commutes(rh_pdf: pd.DataFrame, api_key: str) -> pd.DataFrame:
    rows = []
    for _, e in rh_pdf.iterrows():
        raw = str(e.get(COL_MODE_RAW, e.get(COL_MODE, ""))).strip()
        mode = e.get(COL_MODE) or clean_commute_mode(raw)
        home = str(e.get(COL_HOME, "")).strip()
        gmode, max_km = google_route(mode)

        dist_km = None
        anomaly = None
        if gmode and home:
            dist_km = distance_km(home, OFFICE, gmode, api_key)
            anomaly = dist_km > max_km

        row = e.to_dict()
        row.update(
            {
                "commute_mode": mode,
                "google_travel_mode": gmode,
                "distance_km": dist_km,
                "max_allowed_km": max_km,
                "anomaly_commute": anomaly if anomaly is not None else False,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        print("FAIL: GOOGLE_MAPS_API_KEY manquante dans .env", file=sys.stderr)
        sys.exit(1)

    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    spark = SparkSession.builder.appName("silver").getOrCreate()

    bronze = spark.read.format("delta").load(f"s3a://{bucket}/bronze/activities")
    rh = spark.read.format("delta").load(f"s3a://{bucket}/ref/rh")

    # Bronze = etat courant (MERGE CDC Debezium c/u/r/d)
    silver_acts = bronze.select(
        "id",
        "employee_id",
        "start_date",
        "activity_type",
        "distance_m",
        "end_date",
        "comment",
        "created_at",
    )

    rh_pdf = rh.toPandas()
    if COL_MODE not in rh_pdf.columns:
        rh_pdf[COL_MODE] = rh_pdf[COL_MODE_RAW].map(clean_commute_mode)

    test_id = os.environ.get("SILVER_ONLY_EMPLOYEE_ID", "").strip()
    if test_id:
        rh_pdf = rh_pdf[rh_pdf["employee_id"] == int(test_id)]
        silver_acts = silver_acts.filter(col("employee_id") == int(test_id))
        if rh_pdf.empty:
            print(
                f"FAIL: employee_id={test_id} introuvable dans ref/rh",
                file=sys.stderr,
            )
            spark.stop()
            sys.exit(1)

    rh_enriched_pdf = enrich_rh_commutes(rh_pdf, api_key)
    rh_enriched_df = spark.createDataFrame(rh_enriched_pdf)

    opts = {"overwriteSchema": "true"}
    silver_acts.write.format("delta").mode("overwrite").options(**opts).save(
        f"s3a://{bucket}/silver/activities"
    )
    rh_enriched_df.write.format("delta").mode("overwrite").options(**opts).save(
        f"s3a://{bucket}/silver/rh_enriched"
    )

    n_act = silver_acts.count()
    n_rh = len(rh_enriched_pdf)
    n_anom = int(rh_enriched_pdf["anomaly_commute"].sum())
    spark.stop()

    print(f"silver/activities         : {n_act} lignes")
    print(f"silver/rh_enriched        : {n_rh} lignes")
    print(f"anomalies trajet          : {n_anom}")


if __name__ == "__main__":
    main()
