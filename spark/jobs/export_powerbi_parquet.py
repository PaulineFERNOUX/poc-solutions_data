"""
Exporte la table Gold Delta vers un dossier Parquet pour Power BI.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    gold_path = f"s3a://{bucket}/gold/benefits_employee_year"
    out_path = f"s3a://{bucket}/powerbi/benefits_employee_year_parquet"

    spark = SparkSession.builder.appName("export-powerbi-parquet").getOrCreate()

    df = spark.read.format("delta").load(gold_path)
    count = df.count()
    if count == 0:
        spark.stop()
        raise ValueError("gold/benefits_employee_year est vide")

    (
        df.repartition(1)
        .sortWithinPartitions(col("year").asc(), col("employee_id").asc())
        .write.mode("overwrite")
        .parquet(out_path)
    )

    spark.stop()
    print(f"Export OK: {count} lignes")
    print(f"Source Delta : {gold_path}")
    print(f"Sortie Parquet : {out_path}")


if __name__ == "__main__":
    main()
