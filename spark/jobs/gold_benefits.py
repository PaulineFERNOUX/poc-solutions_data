"""
Gold minimal pour Power BI : KPI avantages par salarie et par annee.
"""
import os
from typing import Dict, List, Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_date,
    lit,
    regexp_replace,
    to_timestamp,
    trim,
    when,
    year,
)

def pick_first_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def load_gold_rules() -> Dict:
    config_path = os.environ.get("RULES_PATH", "/app/config/rules.yml")
    if not os.path.isfile(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    gold = data.get("gold")
    return gold if isinstance(gold, dict) else {}


def main() -> None:
    rules = load_gold_rules()
    sportive_modes = rules.get(
        "sportive_commute_modes",
        ["Marche/running", "Vélo/Trottinette/Autres", "Velo/Trottinette/Autres"],
    )
    prime_rate = float(rules.get("prime_rate", 0.05))
    min_activities_for_wellbeing = int(rules.get("min_activities_for_wellbeing", 15))
    wellbeing_days = int(rules.get("wellbeing_days", 5))
    exclude_commute_anomaly = bool(rules.get("exclude_commute_anomaly", True))

    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    spark = SparkSession.builder.appName("gold-benefits").getOrCreate()

    activities = spark.read.format("delta").load(f"s3a://{bucket}/silver/activities")
    rh_enriched = spark.read.format("delta").load(f"s3a://{bucket}/silver/rh_enriched")
    sport_ref = spark.read.format("delta").load(f"s3a://{bucket}/ref/sport")

    activities_by_year = (
        activities.withColumn("activity_start_ts", to_timestamp(col("start_date")))
        .withColumn("activity_year", year(col("activity_start_ts")))
        .filter(col("activity_year").isNotNull())
        .groupBy("employee_id", "activity_year")
        .agg(count("*").alias("physical_activities_count_year"))
    )

    years = activities_by_year.select("activity_year").distinct()
    if years.rdd.isEmpty():
        years = spark.range(1).select(year(current_date()).alias("activity_year"))

    base = rh_enriched.select(
        "employee_id", *[c for c in rh_enriched.columns if c != "employee_id"]
    ).crossJoin(years)

    salary_col = pick_first_column(
        rh_enriched.columns,
        ["Salaire_brut"],
    )
    if salary_col:
        cleaned_salary = regexp_replace(
            regexp_replace(col(salary_col).cast("string"), ",", "."),
            r"[^0-9.\-]",
            "",
        )
        base = base.withColumn("annual_gross_salary", cleaned_salary.cast("double"))
    else:
        base = base.withColumn("annual_gross_salary", lit(None).cast("double"))

    sport_col = pick_first_column(
        sport_ref.columns,
        ["Pratique_dun_sport", "Pratique_du_sport", "sport", "Sport"],
    )
    if sport_col:
        sport_flag = sport_ref.select(
            "employee_id",
            (trim(col(sport_col).cast("string")) != "").alias("sport_practice_declared"),
        ).dropDuplicates(["employee_id"])
    else:
        sport_flag = sport_ref.select("employee_id").withColumn("sport_practice_declared", lit(False))

    gold = (
        base.join(sport_flag, ["employee_id"], "left")
        .join(activities_by_year, ["employee_id", "activity_year"], "left")
        .withColumn("year", col("activity_year"))
        .drop("activity_year")
        .withColumn("sport_practice_declared", coalesce(col("sport_practice_declared"), lit(False)))
        .withColumn(
            "physical_activities_count_year",
            coalesce(col("physical_activities_count_year"), lit(0)),
        )
        .withColumn("commute_is_sportive", col("commute_mode").isin(sportive_modes))
        .withColumn("commute_anomaly_flag", coalesce(col("anomaly_commute"), lit(False)))
        .withColumn(
            "eligible_prime_sportive",
            col("sport_practice_declared")
            & col("commute_is_sportive")
            & (lit(not exclude_commute_anomaly) | (~col("commute_anomaly_flag"))),
        )
        .withColumn("prime_rate", lit(prime_rate))
        .withColumn(
            "prime_amount",
            when(
                col("eligible_prime_sportive") & col("annual_gross_salary").isNotNull(),
                col("annual_gross_salary") * lit(prime_rate),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "eligible_wellbeing_days",
            col("physical_activities_count_year") >= lit(min_activities_for_wellbeing),
        )
        .withColumn(
            "wellbeing_days_granted",
            when(col("eligible_wellbeing_days"), lit(wellbeing_days)).otherwise(lit(0)),
        )
        .withColumn(
            "eligibility_reason",
            when(~col("sport_practice_declared"), lit("no_sport_declared"))
            .when(~col("commute_is_sportive"), lit("commute_not_sportive"))
            .when(
                lit(exclude_commute_anomaly) & col("commute_anomaly_flag"),
                lit("commute_anomaly"),
            )
            .otherwise(lit("eligible_prime")),
        )
        .withColumn("load_date", current_date())
    )

    out = gold.select(
        "employee_id",
        "year",
        "annual_gross_salary",
        "commute_mode",
        "google_travel_mode",
        "distance_km",
        "max_allowed_km",
        "anomaly_commute",
        "sport_practice_declared",
        "physical_activities_count_year",
        "eligible_prime_sportive",
        "prime_rate",
        "prime_amount",
        "eligible_wellbeing_days",
        "wellbeing_days_granted",
        "eligibility_reason",
        "load_date",
    )

    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"s3a://{bucket}/gold/benefits_employee_year"
    )

    total = out.count()
    eligible_prime = out.filter(col("eligible_prime_sportive")).count()
    eligible_wellbeing = out.filter(col("eligible_wellbeing_days")).count()
    spark.stop()

    print(f"gold/benefits_employee_year : {total} lignes")
    print(f"eligibles prime sportive    : {eligible_prime}")
    print(f"eligibles 5 jours bien-etre : {eligible_wellbeing}")


if __name__ == "__main__":
    main()
