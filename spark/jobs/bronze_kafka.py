"""
Bronze : lit le topic Debezium (dbz.public.activities) et écrit en Delta sur MinIO.
Mode batch (lecture complète du topic) — adapté au POC et au rejeu initial.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

ACTIVITY_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("employee_id", LongType()),
        StructField("start_date", StringType()),
        StructField("activity_type", StringType()),
        StructField("distance_m", LongType()),
        StructField("end_date", StringType()),
        StructField("comment", StringType()),
        StructField("created_at", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField("op", StringType()),
                    StructField("after", ACTIVITY_SCHEMA),
                    StructField("before", ACTIVITY_SCHEMA),
                    StructField("ts_ms", LongType()),
                ]
            ),
        )
    ]
)


def main() -> None:
    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "dbz.public.activities")
    bronze_path = f"s3a://{bucket}/bronze/activities"

    spark = SparkSession.builder.appName("bronze-kafka-to-delta").getOrCreate()

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    if raw.isEmpty():
        print("WARN: aucun message Kafka sur le topic", kafka_topic, file=sys.stderr)
        spark.stop()
        sys.exit(0)

    parsed = (
        raw.select(
            col("topic"),
            col("partition"),
            col("offset"),
            from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("env"),
        )
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("env.payload.op").alias("op"),
            col("env.payload.ts_ms").alias("event_ts_ms"),
            coalesce(col("env.payload.after"), col("env.payload.before")).alias("row"),
        )
        .filter(col("row").isNotNull())
        .select(
            col("op"),
            col("event_ts_ms"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("row.id").alias("id"),
            col("row.employee_id").alias("employee_id"),
            col("row.start_date").alias("start_date"),
            col("row.activity_type").alias("activity_type"),
            col("row.distance_m").alias("distance_m"),
            col("row.end_date").alias("end_date"),
            col("row.comment").alias("comment"),
            col("row.created_at").alias("created_at"),
        )
    )

    count = parsed.count()
    parsed.write.format("delta").mode("overwrite").save(bronze_path)

    read_back = spark.read.format("delta").load(bronze_path)
    delta_count = read_back.count()
    spark.stop()

    print(f"Kafka messages traites : {count}")
    print(f"Delta bronze : {bronze_path} ({delta_count} lignes)")

    if delta_count == 0:
        print("FAIL: table bronze vide", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
