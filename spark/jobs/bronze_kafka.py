"""
Bronze : CDC Debezium (Kafka) → Delta sur MinIO.

- Lecture incrementale du topic (checkpoint offsets sur MinIO)
- MERGE Delta : insert/update (c, u, r) et delete (d) sur la cle id
"""
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, desc, from_json, row_number
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from kafka_offsets import (
    load_topic_offsets,
    merge_partition_offsets,
    save_topic_offsets,
    to_spark_starting_offsets,
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

FLAT_SCHEMA = StructType(
    [
        StructField("op", StringType()),
        StructField("after", ACTIVITY_SCHEMA),
        StructField("before", ACTIVITY_SCHEMA),
        StructField("ts_ms", LongType()),
    ]
)


def parse_kafka_batch(raw):
    json_value = col("value").cast("string")
    flat = from_json(json_value, FLAT_SCHEMA).alias("flat")
    wrapped = from_json(json_value, ENVELOPE_SCHEMA).alias("wrapped")

    parsed = raw.select(
        col("partition"),
        col("offset"),
        flat,
        wrapped,
    )

    op = coalesce(col("wrapped.payload.op"), col("flat.op"))
    ts_ms = coalesce(col("wrapped.payload.ts_ms"), col("flat.ts_ms"))
    row = coalesce(
        col("wrapped.payload.after"),
        col("wrapped.payload.before"),
        col("flat.after"),
        col("flat.before"),
    )

    return (
        parsed.select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            op.alias("op"),
            ts_ms.alias("last_event_ts_ms"),
            row.alias("row"),
        )
        .filter(col("row").isNotNull() & col("op").isNotNull())
        .select(
            col("op"),
            col("last_event_ts_ms"),
            col("kafka_partition"),
            col("kafka_offset"),
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


def latest_event_per_id(batch):
    w = Window.partitionBy("id").orderBy(desc("kafka_offset"))
    return (
        batch.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumnRenamed("op", "last_op")
    )


def delta_table_exists(spark, path: str) -> bool:
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    log_path = jpath(f"{path.rstrip('/')}/_delta_log")
    fs = log_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(log_path)


def merge_into_bronze(spark, bronze_path: str, changes) -> None:
    upsert_cols = [
        "id",
        "employee_id",
        "start_date",
        "activity_type",
        "distance_m",
        "end_date",
        "comment",
        "created_at",
        "last_op",
        "last_event_ts_ms",
        "kafka_partition",
        "kafka_offset",
    ]
    source = changes.select(*upsert_cols)

    if not delta_table_exists(spark, bronze_path):
        initial = source.filter(col("last_op") != "d")
        if initial.isEmpty():
            raise ValueError("Premier chargement bronze : aucun evenement upsert")
        initial.write.format("delta").mode("overwrite").save(bronze_path)
        return

    source.createOrReplaceTempView("cdc_changes")
    spark.sql(
        f"""
        MERGE INTO delta.`{bronze_path}` AS t
        USING cdc_changes AS s
        ON t.id = s.id
        WHEN MATCHED AND s.last_op = 'd' THEN DELETE
        WHEN MATCHED AND s.last_op <> 'd' THEN UPDATE SET *
        WHEN NOT MATCHED AND s.last_op <> 'd' THEN INSERT *
        """
    )


def main() -> None:
    from pyspark.sql.functions import max as spark_max

    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "dbz.public.activities")
    bronze_path = f"s3a://{bucket}/bronze/activities"
    checkpoint_uri = os.environ.get(
        "BRONZE_KAFKA_CHECKPOINT",
        f"s3a://{bucket}/_checkpoints/bronze_kafka_offsets.json",
    )
    reset = os.environ.get("BRONZE_KAFKA_RESET", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    spark = SparkSession.builder.appName("bronze-kafka-cdc-delta").getOrCreate()

    if reset:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        jpath = spark._jvm.org.apache.hadoop.fs.Path
        for uri in (checkpoint_uri, bronze_path):
            path = jpath(uri)
            fs = path.getFileSystem(hadoop_conf)
            if fs.exists(path):
                fs.delete(path, True)
        print(f"Checkpoint et bronze reinitialises")

    saved = None if reset else load_topic_offsets(spark, checkpoint_uri)

    reader = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("endingOffsets", "latest")
    )
    if saved:
        reader = reader.option(
            "startingOffsets",
            to_spark_starting_offsets(kafka_topic, saved),
        )
    else:
        reader = reader.option("startingOffsets", "earliest")

    raw = reader.load()

    if raw.isEmpty():
        print("Aucun nouveau message Kafka")
        spark.stop()
        sys.exit(0)

    batch = parse_kafka_batch(raw).cache()
    n_events = batch.count()
    if n_events == 0:
        batch.unpersist()
        print("Aucun evenement Debezium parse")
        spark.stop()
        sys.exit(0)

    partition_max = {
        str(row["kafka_partition"]): int(row["max_offset"])
        for row in batch.groupBy("kafka_partition")
        .agg(spark_max("kafka_offset").alias("max_offset"))
        .collect()
    }

    changes = latest_event_per_id(batch).cache()
    n_ids = changes.count()
    by_op = {
        r["last_op"]: r["count"] for r in changes.groupBy("last_op").count().collect()
    }

    merge_into_bronze(spark, bronze_path, changes)

    new_checkpoint = merge_partition_offsets(saved, partition_max)
    save_topic_offsets(spark, checkpoint_uri, new_checkpoint)

    total = spark.read.format("delta").load(bronze_path).count()
    batch.unpersist()
    changes.unpersist()
    spark.stop()

    print(f"Evenements Kafka lus     : {n_events}")
    print(f"Ids touches (dernier evt): {n_ids}")
    print(f"Repartition par op       : {by_op}")
    print(f"Checkpoint offsets     : {json.dumps(new_checkpoint)}")
    print(f"Delta bronze           : {bronze_path} ({total} lignes actives)")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
