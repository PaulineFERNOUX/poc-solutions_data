"""Checkpoint offsets Kafka (JSON sur MinIO/S3) pour lectures incrementales."""
from __future__ import annotations

import json
from typing import Dict, Optional


def _hadoop_path(spark, uri: str):
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.Path(uri)


def _hadoop_fs(spark, uri: str):
    path = _hadoop_path(spark, uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, path


def _read_text_file(spark, uri: str) -> str:
    fs, path = _hadoop_fs(spark, uri)
    stream = fs.open(path)
    try:
        buf = spark._jvm.org.apache.hadoop.io.IOUtils.readFullyToByteArray(stream)
        return bytes(bytearray(buf)).decode("utf-8")
    finally:
        stream.close()


def load_topic_offsets(spark, uri: str) -> Optional[Dict[str, int]]:
    """Retourne {partition: next_offset} ou None si checkpoint absent."""
    fs, path = _hadoop_fs(spark, uri)
    if not fs.exists(path):
        return None
    raw = _read_text_file(spark, uri).strip()
    if not raw:
        return None
    data = json.loads(raw)
    return {str(k): int(v) for k, v in data.items()}


def save_topic_offsets(spark, uri: str, offsets: Dict[str, int]) -> None:
    fs, path = _hadoop_fs(spark, uri)
    parent = path.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    payload = json.dumps({str(k): int(v) for k, v in sorted(offsets.items())})
    out = fs.create(path, True)
    try:
        out.write(bytearray(payload.encode("utf-8")))
    finally:
        out.close()


def to_spark_starting_offsets(topic: str, partition_offsets: Dict[str, int]) -> str:
    """Format attendu par spark.read.format('kafka').option('startingOffsets', ...)."""
    inner = ",".join(f'"{p}":{o}' for p, o in sorted(partition_offsets.items()))
    return "{" + f'"{topic}":{{{inner}}}' + "}"


def merge_partition_offsets(
    previous: Optional[Dict[str, int]],
    batch_max: Dict[str, int],
) -> Dict[str, int]:
    """batch_max = partition -> max offset lu (inclus); checkpoint = max + 1."""
    merged = dict(previous or {})
    for part, max_off in batch_max.items():
        merged[str(part)] = max(int(max_off), int(merged.get(str(part), 0))) + 1
    return merged
