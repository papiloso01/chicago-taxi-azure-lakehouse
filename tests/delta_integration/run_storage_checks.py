"""Named local Delta checks, dispatched inside an isolated subprocess JVM."""

import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path
from pyspark.sql import SparkSession
from chicago_taxi.ingestion.landing import ingest
from chicago_taxi.transformations.bronze.taxi_trips import (
    load_run,
    prepare,
    stage_schema,
    merge_sql,
)
from chicago_taxi.transformations.silver.taxi_trips import build_tables
from chicago_taxi.transformations.gold.taxi_marts import build_marts
from tests.support.data import build_silver_input

TARGET = "spark_catalog.default.merge_trips"
VIEW = "bronze_stage_" + "a" * 32


def merge_batch(spark, fixtures, fare="10", timestamp="2025-01-02T00:00:00+00:00", run_id="a" * 32):
    example = json.loads((fixtures / "etl/bronze_record.json").read_text())
    raw = json.dumps(dict(example, fare=fare))
    row = ("a", raw, hashlib.sha256(raw.encode()).hexdigest(), run_id, "fixture.json", timestamp)
    return prepare(spark.createDataFrame([row], stage_schema()))


def initialize_merge_table(spark, fixtures):
    merge_batch(spark, fixtures).write.format("delta").saveAsTable(TARGET)


def apply_merge(spark, frame):
    frame.createOrReplaceTempView(VIEW)
    spark.sql(merge_sql(TARGET, VIEW))


def merge_replay(spark, folder, fixtures):
    initialize_merge_table(spark, fixtures)
    before = spark.table(TARGET).collect()
    apply_merge(spark, merge_batch(spark, fixtures))
    assert spark.table(TARGET).collect() == before, "Replay changed stored records"


def merge_newer(spark, folder, fixtures):
    initialize_merge_table(spark, fixtures)
    apply_merge(spark, merge_batch(spark, fixtures, "20", "2025-01-03T00:00:00+00:00"))
    assert spark.table(TARGET).count() == 1
    assert spark.table(TARGET).first().fare == "20"


def merge_stale(spark, folder, fixtures):
    initialize_merge_table(spark, fixtures)
    apply_merge(spark, merge_batch(spark, fixtures, "99", "2025-01-01T00:00:00+00:00", "f" * 32))
    assert spark.table(TARGET).count() == 1
    assert spark.table(TARGET).first().fare == "10", "Stale extraction replaced newer data"


def merge_tie_break(spark, folder, fixtures):
    initialize_merge_table(spark, fixtures)
    apply_merge(spark, merge_batch(spark, fixtures, fare="30", run_id="c" * 32))
    assert spark.table(TARGET).first().fare == "30"
    apply_merge(spark, merge_batch(spark, fixtures, fare="98", run_id="b" * 32))
    assert spark.table(TARGET).count() == 1
    assert spark.table(TARGET).first().fare == "30", "Lower run ID replaced equal-time publication"


def historical_read(spark, folder, fixtures):
    initialize_merge_table(spark, fixtures)
    version = spark.sql("DESCRIBE HISTORY " + TARGET).first().version
    apply_merge(spark, merge_batch(spark, fixtures, "20", "2025-01-03T00:00:00+00:00"))
    assert spark.table(TARGET).first().fare == "20"
    assert spark.sql(f"SELECT * FROM {TARGET} VERSION AS OF {version}").first().fare == "10"


def silver_tables(spark, fixtures):
    example = json.loads((fixtures / "etl/silver_source.json").read_text())
    return build_tables(build_silver_input(spark, example))


def assert_overwrite_replay(spark, frame, path):
    frame.write.format("delta").mode("overwrite").save(str(path))
    stored = spark.read.format("delta").load(str(path))
    before, schema = stored.collect(), stored.schema
    frame.write.format("delta").mode("overwrite").save(str(path))
    after = spark.read.format("delta").load(str(path))
    assert after.collect() == before
    assert after.schema == schema
    return before


def silver_overwrite(spark, folder, fixtures):
    rows = assert_overwrite_replay(
        spark, silver_tables(spark, fixtures)["fact_trip"], folder / "silver_fact"
    )
    assert len(rows) == 1 and rows[0].trip_total == Decimal("14.75")


def gold_overwrite(spark, folder, fixtures):
    daily = build_marts(silver_tables(spark, fixtures), "delta-gold")["mart_daily_summary"]
    rows = assert_overwrite_replay(spark, daily, folder / "gold_daily")
    assert len(rows) == 1 and rows[0].trip_count == 1
    assert rows[0].known_revenue_usd == Decimal("14.75")


def landing_replay(spark, folder, fixtures):
    rows = json.loads((fixtures / "etl/e2e_trips.json").read_text())[:1]
    run = ingest(folder, "2025-01-01", "2025-01-02", run_id="d" * 32, fetch=lambda *args: rows)
    landing = folder / "taxi_trips" / ("run_id=" + run["run_id"])
    target, audit = "spark_catalog.default.loaded_trips", "spark_catalog.default.loaded_runs"
    first = load_run(spark, landing, target=target, audit_table=audit)
    before = spark.table(target).collect()
    second = load_run(spark, landing, target=target, audit_table=audit)
    assert first["distinct_rows"] == second["distinct_rows"] == 1
    assert spark.table(target).collect() == before
    assert spark.table(audit).count() == 1
    saved = spark.table(audit).first()
    assert saved.landing_run_id == run["run_id"]
    assert saved.extracted_rows == saved.distinct_rows == 1


CHECKS = {
    check.__name__: check
    for check in (
        merge_replay,
        merge_newer,
        merge_stale,
        merge_tie_break,
        historical_read,
        silver_overwrite,
        gold_overwrite,
        landing_replay,
    )
}


if __name__ == "__main__":
    case, folder, jars, fixtures = sys.argv[1:]
    folder, jars, fixtures = Path(folder), Path(jars), Path(fixtures)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("delta-storage-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", str(folder / "warehouse"))
        .config("spark.jars", ",".join(str(p) for p in sorted(jars.glob("*.jar"))))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .getOrCreate()
    )
    try:
        CHECKS[case](spark, folder, fixtures)
        print("PASSED " + case)
    finally:
        spark.stop()
