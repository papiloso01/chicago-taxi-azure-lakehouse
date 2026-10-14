"""Spark Connect compatible bronze preparation and SQL Delta merges."""
import re
import shutil
import uuid
from chicago_taxi.transformations.bronze.landing_manifest import stage_run

SOURCE_FIELDS = ["taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
    "trip_miles", "pickup_community_area", "dropoff_community_area", "fare", "tips",
    "tolls", "extras", "trip_total", "payment_type", "company"]


def table_name(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError("Expected catalog.schema.table identifier")
    return value


def stage_schema():
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([StructField(name, StringType(), False) for name in
        ["trip_id", "raw_payload", "payload_sha256", "landing_run_id", "source_file", "extracted_at_utc"]])


def prepare(df):
    from pyspark.sql import functions as F
    if df.filter(F.col("trip_id").isNull() | (F.trim("trip_id") == "")).limit(1).count():
        raise ValueError("Missing trip key")
    if df.groupBy("trip_id").agg(F.countDistinct("payload_sha256").alias("versions")).filter("versions > 1").limit(1).count():
        raise ValueError("Conflicting payloads for one trip in a landing run")
    # Identical source duplicates choose a stable provenance file.
    from pyspark.sql.window import Window
    df = df.withColumn("_rank", F.row_number().over(Window.partitionBy("trip_id").orderBy("source_file"))).filter("_rank = 1").drop("_rank")
    for name in SOURCE_FIELDS:
        df = df.withColumn(name, F.get_json_object("raw_payload", "$." + name))
    return df.withColumn("extracted_at_utc", F.to_timestamp("extracted_at_utc")).withColumn("bronze_loaded_at_utc", F.current_timestamp())


def merge_sql(target, view):
    target = table_name(target)
    if not re.fullmatch(r"bronze_stage_[0-9a-f]{32}", view):
        raise ValueError("Invalid staging view")
    return f"""MERGE INTO {target} t USING {view} s ON t.trip_id = s.trip_id
    WHEN MATCHED AND (s.extracted_at_utc > t.extracted_at_utc OR
      (s.extracted_at_utc = t.extracted_at_utc AND s.landing_run_id > t.landing_run_id))
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *"""


def load_run(spark, run_path, target="chicago_taxi_dev.bronze.taxi_trips",
             audit_table="chicago_taxi_dev.bronze.landing_runs"):
    target, audit_table = table_name(target), table_name(audit_table)
    manifest, stage = stage_run(run_path)
    view = "bronze_stage_" + uuid.uuid4().hex
    try:
        if manifest["extracted_rows"]:
            source = spark.read.schema(stage_schema()).json(str(stage))
        else:
            source = spark.createDataFrame([], stage_schema())
        if source.count() != manifest["extracted_rows"]:
            raise ValueError("Staged row count differs from manifest")
        source = prepare(source)
        distinct_rows = source.count()
        columns = ", ".join(f"{name} STRING" for name in
            ["trip_id", "raw_payload", "payload_sha256", "landing_run_id", "source_file"] + SOURCE_FIELDS)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({columns}, extracted_at_utc TIMESTAMP, bronze_loaded_at_utc TIMESTAMP) USING DELTA")
        source.createOrReplaceTempView(view)
        spark.sql(merge_sql(target, view))
        # Separate transaction: if audit write fails, replay repairs it after the idempotent trip merge.
        spark.sql(f"CREATE TABLE IF NOT EXISTS {audit_table} (landing_run_id STRING, extracted_rows BIGINT, distinct_rows BIGINT, loaded_at_utc TIMESTAMP) USING DELTA")
        audit = spark.createDataFrame([(manifest["run_id"], manifest["extracted_rows"], distinct_rows)],
                                     "landing_run_id string, extracted_rows long, distinct_rows long")
        audit.createOrReplaceTempView(view + "_audit")
        spark.sql(f"""MERGE INTO {audit_table} t USING {view}_audit s ON t.landing_run_id = s.landing_run_id
        WHEN MATCHED THEN UPDATE SET extracted_rows=s.extracted_rows, distinct_rows=s.distinct_rows, loaded_at_utc=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (landing_run_id, extracted_rows, distinct_rows, loaded_at_utc)
        VALUES (s.landing_run_id, s.extracted_rows, s.distinct_rows, current_timestamp())""")
        return dict(run_id=manifest["run_id"], status="success", extracted_rows=manifest["extracted_rows"], distinct_rows=distinct_rows, target=target)
    finally:
        spark.catalog.dropTempView(view)
        spark.catalog.dropTempView(view + "_audit")
        shutil.rmtree(stage)
