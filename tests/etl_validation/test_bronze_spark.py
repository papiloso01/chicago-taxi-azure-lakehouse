"""Exercise actual Spark plans without needing Databricks credentials or Delta jars."""

import json
import pytest
from chicago_taxi.transformations.bronze.taxi_trips import prepare, stage_schema


def test_identical_duplicates_collapse_and_retain_raw(spark, bronze_record):
    result = prepare(
        spark.createDataFrame([bronze_record(file="page2"), bronze_record()], stage_schema())
    ).collect()
    assert len(result) == 1
    row = result[0]
    assert row.fare == "12.50" and row.source_file == "page1"
    assert row.taxi_id is None
    assert json.loads(row.raw_payload)["mystery"] == {"nested": 42}
    assert row.extracted_at_utc is not None and row.bronze_loaded_at_utc is not None


def test_conflicting_duplicates_fail_before_merge(spark, bronze_record):
    with pytest.raises(ValueError, match="Conflicting"):
        prepare(spark.createDataFrame([bronze_record(), bronze_record(fare="99")], stage_schema()))


def test_missing_key_fails(spark, bronze_record):
    with pytest.raises(ValueError, match="Missing"):
        prepare(spark.createDataFrame([bronze_record(key=" ")], stage_schema()))


def test_empty_run_has_explicit_schema(spark):
    result = prepare(spark.createDataFrame([], stage_schema()))
    assert result.count() == 0
    assert dict(result.dtypes)["fare"] == "string"
    assert dict(result.dtypes)["extracted_at_utc"] == "timestamp"
