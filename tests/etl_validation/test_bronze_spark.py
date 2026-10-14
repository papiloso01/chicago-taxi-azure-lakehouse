"""Exercise actual Spark plans without needing Databricks credentials or Delta jars."""
import hashlib
import json
import pytest
from pyspark.sql import SparkSession
from chicago_taxi.transformations.bronze.taxi_trips import prepare, stage_schema


@pytest.fixture(scope='module')
def spark():
    session = SparkSession.builder.master('local[2]').appName('bronze-validation').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    yield session
    session.stop()


def record(key='a', fare='12.50', file='page1'):
    raw = json.dumps(dict(trip_id=key, fare=fare, mystery={'nested': 42}))
    return (key, raw, hashlib.sha256(raw.encode()).hexdigest(), 'a'*32, file, '2025-01-02T00:00:00+00:00')


def test_identical_duplicates_collapse_and_retain_raw(spark):
    result = prepare(spark.createDataFrame([record(file='page2'), record()], stage_schema())).collect()
    assert len(result) == 1
    row = result[0]
    assert row.fare == '12.50' and row.source_file == 'page1'
    assert row.taxi_id is None
    assert json.loads(row.raw_payload)['mystery'] == {'nested': 42}
    assert row.extracted_at_utc is not None and row.bronze_loaded_at_utc is not None


def test_conflicting_duplicates_fail_before_merge(spark):
    with pytest.raises(ValueError, match='Conflicting'):
        prepare(spark.createDataFrame([record(), record(fare='99')], stage_schema()))


def test_missing_key_fails(spark):
    with pytest.raises(ValueError, match='Missing'):
        prepare(spark.createDataFrame([record(key=' ')], stage_schema()))


def test_empty_run_has_explicit_schema(spark):
    result = prepare(spark.createDataFrame([], stage_schema()))
    assert result.count() == 0
    assert dict(result.dtypes)['fare'] == 'string'
    assert dict(result.dtypes)['extracted_at_utc'] == 'timestamp'
