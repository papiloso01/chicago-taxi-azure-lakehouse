"""Subprocess-only runner: tests production Delta SQL, never mocks writes."""
import json
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from chicago_taxi.ingestion.landing import ingest
from chicago_taxi.transformations.bronze.taxi_trips import load_run, prepare, stage_schema, merge_sql
from tests.support.data import source


def merge_ordering(spark, folder, fixtures):
    target = 'spark_catalog.default.merge_trips'
    view = 'bronze_stage_' + 'a'*32
    raw_example = json.loads((fixtures / 'etl/bronze_record.json').read_text())
    def batch(fare, timestamp, run_id):
        raw = json.dumps(dict(raw_example, fare=fare))
        import hashlib
        row = ('a', raw, hashlib.sha256(raw.encode()).hexdigest(), run_id, 'fixture.json', timestamp)
        return prepare(spark.createDataFrame([row], stage_schema()))
    initial = batch('10', '2025-01-02T00:00:00+00:00', 'a'*32)
    initial.write.format('delta').saveAsTable(target)
    version = spark.sql('DESCRIBE HISTORY ' + target).first().version
    baseline = spark.table(target).collect()
    def merge(frame):
        frame.createOrReplaceTempView(view)
        spark.sql(merge_sql(target, view))
    merge(initial)
    assert spark.table(target).collect() == baseline, 'Replay changed stored records'
    merge(batch('20', '2025-01-03T00:00:00+00:00', 'b'*32))
    assert spark.table(target).first().fare == '20'
    merge(batch('99', '2025-01-01T00:00:00+00:00', 'f'*32))
    assert spark.table(target).first().fare == '20', 'Stale extraction replaced newer data'
    merge(batch('30', '2025-01-03T00:00:00+00:00', 'c'*32))
    assert spark.table(target).first().fare == '30', 'Tie-break by run ID failed'
    merge(batch('98', '2025-01-03T00:00:00+00:00', 'b'*32))
    assert spark.table(target).count() == 1 and spark.table(target).first().fare == '30'
    assert spark.sql(f'SELECT * FROM {target} VERSION AS OF {version}').first().fare == '10'
    # Real Delta overwrite/read, using production silver conversion output.
    example = json.loads((fixtures / 'etl/silver_source.json').read_text())
    from chicago_taxi.transformations.silver.taxi_trips import build_tables
    tables = build_tables(source(spark, example))
    fact = tables['fact_trip']
    path = str(folder / 'silver_fact')
    fact.write.format('delta').mode('overwrite').save(path)
    first = spark.read.format('delta').load(path)
    expected = first.collect()
    fact.write.format('delta').mode('overwrite').save(path)
    assert spark.read.format('delta').load(path).collect() == expected
    assert spark.read.format('delta').load(path).schema == first.schema
    from chicago_taxi.transformations.gold.taxi_marts import build_marts
    daily = build_marts(tables, 'delta-gold')['mart_daily_summary']
    gold_path = str(folder / 'gold_daily')
    daily.write.format('delta').mode('overwrite').save(gold_path)
    before = spark.read.format('delta').load(gold_path).collect()
    assert len(before) == 1 and before[0].trip_count == 1
    from decimal import Decimal
    assert before[0].known_revenue_usd == Decimal('14.75')
    daily.write.format('delta').mode('overwrite').save(gold_path)
    assert spark.read.format('delta').load(gold_path).collect() == before



def landing_replay(spark, folder, fixtures):
    rows = json.loads((fixtures / 'etl/e2e_trips.json').read_text())[:1]
    run = ingest(folder, '2025-01-01', '2025-01-02', run_id='d'*32, fetch=lambda *args: rows)
    landing = folder / 'taxi_trips' / ('run_id=' + run['run_id'])
    target, audit = 'spark_catalog.default.loaded_trips', 'spark_catalog.default.loaded_runs'
    first = load_run(spark, landing, target=target, audit_table=audit)
    before = spark.table(target).collect()
    second = load_run(spark, landing, target=target, audit_table=audit)
    assert first['distinct_rows'] == second['distinct_rows'] == 1
    assert spark.table(target).collect() == before
    assert spark.table(audit).count() == 1
    saved = spark.table(audit).first()
    assert saved.landing_run_id == run['run_id'] and saved.extracted_rows == saved.distinct_rows == 1


if __name__ == '__main__':
    case, folder, jars, fixtures = sys.argv[1:]
    folder, jars, fixtures = Path(folder), Path(jars), Path(fixtures)
    spark = (SparkSession.builder.master('local[2]').appName('delta-storage-tests')
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.warehouse.dir', str(folder / 'warehouse'))
        .config('spark.jars', ','.join(str(p) for p in sorted(jars.glob('*.jar'))))
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.databricks.delta.snapshotPartitions', '2').getOrCreate())
    try:
        globals()[case](spark, folder, fixtures)
        print('PASSED ' + case)
    finally:
        spark.stop()
