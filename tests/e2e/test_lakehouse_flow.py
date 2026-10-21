"""Offline source→landing→bronze→silver→gold flow; excludes Azure and Delta storage writes."""
import json
import shutil
import pytest
from pyspark.sql import SparkSession
from chicago_taxi.ingestion.landing import ingest
from chicago_taxi.geography.community_areas import extract, read_snapshot, REFERENCE_SCHEMA
from chicago_taxi.transformations.bronze.landing_manifest import stage_run
from chicago_taxi.transformations.bronze.taxi_trips import prepare, stage_schema
from chicago_taxi.transformations.silver.taxi_trips import build_tables, validate_tables
from chicago_taxi.transformations.gold.taxi_marts import build_marts, reconcile


def test_raw_pages_through_gold_reporting(tmp_path):
    rows = [dict(trip_id='a',taxi_id='vehicle',trip_start_timestamp='2025-01-01T12:00:00',trip_miles='1',trip_total='10',pickup_community_area='8',dropoff_community_area='9'),
            dict(trip_id='b',trip_start_timestamp='broken',trip_miles='3',trip_total='20')]
    report = ingest(tmp_path,'2025-01-01','2025-01-02',run_id='f'*32,fetch=lambda *args: rows)
    folder = tmp_path/'taxi_trips'/('run_id='+report['run_id'])
    manifest, stage = stage_run(folder)
    spark = SparkSession.builder.master('local[2]').appName('offline-etl').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    try:
        bronze = prepare(spark.read.schema(stage_schema()).json(str(stage)))
        source_areas=[]
        for area in range(1,78):
            x=-88+area/1000
            source_areas.append(dict(area_num_1=str(area),community=f'AREA {area}',the_geom=dict(type='Polygon',
                coordinates=[[[x,41.7],[x+.0005,41.7],[x+.0005,41.701],[x,41.701],[x,41.7]]])))
        area_folder=extract(tmp_path,fetch=lambda *args:source_areas)
        area_manifest,_,clean_areas=read_snapshot(area_folder)
        reference=spark.createDataFrame([dict(row,source_run_id=area_manifest['run_id'],source_sha256=area_manifest['sha256']) for row in clean_areas],REFERENCE_SCHEMA)
        tables = build_tables(bronze,reference)
        tables['ref_community_area']=reference
        assert validate_tables(tables,manifest['extracted_rows']) == dict(input_rows=2,fact_rows=1,quarantined_rows=1)
        assert tables['fact_trip'].first().trip_id == 'a'
        marts = build_marts(tables,'offline-gold')
        assert reconcile(marts,tables) == dict(fact_rows=1,quarantined_rows=1)
        assert marts['mart_daily_summary'].first().known_revenue_usd == 10
        assert {r.area_name for r in marts['mart_area_map_daily'].collect()}=={'AREA 8','AREA 9'}
        assert tables['quarantine_trip'].first().trip_id == 'b'
        assert json.loads((folder/'manifest.json').read_text())['status'] == 'success'
        # Same immutable inputs regenerate the same dimension keys and fact identifiers.
        again = build_tables(bronze)
        assert again['fact_trip'].select('trip_id','taxi_key').collect() == tables['fact_trip'].select('trip_id','taxi_key').collect()
    finally:
        spark.stop()
        shutil.rmtree(stage)
