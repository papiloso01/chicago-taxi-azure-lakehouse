import json
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import SparkSession, functions as F
from chicago_taxi.geography.community_areas import normalize, REFERENCE_SCHEMA
from chicago_taxi.geography.storage import validate_reference, reference_for_silver
from chicago_taxi.transformations.bronze.taxi_trips import prepare,stage_schema
from chicago_taxi.transformations.silver.taxi_trips import build_tables,validate_tables
from chicago_taxi.transformations.gold.taxi_marts import build_marts,reconcile
from chicago_taxi.transformations.gold.contracts import MAP_SCHEMA


@pytest.fixture(scope='module')
def spark():
    s=SparkSession.builder.master('local[2]').appName('community-enrichment').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    yield s
    s.stop()


def reference(spark):
    source=[]
    for area in range(1,78):
        x=-88+area/1000
        source.append(dict(area_num_1=str(area),community=f'AREA {area}',the_geom=dict(type='Polygon',
            coordinates=[[[x,41.7],[x+.0005,41.7],[x+.0005,41.701],[x,41.701],[x,41.7]]])))
    return spark.createDataFrame([dict(row,source_run_id='a'*32,source_sha256='b'*64) for row in normalize(source)],REFERENCE_SCHEMA)


def bronze(spark):
    rows=[]
    for key,pickup,dropoff in [('a','8','9'),('b',None,'8')]:
        raw=json.dumps(dict(trip_id=key,trip_start_timestamp='2025-01-01T12:00:00',trip_total='10',trip_miles='1',
            pickup_community_area=pickup,dropoff_community_area=dropoff))
        rows.append((key,raw,'f'*64,'c'*32,'fixture.json','2025-01-02T00:00:00+00:00'))
    return prepare(spark.createDataFrame(rows,stage_schema()))


def test_lookup_validates_complete_geometry(spark):
    ref=reference(spark)
    assert validate_reference(ref).count()==77
    with pytest.raises(ValueError): validate_reference(ref.filter('area_key != 77'))
    with pytest.raises(ValueError,match='Map point'): validate_reference(ref.withColumn('map_latitude',F.lit(0)))


def test_rebuild_retains_names_and_join_preserves_counts(spark):
    ref=reference(spark); source=bronze(spark)
    old=build_tables(source); enriched=build_tables(source,ref); replay=build_tables(source,ref)
    assert validate_tables(enriched,2)==dict(input_rows=2,fact_rows=2,quarantined_rows=0)
    names={r.area_key:r.area_name for r in enriched['dim_community_area'].collect()}
    assert names=={0:None,8:'AREA 8',9:'AREA 9'}
    assert enriched['dim_community_area'].orderBy('area_key').collect()==replay['dim_community_area'].orderBy('area_key').collect()
    enriched['ref_community_area']=ref
    before=build_marts(old,'gold-test'); after=build_marts(enriched,'gold-test')
    assert reconcile(after,enriched)==dict(fact_rows=2,quarantined_rows=0)
    assert before['mart_daily_summary'].collect()==after['mart_daily_summary'].collect()
    mapped=after['mart_area_map_daily']
    assert [(f.name,f.dataType) for f in mapped.schema]==[(f.name,f.dataType) for f in spark.createDataFrame([],MAP_SCHEMA).schema]
    assert mapped.filter('area_key = 0').count()==0
    assert mapped.filter('area_name IS NULL').count()==0
    assert {r.area_role:r['sum(trip_count)'] for r in mapped.groupBy('area_role').sum('trip_count').collect()}=={'pickup':1,'dropoff':2}
    assert all(r.reference_run_id=='a'*32 for r in mapped.collect())
    broken=dict(after,mart_area_map_daily=mapped.withColumn('trip_count',F.col('trip_count')+1))
    with pytest.raises(ValueError,match='Map enrichment'): reconcile(broken,enriched)


def test_missing_or_stale_names_block_map(spark):
    tables=build_tables(bronze(spark));tables['ref_community_area']=reference(spark)
    with pytest.raises(ValueError,match='area names'): build_marts(tables,'gold-test')


def test_old_silver_job_without_reference_lineage_is_rejected(spark):
    fake=MagicMock();fake.catalog.tableExists.side_effect=[False,True]
    with pytest.raises(ValueError,match='Rebuild silver'): reference_for_silver(fake,'chicago_taxi_dev','silver-1')


def test_gold_reads_the_reference_version_bound_to_silver(spark):
    fake=MagicMock();fake.catalog.tableExists.return_value=True
    fake.table.return_value.filter.return_value.limit.return_value.collect.return_value=[type('Lineage',(),dict(reference_version=4))()]
    with patch('chicago_taxi.geography.storage.validate_reference',side_effect=lambda frame:frame):
        _,version=reference_for_silver(fake,'chicago_taxi_dev','silver-1')
    assert version==4
    assert 'VERSION AS OF 4' in fake.sql.call_args.args[0]
