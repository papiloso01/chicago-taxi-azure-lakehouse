import json
import pytest
from pyspark.sql import functions as F
from chicago_taxi.geography.community_areas import normalize, REFERENCE_SCHEMA
from chicago_taxi.geography.storage import validate_reference
from chicago_taxi.transformations.bronze.taxi_trips import prepare, stage_schema
from chicago_taxi.transformations.silver.taxi_trips import build_tables, validate_tables
from chicago_taxi.transformations.gold.taxi_marts import build_marts, reconcile
from chicago_taxi.transformations.gold.contracts import MAP_SCHEMA


def reference(spark, community_examples):
    return spark.createDataFrame([dict(row,source_run_id='a'*32,source_sha256='b'*64) for row in normalize(community_examples)],REFERENCE_SCHEMA)


def bronze(spark, examples):
    rows=[]
    for example in examples:
        key = example['trip_id']
        raw = json.dumps(example)
        rows.append((key,raw,'f'*64,'c'*32,'fixture.json','2025-01-02T00:00:00+00:00'))
    return prepare(spark.createDataFrame(rows,stage_schema()))


def test_lookup_validates_complete_geometry(spark, community_examples, load_example):
    ref=reference(spark, community_examples)
    assert validate_reference(ref).count()==77
    with pytest.raises(ValueError): validate_reference(ref.filter('area_key != 77'))
    with pytest.raises(ValueError,match='Map point'): validate_reference(ref.withColumn('map_latitude',F.lit(0)))


def test_rebuild_retains_names_and_join_preserves_counts(spark, community_examples, load_example):
    ref=reference(spark, community_examples); source=bronze(spark, load_example("enrichment_trips.json"))
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


def test_missing_or_stale_names_block_map(spark, community_examples, load_example):
    tables=build_tables(bronze(spark, load_example("enrichment_trips.json")));tables['ref_community_area']=reference(spark, community_examples)
    with pytest.raises(ValueError,match='area names'): build_marts(tables,'gold-test')
