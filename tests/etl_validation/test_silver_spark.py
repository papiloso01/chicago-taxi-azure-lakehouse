from datetime import datetime
from decimal import Decimal
import json
import pytest
from pyspark.sql import SparkSession
from chicago_taxi.transformations.bronze.taxi_trips import SOURCE_FIELDS
from chicago_taxi.transformations.silver.taxi_trips import build_tables, validate_tables, load_warehouse
from chicago_taxi.transformations.silver.contracts import SCHEMAS


@pytest.fixture(scope='module')
def spark():
    s = SparkSession.builder.master('local[2]').appName('silver-validation').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    s.conf.set('spark.sql.ansi.enabled','true')
    yield s
    s.stop()


def source(spark, updates=None):
    row = dict(trip_id='a', taxi_id='taxi-A', company='  Yellow   Cab ', payment_type='credit card',
        trip_start_timestamp='2025-01-01T01:00:00.000',trip_end_timestamp='2025-01-01T01:15:00.000',
        trip_seconds='720',trip_miles='2.5',fare='12.50',tips='2.25',tolls='0',extras='0',trip_total='14.75',
        pickup_community_area='8',dropoff_community_area=None,landing_run_id='a'*32,payload_sha256='b'*64)
    row.update(updates or {})
    row['raw_payload'] = json.dumps(row)
    row['extracted_at_utc'] = datetime(2025,1,2)
    columns = ['trip_id']+SOURCE_FIELDS+['raw_payload','landing_run_id','payload_sha256']
    schema = ', '.join(name+' string' for name in columns)+', extracted_at_utc timestamp'
    return spark.createDataFrame([tuple(row.get(name) for name in columns)+ (row['extracted_at_utc'],)],schema)


def test_typed_measures_and_local_time(spark):
    tables = build_tables(source(spark))
    row = tables['fact_trip'].first()
    assert row.trip_km == Decimal('4.023360')
    assert row.trip_total == Decimal('14.75') and row.trip_seconds == 720
    assert row.trip_start_local == datetime(2025,1,1,1)
    assert row.start_date_key == 20250101
    assert row.pickup_area_key == 8 and row.dropoff_area_key == 0
    assert tables['dim_company'].first().company_name == 'YELLOW CAB'
    assert tables['dim_payment_type'].first().payment_type == 'CREDIT CARD'
    assert dict(tables['fact_trip'].dtypes)['trip_start_local'] == 'timestamp_ntz'
    assert tables['quarantine_trip'].count() == 0
    counts = validate_tables(tables,1)
    assert counts == dict(input_rows=1,fact_rows=1,quarantined_rows=0)


def test_missing_optional_values_are_unavailable_not_zero(spark):
    tables = build_tables(source(spark, dict(trip_miles=None,trip_total='',taxi_id=' ',company=None,payment_type=None)))
    row = tables['fact_trip'].first()
    assert row.trip_km is None and row.trip_total is None
    assert not row.has_distance and not row.has_trip_total and not row.has_taxi_id
    assert tables['dim_taxi'].first().is_unknown
    assert tables['dim_taxi'].first().taxi_id is None


@pytest.mark.parametrize('changes,issue', [
    ({'trip_miles':'bad'},'invalid_trip_miles'),
    ({'trip_total':'-1'},'invalid_trip_total'),
    ({'trip_total':'-0.001'},'invalid_trip_total'),
    ({'trip_miles':'-0.0000001'},'invalid_trip_miles'),
    ({'pickup_community_area':'78'},'invalid_pickup_community_area'),
    ({'trip_start_timestamp':'2025-02-30T00:00:00'},'invalid_trip_start_timestamp'),
    ({'trip_start_timestamp':None},'missing_trip_start'),
    ({'trip_end_timestamp':'2025-01-01T00:00:00'},'end_before_start'),
    ({'trip_start_timestamp':'2025-01-01T01:00:00Z'},'invalid_trip_start_timestamp'),
    ({'trip_seconds':'1.5'},'invalid_trip_seconds'),
    ({'trip_miles':'999999999999'},'distance_conversion_overflow'),
])
def test_invalid_rows_retained_with_reasons(spark,changes,issue):
    tables = build_tables(source(spark,changes))
    assert tables['fact_trip'].count() == 0
    row = tables['quarantine_trip'].first()
    assert issue in row.quality_issues
    assert json.loads(row.raw_payload)['trip_id'] == 'a'


def test_zero_distance_is_valid_and_unknown_does_not_collide(spark):
    df = source(spark,dict(trip_id='a',taxi_id=None,trip_miles='0')).unionByName(source(spark,dict(trip_id='b',taxi_id='UNKNOWN',trip_miles='0')))
    tables = build_tables(df)
    assert tables['fact_trip'].count() == 2
    assert tables['dim_taxi'].count() == 2
    assert {r.trip_km for r in tables['fact_trip'].collect()} == {Decimal('0.000000')}


def test_orphan_and_duplicate_keys_fail(spark):
    tables = build_tables(source(spark))
    bad = dict(tables,dim_taxi=tables['dim_taxi'].limit(0))
    with pytest.raises(ValueError,match='Orphan'): validate_tables(bad,1)
    bad = dict(tables,fact_trip=tables['fact_trip'].unionByName(tables['fact_trip']))
    with pytest.raises(ValueError,match='duplicate'): validate_tables(bad,2)


def test_schema_contracts_match_actual_outputs(spark):
    tables = build_tables(source(spark))
    for name, df in tables.items():
        expected = spark.createDataFrame([],SCHEMAS[name]).schema
        assert [(f.name,f.dataType) for f in df.schema] == [(f.name,f.dataType) for f in expected]


def test_catalog_injection_rejected():
    with pytest.raises(ValueError): load_warehouse(None,'a; DROP TABLE b')
