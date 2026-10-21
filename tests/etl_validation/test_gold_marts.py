from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import SparkSession, functions as F
from chicago_taxi.transformations.silver.contracts import SCHEMAS as SILVER
from chicago_taxi.transformations.gold.contracts import SCHEMAS
from chicago_taxi.transformations.gold.taxi_marts import build_marts, reconcile, snapshot_silver, load_gold


@pytest.fixture(scope='module')
def spark():
    s = SparkSession.builder.master('local[2]').appName('gold-validation').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    yield s
    s.stop()


def fixture(spark, empty=False):
    rows=[]
    for key,taxi,revenue,km,pickup,dropoff,day in [
        ('a','A','10','1',8,0,1),('b','B','20','5',8,9,1),('c','B',None,'5',0,8,1),
        ('d',None,'20','0',9,8,1),('e','A',None,None,0,0,2)]:
        rows.append(dict(trip_id=key,taxi_key=taxi or 'unknown',company_key='company',payment_type_key='card',
            start_date_key=20250100+day,trip_start_local=datetime(2025,1,day,12),
            trip_total=Decimal(revenue) if revenue else None,trip_km=Decimal(km) if km else None,
            pickup_area_key=pickup,dropoff_area_key=dropoff,has_taxi_id=taxi is not None,
            has_trip_total=revenue is not None,has_distance=km is not None))
    result = dict(fact_trip=spark.createDataFrame([] if empty else rows,SILVER['fact_trip']),
        quarantine_trip=spark.createDataFrame([],SILVER['quarantine_trip']),
        dim_taxi=spark.createDataFrame([('A','taxi-A',False),('B','taxi-B',False),('unknown',None,True)],SILVER['dim_taxi']),
        dim_company=spark.createDataFrame([('company','COMPANY',False)],SILVER['dim_company']),
        dim_payment_type=spark.createDataFrame([('card','CREDIT CARD',False)],SILVER['dim_payment_type']),
        dim_community_area=spark.createDataFrame([(0,None,None,True),(8,8,None,False),(9,9,None,False)],SILVER['dim_community_area']))
    return result


def test_daily_null_coverage_precision_and_zero(spark):
    marts = build_marts(fixture(spark),'gold-test')
    rows = {r.trip_date:r for r in marts['mart_daily_summary'].collect()}
    first,second = rows[date(2025,1,1)],rows[date(2025,1,2)]
    assert first.trip_count == 4 and first.revenue_trip_count == 3
    assert first.known_revenue_usd == Decimal('50.00')
    assert first.total_km == Decimal('11.000000')
    assert first.revenue_coverage == .75 and first.distance_coverage == 1
    assert first.identified_taxi_trip_count == 3 and first.taxi_identity_coverage == .75
    assert first.avg_known_trip_total_usd == Decimal('16.666667')
    assert first.shortest_positive_trip_km == Decimal('1') and first.longest_trip_km == Decimal('5')
    assert second.known_revenue_usd is None and second.total_km is None
    assert second.revenue_coverage == 0 and second.distance_coverage == 0


def test_vehicle_ranking_excludes_unknown_and_missing_revenue(spark):
    rows = build_marts(fixture(spark),'gold-test')['mart_taxi_revenue_daily'].collect()
    indexed = {(r.trip_date,r.taxi_id):r for r in rows}
    assert {r.taxi_id for r in rows} == {'taxi-A','taxi-B'}
    assert indexed[date(2025,1,1),'taxi-B'].revenue_rank == 1
    assert indexed[date(2025,1,1),'taxi-A'].revenue_rank == 2
    assert indexed[date(2025,1,2),'taxi-A'].revenue_rank is None


def test_all_extreme_ties_retained_and_zero_excluded_from_shortest(spark):
    rows = build_marts(fixture(spark),'gold-test')['mart_distance_extremes'].collect()
    assert {(r.extreme_type,r.trip_id) for r in rows} == {('shortest_positive','a'),('longest','b'),('longest','c')}
    assert all(r.trip_date == date(2025,1,1) for r in rows)


def test_area_roles_do_not_silently_double_trip_totals(spark):
    rows = build_marts(fixture(spark),'gold-test')['mart_area_demand_daily']
    totals = {r.area_role:r['sum(trip_count)'] for r in rows.groupBy('area_role').sum('trip_count').collect()}
    assert totals == {'pickup':5,'dropoff':5}
    assert rows.filter('area_key = 0').count() > 0
    assert all(r.area_name is None for r in rows.collect())


def test_reconciliation_and_schema_contracts(spark):
    silver=fixture(spark)
    marts=build_marts(silver,'gold-test')
    assert reconcile(marts,silver) == dict(fact_rows=5,quarantined_rows=0)
    for name,df in marts.items():
        expected=spark.createDataFrame([],SCHEMAS[name]).schema
        assert [(f.name,f.dataType) for f in df.schema] == [(f.name,f.dataType) for f in expected]


def test_corrupt_aggregate_fails_before_publication(spark):
    silver=fixture(spark)
    marts=build_marts(silver,'gold-test')
    marts['mart_daily_summary']=marts['mart_daily_summary'].withColumn('trip_count',F.col('trip_count')+1)
    with pytest.raises(ValueError,match='reconciliation'): reconcile(marts,silver)


def test_empty_snapshot_does_not_invent_zero_revenue_rows(spark):
    silver=fixture(spark,empty=True)
    marts=build_marts(silver,'gold-test')
    assert reconcile(marts,silver) == dict(fact_rows=0,quarantined_rows=0)
    assert all(frame.count() == 0 for frame in marts.values())


def test_equal_revenues_share_rank_and_zero_revenue_is_known(spark):
    silver=fixture(spark)
    silver['fact_trip']=silver['fact_trip'].filter("trip_id IN ('a','b')").withColumn('trip_total',F.lit(0).cast('decimal(18,2)'))
    rows=build_marts(silver,'gold-test')['mart_taxi_revenue_daily'].collect()
    assert len(rows) == 2 and {r.revenue_rank for r in rows} == {1}
    assert all(r.known_revenue_usd == 0 and r.revenue_trip_count == 1 for r in rows)


def mock_snapshot(changed=False):
    spark=MagicMock()
    audit=dict(silver_load_id='silver-1',completed_at_utc=datetime(2025,1,2),input_rows=5,fact_rows=5,quarantined_rows=0)
    spark.table.return_value.orderBy.return_value.first.side_effect=[audit,audit]
    timestamp=audit['completed_at_utc'] + timedelta(seconds=1 if changed else -1)
    spark.sql.return_value.select.return_value.orderBy.return_value.first.return_value=dict(version=7,timestamp=timestamp)
    return spark,audit


def test_snapshot_pins_versions_and_audit_identity(spark):
    spark,audit=mock_snapshot()
    with patch('chicago_taxi.transformations.gold.taxi_marts.validate_silver',return_value=dict(fact_rows=5,quarantined_rows=0)):
        frames,versions,load_id=snapshot_silver(spark,'chicago_taxi_dev')
    assert load_id == audit['silver_load_id']
    assert set(frames) == set(SILVER) and set(versions.values()) == {7}
    assert any('VERSION AS OF 7' in call.args[0] for call in spark.sql.call_args_list)


def test_unpublished_silver_changes_block_gold_writes(spark):
    spark,_=mock_snapshot(changed=True)
    with pytest.raises(ValueError,match='completed audit'): load_gold(spark)
    assert not any('CREATE TABLE' in call.args[0] for call in spark.sql.call_args_list)
    spark.createDataFrame.assert_not_called()


def test_catalog_identifier_is_validated():
    with pytest.raises(ValueError): load_gold(None,'a; DROP TABLE b')


def test_missing_silver_publication_blocks_gold(spark):
    fake=MagicMock()
    fake.table.return_value.orderBy.return_value.first.return_value=None
    with pytest.raises(ValueError,match='completed silver'): load_gold(fake)
    fake.sql.assert_not_called()


def test_new_silver_publication_during_capture_is_rejected(spark):
    fake,audit=mock_snapshot()
    newer=dict(audit,silver_load_id='silver-2')
    fake.table.return_value.orderBy.return_value.first.side_effect=[audit,newer]
    with pytest.raises(ValueError,match='changed while pinning'): snapshot_silver(fake,'chicago_taxi_dev')


def test_silver_audit_count_mismatch_blocks_gold(spark):
    fake,_=mock_snapshot()
    with patch('chicago_taxi.transformations.gold.taxi_marts.validate_silver',return_value=dict(fact_rows=4,quarantined_rows=1)):
        with pytest.raises(ValueError,match='audit counts'): snapshot_silver(fake,'chicago_taxi_dev')
