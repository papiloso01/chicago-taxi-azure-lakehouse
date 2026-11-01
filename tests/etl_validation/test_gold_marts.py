from datetime import date
from decimal import Decimal
import pytest
from pyspark.sql import functions as F
from chicago_taxi.transformations.gold.contracts import SCHEMAS
from chicago_taxi.transformations.gold.taxi_marts import build_marts, reconcile


def test_daily_null_coverage_precision_and_zero(spark, gold_fixture):
    marts = build_marts(gold_fixture(spark),'gold-test')
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


def test_vehicle_ranking_excludes_unknown_and_missing_revenue(spark, gold_fixture):
    rows = build_marts(gold_fixture(spark),'gold-test')['mart_taxi_revenue_daily'].collect()
    indexed = {(r.trip_date,r.taxi_id):r for r in rows}
    assert {r.taxi_id for r in rows} == {'taxi-A','taxi-B'}
    assert indexed[date(2025,1,1),'taxi-B'].revenue_rank == 1
    assert indexed[date(2025,1,1),'taxi-A'].revenue_rank == 2
    assert indexed[date(2025,1,2),'taxi-A'].revenue_rank is None


def test_all_extreme_ties_retained_and_zero_excluded_from_shortest(spark, gold_fixture):
    rows = build_marts(gold_fixture(spark),'gold-test')['mart_distance_extremes'].collect()
    assert {(r.extreme_type,r.trip_id) for r in rows} == {('shortest_positive','a'),('longest','b'),('longest','c')}
    assert all(r.trip_date == date(2025,1,1) for r in rows)


def test_area_roles_do_not_silently_double_trip_totals(spark, gold_fixture):
    rows = build_marts(gold_fixture(spark),'gold-test')['mart_area_demand_daily']
    totals = {r.area_role:r['sum(trip_count)'] for r in rows.groupBy('area_role').sum('trip_count').collect()}
    assert totals == {'pickup':5,'dropoff':5}
    assert rows.filter('area_key = 0').count() > 0
    assert all(r.area_name is None for r in rows.collect())


def test_reconciliation_and_schema_contracts(spark, gold_fixture):
    silver=gold_fixture(spark)
    marts=build_marts(silver,'gold-test')
    assert reconcile(marts,silver) == dict(fact_rows=5,quarantined_rows=0)
    for name,df in marts.items():
        expected=spark.createDataFrame([],SCHEMAS[name]).schema
        assert [(f.name,f.dataType) for f in df.schema] == [(f.name,f.dataType) for f in expected]


def test_corrupt_aggregate_fails_before_publication(spark, gold_fixture):
    silver=gold_fixture(spark)
    marts=build_marts(silver,'gold-test')
    marts['mart_daily_summary']=marts['mart_daily_summary'].withColumn('trip_count',F.col('trip_count')+1)
    with pytest.raises(ValueError,match='reconciliation'): reconcile(marts,silver)


def test_empty_snapshot_does_not_invent_zero_revenue_rows(spark, gold_fixture):
    silver=gold_fixture(spark,empty=True)
    marts=build_marts(silver,'gold-test')
    assert reconcile(marts,silver) == dict(fact_rows=0,quarantined_rows=0)
    assert all(frame.count() == 0 for frame in marts.values())


def test_equal_revenues_share_rank_and_zero_revenue_is_known(spark, gold_fixture):
    silver=gold_fixture(spark)
    silver['fact_trip']=silver['fact_trip'].filter("trip_id IN ('a','b')").withColumn('trip_total',F.lit(0).cast('decimal(18,2)'))
    rows=build_marts(silver,'gold-test')['mart_taxi_revenue_daily'].collect()
    assert len(rows) == 2 and {r.revenue_rank for r in rows} == {1}
    assert all(r.known_revenue_usd == 0 and r.revenue_trip_count == 1 for r in rows)
