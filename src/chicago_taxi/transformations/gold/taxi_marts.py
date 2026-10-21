"""Coverage-aware reporting marts; accepted silver trips only."""
import json
import re
import uuid
from functools import reduce
from pyspark.sql import functions as F, Window
from chicago_taxi.transformations.gold.contracts import SCHEMAS, KEYS, MAP_SCHEMA
from chicago_taxi.transformations.silver.contracts import SCHEMAS as SILVER_SCHEMAS
from chicago_taxi.transformations.silver.taxi_trips import validate_tables as validate_silver


def aggregate(df, keys):
    grouped = df.groupBy(*keys).agg(F.count('*').alias('trip_count'),
        F.count('trip_total').alias('revenue_trip_count'),F.sum('trip_total').cast('decimal(28,2)').alias('known_revenue_usd'),
        F.count('trip_km').alias('distance_trip_count'),
        F.sum(F.when(F.col('has_taxi_id'),1).otherwise(0)).alias('identified_taxi_trip_count'),F.sum('trip_km').cast('decimal(28,6)').alias('total_km'),
        F.avg('trip_total').cast('decimal(22,6)').alias('avg_known_trip_total_usd'),
        F.avg('trip_km').cast('decimal(22,6)').alias('avg_known_trip_km'),
        F.min(F.when(F.col('trip_km') > 0,F.col('trip_km'))).alias('shortest_positive_trip_km'),
        F.max('trip_km').alias('longest_trip_km'))
    return grouped.withColumn('revenue_coverage', F.col('revenue_trip_count')/F.col('trip_count')).withColumn(
        'distance_coverage',F.col('distance_trip_count')/F.col('trip_count')).withColumn(
        'taxi_identity_coverage',F.col('identified_taxi_trip_count')/F.col('trip_count'))


def build_marts(silver, load_id):
    fact = silver['fact_trip'].withColumn('trip_date',F.to_date('trip_start_local'))
    daily = aggregate(fact,['trip_date'])
    taxi_dim = silver['dim_taxi'].select('taxi_key','taxi_id','is_unknown')
    enriched = fact.join(taxi_dim,'taxi_key','left')
    taxis = aggregate(enriched.filter(~F.col('is_unknown') & F.col('taxi_id').isNotNull()),['trip_date','taxi_key','taxi_id'])
    ranking = Window.partitionBy('trip_date').orderBy(F.col('known_revenue_usd').desc_nulls_last())
    taxis = taxis.withColumn('revenue_rank',F.when(F.col('revenue_trip_count')>0,F.dense_rank().over(ranking)).cast('bigint'))
    company = fact.join(silver['dim_company'].select('company_key','company_name'),'company_key','left').join(
        silver['dim_payment_type'].select('payment_type_key','payment_type'),'payment_type_key','left')
    companies = aggregate(company,['trip_date','company_key','company_name','payment_type_key','payment_type'])
    area_frames = []
    for role in ['pickup','dropoff']:
        area_fact = fact.withColumn('area_key',F.col(role+'_area_key')).withColumn('area_role',F.lit(role))
        area = aggregate(area_fact,['trip_date','area_role','area_key']).join(silver['dim_community_area'],'area_key','left')
        # Keep declared BI column order stable after join.
        area_frames.append(area.select('trip_date','area_role','area_key','area_id','area_name','is_unknown',
            *[name for name in daily.columns if name != 'trip_date']))
    areas = area_frames[0].unionByName(area_frames[1])
    bounds = daily.select('trip_date','shortest_positive_trip_km','longest_trip_km')
    bounded = enriched.join(bounds,'trip_date','inner')
    extremes = []
    for label, bound in [('shortest_positive','shortest_positive_trip_km'),('longest','longest_trip_km')]:
        extremes.append(bounded.filter(F.col('trip_km') == F.col(bound)).select('trip_date',F.lit(label).alias('extreme_type'),
            'trip_id','taxi_id','trip_km','trip_total','pickup_area_key','dropoff_area_key'))
    marts = dict(mart_daily_summary=daily,mart_taxi_revenue_daily=taxis,mart_area_demand_daily=areas,
        mart_company_payment_daily=companies,mart_distance_extremes=extremes[0].unionByName(extremes[1]))
    if 'ref_community_area' in silver:
        reference=silver['ref_community_area']
        mismatched=silver['dim_community_area'].filter('area_key != 0').alias('d').join(reference.alias('r'),'area_key','left')
        if mismatched.filter(F.col('r.area_name').isNull() | ~F.col('d.area_name').eqNullSafe(F.col('r.area_name'))).limit(1).count():
            raise ValueError('Silver area names do not match pinned reference; rebuild silver')
        marts['mart_area_map_daily']=areas.filter('area_key != 0').join(reference.select('area_key','geometry_geojson',
            'map_latitude','map_longitude',F.col('source_run_id').alias('reference_run_id')),'area_key','inner').select(
            *areas.columns,'geometry_geojson','map_latitude','map_longitude','reference_run_id')
    return {name:df.withColumn('gold_load_id',F.lit(load_id)) for name,df in marts.items()}


def reconcile(marts, silver):
    """Check per-date count, known-value coverage and additive measures, before publication."""
    fact = silver['fact_trip'].withColumn('trip_date',F.to_date('trip_start_local'))
    expected = aggregate(fact,['trip_date'])
    checks = [('daily',marts['mart_daily_summary'],expected),
        ('company/payment',marts['mart_company_payment_daily'],expected)]
    for role in ['pickup','dropoff']:
        checks.append((role,marts['mart_area_demand_daily'].filter(F.col('area_role') == role),expected))
    taxi_expected = aggregate(fact.filter('has_taxi_id'),['trip_date'])
    checks.append(('known taxis',marts['mart_taxi_revenue_daily'],taxi_expected))
    metrics = ['trip_count','revenue_trip_count','known_revenue_usd','distance_trip_count','identified_taxi_trip_count','total_km']
    for label, actual, reference in checks:
        actual = actual.groupBy('trip_date').agg(*[F.sum(name).alias(name) for name in metrics])
        target = reference.select('trip_date',*metrics)
        if actual.exceptAll(target).limit(1).count() or target.exceptAll(actual).limit(1).count():
            raise ValueError('Gold reconciliation failed: '+label)
    if 'mart_area_map_daily' in marts:
        expected_map=marts['mart_area_demand_daily'].filter('area_key != 0')
        actual_map=marts['mart_area_map_daily'].select(*expected_map.columns)
        if actual_map.exceptAll(expected_map).limit(1).count() or expected_map.exceptAll(actual_map).limit(1).count():
            raise ValueError('Map enrichment changed area demand counts or revenue')
    keys_to_check=dict(KEYS)
    if 'mart_area_map_daily' in marts:
        keys_to_check['mart_area_map_daily']=['trip_date','area_role','area_key']
    for name, keys in keys_to_check.items():
        frame = marts[name]
        null_key = reduce(lambda left,right:left|right,[F.col(key).isNull() for key in keys])
        if frame.filter(null_key).limit(1).count() or frame.groupBy(*keys).count().filter('count > 1').limit(1).count():
            raise ValueError('Missing or duplicate gold grain: '+name)
    return dict(fact_rows=fact.count(),quarantined_rows=silver['quarantine_trip'].count())


def snapshot_silver(spark, catalog):
    """Fail closed on tables changed since the latest completed silver publication."""
    audit_table = catalog+'.silver.warehouse_loads'
    audit = spark.table(audit_table).orderBy(F.desc('completed_at_utc'),F.desc('silver_load_id')).first()
    if audit is None:
        raise ValueError('A completed silver publication is required')
    frames, versions = {}, {}
    for name in SILVER_SCHEMAS:
        table = catalog+'.silver.'+name
        history = spark.sql('DESCRIBE HISTORY '+table).select('version','timestamp').orderBy(F.desc('version')).first()
        if history['timestamp'] > audit['completed_at_utc']:
            raise ValueError('Silver changed after its completed audit; finish/recover silver first')
        version = int(history['version'])
        versions[name] = version
        frames[name] = spark.sql(f'SELECT * FROM {table} VERSION AS OF {version}')
    latest = spark.table(audit_table).orderBy(F.desc('completed_at_utc'),F.desc('silver_load_id')).first()
    if latest is None or latest['silver_load_id'] != audit['silver_load_id']:
        raise ValueError('Silver publication changed while pinning tables; retry gold')
    counts = validate_silver(frames,audit['input_rows'])
    if counts['fact_rows'] != audit['fact_rows'] or counts['quarantined_rows'] != audit['quarantined_rows']:
        raise ValueError('Silver audit counts do not match pinned tables')
    return frames, versions, audit['silver_load_id']


def load_gold(spark, catalog='chicago_taxi_dev'):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',catalog):
        raise ValueError('Invalid catalog identifier')
    silver, versions, silver_load_id = snapshot_silver(spark,catalog)
    from chicago_taxi.geography.storage import reference_for_silver
    reference, reference_version=reference_for_silver(spark,catalog,silver_load_id)
    if reference is not None:
        silver['ref_community_area']=reference
        versions['ref_community_area']=reference_version
    load_id = uuid.uuid4().hex
    marts = build_marts(silver,load_id)
    summary = reconcile(marts,silver)
    schemas=dict(SCHEMAS)
    if 'mart_area_map_daily' in marts:
        schemas['mart_area_map_daily']=MAP_SCHEMA
    for name, frame in marts.items():
        target = catalog+'.gold.'+name
        spark.sql(f'CREATE TABLE IF NOT EXISTS {target} ({schemas[name]}) USING DELTA')
        frame.write.format('delta').mode('overwrite').saveAsTable(target)
    # Separate table transactions: hold reporting until this completed audit is present.
    audit = spark.createDataFrame([(load_id,silver_load_id,json.dumps(versions,sort_keys=True),summary['fact_rows'],summary['quarantined_rows'])],
        'gold_load_id string, silver_load_id string, silver_versions_json string, fact_rows long, quarantined_rows long').withColumn('completed_at_utc',F.current_timestamp())
    audit.write.format('delta').mode('append').saveAsTable(catalog+'.gold.reporting_loads')
    return dict(status='success',gold_load_id=load_id,silver_load_id=silver_load_id,**summary)
