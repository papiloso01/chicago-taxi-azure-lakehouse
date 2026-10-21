"""Typed local-time taxi warehouse; no inferred driver/gender/route information."""
from pyspark.sql import functions as F
from chicago_taxi.transformations.silver.contracts import SCHEMAS, KEYS

MONEY_FIELDS = ['fare', 'tips', 'tolls', 'extras', 'trip_total']


def nonempty(name):
    return F.col(name).isNotNull() & (F.trim(F.col(name)) != '')


def normalize(name):
    return F.when(nonempty(name), F.upper(F.regexp_replace(F.trim(F.col(name)), r'\s+', ' ')))


def key(name):
    # Separate unknown namespace prevents collision with literal source 'UNKNOWN'.
    return F.sha2(F.when(F.col(name).isNull(), F.lit('unknown:')).otherwise(F.concat(F.lit('value:'), F.col(name))), 256)


def clean(bronze):
    issues = [F.when(~nonempty('trip_id'), F.lit('missing_trip_id'))]
    df = bronze
    for source, dest in [('trip_start_timestamp', 'trip_start_local'), ('trip_end_timestamp', 'trip_end_local')]:
        df = df.withColumn(dest, F.expr(f'try_cast({source} AS TIMESTAMP_NTZ)'))
        format_ok = F.col(source).rlike(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$')
        issues.append(F.when(nonempty(source) & (~format_ok | F.col(dest).isNull()), F.lit('invalid_'+source)))
    issues.append(F.when(~nonempty('trip_start_timestamp'), F.lit('missing_trip_start')))
    issues.append(F.when(F.col('trip_end_local') < F.col('trip_start_local'), F.lit('end_before_start')))
    numeric_types = {'trip_seconds':'BIGINT', 'trip_miles':'DECIMAL(18,6)', **{name:'DECIMAL(18,2)' for name in MONEY_FIELDS}}
    for name, dtype in numeric_types.items():
        dest = '_typed_'+name
        df = df.withColumn(dest, F.expr(f'try_cast({name} AS {dtype})'))
        issues.append(F.when(nonempty(name) & (F.col(dest).isNull() | (F.col(dest) < 0) | (F.expr(f'try_cast({name} AS DOUBLE)') < 0)), F.lit('invalid_'+name)))
    for name in ['pickup_community_area','dropoff_community_area']:
        dest = '_typed_'+name
        df = df.withColumn(dest, F.expr(f'try_cast({name} AS INT)'))
        issues.append(F.when(nonempty(name) & (F.col(dest).isNull() | ~F.col(dest).between(1,77)), F.lit('invalid_'+name)))
    df = df.withColumn('quality_issues', F.filter(F.array(*issues), lambda value: value.isNotNull()))
    df = df.withColumn('_taxi_id', F.when(nonempty('taxi_id'), F.trim('taxi_id')))
    df = df.withColumn('_company_name', normalize('company')).withColumn('_payment_type', normalize('payment_type'))
    df = df.withColumn('taxi_key', key('_taxi_id')).withColumn('company_key', key('_company_name')).withColumn('payment_type_key', key('_payment_type'))
    df = df.withColumn('start_date_key', F.date_format('trip_start_local','yyyyMMdd').cast('int'))
    for role in ['pickup','dropoff']:
        df = df.withColumn(role+'_area_key', F.coalesce(F.col('_typed_'+role+'_community_area'), F.lit(0)))
    df = df.withColumn('trip_km', F.expr('try_cast(_typed_trip_miles * cast(1.609344 AS DECIMAL(7,6)) AS DECIMAL(18,6))'))
    # Converted distance can overflow the bounded precision even when miles fits.
    df = df.withColumn('quality_issues', F.when(F.col('_typed_trip_miles').isNotNull() & F.col('trip_km').isNull(),
        F.concat('quality_issues', F.array(F.lit('distance_conversion_overflow')))).otherwise(F.col('quality_issues')))
    return df


def build_tables(bronze, community_areas=None):
    df = clean(bronze)
    valid = df.filter(F.size('quality_issues') == 0)
    rejected = df.filter(F.size('quality_issues') > 0).select('trip_id','raw_payload','quality_issues','landing_run_id','payload_sha256','extracted_at_utc')
    fact = valid.select('trip_id','taxi_key','company_key','payment_type_key','start_date_key','pickup_area_key','dropoff_area_key',
        'trip_start_local','trip_end_local', *[F.col('_typed_'+name).alias(name) for name in ['trip_seconds','trip_miles']],
        'trip_km', *[F.col('_typed_'+name).alias(name) for name in MONEY_FIELDS],
        F.col('_typed_trip_miles').isNotNull().alias('has_distance'),
        F.col('_typed_trip_total').isNotNull().alias('has_trip_total'),
        F.col('_taxi_id').isNotNull().alias('has_taxi_id'),'landing_run_id','payload_sha256','extracted_at_utc')
    dims = {}
    for table, key_name, value_name, source in [
        ('dim_taxi','taxi_key','taxi_id','_taxi_id'),
        ('dim_company','company_key','company_name','_company_name'),
        ('dim_payment_type','payment_type_key','payment_type','_payment_type')]:
        dims[table] = valid.select(key_name,F.col(source).alias(value_name),F.col(source).isNull().alias('is_unknown')).distinct()
    dates = valid.select(F.col('start_date_key').alias('date_key'),F.to_date('trip_start_local').alias('calendar_date')).distinct()
    dims['dim_date'] = dates.select('*',F.year('calendar_date').alias('year'),F.month('calendar_date').alias('month'),
        F.dayofmonth('calendar_date').alias('day'),F.dayofweek('calendar_date').alias('day_of_week'))
    areas = valid.select(F.col('pickup_area_key').alias('area_key')).union(valid.select(F.col('dropoff_area_key').alias('area_key'))).distinct()
    if community_areas is None:
        area_names = areas.withColumn('area_name',F.lit(None).cast('string'))
    else:
        area_names = areas.join(community_areas.select('area_key','area_name'),'area_key','left')
        if area_names.filter((F.col('area_key')!=0) & F.col('area_name').isNull()).limit(1).count():
            raise ValueError('Community reference is incomplete for observed trips')
    dims['dim_community_area'] = area_names.select('area_key',F.when(F.col('area_key') != 0,F.col('area_key')).alias('area_id'),
        'area_name',(F.col('area_key') == 0).alias('is_unknown'))
    return dict(fact_trip=fact, quarantine_trip=rejected, **dims)


def validate_tables(tables, input_count):
    fact, rejected = tables['fact_trip'], tables['quarantine_trip']
    fact_count, rejected_count = fact.count(), rejected.count()
    if fact_count + rejected_count != input_count:
        raise ValueError('Silver counts do not reconcile to bronze')
    for name, column in KEYS.items():
        df = tables[name]
        if df.filter(F.col(column).isNull()).limit(1).count() or df.groupBy(column).count().filter('count > 1').limit(1).count():
            raise ValueError('Missing or duplicate silver key: '+name)
    for column, dim, target in [('taxi_key','dim_taxi','taxi_key'),('company_key','dim_company','company_key'),
        ('payment_type_key','dim_payment_type','payment_type_key'),('start_date_key','dim_date','date_key'),
        ('pickup_area_key','dim_community_area','area_key'),('dropoff_area_key','dim_community_area','area_key')]:
        if fact.join(tables[dim],fact[column] == tables[dim][target],'left_anti').limit(1).count():
            raise ValueError('Orphan fact key: '+column)
    return dict(input_rows=input_count, fact_rows=fact_count, quarantined_rows=rejected_count)


def load_warehouse(spark, catalog='chicago_taxi_dev'):
    import re
    import uuid
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',catalog):
        raise ValueError('Invalid catalog identifier')
    source = catalog+'.bronze.taxi_trips'
    version = int(spark.sql('DESCRIBE HISTORY '+source).select('version').orderBy(F.desc('version')).first()[0])
    # Pin the source snapshot for every action, including separate table writes.
    bronze = spark.sql(f'SELECT * FROM {source} VERSION AS OF {version}')
    from chicago_taxi.geography.storage import published_reference, record_geography
    reference, reference_version = published_reference(spark,catalog)
    tables = build_tables(bronze,reference)
    summary = validate_tables(tables,bronze.count())
    load_id = uuid.uuid4().hex
    # This is a full current-snapshot warehouse. Each table publishes atomically, but the group
    # is not one transaction. Block downstream readers while rebuilding; rerun repairs failures.
    for name in ['dim_taxi','dim_company','dim_payment_type','dim_date','dim_community_area','quarantine_trip','fact_trip']:
        target = catalog+'.silver.'+name
        spark.sql(f'CREATE TABLE IF NOT EXISTS {target} ({SCHEMAS[name]}) USING DELTA')
        tables[name].write.format('delta').mode('overwrite').saveAsTable(target)
    record_geography(spark,catalog,load_id,reference_version)
    audit = spark.createDataFrame([(load_id, version, summary['input_rows'],summary['fact_rows'],summary['quarantined_rows'])],
        'silver_load_id string, bronze_version long, input_rows long, fact_rows long, quarantined_rows long').withColumn('completed_at_utc',F.current_timestamp())
    audit.write.format('delta').mode('append').saveAsTable(catalog+'.silver.warehouse_loads')
    return dict(status='success',silver_load_id=load_id,bronze_version=version,**summary)
