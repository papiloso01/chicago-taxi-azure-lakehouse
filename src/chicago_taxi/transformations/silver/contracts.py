"""Silver warehouse schema and grain contracts."""
FACT_SCHEMA = '''trip_id STRING, taxi_key STRING, company_key STRING, payment_type_key STRING,
start_date_key INT, pickup_area_key INT, dropoff_area_key INT,
trip_start_local TIMESTAMP_NTZ, trip_end_local TIMESTAMP_NTZ,
trip_seconds BIGINT, trip_miles DECIMAL(18,6), trip_km DECIMAL(18,6),
fare DECIMAL(18,2), tips DECIMAL(18,2), tolls DECIMAL(18,2), extras DECIMAL(18,2), trip_total DECIMAL(18,2),
has_distance BOOLEAN, has_trip_total BOOLEAN, has_taxi_id BOOLEAN,
landing_run_id STRING, payload_sha256 STRING, extracted_at_utc TIMESTAMP'''
SCHEMAS = {
    'fact_trip': FACT_SCHEMA,
    'dim_taxi': 'taxi_key STRING, taxi_id STRING, is_unknown BOOLEAN',
    'dim_company': 'company_key STRING, company_name STRING, is_unknown BOOLEAN',
    'dim_payment_type': 'payment_type_key STRING, payment_type STRING, is_unknown BOOLEAN',
    'dim_date': 'date_key INT, calendar_date DATE, year INT, month INT, day INT, day_of_week INT',
    'dim_community_area': 'area_key INT, area_id INT, area_name STRING, is_unknown BOOLEAN',
    'quarantine_trip': 'trip_id STRING, raw_payload STRING, quality_issues ARRAY<STRING>, landing_run_id STRING, payload_sha256 STRING, extracted_at_utc TIMESTAMP',
}
KEYS = dict(fact_trip='trip_id', dim_taxi='taxi_key', dim_company='company_key',
            dim_payment_type='payment_type_key', dim_date='date_key', dim_community_area='area_key')
