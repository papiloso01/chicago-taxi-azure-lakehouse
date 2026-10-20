"""Dashboard mart grains and exact output schemas."""
METRICS = '''trip_count BIGINT, revenue_trip_count BIGINT, known_revenue_usd DECIMAL(28,2),
distance_trip_count BIGINT, identified_taxi_trip_count BIGINT, total_km DECIMAL(28,6), avg_known_trip_total_usd DECIMAL(22,6),
avg_known_trip_km DECIMAL(22,6), shortest_positive_trip_km DECIMAL(18,6), longest_trip_km DECIMAL(18,6),
revenue_coverage DOUBLE, distance_coverage DOUBLE, taxi_identity_coverage DOUBLE'''
SCHEMAS = {
 'mart_daily_summary': 'trip_date DATE, '+METRICS+', gold_load_id STRING',
 'mart_taxi_revenue_daily': 'trip_date DATE, taxi_key STRING, taxi_id STRING, '+METRICS+', revenue_rank BIGINT, gold_load_id STRING',
 'mart_area_demand_daily': 'trip_date DATE, area_role STRING, area_key INT, area_id INT, area_name STRING, is_unknown BOOLEAN, '+METRICS+', gold_load_id STRING',
 'mart_company_payment_daily': 'trip_date DATE, company_key STRING, company_name STRING, payment_type_key STRING, payment_type STRING, '+METRICS+', gold_load_id STRING',
 'mart_distance_extremes': '''trip_date DATE, extreme_type STRING, trip_id STRING, taxi_id STRING,
trip_km DECIMAL(18,6), trip_total DECIMAL(18,2), pickup_area_key INT, dropoff_area_key INT, gold_load_id STRING''',
}
KEYS = {
 'mart_daily_summary': ['trip_date'],
 'mart_taxi_revenue_daily': ['trip_date','taxi_key'],
 'mart_area_demand_daily': ['trip_date','area_role','area_key'],
 'mart_company_payment_daily': ['trip_date','company_key','payment_type_key'],
 'mart_distance_extremes': ['trip_date','extreme_type','trip_id'],
}
