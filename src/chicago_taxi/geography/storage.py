"""Small reference snapshots in Delta, pinned separately from the trip warehouse."""
import hashlib
import json
import re
import math
from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F
from chicago_taxi.geography.community_areas import REFERENCE_SCHEMA, read_snapshot, polygon


def identifier(catalog):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',catalog): raise ValueError('Invalid catalog identifier')
    return catalog


def validate_reference(frame,run_id=None,sha=None):
    from shapely.geometry import Point
    rows=frame.limit(78).collect()  # Exactly 77 small reference features, never collect trip facts.
    if len(rows)!=77 or {row.area_key for row in rows}!=set(range(1,78)):
        raise ValueError('Reference must contain unique IDs 1 through 77')
    names=[]
    for row in rows:
        if row.area_id!=row.area_key or not row.area_name or not row.area_name.strip(): raise ValueError('Invalid reference name/key')
        names.append(row.area_name)
        geom=polygon(json.loads(row.geometry_geojson))
        if row.map_latitude is None or row.map_longitude is None or not math.isfinite(row.map_latitude) or not math.isfinite(row.map_longitude) or not geom.covers(Point(row.map_longitude,row.map_latitude)):
            raise ValueError('Map point must lie within its boundary')
        if (run_id is not None and row.source_run_id!=run_id) or (sha is not None and row.source_sha256!=sha):
            raise ValueError('Reference provenance mismatch')
    if len(set(names))!=77: raise ValueError('Duplicate community names')
    return frame


def load_reference(spark,folder,catalog='chicago_taxi_dev'):
    catalog=identifier(catalog)
    manifest,raw,clean=read_snapshot(folder)
    run_id,sha=manifest['run_id'],manifest['sha256']
    rows=[dict(row,source_run_id=run_id,source_sha256=sha) for row in clean]
    frame=validate_reference(spark.createDataFrame(rows,REFERENCE_SCHEMA),run_id,sha)
    extracted=datetime.fromisoformat(manifest['extracted_at_utc']).astimezone(timezone.utc).replace(tzinfo=None)
    audit_table=catalog+'.silver.community_area_loads'
    if spark.catalog.tableExists(audit_table):
        previous=spark.table(audit_table).orderBy(F.desc('completed_at_utc')).first()
        if previous is not None:
            if previous.source_run_id==run_id:
                if previous.source_sha256!=sha: raise ValueError('Previously published snapshot hash changed')
                return dict(status='success',source_run_id=run_id,reference_rows=77,reference_version=previous.reference_version,replayed=True)
            if previous.source_extracted_at_utc>extracted:
                raise ValueError('Older reference snapshots cannot replace a newer publication')
    bronze_rows=[(run_id,int(row['area_num_1']),json.dumps(row,sort_keys=True,allow_nan=False),
        hashlib.sha256(json.dumps(row,sort_keys=True,allow_nan=False).encode()).hexdigest()) for row in raw]
    source=spark.createDataFrame(bronze_rows,'source_run_id string, area_id int, raw_payload string, payload_sha256 string')
    view='community_stage_'+uuid.uuid4().hex
    source.createOrReplaceTempView(view)
    try:
        spark.sql(f'CREATE TABLE IF NOT EXISTS {catalog}.bronze.community_area_snapshots (source_run_id STRING, area_id INT, raw_payload STRING, payload_sha256 STRING) USING DELTA')
        spark.sql(f'''MERGE INTO {catalog}.bronze.community_area_snapshots t USING {view} s
        ON t.source_run_id=s.source_run_id AND t.area_id=s.area_id
        WHEN NOT MATCHED THEN INSERT *''')
        target=catalog+'.silver.ref_community_area'
        spark.sql(f'CREATE TABLE IF NOT EXISTS {target} ({REFERENCE_SCHEMA}) USING DELTA')
        frame.write.format('delta').mode('overwrite').saveAsTable(target)
        version=int(spark.sql('DESCRIBE HISTORY '+target).select('version').orderBy(F.desc('version')).first()[0])
        audit=spark.createDataFrame([(run_id,sha,version,77,extracted)],'source_run_id string, source_sha256 string, reference_version long, reference_rows long, source_extracted_at_utc timestamp').withColumn('completed_at_utc',F.current_timestamp())
        audit.write.format('delta').mode('append').saveAsTable(catalog+'.silver.community_area_loads')
        return dict(status='success',source_run_id=run_id,reference_rows=77,reference_version=version)
    finally:
        spark.catalog.dropTempView(view)


def published_reference(spark,catalog):
    catalog=identifier(catalog)
    audit_table=catalog+'.silver.community_area_loads'
    if not spark.catalog.tableExists(audit_table):
        if spark.catalog.tableExists(catalog+'.silver.ref_community_area'):
            raise ValueError('Reference has no completed audit; recover reference loading')
        return None,None
    audit=spark.table(audit_table).orderBy(F.desc('completed_at_utc')).first()
    if audit is None or audit.reference_rows!=77: raise ValueError('Missing complete community reference audit')
    version=int(audit.reference_version)
    frame=spark.sql(f'SELECT * FROM {catalog}.silver.ref_community_area VERSION AS OF {version}')
    return validate_reference(frame,audit.source_run_id,audit.source_sha256),version


def record_geography(spark,catalog,silver_load_id,version):
    frame=spark.createDataFrame([(silver_load_id,version)],'silver_load_id string, reference_version long')
    frame.write.format('delta').mode('append').saveAsTable(identifier(catalog)+'.silver.warehouse_geography')


def reference_for_silver(spark,catalog,silver_load_id):
    catalog=identifier(catalog)
    lineage=catalog+'.silver.warehouse_geography'
    if not spark.catalog.tableExists(lineage):
        if spark.catalog.tableExists(catalog+'.silver.community_area_loads'):
            raise ValueError('Rebuild silver with geography-aware package before gold')
        return None,None
    rows=spark.table(lineage).filter(F.col('silver_load_id')==silver_load_id).limit(2).collect()
    if len(rows)!=1 or rows[0].reference_version is None:
        if spark.catalog.tableExists(catalog+'.silver.community_area_loads'):
            raise ValueError('Silver publication lacks reference lineage; rebuild silver')
        return None,None
    version=int(rows[0].reference_version)
    frame=spark.sql(f'SELECT * FROM {catalog}.silver.ref_community_area VERSION AS OF {version}')
    return validate_reference(frame),version
