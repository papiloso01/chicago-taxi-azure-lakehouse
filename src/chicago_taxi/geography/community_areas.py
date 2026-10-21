"""Complete authoritative Chicago lookup; raw snapshots and strict geometry validation."""
import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from chicago_taxi.ingestion.taxi_api import request_rows

ENDPOINT='https://data.cityofchicago.org/resource/igwz-8jzy.json'
REFERENCE_SCHEMA='area_key INT, area_id INT, area_name STRING, geometry_geojson STRING, map_latitude DOUBLE, map_longitude DOUBLE, source_run_id STRING, source_sha256 STRING'
MAP_NAME='mart_area_map_daily'
MAP_KEYS=['trip_date','area_role','area_key']


def polygon(value):
    from shapely.geometry import shape
    if not isinstance(value,dict) or value.get('type') not in ('Polygon','MultiPolygon'):
        raise ValueError('Expected polygon or multipolygon geometry')
    polygons=[value.get('coordinates')] if value['type']=='Polygon' else value.get('coordinates')
    if not isinstance(polygons,list) or not polygons:
        raise ValueError('Empty polygon coordinates')
    for rings in polygons:
        if not isinstance(rings,list) or not rings: raise ValueError('Missing polygon rings')
        for ring in rings:
            if not isinstance(ring,list) or len(ring)<4 or ring[0]!=ring[-1]:
                raise ValueError('Boundary rings must be closed')
            for position in ring:
                if not isinstance(position,list) or len(position)!=2 or any(type(v) not in (int,float) or not math.isfinite(v) for v in position):
                    raise ValueError('Invalid WGS84 coordinates')
                lon,lat=position
                # Broad Chicago-region sanity bounds catch swapped axes or a wrong dataset.
                if not (-89 <= lon <= -86 and 40 <= lat <= 43):
                    raise ValueError('Coordinates outside Chicago region')
    geom=shape(value)
    if geom.is_empty or not geom.is_valid or geom.area<=0:
        raise ValueError('Invalid or empty boundary topology')
    return geom


def normalize(rows):
    if not isinstance(rows,list) or len(rows)!=77:
        raise ValueError('Expected exactly 77 community areas')
    output=[]
    for row in rows:
        if not isinstance(row,dict): raise ValueError('Expected community area records')
        raw_id=str(row.get('area_num_1','')).strip()
        if not re.fullmatch(r'\d+',raw_id): raise ValueError('Invalid community area ID')
        area=int(raw_id)
        if row.get('area_numbe') is not None:
            from decimal import Decimal
            if Decimal(str(row['area_numbe']))!=Decimal(area): raise ValueError('Conflicting area IDs')
        name=row.get('community')
        if not isinstance(name,str) or not name.strip(): raise ValueError('Missing area name')
        name=' '.join(name.upper().split())
        geom=polygon(row.get('the_geom'))
        point=geom.representative_point()
        output.append(dict(area_key=area,area_id=area,area_name=name,
            geometry_geojson=json.dumps(row['the_geom'],sort_keys=True,separators=(',',':'),allow_nan=False),
            map_latitude=point.y,map_longitude=point.x))
    if {r['area_id'] for r in output}!=set(range(1,78)) or len({r['area_name'] for r in output})!=77:
        raise ValueError('Area IDs/names must be unique and cover 1 through 77')
    return sorted(output,key=lambda row:row['area_id'])


def extract(root,fetch=None):
    root=Path(root)
    if not root.is_absolute(): raise ValueError('Expected absolute landing root')
    run_id=uuid.uuid4().hex
    folder=root/'community_areas'/('run_id='+run_id)
    folder.mkdir(parents=True,exist_ok=False)
    report=dict(source='community_areas',source_url=ENDPOINT,run_id=run_id,status='running',
        extracted_at_utc=datetime.now(timezone.utc).isoformat())
    audit=folder/'audit.json'
    try:
        rows=(fetch or request_rows)(ENDPOINT+'?$limit=1000&$order=area_num_1','')
        body=json.dumps(rows,ensure_ascii=False,allow_nan=False).encode('utf-8')
        (folder/'data.json').write_bytes(body)
        normalize(rows)
        report.update(status='success',rows=77,bytes=len(body),sha256=hashlib.sha256(body).hexdigest())
        audit.write_text(json.dumps(report,indent=2),encoding='utf-8')
        (folder/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        return folder
    except Exception as error:
        report.update(status='failed',error_type=type(error).__name__)
        audit.write_text(json.dumps(report,indent=2),encoding='utf-8')
        raise


def read_snapshot(folder):
    folder=Path(folder)
    manifest=json.loads((folder/'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('status')!='success' or manifest.get('source')!='community_areas' or manifest.get('rows')!=77:
        raise ValueError('Only complete successful community snapshots are loadable')
    run_id=manifest.get('run_id','')
    if not re.fullmatch('[0-9a-f]{32}',run_id) or folder.name!='run_id='+run_id:
        raise ValueError('Snapshot identity mismatch')
    body=(folder/'data.json').read_bytes()
    if len(body)!=manifest['bytes'] or hashlib.sha256(body).hexdigest()!=manifest['sha256']:
        raise ValueError('Snapshot integrity mismatch')
    rows=json.loads(body)
    return manifest,rows,normalize(rows)


def export_geojson(folder):
    manifest,_,rows=read_snapshot(folder)
    features=[dict(type='Feature',id=row['area_id'],properties=dict(area_id=row['area_id'],area_name=row['area_name'],
        reference_run_id=manifest['run_id']),geometry=json.loads(row['geometry_geojson'])) for row in rows]
    path=Path(folder)/'community_areas.geojson'
    path.write_text(json.dumps(dict(type='FeatureCollection',features=features),allow_nan=False),encoding='utf-8')
    return str(path)
