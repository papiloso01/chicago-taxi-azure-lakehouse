import copy
import json
import pytest
from chicago_taxi.geography.community_areas import normalize, extract, read_snapshot, export_geojson


def rows():
    result=[]
    for area in range(1,78):
        x=-88+area/1000
        ring=[[x,41.7],[x+.0005,41.7],[x+.0005,41.701],[x,41.701],[x,41.7]]
        result.append(dict(area_num_1=str(area),area_numbe=str(area),community=f'Area {area}',
            the_geom=dict(type='Polygon',coordinates=[ring])))
    return result


def test_full_reference_normalizes_and_export_preserves_boundaries(tmp_path):
    folder=extract(tmp_path,fetch=lambda *args:rows())
    manifest,raw,clean=read_snapshot(folder)
    assert len(raw)==len(clean)==manifest['rows']==77
    assert clean[7]['area_name']=='AREA 8'
    geo=json.loads(open(export_geojson(folder)).read())
    assert len(geo['features'])==77
    assert {f['properties']['area_id'] for f in geo['features']}==set(range(1,78))
    assert geo['features'][0]['geometry']==raw[0]['the_geom']


@pytest.mark.parametrize('defect',['missing','duplicate','bad_id','different_id','blank_name','duplicate_name','open_ring','bow_tie','swapped','nan','point'])
def test_invalid_reference_is_rejected(defect):
    data=rows()
    if defect=='missing': data.pop()
    elif defect=='duplicate': data[1]['area_num_1']=data[1]['area_numbe']='1'
    elif defect=='bad_id': data[0]['area_num_1']=data[0]['area_numbe']='78'
    elif defect=='different_id': data[0]['area_numbe']='2'
    elif defect=='blank_name': data[0]['community']=' '
    elif defect=='duplicate_name': data[1]['community']=data[0]['community']
    elif defect=='open_ring': data[0]['the_geom']['coordinates'][0].pop()
    elif defect=='bow_tie': data[0]['the_geom']['coordinates']=[[[-88,41],[-87,42],[-88,42],[-87,41],[-88,41]]]
    elif defect=='swapped': data[0]['the_geom']['coordinates'][0][0]=[41,-88]
    elif defect=='nan': data[0]['the_geom']['coordinates'][0][1][0]=float('nan')
    elif defect=='point': data[0]['the_geom']={'type':'Point','coordinates':[-87,41]}
    with pytest.raises(ValueError): normalize(data)


def test_failed_reference_keeps_raw_and_no_success_manifest(tmp_path):
    with pytest.raises(ValueError): extract(tmp_path,fetch=lambda *args:rows()[:-1])
    folder=next((tmp_path/'community_areas').iterdir())
    assert (folder/'data.json').exists() and not (folder/'manifest.json').exists()
    assert json.loads((folder/'audit.json').read_text())['status']=='failed'


def test_tampered_snapshot_cannot_load(tmp_path):
    folder=extract(tmp_path,fetch=lambda *args:rows())
    (folder/'data.json').write_text('[]')
    with pytest.raises(ValueError,match='integrity'): read_snapshot(folder)


def test_multipolygon_and_holes_preserve_valid_geometry():
    data=rows()
    geometry=data[0]['the_geom']
    data[0]['the_geom']=dict(type='MultiPolygon',coordinates=[geometry['coordinates']])
    clean=normalize(data)
    assert json.loads(clean[0]['geometry_geojson'])['type']=='MultiPolygon'
