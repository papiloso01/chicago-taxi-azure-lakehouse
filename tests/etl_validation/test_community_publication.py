from datetime import datetime,timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
import pytest
from chicago_taxi.geography.community_areas import extract,read_snapshot
from chicago_taxi.geography.storage import load_reference


def snapshot(tmp_path):
    data=[]
    for area in range(1,78):
        x=-88+area/1000
        data.append(dict(area_num_1=str(area),community=f'AREA {area}',the_geom=dict(type='Polygon',
            coordinates=[[[x,41.7],[x+.0005,41.7],[x+.0005,41.701],[x,41.701],[x,41.7]]])))
    folder=extract(tmp_path,fetch=lambda *args:data)
    return folder,read_snapshot(folder)[0]


def test_same_snapshot_replay_does_not_rewrite_reference(tmp_path):
    folder,manifest=snapshot(tmp_path)
    fake=MagicMock();fake.catalog.tableExists.return_value=True
    fake.table.return_value.orderBy.return_value.first.return_value=SimpleNamespace(source_run_id=manifest['run_id'],source_sha256=manifest['sha256'],reference_version=3)
    with patch('chicago_taxi.geography.storage.validate_reference',side_effect=lambda frame,*args:frame),patch('chicago_taxi.geography.storage.F.desc',side_effect=lambda value:value):
        result=load_reference(fake,folder)
    assert result['replayed'] and result['reference_version']==3
    fake.sql.assert_not_called()
    fake.createDataFrame.return_value.write.format.assert_not_called()


def test_older_snapshot_cannot_roll_back_a_newer_reference(tmp_path):
    folder,manifest=snapshot(tmp_path)
    fake=MagicMock();fake.catalog.tableExists.return_value=True
    newer=datetime.fromisoformat(manifest['extracted_at_utc']).replace(tzinfo=None)+timedelta(days=1)
    fake.table.return_value.orderBy.return_value.first.return_value=SimpleNamespace(source_run_id='e'*32,source_extracted_at_utc=newer)
    with patch('chicago_taxi.geography.storage.validate_reference',side_effect=lambda frame,*args:frame),patch('chicago_taxi.geography.storage.F.desc',side_effect=lambda value:value):
        with pytest.raises(ValueError,match='Older reference'): load_reference(fake,folder)
    fake.sql.assert_not_called()
