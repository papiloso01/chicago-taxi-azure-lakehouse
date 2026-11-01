from unittest.mock import MagicMock, patch
import pytest
from chicago_taxi.geography.storage import reference_for_silver

def test_old_silver_job_without_reference_lineage_is_rejected():
    fake=MagicMock();fake.catalog.tableExists.side_effect=[False,True]
    with pytest.raises(ValueError,match='Rebuild silver'): reference_for_silver(fake,'chicago_taxi_dev','silver-1')


def test_gold_reads_the_reference_version_bound_to_silver():
    fake=MagicMock();fake.catalog.tableExists.return_value=True
    fake.table.return_value.filter.return_value.limit.return_value.collect.return_value=[type('Lineage',(),dict(reference_version=4))()]
    with patch('chicago_taxi.geography.storage.validate_reference',side_effect=lambda frame:frame), patch('chicago_taxi.geography.storage.F.col', return_value=MagicMock()):
        _,version=reference_for_silver(fake,'chicago_taxi_dev','silver-1')
    assert version==4
    assert 'VERSION AS OF 4' in fake.sql.call_args.args[0]
