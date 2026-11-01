"""Actual local Delta operations in a separate JVM from transformation tests."""
from pathlib import Path
import subprocess
import sys
import pytest


@pytest.mark.parametrize('case', ['merge_ordering', 'landing_replay'])
def test_delta_storage(case, tmp_path, fixtures_dir, pytestconfig):
    jars = Path(pytestconfig.getoption('--delta-jars-dir')).resolve()
    for name in ['delta-spark_2.12', 'delta-storage']:
        if not (jars / f'{name}-3.3.2.jar').is_file():
            pytest.fail('Missing Delta JARs; run python tools/download_delta.py first')
    result = subprocess.run([sys.executable, '-m', 'tests.delta_integration.run_storage_checks',
        case, str(tmp_path), str(jars), str(fixtures_dir)],
        capture_output=True, text=True, timeout=240, cwd=Path(__file__).resolve().parents[2])
    assert result.returncode == 0, result.stdout + result.stderr
    assert f'PASSED {case}' in result.stdout
