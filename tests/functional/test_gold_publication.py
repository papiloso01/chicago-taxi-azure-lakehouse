"""Publication orchestration unit tests; no Spark JVM or Delta writes."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pytest
from chicago_taxi.transformations.silver.contracts import SCHEMAS as SILVER
from chicago_taxi.transformations.gold.taxi_marts import snapshot_silver, load_gold


@pytest.fixture(autouse=True)
def spark_expressions():
    # These tests exercise branching/query construction, not Spark expressions.
    with patch(
        "chicago_taxi.transformations.gold.taxi_marts.F.desc", side_effect=lambda value: value
    ):
        yield


def mock_snapshot(changed=False):
    spark = MagicMock()
    audit = dict(
        silver_load_id="silver-1",
        completed_at_utc=datetime(2025, 1, 2),
        input_rows=5,
        fact_rows=5,
        quarantined_rows=0,
    )
    spark.table.return_value.orderBy.return_value.first.side_effect = [audit, audit]
    timestamp = audit["completed_at_utc"] + timedelta(seconds=1 if changed else -1)

    def query(sql):
        frame = MagicMock()
        if sql.startswith("DESCRIBE HISTORY "):
            name = sql.rsplit(".", 1)[-1]
            version = list(SILVER).index(name) + 7
            frame.select.return_value.orderBy.return_value.first.return_value = dict(
                version=version, timestamp=timestamp
            )
        return frame

    spark.sql.side_effect = query
    return spark, audit


def test_snapshot_pins_versions_and_audit_identity():
    spark, audit = mock_snapshot()
    with patch(
        "chicago_taxi.transformations.gold.taxi_marts.validate_silver",
        return_value=dict(fact_rows=5, quarantined_rows=0),
    ):
        frames, versions, load_id = snapshot_silver(spark, "chicago_taxi_dev")
    assert load_id == audit["silver_load_id"]
    assert set(frames) == set(SILVER)
    assert versions == {name: index + 7 for index, name in enumerate(SILVER)}
    queries = [call.args[0] for call in spark.sql.call_args_list]
    assert {q for q in queries if q.startswith("DESCRIBE HISTORY ")} == {
        "DESCRIBE HISTORY chicago_taxi_dev.silver." + name for name in SILVER
    }
    assert {q for q in queries if q.startswith("SELECT * FROM ")} == {
        f"SELECT * FROM chicago_taxi_dev.silver.{name} VERSION AS OF {index + 7}"
        for index, name in enumerate(SILVER)
    }
    assert len(queries) == 2 * len(SILVER)


def test_unpublished_silver_changes_block_gold_writes():
    spark, _ = mock_snapshot(changed=True)
    with pytest.raises(ValueError, match="completed audit"):
        load_gold(spark)
    assert all(call.args[0].startswith("DESCRIBE HISTORY ") for call in spark.sql.call_args_list)
    spark.createDataFrame.assert_not_called()


def test_catalog_identifier_is_validated():
    with pytest.raises(ValueError):
        load_gold(None, "a; DROP TABLE b")


def test_missing_silver_publication_blocks_gold():
    fake = MagicMock()
    fake.table.return_value.orderBy.return_value.first.return_value = None
    with pytest.raises(ValueError, match="completed silver"):
        load_gold(fake)
    fake.sql.assert_not_called()


def test_new_silver_publication_during_capture_is_rejected():
    fake, audit = mock_snapshot()
    newer = dict(audit, silver_load_id="silver-2")
    fake.table.return_value.orderBy.return_value.first.side_effect = [audit, newer]
    with pytest.raises(ValueError, match="changed while pinning"):
        snapshot_silver(fake, "chicago_taxi_dev")


def test_silver_audit_count_mismatch_blocks_gold():
    fake, _ = mock_snapshot()
    with patch(
        "chicago_taxi.transformations.gold.taxi_marts.validate_silver",
        return_value=dict(fact_rows=4, quarantined_rows=1),
    ):
        with pytest.raises(ValueError, match="audit counts"):
            snapshot_silver(fake, "chicago_taxi_dev")
