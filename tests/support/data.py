"""Shared deterministic DataFrame builders; source examples live in fixtures/etl."""

import json
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from chicago_taxi.transformations.bronze.taxi_trips import SOURCE_FIELDS
from chicago_taxi.transformations.silver.contracts import SCHEMAS as SILVER


def build_silver_input(spark, row, updates=None):
    row = dict(row)
    row.update(updates or {})
    row["raw_payload"] = json.dumps(row)
    row["extracted_at_utc"] = datetime(2025, 1, 2)
    columns = ["trip_id"] + SOURCE_FIELDS + ["raw_payload", "landing_run_id", "payload_sha256"]
    schema = ", ".join(name + " string" for name in columns) + ", extracted_at_utc timestamp"
    return spark.createDataFrame(
        [tuple(row.get(name) for name in columns) + (row["extracted_at_utc"],)], schema
    )


def build_gold_inputs(spark, examples, empty=False):
    rows = []
    for example in examples:
        key, taxi, revenue, km, pickup, dropoff, day = [
            example[k] for k in ["trip_id", "taxi", "revenue", "km", "pickup", "dropoff", "day"]
        ]
        rows.append(
            dict(
                trip_id=key,
                taxi_key=taxi or "unknown",
                company_key="company",
                payment_type_key="card",
                start_date_key=20250100 + day,
                trip_start_local=datetime(2025, 1, day, 12),
                trip_total=Decimal(revenue) if revenue else None,
                trip_km=Decimal(km) if km else None,
                pickup_area_key=pickup,
                dropoff_area_key=dropoff,
                has_taxi_id=taxi is not None,
                has_trip_total=revenue is not None,
                has_distance=km is not None,
            )
        )
    result = dict(
        fact_trip=spark.createDataFrame([] if empty else rows, SILVER["fact_trip"]),
        quarantine_trip=spark.createDataFrame([], SILVER["quarantine_trip"]),
        dim_taxi=spark.createDataFrame(
            [("A", "taxi-A", False), ("B", "taxi-B", False), ("unknown", None, True)],
            SILVER["dim_taxi"],
        ),
        dim_company=spark.createDataFrame([("company", "COMPANY", False)], SILVER["dim_company"]),
        dim_payment_type=spark.createDataFrame(
            [("card", "CREDIT CARD", False)], SILVER["dim_payment_type"]
        ),
        dim_community_area=spark.createDataFrame(
            [(0, None, None, True), (8, 8, None, False), (9, 9, None, False)],
            SILVER["dim_community_area"],
        ),
    )
    return result


def community_rows(example):
    """Expand one geometry example only for tests of the 77-area domain rule."""
    rows = []
    for area in range(1, 78):
        row = deepcopy(example)
        row.update(area_num_1=str(area), area_numbe=str(area), community=f"AREA {area}")
        # Independent IDs/names suffice; geometry validity is tested separately.
        rows.append(row)
    return rows
