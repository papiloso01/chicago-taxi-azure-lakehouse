import hashlib
import json
import pytest
from tests.support.data import build_silver_input, build_gold_inputs, community_rows


@pytest.fixture
def load_example(fixtures_dir):
    def load(name):
        return json.loads((fixtures_dir / "etl" / name).read_text(encoding="utf-8"))

    return load


@pytest.fixture
def bronze_record(load_example):
    def record(key="a", fare="12.50", file="page1"):
        row = load_example("bronze_record.json")
        row.update(trip_id=key, fare=fare)
        raw = json.dumps(row)
        return (
            key,
            raw,
            hashlib.sha256(raw.encode()).hexdigest(),
            "a" * 32,
            file,
            "2025-01-02T00:00:00+00:00",
        )

    return record


@pytest.fixture
def silver_source(load_example):
    return lambda spark, updates=None: build_silver_input(
        spark, load_example("silver_source.json"), updates
    )


@pytest.fixture
def gold_fixture(load_example):
    return lambda spark, empty=False: build_gold_inputs(
        spark, load_example("gold_trips.json"), empty
    )


@pytest.fixture
def community_examples(fixtures_dir):
    example = json.loads(
        (
            fixtures_dir / "wiremock/__files/community_areas/community_areas_response.json"
        ).read_text()
    )[0]
    return community_rows(example)


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def spark(spark_session):
    # Each test has fresh SQL settings and temp views while reusing one JVM.
    session = spark_session.newSession()
    session.conf.set("spark.sql.ansi.enabled", "true")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.catalog.clearCache()
    for table in session.catalog.listTables():
        if table.isTemporary:
            session.catalog.dropTempView(table.name)
