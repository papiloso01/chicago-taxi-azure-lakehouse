# ETL validation

`test_bronze_spark.py` exercises real local Spark plans with Java 17 and the `spark-test` extra. It checks explicit schema, raw values, duplicate handling and empty data. Delta merge and Unity Catalog are tested manually in `docs/bronze_loading.md`; CI does not claim those cloud checks passed.
