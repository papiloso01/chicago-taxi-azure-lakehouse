# Databricks notebook source
# Run using serverless notebook/job compute. Not a SQL-warehouse notebook.
import sys
import uuid

# COMMAND ----------
print("Python:", sys.version)
print("Spark:", spark.version)
print("Running as:", spark.sql("SELECT current_user()").first()[0])
spark.sql("USE CATALOG chicago_taxi_dev")
spark.sql("SHOW SCHEMAS").show()

# COMMAND ----------
test_path = (
    "/Volumes/chicago_taxi_dev/landing/raw_files/"
    f"_connection_tests/{uuid.uuid4().hex}"
)
try:
    # This is a fresh UUID path. errorifexists was rejected by the tested serverless client.
    spark.range(3).write.mode("overwrite").parquet(test_path)
    assert spark.read.parquet(test_path).count() == 3
    print("Storage write/read test passed")
finally:
    dbutils.fs.rm(test_path, True)

dbutils.notebook.exit("SUCCESS")
