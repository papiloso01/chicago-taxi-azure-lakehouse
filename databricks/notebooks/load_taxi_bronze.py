# Databricks notebook source
import json
from chicago_taxi.transformations.bronze.taxi_trips import load_run

# COMMAND ----------
dbutils.widgets.text("landing_run_id", "")
dbutils.widgets.text("landing_root", "/Volumes/chicago_taxi_dev/landing/raw_files")

# COMMAND ----------
import re
run_id = dbutils.widgets.get("landing_run_id")
root = dbutils.widgets.get("landing_root").rstrip("/")
if not re.fullmatch(r"[0-9a-f]{32}", run_id) or not root.startswith("/Volumes/"):
    raise ValueError("Set a valid landing_run_id and absolute volume root")
result = load_run(spark, root + "/taxi_trips/run_id=" + run_id)
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
