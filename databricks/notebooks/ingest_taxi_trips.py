# Databricks notebook source
# Install this repository as an absolute workspace-path project dependency in the
# serverless notebook Environment panel before creating the saved ingestion job.
from datetime import date
import json
from chicago_taxi.ingestion.windows import daily_window, year_window
from chicago_taxi.ingestion.landing import ingest

# COMMAND ----------
dbutils.widgets.dropdown("mode", "daily", ["daily", "explicit", "backfill"])
dbutils.widgets.text("start", "")
dbutils.widgets.text("end", "")
dbutils.widgets.text("year", "2025")
dbutils.widgets.text("page_size", "5000")
dbutils.widgets.text("landing_root", "/Volumes/chicago_taxi_dev/landing/raw_files")

# COMMAND ----------
mode = dbutils.widgets.get("mode")
if mode not in {"daily", "explicit", "backfill"}:
    raise ValueError("Unknown ingestion mode")
if mode == "daily":
    start, end = daily_window()
elif mode == "backfill":
    start, end = year_window(int(dbutils.widgets.get("year")))
else:
    start, end = dbutils.widgets.get("start"), dbutils.widgets.get("end")
    if not start or not end or date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("Explicit mode requires start < exclusive end")

landing_root = dbutils.widgets.get("landing_root")
if not landing_root.startswith("/Volumes/"):
    raise ValueError("Cloud landing_root must be an absolute Unity Catalog volume path")
report = ingest(landing_root, start, end,
                page_size=int(dbutils.widgets.get("page_size")))
# Anonymous public API initially. Add a Key Vault-backed token only after secret access is configured.
print(json.dumps({key: report[key] for key in ["run_id", "status", "extracted_rows", "start", "end_exclusive"]}))
dbutils.notebook.exit(json.dumps({key: report[key] for key in ["run_id", "status", "extracted_rows"]}))
