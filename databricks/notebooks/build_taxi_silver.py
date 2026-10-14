# Databricks notebook source
import json
from chicago_taxi.transformations.silver.taxi_trips import load_warehouse

# COMMAND ----------
dbutils.widgets.text('catalog', 'chicago_taxi_dev')

# COMMAND ----------
result = load_warehouse(spark, dbutils.widgets.get('catalog'))
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
