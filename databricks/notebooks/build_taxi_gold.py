# Databricks notebook source
import json
from chicago_taxi.transformations.gold.taxi_marts import load_gold

# COMMAND ----------
dbutils.widgets.text('catalog', 'chicago_taxi_dev')

# COMMAND ----------
result = load_gold(spark,dbutils.widgets.get('catalog'))
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
