# Databricks notebook source
import json
import re
from pathlib import Path
from chicago_taxi.geography.community_areas import extract, export_geojson
from chicago_taxi.geography.storage import load_reference

# COMMAND ----------
dbutils.widgets.text('landing_root','/Volumes/chicago_taxi_dev/landing/raw_files')
dbutils.widgets.text('reference_run_id','')
dbutils.widgets.text('catalog','chicago_taxi_dev')

# COMMAND ----------
root=dbutils.widgets.get('landing_root').rstrip('/')
run_id=dbutils.widgets.get('reference_run_id')
if not root.startswith('/Volumes/') or (run_id and not re.fullmatch('[0-9a-f]{32}',run_id)):
    raise ValueError('Expected volume root and optional valid snapshot run ID')
folder=Path(root)/'community_areas'/('run_id='+run_id) if run_id else extract(root)
report=load_reference(spark,folder,dbutils.widgets.get('catalog'))
report['geojson_path']=export_geojson(folder)
print(json.dumps(report))
dbutils.notebook.exit(json.dumps(report))
