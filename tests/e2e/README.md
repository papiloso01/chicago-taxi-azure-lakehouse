# Offline end-to-end transformations

`test_landing_to_silver.py` exercises fixture API rows through real landing files, verified staging, bronze preparation and silver fact/dimension/quarantine DataFrames. It tests stable keys and reconciliation. It does not call Azure or write Delta tables; live acceptance is separate.
