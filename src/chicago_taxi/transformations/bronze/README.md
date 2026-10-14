# Bronze

`landing_manifest.py` verifies complete run integrity and prepares bounded page staging.
`taxi_trips.py` defines the Spark schema, retains raw source values and performs replay-safe Delta merges.
See `docs/bronze_loading.md` for the database contract and acceptance steps.
