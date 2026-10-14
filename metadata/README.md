# Metadata

Source and metric rules are initial definitions, not evidence of executed ingestion.
Add per-table contracts (grain, columns/types, keys, required fields, partitions, units, lineage)
alongside the actual transformations. Validate those contracts before publication.

`silver_tables.yml` records warehouse grains, keys, explicit SQL schemas, foreign-key mappings, local-time semantics and publication status. `tools/validate_metadata.py` checks table/key/reference integrity alongside source and reporting rules.
