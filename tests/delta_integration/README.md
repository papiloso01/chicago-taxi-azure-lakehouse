# Local Delta storage integration

See [ETL validation](../etl_validation/README.md) for setup, isolation and coverage boundaries.

Each parametrized test launches an independent Python/Spark subprocess with a fresh warehouse and checksum-pinned Delta JARs. This avoids reusing a Spark JVM that was started without Delta extensions. A failed assertion or subprocess timeout fails CI and includes captured logs.

Eight independently named cases cover replay, newer records, stale records, equal-time tie-breaking, historical reads, silver overwrite, gold overwrite and landing/audit replay. Each starts from its own baseline, so a failure does not suppress unrelated cases. Subprocesses run under coverage measurement and write separate data files for later combination.
