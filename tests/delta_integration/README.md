# Local Delta storage integration

See [ETL validation](../etl_validation/README.md) for setup, isolation and coverage boundaries.

Each parametrized test launches an independent Python/Spark subprocess with a fresh warehouse and checksum-pinned Delta JARs. This avoids reusing a Spark JVM that was started without Delta extensions. A failed assertion or subprocess timeout fails CI and includes captured logs.
