Review changes against docs/architecture.md and docs/roadmap.md. This is a foundation, not a
completed ETL system. Do not mistake placeholder documentation for executable pipelines/tests.
Preserve Chicago end-exclusive dates, source-key replay, trip grain and revenue reconciliation.
Serverless uses Spark Connect; avoid unsupported classic-session/RDD/cache assumptions.
Do not infer driver identities or passenger gender. Retain unknown weather/geography.
No credentials in source; no cloud provisioning or paid model calls without explicit deployment
configuration. MySQL cloud provisioning is blocked. Localhost MySQL is not reachable by Azure.
