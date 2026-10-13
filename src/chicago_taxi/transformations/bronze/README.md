# Bronze transformations

Implementation pending. Preserve payloads and source keys; merge repeated keys without duplicate trips.

Use PySpark DataFrame/SQL APIs supported by serverless Spark Connect. No custom SparkSession,
RDD APIs or DataFrame cache assumptions. Implement and test each layer before scheduling it.
