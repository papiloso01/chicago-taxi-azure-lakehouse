# Databricks execution

Import `notebooks/test_adf_storage_connection.py` as a Python notebook. Create a saved job named
`job_test_storage_connection`, choose serverless task compute, set retries to zero for the initial
smoke test, then run. Record the saved Job ID in your local configuration, not a failed run ID.

ADF uses a Databricks **Job** activity with a **Serverless** linked service. It must not use the
old Notebook activity's new classic cluster configuration. The saved job's run-as identity needs
catalog/schema/volume privileges; the ADF identity needs job invocation permissions.

Owner-confirmed serverless smoke test and ADF invocation succeeded. This file records the tested
notebook; the saved live job has not been exported or deployed automatically by this repo.
Serverless compute and SQL warehouses have distinct capabilities. SQL warehouses cannot execute
this Python notebook. Do not pin classic Runtime settings onto serverless job environments.
