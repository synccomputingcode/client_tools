# Databricks notebook source
# MAGIC %md
# MAGIC ##Generate Sync Gradient Recommendation
# MAGIC This notebook reads the Spark event logs for a previously executed Databricks job and genereates a configuration recommendation to lower the cost of the job. The default setup assumes the following:
# MAGIC
# MAGIC * Eventlogs are automatically stored in s3 on AWS or DBFS
# MAGIC * Databricks, AWS, and Sync secrets are stored in Databricks Secrets
# MAGIC   * Databricks Personal Access Token - PAT
# MAGIC   * AWS Credentials - Access Key ID
# MAGIC   * AWS Credentials - Secret Access Key
# MAGIC   * Sync API token - Access Key Id"
# MAGIC   * Sync API token - Secret Access Key
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

# COMMAND ----------

dbutils.widgets.text("DATABRICKS_JOB_ID", "")
dbutils.widgets.text("DATABRICKS_RUN_ID", "")
dbutils.widgets.text("DATABRICKS_URL", "")
dbutils.widgets.text("DATABRICKS_COMPUTE_TYPE", "")
dbutils.widgets.text("DATABRICKS_PLAN_TYPE", "")
dbutils.widgets.text("SYNC_PROJECT_ID", "")

# COMMAND ----------

from sync import awsdatabricks
smoke_test = awsdatabricks.get_access_report()

for check in smoke_test:
    if check.status != "OK":
        raise Exception(check.name + ":" + check.status + ":" + check.message)

# COMMAND ----------


recommendation = awsdatabricks.record_run(dbutils.widgets.get("DATABRICKS_PARENT_RUN_ID"), dbutils.widgets.get("DATABRICKS_PLAN_TYPE"), dbutils.widgets.get("DATABRICKS_COMPUTE_TYPE"), project_id=dbutils.widgets.get("SYNC_PROJECT_ID"), exclude_tasks=[dbutils.widgets.get("DATABRICKS_TASK_KEY")])

print(recommendation)

# COMMAND ----------


