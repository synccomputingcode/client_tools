# Databricks notebook source
# MAGIC %md
# MAGIC ##Generate Sync Gradient Recommendation
# MAGIC This notebook reads the Spark event logs for a previously executed Databricks job and genereates a configuration recommendation to lower the cost of the job. The default setup assumes the following:
# MAGIC
# MAGIC * Eventlogs are automatically stored in S3 or DBFS
# MAGIC * Databricks, and Sync secrets are stored in Databricks Secrets and provided to the cluster via environment variables
# MAGIC   * DATABRICKS_HOST: Databricks host
# MAGIC   * DATABRICKS_TOKEN: Databricks personal access token
# MAGIC   * SYNC_API_KEY_ID: Sync API key ID
# MAGIC   * SYNC_API_KEY_SECRET: Sync API key secret
# MAGIC * Access to AWS for event logs in S3 and cluster node information is provided by an instance profile or other AWS credentials (AWS Databricks only)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

# COMMAND ----------

dbutils.widgets.text("DATABRICKS_RUN_ID", "")
dbutils.widgets.text("DATABRICKS_JOB_ID", "")
dbutils.widgets.text("DATABRICKS_COMPUTE_TYPE", "")
dbutils.widgets.text("DATABRICKS_PLAN_TYPE", "")
dbutils.widgets.text("SYNC_PROJECT_ID", "")
dbutils.widgets.text("DATABRICKS_TASK_KEY", "")

# COMMAND ----------

import logging

from sync.clients.databricks import get_default_client
from sync.models import Platform, AccessStatusCode
from sync.api import projects

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

platform = get_default_client().get_platform()
if platform is Platform.AWS_DATABRICKS:
    from sync import awsdatabricks as databricks
elif platform is Platform.AZURE_DATABRICKS:
    from sync import azuredatabricks as databricks
else:
    raise ValueError(f"Unsupported platform: {platform}")


access_report = databricks.get_access_report()

for line in access_report:
    print(line)

assert not any(line.status is AccessStatusCode.RED for line in access_report), "Required access is missing"

# COMMAND ----------

response = databricks.create_submission_for_run(
        run_id=dbutils.widgets.get("DATABRICKS_RUN_ID") or dbutils.widgets.get("DATABRICKS_PARENT_RUN_ID"),
        plan_type=dbutils.widgets.get("DATABRICKS_PLAN_TYPE"),
        compute_type=dbutils.widgets.get("DATABRICKS_COMPUTE_TYPE"),
        project_id=dbutils.widgets.get("SYNC_PROJECT_ID"),
        exclude_tasks=([dbutils.widgets.get("DATABRICKS_TASK_KEY")] if dbutils.widgets.get("DATABRICKS_TASK_KEY") else None),
        allow_incomplete_cluster_report=True,
    )

if response.error:
    raise RuntimeError(str(response.error))

print(response.result)

response = projects.get_project(dbutils.widgets.get("SYNC_PROJECT_ID"))

if response.error:
    raise RuntimeError(str(response.error))

project = response.result
if project.get("auto_apply_recs"):
    response = projects.create_project_recommendation(project["id"])

    if response.error:
        raise RuntimeError(str(response.error))

    recommendation_id = response.result

    response = projects.wait_for_recommendation(project["id"], recommendation_id)

    if response.error:
        raise RuntimeError(str(response.error))

    response = databricks.apply_project_recommendation(dbutils.widgets.get("DATABRICKS_JOB_ID"), project["id"], recommendation_id)

    if response.error:
        raise RuntimeError(str(response.error))

print(response.result)

# COMMAND ----------
