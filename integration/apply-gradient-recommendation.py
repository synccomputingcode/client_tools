# Databricks notebook source
# MAGIC %md
# MAGIC ## Install Sync Python SDK Library

# COMMAND ----------

# MAGIC %pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Databricks Python SDK Library

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Credentials and Environment Variables

# COMMAND ----------

# Collect cluster name
dbutils.widgets.text("db_workspace_url", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("prediction_id", "")

# COMMAND ----------

import os
os.environ["DATABRICKS_HOST"] = dbutils.widgets.get("db_workspace_url")
os.environ["SYNC_PREDICTION_ID"] = dbutils.jobs.taskValues.get(taskKey = "get_sync_recommendation", key = "prediction_id")
os.environ["DATABRICKS_JOB_ID"] = dbutils.widgets.get("job_id")

os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get(scope = "demo_workflow", key = "db_token")
os.environ["SYNC_API_KEY_ID"] = dbutils.secrets.get(scope = "demo_workflow", key = "sync_access_key_id")
os.environ["SYNC_API_KEY_SECRET"] = dbutils.secrets.get(scope = "demo_workflow", key = "sync_secret_access_key")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Retrieving Recommendation from Gradient

# COMMAND ----------

import sync
from sync.api.predictions import *

recommendation = sync.api.predictions.get_prediction(os.getenv("SYNC_PREDICTION_ID"))
recommended_config = recommendation.result.get('solutions').get('recommended')
display(recommended_config)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Retrieve Job Cluster Definition from Databricks

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
)

job = w.jobs.get(os.getenv("DATABRICKS_JOB_ID"))

#Current Configuration
print("Name:" + job.settings.job_clusters[0].job_cluster_key)
print("Driver:" + job.settings.job_clusters[0].new_cluster.driver_node_type_id)
print("Worker:" + job.settings.job_clusters[0].new_cluster.node_type_id)
print(job.settings.job_clusters[0].new_cluster.num_workers)
print(job.settings.job_clusters[0].new_cluster.autoscale)
print(job.settings.job_clusters[0].new_cluster.spark_conf)

# COMMAND ----------

from databricks.sdk.service.compute import AutoScale

job.settings.job_clusters[0].new_cluster.driver_node_type_id = recommended_config.get('configuration').get('driver_node_type_id')
job.settings.job_clusters[0].new_cluster.node_type_id = recommended_config.get('configuration').get('node_type_id')
if job.settings.job_clusters[0].new_cluster.num_workers is not None:
    job.settings.job_clusters[0].new_cluster.num_workers = recommended_config.get('configuration').get('num_workers')
if job.settings.job_clusters[0].new_cluster.autoscale is not None:
    job.settings.job_clusters[0].new_cluster.autoscale = AutoScale.from_dict(recommended_config.get('configuration').get('autoscale'))
job.settings.job_clusters[0].new_cluster.spark_conf = recommended_config.get('configuration').get('spark_conf')


# COMMAND ----------

#Updated Configuration
print("Name:" + job.settings.job_clusters[0].job_cluster_key)
print("Driver:" + job.settings.job_clusters[0].new_cluster.driver_node_type_id)
print("Worker:" + job.settings.job_clusters[0].new_cluster.node_type_id)
print(job.settings.job_clusters[0].new_cluster.num_workers)
print(job.settings.job_clusters[0].new_cluster.autoscale)
print(job.settings.job_clusters[0].new_cluster.spark_conf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save Updated Cluster Configuration in Databricks

# COMMAND ----------

w.jobs.update(job_id=os.getenv("DATABRICKS_JOB_ID"),new_settings=job.settings)
