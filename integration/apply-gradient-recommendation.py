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
# MAGIC This uses the secrets setup in the Gradient Auto Setup notebook.

# COMMAND ----------

import os

## If you used the Gradient setup notebook, feel free to copy and paste values here
db_workspace_url='*** Update with your value ***'
job_id='*** Update with your value ***'
sync_project_id='*** Update with your value ***'
sync_access_key_id='*** Update with your value ***'
sync_secret_access_key='*** Update with your value ***'
db_token='*** Update with your value ***'

os.environ["DATABRICKS_TOKEN"] = db_token
os.environ["SYNC_API_KEY_ID"] = sync_access_key_id
os.environ["SYNC_API_KEY_SECRET"] = sync_secret_access_key
os.environ["DATABRICKS_HOST"] = db_workspace_url
os.environ["SYNC_PROJECT_ID"] = sync_project_id
os.environ["DATABRICKS_JOB_ID"] = job_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Retrieving Recommendation from Gradient

# COMMAND ----------

import sync
from sync.api.predictions import *

recommendations = sync.api.predictions.get_predictions(project_id=os.getenv("SYNC_PROJECT_ID"))
recommendation = sync.api.predictions.get_prediction(recommendations.result[-1]["prediction_id"])
recommended_config = recommendation.result.get('solutions').get('recommended')
display(recommended_config)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Retrieve Job Cluster Definition from Databricks

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient(
  host  = os.getenv("DATABRICKS_HOST"),
  token = os.getenv("DATABRICKS_TOKEN")
)

job = w.jobs.get(os.getenv("DATABRICKS_JOB_ID"))

#Current Configuration
print("Name:" + str(job.settings.job_clusters[0].job_cluster_key or ""))
print("Driver:" + str(job.settings.job_clusters[0].new_cluster.driver_node_type_id or ""))
print("Worker:" + str(job.settings.job_clusters[0].new_cluster.node_type_id or ""))
print("Num Workers:" + str(job.settings.job_clusters[0].new_cluster.num_workers or ""))
print("Autoscale:" + str(job.settings.job_clusters[0].new_cluster.autoscale or ""))
print("SparkConf:" + str(job.settings.job_clusters[0].new_cluster.spark_conf or ""))

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
print("Name:" + str(job.settings.job_clusters[0].job_cluster_key or ""))
print("Driver:" + str(job.settings.job_clusters[0].new_cluster.driver_node_type_id or ""))
print("Worker:" + str(job.settings.job_clusters[0].new_cluster.node_type_id or ""))
print("Num Workers:" + str(job.settings.job_clusters[0].new_cluster.num_workers or ""))
print("Autoscale:" + str(job.settings.job_clusters[0].new_cluster.autoscale or ""))
print("SparkConf:" + str(job.settings.job_clusters[0].new_cluster.spark_conf or ""))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save Updated Cluster Configuration in Databricks

# COMMAND ----------

w.jobs.update(job_id=os.getenv("DATABRICKS_JOB_ID"),new_settings=job.settings)
