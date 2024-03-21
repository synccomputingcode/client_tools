# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Gradient Core Hours Calculator

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompts
# MAGIC   - host: Databricks Workspace, leave blank for current workspace
# MAGIC   - token: Databricks Token, leave blank for current workspace
# MAGIC   - days back: start range for run info, these days will be extrapoloated to estimate annual usage
# MAGIC   - Jobs compute rate e.g. enterprise list for jobs compute is .20
# MAGIC
# MAGIC ## Dependencies
# MAGIC   - This notebook will load DBU data from [github](https://github.com/syncpete/notebooks/blob/main/dbus_vcpus.csv) in to a table named gradient_usage_predictions.dbus_jobs_enterprise.
# MAGIC   (https://github.com/syncpete/notebooks/blob/main/dbus_vcpus.csv)
# MAGIC
# MAGIC If you want to load manually [Databricks instructions for creating a table from uploaded CSV](https://docs.databricks.com/en/ingestion/add-data/upload-data.html).
# MAGIC (https://docs.databricks.com/en/ingestion/add-data/upload-data.html)gradient_usage_predictions.dbus_jobs_enterprise

# COMMAND ----------

dbutils.widgets.text("host","")
dbutils.widgets.text("token","")
dbutils.widgets.text("days back", "1")
dbutils.widgets.text("jobs compute rate", ".20")

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh 
# MAGIC  
# MAGIC rm -rf /dbfs/tmp/sync/gradient  # drop any old copies of data
# MAGIC mkdir -p /dbfs/tmp/sync/gradient # ensure destination folder exists
# MAGIC  
# MAGIC # download data to destination folder
# MAGIC wget -N https://raw.githubusercontent.com/syncpete/notebooks/main/dbus_vcpus.csv -P /dbfs/tmp/sync/gradient
# MAGIC
# MAGIC  
# MAGIC # download data to destination folder
# MAGIC wget -N https://raw.githubusercontent.com/syncpete/notebooks/main/fleet.csv -P /dbfs/tmp/sync/gradient

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Usage Data via API

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import jobs
from datetime import datetime

if dbutils.widgets.get("host") is not None and dbutils.widgets.get("token") is not None:
    w = WorkspaceClient(host=dbutils.widgets.get("host"), token =dbutils.widgets.get("token"))
else:
    w = WorkspaceClient()
# if you choose a custom default, then the function will cast all objs at the end to that default type, unless default is None.
from dataclasses import is_dataclass, asdict

def safe_getattr(obj, attr, default=None):
    try:
        # Convert dataclass to dict if necessary
        if is_dataclass(obj):
            obj = asdict(obj)

        for a in attr.split('.'):
            if isinstance(obj, dict):
                obj = obj.get(a)
            else:
                obj = getattr(obj, a, None)

        # Attempt to cast obj to the type of default, if default is not None
        if default is not None and obj is not None:
            obj_type = type(default)
            try:
                return obj_type(obj)
            except (ValueError, TypeError):
                return default
        return obj if obj is not None else default
    except (AttributeError, KeyError):
        return default



# Example usage
#value = safe_getattr(obj, 'settings.schedule', "None")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType, FloatType

csv_filename = "/tmp/sync/gradient/dbus_vcpus.csv"

instance_schema = StructType([
    StructField("instance_type", StringType()),
    StructField("vcpus", LongType()),
    StructField("memory", LongType()),
    StructField("dbus", FloatType())
])
 
sparkInstanceTypesDF = spark.read.csv(csv_filename, sep=',',
                         schema=instance_schema, header=True)

sparkInstanceTypesDF.createOrReplaceTempView("dbus_jobs_enterprise")  

fleet_csv_filename = "/tmp/sync/gradient/fleet.csv"

fleet_schema = StructType([
    StructField("instance_size", StringType()),
    StructField("vcpus", LongType())
])
 
sparkFleetDF = spark.read.csv(fleet_csv_filename, sep=',',
                         schema=fleet_schema, header=True)

sparkFleetDF.createOrReplaceTempView("fleet_size")  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gradient_usage_predictions;
# MAGIC USE gradient_usage_predictions;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.dbus_jobs_enterprise;
# MAGIC create table gradient_usage_predictions.dbus_jobs_enterprise as select * from dbus_jobs_enterprise;
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.fleet_size;
# MAGIC create table gradient_usage_predictions.fleet_size as select * from fleet_size;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: Get Single Cluster By ID
# MAGIC This is needed for any cluster not returned by the list calls

# COMMAND ----------

def get_cluster(job_id, run_id, task_key, cluster_id, compute_definition):
    try:
        clusterObj = w.clusters.get(cluster_id)
        row = {
                "job_id": job_id,
                "run_id": run_id,
                "task_key": task_key,                        
                "cluster_identifier": cluster_id,
                "compute_definition": compute_definition,
                "aws_attributes_availability": safe_getattr(clusterObj, "aws_attributes.availability.value", "None"),
                "enable_elastic_disk": str(safe_getattr(clusterObj, "enable_elastic_disk")),
                "enable_local_disk_encryption": safe_getattr(clusterObj, "enable_local_disk_encryption"),
                "workload_type_jobs": safe_getattr(clusterObj, "workload_type.clients.jobs", "None"),
                "aws_attributes_ebs_volume_count": safe_getattr(clusterObj, "aws_attributes.ebs_volume_count", 0),
                "aws_attributes_ebs_volume_iops": safe_getattr(clusterObj, "aws_attributes.ebs_volume_iops", 0),
                "aws_attributes_ebs_volume_size": safe_getattr(clusterObj, "aws_attributes.ebs_volume_size", 0),
                "aws_attributes_ebs_volume_throughput": safe_getattr(clusterObj, "aws_attributes.ebs_volume_throughput", 0),
                "aws_attributes_ebs_volume_type": safe_getattr(clusterObj, "aws_attributes.ebs_volume_type.value", "None"),
                "aws_attributes_first_on_demand": safe_getattr(clusterObj, "aws_attributes.first_on_demand"),
                "aws_attributes_spot_bid_price_percent": safe_getattr(clusterObj, "aws_attributes.spot_bid_price_percent"),
                "aws_attributes_instance_profile_arn": safe_getattr(clusterObj, "aws_attributes.instance_profile_arn"),        
                "aws_attributes_zone_id": safe_getattr(clusterObj, "aws_attributes.zone_id"),                        
                "driver_node": safe_getattr(clusterObj, "driver_node_type_id", safe_getattr(clusterObj, "node_type_id")),
                "worker_node": safe_getattr(clusterObj, "node_type_id"),
                "num_workers": safe_getattr(clusterObj, "num_workers", 0),
                "autoscale_min": safe_getattr(clusterObj, "autoscale.min_workers", 0),
                "autoscale_max": safe_getattr(clusterObj, "autoscale.max_workers", 0),
                "spark_version": safe_getattr(clusterObj, "spark_version"),
                "policy_id": safe_getattr(clusterObj, "policy_id"),
                "autotermination_minutes": str(safe_getattr(clusterObj, "autotermination_minutes")), 
                "runtime_engine": safe_getattr(clusterObj, "runtime_engine.value", "None"),
                "spark_conf": str(safe_getattr(clusterObj, "spark_conf", "None")),
                "custom_tags": str(safe_getattr(clusterObj, "custom_tags", "None"))   
        }
        return row
    except:
        pass
    #except Exception as e: print(e)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Clusters for APC Lookup
# MAGIC Return information about all pinned clusters, active clusters, up to 200 of the most recently terminated all-purpose clusters in the past 30 days, and up to 30 of the most recently terminated job clusters in the past 30 days.
# MAGIC
# MAGIC For example, if there is 1 pinned cluster, 4 active clusters, 45 terminated all-purpose clusters in the past 30 days, and 50 terminated job clusters in the past 30 days, then this API returns the 1 pinned cluster, 4 active clusters, all 45 terminated all-purpose clusters, and the 30 most recently terminated job clusters.

# COMMAND ----------

clustersGen = w.clusters.list()
clusters = []
for clusterObj in clustersGen:
    row = {
        "cluster_id": safe_getattr(clusterObj, "cluster_id"),
        "name": safe_getattr(clusterObj, "cluster_name"),
        "creator_user_name": safe_getattr(clusterObj, "creator_user_name"),
        "aws_attributes_availability": safe_getattr(clusterObj, "aws_attributes.availability.value", "None"),
        "enable_elastic_disk": str(safe_getattr(clusterObj, "enable_elastic_disk")),
        "enable_local_disk_encryption": safe_getattr(clusterObj, "enable_local_disk_encryption"),
        "workload_type_jobs": safe_getattr(clusterObj, "workload_type.clients.jobs", "None"),
        "workload_type_notebooks": safe_getattr(clusterObj, "workload_type.clients.notebooks", "None"),
        "aws_attributes_ebs_volume_count": safe_getattr(clusterObj, "aws_attributes.ebs_volume_count", 0),
        "aws_attributes_ebs_volume_iops": safe_getattr(clusterObj, "aws_attributes.ebs_volume_iops", 0),
        "aws_attributes_ebs_volume_size": safe_getattr(clusterObj, "aws_attributes.ebs_volume_size", 0),
        "aws_attributes_ebs_volume_throughput": safe_getattr(clusterObj, "aws_attributes.ebs_volume_throughput", 0),
        "aws_attributes_ebs_volume_type": safe_getattr(clusterObj, "aws_attributes.ebs_volume_type.value", "None"),
        "aws_attributes_first_on_demand": safe_getattr(clusterObj, "aws_attributes.first_on_demand"),
        "aws_attributes_spot_bid_price_percent": safe_getattr(clusterObj, "aws_attributes.spot_bid_price_percent", "None"),
        "aws_attributes_instance_profile_arn": safe_getattr(clusterObj, "aws_attributes.instance_profile_arn", "None"),
        "aws_attributes_zone_id": safe_getattr(clusterObj, "aws_attributes.zone_id", "None"),
        "cluster_memory_mb": safe_getattr(clusterObj, "cluster_memory_mb", "None"),
        "cluster_cores": safe_getattr(clusterObj, "cluster_cores"),
        "cluster_source": safe_getattr(clusterObj, "cluster_source.value"),
        "driver_node": safe_getattr(clusterObj, "driver_node_type_id"),
        "worker_node": safe_getattr(clusterObj, "node_type_id"),
        "num_workers": safe_getattr(clusterObj, "num_workers", 0),
        "autoscale_min": safe_getattr(clusterObj, "autoscale.min_workers", 0),
        "autoscale_max": safe_getattr(clusterObj, "autoscale.max_workers", 0),
        "spark_version": safe_getattr(clusterObj, "spark_version"),
        "policy_id": safe_getattr(clusterObj, "policy_id"),
        "custom_tags": safe_getattr(clusterObj, "custom_tags", "None"),
        "autotermination_minutes": safe_getattr(clusterObj, "autotermination_minutes"),
        "spark_conf": safe_getattr(clusterObj, "spark_conf", "None"),
        "runtime_engine": safe_getattr(clusterObj, "runtime_engine.value", "None")
    }
    clusters.append(row)

clusterSchema = StructType([
    StructField("cluster_id", StringType()),
    StructField("name", StringType()),
    StructField("creator_user_name", StringType()),
    StructField("aws_attributes_availability", StringType()),
    StructField("enable_elastic_disk", StringType()),
    StructField("enable_local_disk_encryption", StringType()),
    StructField("workload_type_jobs", StringType()),
    StructField("workload_type_notebooks", StringType()),
    StructField("aws_attributes_ebs_volume_count", LongType()),
    StructField("aws_attributes_ebs_volume_iops", LongType()),
    StructField("aws_attributes_ebs_volume_size", LongType()),
	StructField("aws_attributes_ebs_volume_throughput", LongType()),
	StructField("aws_attributes_ebs_volume_type", StringType()),
	StructField("aws_attributes_first_on_demand", StringType()),
	StructField("aws_attributes_spot_bid_price_percent", StringType()),
	StructField("aws_attributes_instance_profile_arn", StringType()),
	StructField("aws_attributes_zone_id", StringType()),
	StructField("cluster_memory_mb", StringType()),
	StructField("cluster_cores", StringType()),
	StructField("cluster_source", StringType()),
	StructField("driver_node", StringType()),
	StructField("worker_node", StringType()),
	StructField("num_workers", StringType()),
	StructField("autoscale_min", StringType()),
	StructField("autoscale_max", StringType()),
	StructField("spark_version", StringType()),
	StructField("policy_id", StringType()),
	StructField("custom_tags", StringType()),
	StructField("autotermination_minutes", StringType()),
	StructField("spark_conf", StringType()),
	StructField("runtime_engine", StringType()),
])

sparkClustersDF = spark.createDataFrame(data=clusters, schema=clusterSchema)
sparkClustersDF.createOrReplaceTempView("cluster_info")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Pools for Lookup
# MAGIC

# COMMAND ----------

poolsGen = w.instance_pools.list()
pools = []

for poolObj in poolsGen:
    row = {
        "instance_pool_id": safe_getattr(poolObj, "instance_pool_id"),
        "instance_pool_name": safe_getattr(poolObj, "instance_pool_name"),
        "aws_attributes_availability": safe_getattr(poolObj, "aws_attributes.availability.value", "None"),
        "aws_attributes_spot_bid_price_percent": safe_getattr(clusterObj, "aws_attributes.spot_bid_price_percent", "None"),
        "aws_attributes_zone_id": safe_getattr(clusterObj, "aws_attributes.zone_id", "None"),
        "node_type_id": safe_getattr(clusterObj, "node_type_id"),
        "custom_tags": safe_getattr(clusterObj, "custom_tags", "None"),
    }
    pools.append(row)

poolsSchema = StructType([
    StructField("instance_pool_id", StringType()),
    StructField("instance_pool_name", StringType()),
    StructField("aws_attributes_availability", StringType()),
    StructField("aws_attributes_spot_bid_price_percent", StringType()),
    StructField("aws_attributes_zone_id", StringType()),
    StructField("node_type_id", StringType()),
    StructField("custom_tags", StringType())
])

sparkPoolsDF = spark.createDataFrame(data=pools, schema=poolsSchema)
sparkPoolsDF.createOrReplaceTempView("pools_info")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.cluster_info;
# MAGIC create table gradient_usage_predictions.cluster_info as select * from cluster_info;
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.pools_info;
# MAGIC create table gradient_usage_predictions.pools_info as select * from pools_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Job Info
# MAGIC  - Creator
# MAGIC  - Name
# MAGIC  - Default Params
# MAGIC  - Schedule Info

# COMMAND ----------


jobsGen = w.jobs.list(expand_tasks=True)
jobs = []
count = 1
for jobObj in jobsGen:
    row = {
        "job_id": safe_getattr(jobObj, "job_id"),
        "created_time": safe_getattr(jobObj, "created_time"),
        "creator_user_name": safe_getattr(jobObj, "creator_user_name", "None"),
        "name": safe_getattr(jobObj, "settings.name"),
        "parameters": safe_getattr(jobObj, "settings.parameters"),
        "schedule_paused_status": safe_getattr(jobObj, "settings.schedule.pause_status.value", "None"),
        "schedule_quartz_cron_expression": safe_getattr(jobObj, "settings.schedule.quartz_cron_expression", "None"),
        "schedule_timezone_id": safe_getattr(jobObj, "settings.schedule.timezone_id", "None"),
        # Uncomment and adjust the following line if needed
        # "webhook_notifications.on_success": safe_getattr(jobObj, "settings.webhook_notifications.on_success", "None"),
        "task_count": 0 if jobObj.settings.tasks == None else len(jobObj.settings.tasks),
        "job_clusters": 0 if jobObj.settings.job_clusters == None else len(jobObj.settings.job_clusters),
    }
    jobs.append(row)



# Define the schema based on the provided types_dict
schema = StructType([
    StructField("job_id", LongType()),
    StructField("created_time", LongType()),
    StructField("creator_user_name", StringType()),
    StructField("name", StringType()),
    StructField("parameters", ArrayType(StringType())),  # Assuming parameters are a list of strings
    StructField("schedule_paused_status", StringType()),
    StructField("schedule_quartz_cron_expression", StringType()),
    StructField("schedule_timezone_id", StringType()),
    StructField("task_count", IntegerType()),
    StructField("job_clusters", IntegerType())
])

sparkJobsDF = spark.createDataFrame(data=jobs, schema=schema)
sparkJobsDF.createOrReplaceTempView("job_info")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.job_info;
# MAGIC create table gradient_usage_predictions.job_info as select * from job_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Run & Cluster Info
# MAGIC - Duration
# MAGIC - State
# MAGIC - Start, End Times
# MAGIC
# MAGIC ## Get Run Task Cluster Definitions
# MAGIC
# MAGIC This will include clusters for tasks on job run
# MAGIC
# MAGIC  - Data is fetched for the number of trailing days specified in the "days back" prompt
# MAGIC
# MAGIC  - Clusters can be define in the run
# MAGIC     - sharable job cluster
# MAGIC  - Clusters can be defined in the task
# MAGIC     - existing cluster id aka APC
# MAGIC         - if not in cluster lookup, go get it
# MAGIC     - sharable job_cluster, defined in run, linked in task
# MAGIC     - sharable job_cluster, defined in run, not linked in task (missing)
# MAGIC     - new cluster

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Function: Get Cluster From Object
# MAGIC Deserialize Cluster object into python dict

# COMMAND ----------

def serialize_cluster_obj(job_id, run_id, task_key, clusterObj):
        if clusterObj.instance_pool_id is not None:

                pool_cluster_row = sparkPoolsDF.where(col("instance_pool_id") == clusterObj.instance_pool_id).first()

                # If worker is pool driver has to be pool as well.  If no driver_pool specified, it uses same as worker
                if clusterObj.driver_instance_pool_id is not None:
                        driver_pool_cluster_row = sparkPoolsDF.where(col("instance_pool_id") == clusterObj.driver_instance_pool_id).first()
                else: 
                        driver_pool_cluster_row = pool_cluster_row

                # couldn't lookup
                if pool_cluster_row is None:
                        print("Unable to find pool:" + clusterObj.instance_pool_id)
                        return None

                row = {
                        "job_id": job_id,
                        "run_id": run_id,
                        "task_key": task_key,                        
                        "cluster_identifier": "Pool Defined",
                        "compute_definition": 'Jobs Compute',
                        "aws_attributes_availability": pool_cluster_row.asDict()['aws_attributes_availability'],
                        "enable_elastic_disk": str(safe_getattr(clusterObj, "enable_elastic_disk")),
                        "enable_local_disk_encryption": safe_getattr(clusterObj, "enable_local_disk_encryption"),
                        "workload_type_jobs": safe_getattr(clusterObj, "workload_type.clients.jobs", "None"),
                        "aws_attributes_ebs_volume_count": safe_getattr(clusterObj, "aws_attributes.ebs_volume_count", 0),
                        "aws_attributes_ebs_volume_iops": safe_getattr(clusterObj, "aws_attributes.ebs_volume_iops", 0),
                        "aws_attributes_ebs_volume_size": safe_getattr(clusterObj, "aws_attributes.ebs_volume_size", 0),
                        "aws_attributes_ebs_volume_throughput": safe_getattr(clusterObj, "aws_attributes.ebs_volume_throughput", 0),
                        "aws_attributes_ebs_volume_type": safe_getattr(clusterObj, "aws_attributes.ebs_volume_type.value", "None"),
                        "aws_attributes_first_on_demand": safe_getattr(clusterObj, "aws_attributes.first_on_demand"),
                        "aws_attributes_spot_bid_price_percent": pool_cluster_row.asDict()['aws_attributes_spot_bid_price_percent'],
                        "aws_attributes_instance_profile_arn": safe_getattr(clusterObj, "aws_attributes.instance_profile_arn"),        
                        "aws_attributes_zone_id": pool_cluster_row.asDict()['aws_attributes_zone_id'],                        
                        "driver_node": driver_pool_cluster_row.asDict()['node_type_id'],
                        "worker_node": pool_cluster_row.asDict()['node_type_id'],
                        "num_workers": safe_getattr(clusterObj, "num_workers", 0),
                        "autoscale_min": safe_getattr(clusterObj, "autoscale.min_workers", 0),
                        "autoscale_max": safe_getattr(clusterObj, "autoscale.max_workers", 0),
                        "spark_version": safe_getattr(clusterObj, "spark_version"),
                        "policy_id": safe_getattr(clusterObj, "policy_id"),
                        "autotermination_minutes": str(safe_getattr(clusterObj, "autotermination_minutes")), 
                        "runtime_engine": safe_getattr(clusterObj, "runtime_engine.value", "None"),
                        "spark_conf": str(safe_getattr(clusterObj, "spark_conf", "None")),
                        "custom_tags": str(safe_getattr(clusterObj, "custom_tags", "None"))      
                }

               
        else:
                row = {
                        "job_id": job_id,
                        "run_id": run_id,
                        "task_key": task_key,                        
                        "cluster_identifier": "Task Defined",
                        "compute_definition": 'Jobs Compute',
                        "aws_attributes_availability": safe_getattr(clusterObj, "aws_attributes.availability.value", "None"),
                        "enable_elastic_disk": str(safe_getattr(clusterObj, "enable_elastic_disk")),
                        "enable_local_disk_encryption": safe_getattr(clusterObj, "enable_local_disk_encryption"),
                        "workload_type_jobs": safe_getattr(clusterObj, "workload_type.clients.jobs", "None"),
                        "aws_attributes_ebs_volume_count": safe_getattr(clusterObj, "aws_attributes.ebs_volume_count", 0),
                        "aws_attributes_ebs_volume_iops": safe_getattr(clusterObj, "aws_attributes.ebs_volume_iops", 0),
                        "aws_attributes_ebs_volume_size": safe_getattr(clusterObj, "aws_attributes.ebs_volume_size", 0),
                        "aws_attributes_ebs_volume_throughput": safe_getattr(clusterObj, "aws_attributes.ebs_volume_throughput", 0),
                        "aws_attributes_ebs_volume_type": safe_getattr(clusterObj, "aws_attributes.ebs_volume_type.value", "None"),
                        "aws_attributes_first_on_demand": safe_getattr(clusterObj, "aws_attributes.first_on_demand"),
                        "aws_attributes_spot_bid_price_percent": safe_getattr(clusterObj, "aws_attributes.spot_bid_price_percent"),
                        "aws_attributes_instance_profile_arn": safe_getattr(clusterObj, "aws_attributes.instance_profile_arn"),        
                        "aws_attributes_zone_id": safe_getattr(clusterObj, "aws_attributes.zone_id"),                        
                        "driver_node": safe_getattr(clusterObj, "driver_node_type_id", safe_getattr(clusterObj, "node_type_id")),
                        "worker_node": safe_getattr(clusterObj, "node_type_id"),
                        "num_workers": safe_getattr(clusterObj, "num_workers", 0),
                        "autoscale_min": safe_getattr(clusterObj, "autoscale.min_workers", 0),
                        "autoscale_max": safe_getattr(clusterObj, "autoscale.max_workers", 0),
                        "spark_version": safe_getattr(clusterObj, "spark_version"),
                        "policy_id": safe_getattr(clusterObj, "policy_id"),
                        "autotermination_minutes": str(safe_getattr(clusterObj, "autotermination_minutes")), 
                        "runtime_engine": safe_getattr(clusterObj, "runtime_engine.value", "None"),
                        "spark_conf": str(safe_getattr(clusterObj, "spark_conf", "None")),
                        "custom_tags": str(safe_getattr(clusterObj, "custom_tags", "None"))      
                }

        return row                      

# COMMAND ----------

import datetime
from pyspark.sql.functions import col
from databricks.sdk.service.jobs import ListRunsRunType

start_date = (datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0) - datetime.timedelta(int(dbutils.widgets.get("days back"))))
start_date = start_date.timestamp() * 1000

runsGen = w.jobs.list_runs(start_time_from=start_date, expand_tasks=True, completed_only=True)
run_clusters = []
runs = []
no_data = []
for runObj in runsGen:

    for task in runObj.tasks:

        run_info = {
            "run_id": safe_getattr(runObj, "run_id"),
            "job_id": safe_getattr(runObj, "job_id"),
            "task_key": safe_getattr(task, "task_key"),
            "run_name": safe_getattr(runObj, "run_name"),
            "notebook_task_base_params" : safe_getattr(task, "notebook_task.base_parameters.values", "N/A"),
            "notebook_task_path" : safe_getattr(task, "notebook_task.notebook_path", "N/A"),
            "notebook_task_source" : safe_getattr(task, "notebook_task.source", "N/A"),
            "spark_python_task_params" : safe_getattr(task, "spark_python_task.parameters", "N/A"),
            "spark_python_task_python_file" : safe_getattr(task, "spark_python_task.python_file", "N/A"),
            "spark_python_task_source" : safe_getattr(task, "spark_python_task.source", "N/A"),
            "spark_jar_task_parameters" : safe_getattr(task, "spark_jar_task.parameters", "N/A"),
            "spark_jar_task_jar_uri" : safe_getattr(task, "spark_jar_task.jar_uri", "N/A"),
            "spark_jar_task_main_class_name" : safe_getattr(task, "spark_jar_task.main_class_name", "N/A"),
            "spark_submit_task_parameters" : safe_getattr(task, "spark_submit_task.parameters", "N/A"),
            "dbt_task_as_dict" : safe_getattr(task, "dbt_task.as_dict", "N/A"),
            "git_source_git_url" : safe_getattr(task, "git_source.git_url", "N/A"),
            "git_source_job_source" : safe_getattr(task, "git_source.job_source", "N/A"),
            "sql_task_parameters" : safe_getattr(task, "sql_task.parameters", "N/A"),
            "sql_task_query" : safe_getattr(task, "sql_task.query", "N/A"),
            "sql_task_warehouse_id" : safe_getattr(task, "sql_task.warehouse_id", "N/A"),
            "run_job_task_job_parameters" : safe_getattr(task, "run_job_task.job_parameters", "N/A"),
            "run_job_task_job_id" : safe_getattr(task, "run_job_task.job_id", "N/A"),
            "task_duration": safe_getattr(task, "execution_duration"),
            "task_state": safe_getattr(task, "state.result_state.value"),
            "task_starttime": safe_getattr(task, "start_time"),
            "task_endtime": safe_getattr(task, "end_time"),
            "task_setup_duration": safe_getattr(task, "setup_duration"),
            "task_cleanup_duration": safe_getattr(task, "cleanup_duration"),
            "task_execution_duration": safe_getattr(task, "execution_duration"),
            "run_starttime": safe_getattr(runObj, "start_time"),
            "run_endtime": safe_getattr(runObj, "end_time"),
            "run_type": safe_getattr(runObj, "run_type.value", "Unknown"),
            "run_execution_duration": safe_getattr(runObj, "execution_duration"),
            "run_life_cycle_state": safe_getattr(runObj, "state.life_cycle_state.value", "UNKNOWN"),
            "run_result_state": safe_getattr(runObj, "state.result_state.value", "UNKNOWN")
        }
        runs.append(run_info)
        

        if task.existing_cluster_id is not None: #### APC COMPUTE ####
            existing_cluster_row = sparkClustersDF.where(col("cluster_id") == task.existing_cluster_id).first()
            if existing_cluster_row is not None:
                row = {
                    "job_id": safe_getattr(runObj, "job_id"),
                    "run_id": safe_getattr(runObj, "run_id"),
                    "task_key": safe_getattr(task, "task_key"), 
                    "cluster_identifier": safe_getattr(task, "existing_cluster_id"),
                    "compute_definition": 'All Purpose Compute',
                    "aws_attributes_availability": existing_cluster_row.asDict()['aws_attributes_availability'],
                    "enable_elastic_disk": existing_cluster_row.asDict()['enable_elastic_disk'],
                    "enable_local_disk_encryption": existing_cluster_row.asDict()['enable_local_disk_encryption'],
                    "workload_type_jobs": existing_cluster_row.asDict()['workload_type_jobs'],               
                    "aws_attributes_ebs_volume_count": existing_cluster_row.asDict()['aws_attributes_ebs_volume_count'],
                    "aws_attributes_ebs_volume_iops": existing_cluster_row.asDict()['aws_attributes_ebs_volume_iops'],
                    "aws_attributes_ebs_volume_size": existing_cluster_row.asDict()['aws_attributes_ebs_volume_size'], 
                    "aws_attributes_ebs_volume_throughput": existing_cluster_row.asDict()['aws_attributes_ebs_volume_throughput'],
                    "aws_attributes_ebs_volume_type": existing_cluster_row.asDict()['aws_attributes_ebs_volume_type'],
                    "aws_attributes_first_on_demand": existing_cluster_row.asDict()['aws_attributes_first_on_demand'],
                    "aws_attributes_spot_bid_price_percent": existing_cluster_row.asDict()['aws_attributes_spot_bid_price_percent'],
                    "aws_attributes_instance_profile_arn": existing_cluster_row.asDict()['aws_attributes_instance_profile_arn'],      
                    "aws_attributes_zone_id": existing_cluster_row.asDict()['aws_attributes_zone_id'],
                    "driver_node":  existing_cluster_row.asDict()['driver_node'],
                    "worker_node": existing_cluster_row.asDict()['worker_node'],
                    "num_workers": existing_cluster_row.asDict()['num_workers'],
                    "autoscale_min": existing_cluster_row.asDict()['autoscale_min'],
                    "autoscale_max": existing_cluster_row.asDict()['autoscale_max'],
                    "spark_version": existing_cluster_row.asDict()['spark_version'],
                    "policy_id": existing_cluster_row.asDict()['policy_id'],
                    "autotermination_minutes": existing_cluster_row.asDict()['autotermination_minutes'],   
                    "runtime_engine": existing_cluster_row.asDict()['runtime_engine'],
                    "spark_conf": existing_cluster_row.asDict()['spark_conf'],
                    "custom_tags": existing_cluster_row.asDict()['custom_tags'],                 
                }
                run_clusters.append(row)
            else: # APC not in Cluster lookup
                single_cluster=get_cluster(safe_getattr(runObj, "job_id"),safe_getattr(runObj, "run_id"),safe_getattr(task, "task_key"),safe_getattr(task, "existing_cluster_id"),'All Purpose Compute')
                if single_cluster is not None:
                    run_clusters.append(single_cluster)
                else: # Unable to retrieve cluster
                    no_data.append({"job_id": runObj.job_id, "run_id": runObj.run_id, "task_key": task.task_key, "cluster_id": task.existing_cluster_id, "status": "APC not in cluster list and can't be retrieved"}) 
        else:  #### JOBS COMPUTE DEFINED IN TASK ####
            if hasattr(task, "job_cluster_key"): #### JOBS COMPUTE DEFINED IN SHARED JOBS CLUSTER ####
                for cluster in runObj.job_clusters:
                    if (cluster.job_cluster_key == task.job_cluster_key):
                        row = serialize_cluster_obj(safe_getattr(runObj, "job_id"),safe_getattr(runObj, "run_id"),safe_getattr(task, "task_key"), cluster.new_cluster)
                        if row is not None:
                            run_clusters.append(row) 
                        else:
                            no_data.append({"job_id": runObj.job_id, "run_id": runObj.run_id, "task_key": task.task_key, "cluster_id": cluster.new_cluster.cluster_instance.cluster_id, "status": "Unable to serialize"})   
            else: #### NEW CLUSTER DEFINED IN TASK ###
                if task.new_cluster is not None: 
                    row = serialize_cluster_obj(safe_getattr(runObj, "job_id"),safe_getattr(runObj, "run_id"),safe_getattr(task, "task_key"), task.new_cluster)
                    if row is not None:
                        run_clusters.append(row)
                    else:
                        no_data.append({"job_id": runObj.job_id, "run_id": runObj.run_id, "task_key": task.task_key, "cluster_id": task.new_cluster.cluster_instance.cluster_id, "status": "Unable to serialize"})     
                else: # get cluster from instance
                    if task.cluster_instance is not None:
                        single_cluster=get_cluster(safe_getattr(runObj, "job_id"),safe_getattr(runObj, "run_id"),safe_getattr(task, "task_key"),safe_getattr(task, "cluster_instance.cluster_id"),'Jobs Compute')                            
                        if single_cluster is not None: # lookup worked
                            run_clusters.append(single_cluster)
                        else: # cluster instance but can't retrieve
                            no_data.append({"job_id": runObj.run_id, "run_id": runObj.run_id, "task_key": task.task_key, "cluster_id": task.cluster_instance.cluster_id, "status": "Unable to lookup job cluster"})
                    else: # Not defined in Task and no cluster instance
                        no_data.append({"job_id": runObj.run_id, "run_id": runObj.run_id, "task_key": task.task_key, "cluster_id": "unknown", "status": "Not defined and no cluster instance"})
     


# Define the schema based on the provided types_dict
run_schema = StructType([
    StructField("run_id", LongType()),
    StructField("job_id", LongType()),
    StructField("task_key", StringType()),
    StructField("run_name", StringType()),
    StructField("notebook_task_base_params", StringType()),
    StructField("notebook_task_path", StringType()),
    StructField("notebook_task_source", StringType()),
    StructField("spark_python_task_params", StringType()),
    StructField("spark_python_task_python_file", StringType()),
    StructField("spark_python_task_source", StringType()),
    StructField("spark_jar_task_parameters", StringType()),
    StructField("spark_jar_task_jar_uri", StringType()),
    StructField("spark_jar_task_main_class_name", StringType()),
    StructField("spark_submit_task_parameters", StringType()),
    StructField("dbt_task_as_dict", StringType()),
    StructField("git_source_git_url", StringType()),
    StructField("git_source_job_source", StringType()),
    StructField("sql_task_parameters", StringType()),
    StructField("sql_task_query", StringType()),
    StructField("sql_task_warehouse_id", StringType()),
    StructField("run_job_task_job_parameters", StringType()),
    StructField("run_job_task_job_id", StringType()),
    StructField("task_duration", LongType()),
    StructField("task_state", StringType()),
    StructField("task_starttime", LongType()),
    StructField("task_endtime", LongType()),
    StructField("task_setup_duration", LongType()),
    StructField("task_cleanup_duration", LongType()),
    StructField("task_execution_duration", LongType()),
    StructField("run_starttime", LongType()),
    StructField("run_endtime", LongType()),
    StructField("run_type", StringType()),
    StructField("run_execution_duration", LongType()),
    StructField("run_life_cycle_state", StringType()),
    StructField("run_result_state", StringType())
])


sparkRunsDF = spark.createDataFrame(data=runs, schema=run_schema)
sparkRunsDF.createOrReplaceTempView("job_run_info")
print(sparkRunsDF.count())                    

# Define the schema based on the provided types_dict
cluster_schema = StructType([
    StructField("job_id", LongType()),
    StructField("run_id", LongType()),
    StructField("task_key", StringType()),
    StructField("cluster_identifier", StringType()),
    StructField("compute_definition", StringType()),
    StructField("aws_attributes_availability", StringType()),
    StructField("enable_elastic_disk", StringType()),
    StructField("enable_local_disk_encryption", StringType()),
    StructField("workload_type_jobs", StringType()),
    StructField("aws_attributes_ebs_volume_count", LongType()),
    StructField("aws_attributes_ebs_volume_iops", LongType()),
    StructField("aws_attributes_ebs_volume_size", LongType()),
    StructField("aws_attributes_ebs_volume_throughput", LongType()),
    StructField("aws_attributes_ebs_volume_type", StringType()),
    StructField("aws_attributes_first_on_demand", StringType()),
    StructField("aws_attributes_spot_bid_price_percent", StringType()),
    StructField("aws_attributes_instance_profile_arn", StringType()),
    StructField("aws_attributes_zone_id", StringType()),
    StructField("driver_node", StringType()),
    StructField("worker_node", StringType()),
    StructField("num_workers", StringType()),
    StructField("autoscale_min", StringType()),
    StructField("autoscale_max", StringType()),
    StructField("spark_version", StringType()),
    StructField("policy_id", StringType()),
    StructField("autotermination_minutes", StringType()),
    StructField("runtime_engine", StringType()),
    StructField("spark_conf", StringType()),
    StructField("custom_tags", StringType())
])

sparkRunClustersDF = spark.createDataFrame(data=run_clusters, schema=cluster_schema)
sparkRunClustersDF.createOrReplaceTempView("job_run_cluster_info")

error_schema = StructType([
    StructField("job_id", StringType()),
    StructField("run_id", StringType()),
    StructField("task_key", StringType()),        
    StructField("cluster_id", StringType()),
    StructField("status", StringType())
])
   
sparkNoClustersDF = spark.createDataFrame(data=no_data, schema=error_schema)
sparkNoClustersDF.createOrReplaceTempView("job_run_no_cluster_info")



# COMMAND ----------

display(sparkPoolsDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.job_run_info;
# MAGIC create table gradient_usage_predictions.job_run_info as select * from job_run_info;
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.job_run_cluster_info;
# MAGIC create table gradient_usage_predictions.job_run_cluster_info as select * from job_run_cluster_info;
# MAGIC
# MAGIC drop table if exists gradient_usage_predictions.job_run_no_cluster_info;
# MAGIC create table gradient_usage_predictions.job_run_no_cluster_info as select * from job_run_no_cluster_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tasks Cluster Info

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'all runs tasks', count(distinct run_id, task_key) counts from gradient_usage_predictions.job_run_info
# MAGIC union
# MAGIC select 'tasks with cluster definition', count(distinct run_id, task_key) from gradient_usage_predictions.job_run_cluster_info
# MAGIC union
# MAGIC select 'tasks with no cluster definition', count(distinct run_id, task_key) from gradient_usage_predictions.job_run_no_cluster_info
# MAGIC union
# MAGIC select 'clusters we couldnt look up', count(distinct cluster_id) from gradient_usage_predictions.job_run_no_cluster_info;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gradient_usage_predictions.job_run_no_cluster_info 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view gradient_usage_predictions.all_job_clusters as
# MAGIC select job_id,
# MAGIC task_key,
# MAGIC cluster_identifier,
# MAGIC compute_definition,
# MAGIC aws_attributes_availability,
# MAGIC enable_elastic_disk,
# MAGIC cast(enable_local_disk_encryption as STRING) enable_local_disk_encryption,
# MAGIC workload_type_jobs,
# MAGIC aws_attributes_ebs_volume_count,
# MAGIC aws_attributes_ebs_volume_iops,
# MAGIC aws_attributes_ebs_volume_size,
# MAGIC aws_attributes_ebs_volume_throughput,
# MAGIC aws_attributes_ebs_volume_type,
# MAGIC aws_attributes_first_on_demand,
# MAGIC aws_attributes_spot_bid_price_percent,
# MAGIC aws_attributes_instance_profile_arn,
# MAGIC aws_attributes_zone_id,
# MAGIC driver_node,
# MAGIC worker_node,
# MAGIC num_workers,
# MAGIC autoscale_min,
# MAGIC autoscale_max,
# MAGIC spark_version,
# MAGIC policy_id,
# MAGIC autotermination_minutes,
# MAGIC runtime_engine,
# MAGIC case when (num_workers = 'None') then 'Autoscale' else 'Fixed' end node_provision_method,
# MAGIC custom_tags,
# MAGIC spark_conf
# MAGIC from gradient_usage_predictions.job_run_cluster_info
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view gradient_usage_predictions.run_usage as
# MAGIC select distinct
# MAGIC        jri.job_id,
# MAGIC        jri.run_id,
# MAGIC        jri.run_name,
# MAGIC        jri.run_type,
# MAGIC        jri.run_starttime,
# MAGIC        jri.run_result_state,
# MAGIC        ajc.driver_node,
# MAGIC        fsd.vcpus driver_vcps,
# MAGIC        ajc.node_provision_method,
# MAGIC        ajc.worker_node,
# MAGIC        case when (ajc.num_workers = 'None') then 0 else ajc.num_workers end worker_nodes,
# MAGIC        case when (ajc.autoscale_min = 'None') then 0 else ajc.autoscale_min end autoscale_min,
# MAGIC        case when (ajc.autoscale_max = 'None') then 0 else ajc.autoscale_max end autoscale_max,              
# MAGIC        fsw.vcpus worker_vcps,
# MAGIC        round((jri.run_endtime - jri.run_starttime) / 1000 / 60,2) duration_min,
# MAGIC        round(((jri.run_endtime - jri.run_starttime) / 1000 / 60 / 60 * fsd.vcpus) + ((jri.run_endtime - jri.run_starttime) / 1000 / 60 / 60 * fsw.vcpus * case when (ajc.node_provision_method = 'Autoscale') then ajc.autoscale_max else ajc.num_workers end), 2) core_hrs
# MAGIC   from gradient_usage_predictions.job_run_info jri
# MAGIC   join gradient_usage_predictions.all_job_clusters ajc on jri.job_id = ajc.job_id and jri.task_key = ajc.task_key
# MAGIC   join gradient_usage_predictions.fleet_size fsd on fsd.instance_size = substring_index(ajc.driver_node, '.', -1)
# MAGIC   join gradient_usage_predictions.fleet_size fsw on fsw.instance_size = substring_index(ajc.worker_node, '.', -1)
# MAGIC ;
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gradient_usage_predictions;

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select jri.run_result_state, count(*)
# MAGIC from gradient_usage_predictions.job_run_info jri
# MAGIC group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Runs by Run Type & Compute Definition

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(from_unixtime(jri.run_starttime/1000)) day,
# MAGIC        jri.run_type, ajc.compute_definition,
# MAGIC        count(distinct jri.run_name || jri.task_key) job_task_count,
# MAGIC        count(distinct jri.run_id) run_count
# MAGIC   from gradient_usage_predictions.job_run_info jri
# MAGIC   join gradient_usage_predictions.all_job_clusters ajc on (jri.job_id = ajc.job_id and jri.task_key = ajc.task_key) 
# MAGIC  group by to_date(from_unixtime(jri.run_starttime/1000)), jri.run_type, ajc.compute_definition
# MAGIC  order by day asc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Submit Groupings
# MAGIC Number of run_ids = number of job_ids but run_name and code is same

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  to_date(from_unixtime(run_starttime/1000)) day,
# MAGIC         case when (notebook_task_path <> 'N/A') then 'Notebook Task'
# MAGIC             when (run_job_task_job_id <> 'N/A') then 'Job Run Task'
# MAGIC             when (spark_jar_task_jar_uri <> 'N/A') then 'Spark Jar Task'
# MAGIC             when (spark_python_task_python_file <> 'N/A') then 'Spark Python Task'
# MAGIC             when (sql_task_query <> 'N/A') then 'SQL Task'
# MAGIC             when (dbt_task_as_dict <> 'N/A') then 'DBT Task' end task_type,
# MAGIC             count(distinct job_run_info.run_id) distinct_run_ids,
# MAGIC             count(distinct job_run_info.job_id) distinct_job_ids
# MAGIC from gradient_usage_predictions.job_run_info 
# MAGIC join all_job_clusters using (job_id, task_key)
# MAGIC group by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Usage 
# MAGIC - dbus (dbus * hours * nodes)
# MAGIC - core_hrs (vcpu * hours * nodes)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gradient_usage_predictions.run_usage;

# COMMAND ----------

import math

jobs_compute_rate = dbutils.widgets.get("jobs compute rate")
start_date = (datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0) - datetime.timedelta(int(dbutils.widgets.get("days back"))))
start_date_truncated = datetime.date(start_date.year, start_date.month, start_date.day)
print(start_date_truncated)

end_date = (datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0))
end_date_truncated = datetime.date(end_date.year, end_date.month, end_date.day)

yr_mult = math.floor(365/int(dbutils.widgets.get("days back")))
print(yr_mult)

calc = f"""select '{str(start_date_truncated)} - {str(end_date_truncated)}',
            count(distinct run_name) jobs,
            count(distinct run_id) runs,
            round(sum(core_hrs),2) core_hrs,
            round(sum(core_hrs) * {yr_mult},2) est_annual_core_hrs,
            round(sum(core_hrs) * {yr_mult} * .006,2) est_annual_core_hr_cost
         from gradient_usage_predictions.run_usage
         where date_trunc('DAY', to_date(from_unixtime(run_starttime/1000))) >= '{str(start_date_truncated)}'
           and date_trunc('DAY', to_date(from_unixtime(run_starttime/1000))) <  '{str(end_date_truncated)}'"""

display(spark.sql(calc))         

# COMMAND ----------


