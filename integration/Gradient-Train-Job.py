# Databricks notebook source
# MAGIC %md
# MAGIC #Train Gradient Model for Databricks Job
# MAGIC This notebook executes a job multiple time in order to complete the Gradient Learning phase. The default setup assumes the following:
# MAGIC   * The Gradient Webhook has been configured
# MAGIC   * The Databricks Job has been Gradient enabled
# MAGIC ## Update these values below
# MAGIC   * DATABRICKS_HOST: Databricks host
# MAGIC   * DATABRICKS_TOKEN: Databricks personal access token
# MAGIC   * DATABRICKS_JOB_ID: Job ID to train
# MAGIC   * TRAINING_RUNS: Number of training runs
# MAGIC

# COMMAND ----------

# Update these values
DATABRICKS_HOST = <your-host-address>
DATABRICKS_TOKEN = <your-token>
DATABRICKS_JOB_ID = <your-job-id>
TRAINING_RUNS = 10

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import jobs
from databricks.sdk.service.compute import AutoScale

w = WorkspaceClient(
  host  = DATABRICKS_HOST,
  token = DATABRICKS_TOKEN
)

job = w.jobs.get(DATABRICKS_JOB_ID)

#Current Configuration
print("Name:" + str(job.settings.job_clusters[0].job_cluster_key or ""))
print("Num Workers:" + str(job.settings.job_clusters[0].new_cluster.num_workers or ""))
print("Autoscale:" + str(job.settings.job_clusters[0].new_cluster.autoscale or ""))
print("Original First On Demand:" + str(job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand or ""))

if job.settings.job_clusters[0].new_cluster.autoscale is not None:
    autoscale = job.settings.job_clusters[0].new_cluster.autoscale.max_workers
else:
    autoscale = 0

orgiginal_ondemand = job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand or 0
ondemand = (job.settings.job_clusters[0].new_cluster.num_workers or 0) + autoscale + 1
print("First_on_demand:" + str(ondemand or ""))

job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand = ondemand

w.jobs.update(job_id=DATABRICKS_JOB_ID,new_settings=job.settings)

print("Num Workers:" + str(job.settings.job_clusters[0].new_cluster.num_workers or ""))
print("Autoscale:" + str(job.settings.job_clusters[0].new_cluster.autoscale or ""))
print("New First On Demand:" + str(job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand or ""))

def get_run_result_state(run_id):
    run_state = w.jobs.get_run(run_id=run_id).state.life_cycle_state
    result_state = w.jobs.get_run(run_id=run_id).state.result_state

    while (run_state.value not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")):
        print("...")
        time.sleep(30)
        result_state = w.jobs.get_run(run_id=run_id).state.result_state
        run_state = w.jobs.get_run(run_id=run_id).state.life_cycle_state
    else:
        if (result_state is None):
            return "UNSUCCESSFUL"
        else:
            return result_state.value

def submit_test_runs(job_id, training_runs):
    current_run=1
    while current_run <= training_runs:
        print("Starting run " + str(current_run) + " of " + str(training_runs))
        run = w.jobs.run_now(job_id=job_id)
        print("run id:" + str(run.response.run_id))
        termination_state = get_run_result_state(run.response.run_id)
        if termination_state == 'SUCCESS':
            print("Completed")
            current_run = current_run + 1
        else:
            print("Unsuccessful run. Training Ended")
            current_run = training_runs + 1

submit_test_runs(job_id=DATABRICKS_JOB_ID, training_runs=TRAINING_RUNS)

job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand = orgiginal_ondemand
w.jobs.update(job_id=DATABRICKS_JOB_ID,new_settings=job.settings)
print("Restored First On Demand:" + str(job.settings.job_clusters[0].new_cluster.aws_attributes.first_on_demand or ""))


# COMMAND ----------


