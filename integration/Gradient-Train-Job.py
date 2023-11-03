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
DATABRICKS_HOST = dbutils.secrets.get(scope="Sync Computing | 45g7cc4-7e2b-45d6-92bc-0bg229b6025f", key="DATABRICKS_HOST")
DATABRICKS_TOKEN = dbutils.secrets.get(scope="Sync Computing | 45g7cc4-7e2b-45d6-92bc-0bg229b6025f", key="DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = 227554421094496
TRAINING_RUNS = 3

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import jobs

w = WorkspaceClient(
  host  = DATABRICKS_HOST,
  token = DATABRICKS_TOKEN
)

def get_run_result_state(run_id):
    run_status = w.jobs.get_run(run_id=run_id).state.result_state
    while (run_status is None):
        print("...")
        time.sleep(60)
        run_status = w.jobs.get_run(run_id=run_id).state.result_state
    else:
        return run_status.value

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

