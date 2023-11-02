# Databricks notebook source
# MAGIC %md
# MAGIC ##Train Gradient Model for Databricks Job
# MAGIC This notebook executes a job multiple time in order to complete the Gradient Learning phase. The default setup assumes the following:
# MAGIC   * The Gradient Webhook has been configured
# MAGIC   * The Databricks Job has been Gradient enabled
# MAGIC   * DATABRICKS_HOST: Databricks host
# MAGIC   * DATABRICKS_TOKEN: Databricks personal access token
# MAGIC   * DATABRICKS_JOB_ID: Job ID to train
# MAGIC   * TRAINING_RUNS: Number of training runs
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython

# COMMAND ----------

dbutils.widgets.text("DATABRICKS_HOST", "")
dbutils.widgets.text("DATABRICKS_TOKEN", "")
dbutils.widgets.text("DATABRICKS_JOB_ID", "")
dbutils.widgets.text("TRAINING_RUNS", "")

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import jobs

w = WorkspaceClient(
  host  = dbutils.widgets.get("DATABRICKS_HOST"),
  token = dbutils.widgets.get("DATABRICKS_TOKEN")
)

def get_run_result_state(run_id):
    run_status = w.jobs.get_run(run_id=run_id).state.result_state
    while (run_status is None):
        print("...")
        time.sleep(60)
        run_status = w.jobs.get_run(run_id=run_id).state.result_state
    else:
        return run_status.value

def submit_test_runs(job_id, test_runs):
    current_run=1
    while current_run <= test_runs:
        print("Starting run " + str(current_run) + " of " + str(test_runs))
        run = w.jobs.run_now(job_id=job_id)
        print("run id:" + str(run.response.run_id))
        termination_state = get_run_result_state(run.response.run_id)
        if termination_state == 'SUCCESS':
            print("Completed")
            current_run = current_run + 1
        else:
            print("Unsuccessful run. Training Ended")
            current_run = test_runs + 1



submit_test_runs(job_id=dbutils.widgets.get("DATABRICKS_JOB_ID"), training_runs=int(dbutils.widgets.get("TRAINING_RUNS")))

