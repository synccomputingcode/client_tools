# Databricks notebook source
# Setup Text Fields
dbutils.widgets.text("Databricks Token", "")
dbutils.widgets.text("Sync API Key ID", "")
dbutils.widgets.text("Sync API Key Secret", "")
dbutils.widgets.text("Databricks Host", "")
dbutils.widgets.text("Databricks Job ID", "")
dbutils.widgets.dropdown("Training Runs", "10", ["1","2","3","4","5","6","7","8","9","10"])

# COMMAND ----------

# MAGIC %md
# MAGIC #Train Gradient Model for Databricks Job
# MAGIC This notebook executes a job multiple time in order to complete the Gradient Learning phase. The default setup assumes the following:
# MAGIC  * The Gradient Webhook has been configured
# MAGIC  * The Databricks Job has been Gradient enabled
# MAGIC
# MAGIC This job will confirgure all runs to execute using ON DEMAND nodes only.  The orginal settings will be restored after training is complete.
# MAGIC

# COMMAND ----------

# MAGIC %pip install -I https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
DATABRICKS_JOB_ID = dbutils.widgets.get("Databricks Job ID")
TRAINING_RUNS = int(dbutils.widgets.get("Training Runs"))
os.environ["DATABRICKS_TOKEN"] =  dbutils.widgets.get("Databricks Token")
os.environ["SYNC_API_KEY_ID"] = dbutils.widgets.get("Sync API Key ID")
os.environ["SYNC_API_KEY_SECRET"] = dbutils.widgets.get("Sync API Key Secret")
os.environ["DATABRICKS_HOST"] = dbutils.widgets.get("Databricks Host").rstrip('\/')
print(dbutils.widgets.get("Databricks Host").rstrip('\/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure all training runs to execute using ON DEMAND clusters

# COMMAND ----------

from sync.clients import databricks
import orjson

client = databricks.get_default_client()
job = client.get_job(DATABRICKS_JOB_ID)

if "settings" not in job:
    print("Your job has been configured without a 'settings' block. This shouldn't happen! Please let the Sync team know you encountered this issue.")

try: 
    job_clusters = job["settings"]["job_clusters"][0]["new_cluster"]
    #Find the original number of on-demand workers
    original_ondemand = job_clusters["aws_attributes"]["first_on_demand"] or 0

    #Update the cluster to all on-demand.  Answer will depend on if autoscaling is enabled
    if("autoscale" in job_clusters.keys()):
        updated_ondemand = job_clusters["autoscale"]["max_workers"] + 1
    else:
        updated_ondemand = (job_clusters["num_workers"] or 0) + 1


    print(f"first_on_demand: {original_ondemand or ''} -> {updated_ondemand or ''}")


    job_clusters["aws_attributes"]["first_on_demand"] = updated_ondemand
    new_settings = {"job_clusters": job["settings"]["job_clusters"]}
    client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=new_settings)
    
except KeyError as k:
    print(f"We hit an error in the setup process. Currently, Gradient is only designed to work with Job clusters, not Task clusters. Contact the Sync Team for next steps. Error: {k}")    
    

# COMMAND ----------

import json
import requests
import time

def run_job(run_job_id):
    values = {'job_id': run_job_id}

    resp = requests.post(os.getenv("DATABRICKS_HOST") + '/api/2.1/jobs/run-now',
                            data=json.dumps(values), auth=("token", os.getenv("DATABRICKS_TOKEN")))
    runjson = resp.text
    print(runjson)

    d = json.loads(runjson)
    runid = d['run_id']
    print(f"run_id: {runid}")

    while True:
        time.sleep(60)
        print("...")
        endpoint = f"{os.getenv('DATABRICKS_HOST')}/api/2.1/jobs/runs/get?run_id={runid}"
        jobresp = requests.get(endpoint, auth=("token", os.getenv("DATABRICKS_TOKEN")))
        j = json.loads(jobresp.text)            
        current_state = j['state']['life_cycle_state']
        runid = j['run_id']
        if current_state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']:
            termination_state = j['state']['result_state']
            break
    return termination_state
    

def submit_test_runs(submit_job_id, training_runs):
    current_run=1
    while current_run <= training_runs:
        print(f"Starting run {current_run} of {training_runs}")
        termination_state = run_job(run_job_id=submit_job_id)
        print(f"Run status: {termination_state}")
        if termination_state == 'SUCCESS':
            print("Completed")
            current_run = current_run + 1
            wait_sec=1200
            print(f"Waiting for log submission and rec generation: {wait_sec} sec")
            time.sleep(wait_sec) #need to wait to allow logs to be submitted and rec to be generated
        else:
            print("Unsuccessful run. Training Ended")
            current_run = training_runs + 1

submit_test_runs(submit_job_id=DATABRICKS_JOB_ID, training_runs=TRAINING_RUNS)

# COMMAND ----------


