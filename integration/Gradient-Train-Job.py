# Databricks notebook source
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

dbutils.library.restartPython

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update these values below
# MAGIC  * DATABRICKS_HOST: Databricks host
# MAGIC  * DATABRICKS_TOKEN: Databricks personal access token
# MAGIC  * DATABRICKS_JOB_ID: Job ID to train
# MAGIC  * TRAINING_RUNS: Number of training runs

# COMMAND ----------

import os
# Update these values
DATABRICKS_JOB_ID = 65609458830436
TRAINING_RUNS = 2

os.environ["DATABRICKS_TOKEN"] = ""
os.environ["SYNC_API_KEY_ID"] = ""
os.environ["SYNC_API_KEY_SECRET"] = ""
os.environ["DATABRICKS_HOST"] = "https://[EDITME].cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure all training runs to execute using ON DEMAND clusters

# COMMAND ----------

from sync.clients import databricks
import orjson

client = databricks.get_default_client()
job = client.get_job(DATABRICKS_JOB_ID)

print(job)

#Find the original number of on-demand workers
original_ondemand = job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["first_on_demand"] or 0
original_availability = job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["availability"]
print("Original First_on_demand:" + str(original_ondemand or ""))
print("Original Availability: " + original_availability)

#Update the cluster to all on-demand.  Answer will depend on if autoscaling is enabled
if("autoscale" in job["settings"]["job_clusters"][0]["new_cluster"].keys()):
    autoscale = job["settings"]["job_clusters"][0]["new_cluster"]["autoscale"]["max_workers"]
    updated_ondemand = autoscale + 1
    print("Updated First_on_demand:" + str(updated_ondemand or "")) 
else:
    autoscale = 0
    updated_ondemand = (job["settings"]["job_clusters"][0]["new_cluster"]["num_workers"] or 0) + 1
    print("Updated First_on_demand:" + str(updated_ondemand or "")) 


#Update the first_on_demand to the new updated on-demand value
job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["first_on_demand"] = updated_ondemand
job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["availability"] = "ON_DEMAND"

#Update the cluster settings
client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=job)

# COMMAND ----------

import json
import requests
import os
import time

def run_job(run_job_id):
    values = {'job_id': run_job_id}

    resp = requests.post(os.getenv("DATABRICKS_HOST") + '/api/2.1/jobs/run-now',
                            data=json.dumps(values), auth=("token", os.getenv("DATABRICKS_TOKEN")))
    runjson = resp.text
    print(resp)
    d = json.loads(runjson)
    print("run_id:" + str(d['run_id']))
    runid = d['run_id']

    waiting = True
    while waiting:
        time.sleep(60)
        print("...")
        jobresp = requests.get(os.getenv("DATABRICKS_HOST") + '/api/2.1/jobs/runs/get?run_id='+str(runid),
                            auth=("token", os.getenv("DATABRICKS_TOKEN")))
        jobjson = jobresp.text
        j = json.loads(jobjson)            
        current_state = j['state']['life_cycle_state']
        runid = j['run_id']
        if current_state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']:
            termination_state = j['state']['result_state']
            break
    return termination_state
    

def submit_test_runs(submit_job_id, training_runs):
    current_run=1
    while current_run <= training_runs:
        print("Starting run " + str(current_run) + " of " + str(training_runs))
        termination_state = run_job(run_job_id=submit_job_id)
        print("run status:" + termination_state)
        if termination_state == 'SUCCESS':
            print("Completed")
            current_run = current_run + 1
        else:
            print("Unsuccessful run. Training Ended")
            current_run = training_runs + 1

submit_test_runs(submit_job_id=DATABRICKS_JOB_ID, training_runs=TRAINING_RUNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Restore original ON DEMAND settings

# COMMAND ----------

job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["first_on_demand"] = original_ondemand
job["settings"]["job_clusters"][0]["new_cluster"]["aws_attributes"]["availability"] = original_availability
print(job)
original_settings = {"job_clusters": job["settings"]["job_clusters"]}
print(original_settings)
#client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=original_settings)
client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=job)
