# Databricks notebook source
# Setup Text Fields
dbutils.widgets.text("Databricks Token", "")
dbutils.widgets.text("Databricks Host", "")
dbutils.widgets.text("Databricks Job ID", "")
dbutils.widgets.dropdown("Databricks Plan", "Premium", ["Standard", "Premium", "Enterprise"])

dbutils.widgets.text("Sync API Key ID", "")
dbutils.widgets.text("Sync API Key Secret", "")
dbutils.widgets.dropdown("Training Runs", "10", ["1","2","3","4","5","6","7","8","9","10"])
dbutils.widgets.dropdown("Bypass Webhook", "False", ["True", "False"])


# COMMAND ----------

# MAGIC %md
# MAGIC #Train Gradient Model for Databricks Job
# MAGIC This notebook executes a job multiple time in order to complete the Gradient Learning phase. The default setup assumes the following:
# MAGIC  * The Gradient Webhook has been configured
# MAGIC  * The Databricks Job has been Gradient enabled
# MAGIC
# MAGIC This job will configure all runs to execute using ON DEMAND nodes only.  The orginal settings will be restored after training is complete.
# MAGIC

# COMMAND ----------

# MAGIC %pip install -I https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz
# MAGIC %pip install databricks-cli

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
DATABRICKS_JOB_ID = dbutils.widgets.get("Databricks Job ID")
DATABRICKS_PLAN = dbutils.widgets.get("Databricks Plan")
TRAINING_RUNS = int(dbutils.widgets.get("Training Runs"))
BYPASS_WEBHOOK = eval(dbutils.widgets.get("Bypass Webhook"))
os.environ["DATABRICKS_TOKEN"] =  dbutils.widgets.get("Databricks Token")
os.environ["SYNC_API_KEY_ID"] = dbutils.widgets.get("Sync API Key ID")
os.environ["SYNC_API_KEY_SECRET"] = dbutils.widgets.get("Sync API Key Secret")
os.environ["DATABRICKS_HOST"] = dbutils.widgets.get("Databricks Host").rstrip('\/')
print(dbutils.widgets.get("Databricks Host").rstrip('\/'))


# COMMAND ----------

from sync.clients.databricks import get_default_client
from sync.models import Platform, AccessStatusCode
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

platform = get_default_client().get_platform()
if platform is Platform.AWS_DATABRICKS:
    from sync import awsdatabricks as sync_databricks
elif platform is Platform.AZURE_DATABRICKS:
    from sync import azuredatabricks as sync_databricks
else:
    raise ValueError(f"Unsupported platform: {platform}")


access_report = sync_databricks.get_access_report()

for line in access_report:
    logger.info(line)

assert not any(line.status is AccessStatusCode.RED for line in access_report), "Required access is missing"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure all training runs to execute using ON DEMAND clusters

# COMMAND ----------

from sync.clients import databricks
from sync.api import projects
import orjson

databricks_client = databricks.get_default_client()
job = databricks_client.get_job(DATABRICKS_JOB_ID)

if "settings" not in job:
    print("Your job has been configured without a 'settings' block. This shouldn't happen! Please let the Sync team know you encountered this issue.")

try: 
    #default to job_clusters if they exist, else get tasks (which should always exist)
    if "job_clusters" in job["settings"].keys():
        key = "job_clusters"
        clusters =  job["settings"].get("job_clusters")
    else:
        key = "tasks"
        clusters = job["settings"].get("tasks")
    #grabbing the relevant fields
    for cluster in clusters:
        cluster = cluster["new_cluster"]

        if "aws_attributes" in cluster.keys():
            cloud_attributes_key = "aws_attributes"
        elif "azure_attributes" in cluster.keys():
            cloud_attributes_key = "azure_attributes"

        #Find the original number of on-demand workers
        original_ondemand = cluster[cloud_attributes_key]["first_on_demand"] or 0

        #Update the cluster to all on-demand.  Answer will depend on if autoscaling is enabled
        if("autoscale" in cluster.keys()):
            updated_ondemand = cluster["autoscale"]["max_workers"] + 1
        else:
            updated_ondemand = (cluster["num_workers"] or 0) + 1


        print(f"first_on_demand: {original_ondemand or ''} -> {updated_ondemand or ''}")


        cluster[cloud_attributes_key]["first_on_demand"] = updated_ondemand
        new_settings = {key: clusters}
        databricks_client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=new_settings)
    
except KeyError as k:
    print(f"We hit an error in the setup process. Contact the Sync Team for next steps. Error: {k}")    


# COMMAND ----------

import json
import requests
import time
from dateutil import parser
from datetime import datetime, timezone
from sync.clients import sync, databricks
from sync.api import projects

databricks_client = databricks.get_default_client()
sync_client = sync.get_default_client()

class RecommendationError(Exception):
    "Raised something goes wrong with the generation of a GradientML Recommendation"

    def __init__(self, error):
        super().__init__("Recommendation Error: " + str(error))

def get_custom_tag_value(key):
    #this will not work correctly for jobs with multiple clusters
    job = databricks_client.get_job(DATABRICKS_JOB_ID)
    if clusters := (job["settings"].get("job_clusters") or job["settings"].get("tasks")):
        val = clusters[0]["new_cluster"]["custom_tags"].get(key)
    else:
        raise(ValueError("This job does not have configured task-level or job-level compute clusters"))
    return val

if project_id := get_custom_tag_value("sync:project-id"):
    if not projects.get_project(project_id).result["auto_apply_recs"]:
        raise RecommendationError("Recommendations will not be created for this project because 'auto_apply_recs' is False")
else:
    raise ValueError("Job has not been properly onboarded. Cluster tags are missing")

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

def wait_for_recommendation(starting_recommendation_id):
    print(f"Waiting for log submission and rec generation and application")
    start_time = datetime.now(timezone.utc)
    current_recommendation_id = get_custom_tag_value("sync:recommendation-id")

    starting_recommendation_submission_id = None
    current_recommendation_submission_id = None

    if project_id := get_custom_tag_value("sync:project-id"):
        if starting_recommendation_id:
            starting_recommendation_submission_id = sync_client.get_project_recommendation(project_id, starting_recommendation_id)["result"]["context"]["latest_submission_id"]
        if current_recommendation_id:
            current_recommendation_submission_id = sync_client.get_project_recommendation(project_id, current_recommendation_id)["result"]["context"]["latest_submission_id"]
    
    while current_recommendation_submission_id == starting_recommendation_submission_id:
        time.sleep(30)
        current_reccomendation_id = get_custom_tag_value("sync:recommendation-id")

        if project_id := get_custom_tag_value("sync:project-id"):
            if current_recommendation_id:
                current_recommendation_submission_id = sync_client.get_project_recommendation(project_id, current_reccomendation_id)["result"]["context"]["latest_submission_id"]
            #check if a new recommendation has been created, and provide a 5 min leeway period to update the databricks job
            if recommendations := sync_client.get_project_recommendations(get_custom_tag_value("sync:project-id"))["result"]:
                latest_recommendation = recommendations[0]
                if latest_recommendation["id"] != starting_recommendation_id:
                    if latest_recommendation["state"] == 'FAILURE':
                        raise RecommendationError("Recommendation state is FAILURE")
                    latest_rec_created_time = parser.parse(latest_recommendation["created_at"])
                    if (datetime.now(timezone.utc) - latest_rec_created_time).total_seconds() > 5 * 60:
                        raise RecommendationError("Recommendation was unsuccessfully applied to databricks job")
        
        #timeout if it takes longer that 45 minutes
        if (datetime.now(timezone.utc) - start_time).total_seconds() > 45 * 60:
            raise RecommendationError("Recommendation timed out (45 minutes)")

def submit_test_runs(submit_job_id, training_runs):
    current_run=1
    while current_run <= training_runs:
        print(f"Starting run {current_run} of {training_runs}")
        current_recommendation_id = get_custom_tag_value("sync:recommendation-id")
        termination_state = run_job(run_job_id=submit_job_id)
        print(f"Run status: {termination_state}")
        if termination_state == 'SUCCESS':
            print("Completed")
            current_run = current_run + 1
            wait_for_recommendation(current_recommendation_id)
        else:
            print("Unsuccessful run. Training Ended")
            current_run = training_runs + 1


# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from sync._databricks import get_recommendation_job
import datetime
from sync.api.projects import create_project, create_project_recommendation, wait_for_recommendation

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])

def create_submission_and_get_recommendation_job_for_run(
    dbx_job_id: str,
    dbx_run_id: str,
    plan_type: str,
    compute_type: str,
    sync_project_id: str):

    response = sync_databricks.create_submission_for_run(
        dbx_run_id, plan_type, compute_type, sync_project_id, False, False
    )
    if response.error:
        raise Exception(response)
    logger.info(f"submission_id={response.result}")

    response = create_project_recommendation(sync_project_id)
    if response.error:
        raise Exception(response)
    logger.info(f"recommendation_id={response.result}")

    response = wait_for_recommendation(sync_project_id, response.result)
    if response.error:
        raise Exception(response)

    return get_recommendation_job(dbx_job_id, sync_project_id, response.result["id"])

def run_and_monitor_job(dbx_job_id: str) -> str:
    logger.info("Running and monitoring databricks job")
    waiter = w.jobs.run_now(dbx_job_id)
    run = waiter.result(timeout=datetime.timedelta(minutes=240))
    logger.info(f"job finished: {run.run_page_url}")
    return str(run.run_id)


def delete_webhooks_from_job(sync_project_id: str, databricks_api_client: ApiClient):

    logger.info("Deleting webhooks from job")
    response = sync_databricks.get_project_job(job_id=DATABRICKS_JOB_ID, project_id=sync_project_id)

    job = response.result
    settings = job["settings"]

    if "webhook_notifications" in settings:
        settings["webhook_notifications"] = None

    for task in settings["tasks"]:
        if "webhook_notifications" in task:
            task["webhook_notifications"] = None

    update = JobsApi(databricks_api_client).reset_job(
        {
            "job_id": DATABRICKS_JOB_ID,
            "new_settings": settings,
        }
    )

def validate_job_for_bypass(databricks_api_client: ApiClient):

    logger.info("Validating job for webhook bypass")
    job = JobsApi(databricks_api_client).get_job(DATABRICKS_JOB_ID)
    settings = job["settings"]

    if settings.get("format") == "MULTI_TASK":

        num_clusters = 0
        job_cluster_key_set = set({})
        for task in settings["tasks"]:
            if job_cluster_key := task.get("job_cluster_key"):
                if job_cluster_key not in job_cluster_key_set:
                    job_cluster_key_set.add(job_cluster_key)
                    num_clusters += 1
            elif task.get("new_cluster"):
                num_clusters +=1

        if num_clusters > 1:
            raise Exception(f"Webhook bypass is only allowed for single-cluster jobs. Detected {num_clusters} clusters.")


def run_loop_with_library():

    databricks_api_client = ApiClient(host=os.getenv("DATABRICKS_HOST"), token=os.getenv("DATABRICKS_TOKEN"))
    validate_job_for_bypass(databricks_api_client)

    # 1. Create the Sync Project
    logger.info("Creating Sync project")
    response = create_project("sync_test_project", platform, job_id=DATABRICKS_JOB_ID)
    if response.error:
        raise Exception(response)
    sync_project_id = response.result["id"]
    logger.info(f"project_id={sync_project_id}")

    # 2. Delete webhooks from job settings.
    #    Submissions & Recommendations will be handled in this method
    delete_webhooks_from_job(sync_project_id, databricks_api_client)

    for _ in range(TRAINING_RUNS):

        # 3. Run and monitor the databricks job
        logger.info("Starting databricks job run")
        dbx_run_id = run_and_monitor_job(DATABRICKS_JOB_ID)
        logger.info(f"run_id={dbx_run_id}")

        response = create_submission_and_get_recommendation_job_for_run(
            DATABRICKS_JOB_ID,
            dbx_run_id,
            DATABRICKS_PLAN,
            "Jobs Compute",
            sync_project_id
        )
        if response.error:
            raise Exception(response)

        update = JobsApi(databricks_api_client).reset_job(
            {
                "job_id": DATABRICKS_JOB_ID,
                "new_settings": response.result["settings"],
            }
        )

# COMMAND ----------

if not BYPASS_WEBHOOK:
    submit_test_runs(submit_job_id=DATABRICKS_JOB_ID, training_runs=TRAINING_RUNS)
else:
    run_loop_with_library()
