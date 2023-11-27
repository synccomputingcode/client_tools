# Databricks notebook source
# Setup Text Fields
dbutils.widgets.text("Databricks Token", "")
dbutils.widgets.text("Databricks Host", "")
dbutils.widgets.text("Databricks Job ID", "")

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
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
DATABRICKS_JOB_ID = dbutils.widgets.get("Databricks Job ID")
TRAINING_RUNS = int(dbutils.widgets.get("Training Runs"))
BYPASS_WEBHOOK = eval(dbutils.widgets.get("Bypass Webhook"))
os.environ["DATABRICKS_TOKEN"] =  dbutils.widgets.get("Databricks Token")
os.environ["SYNC_API_KEY_ID"] = dbutils.widgets.get("Sync API Key ID")
os.environ["SYNC_API_KEY_SECRET"] = dbutils.widgets.get("Sync API Key Secret")
os.environ["DATABRICKS_HOST"] = dbutils.widgets.get("Databricks Host").rstrip('\/')


print(f"DATABRICKS_JOB_ID: {DATABRICKS_JOB_ID}")
print(f"TRAINING_RUNS: {TRAINING_RUNS}")
print(f"BYPASS_WEBHOOK: {BYPASS_WEBHOOK}")
print(f"DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST')}")


# COMMAND ----------

from sync.models import Platform, AccessStatusCode
from sync._databricks import get_recommendation_job
from sync.clients import sync, databricks
from sync.api import projects
import time

import logging

sync_client = sync.get_default_client()
sync_databricks_client = databricks.get_default_client()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

platform = sync_databricks_client.get_platform()
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


def get_cluster_for_job(job: dict | None) -> dict:

    if job is None:
        job = sync_databricks_client.get_job(DATABRICKS_JOB_ID)

    job_settings = job["settings"]

    cluster = None
    if job_settings["format"] == "SINGLE_TASK":
        cluster = job_settings["new_cluster"]
    elif job_settings["format"] == "MULTI_TASK":

        task = job_settings["tasks"][0]
        if new_task_cluster := task.get("new_cluster"):
            cluster = new_task_cluster
        elif job_cluster_key := task.get("job_cluster_key"):
            for job_cluster in job_settings["job_clusters"]:
                if job_cluster["job_cluster_key"] == job_cluster_key:
                    cluster = job_cluster["new_cluster"]
                    break

    if cluster:
        return cluster
    else:
        raise ValueError("Could not identify a cluster for this job")

def get_tag_for_job(job: dict, tag_key: str) -> str | None:
    cluster = get_cluster_for_job(job)
    return cluster["custom_tags"].get(tag_key)
    

def validate_job():  
    logger.info("Validating Databricks Job")
    job = sync_databricks_client.get_job(DATABRICKS_JOB_ID)
    job_settings = job["settings"]

    # 1. Check that this is a single-cluster job
    logger.info("Checking that job only has one cluster")
    if job_settings.get("format") == "MULTI_TASK":
        num_clusters = 0
        job_cluster_key_set = set({})
        for task in job_settings["tasks"]:
            if job_cluster_key := task.get("job_cluster_key"):
                if job_cluster_key not in job_cluster_key_set:
                    job_cluster_key_set.add(job_cluster_key)
                    num_clusters += 1
            elif task.get("new_cluster"):
                num_clusters +=1

        assert num_clusters==1,f"This notebook supports jobs with 1 cluster, but detected {num_clusters} clusters"

    # 2. Check for project_id and auto_apply_recs
    logger.info("Checking for project-id and auto-apply settings")
    if project_id := get_tag_for_job(job, "sync:project-id"):
        logger.info(f"Found project-id: {project_id}")
        if not projects.get_project(project_id).result["auto_apply_recs"]:
            raise RecommendationError("Recommendations will not be created for this project because 'auto_apply_recs' is False")
    else:
        raise ValueError("Job has not been properly onboarded. Cluster tag 'sync:project-id' is missing")

    # 3. Check that logs are being sent to dbfs
    cluster = get_cluster_for_job(job)
    if not cluster.get("cluster_log_conf", {}).get("dbfs"):
        raise ValueError(f"Cluster logs must be sent to dbfs. Current setting: {cluster.get('cluster_log_conf')}")

validate_job()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure all training runs to execute using ON DEMAND clusters

# COMMAND ----------

job = sync_databricks_client.get_job(DATABRICKS_JOB_ID)

if "settings" not in job:
    logger.error("Your job has been configured without a 'settings' block. This shouldn't happen! Please let the Sync team know you encountered this issue.")

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


        logger.info(f"first_on_demand: {original_ondemand or ''} -> {updated_ondemand or ''}")


        cluster[cloud_attributes_key]["first_on_demand"] = updated_ondemand
        new_settings = {key: clusters}
        sync_databricks_client.update_job(job_id=DATABRICKS_JOB_ID, new_settings=new_settings)
    
except KeyError as k:
    logger.error(f"We hit an error in the setup process. Contact the Sync Team for next steps. Error: {k}")    


# COMMAND ----------

class RecommendationError(Exception):
    "Raised something goes wrong with the generation of a GradientML Recommendation"

    def __init__(self, error):
        super().__init__("recommendation Error: " + str(error))

def run_job(run_job_id: str):

    run_start = sync_databricks_client.create_job_run({"job_id": run_job_id})
    run_id = run_start['run_id']

    logger.info(f"run_id: {run_id}")
    logger.info("waiting for job run to complete...")

    while True:
        time.sleep(60)
        run = sync_databricks_client.get_run(run_id)
        current_state = run["state"]["life_cycle_state"]
        runid = run["run_id"]
        if current_state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']:
            termination_state = run["state"]["result_state"]
            break
    return run

def wait_for_recommendation(starting_recommendation_id: str | None) -> None:
    logger.info(f"waiting for log submission and rec generation and application")
    logger.info(f"starting recommendation id: {starting_recommendation_id}")    

    while True:
        time.sleep(30)
        job = sync_databricks_client.get_job(DATABRICKS_JOB_ID)
        project_id = get_tag_for_job(job, "sync:project-id")
        current_recommendation_id = get_tag_for_job(job, "sync:recommendation-id")

        if current_recommendation_id != starting_recommendation_id:
            logger.info(f"current recommendation id: {current_recommendation_id}")
            current_recommendation = sync_client.get_project_recommendation(project_id, current_recommendation_id)["result"]
            if current_recommendation["state"] == "FAILURE":
                raise RecommendationError("Recommendation state is FAILURE")
            elif current_recommendation["state"] == "SUCCESS":
                return
            else:
                raise RecommendationError(f"Unexpected recommmendation state: {current_recommendation['id']} / {current_recommendation['state']}")


def run_loop_with_webhook():
    current_run=1
    while current_run <= TRAINING_RUNS:
        logger.info(f"starting run {current_run} of {TRAINING_RUNS}")
        current_recommendation_id = get_tag_for_job(None, "sync:recommendation-id")
        run = run_job(run_job_id=DATABRICKS_JOB_ID)
        logger.info(f"run status: {run['state']['result_state']}")
        if run["state"]["result_state"] == 'SUCCESS':
            logger.info("run completed")
            current_run = current_run + 1
            wait_for_recommendation(current_recommendation_id)
        else:
            logger.info("training ended due to unsuccessful run")
            current_run = TRAINING_RUNS + 1


# COMMAND ----------

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

    response = projects.create_project_recommendation(sync_project_id)
    if response.error:
        raise Exception(response)
    logger.info(f"recommendation_id={response.result}")

    response = projects.wait_for_recommendation(sync_project_id, response.result)
    if response.error:
        raise Exception(response)

    return get_recommendation_job(dbx_job_id, sync_project_id, response.result["id"])

def run_and_monitor_job(dbx_job_id: str) -> str:
    logger.info("running and monitoring databricks job")
    run = run_job(dbx_job_id)
    logger.info(f"job finished: {run['run_page_url']}")
    return str(run["run_id"])


def delete_webhooks_from_job(sync_project_id: str):

    logger.info("deleting webhooks from job")
    response = sync_databricks.get_project_job(job_id=DATABRICKS_JOB_ID, project_id=sync_project_id)

    job = response.result
    settings = job["settings"]

    if "webhook_notifications" in settings:
        settings["webhook_notifications"] = {}

    for task in settings["tasks"]:
        if "webhook_notifications" in task:
            task["webhook_notifications"] = {}
    
    update = sync_databricks_client.update_job(
        job_id=DATABRICKS_JOB_ID,
        new_settings=settings,
    )

def get_databricks_plan():

    workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    configs = sync_client.get_workspace_config(workspace_id)
    if plan := configs.get("result", {}).get("plan_type"):
        return plan
    else:
        raise Exception(f"Could not identify Databricks plan for workspace {workspace_id}")

def run_loop_with_library():

    sync_project_id = get_tag_for_job(None, "sync:project-id")
    databricks_plan = get_databricks_plan()
    logger.info(f"project_id={sync_project_id}")

    # 1. Delete webhooks from job settings.
    #    Submissions & Recommendations will be handled in this method
    delete_webhooks_from_job(sync_project_id)

    for _ in range(TRAINING_RUNS):

        
        # 2. Run and monitor the databricks job
        logger.info("starting databricks job run")
        dbx_run_id = run_and_monitor_job(DATABRICKS_JOB_ID)
        logger.info(f"run_id={dbx_run_id}")

        response = create_submission_and_get_recommendation_job_for_run(
            DATABRICKS_JOB_ID, dbx_run_id, databricks_plan, "Jobs Compute", sync_project_id
        )
        if response.error:
            raise Exception(response)

        # 3. Update the job
        update = sync_databricks_client.update_job(
            job_id= DATABRICKS_JOB_ID,
            new_settings= response.result["settings"],
        )

# COMMAND ----------

if not BYPASS_WEBHOOK:
    run_loop_with_webhook()
else:
    run_loop_with_library()
