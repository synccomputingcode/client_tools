# Databricks notebook source
# MAGIC %md
# MAGIC # Configure Workflow for Gradient
# MAGIC
# MAGIC ##Prerequisites
# MAGIC  - Jobs can only have one compute cluster defined
# MAGIC  - Gradient needs access to an Instance Profile that has S3 or DBFS access to where logs are stored and ability to describe the cluster using the AWS EC2 APIs.  [See documentation for details](https://docs.synccomputing.com/security/gradient-permissions).
# MAGIC
# MAGIC ## Notebook Parameters
# MAGIC ### *** UPDATE BELOW WITH YOUR VALUES***
# MAGIC  - generate_clone :True or False (Generating is clone is recommended for initial testing)
# MAGIC  - db_workspace_url : The workspace URL
# MAGIC  - job_id :  Job you wish to integrate with Gradient
# MAGIC  - sync_project_id : Gradient Project ID.  See here for instructions to create a project
# MAGIC  - secret_scope :  Databricks Secret Scope to store required credentials.  This notebook will create if missing.
# MAGIC    - db_token : Token for account with perms to update jobs
# MAGIC    - sync_access_key_id : [See here](https://docs.synccomputing.com/sync-gradient/account-settings)
# MAGIC    - sync_secret_access_key : [See here](https://docs.synccomputing.com/sync-gradient/account-settings)
# MAGIC  - workspace_path_init : Workspace path to Gradient init script. This notebook will download latest version. [See Source Here](https://github.com/synccomputingcode/client_tools/tree/main/integration).
# MAGIC     - include file name : e.g. /Users/demo/SyncComputing/send-logs-gradient
# MAGIC  - gradient_notebook_path :  Workspace path to gradient notebook.  This notebook will download latest version. [See Source Here](https://github.com/synccomputingcode/client_tools/tree/main/integration).
# MAGIC     - include file name : e.g. /Users/demo/SyncComputing/gradient_agent.sh

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Parameters
# MAGIC ### *** UPDATE WITH YOUR VALUES***

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient()
workspace_path_init='/Users/' + w.current_user.me().user_name + '/gradient-agent.sh'
gradient_notebook_path='/Users/' + w.current_user.me().user_name + '/send-logs-gradient'
gradient_apply_rec_notebook_path='/Users/' + w.current_user.me().user_name + '/apply-gradient-recommendation'
secret_scope='gradient_' + w.current_user.me().user_name
generate_clone=True

#### UPDATE THESE VALUES ####
db_workspace_url='*** Update with your value ***'
job_id='*** Update with your value ***'
sync_project_id='*** Update with your value ***'
sync_access_key_id='*** Update with your value ***'
sync_secret_access_key='*** Update with your value ***'
db_token='*** Update with your value ***'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Required Files
# MAGIC  - Gradient Agent (saved to path specified in workspace_path_init)
# MAGIC  - Gradient Notebook (saved to path specified in gradient_notebook_path)

# COMMAND ----------

import base64
import requests

url = "https://raw.githubusercontent.com/synccomputingcode/client_tools/main/integration/gradient-agent.sh"
file_contents = requests.get(url).text

w.workspace.import_(content=base64.b64encode((file_contents).encode()).decode(),
                    format=workspace.ImportFormat.AUTO,
                    overwrite=True,
                    path=workspace_path_init)

# COMMAND ----------

url = "https://raw.githubusercontent.com/synccomputingcode/client_tools/main/integration/send-logs-gradient.py"
file_contents = requests.get(url).text

w.workspace.import_(content=base64.b64encode((file_contents).encode()).decode(),
                    format=workspace.ImportFormat.SOURCE,
                    overwrite=True,
                    language=workspace.Language.PYTHON,
                    path=gradient_notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create New Secret Key Value Pairs for Credentials

# COMMAND ----------

scope_name = secret_scope
scopes = w.secrets.list_scopes()
if any(x for x in scopes if x.name == scope_name):
    print('Using existing scope:' + scope_name)
else:
    w.secrets.create_scope(scope=scope_name)
    print(scope_name + ' created')

# Update the values for the secrets above if you are not using existing secrets
w.secrets.put_secret(scope=scope_name, key='db_token', string_value=db_token)
w.secrets.put_secret(scope=scope_name, key='sync_access_key_id', string_value=sync_access_key_id)
w.secrets.put_secret(scope=scope_name, key='sync_secret_access_key', string_value=sync_secret_access_key)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify AWS Credentials Specified
# MAGIC   - Gradient requires AWS permissions:
# MAGIC     - EC2 Describe cluster
# MAGIC     - S3 RW to read and write from the Spark Log direct (not required for logs stored on DBFS)
# MAGIC
# MAGIC   [See documentation for details](https://docs.synccomputing.com/security/gradient-permissions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone Job to Create Gradient Enabled Test Job

# COMMAND ----------

from databricks.sdk.service import jobs
from datetime import datetime

source_job = w.jobs.get(job_id=job_id)

# COMMAND ----------


now = datetime.now()

if generate_clone:
    job_clone_response = w.jobs.create(name=source_job.settings.name + '_gradient_enabled_' + now.strftime("%m.%d.%Y_%H:%M:%S"),
                                #continuous=source_job.settings.continuous,
                                compute=source_job.settings.compute,
                                email_notifications=source_job.settings.email_notifications,
                                format=source_job.settings.format,
                                git_source=source_job.settings.git_source,
                                health=source_job.settings.health,
                                job_clusters=source_job.settings.job_clusters,
                                max_concurrent_runs=source_job.settings.max_concurrent_runs, 
                                notification_settings=source_job.settings.notification_settings,
                                parameters=source_job.settings.parameters,
                                run_as=source_job.settings.run_as,
                                #schedule=jsource_ob.settings.schedule,
                                tags=source_job.settings.tags, 
                                tasks=source_job.settings.tasks,
                                timeout_seconds=source_job.settings.timeout_seconds,
                                #trigger=source_job.settings.trigger,
                                webhook_notifications=source_job.settings.webhook_notifications
    )
    job = w.jobs.get(job_clone_response.job_id)
else:
    job = source_job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Init Script to Existing Cluster

# COMMAND ----------

from databricks.sdk.service.compute import InitScriptInfo
from databricks.sdk.service.compute import WorkspaceStorageInfo

if job.settings.job_clusters[0].new_cluster.init_scripts is None:
    job.settings.job_clusters[0].new_cluster.init_scripts = []
    job.settings.job_clusters[0].new_cluster.init_scripts.append(InitScriptInfo(dbfs=None, s3=None, workspace=WorkspaceStorageInfo(destination=workspace_path_init)))
    print('Adding Gradient Agent')
else:
    print('Init scripts found')
    if InitScriptInfo(dbfs=None, s3=None, workspace=WorkspaceStorageInfo(destination=workspace_path_init)) not in job.settings.job_clusters[0].new_cluster.init_scripts:
        job.settings.job_clusters[0].new_cluster.init_scripts.append(InitScriptInfo(dbfs=None, s3=None, workspace=WorkspaceStorageInfo(destination=workspace_path_init)))
        print('Adding Gradient Agent to existing list')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Environment Variables to Existing Cluster

# COMMAND ----------


print(job.settings.job_clusters[0].new_cluster.spark_env_vars)
if job.settings.job_clusters[0].new_cluster.spark_env_vars is None:
    job.settings.job_clusters[0].new_cluster.spark_env_vars = {}
    
job.settings.job_clusters[0].new_cluster.spark_env_vars["DATABRICKS_TOKEN"] = '{{secrets/' + secret_scope + '/db_token}}'
job.settings.job_clusters[0].new_cluster.spark_env_vars["SYNC_PROJECT_ID"] = sync_project_id
job.settings.job_clusters[0].new_cluster.spark_env_vars["DATABRICKS_HOST"] = db_workspace_url
print(job.settings.job_clusters[0].new_cluster.spark_env_vars)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create new single node cluster for Gradient task

# COMMAND ----------

from databricks.sdk.service.jobs import *
from databricks.sdk.service.compute import *
from databricks.sdk.service.compute import *

job.settings.job_clusters.append(JobCluster(job_cluster_key='get_recommendation_cluster',
                                            new_cluster=ClusterSpec(autoscale=None, autotermination_minutes=None,
                                                                    aws_attributes=AwsAttributes(availability=None, ebs_volume_count=None, ebs_volume_iops=None,
                                                                                                 ebs_volume_size=None, ebs_volume_throughput=None,
                                                                                                 ebs_volume_type=None, first_on_demand=None,
                                                                                                 instance_profile_arn=job.settings.job_clusters[-1].new_cluster.aws_attributes.instance_profile_arn,   #copies from existing compute
                                                                                                 spot_bid_price_percent=None, zone_id=None),
                                                                    azure_attributes=None,cluster_log_conf=None, cluster_name='',
                                                                    cluster_source=None, custom_tags={'ResourceClass': 'SingleNode'},
                                                                    data_security_mode=None, docker_image=None,
                                                                    node_type_id='i3.xlarge',
                                                                    driver_node_type_id=None, enable_elastic_disk=None,
                                                                    enable_local_disk_encryption=None, gcp_attributes=None,
                                                                    init_scripts=None, num_workers=0, policy_id=None,
                                                                    runtime_engine=job.settings.job_clusters[-1].new_cluster.runtime_engine, #copied from existing compute
                                                                    single_user_name=None,
                                                                    spark_conf={'spark.master': 'local[*, 4]', 'spark.databricks.cluster.profile': 'singleNode'},
                                                                    spark_env_vars={'DATABRICKS_HOST': db_workspace_url,
                                                                                    'DATABRICKS_TOKEN': '{{secrets/' + secret_scope + '/db_token}}',
                                                                                    'SYNC_API_KEY_ID': '{{secrets/' + secret_scope + '/sync_access_key_id}}',
                                                                                    'SYNC_API_KEY_SECRET': '{{secrets/' + secret_scope + '/sync_secret_access_key}}'},
                                                                    spark_version='13.0.x-scala2.12', ssh_public_keys=None, workload_type=None)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create task to request a recommendation from Gradient

# COMMAND ----------

from databricks.sdk.service.jobs import *

job.settings.tasks.append(Task(task_key='get_sync_recommendation',
                               compute_key=None, condition_task=None,
                               dbt_task=None, depends_on=[TaskDependency(task_key=job.settings.tasks[-1].task_key, outcome=None)],
                               description=None, email_notifications=None,
                               existing_cluster_id=None, job_cluster_key='get_recommendation_cluster',
                               libraries=None, max_retries=None, min_retry_interval_millis=None,
                               new_cluster=None, notebook_task=NotebookTask(notebook_path=gradient_notebook_path,
                                                                            base_parameters={'DATABRICKS_PLAN_TYPE': 'Standard',
                                                                                             'DATABRICKS_PARENT_RUN_ID': '{{parent_run_id}}',
                                                                                             'DATABRICKS_RUN_ID': '{{run_id}}',
                                                                                             'DATABRICKS_TASK_KEY': '{{task_key}}',
                                                                                             'DATABRICKS_JOB_ID': '{{job_id}}',
                                                                                             'DATABRICKS_COMPUTE_TYPE': 'Jobs Compute',
                                                                                             'SYNC_PROJECT_ID': sync_project_id},
                                                                            source=job.settings.tasks[-1].notebook_task.source), #fix this
                               notification_settings=TaskNotificationSettings(alert_on_last_attempt=False, no_alert_for_canceled_runs=False, no_alert_for_skipped_runs=False), pipeline_task=None, python_wheel_task=None, retry_on_timeout=None,
                               spark_jar_task=None, spark_python_task=None, spark_submit_task=None,
                               sql_task=None, timeout_seconds=0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save changes to Job

# COMMAND ----------

w.jobs.update(job_id=job.job_id,new_settings=job.settings)
