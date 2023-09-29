#!/bin/bash

echo "$(date -u): Sync init script starting"

REQUIRED_ENV_VARS=(DATABRICKS_HOST DATABRICKS_TOKEN SYNC_JOB_ID DB_CLUSTER_ID DB_IS_DRIVER)
SYNC_VENV_PATH=/tmp/sync

function print_usage {
  local remaining_env_vars=(${REQUIRED_ENV_VARS[*]:1})
  cat <<EOD
Required environment variables:
  $(printf "%s\n" ${REQUIRED_ENV_VARS[0]} "${remaining_env_vars[@]/#/  }")
EOD
}

for var in ${REQUIRED_ENV_VARS[*]}; do
  if [[ -z ${!var} ]]; then
    >&2 echo "$var is missing"
    >&2 print_usage
    exit 1
  fi
done

if [[ $DB_IS_DRIVER != TRUE ]]; then
  echo "Not running on a Databricks cluster driver. Exiting."
  exit
fi

virtualenv $SYNC_VENV_PATH

. $SYNC_VENV_PATH/bin/activate

pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

PLATFORM=$(python <<EOD
from sync.clients.databricks import get_default_client
print(get_default_client().get_platform().value)
EOD
)

if [[ -z $SYNC_PROJECT_ID ]]; then
  SYNC_PROJECT_ID=$(python <<EOD
from sync.${PLATFORM/-/} import get_cluster
from sync.api.projects import get_projects

cluster_response = get_cluster("$DB_CLUSTER_ID")
if cluster_response.result:
  cluster = cluster_response.result
  if cluster['cluster_source'] == "JOB":
    project_id = cluster.get('custom_tags', {}).get('sync:project-id')
    if project_id:
      print(project_id)
EOD
)
fi

if [[ -z $SYNC_PROJECT_ID ]]; then
  >&2 echo "No project found for job running on cluster $DB_CLUSTER_ID"
  exit 1
fi

sync-cli --debug $PLATFORM monitor-cluster $DB_CLUSTER_ID & disown
