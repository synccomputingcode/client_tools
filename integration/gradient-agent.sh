#!/bin/bash

echo "$(date -u): Sync init script starting"

REQUIRED_ENV_VARS=(DATABRICKS_HOST DATABRICKS_TOKEN DB_CLUSTER_ID DB_IS_DRIVER)
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

sync-cli --debug $PLATFORM monitor-cluster $DB_CLUSTER_ID & disown
