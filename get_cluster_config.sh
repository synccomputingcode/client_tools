#!/bin/bash
function usage {
  echo $1
  echo "USAGE: emr_clusterConf.sh -i <cluster-id> -r <region>"
  echo "  <cluster-id> is EMR cluster id, starts with 'j-'."
  echo "  <region> is the AWS region where the EMR cluster ran."
  exit 9
}

function prereqs {
# aws client returns 252 when no command is provided
# 127 is returned, at least on a Mac when the command is missing
  aws >/dev/null 2>&1
  if [[ $? -eq 127 ]]; then
    error "aws client is missing. Installation instructions: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html"
  fi
}

function verify_aws_cli_call {
  if [[ $? -ne 0 ]]; then
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -r)
      REGION="$2"
      shift # past argument
      shift # past value
      ;;
    -i)
      CLUSTER_ID="$2"
      shift # past argument
      shift # past value
      ;;
  esac
done

prereqs

[[ -z "$REGION" ]] && usage "Region is required."

[[ -z "$CLUSTER_ID" ]] && usage "Cluster ID is required"


DESCRIBE_CLUSTER=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --output json)
verify_aws_cli_call $?

LIST_INSTANCES=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --region ${REGION} --output json)
verify_aws_cli_call $?

LIST_STEPS=$(aws emr list-steps --cluster-id ${CLUSTER_ID} --region ${REGION} --output json)
verify_aws_cli_call $?

FORMAT='{"describe_cluster": %s, "list_instances": %s, "list_steps": %s, "region": "%s"}\n'
OUTPUT=$(printf "$FORMAT" "$DESCRIBE_CLUSTER" "$LIST_INSTANCES" "$LIST_STEPS" "$REGION")


jq --version >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
  >&2 echo "\nInstall JQ at https://stedolan.github.io/jq/ for a pretty-print JSON output\n"
  echo $OUTPUT
else
  echo $OUTPUT | jq '.'
fi
