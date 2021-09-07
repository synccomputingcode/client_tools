#!/bin/bash

function usage {
  echo $1
  echo "USAGE: emr_cluster_script.sh -i <cluster-id> -r <region>"
  echo "  <cluster-id> is EMR cluster id, starts with 'j-'."
  echo "  <region> is the AWS region where the EMR cluster ran."
  exit 9
}

function error {
  echo $1
  exit 1
}


function prereqs {
# aws client returns 252 when no command is provided
# 127 is returned, at least on a Mac when the command is missing
  aws >/dev/null 2>&1
  if [[ $? -eq 127 ]]; then
    error "aws client is missing. Installation instructions: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html"
  fi

  jq --help >/dev/null 2>&1 || error "'jq' is missing. Installation instructions: https://stedolan.github.io/jq/download/"
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

RESPONSE=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION})

AZ=$(echo $RESPONSE | jq .Cluster.Ec2InstanceAttributes.Ec2AvailabilityZone | sed -e 's/^"//' -e 's/"$//')

# Just in case AWS changes the order the master and core machines are reported.
INSTANCE_ONE=$(echo $RESPONSE | jq '.Cluster.InstanceGroups[0] as $parent | .Cluster.InstanceGroups[0].InstanceGroupType | $parent')
INSTANCE_TWO=$(echo $RESPONSE | jq '.Cluster.InstanceGroups[1] as $parent | .Cluster.InstanceGroups[0].InstanceGroupType | $parent')

echo $INSTANCE_ONE | grep MASTER | grep InstanceGroupType >/dev/null 2>&1
if [[ $? -eq 0 ]]; then
# Found the master node
  NUM_WORKERS=$(echo $INSTANCE_TWO | jq '.RequestedInstanceCount')
  DRIVER_INSTANCE_TYPE=$(echo $INSTANCE_ONE | jq '.InstanceType'| sed -e 's/^"//' -e 's/"$//')
  DRIVER_EBS_SIZE=$(echo $INSTANCE_ONE | jq '.EbsBlockDevices[0].VolumeSpecification.SizeInGB')
  DRIVER_EBS_VOLUMES=$(echo $INSTANCE_ONE | jq '.EbsBlockDevices | length')
  WORKER_INSTANCE_TYPE=$(echo $INSTANCE_TWO | jq '.InstanceType' | sed -e 's/^"//' -e 's/"$//')
  WORKER_EBS_SIZE=$(echo $INSTANCE_TWO | jq '.EbsBlockDevices[0].VolumeSpecification.SizeInGB')
  WORKER_EBS_VOLUMES=$(echo $INSTANCE_TWO | jq '.EbsBlockDevices | length')
else
  NUM_WORKERS=$(echo $INSTANCE_ONE | jq '.RequestedInstanceCount')
  WORKER_INSTANCE_TYPE=$(echo $INSTANCE_ONE | jq '.InstanceType'| sed -e 's/^"//' -e 's/"$//')
  WORKER_EBS_SIZE=$(echo $INSTANCE_ONE | jq '.EbsBlockDevices[0].VolumeSpecification.SizeInGB')
  WORKER_EBS_VOLUMES=$(echo $INSTANCE_ONE | jq '.EbsBlockDevices | length')
  DRIVER_INSTANCE_TYPE=$(echo $INSTANCE_TWO | jq '.InstanceType'| sed -e 's/^"//' -e 's/"$//')
  DRIVER_EBS_SIZE=$(echo $INSTANCE_TWO | jq '.EbsBlockDevices[0].VolumeSpecification.SizeInGB')
  DRIVER_EBS_VOLUMES=$(echo $INSTANCE_TWO | jq '.EbsBlockDevices | length')
fi

echo "driverInstanceType = ${DRIVER_INSTANCE_TYPE}"
echo "driverEBSSize_GB   = ${DRIVER_EBS_SIZE}"
echo "driverEBSVolumes   = ${DRIVER_EBS_VOLUMES}"
echo "workerInstanceType = ${WORKER_INSTANCE_TYPE}"
echo "numWorkers         = ${NUM_WORKERS}"
echo "workerEBSSize_GB   = ${WORKER_EBS_SIZE}"
echo "workerEBSVolumes   = ${WORKER_EBS_VOLUMES}"
echo "region             = ${REGION}"
echo "availabilityZone   = ${AZ}"

printf '{"driverInstanceType":"%s","driverEBSSize_GB":"%s","driverEBSVolumes":"%s","workerInstanceType":"%s","numWorkers":"%s","workerEBSSize_GB":"%s","workerEBSVolumes":"%s","region":"%s","availabilityZone":"%s"}\n' \
"${DRIVER_INSTANCE_TYPE}" "${DRIVER_EBS_SIZE}" "${DRIVER_EBS_VOLUMES}" "${WORKER_INSTANCE_TYPE}" "${NUM_WORKERS}" \
"${WORKER_EBS_SIZE}" "${WORKER_EBS_VOLUMES}" "${REGION}" "${AZ}"
