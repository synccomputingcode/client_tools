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


CLUSTER_TYPE=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --output json --query 'Cluster.InstanceCollectionType')

#echo $CLUSTER_TYPE

RESPONSE=""
LIST_INSTANCES=""

if [[ $CLUSTER_TYPE == '"INSTANCE_GROUP"' ]];
then
    # UNIFORM
    RESPONSE=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --output json --query 'Cluster.{ID: Id, Timeline: Status.Timeline, AvailabilityZone: Ec2InstanceAttributes.Ec2AvailabilityZone, InstanceCollectionType: InstanceCollectionType, InstanceGroups: InstanceGroups[*].{InstanceGroupType: InstanceGroupType, InstanceType: InstanceType, RequestedInstanceCount: RequestedInstanceCount, VolumeSpecification: EbsBlockDevices[*].{SizeInGB: VolumeSpecification.SizeInGB}}}')
    JSON='{"cluster": %s, "region": "%s"}\n'
    OUTPUT=$(printf "$JSON" "$RESPONSE" "$REGION")

else
    # FLEET
    RESPONSE=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --output json --query 'Cluster.{ID: Id, Timeline: Status.Timeline, AvailabilityZone: Ec2InstanceAttributes.Ec2AvailabilityZone, InstanceCollectionType: InstanceCollectionType, InstanceFleets: InstanceFleets[*].{FleetId: Id, InstanceFleetType: InstanceFleetType, InstanceTypeSpecifications: InstanceTypeSpecifications[*].{InstanceType: InstanceType, VolumeSpecification: EbsBlockDevices[*].{SizeInGB: VolumeSpecification.SizeInGB}}}}')
    LIST_INSTANCES=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --region ${REGION} --output json --query 'Instances[*].{Id: Id, InstanceFleetId: InstanceFleetId, InstanceType: InstanceType, Timeline: Status.Timeline}')
    JSON='{"cluster": %s, "list-instances": %s, "region": "%s"}\n'
    OUTPUT=$(printf "$JSON" "$RESPONSE" "$LIST_INSTANCES" "$REGION")
fi


echo $OUTPUT


