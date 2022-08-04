# Sync Computing Client Tools

This repo contains Sync Computing tools used to feed the required information into the Sync Autotuner for Apache Spark.

# Databricks Instructions

## Step 1: Retrieve your Databricks cluster eventlog

`get_databrick_cluster_eventlog.sh` makes the appropriate Databricks CLI calls and combines the data into a single output file.

### Pre-requisites

The script relies on Databricks CLI to retrieve the cluster events

[Databricks CLI Installation instructions](https://docs.databricks.com/dev-tools/cli/index.html#set-up-the-cli)

### Usage
```bash
./get_databricks_cluster_eventlog.sh -i <cluster-id> [-r <results directory>]
```

### Example
```bash
./get_databricks_cluster_eventlog.sh -i 2631-121255-j612dkia -r /path/to/results
```

If the `-r` flag is excluded, then a new directory `databricks_cluster_eventlogs` will be created in the same directory as this script and results will be saved there.

Instructions for finding a cluster-id through the Databricks console can be found [here](https://docs.databricks.com/workspace/workspace-details.html#cluster-url-and-id). Alternatively, the cluster-id associated with a given Databricks Spark eventlog can be found opening the eventlog in a text editor and searching for the string **spark.databricks.clusterUsageTags.clusterId**.

### Example Output
```json
{
  "events": [
    {
      "cluster_id": "2631-121255-j612dkia",
      "timestamp": 1659615195995,
      "type": "CREATING",
      "details": {
        "cluster_size": {
          "autoscale": {
            "min_workers": 2,
            "max_workers": 8
          }
        },
        "user": "user@company.com",
        "job_run_name": "job-1234567-run-1000"
      }
    },
    ...,
    {
      "cluster_id": "2631-121255-j612dkia",
      "timestamp": 1659616529106,
      "type": "TERMINATING",
      "details": {
        "reason": {
          "code": "JOB_FINISHED",
          "type": "SUCCESS"
        }
      }
    }
  ],
  "total_count": 22
}    
```
# EMR Instruction

## Step 1: Retrieve and paste your cluster info (get_cluster_config.sh)

`get_cluster_config.sh` parses results of AWS CLI 'describe-cluster' and 'list-instances' command into a format accepted by our Prediction UI.

### Pre-requisites

The script relies on AWS CLI to retreive the data.

[AWS CLI installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)


### Usage

```bash
./get_cluster_config.sh -i <cluster-id> -r <region>
```

### Example Command

```bash
./get_cluster_config.sh -i j-3WBO3J7R6PVS -r us-east-1
```

### Example Output (copy and paste into Autotuner step #1)

```bash
{"cluster": { "ID": "j-3M0WNYKET5TIV", "Timeline": { "CreationDateTime": 1639990568.943, "ReadyDateTime": 1639991128.293, "EndDateTime": 1640004773.038 }, "AvailabilityZone": "us-east-1d", "InstanceCollectionType": "INSTANCE_GROUP", "InstanceGroups": [ { "InstanceGroupType": "MASTER", "Market": "ON_DEMAND", "InstanceType": "m5.xlarge", "RequestedInstanceCount": 1, "VolumeSpecification": [ { "SizeInGB": 32 }, { "SizeInGB": 32 } ] }, { "InstanceGroupType": "CORE", "Market": "SPOT", "InstanceType": "m5.4xlarge", "RequestedInstanceCount": 4, "VolumeSpecification": [ { "SizeInGB": 20 } ] } ] }, "region": "us-east-1"}
```

`<cluster id>` is the cluster id that you are interested in parsing. The cluster id is prefixed with 'j-'.

<img src="https://user-images.githubusercontent.com/59929718/147899913-c1305da0-aab5-4882-8faa-3beeff710ec8.png" width="50%" height="50%">

`<region>` represents [the region the cluster ran in](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions). The script doesn't rely on the region configured in AWS config to align with the region the cluster actually ran in. (e.g. us-east-1)


## Step 2: Retrieve EMR Spark logs and upload into Autotuner step #2

1.  Assure that you have spark.eventLog.enabled set to true for any jobs you are interested in optimizing. 

2.  Go to the EMR console in AWS, and find the cluster that ran the job you are interested in optimizing. Click on the cluster name to view details of the cluster.
<img src="https://user-images.githubusercontent.com/59929718/147900986-44b68adf-8f7d-4fda-b84b-2c54f6015fc5.png" width="50%" height="50%">

3.  Once you are in the cluster information page, click on the “Application user interfaces” tab, and click on “Spark history server” (in red below) under “Persistent application user interfaces.”
<img src="https://user-images.githubusercontent.com/59929718/147901007-81f08b39-1c20-468f-b57c-57dcfe4e46d5.png" width="50%" height="50%">


4.  A new tab should open up with the Spark history server. It may take a minute to load. Click the download button under the event log column to download the Spark event log. Upload this log into the Autotuner in step #2.
<img src="https://user-images.githubusercontent.com/59929718/147901014-2c111ad3-3a74-4786-971c-880e578c9257.png" width="50%" height="50%">

