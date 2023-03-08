# Sync Computing Client Tools

This repo contains Sync Computing tools used to feed the required information into the Sync Autotuner for Apache Spark.


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
`<region>` represents [the region the cluster ran in](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions).

New EMR console             |  Old EMR Console
:-------------------------:|:-------------------------:
<img src="https://user-images.githubusercontent.com/4088105/223570783-1a729e33-e270-4e4b-82bc-e380fed764ef.png">  |  <img src="https://user-images.githubusercontent.com/4088105/223570400-ef23916f-dab5-465e-8ff5-ca1a57f137be.png">


## Step 2: Retrieve EMR Spark logs and upload into Autotuner

1.  Go to the EMR console in AWS, and find the cluster that ran the job you are interested in optimizing. Click on the cluster name to view details of the cluster.

2.  Verify that you have `spark.eventLog.enabled` set to true for any jobs you are interested in optimizing. The Sync Autotuner needs a Spark event log from a job run in order to provide optimized cluster configurations for the job.

New EMR console             |  Old EMR Console
:-------------------------:|:-------------------------:
<img src="https://user-images.githubusercontent.com/4088105/223572670-4ee02e08-3a2e-4021-add6-185f645838fe.png">  |  <img src="https://user-images.githubusercontent.com/4088105/223572532-aa20eb49-a010-401f-a63b-cab035efea5a.png">

3.  If `spark.eventLog.dir` is set and specifies an S3 location then download the Spark event log from the specified S3 location. Skip to Step 7.

4.  If `spark.eventLog.dir` is **not set**, follow the steps below to download the Spark event log from the Spark history server.

5.  Once you are in the cluster information page, click on the “Application user interfaces” tab, and click on “Spark history server” (in red below) under “Persistent application user interfaces.”

New EMR console             |  Old EMR Console
:-------------------------:|:-------------------------:
<img src="https://user-images.githubusercontent.com/4088105/223585554-df6b249d-10ca-41ef-b8a9-ff328a709a9f.png">  |  <img src="https://user-images.githubusercontent.com/4088105/223585649-8f32d7dd-e20d-49af-b307-dba292fcebcd.png">

6.  A new tab should open up with the Spark history server. It may take a minute to load. Click the download button under the event log column to download the Spark event log.
<img src="https://user-images.githubusercontent.com/59929718/147901014-2c111ad3-3a74-4786-971c-880e578c9257.png" width="50%" height="50%">

7.  Upload the Spark event log into the Autotuner.
<img src="https://user-images.githubusercontent.com/4088105/223587432-013fd96d-597a-49c0-969b-edf0e406706b.png" width="50%" height="50%">



# Databricks Tools

These tools are not currently necessary to run the Autotuner for Databricks eventlogs.

## Retrieving your Databricks cluster eventlog

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
