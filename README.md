# Sync Computing Client Tools

This repo contains Sync Computing tools used to feed our product.

## emr_cluster_script.sh

`emr_cluster_script.sh` parses the results of AWS CLI 'describe-cluster' command into a format easily applied to our Prediction UI.

### Pre-requisites

The script relies on AWS CLI to retreive the data, and `jq` to parse the JSON. 

[AWS ClI installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

[jq installation instructions](https://stedolan.github.io/jq/download/)

### Usage

```bash
./create-s3-bucket.sh -i <cluster-id> -r <region>
```

`<cluster id>` is the cluster id, your interested in parsing. The cluster id is prefixed with 'j-'.

`<region>` represents the region the cluster ran in. The script doesn't rely on the region configured in AWS config to align with the region the cluster actually ran in.
