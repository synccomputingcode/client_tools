# Sync Computing Client Tools

This repo contains Sync Computing tools used to feed into our product.

## get_cluster_config.sh

`get_cluster_config.sh` parses results of AWS CLI 'describe-cluster' and 'list-instances' command into a format accepted by our Prediction UI.

### Pre-requisites

The script relies on AWS CLI to retreive the data.

[AWS ClI installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)


### Usage

```bash
./get_cluster_config.sh -i <cluster-id> -r <region>
```

`<cluster id>` is the cluster id that you are interested in parsing. The cluster id is prefixed with 'j-'.

`<region>` represents the region the cluster ran in. The script doesn't rely on the region configured in AWS config to align with the region the cluster actually ran in.
