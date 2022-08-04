#!/bin/bash

#HOME="$( cd -- "$(dirname "$0")" &>/dev/null; pwd -P )"

# Default directory to save results to
results_dir="$PWD/databricks_cluster_eventlogs"

function print_usage() {
  cat <<-EOD
	Usage: $0 -c <clusterId> [-r <results directory>]
	Options:
	  -c <cluster ID>  Databricks cluster ID
	  -r <results directory>  directory in which to save cluster eventlog
	  -h                      show this helpful message and exit
	EOD
}

while getopts c:r:h: flag
do
    case "${flag}" in
        c) clusterid="$OPTARG";;
        r) results_dir="$OPTARG";;
        h) print_usage && exit;;
        *) >&2 print_usage && exit 1;;
    esac
done


offset=0
function eventlog_cli_call {
    databricks clusters events --cluster-id $clusterid --output JSON --order ASC --offset $offset
}


call=$(eventlog_cli_call)


events=$(jq '.events' <<< $call)
total_count=$(jq '.total_count' <<< $call)

# Repeat databricks call until there are no next pages
while $(jq 'has("next_page")' <<< $call)
do
    offset=$(jq '.next_page.offset' <<< $call)
    call=$(eventlog_cli_call)
    new_events=$(jq '.events' <<<$call)

    # Append the new_events onto the events list
    events=$(jq --argjson x "$events" --argjson y "$new_events" -n '$x + $y')
done

# Save to file
save_path="$results_dir/cluster-eventlog-$clusterid.json"
echo "Saving results to: $save_path"
jq -n --argjson e "$events" --argjson t "$total_count" '{events:$e, total_count:$t}' > "$save_path"