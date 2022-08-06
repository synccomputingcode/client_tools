#!/bin/bash

# Default directory to save results to
SCRIPT_DIR="$(dirname "$0")"
results_dir="$SCRIPT_DIR/databricks_cluster_eventlogs"
mkdir -p $results_dir

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


# Make initial databricks call
call="$(eventlog_cli_call)"
events="$(jq '.events' <<< "$call")"
total_count=$(jq '.total_count' <<< "$call")

# Repeat databricks call until there's no "next_page"
while $(jq 'has("next_page")' <<< "$call")
do
    offset=$(jq '.next_page.offset' <<< "$call")
    call="$(eventlog_cli_call)"
    new_events="$(jq '.events' <<< "$call")"

    # Append the new_events onto the events list
    events=$(jq --argjson x "$events" --argjson y "$new_events" -n '$x + $y')
done

# Save to file
save_path="$results_dir/cluster-eventlog-$clusterid.json"
echo "Saving results to: $save_path"
jq -n --argjson e "$events" --argjson t "$total_count" '{events:$e, total_count:$t}' > "$save_path"