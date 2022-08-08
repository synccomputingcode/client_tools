#!/bin/bash

# Default directory to save results to
SCRIPT_DIR="$(dirname "$0")"
results_dir="$SCRIPT_DIR/databricks_cluster_eventlogs"
mkdir -p $results_dir

function print_usage() {
  cat << EOD
  Usage: $0 -c <clusterId> [-r <results directory>]
  Options:
  -c <cluster ID>         (required) Databricks cluster ID
  -r <results directory>  Directory in which to save cluster eventlog
  -h                      Show this helpful message and exit
EOD
}

while getopts c:r:h flag
do
    case "${flag}" in
        c) clusterid="$OPTARG";;
        r) results_dir="$OPTARG";;
        h) print_usage && exit;;
        *) >&2 print_usage && exit 1;;
    esac
done

# Vadidates the existence of -c argument
TMP=${clusterid:?"Argument -c is required. Run script with -h option for help."}

offset=0
function eventlog_cli_call {
    databricks clusters events --cluster-id $clusterid --output JSON --order ASC --offset $offset
}

# Check for databricks CLI installation
if ! command -v databricks &> /dev/null
then
    echo "Databricks CLI could not be found. Please install and configure before running script."
fi

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