#!/bin/bash

date -u +"%Y-%m-%d %H:%M:%S UTC"
echo "Running on the driver? $DB_IS_DRIVER"
echo "Driver ip: $DB_DRIVER_IP"
echo "Sync Project ID: $SYNC_PROJECT_ID"

if [ ! -z "${SYNC_PROJECT_ID}" ]; then
    if [ "$DB_IS_DRIVER" = "TRUE" ]; then
        rm /tmp/start_sync.sh
        rm -r sync-venv

        # Create a virtualenv for us to use so that installing the sync-cli doesn't
        #  impact any Databricks-default or user-installed libraries.
        virtualenv sync-venv
        source ./sync-venv/bin/activate

        cat <<EOF >> /tmp/start_sync.sh
#!/bin/bash

echo "Python location: $(which python)"

echo "Installing Sync CLI..."

echo "$(pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz)"

echo "$(sync-cli --version)"

echo "Monitoring $DB_CLUSTER_ID"

sync-cli aws-databricks monitor-cluster $DB_CLUSTER_ID
EOF

        chmod a+x /tmp/start_sync.sh
        /tmp/start_sync.sh & disown
    fi
fi
