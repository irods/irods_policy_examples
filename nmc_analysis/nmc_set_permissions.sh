#!/bin/bash

logical_path=$1
logfile=$(ls -rt /var/lib/irods/log/rodsLog* | tail -n1)
collection=$(dirname ${logical_path})
dataobject=$(basename ${logical_path})
physical_path=$(iquest "%s" "select DATA_PATH where COLL_NAME = '${collection}' and DATA_NAME = '${dataobject}' and RESC_NAME = 'nmc-analysis'")
echo "physical_path[${physical_path}]" >> $logfile
chmod 744 -R ${physical_path}

exit 0;

