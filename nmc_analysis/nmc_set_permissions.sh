#!/bin/bash

target_resource="nmc-analysis"

logical_path=$1
collection=$(dirname ${logical_path})
dataobject=$(basename ${logical_path})
CHMOD_TARGET=$(iquest "%s" "select DATA_PATH where COLL_NAME = '${collection}' and DATA_NAME = '${dataobject}' and RESC_NAME = '${target_resource'")
while [ "${CHMOD_TARGET}" != "/" ] ; do
    if [ -d "${CHMOD_TARGET}" ] ; then
        TARGET_PERMISSIONS="755"
    else
        TARGET_PERMISSIONS="644"
    fi
    CMD="chmod ${TARGET_PERMISSIONS} ${CHMOD_TARGET}"
    ${CMD}
    CHMOD_TARGET=$(dirname "${CHMOD_TARGET}")
done
