import irods_types
import os

# POLICY USE CASES
# 0 - New data arrives in the scanned file system and is registered into the SCANNED_RESOURCE
#     The newly registered data will be replicated to the DESTINATION_RESOURCE_ROOT
#
# 1 - New identical data arrives in the scanned file system that is already registered elsewhere
#     Register this new data into SCANNED_RESOURCE as a replica of the existing data
#
# 2 - New very similar data arrives in the scanned file system that is already registered elsewhere
#     Register this new data into SCANNED_RESOURCE as a replica of the existing data
#     Since the file sizes are not identical, update all the other replicas
#
# 3 - Local modifications are made to existing replicas on SCANNED_RESOURCE
#     Detect and update all the other replicas

# POLICY CONFIGURATION VARIABLES
SCANNED_RESOURCE = 'example_scanned_resc'
DESTINATION_RESOURCE_ROOT = 'example_dest_resc_root'
LIST_OF_DESTINATION_RESOURCE_LEAVES = ['a','b','c']

# Given a condInput from a policy enforcement point
# Search for a given key and return the value
def get_value_for_key(kvpairs, key):
    # search and return given keyword
    for i in range(kvpairs.len):
        if str(kvpairs.key[i]) == key:
            return str(kvpairs.value[i])
    return "KEY NOT FOUND"

# Given a logical path and a replica number
# Perform a general query to determine the resource name
def get_resource_name_by_replica_number(callback, logical_path, replica_number):
    coll_name = os.path.dirname(logical_path)
    data_name = logical_path.split('/')[-1]
    conditions = "COLL_NAME = '{0}' AND DATA_NAME = '{1}' AND DATA_REPL_NUM = '{2}'".format(coll_name, data_name, replica_number)

    ret_val = callback.msiMakeGenQuery("RESC_NAME", conditions, irods_types.GenQueryInp())
    genQueryInp = ret_val['arguments'][2]

    ret_val = callback.msiExecGenQuery(genQueryInp, irods_types.GenQueryOut())
    genQueryOut = ret_val['arguments'][1]
    result_count = genQueryOut.rowCnt

    resource_name = str(genQueryOut.sqlResult[0].row(0)) if result_count > 0 else None
    return (result_count, resource_name)

# Given a logical path
# Perform a general query to determine the replica size
def get_existing_replica_size_from_destination(callback, logical_path):
    coll_name = os.path.dirname(logical_path)
    data_name = logical_path.split('/')[-1]
    resc_names = ','.join(["'"+i+"'" for i in LIST_OF_DESTINATION_RESOURCE_LEAVES])
    conditions = "COLL_NAME = '{0}' AND DATA_NAME = '{1}' AND RESC_NAME IN ({2})".format(coll_name, data_name, resc_names)

    ret_val = callback.msiMakeGenQuery("DATA_SIZE", conditions, irods_types.GenQueryInp())
    genQueryInp = ret_val['arguments'][2]

    ret_val = callback.msiExecGenQuery(genQueryInp, irods_types.GenQueryOut())
    genQueryOut = ret_val['arguments'][1]
    previously_registered_replicas = genQueryOut.rowCnt

    logical_size = int(genQueryOut.sqlResult[0].row(0)) if previously_registered_replicas > 0 else None
    return (previously_registered_replicas, logical_size)

# Dynamic Policy Enforcement Point for rsPhyPathReg
# Required for Use Cases 0, 1, 2
def pep_api_phy_path_reg_post(rule_args, callback, rei):
    args = rule_args[2]
    logical_path = str(args.objPath)

    # do nothing if we're not on SCANNED_RESOURCE
    target_resource = get_value_for_key(args.condInput,"destRescName")
    if SCANNED_RESOURCE != target_resource:
        return

    # determine whether a replica already exists
    previously_registered_replicas, logical_size = get_existing_replica_size_from_destination(callback, logical_path)
    if previously_registered_replicas > 0:
        # replica already exists, stat the file system to get the new replica's size
        physical_path = get_value_for_key(args.condInput,"filePath")
        physical_size = os.stat(physical_path).st_size
        # compare new replica size with existing replica size
        if physical_size != logical_size:
            # Use Case 2 - Sizes Do Not Match, Update All Existing Replicas
            status = 0
            params = "irodsAdmin=++++updateRepl=++++all="
            callback.msiDataObjRepl(logical_path,params,status)
        else:
            # Use Case 1 - Sizes Match, Do Nothing
            pass
    else:
        # Use Case 0 - Replicate New Data to DESTINATION_RESOURCE_ROOT
        status = 0
        params = "irodsAdmin=++++destRescName={0}".format(DESTINATION_RESOURCE_ROOT)
        callback.msiDataObjRepl(logical_path,params,status)

# Dynamic Policy Enforcement Point for rsModDataObjMeta
# Required for Use Case 3
def pep_api_mod_data_obj_meta_post(rule_args, callback, rei):
    args = rule_args[2]
    logical_path = str(args['logical_path'])

    # do nothing if we're not on SCANNED_RESOURCE
    target_replica_number = args['replica_number']
    result_count, target_resource = get_resource_name_by_replica_number(callback, logical_path, target_replica_number)
    if SCANNED_RESOURCE != target_resource:
        return

    # determine whether a replica already exists
    previously_registered_replicas, logical_size = get_existing_replica_size_from_destination(callback, logical_path)
    if previously_registered_replicas > 0:
        # replica already exists, get the new replica's size
        physical_size = int(args['dataSize'])
        # compare sizes
        if physical_size != logical_size:
            # update if not equal
            status = 0
            params = "irodsAdmin=++++updateRepl=++++all="
            callback.msiDataObjRepl(logical_path,params,status)
        else:
            # match, do nothing
            # print("data sizes match - no action taken")
            pass
