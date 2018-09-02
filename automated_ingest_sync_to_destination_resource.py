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



###################################
# POLICY CONFIGURATION VARIABLES

SCANNED_RESOURCE_ROOT = 'example_scanned_resc'
DESTINATION_RESOURCE_ROOT = 'example_destination_resc'

# By default, for root resources which have no children, the leaves variables
# are set to be the above root variables.  The leaves are the roots.
#

LIST_OF_SCANNED_RESOURCE_LEAVES = [ SCANNED_RESOURCE_ROOT ]
LIST_OF_DESTINATION_RESOURCE_LEAVES = [ DESTINATION_RESOURCE_ROOT ]

# When root resources have descendants in a tree (and are not standalone resources),
# the leaves variables must be set to contain all the leaves of the respective tree.
#
# For example, given `ilsresc` for the scanned root:
#
# src_root:passthru
# `-- src_random:random
#     |-- src0:unixfilesystem
#     |-- src1:unixfilesystem
#     `-- src2:unixfilesystem
#
# SCANNED_RESOURCE_ROOT = 'src_root'
# LIST_OF_SCANNED_RESOURCE_LEAVES  = ['src0', 'src1', 'src2']
#
# And `ilsresc` for the destination root:
#
# dst_root:passthru
# `-- dst_compound:compound
#     |-- dst_cache:unixfilesystem
#     `-- dst_archive:s3
#
# DESTINATION_RESOURCE_ROOT = 'dst_root'
# LIST_OF_DESTINATION_RESOURCE_LEAVES = ['dst_cache', 'dst_archive']
###################################

def resource_is_not_target(resc_name):
    return not ((resc_name == SCANNED_RESOURCE_ROOT) or (resc_name in LIST_OF_SCANNED_RESOURCE_LEAVES))

# Given a condInput from a policy enforcement point
# Search for a given key and return the value
def get_value_for_key(kvpairs, key):
    # search and return given keyword
    for i in range(kvpairs.len):
        if str(kvpairs.key[i]) == key:
            return str(kvpairs.value[i])
    return "KEY NOT FOUND"

# Given a replica number and a logical path
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

# Given a logical path and a resource name
# Perform a general query to determine the replica size
def get_existing_replica_size_from_destination(callback, logical_path):
    coll_name = os.path.dirname(logical_path)
    data_name = logical_path.split('/')[-1]
    resc_names = ','.join(["'"+i+"'" for i in LIST_OF_DESTINATION_RESOURCE_LEAVES])
    conditions = "COLL_NAME = '{0}' AND DATA_NAME = '{1}' AND RESC_NAME in ({2})".format(coll_name, data_name, resc_names)

    ret_val = callback.msiMakeGenQuery("DATA_SIZE", conditions, irods_types.GenQueryInp())
    genQueryInp = ret_val['arguments'][2]

    ret_val = callback.msiExecGenQuery(genQueryInp, irods_types.GenQueryOut())
    genQueryOut = ret_val['arguments'][1]
    previously_registered_replicas = genQueryOut.rowCnt

    logical_size = int(genQueryOut.sqlResult[0].row(0)) if previously_registered_replicas > 0 else None
    return (previously_registered_replicas, logical_size)

# Dynamic Policy Enforcement Point for rsModDataObjMeta
# Required for Use Case 3
def pep_api_mod_data_obj_meta_post(rule_args, callback, rei):
    args = rule_args[2]
    logical_path = str(args['logical_path'])

    # do nothing if we're not on SCANNED_RESOURCE
    target_replica_number = args['replica_number']
    result_count, target_resource = get_resource_name_by_replica_number(callback, logical_path, target_replica_number)


    if resource_is_not_target(target_resource):
        #print("pep_api_mod_data_obj_meta_post - resources are not a match")
        return

    # determine whether a replica already exists
    previously_registered_replicas, logical_size = get_existing_replica_size_from_destination(callback, logical_path)
    if previously_registered_replicas > 0:
        # replica already exists, get the new replica's size
        physical_size = int(args['dataSize'])
        # compare sizes
        if physical_size != logical_size:
            #print("pep_api_mod_data_obj_meta_post - Use Case 3")
            # update if not equal
            status = 0
            params = "irodsAdmin=++++updateRepl=++++all="
            callback.msiDataObjRepl(logical_path,params,status)
        else:
            # match, do nothing
            #print("pep_api_mod_data_obj_meta_post - data sizes match, no action taken")
            pass

# Dynamic Policy Enforcement Point for rsPhyPathReg
# Required for Use Cases 0, 1, 2
def pep_api_phy_path_reg_post(rule_args, callback, rei):
    args = rule_args[2]
    logical_path = str(args.objPath)
    resc_hier = get_value_for_key(args.condInput,"resc_hier")

    if 'KEY NOT FOUND' != resc_hier:
        target_resource = resc_hier.split(';')[0]
    else:
        target_resource = get_value_for_key(args.condInput,"destRescName")

    # do nothing if we're not on SCANNED_RESOURCE
    if resource_is_not_target(target_resource):
        #print("pep_api_phy_path_reg_post - resources are not a match")
        return


    # determine whether a replica already exists
    previously_registered_replicas, logical_size = get_existing_replica_size_from_destination(callback, logical_path)
    if previously_registered_replicas > 0:
        # replica already exists, stat the file system to get the new replica's size
        physical_path = get_value_for_key(args.condInput,"filePath")

        data_size_str = get_value_for_key(args.condInput,"dataSize")
        physical_size = (int(data_size_str) if data_size_str != 'KEY NOT FOUND' else os.stat(physical_path).st_size)
        # compare new replica size with existing replica size
        if physical_size != logical_size:
            # Use Case 2 - Sizes Do Not Match, Update All Existing Replicas
            #print("pep_api_phy_path_reg_post - Use Case 2")
            status = 0
            params = "irodsAdmin=++++updateRepl=++++all="
            callback.msiDataObjRepl(logical_path,params,status)
        else:
            # Use Case 1 - Sizes Match, Do Nothing
            #print("pep_api_phy_path_reg_post - Use Case 1")
            pass
    else:
        # Use Case 0 - Replicate New Data to DESTINATION_RESOURCE_ROOT
        #print("pep_api_phy_path_reg_post - Use Case 0")
        status = 0
        params = "irodsAdmin=++++destRescName={0}".format(DESTINATION_RESOURCE_ROOT)
        callback.msiDataObjRepl(logical_path,params,status)
