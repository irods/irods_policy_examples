import os
from genquery import *
from subprocess import Popen, PIPE

############

nmc_target_resource = 'nmc-analysis'
nmc_remote_hostname = 'localhost'
nmc_change_permission_script = 'nmc_set_permissions.sh'
nmc_a = 'nmc'
nmc_v = 'analysis'
nmc_u = ''
nmc_enqueued = '{}::enqueued'.format(nmc_a)

############

delay_condition = '<PLUSET>1s</PLUSET><INST_NAME>irods_rule_engine_plugin-{0}-instance</INST_NAME>'

# Add three rules to the delay queue, and run them periodically, forever
def nmc_add_sweeper_to_queue(rule_args, callback, rei):
    global delay_condition
    ruletext  = 'callback.nmc_replicate_dataobjs_under_tagged_collections();'
    ruletext += 'callback.nmc_replicate_tagged_dataobjs();'
    ruletext += 'callback.nmc_trim_untagged_dataobjs_on_target_resource();'
    print('queuing', ruletext)
    callback.delayExec(delay_condition.format('python')+'<EF>REPEAT FOR EVER</EF>', ruletext, '')

# Replicate any data objects under a tagged collection that are not already cleanly on the target resource
# Run forever as part of the sweeper
def nmc_replicate_dataobjs_under_tagged_collections(rule_args, callback, rei):
    global nmc_target_resource
    global nmc_a
    global nmc_v
    global nmc_u
    global nmc_enqueued
    global nmc_remote_hostname
    global nmc_change_permission_script
    main_select = "select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r"
    main_select_metadata = ", R_META_MAIN m, R_OBJT_METAMAP o"
    main_conditionals = "where d.coll_id = c.coll_id and r.resc_id = d.resc_id and d.data_is_dirty = '1'"
    main_conditionals += " and (c.coll_name = ? or c.coll_name like ?) and r.resc_name"
    main_ignore_enqueued = "and not (m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = '')"
    # find tagged collections
    for result in row_iterator("COLL_NAME",
                               "META_COLL_ATTR_NAME = '{0}' and META_COLL_ATTR_VALUE = '{1}' and META_COLL_ATTR_UNITS = '{2}'".format(nmc_a, nmc_v, nmc_u),
                               AS_LIST,
                               callback):
#        print(result)
        logical_path = result[0]
        # find data objects under this collection (not enqueued) without a good replica on the target resource
        query = '{0}{1} {2} != ? {3} except ({0} {2} = ?)'.format(main_select, main_select_metadata, main_conditionals, main_ignore_enqueued)
#        print('query',query)
        p = Popen(['iquest', '--no-page', '--sql', query,
                   logical_path, logical_path+'%', nmc_target_resource,
                   nmc_enqueued, 'true',
                   logical_path, logical_path+'%', nmc_target_resource],
                   stdout=PIPE)
        out, err = p.communicate()
        if out == 'No rows found\n':
            return
        if (err):
            print('err', err)
        results = out.split('----\n')
        for i in results:
            parts = i.split('\n')[:-1]
            path_to_replicate = '/'.join(parts)
#            print(path_to_replicate)
            # replicate
            ruletext  = "msiDataObjRepl('{0}','destRescName={1}++++irodsAdmin=',*status);".format(path_to_replicate, nmc_target_resource)
            # remove as enqueued
            ruletext += "msiModAVUMetadata('-d','{0}','rm','{1}','{2}','');".format(path_to_replicate, nmc_enqueued, 'true')
            # set permissions on target replica
            # TODO with remote... causing this
            # Nov  4 14:15:57 pid:14724 remote addresses: 127.0.0.1, ::1 ERROR: caught python exception: TypeError: No to_python (by-value) converter found for C++ type: int
            # Nov  4 14:15:57 pid:14724 remote addresses: 127.0.0.1, ::1 ERROR: Rule Engine Plugin returned [0].
            # but working...
            ruletext += "remote('{0}',''){{msiExecCmd('{1}','{2}','null','null','null',*status)}}".format(nmc_remote_hostname, nmc_change_permission_script, path_to_replicate)
            print('queuing', ruletext)
            # mark as enqueued
            callback.msiModAVUMetadata('-d',path_to_replicate,'set',nmc_enqueued,'true','')
            # enqueue
            callback.delayExec(delay_condition.format('irods_rule_language'), ruletext, '')

# Replicate any tagged data objects that are not already cleanly on the target resource
# Run forever as part of the sweeper
def nmc_replicate_tagged_dataobjs(rule_args, callback, rei):
    global nmc_target_resource
    global nmc_a
    global nmc_v
    global nmc_u
    global nmc_enqueued
    global nmc_remote_hostname
    global nmc_change_permission_script
    main_select = "select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_META_MAIN m, R_OBJT_METAMAP o, R_RESC_MAIN r"
    main_conditionals =  "where d.coll_id = c.coll_id and o.object_id = d.data_id and o.meta_id = m.meta_id and r.resc_id = d.resc_id "
    main_conditionals += "and m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = '' and d.data_is_dirty = '1' and r.resc_name"
    main_ignore_enqueued = "and not (m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = '')"
    query = '{0} {1} != ? {2} except ({0} {1} = ?)'.format(main_select, main_conditionals, main_ignore_enqueued)
#    print('query', query)
    p = Popen(['iquest', '--no-page', '--sql', query,
#               nmc_a, nmc_v, nmc_u, nmc_target_resource,
#               nmc_a, nmc_v, nmc_u, nmc_target_resource],
# workaround, iquest cannot handle empty strings as parameters to specific queries
# so, hardcoded above in the main_conditionals with empty string for m.meta_attr_unit
               nmc_a, nmc_v, nmc_target_resource,
               nmc_enqueued, 'true',
               nmc_a, nmc_v, nmc_target_resource],
               stdout=PIPE)
    out, err = p.communicate()
#    print('out', out)
    if out == 'No rows found\n':
        return
    if (err):
        print('err', err)
    results = out.split('----\n')
    for i in results:
        parts = i.split('\n')[:-1]
        path_to_replicate = '/'.join(parts)
        # replicate
        ruletext  = "msiDataObjRepl('{0}','destRescName={1}++++irodsAdmin=',*status);".format(path_to_replicate, nmc_target_resource)
        # remove as enqueued
        ruletext += "msiModAVUMetadata('-d','{0}','rm','{1}','{2}','');".format(path_to_replicate, nmc_enqueued, 'true')
        # set permissions on target replica
        ruletext += "remote('{0}',''){{msiExecCmd('{1}','{2}','null','null','null',*status)}}".format(nmc_remote_hostname, nmc_change_permission_script, path_to_replicate)
        print('queuing', ruletext)
        # mark as enqueued
        callback.msiModAVUMetadata('-d',path_to_replicate,'set',nmc_enqueued,'true','')
        # enqueue
        callback.delayExec(delay_condition.format('irods_rule_language'), ruletext, '')

# Trim any untagged data objects from the target_resource
# Run forever as part of the sweeper
def nmc_trim_untagged_dataobjs_on_target_resource(rule_args, callback, rei):
    global nmc_target_resource
    global nmc_a
    global nmc_v
    global nmc_u
    global nmc_enqueued
    for result in row_iterator("COLL_NAME, DATA_NAME",
                               "DATA_RESC_NAME = '{0}'".format(nmc_target_resource),
                               AS_LIST,
                               callback):
#        print('nmc_trim_untagged_dataobjs_on_target_resource - found', result)
        logical_path = "{0}/{1}".format(result[0],result[1])
        if not (nmc_dataobj_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u) or
                nmc_dataobj_has_avu(callback, logical_path, nmc_enqueued, 'true', '') or
                nmc_any_recursive_parent_path_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u)):
            # trim
            ruletext = "msiDataObjTrim('{0}', '{1}', 'null', '1', '1', 0);".format(logical_path, nmc_target_resource)
            # remove as enqueued
            ruletext += "msiModAVUMetadata('-d','{0}','rm','{1}','{2}','');".format(logical_path, nmc_enqueued, 'true')
            # log
            ruletext += "writeLine('serverLog','msiDataObjTrim [{0}] from [{1}] complete');".format(logical_path, nmc_target_resource)
            print('queuing', ruletext)
            # mark as enqueued
            callback.msiModAVUMetadata('-d',logical_path,'set',nmc_enqueued,'true','')
            # enqueue
            callback.delayExec(delay_condition.format('irods_rule_language'), ruletext, '')

# Check whether a dataobj has a particular AVU
# return boolean
def nmc_dataobj_has_avu(callback, logical_path, a, v, u):
    logical_path = str(logical_path)
#    print('checking... ' + str(logical_path))
    collection_name = os.path.dirname(logical_path)
    dataobject_name = os.path.basename(logical_path)
    for result in row_iterator("META_DATA_ATTR_NAME, META_DATA_ATTR_VALUE, META_DATA_ATTR_UNITS",
                               "COLL_NAME = '{0}' and DATA_NAME = '{1}' and META_DATA_ATTR_NAME = '{2}' and META_DATA_ATTR_VALUE = '{3}' and META_DATA_ATTR_UNITS = '{4}'".format(collection_name, dataobject_name, a, v, u),
                               AS_LIST,
                               callback):
        return True
    return False

# Check whether any parent, recursively, has a particular AVU
# return collection logical_path or False
def nmc_any_recursive_parent_path_has_avu(callback, logical_path, a, v, u):
    logical_path = str(logical_path)
    found_avu = False
    while (logical_path != '/'):

#        print('checking... ' + str(logical_path))
        for result in row_iterator("META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE, META_COLL_ATTR_UNITS",
                                   "COLL_NAME = '{0}' and META_COLL_ATTR_NAME = '{1}' and META_COLL_ATTR_VALUE = '{2}' and META_COLL_ATTR_UNITS = '{3}'".format(logical_path, a, v, u),
                                   AS_LIST,
                                   callback):
            found_avu = logical_path
        if found_avu:
            break
        logical_path = os.path.dirname(logical_path)
    return found_avu

# Check whether any data objects below this path have a particular AVU
# return data object logical_path or False
def nmc_any_descendent_dataobject_path_has_avu(callback, logical_path, a, v, u):
    logical_path = str(logical_path)
    found_avu = False
    for result in row_iterator("COLL_NAME, DATA_NAME",
                               "COLL_NAME = '{0}' || like '{0}/%' and META_DATA_ATTR_NAME = '{1}' and META_DATA_ATTR_VALUE = '{2}' and META_DATA_ATTR_UNITS = '{3}'".format(logical_path, a, v, u),
                               AS_LIST,
                               callback):
        descendent = '{0}/{1}'.format(result[0], result[1])
        return descendent
    return False

# Check whether any subcollections below this path have a particular AVU
# return subcollection logical_path or False
def nmc_any_descendent_subcollection_path_has_avu(callback, logical_path, a, v, u):
    logical_path = str(logical_path)
    found_avu = False
    for result in row_iterator("COLL_NAME",
                               "COLL_NAME = '{0}' || like '{0}/%' and META_COLL_ATTR_NAME = '{1}' and META_COLL_ATTR_VALUE = '{2}' and META_COLL_ATTR_UNITS = '{3}'".format(logical_path, a, v, u),
                               AS_LIST,
                               callback):
        descendent = result[0]
        return descendent
    return False

# Call msiExit and send a good error message to the client
def nmc_halt_if_tagged(callback, logical_path):
    global nmc_a
    global nmc_v
    global nmc_u
    # -41000 SYS_DELETE_DISALLOWED
    error_message =  "SYS_DELETE_DISALLOWED\n"
    error_message += " NMC Policy - Marked as 'Analysis' - Cannot Remove\n"
    error_message += " {0}\n"
    # check this data object
    if nmc_dataobj_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u):
        error_message += " If analysis is complete:\n $ imeta rm -d {0} {1} {2} {3}"
        callback.msiExit('-41000', error_message.format(logical_path, nmc_a, nmc_v, nmc_u))
    # check self and all parent collections
    tagged_collection = nmc_any_recursive_parent_path_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u)
    if tagged_collection:
        error_message += " If analysis is complete:\n $ imeta rm -C {0} {1} {2} {3}"
        callback.msiExit('-41000', error_message.format(tagged_collection, nmc_a, nmc_v, nmc_u))
    # check descendent data objects
    tagged_descendent_dataobject = nmc_any_descendent_dataobject_path_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u)
    if tagged_descendent_dataobject:
        error_message += " If analysis is complete:\n $ imeta rm -d {0} {1} {2} {3}"
        callback.msiExit('-41000', error_message.format(tagged_descendent_dataobject, nmc_a, nmc_v, nmc_u))
    # check descendent subcollections
    tagged_descendent_collection = nmc_any_descendent_subcollection_path_has_avu(callback, logical_path, nmc_a, nmc_v, nmc_u)
    if tagged_descendent_collection:
        error_message += " If analysis is complete:\n $ imeta rm -C {0} {1} {2} {3}"
        callback.msiExit('-41000', error_message.format(tagged_descendent_collection, nmc_a, nmc_v, nmc_u))

# Disallow trimming of a replica if tagged
def pep_api_data_obj_trim_pre(rule_args, callback, rei):
    logical_path = str(rule_args[2].objPath)
#    print('trim pre', logical_path)
    nmc_halt_if_tagged(callback, logical_path)

# Disallow removal of a data object if tagged
def pep_api_data_obj_unlink_pre(rule_args, callback, rei):
    logical_path = str(rule_args[2].objPath)
#    print('unlink pre', logical_path)
    nmc_halt_if_tagged(callback, logical_path)

# Disallow removal of a collection if tagged
def pep_api_rm_coll_pre(rule_args, callback, rei):
    logical_path = str(rule_args[2].collName)
#    print('rm coll pre', logical_path)
    nmc_halt_if_tagged(callback, logical_path)

def nmc_halt_if_enqueued(callback, logical_path):
    global nmc_enqueued
    # -41000 SYS_DELETE_DISALLOWED
    error_message =  "SYS_DELETE_DISALLOWED\n"
    error_message += " NMC Policy - Marked as 'Enqueued' - Cannot Remove Metadata\n"
    error_message += " {0}\n"
    # check this data object
    if nmc_dataobj_has_avu(callback, logical_path, nmc_enqueued, 'true', ''):
        callback.msiExit('-41000', error_message.format(logical_path))
    # check descendent data objects
    tagged_descendent_dataobject = nmc_any_descendent_dataobject_path_has_avu(callback, logical_path, nmc_enqueued, 'true', '')
    if tagged_descendent_dataobject:
        callback.msiExit('-41000', error_message.format(tagged_descendent_dataobject))

# Disallow removal of tagging from data object if data object is marked as enqueued
# Disallow removal of tagging from collection if any data objects below it are marked as enqueued
def pep_api_mod_avu_metadata_pre(rule_args, callback, rei):
    global nmc_a
    global nmc_v
    global nmc_u
    avu_operation = rule_args[2]['arg0']
    avu_type = rule_args[2]['arg1']
    logical_path = rule_args[2]['arg2']
    a = rule_args[2]['arg3']
    v = rule_args[2]['arg4']
    try:
        u = rule_args[2]['arg5']
    except KeyError:
        u = ''
    # rm tag from data object or collection
    if (avu_operation in ['rm'] and avu_type in ['-d', '-C'] and [a, v, u] == [nmc_a, nmc_v, nmc_u]):
        nmc_halt_if_enqueued(callback, logical_path)
