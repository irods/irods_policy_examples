# iRODS Asynchronous Replication Policy
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is a policy implementation which provides the ability to map specific
# source resources to specific destination resources for asynchronous replication.
#
# These source and destination resources are kept in a table (*resource_map) which
# maintains the single point of truth for the mapping, keeping the business logic
# simple and providing a generic mechanism for asynchronous replication.
#
# The pep_api_data_obj_put_post() PEP is defined and can queue the new data object
# for replication using the msiDataObjRepl() microservice.
#

# The code to return to the Rule Engine Plugin Framework
# so it looks for additional PEPs to fire.
# Available in server version >= 4.2.6
RULE_ENGINE_CONTINUE() { 5000000 }

# Single point of truth for an error value
get_error_value(*err) { *err = "ERROR_VALUE" }

# Split the leaf from a resource hierarchy string of the form 'A;B;C;D'.
# If the root resource is the last one, it is returned as the leaf and
# *err_val is assigned as the resulting hierarchy.
split_leaf_from_resource_hierarchy(*hier, *root, *leaf) {
    get_error_value(*err_val)

    *root = trimr(*hier, ";")
    if(strlen(*root) == strlen(*hier)) {
        *leaf = *root
        *root = *err_val
    } else {
        *leaf = substr(*hier, strlen(*root)+1, strlen(*hier))
    }
}

get_destination_resource(*resc_name, *dest_resc) {
    # Prepare the out variable should we not find a mapping.
    get_error_value(*dest_resc)

    # Create a map of source and destination resources.
    # This is the single point of truth for replicating
    # from source to destination resources.  Adjust as
    # requirements change in the future.
    *resource_map = list(
                        list("source0", "destination0"),
                        list("source1", "destination1"),
                        list("source2", "destination2"),
                        list("source3", "destination3")
                    )
    foreach(*e in *resource_map) {
        *src = elem(*e, 0)
        if(*resc_name == *src) {
            *dest_resc = elem(*e, 1)
        }
    }
}

schedule_data_object_replication(*logical_path, *source_resource, *destination_resource) {
    *num_threads = "2"

    *key_value_pairs = "numThreads="++str(*num_threads)
    *key_value_pairs = *key_value_pairs ++ "++++rescName=*source_resource"
    *key_value_pairs = *key_value_pairs ++ "++++destRescName=*destination_resource"
    writeLine("serverLog", "Scheduling Replication :: [*logical_path] [*key_value_pairs]")

    # Consider adding retry parameters to delay() : <EF>1s DOUBLE UNTIL SUCCESS OR 10 TIMES<EF>
    delay("<PLUSET>1s</PLUSET>") {
        writeLine("serverLog", "Replicating :: [*logical_path] [*key_value_pairs]")
        msiDataObjRepl(*logical_path, *key_value_pairs, *status)
    }
}

pep_api_data_obj_put_post(*INSTANCE_NAME, *COMM, *DATAOBJINP, *BBUFF, *PORTAL_OPR_OUT) {
    get_error_value(*err_val)
    *resc_hier    = *DATAOBJINP.resc_hier
    *logical_path = *DATAOBJINP.obj_path

    while(*err_val != *resc_hier) {
        # Separate the leaf from the rest of the hierarchy.
        # Overwrite *resc_hier with remaining hierarchy for next iteration.
        split_leaf_from_resource_hierarchy(*resc_hier, *resc_hier, *leaf)

        writeLine("serverLog", "Processing Leaf [*leaf]")

        # Map the leaf to a destination resource name.
        get_destination_resource(*leaf, *destination_resource)
        if(*err_val != *destination_resource) {
            writeLine("serverLog", "Replicate [*logical_path] from [*leaf] to [*destination_resource]")
            schedule_data_object_replication(*logical_path, *leaf, *destination_resource)
        }
    }

    # Return continuation code
    RULE_ENGINE_CONTINUE;
}
