# UNC Neuroscience Microscopy Core (NMC) at the UNC Neuroscience Center at the UNC-Chapel Hill School of Medicine

As part of the BRAIN-I project, this iRODS policy set defines the policies for data analysis, replication, and retention in the NMC.

There are two parts of the policy managing the data flow within the iRODS Zone:

## Automatic

The [iRODS Storage Tiering Framework](https://github.com/irods/irods_capability_storage_tiering) is handling newly ingested data and moving it into the long-term storage housed at [RENCI](https://renci.org/).
RENCI is providing storage and visualization tooling that prioritizes that local, long-term storage.

## Manual

When NMC staff want to run local analysis on data already in the iRODS namespace, they can 'tag' the data of interest, and this policy will manage the replication to their local machine, set permissions, and prevent removal of that data from the system until it has been 'untagged'.
Once 'untagged', the data will be trimmed from the researchers' local storage and remain housed only in long-term storage at RENCI.

# Installation

As the **irods** service account:
```
echo "from nmc_analysis_sweeper import *" > /etc/irods/core.py
cp nmc_analysis_sweeper.py /etc/irods/
cp nmc_set_permissions.sh /var/lib/irods/msiExecCmd_bin/
chmod +x /var/lib/irods/msiExecCmd_bin/nmc_set_permissions.sh
iadmin asq "select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r where c.coll_id = d.coll_id and d.resc_id = r.resc_id and d.data_is_dirty = '1' and r.resc_name != ? and c.coll_name = ? union select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r where c.coll_id = d.coll_id and d.resc_id = r.resc_id and d.data_is_dirty = '1' and r.resc_name != ? and c.coll_name like ? except select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r where c.coll_id = d.coll_id and d.resc_id = r.resc_id and d.data_is_dirty = '1' and r.resc_name = ? except select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_META_MAIN m, R_OBJT_METAMAP o where c.coll_id = d.coll_id and d.data_id = o.object_id and o.meta_id = m.meta_id and m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = ''" nmc_find_data_objects_below
iadmin asq "select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r, R_META_MAIN m, R_OBJT_METAMAP o where c.coll_id = d.coll_id and d.resc_id = r.resc_id and d.data_is_dirty = '1' and r.resc_name != ? and d.data_id = o.object_id and o.meta_id = m.meta_id and m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = '' except select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_RESC_MAIN r where c.coll_id = d.coll_id and d.resc_id = r.resc_id and d.data_is_dirty = '1' and r.resc_name = ? except select c.coll_name, d.data_name from R_COLL_MAIN c, R_DATA_MAIN d, R_META_MAIN m, R_OBJT_METAMAP o where c.coll_id = d.coll_id and d.data_id = o.object_id and o.meta_id = m.meta_id and m.meta_attr_name = ? and m.meta_attr_value = ? and m.meta_attr_unit = ''" nmc_find_tagged_data_objects
irule -r irods_rule_engine_plugin-python-instance nmc_add_sweeper_to_queue null null
```

# Testing

```
$ git clone https://github.com/bats-core/bats-core
$ time bash bats-core/bin/bats test_nmc_analysis.bats
 ✓ tag a collection
 ✓ tag a data object
 ✓ untag a collection
 ✓ untag a data object
 ✓ overwrite a tagged data object
 ✓ overwrite a data object under a tagged collection
 ✓ trim a tagged data object - DISALLOWED
 ✓ trim a data object under a tagged collection - DISALLOWED
 ✓ remove a tagged data object - DISALLOWED
 ✓ remove a tagged collection - DISALLOWED
 ✓ remove a data object under a tagged collection - DISALLOWED
 ✓ remove a collection under a tagged collection - DISALLOWED
 ✓ remove a collection containing a tagged data object - DISALLOWED
 ✓ remove a collection containing a tagged collection - DISALLOWED
 ✓ untag an enqueued data object - DISALLOWED
 ✓ untag a collection with an enqueued descendent data object - DISALLOWED

16 tests, 0 failures


real    2m4.745s
user    0m8.606s
sys     0m2.172s
```

# TODO

 - possible Popen reuse
 - possible 41000 error code reuse
