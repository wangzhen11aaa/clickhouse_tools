# clickhouse_tools

## migrator_meta_data.py

This tool is used to migrate all tables's metadata from one cluster to an new cluster.
For this project, we just add some suffix after the root paths of zk.
You can run it simply by
  python3 migrator_meta_data.py --source-cluster-ip xxx --source-cluster-port xx --target-cluster-ip --target-cluster-port --zk-path-suffix

## validator.py
This tool is used to validate whether these data migrated are "equal" to origin cluster.
You can run it simply by
  python3 validate.py database_name source_cluster_ip target_cluster_ip

## migrate_v1.py
This tool is used to migrate data from cluster to another cluster.
You can run it simply by
  python3 migrate_v1.py database_name source_cluster_ips target_cluster_ips concurrent_nums

This tool will insert select * from way, and select from source ck cluster uniformly and insert into target ck cluster uniformly too.

## clickhouse_migrate_via_scp.py && clickhouse_migrate_multiple_thread.py
These tools will use scp method to migrate data locally(from one node A to node A'), first it will compress the data under the data directory, then scp to corresponding
node, then move these data to detached, then attach these data.

