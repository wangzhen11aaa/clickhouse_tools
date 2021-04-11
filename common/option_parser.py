#!/usr/bin/python2

# All the options and process

from optparse import OptionParser

# Define the options and default value, parse these options

def parse_option():
    parser = OptionParser()
    parser.add_option("--source-cluster-ip", dest="source_cluster_ip", type = 'string', default = '', help = 'clickhouse source cluster ip, default localhost')
    parser.add_option("--source-cluster-port", dest="source_cluster_port", type = 'int', default=9000, help='clickhouse source cluster port, default 9000')
    
    #parser.add_option("--migrate-database", dest="migrate_database", type = 'string', default = '', help = 'If not set, we iterate all tables of the whole databases')
    
    parser.add_option("--target-cluster-ip", dest="target_cluster_ip", type = 'string', default = '', help = 'clickhouse target cluster ip, default localhost')
    parser.add_option("--target-cluster-port", dest="target_cluster_port", type = 'int', default=9000, help='clickhouse target cluster port, default 9000')
    return parser.parse_args()