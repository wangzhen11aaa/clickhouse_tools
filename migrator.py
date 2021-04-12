#!/usr/local/bin/python2

import common.option_parser as option_parser
from utils.clickhouse_connector import ClickHouseConnector
from clickhouse_driver import Client
from common.mc_logging import mc_logger

if __name__ == "__main__":
    # We only use double-dash (long) option, args will be empty.
    (op, args) = option_parser.parse_option()

    if not op.source_cluster_ip or not op.target_cluster_ip:
        mc_logger.error("source or target cluster ip can not be empty! \n use --help for prompt") 
        exit(-1) 
    if not op.zk_path_suffix: 
        mc_logger.error("New root path for replicatedMerge Engine must not be emtpy! \n use --help for prompt")
        exit(-1)
    # Initialize the ClickHouseConnector
    ClickHouseConnector.initialize(op)
    ClickHouseConnector.get_databases()
    
    ClickHouseConnector.do_create_databases()
    ClickHouseConnector.do_create_tables()
        # First create database on target cluster
     #print "len(args) " + str(len(args))
    #print "args" + "".join(args) 
    #TableSchemaModifier.modify("CREATE TABLE etl.t_report_main_city_desire_66_local (`id` Int64, `main_city_id` Int64, `main_city_name` String, `turnover` Int64, `alive` Int64, `turnover_vege` Int64, `alive_vege` Int64, `turnover_meat_bird` Int64, `alive_meat_bird` Int64, `turnover_rice` Int64, `alive_rice` Int64, `turnover_drink` Int64, `alive_drink` Int64, `is_deleted` Int64, `create_time` Int64, `update_time` Int64, `v` Int64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse-rt/etl/tables/{layer}-{shard}/t_report_main_city_desire_66_local', '{replica}', v) ORDER BY id SETTINGS index_granularity = 8192", "etl", "t_report_main_city_desire_66_local")
    #TableSchemaModifier.modify("CREATE TABLE etl.t_report_main_city_desire_66 (`id` Int64, `main_city_id` Int64, `main_city_name` String, `turnover` Int64, `alive` Int64, `turnover_vege` Int64, `alive_vege` Int64, `turnover_meat_bird` Int64, `alive_meat_bird` Int64, `turnover_rice` Int64, `alive_rice` Int64, `turnover_drink` Int64, `alive_drink` Int64, `is_deleted` Int64, `create_time` Int64, `update_time` Int64, `v` Int64) ENGINE = Distributed(replicated_stock, etl, t_report_main_city_desire_66_local, id)", "etl", "t_report_main_city_desire_66")   
