#!/usr/bin/python2

from common.mc_logging import mc_logger

import re

"""
    TableSchemaModifier class
    Pass table schema, then add some part and modify path in zookeeper.
    Then return the result.
    e.g
    1. For local table:
        CREATE TABLE etl.t_report_main_city_desire_66_local 
        (`id` Int64, ... ,`v` Int64) 
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse-rt/etl/tables/{layer}-{shard}/t_report_main_city_desire_66_local', 
        '{replica}', v) ORDER BY id SETTINGS index_granularity = 8192
        =>
        CREATE TABLE IF NOT EXISTS etl.t_report_main_city_desire_66_local on cluster replicated_stock (`id` Int64, ..., `v` Int64 )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse-rt-new/etl/tables/{layer}-{shard}/t_report_main_city_desire_66_local', 
        '{replica}', v) ORDER BY id SETTINGS index_granularity = 8192
    2. For distributed table:
        CREATE TABLE etl.t_report_main_city_desire_66 (`id` Int64,..., `v` Int64) ENGINE = 
        Distributed(replicated_stock, etl, t_report_main_city_desire_66_local, id)
        =>
        CREATE TABLE etl.t_report_main_city_desire_66 on cluster replicated_stock (`id` Int64, ... , `v` Int64) ENGINE=
        Distributed(replicated_stock, etl, t_report_main_city_desire_66_local, id)
"""


class TableSchemaModifier:

    MergeTreePattern = "(?<=MergeTree\('/).*?(?=/)"
    # This method will modify these schema.
    @classmethod
    def modify(cls, table_schema, database, table, zk_path_suffix_to_append):
        mc_logger.info("table_schema :%s " % (table_schema))
        mc_logger.info("database : %s" % (database))
        mc_logger.info("table : %s"  %(table))
        database_table_str = database + '.' + table
        table_schema = table_schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").replace(database_table_str, database_table_str + " on cluster replicated_stock")
        
        # If table is distributed table.
        if "Distributed" in table_schema:
            pass
        else:
            # Use re to match the first part of the 
            findall_ret = re.findall(cls.MergeTreePattern, table_schema)
            if not findall_ret:
                mc_logger.error("table_schema %s is bad" %(table_schema))
            else:
                try:
                    mc_logger.info("findall_ret.groups's length : %s "%(len(findall_ret)) )
                    assert len(findall_ret) == 1
                except AssertionError:
                    mc_logger.error("Bad Matching " + " ".join(findall_ret))
                mc_logger.info("Good Matching  " + (findall_ret[0] ))
                table_schema = table_schema.replace(findall_ret[0], findall_ret[0] + zk_path_suffix_to_append)
        mc_logger.info("Target target_schema %s" %(table_schema))
        return table_schema

if __name__ == "__main__":
    #(op, args) = parse_option()
       
    TableSchemaModifier.modify("CREATE TABLE etl.t_report_main_city_desire_66_local (`id` Int64, `main_city_id` Int64, `main_city_name` String, `turnover` Int64, `alive` Int64, `turnover_vege` Int64, `alive_vege` Int64, `turnover_meat_bird` Int64, `alive_meat_bird` Int64, `turnover_rice` Int64, `alive_rice` Int64, `turnover_drink` Int64, `alive_drink` Int64, `is_deleted` Int64, `create_time` Int64, `update_time` Int64, `v` Int64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse-rt/etl/tables/{layer}-{shard}/t_report_main_city_desire_66_local', '{replica}', v) ORDER BY id SETTINGS index_granularity = 8192", "etl", "t_report_main_city_desire_66_local")
    TableSchemaModifier.modify("CREATE TABLE etl.t_report_main_city_desire_66 (`id` Int64, `main_city_id` Int64, `main_city_name` String, `turnover` Int64, `alive` Int64, `turnover_vege` Int64, `alive_vege` Int64, `turnover_meat_bird` Int64, `alive_meat_bird` Int64, `turnover_rice` Int64, `alive_rice` Int64, `turnover_drink` Int64, `alive_drink` Int64, `is_deleted` Int64, `create_time` Int64, `update_time` Int64, `v` Int64) ENGINE = Distributed(replicated_stock, etl, t_report_main_city_desire_66_local, id)", "etl", "t_report_main_city_desire_66")   

