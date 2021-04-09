#!/usr/bin/python2

from clickhouse_driver import Client
from common.mc_logging import mc_logger
"""
    This ClickHouseConnector class is responsible for connecting the 
    source clickhouse database and target clickhouse database and migrate the
    metadata both include database and table.
    We can use this class as a singleton, so we only have classmethod instead.
"""
class ClickHouseConnector:
    client_for_source = None
    client_for_target = None
    
    source_ck_ip = None
    source_ck_port = None
    target_ck_ip = None
    target_ck_port =None

    # SQL COMMAND PART
    SHOW_DATABASE="SHOW DATABASES"
    # use database
    USE_DATABASE="use %s"
    SHOW_TABLES = "SHOW TABLES"
    SHOW_TABLE_SCHEMA="SHOW CREATE TABLE %s.%s"
    
    # DDL
    CREATE_DATABASE_QUERY="CREATE DATABASE %s ON cluster replicated_stock"
    
    """
       Store all the database in the source clickhouse cluster.
       e.g
       >>> client.execute("SHOW DATABASES")
        [(u'cloudatlas',), (u'csp',), (u'customer_center',), (u'default',), (u'dim',), 
        (u'hrdb',), (u'msc',), (u'system',), (u'test',)]
    """    
    source_database_list = []
    
    """
        Store all the tables of one database.
        e.g
        >>> client.execute("use cloudatlas")
            []
        >>> client.execute("show tables")
            [(u'cloudatlas_user',), (u'cloudatlas_user_group',), (u'cloudatlas_user_group_local',), (u'cloudatlas_user_local',), (u'dim_sku',), (u'trace_events',)]
    """

    table_list = []
    local_table_list = []
    no_local_table_list = []
    # Initialize all the information
    @classmethod
    def initialize(cls, op):
        cls.source_ck_ip = op.source_cluster_ip
        cls.source_ck_port = op.source_ck_port
        cls.target_ck_ip = op.target_ck_ip
        cls.target_ck_port = op.target_ck_port

    @classmethod
    def get_source_connection(cls):
        # Use default arguments for Connection, when change the ip, client will be replaced and old connection will be 
        # disconnected automatically.
        cls.client_for_source = Client(host=cls.source_ck_ip, port=cls.source_ck_port)

    @classmethod
    def get_target_connection(cls):
        cls.client_for_target = Client(host=cls.target_ck_ip, port=cls.target_ck_port)

    # Common method for all the query result of ClickHouse Client.
    @classmethod
    def get_rid_of_tuple_in_list(cls, l):
        l = [item[0] for item in l]

    @classmethod
    def get_databases(cls):
        cls.source_database_list = cls.client_for_source.execute(cls.SHOW_DATABASE)
        cls.get_rid_of_tuple_in_list(cls.source_database_list)
        
    @classmethod
    def get_tables_and_split(cls, database):
        cls.client_for_source.execute(cls.USE_DATABASE %database)
        cls.table_list = cls.client_for_source.execute(cls.SHOW_TABLES)
        cls.get_rid_of_tuple_in_list(cls.table_list)
        cls.local_table_list = [filter(lambda item: ("_local" in item), cls.table_list)]
        cls.no_local_table_list = [filter(lambda item:("_local" not in item), cls.table_list)]
 
    # Return the table schema.
    @classmethod
    def fetch_table_schema(cls, database, table):
        mc_logger.info("Fetch table schema, %s.%s" %(database, table))
        return cls.client_for_source.execute(cls.SHOW_TABLE_SCHEMA %(database, table))

    @classmethod
    def create_database(cls, database):
        mc_logger.info("Create database, Ready Go!")
        cls.client_for_target.execute(cls.CREATE_DATABASE_QUERY %(database))

if __name__ == "__main__":
    ClickHouseConnector.source_ck_ip = "192.168.254.43"
    ClickHouseConnector.source_ck_port = 9001
    ClickHouseConnector.target_ck_ip = "192.168.254.43"
    ClickHouseConnector.target_ck_port = 9001
    ClickHouseConnector.get_source_connection()
    ClickHouseConnector.get_target_connection()
    ClickHouseConnector.get_databases()
    ClickHouseConnector.create_database("default")
    print "database " + str(ClickHouseConnector.source_database_list)
    print "tables " + str(ClickHouseConnector.table_list)
    print "table schema " + str(ClickHouseConnector.fetch_table_schema("cloudatlas", "cloudatlas_user_group_local"))
    