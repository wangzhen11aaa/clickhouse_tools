#!/usr/bin/python2

from clickhouse_driver import Client
from common.mc_logging import mc_logger
from utils.table_schema_modifier import TableSchemaModifier

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
        cls.get_source_connection()
        cls.get_databases()
        cls.get_target_connection()
        cls.get_tables_and_split()

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
        return l

    @classmethod
    def get_databases(cls):
        cls.source_database_list = cls.client_for_source.execute(cls.SHOW_DATABASE)
        cls.source_database_list = cls.get_rid_of_tuple_in_list(cls.source_database_list)

    # The tables will be overwritten when this method invokes.
    @classmethod
    def get_tables_and_split(cls, database):
        cls.client_for_source.execute(cls.USE_DATABASE %database)
        cls.table_list = cls.client_for_source.execute(cls.SHOW_TABLES)
        cls.table_list = cls.get_rid_of_tuple_in_list(cls.table_list)
        mc_logger.info("cls.table_list after get_rid_of_tuple_in_list method : " + str(cls.table_list))
        cls.local_table_list = filter(lambda item: ("_local" in item), cls.table_list)
        mc_logger.info("cls.local_table_list after filter method : " + str(cls.local_table_list))
        cls.no_local_table_list = filter(lambda item:("_local" not in item), cls.table_list)
        mc_logger.info("cls.no_local_table_list after filter method : " + str(cls.no_local_table_list))
 
    # Return the table schema.
    @classmethod
    def fetch_table_schema(cls, database, table):
        mc_logger.info("Fetch table schema, %s.%s" %(database, table))
        return cls.client_for_source.execute(cls.SHOW_TABLE_SCHEMA %(database, table))

    @classmethod
    def create_database(cls, database):
        mc_logger.info("Create database %s, Ready Go!" %(database))
        cls.client_for_target.execute(cls.CREATE_DATABASE_QUERY %(database))

    @classmethod
    def create_table(cls, table_schema):
        mc_logger.info("Create table <table schema %s>, Ready Go !" %(table_schema))
        cls.client_for_target.execute(table_schema)

    @classmethod
    def do_create_databases(cls):
         # Loop each database to create database
        for database in cls.source_database_list:
            try:
                mc_logger.info ("do create databse %s" %(database))
                cls.create_database(database)
            except Exception, e:
                mc_logger.error("Exception when execute create database %s" %(e.message))

    @classmethod
    def do_create_tables(cls):
        # Loop each database to create table, First we create local tables
        for database in cls.source_database_list:
            if database == "system":
                continue
            cls.get_tables_and_split(database)
            mc_logger.info("local tables %s in %s" %(str(cls.local_table_list), database))
            # First process local_table
            for local_table in cls.local_table_list:
                # Fetch the table schema
                table_schema = cls.fetch_table_schema(database, local_table)[0][0]
                mc_logger.info("Table's schema %s" %(table_schema))
                target_table_schema = TableSchemaModifier.modify(table_schema, database, local_table)
                try:
                    mc_logger.info("Will construct table on target cluster")
                    cls.create_table(target_table_schema)
                except Exception:
                    mc_logger.error("Exception when executing create table %s" %(Exception.message))
            # Create distributed tables
            mc_logger.info("Will Create distributed Tables in %s" %(database))
            cls.do_create_distributed_tables(database)
            
    @classmethod
    def do_create_distributed_tables(cls, database):
        mc_logger.info("distributed tables %s in %s" %(str(cls.no_local_table_list), database))
        for distributed_table in cls.no_local_table_list:
          # Fetch the table schema
          table_schema = cls.fetch_table_schema(database, distributed_table)[0][0]
          mc_logger.info("Table's schema %s" %(table_schema))
          target_table_schema = TableSchemaModifier.modify(table_schema, database, distributed_table)
          try:
              mc_logger.info("Will construct table on target cluster")
              cls.create_table(target_table_schema)
          except Exception:
              mc_logger.error("Exception when executing create table %s" %(Exception.message))
   

if __name__ == "__main__":
    ClickHouseConnector.source_ck_ip = "192.168.254.43"
    ClickHouseConnector.source_ck_port = 9001
    ClickHouseConnector.target_ck_ip = "192.168.254.46"
    ClickHouseConnector.target_ck_port = 9002
    ClickHouseConnector.get_source_connection()
    ClickHouseConnector.get_target_connection()
    ClickHouseConnector.get_databases()
    ClickHouseConnector.do_create_databases()
    ClickHouseConnector.do_create_tables()
    #print "database " + str(ClickHouseConnector.source_database_list)
    #print "tables " + str(ClickHouseConnector.table_list)
    #print "table schema " + str(ClickHouseConnector.fetch_table_schema("cloudatlas", "cloudatlas_user_group_local"))
    