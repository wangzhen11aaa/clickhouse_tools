import re
import sys
import logging
from clickhouse_driver import Client
logging.basicConfig(filename="validate.log", filemode="w", level=logging.INFO)

#用于判断表是否为静态表(去掉依赖表为ReplacingMergeTree和_local表名中含有-rt(实时表))
skip_table_set = set()

# Table class used for store table information
class Table(object):
    _database = None
    _name = None
    _create_table_query = None
    # 用于校验的字段
    FLOAT64_FIELD_PATTERN = "(?<=, `).*?(?=` Float64)"
    INT64_FIELD_PATTERN = "(?<=, `).*?(?=` Int64)"


    def __init__(self, database, name, create_table_query):
        self._database = database
        self._name = name
        self._create_table_query = create_table_query
        self._float64_field_pattern = re.compile(self.FLOAT64_FIELD_PATTERN)
        self._int64_field_pattern = re.compile(self.INT64_FIELD_PATTERN)
        self._field_list_for_check = []

    # print _create_table_query
    def get_create_table_query(self):
        print(self._create_table_query)

    # 我们使用money或者discount的字段
    def set_sum_field_via_create_table_query(self):
        _float64_field_list = self._float64_field_pattern.findall(self._create_table_query)
        _int64_field_list = self._int64_field_pattern.findall(self._create_table_query)
        # 过滤掉包含空格的字符串,一般都不是想要的字段
        for _float64_field_list_iter in _float64_field_list:
            if ' ' in _float64_field_list_iter:
                pass
            else:
                self._field_list_for_check.append(_float64_field_list_iter)
        for _int64_field_list_iter in _int64_field_list:
            if ' ' in _int64_field_list_iter:
                pass
            else:
                self._field_list_for_check.append(_int64_field_list_iter)

        # 我们只取最多5个field
        if len(self._field_list_for_check) > 5:
            self._field_list_for_check = self._field_list_for_check[0:5]
        return self._field_list_for_check

    def get_table(self):
        return self._name

    def get_database(self):
        return self._database

    def get_create_table_query(self):
        return self._create_table_query

class Validator(object):
    _source_conn = None
    _target_conn = None

    #因为我们都是用的数据类型的字段，我们直接进行count(0)和sum进行校验

    # 总行数校验
    _validate_count_sql = "select count(0) from %s.%s"

    # 总数校验
    _validate_sum_sql = "select sum(%s) from %s.%s"

    # Validator constructor
    def __init__(self, source_ip='10.9.7.11', target_ip='10.12.1.11'):
        self._source_conn =  Client(host=source_ip, user='default', password='')
        self._target_conn =  Client(host=target_ip, user='default', password='')

    # Get all tables from source
    def get_all_tables(self, _database):
        q = f'''
        SELECT database, name, create_table_query
        FROM system.tables
        WHERE database NOT IN ('system')
        ORDER BY database, name
        '''
        rows = self._source_conn.execute(q)
        if(_database != None):
            logging.info("Will validate %s" %(_database))
            tables = [Table(*values) for values in rows if values[0] == _database]
        else:
            logging.info("database is None, will validate all database")
            tables = [Table(*values) for values in rows]

        #初始化全局的map
        for table in tables:
            if "_local" in table.get_table():
                if "Replacing" in table.get_create_table_query():
                    skip_table_set.add(table.get_table())
        return tables

    # 检测
    def check(self, source_rows, target_rows, database, table, field):
        logging.info("check field " + field)
        logging.info(source_rows)
        logging.info(target_rows)
        if (abs(source_rows[0][0] - target_rows[0][0]) < 0.01):
            logging.info("Check field %s in %s.%s is ok" %(field, database, table))
        else:
            logging.info("Diff in filed  %s in %s.%s:  %f" %(field, database, table, abs(source_rows[0][0] - target_rows[0][0])))
            logging.info("Check filed %s is bad" %(field))
            #exit(-1)

    #根据database, table, 以及field去校验
    def do_validate(self, database, table, field):
        if table +"_local" in skip_table_set:
            logging.info("%s local table passed, for local table is ReplacingMergeTree" %table)
            return
        print("Current database " + database)
        print("Current table " + table)
        
        #总行数校验
        __validate_count_sql = self._validate_count_sql %(database, table)
        print("Current validate_count_sql :" + __validate_count_sql)
        # 数据源端执行
        source_rows = self._source_conn.execute(__validate_count_sql)
        # Target端执行
        target_rows = self._target_conn.execute(__validate_count_sql)
        self.check(source_rows, target_rows, database, table, field)

        __validate_sum_sql = self._validate_sum_sql %(field, database, table)
        print("Current validate_sum_sql " + __validate_sum_sql)

        # 数据源端执行
        source_rows = self._source_conn.execute(__validate_sum_sql)

        # Target端执行
        target_rows = self._target_conn.execute(__validate_sum_sql)
        self.check(source_rows, target_rows, database, table, field)


if __name__ == "__main__":

    database = None
    tables = None
    database = sys.argv[1]
    source_ck_node_ip = sys.argv[2]
    target_ck_node_ip = sys.argv[3]
    validator = Validator(source_ck_node_ip, target_ck_node_ip);
    if(database != None):
        tables = validator.get_all_tables(database);
    else:
        tables = validator.get_all_tables(database);
    for table in tables:
        if "_local" in table.get_table() or "_rt" in table.get_table() :
            logging.info("local table or rt table %s ignored " %(table.get_table()))
            continue
        table.get_create_table_query()
        l = table.set_sum_field_via_create_table_query()

        for _l in l:
            print(_l)
            validator.do_validate(table.get_database(), table.get_table(), _l)
