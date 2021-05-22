import collections
import datetime
import functools
import logging
import time
import sys
import random
import subprocess
import threading

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED


from clickhouse_driver import Client


logging.basicConfig(filename="migrate.log", filemode="w", level=logging.INFO)

class RemoteIPConnectionCounter(object):
    lock = threading.Lock()
    ipCountDict = dict()

    # 通过已有ip地址初始化Dict
    # ipString:ip1,ip2,ip3,...
    @classmethod
    def initIPCountDict(cls, ipString):
        ipList = ipString.rstrip(',').split(",")
        for _ip in ipList:
            cls.ipCountDict.setdefault(_ip, 0)
        print(100*'-')
        print(repr(cls.ipCountDict))
        print(100*'-')

    # 获取当前最少的连接的机器
    @classmethod
    def getLeastIPConnectionIPAndIncreaseCount(cls):
        cls.lock.acquire()
        #sorted Dict return list of tuple()
        print(100*'-')
        print("Current ipCountDict before getLeastIpConnection : %s " %(repr(cls.ipCountDict)))
        logging.info("Current ipCountDict before getLeastIpConnection : %s " %(repr(cls.ipCountDict)))
        tmpSortedList = sorted(cls.ipCountDict.items(), key=lambda item:item[1])
        print("Sorted ip List: " + repr(tmpSortedList))
        logging.info("Sorted ip List: " + repr(tmpSortedList))
        ipLeastConnection = tmpSortedList[0][0]
        # 计数
        cls.ipCountDict[ipLeastConnection]+=1
        print("Current ipCountDict after getLeastIpConnection : %s " %(repr(cls.ipCountDict)))
        logging.info("Current ipCountDict after getLeastIpConnection : %s " %(repr(cls.ipCountDict)))
        print(100*'-')
        logging.info(100*'-')
        cls.lock.release()
        return ipLeastConnection
    #当ip使用结束后，对相应的引用减一
    @classmethod
    def releaseIPConnectionAndDecreaseCount(cls, IPReleased):
        cls.lock.acquire()
        print(100*'-')
        logging.info("release %s \n" %(IPReleased))
        logging.info(100*'-')
        print("Current ipCountDict before releasing : %s \n" %(repr(cls.ipCountDict)))
        logging.info("Current ipCountDict before releasing : %s \n" %(repr(cls.ipCountDict)))
        if sum(cls.ipCountDict.values()) == 6:
            cls.clearDict()
        #cls.ipCountDict[IPReleased]-=1
        print("Current ipCountDict after releasing : %s \n" %(repr(cls.ipCountDict)))
        logging.info("Current ipCountDict after releasing : %s \n" %(repr(cls.ipCountDict)))
        print(100*'-')
        logging.info(100*'-')
        cls.lock.release()

    @classmethod
    def clearDict(cls):
        for key in cls.ipCountDict.keys():
            cls.ipCountDict[key] = 0

def format_partition_expr(p):
    if isinstance(p, int):
        return p
    return "'{p}'"


def execute_queries(conn_list, queries):
    if isinstance(queries, str):
        queries = queries.split(';')
    for q in queries:
        logging.info("execute query : " + q)
        random.choice(conn_list).execute(q.strip(), settings=settings)

def new_execute_queries(queries):
    if isinstance(queries, str):
        queries = queries.split(';')
    for q in queries:
        logging.info("execute query : " + q)
        print(100*'-')
        logging.info(100*'-')
        ipLeastIpConnection = RemoteIPConnectionCounter.getLeastIPConnectionIPAndIncreaseCount()
        print("get ip: %s : " %(ipLeastIpConnection))
        logging.info("get ip: %s : " %(ipLeastIpConnection))
        print(100*'-')
        logging.info(100*'-')
        target_conn =  Client(host=ipLeastIpConnection, user='default', password='')
        try:
            target_conn.execute(q.strip(), settings=settings)
        except Exception as ex:
            logging.info(ex)
        RemoteIPConnectionCounter.releaseIPConnectionAndDecreaseCount(ipLeastIpConnection)

def execute_query(conn, query):
    logging.info("execute query : " + query)
    conn.execute(query.strip(), settings=settings)

class Table(object):

    cls_target_conn = Client(host="10.12.1.11", user='default', password='')
    def __init__(self, database, name, ddl, partition_key, is_view):
        self._source_conn =  Client(host="10.9.7.11", user='default', password='')
        self._target_conn =  Client(host="10.12.1.11", user='default', password='')
        self._target_conn_list =  [Client(host="10.12.1.11", user='default', password=''),Client(host="10.12.1.12", user='default', password=''),Client(host="10.12.1.13", user='default', password=''),Client(host="10.12.1.14", user='default', password=''),Client(host="10.12.1.15", user='default', password=''),Client(host="10.12.1.16", user='default', password='')]

        self.database = database
        self.name = name
        self.ddl = ddl.replace(f'CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
        self.partition_key = partition_key
        self.is_view = is_view

    def exists(self, conn):
        q = f"SELECT name FROM system.tables WHERE database = '{self.database}' AND name = '{self.name}'"
        return len(conn.execute(q)) > 0

    def get_partitions(self, conn):
        partitions = []
        q = f'SELECT {self.partition_key}, count() FROM {self.identity} GROUP BY {self.partition_key} ORDER BY {self.partition_key}'
        partitions = collections.OrderedDict(conn.execute(q))
        return partitions

    def get_total_count(self, conn):
        q = f"SELECT COUNT() FROM {self.identity}"
        return conn.execute(q)[0][0]

    def check_consistency(self):
        if not self.exists(self._target_conn):
            return False, None

        source_ttl_count = self.get_total_count(self._source_conn)
        target_ttl_count = self.get_total_count(self._target_conn)
        if source_ttl_count == target_ttl_count:
            return True, None

        if not self.partition_key:
            return False, None

        source_partitions = self.get_partitions(self._source_conn)
        target_partitions = self.get_partitions(self._target_conn)
        bug_partitions = []
        for p, c in source_partitions.items():
            if p not in target_partitions or c != target_partitions[p]:
                bug_partitions.append(p)
        return False, bug_partitions

    def create(self, replace=False):
        self._target_conn.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        if self.is_view:
            replace = True
        if replace:
            self._target_conn.execute(f'DROP TABLE IF EXISTS {self.identity}')
        self._target_conn.execute(self.ddl)

    def copy_data_from_remote(self, idx, total_tables, by_partition=True):
        logging.info('>>>> start to migrate table %s, progress %s/%s', self.identity, idx+1, total_tables)
        self.create()
        if self.is_view:
            logging.info('ignore view %s', self.identity)
            return

        is_identical, bug_partitions = self.check_consistency()
        if is_identical:
            logging.info('table %s has the same number of rows, skip', self.identity)
            return

        logging.info("starting _copy_table_from_remote()")
        self._copy_table_from_remote()

    # detach all the data via truncate
    @classmethod
    def truncate_remote_table_data(cls, local_tables):
        #保证
        for _local_table in local_tables:
            query = f'''
                TRUNCATE TABLE {_local_table.identity} ON CLUSTER replicated_stock;
            '''
            print(query)
            execute_query(cls.cls_target_conn, query)

    # detach all the data via truncate
    @classmethod
    def drop_remote_table_data(cls, local_tables):
        #保证
        for _local_table in local_tables:
            query = f'''
                DROP TABLE {_local_table.identity} ON CLUSTER replicated_stock;
            '''
            print(query)
            try:
                execute_query(cls.cls_target_conn, query)
            except Exception as ex:
                print(ex)

    def _copy_table_from_remote(self):
        queries = f'''
        INSERT INTO {self.identity}
        SELECT * FROM remote('{self._source_conn.connection.hosts[0][0]}:{self._source_conn.connection.hosts[0][1]}', {self.identity}, '{self._source_conn.connection.user}', '{self._source_conn.connection.password}')
        '''
        print(queries)
        #execute_queries(self._target_conn_list, queries)
        new_execute_queries(queries)

    # 这个函数暂时先不用
    def _copy_partition_from_remote(self, partition):
        partition = format_partition_expr(partition)
        queries = f'''
        ALTER TABLE {self.identity} DROP PARTITION {partition};
        INSERT INTO {self.identity}
        SELECT * FROM remote('{self._source_conn.connection.hosts[0][0]}:{self._source_conn.connection.hosts[0][1]}', {self.identity}, '{self._source_conn.connection.user}', '{self._source_conn.connection.password}')
        WHERE {self.partition_key} = {partition}
        '''
        #execute_queries(self._target_conn_list, queries)
        new_execute_queries(queries)


    @property
    def identity(self):
        return f'{self.database}.{self.name}'

    def __str__(self):
        return self.identity

    __repr__ = __str__


settings = {}
#settings={'max_insert_block_size':1048676*1024*1024,'min_insert_block_size_rows': 1048676*1024, 'min_insert_block_size_bytes':1048676*1024*1024}
def get_all_tables(_database):
    q = f'''
    SELECT database, name, create_table_query, partition_key, engine = 'View' AS is_view
    FROM system.tables
    WHERE database NOT IN ('system')
    ORDER BY if(engine = 'View', 999, 0), database, name
    '''
    source_conn = Client(host="10.9.7.11", user='default', password='')
    #source_conn.settings['insert_block_size']=1048576*1024
    #print(dir(source_conn.settings))
    #return
    rows = source_conn.execute(q, settings=settings)
    if(_database):
        tables = [Table(*values) for values in rows if values[0] == _database]
    else:
        tables = [Table(*values) for values in rows]
    return tables


def copy_remote_tables(tables):
    # 这里我们使用python的ThreadPool多线程模型

    start_time = datetime.datetime.now()
    #_executor = ThreadPoolExecutor(max_workers=20)
    #tasks = [_executor.submit((t.copy_data_from_remote, (idx, len(tables))) for idx, t in enumerate(tables))]
    #for idx, t in enumerate(tables):
    #wait(tasks, return_when=ALL_COMPLETED)
    #     logging.info('>>>> start to migrate table %s, progress %s/%s', t.identity, idx+1, len(tables))
    #     t.copy_data_from_remote()
    logging.info('<<<< migrated table in %s', datetime.datetime.now() - start_time)


# 参数times用来模拟网络请求的时间
def get_html(times):
    now = datetime.datetime.now()
    print(now.strftime("%H:%M:%S"))
    time.sleep(times)
    print("get page {}s finished".format(times))
    return time

def with_retry(max_attempts=5, backoff=120):
    def decorator(f):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            print("hello with_retry")
            attempts = 0
            while True:
                attempts += 1
                logging.info('start attempt #%s', attempts)
                try:
                    f(*args, **kwargs)
                except Exception as e:
                    if attempts >= max_attempts:
                        raise e
                    logging.exception('caught exception')
                    time.sleep(backoff)
                break
        return inner
    return decorator


@with_retry()
def main(_databaseList):
    #print (f"{self._source_conn.connection.hosts[0][0]}:{self._source_conn.connection.hosts[0][1]}")
    print("default concurrency is 6")
    for _database in _databaseList:
        tables = get_all_tables(_database)
        logging.info('got %d tables: %s', len(tables), tables)
        local_tables = list(filter(lambda table: "_local" in table.identity, tables))
        logging.info("local_tables number: %d " %(len(local_tables)))
        Table.truncate_remote_table_data(local_tables)
        #Table.drop_remote_table_data(local_tables)
        global_tables = list(filter(lambda table: "_local" not in table.identity, tables))
        logging.info("global_tables number: %d " %(len(global_tables)))
        #copy_remote_tables(tables)
        _executor = ThreadPoolExecutor(max_workers=6)
        tasks = [_executor.submit(t.copy_data_from_remote, idx, len(global_tables)) for idx, t in enumerate(global_tables)]
        # # for idx, t in enumerate(tables):
        wait(tasks, return_when=ALL_COMPLETED)
        logging.info("execute after wait")

if __name__ == '__main__':
    print("Input database list, and target machien ips string, seperated by ','")
    databaseString = sys.argv[1]
    remoteIpString = sys.argv[2]
    RemoteIPConnectionCounter.initIPCountDict(remoteIpString)
    databaseList = databaseString.rstrip(',').split(',')
    if (databaseList == None):
        print ("No database input, will sync all database")
    main(databaseList)
    print("main")