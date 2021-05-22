#/usr/bin/python
import sys
import subprocess
import logging

logging.basicConfig(filename="migrate_via_scp.log", filemode="w", level=logging.INFO)

dataPath = "/data/d1/clickhouse/data"
migrateFromPath = "/data/d1/migrate_from"


# 定义语法糖用于标准输出日志,因为需要对类变量及方法进行判断，然后分别去打印。并不会省事，所以不使用此方法。

# def myLogger(fn):
#     def wrapper(*args, **kargs):
#        print("fn's name %s : " % (fn.__name__)) 
#        logging.info("fn's name %s : " % (fn.__name__)) 
       

# TODO这些方法可以使用语法糖，来解决日志打印的通用问题
"""
因为我们的数据目录都是在 /data/d1/clickhouse/data下，所以我们可以得到这些库.
"""
class DataBaseSelector(object):

    #获取所有databases的名称,除去test和system及default,test库
    #@databaseNamesToFilter: ,分隔
    @classmethod
    def getDatabaseNames(cls, databaseNamesToFilter="test,system,default,test"):
        filteredOutDataBaseSet = set(databaseNamesToFilter.split(","))

        databaseNamesBytes = subprocess.Popen("ls %s" %(dataPath), shell=True, \
            stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]

        print(databaseNamesBytes.decode('utf-8'))
        databaseNamesList = list(filter(lambda _x:_x != '',databaseNamesBytes.decode('utf-8').split("\n")))
        print(databaseNamesList)
        # 过滤掉某些库
        databaseNamesList = list(filter(lambda _name:_name not in filteredOutDataBaseSet,databaseNamesList))
        return databaseNamesList

"""
我们根据已有的dataPath和DatabaseSelector的getDatabaseNames方法返回的databaseNamesList，去获取_local结尾的表
"""

class TableSelector(object):
    @classmethod
    def getTableNames(cls, databaseNamesList, databaseTableDict, tableNamesToFilterPattern="_local"):
        for databaseName in databaseNamesList:
            _databasePath = dataPath + "/" + databaseName
            tableNamesBytes = subprocess.Popen("ls %s" %(_databasePath), shell=True, \
                stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()[0]
            # 去掉''
            tableNamesList = list(filter(lambda _x: _x != '',tableNamesBytes.decode("utf-8").split("\n")))
            #仅仅需要_local的表
            tableNamesList = list(filter(lambda _x: tableNamesToFilterPattern in _x, tableNamesList))
            databaseTableDict.setdefault(databaseName, tableNamesList)

        return tableNamesList


"""
我们根据已有的dataPath+databasePath+tableName构建成表的路径.
"""
class PartitionSelector(object):
    pass


class InitEnv(object):
    cleanLocalEnvCMD = "sudo su - root -c 'rm -rf /data/d1/migrate_from'"
    createLocalEnvCMD = "sudo su - root -c 'mkdir -p  /data/d1/migrate_from && chown -R op_admin:op_admin /data/d1/migrate_from'"

    # %s 是ip的 占位符
    cleanRemoteEnvCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'rm -rf /data/d1/migrate_in\\'"
    createRemoteEnvCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'mkdir -p /data/d1/migrate_in\\'"
    #remoteChownCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'chown -R op_admin:op_admin /data/d1/migrate_in\\'"

    @classmethod
    def initLocalEnv(cls):
        out, err = subprocess.Popen(cls.cleanLocalEnvCMD,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("cleanLocalEnvCMD %s stdout : " %(cls.cleanLocalEnvCMD) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("cleanLocalEnvCMD stdout : " + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        out, err = subprocess.Popen(cls.createLocalEnvCMD,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("createLocalEnvCMD %s stdout : " %(cls.createLocalEnvCMD) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("createLocalEnvCMD stdout : " + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))

    @classmethod
    def initRemoteEnv(cls, ip):
        out, err = subprocess.Popen(cls.cleanRemoteEnvCMD %(ip), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("cleanRemoteEnvCMD stdout %s : " %(cls.cleanRemoteEnvCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("cleanRemoteEnvCMD stdout %s : " %(cls.cleanRemoteEnvCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        
        out, err = subprocess.Popen(cls.createRemoteEnvCMD %(ip), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("createRemoteEnvCMD stdout %s : " %(cls.createRemoteEnvCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("createRemoteEnvCMD stdout %s : " %(cls.createRemoteEnvCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        
        out, err = subprocess.Popen(cls.remoteChownCMD %(ip), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("remoteChownCMD stdout %s : " %(cls.remoteChownCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("remoteChownCMD stdout %s : " %(cls.remoteChownCMD %(ip)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))



# """
# 我们需要对tar数据压缩包进行传输
# """

# class Transfer(object):
#     # 根据本地的tarFilePath发送到目标远程机器
#     transferCMD="scp -P 50022 %s op_admin@%s:%s"
#     @classmethod
#     def transferTar(cls, tarFilePath, ip, targetDataPath="/data/d1/migrate_in"):
#         transferCMDResult = subprocess.Popen(cls.transferCMD %(tarFilePath, ip, targetDataPath), \
#             shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
#         logging.info("transferCMD:%s transferCMDResult " %(cls.transferCMD %(tarFilePath, ip, targetDataPath)) + transferCMDResult.decode('utf-8'))


if __name__ == "__main__":
    #databaseName = sys.argv[1]
    #tableName = sys.argv[2]

    #目标集群的某台ip地址
    #targetIp = sys.argv[3]
    pass

    # 初始化当前环境
    InitEnv.initLocalEnv()
    InitEnv.initRemoteEnv("10.12.1.11")

    #测试DataBaseSelector
    r1 = DataBaseSelector.getDatabaseNames()
    print(repr(r1))
    d={}
    TableSelector.getTableNames(r1,d)
    print(d)
    for _database in d:
        _database = 'app'
        for _table in d[_database]:
            Compressor.compress(_database, _table)
            _tarPath = migrateFromPath+"/"+_table+".tar.gz"
            Md5Sum.localMD5Sum(_tarPath)
            Transfer.transferTar(_tarPath, '10.12.1.11')
        break