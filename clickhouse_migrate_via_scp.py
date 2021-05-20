#/usr/bin/python
import sys
import subprocess
import logging

logging.basicConfig(filename="migrate_via_scp.log", filemode="w", level=logging.INFO)

dataPath = "/data/d1/clickhouse/data"
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
            stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].split(" ")
        
        databaseNamesList = databaseNamesBytes.decode('utf-8').split("\n")
        # 过滤掉某些库
        databaseNamesList = [_name not in filteredOutDataBaseSet for _name in databaseNamesList]
        return databaseNamesList

"""
我们根据已有的dataPath和DatabaseSelector的getDatabaseNames方法返回的databaseNamesList，去获取_local结尾的表
"""

class TableSelector(object):
    @classmethod
    def getTableNames(cls, databaseNamesList, tableNamesToFilterPattern="_local"):
        for databaseName in databaseNamesList: 
            _databasePath = dataPath + "/" + databaseName
            tableNamesBytes = subprocess.Popen("ls %s"(_databasePath), shell=True, \
                stdout=subprocess.PIPE,stder=subprocess.PIPE).communicate()[0].split(" ")
            tableNamesList = tableNamesBytes.decode("utf-8").split("\n")
            #仅仅需要_local的表
            tableNamesList = [tableNamesToFilterPattern not in _name for _name in tableNamesList]

        return tableNamesList


"""
我们根据已有的dataPath+databasePath+tableName构建成表的路径.
"""
class PartitionSelector(object):
    pass

class InitEnv(object):
    cleanLocalEnvCMD = "sudo su - root -c 'rm -rf /data/d1/migrate_from/*'"
    createLocalEnvCMD = "sudo su - root -c 'mkdir -p  /data/d1/migrate_from'"

    # %s 是ip的 占位符
    cleanRemoteEnvCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'rm -rf /data/d1/migrate_in/*\\'""
    createRemoteEnvCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'mkdir -p  /data/d1/migrate_in\\'"
    @classmethod
    def initLocalEnv(cls):
        out, err = subprocess.Popen(cleanLocalEnvCMD,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        logging.info("cleanLocalEnvCMD stdout : " + out + " stderr: " + err)
        out, err = subprocess.Popen(createLocalEnvCMD,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        logging.info("createLocalEnvCMD stdout : " + out + " stderr: " + err)
    @classmethod
    def initRemoteEnv(cls, ip):
        out, err = subprocess.Popen(cleanRemoteEnvCMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        logging.info("cleanLocalEnvCMD stdout : " + out + " stderr: " + err)
        out, err = subprocess.Popen(createRemoteEnvCMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        logging.info("createLocalEnvCMD stdout : " + out + " stderr: " + err)

"""
我们使用shell命令对目标数据目录进行压缩
"""
class Compressor(object):
    #传入数据路径，进行压缩
    def compress(self, dataPath):
        pass

"""
我们对已经压缩的文件进行Md5算法求一个值
"""
class Validator(object):
    @classmethod
    def localMd5Sum(cls):
        
    pass

class Transfer(object):
    pass


if __name__ == "__main__":
    databaseName = sys.argv[1]
    tableName = sys.argv[2]

    #目标集群的某台ip地址
    targetIp = sys.argv[3]
    