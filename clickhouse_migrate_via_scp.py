#/usr/bin/python
import sys
import subprocess
import logging

logging.basicConfig(filename="migrate_via_scp.log", filemode="w", level=logging.INFO)

dataPath = "/data/d1/clickhouse/data"
migrateFromPath = "/data/d1/migrate_from"
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
    # 创建migrate_in目录
    createRemoteEnvCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'mkdir -p /data/d1/migrate_in\\'"
    """ssh -p 50022 op_admin@10.12.1.11 sudo su - root -c \\'chown -R op_admin:op_admin /data/d1/migrate_in\\'"""
    # 改变migrate_in的owner
    remoteChownCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'chown -R op_admin:op_admin /data/d1/migrate_in\\'"

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


"""
我们使用shell命令对目标数据目录进行压缩
"""
class Compressor(object):
    # 跳转到目录,为了控制压缩路径为./,压缩然后mv
    compressAndMoveCMD = "cd %s && touch %s.tar.gz && tar --exclude=%s.tar.gz --exclude=format_version.txt -czvf %s.tar.gz . && mv -f %s.tar.gz /data/d1/migrate_from"
    # 测试
    """compressAndMoveCMD = "cd /data/d1/clickhouse/data/etl/etl_app_bicore_cityline_classmmc_wma_profit_1d_local && \
        touch etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar.gz && tar --exclude=etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar.gz \
        --exclude=format_version.txt -czf etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar.gz . && mv -f etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar.gz /data/d1/migrate_from"""

    #传入数据路径，进行压缩
    @classmethod
    def compress(cls, database, table):
        _tablePath = dataPath+'/'+database+'/'+table
        out, err = compressAndMoveCMDResult = subprocess.Popen(cls.compressAndMoveCMD %(_tablePath, table, table, table, table),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("compressAndMoveCMD stdout %s : " %(cls.compressAndMoveCMD %(_tablePath, table, table, table, table)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))
        logging.info("compressAndMoveCMD stdout %s : " %(cls.compressAndMoveCMD %(_tablePath, table, table, table, table)) + out.decode('utf-8') + " stderr: " + err.decode('utf-8'))


# """
# 我们对已经压缩的文件进行Md5算法求一个值,这里已经开始并行执行
# """
# class Md5Sum(object):
#     localMD5SumCMD = "md5sum %s"
#     remoteMD5SumCMD = "scp -P 50022 op_admin@%s sudo su -root -c \\'md5sum /data/d1/migrate_in/%s\\'"
#     @classmethod
#     def localMD5Sum(cls, tarFilePath):
#         localMD5SumCMDResult = subprocess.Popen(cls.localMD5SumCMD %(tarFilePath), \
#             shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
#         logging.info("localMD5SumCMD:%s localMD5SumCMDResult " %(cls.localMD5SumCMD %(tarFilePath))+ localMD5SumCMDResult.decode('utf-8'))
#         print("localMD5SumCMD %s: localMD5SumCMDResult " %(cls.localMD5SumCMD %(tarFilePath))+ localMD5SumCMDResult.decode('utf-8'))

#     @classmethod
#     def remoteMD5Sum(cls, ip, tarFileName):
#         remoteMD5SumCMDResult = subprocess.Popen(cls.remoteMD5SumCMD %(ip, tarFileName), \
#             shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
#         logging.info("remoteMD5SumCMD:%s remoteMD5SumCMDResult " %(cls.remoteMD5SumCMD %(tarFilePath))+ remoteMD5SumCMDResult.decode('utf-8'))
#         print("remoteMD5SumCMD: remoteMD5SumCMDResult " %(cls.remoteMD5SumCMD %(tarFilePath))+ remoteMD5SumCMDResult.decode('utf-8'))


"""
我们需要对tar数据压缩包进行传输
"""

class Transfer(object):
    # 根据本地的tarFilePath发送到目标远程机器
    transferCMD="cd %s && scp -P 50022 %s op_admin@%s:%s"
    """
        transferCMD = "cd /data/d1/migrate_from && scp -P 50022 \
            etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar op_admin@10.12.1.11:/data/d1/migrate_in/"
    """
    @classmethod
    def transferTar(cls, tarFilePath, ip, targetDataPath="/data/d1/migrate_in"):
        transferCMDResult = subprocess.Popen(cls.transferCMD %(migrateFromPath, tarFilePath, ip, targetDataPath), \
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
        logging.info("transferCMD transferCMDResult :%s " %(cls.transferCMD %(migrateFromPath, tarFilePath, ip, targetDataPath)) + transferCMDResult.decode('utf-8'))

"""
    数据传输过去以后，我们需要移动到对应的detached的目录，然后修改tar权限，解压，然后进行attach.
    这几步操作都需要走ssh,进行远程控制操作
"""
class Move2Detached(object):
    moveCMD="ssh -p 50022 op_admin@%s sudo su - root -c \\' mv %s/%s %s/%s/%s/detached \\' "
    """
        moveCMD="ssh -p 50022 op_admin@10.12.1.11 sudo su - \
            root -c \\' mv /data/d1/migrate_in/etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar /data/d1/clickhouse/data/etl/etl_app_bicore_cityline_classmmc_wma_profit_1d_local/detached \\' "
    """
    @classmethod
    def doMove2Detached(cls, ip, databaseName, tableName, targetDataPath="/data/d1/migrate_in"):
        doMove2DetachedResult = subprocess.Popen(cls.moveCMD %(ip,targetDataPath, tableName+".tar.gz", dataPath, databaseName, tableName), \
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
        logging.info("moveCMD CMDResult: %s " %(cls.transferCMD %(ip, targetDataPath, tableName+".tar.gz", dataPath, databaseName, tableName)) + transferCMDResult.decode('utf-8'))

"""
    进行解压
"""
class Extract(object):
    chownCMD = "ssh -p 50022 op_admin@%s sudo su - root -c \\'chown clickhouse:clickhouse %s \\'"
    """
        chownCMD="ssh -p 50022 op_admin@10.12.1.11 sudo su - root -c \\' chown clickhouse:clickhouse \
            /data/d1/clickhouse/data/etl/etl_app_bicore_cityline_classmmc_wma_profit_1d_local/detached/etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar \\'"
    """
    extractCMD="ssh -p 50022 op_admin@%s sudo su - root -c \\' tar xf %s \\' "
    """
        extractCMD = "ssh -p 50022 op_admin@10.12.1.11 sudo su - root -c \\' tar xf \
           /data/d1/clickhouse/data/etl/etl_app_bicore_cityline_classmmc_wma_profit_1d_local/detached/etl_app_bicore_cityline_classmmc_wma_profit_1d_local.tar -C \
              /data/d1/clickhouse/data/etl/etl_app_bicore_cityline_classmmc_wma_profit_1d_local/detached/ \\'"
    """
    @classmethod
    def chownAndExtract(cls, ip, databaseName, tableName):
        targetTarPath = dataPath+"/"+databaseName+"/"+tableName+"/detached/"+tableName+".tar.gz"
        chownCMDResult = subprocess.Popen(cls.chownCMD %(ip, targetTarPath), \
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
        logging.info("chownCMD : chownCMDResult %s" %(cls.chownCMD %(ip, targetTarPath) + chownCMDResult.decode('utf-8')))

        etractCMDResult = subprocess.Popen(cls.extractCMD %(ip, targetTarPath), \
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
        logging.info("extractCMD extractCMDResult: %s " %(cls.extractCMD %(ip, targetTarPath) + extractCMDResult.decode('utf-8')))
                     
"""
    最后一步，进行Attach工作
"""
class Attach(object):
    AttachCMD="ssh -p 50022 op_admin@%s sudo su - root -c \\'bash /data/service/load_data.sh %s %s\\' "
    """
        AttachCMD="ssh -p 50022 op_admin@10.12.1.11 sudo su - root -c \\'bash /data/service/load_data.sh etl etl_app_bicore_cityline_classmmc_wma_profit_1d_local \\'"
    """
    @classmethod
    def doAttach(cls, ip, databaseName, tableName):
        attachCMDResult = subprocess.Popen(cls.AttachCMD %(ip, databaseName, tableName), \
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
        logging.info("attachCMD : attachCMDResult %s" %(cls.AttachCMD %(ip, databaseName, tableName) + attachCMDResult.decode('utf-8')))
       


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