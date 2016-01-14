#!/usr/bin/env python
# -*- encoding: utf-8 -*-
##TODO:db.runCommand()等未实现功能, 修补bug


import os
import threading
import MySQLdb


L_SWITCH = 1
S_SWITCH = 1

        
configure = {"hostname": "localhost", "username": "root", "password": "lai", "database": "test" }    

def connect(hostname, username, password, database, port=3306):
    conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=port)
    cur = conn.cursor()
    return cur

cursor = connect(**configure)

###Base class################
class ExtractSql:

    def __init__(self, obj):
        self.obj = obj

    def __iter__(self): # for Find
        sql =  self.to_sql()
        print sql
        count = cursor.execute(sql)
        print "there are %d rows record" % count
        result = cursor.fetchall()
        return iter(result)
    def execute(self): # insert, update, remove etc
        sql = self.to_sql()
        print sql
        count = cursor.execute(sql)
        return count

        
DATABASE_METHODS = ["createDatabase", "dropDatabase", 'cloneCollection', 'cloneDatabase', 'commandHelp', 'copyDatabase', 'createCollection', 'currentOp', 'eval', 'fsyncLock', 'fsyncUnlock', 'getCollection', 'getCollectionInfos', 'getCollectionNames', 'getLastError', 'getLastErrorObj', 'getLogComponents', 'getMongo', 'getName', 'getPrevError', 'getProfilingLevel', 'getProfilingStatus', 'getReplicationInfo', 'getSiblingDB', 'help', 'hostInfo', 'isMaster', 'killOp', 'listCommands', 'loadServerScripts', 'logout', 'printCollectionStatus', 'printReplicationInfo', 'printShardingStatus', 'printSlaveReplicationInfo', 'repairDatabase', 'resetError', 'runCommand', 'serverBuildInfo', 'serverCmdLineOpts', 'serverStatus', 'setLogLevel', 'setProfilingLevel', 'shutdownServer', 'stats', 'version', 'upgradeCheck', 'upgradeCheckAllDBs']


###classes for Database and database's methods####
class Db(dict, ExtractSql):
    """Docstring for Db. """
    def __init__(self, name):
        self.name = name
    def createDatabase(self):
        return CreateDatabase(self)
        
    def dropDatabase(self):
        return DropDatabase(self)
        
    def __getattr__(self, attr):
        if attr not in DATABASE_METHODS:
            return Table(self, attr)

    def cloneDatabase(self, host_name):
        return CloneDatabase(self, host_name)

    def cloneCollection(self,from_host, from_coll, query=None):
        """
        e.g:
        db.cloneCollection('mongodb.example.net:27017', 'users.profiles', {'active': 'true'}) -->从服务器mongodb.example.net:27017的users数据库的profiles集合中复制条件满足{' active': 'true'}的文档到当前的数据库集合中。
        """
        return CloneCollection(self, from_host, from_coll, query)
    
    def copyDatabase(self, from_db, to_db, from_host=None, username=None, password=None, mechanisum=None):
        return CopyDatabase(self, from_db, to_db, from_host, username, password, mechanisum)

    def createCollection(self, name, options=None):
        raise ValueError('createCollection unsupported')
        #return CreateCollection(self, name, options)

    
    def currentOp(self, operatoins=None):
        return CurrentOp(self, operations)

    def eval(self, func, arguments):
        raise ValueError('eval unsupported')
        #return Eval(self, func, arguments)

    def fsyncLock(self):
        raise ValueError('fsyncLock unsupported')

    def fsyncUnlock(self):
        raise ValueError('fsyncUnlock unsupported')

    def getCollection(self, name):
        return GetCollection(self, name)

    def getCollectionInfos(self):
        return GetCollectionInfos(self)

    def getCollectionNames(self):
        return GetCollectionNames(self)

    def getLastError(self, w_c=None, w_timeout=None):
        raise ValueError('getLastError unsupported')
        #return GetLastError(self, w_c, w_timeout)

    def getLastErrorObj(self, key=None, w_timeout=None):
        raise ValueError('getLastErrorObj unsupported')
        #return GetLastErrorObj(self, key, w_timeout)

    def getLogComponents(self):
        return GetLogComponents(self)

    def getMongo(self):
        return GetMongo(self)

    def getName(self):
        return GetName(self)

    def getPrevError(self):
        return GetPrevError(self)

    def getProfilingLevel(self):
        return GetProfilingLevel(self)

    def getProfilingStatus(self):
        return GetProfilingStatus(self)

    def getReplicationInfo(self):
        raise ValueError('getReplicationInfo unsupported')
        #return GetReplicationInfo(self)

    def getSiblingDB(self, database):
        return GetSiblingDB(self, database) 

    def help(self):
        return Help(self)

    def hostInfo(self):
        return HostInfo(self)

    def isMaster(self):
        raise ValueError('isMaster unsupported')
        #return IsMaster(self)

    def killOp(self, opid):
        return KillOp(self, opid)

    def listCommands(self):
        return ListCommands(self)

    def loadServerScripts(self):
        raise ValueError('loadServerScripts unsupported')
        #return LoadServerScripts(self)

    def logout(self):
        return Logout(self)

    def printCollectionStatus(self):
        return PrintCollectionStatus(self)

    def printReplicationInfo(self):
        raise ValueError('printReplicationInfo unsupported')
        #return PrintReplicationInfo(self)

    def printShardingStatus(self, verbose=False):
        raise ValueError('printShardingStatus unsupported')
        #return PrintShardingStatus(self, verbose)

    def printSlaveReplicationInfo(self):
        raise ValueError('printSlaveReplicationInfo unsupported')
        #return PrintSlaveReplicationStatus(self)

    def repairDatabase(self):
        return RepairDatabase(self)
    
    def resetError(self):
        return ResetError(self)

    def runCommand(self, command):
        return RunCommand(self, command)

    def serverBuildInfo(self):
        return ServerBuildInfo(self)

    def serverCmdLineOpts(self):
        raise ValueError('serverCmdBuildInfo unsupported')
        # return GetCmdLineOpts(self)

    def serverStatus(self):
        return ServerStatus(self)

    def setProfilingLevel(self, level=None, slowms=None):
        return SetProfilingLevel(self, level, slowms)

    def shutdownServer(self):
        return ShutdownServer(self)

        
    def setLogLevel(self, level=None, component=None):
        raise ValueError('setLogLevel unsupported')
        #return SetLogLevel(self, level, component)


    def stats(self, scale=None):
        return Stats(self, scale)

    def version(self):
        return Version(self)

    def upgradeCheck(self, scope=None):
        raise ValueError('upgradeCheck unsupported')
        #return UpgradeCheck(self, scope)

    def upgradeCheckAllDBs(self):
        raise ValueError('upgradeCheckAllDBs unsupported')
        # return UpgradeCheckAllDBs(self)


class CloneDatabase(object, ExtractSql):
    def __init__(self, db, host_name):
        self.db = db
        self.host_name = host_name

    def to_sql(self):
        r_user_name = raw_input('remote_mysql_username: \n')
        r_password = raw_input('remote_mysql_password: \n')
        l_user_name = raw_input('localhost_user_name: \n')
        l_password = raw_input('localhost_password: \n')
        if r_user_name and r_password and l_user_name and l_password:
            r_mysql_dump = 'mysqldump -h %s -u %s -p%s %s > %s.sql' % (self.host_name, r_user_name, r_password, self.db.name, self.db.name)
            l_mysql_import = 'mysql -h localhost -u %s -p%s < %s.sql' % (l_user_name, l_password, self.db.name)
            os.system('&&'.join([r_mysql_dump, l_mysql_import]))
            return 'OK'
        else:
            ValueError('To CloneDatabase, input infos')
                             
        
        
class UpgradeCheck(object, ExtractSql):
    pass


class UpgradeCheckAllDBs(object, ExtractSql):
    pass

class Version(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SELECT VERSION()'

        
class Stats(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        table_name = 'information_schema.PROCESSLIST'
        where_fmt = 'WHERE' + 'DB=%s' % self.db.name
        return 'SELECT * FROM %s %s' % (table_name, where_fmt)


class ShutdownServer(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return os.system('service mysql stop')


class SetProfilingLevel(object, ExtractSql):
    def __init__(self, db, level, slowms=None):
        self.db = db
        self.level = level
        self.slowms = None

    def to_sql(self):
        return 'SET PROFILING = 1'
        

        
class ServerStatus(object, ExtractSql):
    def  __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SHOW STATUS'


class ServerBuildInfo(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return "SELECT VERSION()"
    

class ServerCmdLineOpts(object, ExtractSql):
    pass


class SetLogLevel(object, ExtractSql):
    pass

    

class CreateDatabase(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'CREATE DATABASE %s' % self.db.name


class DropDatabase(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'DROP DATABASE %s' % self.db.name


class CloneCollection(object, ExtractSql):
    def __init__(self, db, from_host, from_coll, query=None):
        self.db = db
        self.from_host = from_host
        self.from_coll = from_coll
        self.query = query

    def to_sql(self):
        if '.' in self.from_coll:
            old_table_name = self.from_coll.split('.')[-1]
        else:
            old_table_name = self.from_coll
        new_table_name = self.db.old_table_name.name
        if self.query:
            where_fmt = 'WHERE ' + handle_condition(self.query)
        else:
            where_fmt = ''
        return 'SELECT * INTO %s FROM %s %s' % (new_table_name, old_table_name, where_fmt)
        

class CopyDatabase(object, ExtractSql):
    def __init__(self, db, from_db, to_db, from_host=None, username=None, password=None, mechanisum=None):
        self.db = db
        self.from_db = from_db
        self.to_db = to_db
        self.from_host = from_host
        self.username= username
        self.password = password
        self.mechanisum = mechanisum
        
    def to_sql(self):
        if self.from_host is not None and self.username is not None and self.password is not None:
            dump_sql = 'mysqldump -h %s -u %s -p%s %s > %s' % (self.from_host, self.username, self.password, self.from_db, '%s.sql'.format(self.from_db))
            l_host = raw_input('to_mysql_hostname:')
            l_user = raw_input('to_mysql_user:')
            l_password = raw_input('to_mysql_password:')
            import_sql = 'mysql -h %s -u %s -p%s < %s.sql' % (l_host, l_user, l_password, self.from_db)
            os.system('&&'.join([dump_sql, import_sql]))
            return 'OK'
        else:
            raise ValueError('To CopyDatabase, input host, username, password')
        

class CreateCollection(object, ExtractSql):
    def __init__(self, db, table_name, options=None):
        self.db = db
        self.table_name = table_name
        self.options = options

    def handle_fields(self):
        fields_fmt_list = []
        if isinstance(self.options, dict) and self.options != {}:
            for key, val in self.options.items():
                pass
                
            
        else:
            raise ValueError('The second parameter must be a dict and not {}')
        if len(fields_fmt_list) == 1:
            fields_fmt = fields_fmt_list[0]
        elif len(fields_fmt_list) == 0:
            return ''
        else:
            fields_fmt = ','.join(fields_fmt_list)
        return "(%s)" % fields_fmt


    def to_sql(self):
        sql_list = []
        sql_list.append('USE %s' % self.db.name)
        fileds_fmt = self.handle_fields(self.options)
        sql_list.append('CREATE TABLE %s %s' % (self.table_name, fileds_fmt)) #字段和字段类型的来源
        sql = ';'.join(sql_list)
        return sql


class CurrentOp(object, ExtractSql):
    def __init__(self, db, operations=None):
        self.db = db
        self.operations = operations

    def to_sql(self):
        talbe_name = 'information_schema.PROCESSLIST'
        if operations:
            if operations == True:
                where_fmt = 'WHERE ' + 'DB=%s' % self.db.name
                return 'SELECT * FROM %s %s ' % (table_name, where_fmt)
            #elif isinstance(operations, dict):
             #   where_fmt = 'WHERE' 
              #  return 'SELECT * FROM %s %s' % (table_name, where_fmt) 
        else:
            where_fmt = 'WHERE ' + 'DB=%s' % self.db.name
            return 'SELECT * FROM %s %s' % (table_name, where_fmt)


class Eval(object, ExtractSql):
    def __init__(self, db, func, arguments):
        self.db = db
        self.func = func
        self.arguments = arguments

    def to_sql(self):
        raise ValueError('Eval unsupported')

class fsynLock(object, ExtractSql):
    pass

class fsynUnlock(object, ExtractSql):
    pass
    
class GetCollection(object, ExtractSql):
    def __init__(self, db, name):
        self.db = db
        self.name = name 

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        where_fmt = 'WHERE ' + 'TABLE_SCHEMA=%s AND TABLE_NAME=%s' % (self.db.name, self.name)
        return 'SELECT TABLE_NAME FROM %s %s' % (table_name, where_fmt)


class GetCollectionInfos(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        where_fmt = 'WHERE ' + 'TABLE_SCHEMA=%s' % self.db.name
        return 'SELECT * FROM %s %s' % (table_name, where_fmt)


class GetCollectionNames(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        where_fmt = 'WHERE ' + 'TABLE_SCHEMA=%s' % self.db.name
        return 'SELECT TABLE_NAME FROM %s %s' % (table_name, where_fmt)


class GetLastError(object, ExtractSql):
    def __init__(self, db, w, wtimeout):
        self.db = db
        self.w = w
        self.wtimeout = wtimeout

    def to_sql(self):
        raise ValueError('GetLastError can\'t To_sql()')


class GetLastErrorObj(object, ExtractSql):
    pass


class GetLogComponents(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return '%s; %s' % ('USE %s' % self.db.name, 'SHOW status')


class GetMongo(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SHOW PROCESSLIST'

    
class GetName(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SELECT database()'


class GetPrevError(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SHOW ERRORS LIMIT 1'
    

class GetProfilingLevel(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SHOW PROFILES'

        
class GetProfilingStatus(object, ExtractSql):
    def __init__(self, db):
        self.db = db
        self.get_profiling_level = GetProfilingLevel(self.db)

    def to_sql(self):
        return self.get_profiling_level.to_sql()
    

class GetReplicationInfo(object, ExtractSql):
    pass


class GetSiblingDB(object, ExtractSql):
    def __init__(self, db, database):
        self.db = db
        self.database = database

    def to_sql(self):
        return 'USE %s' % self.database

        
class Help(object, ExtractSql):
    def __init__(self, obj):
        self.obj = obj

    def to_sql(self):
        return 'help'


class HostInfo(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        table_name = 'information_schema.STATISTICS, information_schema.PROCESSLIST'
        where_fmt = 'information_schema.STATISTICS.TABLE_SCHEMA=%s AND information.PROCESSLIST.DB=%s' % (self.db.name, self.db.name)
        return 'SELECT * FORM %s %s' % (table_name, where_fmt)

    
class IsMaster(object, ExtractSql):
    pass
            


class KillOp(object, ExtractSql):
    def __init__(self, db, opid):
        self.db = db
        self.opid = opid

    def to_sql(self):
        return 'SHOW PROCESSLIST; KILL %s' % self.opid


class ListCommands(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return Help(self.db).to_sql()


class ListDatabases(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return "SHOW DATABASES"


class LoadServerScripts(object, ExtractSql):
    pass


class Logout(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'DROP DATABASE %s' % (self.db.name)


class PrintCollectionStatus(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        table_name = 'information_schema.STATISTICS'
        where_fmt = 'TABLE_SCHEMA=%s' % self.db.name
        return 'SELECT * FORM %s %s' % (table_name, where_fmt)

    
class PrintReplicationInfo(object, ExtractSql):
    pass


class PrintShardingStatus(object, ExtractSql):
    pass

    
class PrintSlaveReplicationStatus(object, ExtractSql):
    pass

class RepairDatabase(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'USE %s' % self.db.name + ';' + 'REPAIR TABLE *'


class ResetError(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return GetPrevError(self.db).to_sql()


class Ping(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SHOW PROCESSLIST'


    
class RunCommand(object, ExtractSql):
    def __init__(self, db, command):
        self.db = db
        self.command = command

    def to_sql(self):
        cmd_doc = self.command
        if isinstance(cmd_doc, str):
            pass
        elif isinstance(cmd_doc, dict):
            if 'drop' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('drop'))
                return Drop(table).to_sql()
                              
            elif 'buildInfo' in cmd_doc.keys():
                if cmd_doc.get('buildInfo') == 1:
                    return ServerBuildInfo(self.db).to_sql()
                              
            elif 'collStats' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('collStats'))
                return Stats(table).to_sql()

                
            elif 'distinct' if cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('distinct'))
                key_val = cmd_doc.get('key')
                query_val = cmd_doc.get('query')
                return Distinct(table, key_val, query_val).to_sql()

            elif 'dropDatabase' in cmd_doc.keys():
                if cmd_doc.get('dropDatabase') == 1:
                    return DropDatabase(self.db).to_sql()
            elif 'dropIndexes' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('dropIndexes'))
                index_name = cmd_doc.get('index')
                if index_name == '*':
                    return DropIndexes(table).to_sql()
                else:
                    return DropIndex(table, index_name).to_sql()
                
            elif 'findAndModify' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('findAndModify'))
                doc = cmd_doc.copy()
                del doc['findAndModify']
                return FindAndModify(table, doc).to_sql()

            elif 'getLastError' in  cmd_doc.keys():
                if cmd_doc.get('getLastError') == 1:
                    doc = cmd_doc.copy()
                    del doc['getLastError']
                    options = doc
                    return GetLastError(self.db, options).to_sql()
                
            elif 'isMaster' in cmd_doc.keys():
                if cmd_doc.get('isMaster') == 1:
                    return IsMaster(self.db).to_sql()
            elif 'listCommands' in cmd_doc.keys():
                if cmd_doc.get('listCommand') == 1:
                    return ListCommands(self.db).to_sql()
            elif 'listDatabases' in cmd_doc.keys():
                if cmd_doc.get('listDatabases') == 1:
                    return ListDatabases(self.db).to_sql()
            elif 'ping' in cmd_doc.keys():
                if cmd_doc.get('ping') == 1:
                    return Ping(self.db).to_sql()
            elif 'renameCollection' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('renameCollection'))
                new_name = cmd_doc.get('target')
                return RenameCollection(table, new_name)

            elif 'repairDatabase' in cmd_doc.keys():
                if cmd_doc.get('repairDatabase') == 1:
                    return RepairDatabase(self.db).to_sql()

            elif 'serverStatus' in cmd_doc.keys():
                if cmd_doc.get('serverStatus') == 1:
                    return ServerStatus(self.db).to_sql()
            elif 'cloneCollection' in cmd_doc.keys():
                if cmd_doc.get('cloneCollection') == 1:
                    frm = cmd_doc.get('from')
                    coll_name = cmd_doc.get('collection')
                    query = cmd_doc.get('query', None)
                    return CloneCollection(self.db, frm, coll_name, query).to_sql()
            elif 'cloneDatabase' in cmd_doc.keys():
                if cmd_doc.get('cloneDatabase') == 1:
                    host_name = cmd_doc.get('hostname')
                    return CloneDatabase(self.db, host_name).to_sql()
            elif 'commandHelp' in cmd_doc.keys():
                if  cmd_doc.get('commandHelp') == 1:
                    cmd = cmd_doc.get('command')
                    return CommandHelp(self.db, cmd).to_sql()
            elif 'copyDatabase' in cmd_doc.keys():
                if cmd_doc.get('copyDatabase') == 1:
                    frm_db = cmd_doc.get('fromdb')
                    to_db = cmd_doc.get('todb')
                    frm_host = cmd_doc.get('fromhost', None)
                    user_name = cmd_doc.get("username", None)
                    password = cmd_doc.get('password', None)
                    mechanism = cmd_doc.get('mechanism', None)
                    return CopyDatabase(self.db, frm_db, to_db, user_name, password, mechanism).to_sql()

            elif 'createCollection' in cmd_doc.keys():
                if cmd_doc.get('createCollection') == 1:
                    coll_name = cmd_doc.get('name')
                    del cmd_doc['createCollection']
                    options = cmd_doc
                    return CreateCollection(self.db, name, options).to_sql()
            elif 'currentOp' in cmd_doc.keys():
                if cmd_doc.get('currentOp') == 1:
                    operations = cmd_doc.get('operations')
                    return CurrentOp(self.db, operations).to_sql()
            elif 'dropDatabase' in cmd_doc.keys():
                if cmd_doc.get('dropDatabase') == 1:
                    return DropDatabase(self.db).to_sql()

            elif 'eval' in cmd_doc.keys():
                if cmd_doc.get('eval') == 1:
                    func = cmd_doc.get('function')
                    args = cmd_doc.get('arguments', None)
                    return Eval(self.db, func, args).to_sql()
            elif 'fsyncLock' in cmd_doc.keys():
                if cmd_doc.get('fsyncLock') == 1:
                    return FsyncLock(self.db).to_sql()
            elif 'fsyncUnlock' in cmd_doc.keys():
                if cmd_doc.get('fsyncUnlock') == 1:
                    return FsyncUnlock(self.db).to_sql()
            elif 'getCollection' in cmd_doc.keys():
                if cmd_doc.get('getCollection') == 1:
                    coll_name = cmd_doc.get('name')
                    return GetCollection(self.db, coll_name).to_sql()
            elif 'getCollectionInfos' in cmd_doc.keys():
                if cmd_doc.get('getCollectionInfos') == 1:
                    return GetCollectionInfos(self.db).to_sql()
            elif 'getCollectionNames' in cmd_doc.keys():
                if cmd_doc.get('getCollectionNames') == 1:
                    return GetCollectionNames(self.db).to_sql()
            elif 'getLastErrorObj' in cmd_doc.keys():
                if cmd_doc.get('getLastErrorObj') == 1:
                    return GetLastErrorObj(self.db).to_sql()
            elif 'getLogComponents' in cmd_doc.keys():
                if cmd_doc.get('getLogComponents') == 1:
                    return GetLogComponents(self.db).to_sql()
            elif 'getMongo' in cmd_doc.keys():
                if cmd_doc.get('getMongo') == 1:
                    return GetMongo(self.db).to_sql()
            elif 'getName' in cmd_doc.keys():
                if cmd_doc.get('getName') == 1:
                    return GetName(self.db).to_sql()
            elif 'getPrevError' in cmd_doc.keys():
                if cmd_doc.get('getPrevError') == 1:
                    return GetPrevError(self.db).to_sql()
            elif 'getProfilingLevel' in cmd_doc.keys():
                if cmd_doc.get('getProfilingLevel') == 1:
                    return GetProfilingLevel(self.db).to_sql()
            elif 'getProfilingStatus' in cmd_doc.keys():
                if cmd_doc.get('getProfilingStatus') == 1:
                    return GetProfilingStatus(self.db).to_sql()
            elif 'getReplicationInfo' in cmd_doc.keys():
                if cmd_doc.get('getReplicationInfo') == 1:
                    return GetReplicationInfo(self.db).to_sql()
            elif 'getSiblingDB' in cmd_doc.keys():
                if cmd_doc.get('getSiblingDB') == 1:
                    db_name = cmd_doc.get('database')
                    return GetSiblingDB(self.db, db_name).to_sql()

            elif 'help' in cmd_doc.keys():
                if cmd_doc.get('help') == 1:
                    return Help(self.db).to_sql()
            elif 'hostInfo' in cmd_doc.keys():
                if cmd_doc.get('hostInfo') == 1:
                    return HostInfo(self.db).to_sql()
            elif 'killOp' in cmd_doc.keys():
                if cmd_doc.get('killOp') == 1:
                    op_id = cmd_doc.get('opid')
                    return KillOp(self.db, op_id).to_sql()
            elif 'loadServerScripts' in cmd_doc.keys():
                if cmd_doc.get('loadServerScripts') == 1:
                    return LoadServerScripts(self.db).to_sql()
            elif 'logout' in cmd_doc.keys():
                if cmd_doc.get('logout') == 1:
                    return Logout(self.db).to_sql()
            elif 'printCollectionStats' in cmd_doc.keys():
                if cmd_doc.get('printCollectionStats') == 1:
                    return PringCollectionStats(self.db).to_sql()
            elif 'printReplicationInfo' in cmd_doc.keys():
                if cmd_doc.get('printReplicationInfo') == 1:
                    return PrintReplicationInfo(self.db).to_sql()
            elif 'printShardingStatus' in cmd_doc.keys():
                if cmd_doc.get('printShardingStatus') == 1:
                    verbose = cmd_doc.get('verbose', False)
                    return PrintShardingStatus(self.db, verbose).to_sql()
            elif 'printSlaveReplicationInfo' in cmd_doc.keys():
                if cmd_doc.get('printSlaveReplicationInfo') == 1:
                    return PrintSlaveReplicationInfo(self.db).to_sql()
            elif 'resetError' in cmd_doc.keys():
                if cmd_doc.get('resetError') == 1:
                    return ResetError(self.db).to_sql()
            elif 'serverBuildInfo' in cmd_doc.keys():
                if cmd_doc.get('serverBuildInfo') == 1:
                    return ServerBuildInfo(self.db).to_sql()
            elif 'serverCmdLineOpts' in cmd_doc.keys():
                if cmd_doc.get('serverCmdLineOpts') == 1:
                    return ServerCmdLineOpts(self.db).to_sql()
            elif 'setLogLevel' in cmd_doc.keys():
                if cmd_doc.get('setLogLevel') == 1:
                    level = cmd_doc.get('level')
                    component = cmd_doc.get('component', None)
                    return SetLogLevel(self.db, level, component).to_sql()
            elif 'setProfilingLevel' in cmd_doc.keys():
                if cmd_doc.get('setProfilingLevel') == 1:
                    lev = cmd_doc.get('level')
                    slowms = cmd_doc.get('slowms', None)
                    return SetProfilingLevel(self.db, lev, slowms).to_sql()
            elif 'shutdownServer' in cmd_doc.keys():
                if cmd_doc.get('shutdownServer') == 1:
                    return ShutdwonServer(self.db).to_sql()
            elif 'stats' in cmd_doc.keys():
                if cmd_doc.get('stats') == 1:
                    scale = cmd_doc.get('scale')
                    return Stats(self.db, scale).to_sql()
            elif 'version' in cmd_doc.keys():
                if cmd_doc.get('version') == 1:
                    return Version(self.db).to_sql()
            elif 'upgradeCheck' in cmd_doc.keys():
                if cmd_doc.get('upgradeCheck') == 1:
                    scope = cmd_doc.get('scope', None)
                    return UpgradeCheck(self.db, scope).to_sql()
            elif 'upgradeCheckAllDB' in cmd_doc.keys():
                if cmd_doc.get('upgradeCheckAllDB') == 1:
                    return UpgradeCheckAllDB(self.db).to_sql()
            ## collection methods ## 
            elif 'aggregate' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('aggregate'))
                pipeline = cmd_doc.get('pipeline')
                ag_opts = cmd_doc.get('options')
                return Aggregate(table, pipeline, ag_opts).to_sql()
            elif 'bulkWrite' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('bulkWrite'))
                bw_ops = cmd_doc.get('operations')
                bw_wc = cmd_doc.get('writeConcern', None)
                return BulkWrite(table, bw_ops, bw_wc).to_sql()
            elif 'count' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('count'))
                cot_query = cmd_doc.get('query')
                return Count(table, cot_query).to_sql()
            elif 'copyTo' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('copyTo'))
                new_coll = cmd_doc.get('newCollection')
                return CopyTo(table, new_coll).to_sql()
            elif 'createIndex' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('createIndex'))
                ci_keys = cmd_doc.get('keys')
                ci_opts = cmd_doc.get('options', None)
                return CreateIndex(table, ci_keys, ci_opts).to_sql()
            elif 'dataSize' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('dataSize'))
                return DataSize(table).to_sql()
            elif 'deleteOne' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('deleteOne'))
                query = cmd_doc.get('filter')
                w_c = cmd_doc.get('writeConcern', None)
                return DeleteOne(table, query, w_c).to_sql()
            elif 'deleteMany' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('deleteMany'))
                w_c = cmd_doc.get('writeConcern', None)
                return DeleteMany(table, w_c).to_sql()
            elif 'dropIndex' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('dropIndex'))
                index_name = cmd_doc.get('index')
                return DropIndex(table, index_name).to_sql()
            elif 'dropIndexes' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('dropIndexes'))
                return DropIndexes(table).to_sql()
            elif 'ensureIndex' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('ensureIndex'))
                keys = cmd_doc.get('keys')
                options = cmd_doc.get('options', None)
                return EnsureIndex(table, keys, options).to_sql()
            elif 'explain' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('explain'))
                verbosity = cmd_doc.get('verbosity', None)
                return Explain(table, verbosity).to_sql()
            elif 'find' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('find'))
                query = cmd_doc.get('query',None)
                projection = cmd_doc.get('projection', None)
                return Find(table, query, projection).to_sql()
            elif 'findOne' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('findOne'))
                query = cmd_doc.get('query', None)
                projection = cmd_doc.get('projection', None)
                return FindOne(table, query, projection).to_sql()
            elif 'findOneAndDelete' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('findOneAndDelete'))
                query = cmd_doc.get('filter', None)
                options = cmd_doc.get('options', None)
                return FindOneAndDelete(table, query, options).to_sql()
            elif 'findOneAndReplace' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('findOneAndReplace'))
                query = cmd_doc.get('filter', None)
                replacement = cmd_doc.get('replacement')
                options = cmd_doc.get('options', None)
                return FindOneAndReplace(table, query, replacement, options).to_sql()
            elif 'findOneAndUpdate' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('findOneAndUpdate'))
                query = cmd_doc.get('filter', None)
                update = cmd_doc.get('update')
                options = cmd_doc.get('options', None)
                return FindOneAndUpdate(table, query, update, options).to_sql()
            elif 'getIndexes' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('getIndexes'))
                return GetIndexes(table).to_sql()
            elif 'getShardDistribution' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('getShardDistribution'))
                return GetShardDistribution(table).to_sql()
            elif 'getShardVersion' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('getShardVersion'))
                return GetShardVersion(table).to_sql()
            elif 'group' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('group'))
                del cmd_doc['group']
                doc = cmd_doc
                return Group(table, doc).to_sql()
            elif 'insert' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('insert'))
                doc = cmd_doc.get('document')
                w_c = cmd_doc.get('writeConcern', None)
                ordered = cmd_doc.get('ordered', False)
                return Insert(table, doc, w_c, ordered).to_sql()
            elif 'insertOne' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('insertOne'))
                doc = cmd_doc.get('document')
                w_c = cmd_doc.get('writeConcern', None)
                return InsertOne(table, doc, w_c).to_sql()
            elif 'insertMany' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('insertMany'))
                docs = cmd_doc.get('document')
                del cmd_doc['insertMany']
                del cmd_doc['document']
                options = cmd_doc
                #w_c = cmd_doc.get('writeConcern', None)
                #ordered = cmd_doc.get('ordered', None)
                return InsertMany(table, docs, options).to_sql()
            elif 'isCapped' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('isCapped'))
                return IsCapped(table).to_sql()
            elif 'mapReduce' in cmd_doc.get('mapReduce'):
                table = Table(self.db, cmd_doc.get('mapReduce'))
                map_func = cmd_doc.get('map')
                reduce_func = cmd_doc.get('reduce')
                del cmd_doc['mapReduce']
                del cmd_doc['map']
                del cmd_doc['reduce']
                options = cmd_doc
                return mapReduce(table, map_func, reduce_func, options).to_sql()
            elif 'reIndex' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('reIndex'))
                return ReIndex(table).to_sql()
            elif 'replaceOne' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('replaceOne'))
                query = cmd_doc.get('filter')
                replacement = cmd_doc.get('replacement')
                del cmd_doc['replaceOne']
                del cmd_doc['filter']
                del cmd_doc['replacement']
                options = cmd_doc
                return replaceOne(table, query, replacement, options).to_sql()
            elif 'remove' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('remove'))
                query = cmd_doc.get('filter')
                del cmd_doc['remove']
                del cmd_doc['filter']
                options = cmd_doc
                return Remove(table, query, options).to_sql()
            elif 'save' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('save'))
                doc = cmd_doc.get('document')
                w_c = cmd_doc.get('writeConcern')
                return Save(table, doc, w_c).to_sql()
            elif 'stats' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('stats'))
                scale = cmd_get('scale', None)
                options = cmd_get('options', None)
                return Stats(table, scale, options).to_sql()
            elif 'storageSize' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('storageSize'))
                return StorageSize(table).to_sql()
            elif 'totalSize' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('totalSize'))
                return TotalSize(table).to_sql()
            elif 'totalIndexSize' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('totalIndexSize'))
                return TotalIndexSize(table).to_sql()
            elif 'Update' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('Update'))
                query = cmd_doc.get('query')
                update = cmd_doc.get('update')
                del cmd_doc['update']
                del cmd_doc['Update']
                del cmd_doc['query']
                options = cmd_doc
                return Update(table, query, update, options).to_sql()
            elif 'updateOne' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('updateOne'))
                query = cmd_doc.get('filter')
                update = cmd_doc.get('update')
                del cmd_doc['updateOne']
                del cmd_doc['filter']
                del cmd_doc['update']
                options = cmd_doc
                return UpdateOne(table, query, update, options).to_sql()
            elif 'updateMany' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('updateMany'))
                query = cmd_doc.get('filter')
                update = cmd_doc.get('update')
                del cmd_doc['updateMany']
                del cmd_doc['query']
                del cmd_doc['update']
                options = cmd_doc
                return UpdateMany(table, query, update, options).to_sql()
            elif 'validate' in cmd_doc.keys():
                table = Table(self.db, cmd_doc.get('validate'))
                full = cmd_doc.get('full', None)
                return Validate(table, full).to_sql()


       
###classes for collection and collection methods#####                                    

class Table(object, ExtractSql):
    def __init__(self, db, name):
        self.db = db
        self.name = name

    def find(self, condition=None, field=None):
        return Find(self, condition, field)

    def insert(self, doc, w_c=None, orders=None):
        return Insert(self, doc)

    def remove(self, condition=None, options=None):
        return Remove(self, condition)

    def update(self, condition, operation, options=None):
        return Update(self, condition, operation)

    def save(self, doc, write_concern=None):
        return Save(self, doc, write_concern)

    def __getattr__(self, attr):
        pass

    def aggregate(self, stages, option=None):
        return Aggregate(self, stages, option)

    def count(self, query=None):
        return Count(self, query)
    
    def copyTo(self, new_collection):
        return CopyTo(self, new_collection)

    def dataSize(self):
        return DataSize(table)

    def deleteOne(self, query, w_c=None):
        return DeleteOne(self, query, w_c)

    def deleteMany(self, query,w_c=None):
        return DeleteMany(self, query, w_c)

    def createIndex(self,keys, options=None):
        return CreateIndex(self, keys, options)

    def dataSize(self):
        return DataSize(self)

    def dropIndex(self,index):
        return DropIndex(self, index)

    def dropIndexes(self):
        return DropIndexes(self)

    def ensureIndex(self, keys, options=None):
        return EnsureIndex(self, keys, options)

    def explain(self, verbosity='queryPlanner'):
        return Explain(self, verbosity)

    def findAndModify(self,doc):
        return FindAndModify(self, doc)

    def dropCollection(self):
        return DropColletion(self)

    def distinct(self, field, query):
        return Distinct(self, field, query)

    def findOne(self, query, projection):
        return FindOne(self, query, projection)

    def findOneAndDelete(self, query, options=None):
        return FindOneAndDelete(self, query, options)

    def findOneAndReplace(self, query, replacement, options=None):
        return FindOneAndReplace(self, query, replacement, options)

    def findOneAndUpdate(self, query, update, options=None):
        return FindOneAndUpdate(self, query, update, options)

    def getIndexes(self):
        return GetIndexes(self)

    def getShardDistribution(self):
        raise ValueError('getShardDistribution unsupported')
        #return GetShardDistribution(self)

    def getShardVersion(self):
        raise ValueError('getShardVersion unsupported')
        #return GetShardDistribution(self)

    def group(self, doc):
        return Group(self, doc)

    def insertOne(self, doc, options=None):
        return InsertOne(self, doc, options)

    def insertMany(self, docs, options=None):
        return InsertMany(self, docs, options=None)

    def isCapped(self):
        return IsCapped(self)

    def mapReduce(self, map_func, reduce_func, doc):
        return MapReduce(self, map_func, reduce_func, doc)

    def reIndex(self):
        return ReIndex(self)

    def replaceOne(self, query, rep, option=None):
        return ReplaceOne(self, query, rep,option)

    def renameCollection(self, new_name, option=False):
        return RenameCollection(self, new_name, option)

    def stats(self, scale=None, option=None):
        return Stats(self, scale, option)

    def storageSize(self):
        return StorageSize(self)

    def totalSize(self):
        return TotalSize(self)

    def totalIndexSize(self):
        return TotalIndexSize(self)

    def updateOne(self, query, update, options=None):
        return Update(self, query, update, options)

    def updateMany(self, query, update, options=None):
        return UpdateMany(self, query, update, options)

    def bulkWrite(self, operations,options=None):
        return BulkWrite(self, operations, options)

    def drop(self):
        return Drop(self)

    def validate(self, full=False):
        return Validate(self)

    
class Drop(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        return 'DROP TABLE %s' % self.table.name

class BulkWrite(object, ExtractSql):
    def __init__(self, table, operations, options):
        self.table = table
        self.operations = operations
        self.options = options

    def to_sql(self):
        operation_fmt_list = []
        for operation in self.operations:
            if operation.keys()[0] == 'insertOne':
                doc = operation.values()[0].get('documnet', None)
                insert_one_fmt = InsertOne(self.table, doc).to_sql()
                operation_fmt_list.append(insert_one_fmt)
            elif operation.keys()[0] == 'updateOne':
                query = operation.values()[0].get('filter', None)
                update = operation.values()[0].get('update', None)
                upsert = operation.values()[0].get('upsert', False)
                update_one_fmt = UpdateOne(self.table, query, update)
                operation_fmt_list.append(update_one_fmt)
            elif operation.keys()[0] == 'updateMany':
                query = operation.values()[0].get('filter', None)
                update = operation.values()[0].get('update', None)
                upsert = operation.values()[0].get('upsert', False)
                update_many_fmt = UpdateMany(self.table, query, update)
                operation_fmt_list.append(update_many_fmt)
            elif operation.keys()[0] == 'replaceOne':
                query = operation.values()[0].get('filter', None)
                replace = operation.values()[0].get('replacement', None)
                upsert = operation.values()[0].get('upsert', False)
                replace_one_fmt = UpdateOne(self.table, query, replace)
                operation_fmt_list.append(replace_one_fmt)
            elif operation.keys()[0] == 'deleteOne':
                query = operation.values()[0].get('filter', None)
                delete_one_fmt = DeleteOne(self.table, query)
                operation_fmt_list.append(delete_one_fmt)
            elif operation.keys()[0] == 'deleteMany':
                query = operation.values[0].get('filter', None)
                delete_many_fmt = DeleteMany(self.table, query)
                operation_fmt_list.append(delete_many_fmt)
            if len(operation_fmt_list) == 0:
                operation_fmt = ''
            elif len(operation_fmt_list) == 1:
                operation_fmt = operation_fmt_list[0]
            else:
                operation_fmt = ';'.join(operation_fmt_list)
            return operation_fmt

        
def handle_condition(condition):
        """a condition dict -> a sql format string """
        fmt = []
        if condition == {} or condition is None:
                return ''
        for field, value in condition.items():
            if isinstance(value, list):
                l_fmt = []
                for v in value:
                    l_fmt.append(handle_condition(v))
                if field == '$or':
                    l_fmt = '(' + ' or '.join(l_fmt) + ')'
                elif field == '$and':
                    l_fmt = '(' + ' and '.join(l_fmt) +')'
                fmt.append(l_fmt)
        
            elif isinstance(value, dict):
                d_fmt = []
                for k, v in value.items():
                    if k == '$lt':
                        d_fmt.append('{0}<{1}'.format(field, v))
                    elif k == '$lte':
                        d_fmt.append('{0}<={1}'.format(field, v))
                    elif k == '$gt':
                        d_fmt.append('{0}>{1}'.format(field, v))
                    elif k == '$gte':
                        d_fmt.append('{0}>={1}'.format(field, v))
                    elif k == '$ne':
                        d_fmt.append('{0}!={1}'.format(field, v))
                    elif k == '$eq':
                        d_fmt.append('{0}={1}'.format(field, v))
                    elif k == "$in":
                        d_fmt.append('{0} IN {1}'.format(field, str(tuple(v))))
                    

                    else:
                        d_fmt.append(handle_condition(v))
                if len(d_fmt) == 1:
                    d_fmt = d_fmt[0]
                else:
                    d_fmt = ' and '.join(d_fmt)
                fmt.append(d_fmt)
            elif isinstance(value, tuple):
                fmt.append('%s %s' % (field, value[0]))
                
            else:
                fmt.append('{0}={1}'.format(field, value))

        if len(fmt) == 1:
            fmt = fmt[0]
            return fmt
        fmt = ' and '.join(fmt)
        return fmt
        

class Save(object, ExtractSql):
    def __init__(self, table, doc, write_concern=None):
        self.table = table
        self.doc = doc
        self.write_concern = write_concern

    def to_sql(self):
        doc = self.doc.copy()
        if '_id' in self.doc.keys():
            id = doc.pop('_id')
            return Update(self.table, {"_id": id}, doc).to_sql()
        return Insert(self.table, self.doc).to_sql()

class Insert(object, ExtractSql):
    """ Insert doc string"""
    def __init__(self, table, doc, w_c=None, orders=None):
        self.table = table
        self.doc = doc
        self.w_c = w_c
        self.orders = orders

    def handle_doc(self):
        """ list or dict -> format string """
        values_list = []
        fields_list = []
        if isinstance(self.doc, list):
            new_doc = []
            fields_list = [field for d in self.doc for field in d.keys()]
            for d in self.doc:
                for k in set(fields_list):
                    d[k] = d.get(k, 'null')
                new_doc.append(d)

            for new_d in new_doc:
                mul_value_list = [val for val in new_d.values()]
                #values_list.append('({0})'.format(','.join(mul_value_list)))
                values_list.append(str(tuple(mul_value_list)))
            mul_value_fmt = ','.join(values_list)
            mul_field_fmt = ','.join(set(fields_list))
            return mul_field_fmt, mul_value_fmt
        if isinstance(self.doc, dict):
            sin_list = []
            for k, v in self.doc.items():
                sin_list.append(str(v))
                fields_list.append(k)
            sin_field_fmt = ','.join(fields_list)
            sin_value_fmt = '({})'.format(','.join(sin_list))
            return sin_field_fmt, sin_value_fmt

    def to_sql(self):
        field_fmt, value_fmt = self.handle_doc()
        return "INSERT INTO %s (%s) VALUES %s" % (self.table.name, field_fmt, value_fmt)
        

class Remove(object, ExtractSql):
    def __init__(self, table, condition=None, options=None):
        self.table = table
        self.condition = condition
        self.options = options

    def to_sql(self):
        condition = handle_condition(self.condition)
        if condition == '':
            return "DELETE FROM table %s" % self.table.name
        return "DELETE FROM %s WHERE %s" % (self.table.name, condition)
        

class Update(object, ExtractSql):
    def __init__(self, table, condition, operation, options=None):
        self.table = table
        self.condition = condition
        self.operation = operation
        self.options = options

    def handle_operation(self):
        operation_fmt_list = []
        for op, val in self.operation.items():
            if op == '$set':
                for k, v in val.items():
                    operation_fmt_list.append('set {0}={1}'.format(k, v))
            elif op == '$inc':
                for k, v in val.items():
                    operation_fmt_list.append('set {0}={0}+{1}'.format(k, v))
            elif op == '$unset':
                operation_fmt_list.append('alter table {0} drop {1}'.format(self.table.name, val.keys()[0]))
            elif op == '$push':
                operation_fmt_list.append('alter table {0} add {1}, set {2}={3}'.format(self.table.name, val.keys()[0], val.keys()[0], val.values()[0]))
            else:
                operation_fmt_list.append('set {0}={1}'.format(op, val))
           
        if len(operation_fmt_list) == 1:
            return operation_fmt_list[0]
        elif len(operation_fmt_list) == 0:
            return ''
        return ','.join(operation_fmt_list)
            
    def to_sql(self):
        condition = handle_condition(self.condition)
        operation  = self.handle_operation()
        if operation == "":
            raise ValueError('\'Update\' object The secend parameter can\'t be \'{}\'')
        if condition == '':
            return "UPDATE %s %s " % (self.table.name, operation)
        return "UPDATE %s %s WHERE %s" % (self.table.name, operation, condition)
        

class Find(object, ExtractSql):
    """Find doc string """
    
    def __init__(self, table, condition=None, field=None):
        self.table = table
        self.field = field
        self.condition = condition

    def handle_field(self):
        proj_fmt_list = []
        if self.field == {} or self.field is None:
            return ''
        for key, val in self.field.items():
            if val != 0 or val != False:
                proj_fmt_list.append(key)
        if len(proj_fmt_list) == 1:
            proj_fmt = proj_fmt_list.pop()
        proj_fmt = ','.join(proj_fmt_list)
        return proj_fmt

    def to_sql(self):
        condition = handle_condition(self.condition) 
        field = self.handle_field()
        if field != '':
            if condition != '':
                return "SELECT %s FROM %s WHERE %s" % (field, self.table.name, condition)
            return "SELECT %s FROM %s " % (field, self.table.name)
        else:
            if condition != '':
                return "SELECT %s FROM %s WHERE %s" % ("*", self.table.name, condition)
            return "SELECT %s FROM %s " % ("*", self.table.name)
    
    def limit(self, count):
        global L_SWITCH
        if L_SWITCH:
            L_SWITCH = 0
            return Limit(self, count)
        else:
            raise AttributeError('too many limit')
        
    def skip(self, count):
        global S_SWITCH
        if S_SWITCH:
            S_SWITCH = 0
            return Skip(self, count)
        else:
            raise AttributeError('too many skip')

    def sort(self, condition):
        return Sort(self, condition)

    def __getattr__(self, attr):
        pass 

        
class Limit(object, ExtractSql):
    def __init__(self, obj, count):
        self.obj = obj
        self.count = count

    def skip(self, count):
        global S_SWITCH
        if S_SWITCH:
            S_SWITCH = 0
            return Skip(self, count)
        else:
            raise AttributeError('too many skip')

    def to_sql(self):
        return "%s LIMIT %d" % (self.obj.to_sql(), self.count)


class Skip(object, ExtractSql):
    def __init__(self, obj, count):
        self.obj = obj
        self.count = count

    def limit(self, count):
        global L_SWITCH
        if L_SWITCH:
            L_SWITCH = 0
            return Limit(self, count)
        else:
            raise AttributeError("too many limit")

    def to_sql(self):
        return "%s OFFSET %d " % (self.obj.to_sql(), self.count)
    


class Sort(object, ExtractSql):
    def __init__(self, obj, condition):
        self.obj = obj
        self.condition = condition
    def limit(self, count):
        global L_SWITCH
        if L_SWITCH:
            L_SWITCH = 0
            return Limit(self, count)
        else:
            raise AttributeError("too many limit")

    def skip(self, count):
        global S_SWITCH
        if S_SWITCH:
            S_SWITCH = 0
            return Skip(self, count)
        else:
            raise AttributeError("too many skip ")        
        
    def to_sql(self):
        sort_fmt_list = []
        for key, val in self.condition.items():
            if val == 1:
                sort_fmt_list.append('{} ASC'.format(key))
            else:
                sort_fmt_list.append('{} DESC'.format(key))
        if len(sort_fmt_list) == 0:
            return ''
        elif len(sort_fmt_list) == 1:
            sort_fmt = sort_fmt_list[0]
        else:
            sort_fmt = ','.join(sort_fmt_list)
        return "%s ORDER BY %s" % (self.obj.to_sql(), sort_fmt)
    

class Aggregate(object, ExtractSql):
    def __init__(self, table, stages, option=None):
        self.table = table
        self.stages = stages
        self.option = option

    def handle_match(self, match_value):
        match_fmt = handle_condition(match_value)
        return match_fmt

    def handle_group(self, group_value):
        group_fmt_list =[]
        proj_fmt_list = []
        for key, val in group_value.items():
            if key == '_id':
                if isinstance(val, str) and val != 'null':
                    group_fmt_list.append(val)
                    proj_fmt_list.append(val)
                elif isinstance(val, dict):
                    for k in val.keys():
                        group_fmt_list.append(k)
                        proj_fmt_list.append(k)
            else:
                for k, v in val.items():
                    if k =='$sum':
                        proj_fmt_list.append("%s AS %s" % (self.handle_sum(v), key))
        if len(proj_fmt_list) < 2:
            proj_fmt = proj_fmt_list[0]
        else:
            proj_fmt = ','.join(proj_fmt_list)
       
        if len(group_fmt_list) == 1:
            group_fmt = 'GROUP BY ' + group_fmt_list[0] 
        elif len(group_fmt_list) == 0:
            group_fmt = ''
        else:
            group_fmt = 'GROUP BY ' + ','.join(group_fmt_list)
        return proj_fmt, group_fmt
        
    def handle_project(self, project_value):
        proj_fmt_list = []
        for key, val in project_value.items():
            if val != 0 or val != False:
                if isinstance(val, dict):
                    proj_fmt_list.append(val.keys()[0])
                else:
                    proj_fmt_list.append(key)
        if len(proj_fmt_list) < 2:
            proj_fmt = proj_fmt_list[0]
        else:
            proj_fmt = ','.join(proj_fmt_list)
        return proj_fmt
                
    def handle_sort(self, sort_value):
        sort_fmt_list = []
        for key, val in sort_value.items():
            if val == 1:
                sort_fmt_list.append('%s ASC' % key)
            else:
                sort_fmt_list.append('%s DESC' % key)
        if len(sort_fmt_list) < 2:
            sort_fmt = sort_fmt_list[0]
        else:
            sort_fmt = ','.join(sort_fmt_list)
        return sort_fmt

    def handle_limit(self, limit_value):
        return limit_value

    def handle_skip(self, skip_value):
        return skip_value

    def handle_sum(self, sum_value):
        if str(sum_value).startswith('$'):
            return 'SUM(%s)' % sum_value
        else:
            return 'count(*)'

    def handle_unwind(self, unwind_value):
        raise AttributeError('not  finish the function')

    def to_sql(self):
        select_fmt_list = []
        option_fmt_list = []
        stage_name_list = []
        having_fmt = ''
        sub_group_fmt = ''
        for stage in self.stages:
            stage_name_list.append(stage.keys()[0])

        #print stage_name_list
        for stage in self.stages:
            if stage.keys()[0] == '$match':
                if '$group' in stage_name_list and '$match' in stage_name_list and stage_name_list.index('$match') > stage_name_list.index('$group'):

                    having_fmt = 'HAVING '+ self.handle_match(stage.values()[0])
                    option_fmt_list.append(having_fmt)
                    
                else:
                    option_fmt_list.append('WHERE ' + self.handle_match(stage.values()[0]))
                    stage_name_list.remove('$match')
          
            elif stage.keys()[0] == '$group':
                proj_fmt, group_fmt = self.handle_group(stage.values()[0])
                if stage_name_list.count('$group') == 2:
                    sub_group_fmt = "(SELETCT %s FROM %s %s)" % (proj_fmt, self.table.name, group_fmt)
                    stage_name_list.remove('$group')
                else:
                    select_fmt_list.append(proj_fmt)
                    option_fmt_list.append(group_fmt)

            elif stage.keys()[0] == '$project':
                project_fmt = self.handle_project(stage.values()[0])
                select_fmt_list.append(project_fmt)

            elif stage.keys()[0] == '$sort':
                sort_fmt = self.handle_sort(stage.values()[0])
                option_fmt_list.append('ORDER BY' + sort_fmt
)

            elif stage.keys()[0] == '$limit':
                limit_fmt = self.handle_limit(stage.values()[0])
                option_fmt_list.append('LIMIT %s' % limit_fmt)

            elif stage.keys()[0] == '$skip':
                skip_fmt = self.handle_skip(stage.values()[0])
                option_fmt_list.append('SKIP %s' % skip_fmt)

            elif stage.keys()[0] == '$sum':
                sum_fmt = self.handle_sum(stage.values()[0])
                select_fmt_list.append(sum_fmt)

            elif stage.keys()[0] == '$unwind':
                unwind_fmt = self.handle_unwind(stage.values()[0])
                option_fmt_list.append(unwind_fmt)    
        if len(select_fmt_list) == 1:
            select_fmt = select_fmt_list[0]
        elif len(select_fmt_list) == 0:
            select_fmt = '*'
        else:
            select_fmt = ','.join(select_fmt_list)
        if len(option_fmt_list) == 1:
            option_fmt = option_fmt_list[0]
        elif len(option_fmt_list) == 0:
            option_fmt = ''
        else:
            option_fmt = ' '.join(option_fmt_list)
        #print option_fmt
        sql = 'SELECT %s From %s %s' % (select_fmt, self.table.name if sub_group_fmt == '' else sub_group_fmt, option_fmt)
        return sql

class Count(object, ExtractSql):
    def __init__(self, table, query=None):
        self.table = table
        self.query = query

    def to_sql(self):
        if self.query is None:
            condition_fmt = ''
        else:
            condition_fmt = 'WHERE ' + handle_condition(self.query)
        proj_fmt = 'count(*)'
        return 'SELECT %s FROM %s %s' % (proj_fmt, self.table.name, condition_fmt)


class CopyTo(object, ExtractSql):
    def __init__(self, table, new_collection):
        self.table = table
        self.new_collection = new_collection

    def to_sql(self):
        return 'SELECT * INTO %s FROM %s' % (self.new_collection, self.table.name)



class DataSize(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        return 'SELECT * FROM %s %s' % (table_name, where_fmt)


class DataSize(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        proj_fmt = 'DATA_LENGTH, INDEXT_LENGTH'
        where_fmt = 'TABLE_NAME=%s' % self.table.name
        return 'SELECT %s FROM %s %s' % (proj_fmt, table_name, where_fmt)



class DeleteOne(object, ExtractSql):
    def __init__(self, table, query, w_c=None):
        self.table = table
        self.query = query
        self.w_c = w_c

    def to_sql(self):
        return '%s LIMIT 1' % Remove(self.table, self.query).to_sql() 


class DeleteMany(object, ExtractSql):
    def __init__(self, table, query, w_c=None):
        self.table = table
        self.query = query
        self.w_c = w_c

    def to_sql(self):
        return Remove(self.table, self.query).to_sql()


class CreateIndex(object, ExtractSql):
    def __init__(self, table, keys, option=None):
        self.table = table
        self.keys = keys
        self.option = option

    def handle_keys(self):
        index_name_list = []
        index_field_list = []
        for key, val in self.keys.items():
            if val == 1:
                index_field_list.append('{0} ASC'.format(key))
                index_name_list.append('{0}_asc'.format(key))
            elif val == -1:
                index_field_list.append('%s DESC' % key)
                index_name_list.append('%s_desc' % key)
        index_name = '_'.join(index_name_list)
        index_field = ','.join(index_field_list)
        return index_name, index_field
                
    def to_sql(self):
        index_name, field_name = self.handle_keys()
        return 'CREATE INDEX %s ON %s (%s)' % (index_name, self.table.name, field_name)

class DropIndex(object, ExtractSql):
    def __init__(self, table, index):
        self.table = table
        self.index = index

    def to_sql(self):
        table_name = self.table.name
        if isinstance(self.index, str):
            index_name = self.index
        elif isinstance(self.index,dict):
            raise ValueError('parameter must be string: index\'s name')
        return 'DROP INDEX %s ON %s' % (index_name, table_name)


class DropIndexes(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema.STATISTICS'
        where_fmt = 'TABLE_SCHEMA=%s AND TABLE_NAME="%s"' % (self.table.db.name, self.table.name)
        return "DELETE FORM %s %s" % (table_name, where_fmt)


class EnsureIndex(object, ExtractSql):
    def __init__(self, table, keys, options=None):
        self.create_index = CreateIndex(table, keys, options)

    def to_sql(self):
        return self.create_index.to_sql()


class Explain(object, ExtractSql):
    def __init__(self, table, verbosity='queryPlanner'):
        raise ValueError('Explain unsupported')

    def to_sql(self):
        pass

class FindAndModify(object, ExtractSql):
    def __init__(self, table, doc):
        self.table = table
        self.doc = doc

    def to_sql(self):
        fields_value = self.doc.get('fields', None)
        query_value = self.doc.get('query', None)
        update_value = self.doc.get('update', None)
        remove_value = self.doc.get('remove', False)
        sort_value = self.doc.get('sort', None)
        new_value = self.doc.get('new', False)
        upsert_value = self.doc.get('upsert', False)
        bypassDocVal_value = self.doc.get('bypassDocumentValidation', None)
        if sort_value:
            orginal_sql = '%s LIMIT 1' % Find(self.table, query_value, fields_value).sort(sort_value).to_sql()
        else:
            original_sql = '%s LIMIT 1' % Find(self.table, query_value, fields_value).to_sql()
        sql = ''
        if update_value and remove_value:
            raise ValueError("'update','remove'不能同时出现")
        elif update_value:
            update_sql = Update(self.table, query_value, update_value).to_sql()
            if new_value:
                sql = update_sql + ';' + original_sql
            else:
                sql = original_sql + ';' + update_sql
            #upsert 暂不支持
            #if upsert_value:
             #   insert_sql = Insert(self.table, 
             #   sql = original_sql + ';' + insert_sql 
           # else:
            #    sql = original_sql + ';' + update_sql
        elif remove_value:
            remove_sql = Remove(self.table, query_value)
            sql = original_sql + ';' + remove_sql
        return sql

        
class DropCollection(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        return 'DROP TABLE %s' % self.table.name

class Distinct(object, ExtractSql):
    def __init__(self, table, field, query):
        self.table = table
        self.field = field
        self.query = query

    def to_sql(self):
        query = handle_condition(self.query)
        if query:
            query = 'WHERE ' + query
            return 'SELECT DISTINCT %s FROM %s %s' % (self.field, self.table.name, query)
        else:
            return 'SELECT DISTINCT %s FROM %s' % (self.field, self.table.name) 

            

class FindOne(object, ExtractSql):
    def __init__(self, table, query, projection):
        self.table = table
        self.query = query
        self.projection = projection
       
    def to_sql(self):
        return "%s LIMIT 1" % Find(self.table, self.query, self.projection).to_sql()


class FindOneAndDelete(object, ExtractSql):
    def __init__(self, table, query, options=None):
        self.table= table
        self.query = query
        self.options = options

    def handle_sort(self):
        sort_fmt_list = []
        sort_value = self.options.get('sort', None)
        if sort_value:
            for key, val in sort_value.items():
                if val == 1:
                    sort_fmt_list.append('%s ASC' % key)
                else:
                    sort_fmt_list.append('%s DESC' % key)
        if len(sort_fmt_list) == 0:
            sort_fmt = ''
        elif len(sort_fmt_list) == 1:
            sort_fmt = 'ORDER BY %s' % sort_fmt_list[0]
        else:
            sort_fmt = 'ORDER BY %s' % ','.join(sort_fmt_list)
        return sort_fmt

    def to_sql(self):
        import time
        start = time.time()#为了支持outtime， 待实现
        table = self.table
        query = self.query
        if self.options:
            projections = self.options.get('projection', None)
            sort_value = self.options.get('sort', None)
            max_time = self.options.get('maxTimeMs', None)
        else:
            projections = {}
            sort_value = {}
            max_time = None
        if sort_value:
            original_sql = Find(table, query, projections).sort(sort_value).limit(1).to_sql()
            sort_fmt = self.handle_sort()
            delete_sql = '%s %s LIMIT 1' % (Remove(self.table, query).to_sql(), sort_fmt)
        else:
            original_sql = '%s LIMIT 1' % Find(table, query, projections).to_sql()
            delete_sql = '%s LIMIT 1' % Remove(self.table, query).to_sql()
        sql = original_sql + ';' + delete_sql
        return sql


class FindOneAndReplace(object, ExtractSql):
    def __init__(self, table, query, replacement, options=None):
        self.table= table
        self.query = query
        self.replacement = replacement
        self.options = options

    def to_sql(self):
        if self.options:
            projection = self.options.get(projection, None)
            sort_value = self.options.get(sort, None)
            max_time = self.options.get('maxTimeMS', None)
            upsert = self.options.get('upsert', False)
            return_new = self.options.get('returnNewDocument', False)
        else:
            projection = None
            sort_value = None
            max_time = None
            upset = False
            return_new = False
        if sort_value:
            original_sql = "%s LIMIT 1" % Find(self.table, self.query,projection).sort(sort_value).to_sql()
        else:
            original_sql = "%s LIMIT 1" % Find(self.table, self.query, projection).to_sql()
        
        update_sql = Update(self.table, self.query, self.replacement).to_sql()
        
        if return_new:
            sql = update_sql + ';' + original_sql
        else:
            sql = original_sql + ';' + update_sql
        #upsert暂不支持,可用子查询实现，待完成。XXXAndxxx之类的类也可以实现
        return sql

class FindOneAndUpdate(object, ExtractSql):
    def __init__(self, table, query, update, options=None):
        self.table = table
        self.query = query
        self.update = update
        self.options = options

    def to_sql(self):
        if self.options:
            sort_value = self.options.get('sort', None)
            return_new = self.options.get('returnNewDocument', None)
            projection = self.options.get('projection', None)
        else:
            sort_value = None
            return_new = None
            projection = None
        if sort_value:
            sub_selection = "%s LIMIT 1" % Find(self.table, self.query, projection).sort(sort_value).to_sql()
        else:
            sub_selection = "%s LIMIT 1" % Find(self.table, self.query, projection).to_sql()
        update_sql = Update(self.table, {'exists': (sub_selection,)}, self.update)
        if return_new:
            sql = update_sql + ';' + sub_selection
        else:
            sql = sub_selection
        return sql

        
class GetIndexes(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        return 'SHOW INDEX FORM %s' % self.table.name


class GetShardDistribution(object, ExtractSql):
    pass


class GetShardVersion(object, ExtractSql):
    pass


class InsertOne(object, ExtractSql):
    def __init__(self, table, doc, options=None):
        self.table = table
        self.doc = doc
        self.options = options

    def to_sql(self):
        return Insert(self.table, self.doc, self.options).to_sql()


class InsertMany(object, ExtractSql):
    def __init__(self, table, docs, options=None):
        self.table = table
        self.docs = docs
        self.options = options

    def to_sql(self):
        return Insert(self.table, self.docs, self.options).to_sql()


class IsCapped(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema'
        where_fmt = 'TABLE_SCHMA=%s' % self.table.name
        return 'SELECT TABLE_TYPE FORM %s %s' % (table_name, where_fmt)
        

class Group(object, ExtractSql):
    """键keyf, reduce, fnalize 对应值为function，以及键initial是对reduce的操作等未实现
    """
    def __init__(self, table, doc):
        self.table = table
        self.doc = doc

    def handle_key(self):
        group_fmt_list = []
        proj_fmt_list = []
        for key, val in self.key.items():
            if val == 1:
                proj_fmt_list.append(key)
                group_fmt_list.append('%s ASC' % key)
            else:
                proj_fmt_list.append(key)
                group_fmt_list.append('%s DESC' % key)
        if len(proj_fmt_list) == 0:
            return ''
        elif len(proj_fmt_list) == 1:
            proj_fmt = proj_fmt_list[0]
        else:
            proj_fmt = ','.join(proj_fmt_list)

        if len(group_fmt_list) == 0:
            return ''
        elif len(group_list_list) == 1:
            group_fmt = 'GROUP BY ' + group_fmt_list[0]
        else:
            group_fmt = 'GROUP BY' + ' '.join(group_fmt_list)
        return proj_fmt, group_fmt
        
    def to_sql(self):
        proj_fmt_list = []
        option_fmt_list = []
        where_fmt = handle_condition(self.doc['cond'])
        proj_fmt, group_fmt = self.handle_key()
        if where_fmt:
            option_fmt_list.append('WHERE ' + where_fmt)
        proj_fmt_list.append(proj_fmt)
        option_fmt_list.append(group_fmt)
        if len(proj_fmt_list) == 0:
            proj_fmt = '*'
        elif len(proj_fmt_list) == 1:
            proj_fmt = proj_fmt_list[0]
        else:
            proj_fmt = ','.join(proj_fmt_list)
            
        if len(option_fmt_list) == 1:
            option_fmt = option_fmt_list[0]
        elif len(option_fmt_list) == 0:
            option_fmt = ''
        else:
            option_fmt = ' '.join(option_fmt_list)
        return 'SELECT %s FROM %s %s' % (proj_fmt, self.table.name, option_fmt)


class MapReduce(object, ExtractSql):
    def __init__(self, table, map_func, reduce_func, doc):
        self.table = table
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.doc = doc
    

    def to_sql(self):
        raise ValueError('MapReduce uncompleted')


class ReIndex(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        return "REPAIR TABLE %s QUICK" % self.table.name
            
    
class ReplaceOne(object, ExtractSql):
    def __init__(self, table, query, rep, option=None):
        self.table = table
        self.query = query
        self.rep = rep
        self.option = option

    def to_sql(self):
        sub_selection = "%s LIMIT 1" % Find(self.table, self.query).to_sql()
        return Update(self.table, {'exists': (sub_selection,)}, self.rep, self.option).to_sql()


class RenameCollection(object, ExtractSql):
    def __init__(self, table, new_name, option=False):
        self.table = table
        self.new_name = new_name
        self.option = option

    def to_sql(self):
        return 'ALTER TABLE %s RENAME TO %s' % (self.table.name, self.new_name)

        
class Stats(object, ExtractSql):
    def __init__(self, table, scale=None, option=None):
        self.table = table
        self.scale = scale
        self.option = option

    def to_sql(self):
        table_name = 'information_schema.STATISTICS'
        where_fmt = 'TABLE_NAME="%s"' % self.table.name
        return 'SELECT * FROM %s %s' % (table_name, where_fmt)


class StorageSize(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        return 'SELECT DATE_LENGTH AS  storagesize FROM %s WHERE TABLE_NAME="%s" ' % (table_name, self.table.name)


class TotalSize(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        return 'SELECT DATA_LENGTH + INDEX_LENGTH AS totalsize FROM %s WHERE TABLE_NAME="%s"' % (table_name, self.table.name)


class TotalIndexSize(object, ExtractSql):
    def __init__(self, table):
        self.table = table

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        return 'SELECT INDEX_LENGTH AS totalIndexSize FROM %s WHERE TABLE_NAME = "%s"' % (table_name, self.table.name)


class Validate(object, ExtractSql):
    def __init__(self, table, full=False):
        self.table = table
        self.full = full

    def to_sql(self):
        if self.full:
            table_name = 'information_schema.STATISTICS, information_schema.TABLES'
            
        else:
            table_name = 'information_schema.TABLES'
        return 'SELECT * FROM %s WHERE TABLE_NAME="%s"' % (table_name, self.table.name)


class UpdateOne(object, ExtractSql):
    def __init__(self, table, query, update, options=None):
        self.table = table
        self.query = query
        self.update = update
        self.options = options

    def to_sql(self):
        return ReplaceOne(self.table, self.query, self.update, self.options).to_sql()


class UpdateMany(object, ExtractSql):
    def __init__(self, table, query, update, options=None):
        self.table = table
        self.query = query
        self.update = update
        self.options = options

    def to_sql(self):
        return Update(self.table, self.query, self.update, self.options).to_sql()
    



if __name__ == "__main__":
    import test
    t = test.Test()
    t.main()
