#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

    def cloneCollection(self,from_host, from_coll, query=None):
        """
        e.g:
        db.cloneCollection('mongodb.example.net:27017', 'users.profiles', {'active': 'true'}) -->从服务器mongodb.example.net:27017的users数据库的profiles集合中复制条件满足{' active': 'true'}的文档到当前的数据库集合中。
        """
        return CloneCollection(self, from_host, from_coll, query)
    
    def copyDatabase(self, from_db, to_db, from_host=None, username=None, password=None, mechanisum=None):
        return CopyDatabase(self, from_db, to_db, from_host, username, password, mechanisum)

    def createCollection(self, name, options=None):
        raise ValueError('unsupported')
        #return CreateCollection(self, name, options)

    
    def currentOp(self, operatoins=None):
        return CurrentOp(self, operations)

    def eval(self, func, arguments):
        return Eval(self, func, arguments)

    def fsyncLock(self):
        raise ValueError('unsupported')

    def fsyncUnlock(self):
        raise ValueError('unsupported')

    def getCollection(self, name):
        return GetCollection(self, name)

    def getCollectionInfos(self):
        return GetColletionInfos(self)

    def getCollectionNames(self):
        return GetCollectionNames(self)

    def getLastError(self, w_c=None, w_timeout=None):
        raise ValueError('unsupported')
        #return GetLastError(self, w_c, w_timeout)

    def getLastErrorObj(self, key=None, w_timeout=None):
        raise ValueError('unsupported')
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
        raise ValueError('unsupported')
        #return GetReplicationInfo(self)

    def getSiblingDB(self, database):
        return GetSiblingDB(self, database) 

    def help(self):
        return Help(self)

    def hostInfo(self):
        return HostInfo(self)

    def isMaster(self):
        raise ValueError('unsupported')
        #return IsMaster(self)

    def killOp(self, opid):
        return KillOp(self, opid)

    def listCommands(self):
        return ListCommands(self)

    def loadServerScripts(self):
        raise ValueError('unsupported')
        #return LoadServerScripts(self)

    def logout(self):
        return Logout(self)

    def printCollectionStatus(self):
        return PrintCollectionStatus(self)

    def printReplicationInfo(self):
        raise ValueError('unsupported')
        #return PrintReplicationInfo(self)

    def printShardingStatus(self, verbose=False):
        raise ValueError('unsupported')
        #return PrintShardingStatus(self, verbose)

    def printSlaveReplicationInfo(self):
        raise ValueError('unsupported')
        #return PrintSlaveReplicationStatus(self)

    def repairDatabase(self):
        raise ValueError('unsupported')
        #return RepairDatabase(self)

    def resetError(self):
        return ResetError(self)

    def runCommand(self, command):
        return RunCommand(self, command)
        


class CreateDatabase(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'CREATE DATABASE %s' % self.db.name


class DropDatabase(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self, db):
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
        else:
            dump_sql = 'mysqldump %s > %s' % (self.from_db, '%s.sql'.format(self.from_db))
            
        
        import_sql = 'mysqlimport %s %s' % (self.to_db,'%s.sql'.format(self.from_db ))
        try:
            os.system(dump_sql)
            os.system(import_sql)
        except:
            raise ValueError('input host, username, password')
        else:
            return 'USE %s' % self.to_db


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
        raise ValueError('unsupported')

class fsynLock(object, ExtractSql):
    pass

class fsynUnlock(object, ExtractSql):
    pass
    
class GetCollection(object, ExtractSql):
    def __init__(self, db, name):
        self.db = db
        self.name

    def to_sql(self):
        table_name = 'information_schema.TABLES'
        where_fmt = 'WHERE ' + 'TABLE_SCHEMA=%s TABLE_NAME=%s' % (self.db.name, self.name)
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
    pass


class GetLastErrorObj(object, ExtractSql):
    pass


class GetLogComponents(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return '%s; %s' % ('USE %s' % self.db.name, 'SHOW status')


class GetMongo(object, ExtractSql):
    def __init__(self,db):
        self.db = db

    def to_sql(self):
        return 'SHOW PROCESSLIST'

    
class GetName(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return 'SELECT database()'


class GetPrevError(object, ExtractSql):
    def __init__(self):
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
    pass


class ResetError(object, ExtractSql):
    def __init__(self, db):
        self.db = db

    def to_sql(self):
        return GetPrevError(self.db).to_sql()

    
class RunCommand(object, ExtractSql):
    def __init__(self, db, command):
        self.db = db
        self.command = command

    def to_sql(self):
        commands = ['createColletion']
        if isinstance(self.command, str):
            pass
        elif isinstance(self.command, dict):
            pass

       
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

    def ensureIndex(self, keys, options):
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

    def findOneAndReplace(self, query, replacement, options):
        return FindOneAndReplacement(self, query, replacement, options)

    def findOneAndUpdate(self, query, update, options=None):
        return FindAndUpdate(self, query, update, options)

    def getIndexes(self):
        return GetIndexes(self)

    def getShardDistribution(self):
        raise ValueError('unsupported')
        #return GetShardDistribution(self)

    def getShardDistribution(self):
        raise ValueError('unsupported')
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

    def renameCollection(self, new_name, option=None):
        return RenameCollection(self, new_name, option=None)

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
        return updateMany(self, query, update, options)

        
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
            raise ValueError('The secend parameter can\'t be \'{}\'')
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
        return 'SELECT * INTO %s FROM %s' (self.new_collection, self.table.name)



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
        raise ValueError('unsupported')#一次性删除！


class EnsureIndex(object, ExtractSql):
    def __init__(self, table, keys, options):
        self.create_index = CreateIndex(table, keys, options)

    def to_sql(self):
        return self.create_index.to_sql()


class Explain(object, ExtractSql):
    def __init(self, table, verbosity='queryPlanner'):
        raise ValueError('unsupported')


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
        bypassDocVal_value = self.doct.get('bypassDocumentValidation', None)
        if sort_value:
            orginal_sql = Find(self.table, query_value, fields_value).sort(sort_value).limit(1)
        else:
            original_sql = Find(self.table, query_value, fields_value).limit(1).to_sql()
        if update_value and remove_value:
            raise ValueError("'update','remove'不能同时出现")
        elif update_value:
            update_sql = Upddate(self.table, query_value, update_value)
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
        if query != '':
            query = 'WHERE ' + query
        return 'SELECT DISTINCT %s FROM %s %s' % (self.field, self.table.name, query)


class FindOne(Find, ExtractSql):
    def __init__(self, table, query, projection):
        self.table = table
        self.query = query
        self.projection = projection
        super(FindOne, self).__init__(self.table, self.query, self.projection)

    def to_sql(self):
        global L_SWITCH
        L_SWITCH = 1
        return super(FindOne, self).limit(1).to_sql()


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
        start = time.time()
        table = self.table
        query = self.options.get('filter', None)
        projections = self.options.get('projection', None)
        sort_value = self.options.get('sort', None)
        max_time = self.options.get('maxTimeMs', None)
        
        if sort:
            original_sql = Find(table, query, projections).sort(sort_value).limit(1).to_sql()
            sort_fmt = self.handle_sort()
            delete_sql = '%s %s LIMIT 1' % (Remove(self.table, query), sort_fmt)
        else:
            original_sql = Find(table, query, projections).limit(1).to_sql()
            delete_sql = '%s LIMIT 1' % Remove(self.table, query)
        sql = original_sql + ';' + delete_sql
        return sql


class FindOneAndReplace(object, ExtractSql):
    def __init__(self, table, query, replacement, options=None):
        self.table= table
        self.query = query
        self.replacement = replacement
        self.options = options

    def to_sql(self):
        projection = self.options.get(projection, None)
        sort_value = self.options.get(sort, None)
        max_time = self.options.get('maxTimeMS', None)
        upsert = self.options.get('upsert', False)
        return_new = self.options.get('returnNewDocument', False)
        
        if sort_value:
            original_sql = Find(self.table, self.query,projection).sort(sort_value).limit(1).to_sql()
        else:
            original_sql = Find(self.table, self.query, projection).limit(1).to_sql()
        
        update_sql = Update(self.table, self.query, self.replacement).to_sql()
        
        if return_new:
            sql = update_sql + ';' + original_sql
        else:
            sql = original_sql + ';' + update_sql
        #upsert暂不支持,可用子查询实现，待完成。XXXAndxxx之类的类也可以实现
        return sql

class FindoneAndUpdate(object, ExtractSql):
    def __init__(self, table, query, update, options):
        self.table = table
        self.query = query
        self.update = update
        self.options = options

    def to_sql(self):
        if self.option.get('sort'):
            sub_selection = Find(self.table, self.query, self.options.get('projection', None)).sort(self.options.get('sort', None)).limit(1).to_sql()
        else:
            sub_selection = Find(self.table, self.query, self.options.get('projection', None)).limit(1).to_sql()
        update_sql = Update(self.table, {'exists': (sub_selection,)}, self.update)
        if self.options.get('returnNewDocument'):
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
        raise ValueError('uncompleted')


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
        sub_selection = Find(self.table, self.query).limit(1).to_sql()
        return Update(self.table, {'exists': (sub_selection,)}, self.rep, self.option)


class RenameCollection(object, ExtractSql):
    def __init__(self, table, new_name, option=None):
        self.table = table
        self.new_name = self.new_name
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


class totalSize(object, ExtractSql):
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
    db = Db('test')
    #for result in db.pet.find().sort({"name": 1}).limit(10).skip(1):
        #print result
    obj_list = []
    obj_list.append(db.pet.find().sort({"name": 1}).limit(8).skip(1))
    #obj_list.append(db.pet.find().sort({"name": 1}).limit(9).skip(1).limit(8))  # 多个limit or skip 
    obj_list.append(db.pet.remove({"name": "Slim"}))
    obj_list.append(db.pet.update({},{"$set": {"name": 'dd', 'age': 14}}))
    #obj_list.append(db.pet.update({}, {}))
    obj_list.append(db.pet.save({"name": 1, "age": 28, "_id": 100}))

    obj_list.append(db.pet.save({"name": 1, "age": 28}))
    obj_list.append(db.pet.find({"age": {"$in": [12, 25, 29]}}))
    obj_list.append(db.orders.aggregate([{"$group": {"_id": "null", "count": {"$sum": 1 }}},{'$sort': {'name':1, 'age': -1}}]))
    obj_list.append(db.pet.aggregate([{'$match': {'name': 'dd'}},{'$limit': 1}, {'$skip': 6}]))
    obj_list.append(db.pet.aggregate([{'$match': {'name': 'dd'}}, {'$project': {'name':{'lastname':1}, 'id': 0, 'myarray': ['$key', '$val']}}]))
    obj_list.append(db.orders.aggregate( [{'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}]))
    obj_list.append(db.articles.aggregate([{'$match': {'$or':[{'score':{'$gt': 70, '$lt': 90}}, {'view': {'$gte': 1000}}]}}, {'$group': {'_id': 'null', 'count': {'$sum': 1}}}]))
    obj_list.append(db.artcles.aggregate([{'$match': {'status':'A'}},{"$project": {'name': 'true'}}, {'$group': {'_id': {"cust_id": "$cust_id", 'order_data': {'month': 12} }, 'total': {'$sum': 1}}},{'$match': {'total': {'$gt': 250}}}]))
    obj_list.append(db.artcles.aggregate([{'$match': {'status':'A'}},{'$group': {'_id': {"cust_id": "$cust_id", 'order_data': {'month': 12} }, 'total': {'$sum': 1}}},{'$match': {'total': {'$gt': 250}}}]))
    obj_list.append( db.orders.aggregate( [{'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}, {'$group':{'_id': 'null', 'count': {'$sum': 1}}}]))


    obj_list.append(db.test.count({'name': 11}))
    obj_list.append(db.test.createIndex({'age': 1, 'owner': -1}))
    
    #obj_list.append(db.test.findAndModify({'query': {'name': 'andy'}, 'sort': {'rating': 1}, 'update': {'$inc': {'score': 1}}, 'upsert': 'true'})) # find找到的记录如何返回，以便modify修改，并且只对一个记录进行增、删、改的sql语句的写法。



    for obj in obj_list:
        print obj.to_sql()
    
