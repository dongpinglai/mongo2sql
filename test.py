#!/usr/bin/env python
# -*- encoding: utf-8 -*-
###TODO: 增加db.coll.bulkwrite() 测试

import random
import mongo2sql


#参数列表
operations = ['insertOne', 'updateOne', 'updateMany', 'deleteOne', 'deleteMany', 'replaceOne']

CONDITIONS = [{}, {"name": 1, "age": 28}, {"name": "Slim"}, {"name": 1}, {"name": 'dd', 'age': 14}, {'total': {'$gt': 250}}]

UPDATES = [{"$set": {"name": 'dd', 'age': 14}}]

STAGES = [{"$group": {"_id": "null", "count": {"$sum": 1 }}}, {'$sort': {'name':1, 'age': -1}}, {'$match': {'name': 'dd'}},{'$limit': 1}, {'$skip': 6},  {'$project': {'name':{'lastname':1}, 'id': 0, 'myarray': ['$key', '$val']}}, {'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}, {'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}, {'$group':{'_id': 'null', 'count': {'$sum': 1}}}, {'$match': {'status':'A'}},{"$project": {'name': 'true'}}, {'$group': {'_id': {"cust_id": "$cust_id",'order_data': {'month': 12} }, 'total': {'$sum': 1}}},{'$match': {'total': {'$gt': 250}}}]

SORTS = [{"name": 1}, {'age': 1, 'owner': -1}]

PROJECTIONS = [{"name": 1},  {'name': 0, '_id': 1}]

ORDERS = [{'age': 1, 'owner': -1}, {'name': 1}]

COUNTS = [x for x in range(100)]

INDEX_NAMES = ['INDEX_NAME_%d' % i for i in range(10)]

DATABASE_METHODS = ["createDatabase", "dropDatabase", 'cloneCollection', 'cloneDatabase', 'commandHelp', 'copyDatabase', 'createCollection', 'currentOp', 'eval', 'fsyncLock', 'fsyncUnlock', 'getCollection', 'getCollectionInfos', 'getCollectionNames', 'getLastError', 'getLastErrorObj', 'getLogComponents', 'getMongo', 'getName', 'getPrevError', 'getProfilingLevel', 'getProfilingStatus', 'getReplicationInfo', 'getSiblingDB', 'help', 'hostInfo', 'isMaster', 'killOp', 'listCommands', 'loadServerScripts', 'logout', 'printCollectionStatus', 'printReplicationInfo', 'printShardingStatus', 'printSlaveReplicationInfo', 'repairDatabase', 'resetError', 'runCommand', 'serverBuildInfo', 'serverCmdLineOpts', 'serverStatus', 'setLogLevel', 'setProfilingLevel', 'shutdownServer', 'stats', 'version', 'upgradeCheck', 'upgradeCheckAllDBs']


COLLECTION_METHODS = ['aggregate', 'bulkWrite', 'count', 'copyTo', 'createIndex', 'dataSize', 'deleteOne', 'deleteMany', 'distinct', 'drop', 'dropIndex', 'dropIndexes', 'ensureIndex', 'explain', 'find', 'findAndModify', 'findOne', 'findOneAndDelete', 'findOneAndReplace', 'findOneAndUpdate', 'getIndexes', 'getShardDistribution', 'getShardVersion', 'group', 'insert', 'insertOne', 'insertMany', 'isCapped', 'mapReduce', 'reIndex', 'replaceOne', 'remove', 'renameCollection', 'save', 'stats', 'storageSize', 'totalSize', 'totalIndexSize', 'update', 'updateOne', 'updateMany', 'validate']

INSERTS = [{"name": 1}, {'age': 1, 'owner': -1}, {'name': 'dd'}]

from_list = ['from_host1', 'from_host2', 'from_host3']
to_list = ['to_1_table_or_database', 'to2_table_or_database', 'to3_table_or_database', 'to4_database_or_table']
cmds = ['cmd1', 'cmd2', 'cmd3']
names = ['test_%d' % i for i in range(9)] 

#随机传入参数
insert_val = random.choice(INSERTS)
insert_vals = random.sample(INSERTS, random.randint(2, 3))
operation = random.choice(operations)
condition = random.choice(CONDITIONS)
update = random.choice(UPDATES)
stage = random.sample(STAGES, random.randint(1, 3))
sort_val = random.choice(SORTS)
order = random.choice(ORDERS)
projection = random.choice(PROJECTIONS)
count = random.choice(COUNTS)
frm = random.choice(from_list)
to = random.choice(to_list)
cmd = random.choice(cmds)
index_name = random.choice(INDEX_NAMES)
db_name = random.choice(names)
tb_name = random.choice(names)
db = mongo2sql.Db(db_name)


class Test(object):
    def test_database_methods(self):
        for i in range(len(DATABASE_METHODS)):
            if DATABASE_METHODS[i] == 'cloneCollection':
                print "cloneCollection =>", getattr(db, DATABASE_METHODS[i])(frm, to, condition).to_sql()
            elif DATABASE_METHODS[i] == 'cloneDatabase':
                #getattr(db, DATABASE_METHODS[i])(frm).to_sql()
                pass
            elif DATABASE_METHODS[i] == 'commandHelp':
                #getattr(db, DATABASE_METHODS[i])(random.choice(cmds)).to_sql()
                pass
            elif DATABASE_METHODS[i] == 'copyDatabase':
                #getattr(db, DATABASE_METHODS[i])(frm, to).to_sql()
                pass
            elif DATABASE_METHODS[i] == 'createCollection':
                #getattr(db, DATABASE_METHODS[i])('test_coll').to_sql()
                pass
            elif DATABASE_METHODS[i] == 'currentOp':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'dropDatabase':
                print 'dropDatabase =>', getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'createDatabase':
                print 'createDatabase =>', getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'eval':
                #getattr(db, DATABASE_METHODS[i])('func').to_sql()
                pass
            elif DATABASE_METHODS[i] == 'fsyncLock':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'fsyncUnlock':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getCollection':
                print 'getCollection =>', getattr(db, DATABASE_METHODS[i])(tb_name).to_sql()
            elif DATABASE_METHODS[i] == 'getCollectionInfos':
                print 'getCollectionInfos =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getCollectionNames':
                print 'getCollectionNames =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getLastError':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getLastErrorObj':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getLogComponents':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getMongo':
                print 'getMongo => ', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getName':
                print 'getName =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getPrevError':
                print 'getPrevError =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getProfilingLevel':
                print 'getProfilingLevel =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getProfilingStatus':
                print 'getProfilingStatus =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getReplicationInfo':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getSiblingDB':
                print 'getSiblingDB =>', getattr(db, DATABASE_METHODS[i])(db_name).to_sql()
            elif DATABASE_METHODS[i] == 'help':
                print 'help =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'hostInfo':
                print 'hostInfo =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'isMaster':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'killOp':
                print 'killOp =>', getattr(db, DATABASE_METHODS[i])(count).to_sql()
            elif DATABASE_METHODS[i] == 'listCommands':
                print 'listComands =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'loadServerScripts':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'logout':
                print 'logout =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printCollectionStatus':
                print 'print CollectionStatus =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printReplicationInfo':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'printShardingStatus':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'printSlaveReplicationInfo':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'repairDatabase':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'resetError':
                print 'resetError =>', getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'runCommand':
                #getattr(db, DATABASE_METHODS[i])('').to_sql()
                pass
            elif DATABASE_METHODS[i] == 'serverBuildInfo':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'serverCmdLineOpts':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'serverStatus':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'setLogLevel':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'setProfilingLevel':
                getattr(db, DATABASE_METHODS[i])(count).to_sql()
                
            elif DATABASE_METHODS[i] == 'shutdownServer':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'stats':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'version':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'upgradeCheck':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'upgradeCheckAllDBs':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            else:
                print 'DATABASE_METHOD "%s" not in...' % DATABASE_METHODS[i]


    def test_collection_methods(self):
        for i in range(len(COLLECTION_METHODS)):
            if COLLECTION_METHODS[i] == 'aggregate':
                print 'aggregate =>', getattr(db.test, COLLECTION_METHODS[i])(stage).to_sql()
            elif COLLECTION_METHODS[i] == 'bulkWrite':
                #getattr(db.test, COLLECTION_METHODS[i])(random.sample(operations, random.randint(1, 6))).to_sql()
                pass
            elif COLLECTION_METHODS[i] == 'count':
                print 'count =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'copyTo':
                print 'copyTo =>', getattr(db.test, COLLECTION_METHODS[i])(to).to_sql()
            elif COLLECTION_METHODS[i] == 'createIndex':
                print 'createIndex =>', getattr(db.test, COLLECTION_METHODS[i])(sort_val).to_sql()
            elif COLLECTION_METHODS[i] == 'dataSize':
                print 'dataSize =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'deleteOne':
                print 'deleteOne =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'deleteMany':
                print 'deleteMany =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'distinct':
                print 'distinct =>', getattr(db.test, COLLECTION_METHODS[i])(projection, condition).to_sql()
            elif COLLECTION_METHODS[i] == 'drop':
                print 'drop =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'dropIndex':
                print 'dropIndex =>', getattr(db.test, COLLECTION_METHODS[i])(index_name).to_sql()
            elif COLLECTION_METHODS[i] == 'dropIndexes':
                print 'dropIndexes => ', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'ensureIndex':
                print 'ensureIndex =>', getattr(db.test, COLLECTION_METHODS[i])(sort_val).to_sql()
            elif COLLECTION_METHODS[i] == 'explain':
                #getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                pass
            elif COLLECTION_METHODS[i] == 'find':
                print 'find =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).to_sql()
                #print 'find =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).limit(count).to_sql()
                print 'find =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).sort(sort_val).to_sql()
                #print 'find =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).skip(count).to_sql()
                print 'find =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).sort(sort_val).limit(count).skip(count).to_sql()
            elif COLLECTION_METHODS[i] == 'findAndModify':
                print 'findAndModify =>', getattr(db.test, COLLECTION_METHODS[i])({'query': condition, 'update': update}).to_sql()
            elif COLLECTION_METHODS[i] == 'findOne':
                print 'findOne =>', getattr(db.test, COLLECTION_METHODS[i])(condition, projection).to_sql()
            elif COLLECTION_METHODS[i] == 'findOneAndDelete':
                print 'findOneAndDelete =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'findOneAndReplace':
                print 'findOneAndReplace =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'findOneAndUpdate':
                print 'findOneAndUpdate =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'getIndexes':
                print 'getIndexes =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'getShardDistribution':
                #getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                pass
            elif COLLECTION_METHODS[i] == 'getShardVersion':
                #getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                pass
            elif COLLECTION_METHODS[i] == 'group':
                #getattr(db.test, COLLECTION_METHODS[i])(self.sort_val,)
                pass
            elif COLLECTION_METHODS[i] == 'insert':
                print 'insert =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'insertOne':
                print 'insertOne =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'insertMany':
                print 'insertMany =>', getattr(db.test, COLLECTION_METHODS[i])(update).to_sql()
            elif COLLECTION_METHODS[i] == 'isCapped':
                print 'isCapped =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'mapReduce':
                #getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                pass
            elif COLLECTION_METHODS[i] == 'reIndex':
                print 'reIndex =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'replaceOne':
                print 'replaceONe =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'remove':
                print 'remove =>', getattr(db.test, COLLECTION_METHODS[i])(condition).to_sql()
            elif COLLECTION_METHODS[i] == 'renameCollection':
                print 'renameCollection =>', getattr(db.test, COLLECTION_METHODS[i])('new_test_name').to_sql()
            elif COLLECTION_METHODS[i] == 'save':
                print 'save =>', getattr(db.test, COLLECTION_METHODS[i])(update).to_sql()
            elif COLLECTION_METHODS[i] == 'stats':
                print 'stats =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'storageSize':
                print 'storageSize =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'totalSize':
                print 'totalSize =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'totalIndexSize':
                print 'totalIndexSize =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            elif COLLECTION_METHODS[i] == 'update':
                print 'update =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'updateOne':
                print 'updateOne =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'updateMany':
                print 'updateMany =>', getattr(db.test, COLLECTION_METHODS[i])(condition, update).to_sql()
            elif COLLECTION_METHODS[i] == 'validate':
                print 'validate =>', getattr(db.test, COLLECTION_METHODS[i])().to_sql()
            else:
                print 'COLLECTION_METHOD "%s" not in' % COLLECTION_METHODS[i]
        
    def main(self):
        try:
            print 'mongo2sql database_methods:'.center(88, '-')
            self.test_database_methods()
            print 'mongo2sql collection_methods:'.center(88, '-')
            self.test_collection_methods()
        except ValueError, e:
            print e
        finally:
            from imp import reload
            reload(mongo2sql)
        



                    
                    
if __name__ == '__main__':
    test = Test()
    test.main()

         
         
    

         
        
