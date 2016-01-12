#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import random
import mongo2sql

operations = ['insertOne', 'updateOne', 'updateMany', 'deleteOne', 'deleteMany', 'replaceOne']

CONDITIONS = [{}, {"name": 1, "age": 28}, {"name": "Slim"}, {"name": 1}]

UPDATES = [{}, {"$set": {"name": 'dd', 'age': 14}}]

STAGES = [{"$group": {"_id": "null", "count": {"$sum": 1 }}}, {'$sort': {'name':1, 'age': -1}}, {'$match': {'name': 'dd'}},{'$limit': 1}, {'$skip': 6},  {'$project': {'name':{'lastname':1}, 'id': 0, 'myarray': ['$key', '$val']}}, {'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}, {'$group': {'_id': {'cust_id': "$cust_id",'ord_date': {'month': { '$month': "$ord_date" },'day': { '$dayOfMonth':"$ord_date" },'year': { '$year': "$ord_date"}}}}}, {'$group':{'_id': 'null', 'count': {'$sum': 1}}}, {'$match': {'status':'A'}},{"$project": {'name': 'true'}}, {'$group': {'_id': {"cust_id": "$cust_id",'order_data': {'month': 12} }, 'total': {'$sum': 1}}},{'$match': {'total': {'$gt': 250}}}]

SORTS = [{"name": 1}, {'age': 1, 'owner': -1}]

PROJECTIONS = [{"name": 1}]

ORDERS = [{'age': 1, 'owner': -1}]

COUNTS = [x for x in range(100)]

DATABASE_METHODS = ["createDatabase", "dropDatabase", 'cloneCollection', 'cloneDatabase', 'commandHelp', 'copyDatabase', 'createCollection', 'currentOp', 'eval', 'fsyncLock', 'fsyncUnlock', 'getCollection', 'getCollectionInfos', 'getCollectionNames', 'getLastError', 'getLastErrorObj', 'getLogComponents', 'getMongo', 'getName', 'getPrevError', 'getProfilingLevel', 'getProfilingStatus', 'getReplicationInfo', 'getSiblingDB', 'help', 'hostInfo', 'isMaster', 'killOp', 'listCommands', 'loadServerScripts', 'logout', 'printCollectionStatus', 'printReplicationInfo', 'printShardingStatus', 'printSlaveReplicationInfo', 'repairDatabase', 'resetError', 'runCommand', 'serverBuildInfo', 'serverCmdLineOpts', 'serverStatus', 'setLogLevel', 'setProfilingLevel', 'shutdownServer', 'stats', 'version', 'upgradeCheck', 'upgradeCheckAllDBs']


COLLECTION_METHODS = ['aggregate', 'bulkWrite', 'count', 'copyTo', 'createIndex', 'dataSize', 'deleteOne', 'deleteMany', 'distinct', 'drop', 'dropIndex', 'dropIndexes', 'ensureIndex', 'explain', 'find', 'findAndModify', 'findOne', 'findOneAndDelete', 'findOneAndReplace', 'findOneAndUpdate', 'getIndexes', 'getShardDistribution', 'getShardVersion', 'group', 'insert', 'insertOne', 'insertMany', 'isCapped', 'mapReduce', 'reIndex', 'replaceOne', 'remove', 'renameCollection', 'save', 'stats', 'storageSize', 'totalSize', 'totalIndexSize', 'update', 'updateOne' 'updateMany', 'validate']


from_list = ['from_host1', 'from_host2', 'from_host3']
to_list = ['to_1', 'to2', 'to3', 'to4']
cmds = ['cmd1', 'cmd2', 'cmd3']

db = mongo2sql.Db('test_d')


class Test(object):
    def __init__(self):
        #self.db_method = random.choice(DATABASE_METHODS)
        #self.coll_method = random.choice(COLLECTION_METHODS)
        self.count = random.choice(COUNTS)
        self.update = random.choice(UPDATES)
        self.condition = random.choice(CONDITIONS)
        self.sort_val = random.choice(SORTS)
        self.stage_sample = random.sample(STAGES, random.randint(1, 3))
        self.projection = random.choice(PROJECTIONS)
        self.order = random.choice(ORDERS)
        self.frm = random.choice(from_list)
        self.to = random.choice(to_list)

    def test_database_methods(self):
        
        for i in range(len(DATABASE_METHODS)):
            if DATABASE_METHODS[i] == 'cloneCollection':
                getattr(db, DATABASE_METHODS[i])(self.frm, self.to, self.condition).to_sql()
                
            elif DATABASE_METHODS[i] == 'cloneDatabase':
                #getattr(db, DATABASE_METHODS[i])(self.frm).to_sql()
                pass
            elif DATABASE_METHODS[i] == 'commandHelp':
                #getattr(db, DATABASE_METHODS[i])(random.choice(cmds)).to_sql()
                pass
            elif DATABASE_METHODS[i] == 'copyDatabase':
                #getattr(db, DATABASE_METHODS[i])('frm_db', 'to_db').to_sql()
                pass
            elif DATABASE_METHODS[i] == 'createCollection':
                #getattr(db, DATABASE_METHODS[i])('test_coll').to_sql()
                pass
            elif DATABASE_METHODS[i] == 'currentOp':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'dropDatabase':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
            elif DATABASE_METHODS[i] == 'createDatabase':
                getattr(db, DATABASE_METHODS[i])().to_sql()
                
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
                getattr(db, DATABASE_METHODS[i])('test_coll').to_sql()
            elif DATABASE_METHODS[i] == 'getCollectionInfos':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getCollectionNames':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getLastError':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getLastErrorObj':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getLogComponents':
                #getattr(db, DATABASE_METHODS[i])().to_sql()
                pass
            elif DATABASE_METHODS[i] == 'getMongo()':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getName':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getPrevError':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getProfilingLevel':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getProfilingStatus':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getReplicationInfo':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'getSiblingDB':
                getattr(db, DATABASE_METHODS[i])('test_db').to_sql()
            elif DATABASE_METHODS[i] == 'help':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'hostInfo':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'isMaster':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'killOp':
                getattr(db, DATABASE_METHODS[i])('opid_t001').to_sql()
            elif DATABASE_METHODS[i] == 'listCommands':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'loadServerScripts':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'logout':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printCollectionStats':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printReplicationInfo':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printShardingStatus':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'printSlaveReplicationInfo':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'repairDatabase':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'resetError':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'runCommand':
                getattr(db, DATABASE_METHODS[i])('').to_sql()
            elif DATABASE_METHODS[i] == 'serverBuidInfo':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'serverCmdLineOpts':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'serverStatus':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'setLogLevel':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'setProfilingLevel':
                getattr(db.DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'shutdownServer':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'stats':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'version':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'upgradeCheck':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            elif DATABASE_METHODS[i] == 'upgradeCheckAllDBs':
                getattr(db, DATABASE_METHODS[i])().to_sql()
            else:
                print 'DATABASE_METHOD "%s" not in...' % DATABASE_METHODS[i]


        def test_collection_methods(self):
            for i in range(len(COLLECTION_METHODS)):
                if COLLECTION_METHODS[i] == 'aggregate':
                    getattr(db.test, COLLECTION_METHODS[i])(self.stage_sample)
                elif COLLECTION_METHODS[i] == 'bulkWrite':
                    getattr(db.test, COLLECTION_METHODS[i])(random.sample(operations, randint(1, 6))).to_sql()
                elif COLLECTION_METHODS[i] == 'count':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'copyTo':
                    getattr(db.test, COLLECTION_METHODS[i])(self.to).to_sql()
                elif COLLECTION_METHODS[i] == 'createIndex':
                    getattr(db.test, COLLECTION_METHODS[i])(self.sort_val).to_sql()
                elif COLLECTION_METHODS[i] == 'dataSize':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'deleteOne':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).tosql()
                elif COLLECTION_METHODS[i] == 'deleteMany':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'distinct':
                    getattr(db.test, COLLECTION_METHODS[i])(self.projection, self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'drop':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'dropIndex':
                    getattr(db.test, COLLECTION_METHODS[i])('drop_index_name').to_sql()
                elif COLLECTION_METHODS[i] == 'dropIndexes':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'ensureIndex':
                    getattr(db.test, COLLECTION_METHODS[i])(self.sort_val).to_sql()
                elif COLLECTION_METHODS[i] == 'explain':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'find':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.projection)
                elif COLLECTION_METHODS[i] == 'findAndModify':
                    getattr(db.test, COLLECTION_METHODS[i])({'query': self.condition, 'update': self.update}).to_sql()
                elif COLLECTION_METHODS[i] == 'findOne':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.projection).to_sql()
                elif COLLECTION_METHODS[i] == 'findOneAndDelete':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'findOneAndReplace':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'findOneAndUpdate':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'getIndexes':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'getShardDistribution':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'getShardVersion':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'group':
                    #getattr(db.test, COLLECTION_METHODS[i])(self.sort_val,
                    pass
                elif COLLECTION_METHODS[i] == 'insert':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'insertOne':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'insertMany':
                    getattr(db.test, COLLECTION_METHODS[i])(random.sample(CONDITIONS, (1, 3))).to_sql()
                elif COLLECTION_METHODS[i] == 'isCapped':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'mapReduce':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'reIndex':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'replaceOne':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'remove':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition).to_sql()
                elif COLLECTION_METHODS[i] == 'renameCollection':
                    getattr(db.test, COLLECTION_METHODS[i])('new_test_name').to_sql()
                elif COLLECTION_METHODS[i] == 'save':
                    getattr(db.test, COLLECTION_METHODS[i])(self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'stats':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'storageSize':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'totalSize':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'totalIndexSize':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                elif COLLECTION_METHODS[i] == 'update':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'updateOne':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'updateMany':
                    getattr(db.test, COLLECTION_METHODS[i])(self.condition, self.update).to_sql()
                elif COLLECTION_METHODS[i] == 'validate':
                    getattr(db.test, COLLECTION_METHODS[i])().to_sql()
                else:
                    print 'COLLECTION_METHOD "%s" not in' % COLLECTION_METHODS[i]
        
    def main(self):
        try:
            self.test_database_methods()
            self.test_collection_methods()
        except ValueError, e:
            print e
        finally:
            from imp import reload
            reload(mongo2sql)
        



                    
                    
if __name__ == '__main__':
    test = Test()
    test.main()

         
         
    

         
        
