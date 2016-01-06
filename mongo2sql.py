#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import threading
import MySQLdb


#class Switch(object):
 #   def __enter__(self):
  #      return self

   # def __exit__(self, type, val, trace):
    #    del self

L_SWITCH = 1
S_SWITCH = 1

        
configure = {"hostname": "localhost", "username": "root", "password": "lai", "database": "test" }    

def connect(hostname, username, password, database, port=3306):
    conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=port)
    cur = conn.cursor()
    return cur

cursor = connect(**configure)

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
        

class Db(dict, ExtractSql):

    """Docstring for Db. """

    def __init__(self, name):
        self.name = name
    def createDatabase(self):
        return "CREATE DATABSE {}".format(self.name)
        
    def dropDatabase(self):
        return "DROP DATABSE {}".format(self.name)
        
    def __getattr__(self, attr):
        if attr not in ["createDatabase", "dropDatabase"]:
            return Table(self, attr)

            
    
class Table(object, ExtractSql):
    def __init__(self, db, name):
        self.db = db
        self.name = name

    def find(self, condition=None, field=None):
        return Find(self, condition, field)

    def insert(self, doc):
        return Insert(self, doc)

    def remove(self, condition=None):
        return Remove(self, condition)

    def update(self, condition, operation):
        return Update(self, condition, operation)

    def save(self, doc, write_concern=None):
        return Save(self, doc, write_concern)

    def __getattr__(self, attr):
        pass

    def aggregate(self, stages, option=None):
        return Aggregate(self, stages, option)

    def count(self, query=None):
        return Count(self, query)

    def createIndex(self,keys, options=None):
        return CreateIndex(self, keys, options)

    def dropIndex(self,index):
        return DropIndex(self, index)

    def findAndModify(self,doc):
        return FindAndModify(self, doc)

    def deleteOne(self, query):
        return DeleteOne(self, query)

    def dropCollection(self):
        return DropColletion(self)

    def distinct(self, field, query):
        return Distinct(self, field, query)

    def findOne(self, query, projection):
        return FindOne(self, query, projection)


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
    def __init__(self, table, doc):
        self.table = table
        self.doc = doc

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
    def __init__(self, table, condition=None):
        self.table = table
        self.condition = condition

    def to_sql(self):
        condition = handle_condition(self.condition)
        if condition == '':
            return "Drop table %s" % self.table.name
        return "DELETE FROM %s WHERE %s" % (self.table.name, condition)
        

class Update(object, ExtractSql):
    def __init__(self, table, condition, operation, option=None):
        self.table = table
        self.condition = condition
        self.operation = operation
        self.option = option

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


class FindAndModify(Find, Update, Remove):
    def __init__(self, table, doc):
        self.table = table
        self.doc = doc
        

    def to_sql(self):
        fields_value = self.doc.get('fields', None)
        query_value = self.doc.get('query', None)
        update_value = self.doc.get('update', None)
        remove_value = self.doc.get('remove', False)
        sort_value = self.doc.get('sort', None)
        obj = Find(self.table, query_value, fields_value).limit(1)
        if sort_value:
            obj = Find(self.table, query_value, fields_value).sort(sort_value).limit(1)
            pass

        if update_value:
            obj = Update(self.table, query_value, update_value)
        elif remove_value:
            obj = Find(self.table, query_value, fields_value).sort(sort_value).limit(1)
            pass
        return obj.to_sql()

class DeleteOne(Remove, ExtractSql):
    def __init__(self, table, query):
        self.table = table
        self.query = query
        super(DeleteOne, self).__init__(self.table, self.query)
        
    def to_sql(self):
        sql = super(DeleteOne, self).to_sql()
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
        return super(FindOne, self).limit(1).to_sql()


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
    
