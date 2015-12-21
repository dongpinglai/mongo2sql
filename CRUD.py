#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import MySQLdb

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

    def save(self, doc, write_concern):
        return Save(self, doc, write_concern)

    def __getattr__(self, attr):
        pass

        
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
                        d_fmt.append('{0} IN ({1})'.format(field, str(tuple(v))))
                    

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
        

class Save(object, ExtractSql, Insert):
    def __init__(self, table, doc, write_concern):
        self.table = table
        self.doc = doc
        self.write_concern = write_concern

    def to_sql(self):
        if '_id' in self.doc.keys():
            return Update(self.table, {"_id": self.doc["_id"]}, self.doc).to_sql()
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
        operation_fmt = ''
        for op, obj in self.operation.items():
            field = obj.keys()[0]
            value = obj.values()[0]                
            if op == '$set':
                operation_fmt = 'set {0}={1}'.format(field, value)
            elif op == '$inc':
                operation_fmt = 'set {0}={0}+{1}'.format(field, value)
            elif op == '$unset':
                operation_fmt = 'alter table {0} drop {1}'.format(self.table.name, field)
            elif op == '$push':
                operation_fmt = 'alter table {0} add {1}, set {2}={3}'.format(self.table.name, field, field, value)
            else:
                operation_fmt = 'set {0}={1}'.format(field, value)
            operation_fmt_list.append(operation_fmt)
        if len(operation_fmt_list) == 1:
            return operation_fmt_list[0]
        return ','.join(operation_fmt_list)
            
    def to_sql(self):
        condition = handle_condition(self.condition)
        operation  = self.handle_operation()
        if operation == "":
            raise ValueError('The secend parameter can\'t be \'{}\'')
        return "UPDATE %s %s WHERE %s" % (self.table.name, operation, condition)
        
class Find(object, ExtractSql):
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
        return Limit(self, count)
        
    def skip(self, count):
        return Skip(self, count)

    def sort(self, condition):
        return Sort(self, condition)

    def __getattr__(self, attr):
        pass 
        
class Limit(object, ExtractSql):
    def __init__(self, obj, count):
        self.obj = obj
        self.count = count

    def skip(self, count):
        return Skip(self, count)

    def to_sql(self):
        return "%s LIMIT %d" % (self.obj.to_sql(), self.count)

class Skip(object, ExtractSql):
    def __init__(self, obj, count):
        self.obj = obj
        self.count = count

    def limit(self, count):
        return Limit(self, count)

    def to_sql(self):
        return "%s OFFSET %d " % (self.obj.to_sql(), self.count)
    

class Sort(object, ExtractSql):
    def __init__(self, obj, condition):
        self.obj = obj
        self.condition = condition
    def limit(self, count):
        return Limit(self, count)

    def skip(self, count):
        return Skip(self, count)
        
    def to_sql(self):
        sort_fmt_list = []
        for key, val in self.condition.items():
            if val == 1:
                sort_fmt_list.append('{} ASC'.format(key))
            else:
                sort_fmt_list.append('{} DESC'.format(key))
        sort_fmt = ','.join(sort_fmt_list)
        return "%s ORDER BY %s" % (self.obj.to_sql(), sort_fmt)
    

if __name__ == "__main__":
    db = Db('test')
    for result in db.pet.find().sort({"name": 1}).limit(10).skip(1):
        print result
    
    count = db.pet.remove({"name": "Slim"}).execute()
    print count
