#!/usr/bin/env python
# -*- encoding: utf-8 -*-


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
        operation_fmt = ''
        for op, val in self.operation.items():
            if op == '$set':
                operation_fmt = 'set {0}={1}'.format(val.keys()[0], val.values()[0])
            elif op == '$inc':
                operation_fmt = 'set {0}={0}+{1}'.format(val.keys()[0], val.values()[0])
            elif op == '$unset':
                operation_fmt = 'alter table {0} drop {1}'.format(self.table.name, val.keys()[0])
            elif op == '$push':
                operation_fmt = 'alter table {0} add {1}, set {2}={3}'.format(self.table.name, val.keys()[0], val.keys()[0], val.values()[0])
            else:
                operation_fmt = 'set {0}={1}'.format(op, val)
            operation_fmt_list.append(operation_fmt)
        if len(operation_fmt_list) == 1:
            return operation_fmt_list[0]
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
    

if __name__ == "__main__":
    db = Db('test')
    #for result in db.pet.find().sort({"name": 1}).limit(10).skip(1):
        #print result
    obj_list = []
    obj_list.append(db.pet.find().sort({"name": 1}).limit(8).skip(1))
    #obj_list.append(db.pet.find().sort({"name": 1}).limit(9).skip(1).limit(8))  # 多个limit or skip 
    obj_list.append(db.pet.remove({"name": "Slim"}))
    obj_list.append(db.pet.update({},{"$set": {"name": 'dd'}}))
    #obj_list.append(db.pet.update({}, {}))
    obj_list.append(db.pet.save({"name": 1, "age": 28, "_id": 100}))

    obj_list.append(db.pet.save({"name": 1, "age": 28}))
    
    for obj in obj_list:
        print obj.to_sql()
    
