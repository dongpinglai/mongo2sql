#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#created by: laidongping2006@sina.com 

from collections import defaultdict
import re
import sys
from threading import Thread
from Queue import Queue




class Transformer(object):
    
    def __init__(self, m_statement):
        print m_statement        
        try:
            PATTERN = r'^db\.(?P<coll>[^.]+)\.(?P<op>\w+)\((?P<op_args>[^)]*)\)\.?(?P<option_ops>.*)$'
            m = re.match(PATTERN, m_statement)
            print m.groups(0)
        except:
            #exc_type, exc_value, exc_traceback = sys.exc_info()
            #print exc_type, exc_value, exc_traceback
            print 'check your statement, please input statement again...'
            
            print '{:*^88}'.format('end')
            sys.exit(0)

        else:
            
            
            self.db = 'database'
            self.coll = m.group('coll') # collection name string
            self.op = m.group('op') # the first function name string
            self.op_args = m.group('op_args') # the first function args string
            self.option_ops = m.group('option_ops') # other function string
            #print 'self.db: '
            #print self.db
            #print 'self.coll: '
            #print self.coll
            #print 'self.op: '
            #print self.op
            #print 'self.op_args: '
            #print self.op_args
            #print 'self.option_ops: '
            #print self.option_ops
            print '-' * 66
            self.transform()
       
    def transform(self):
        """ selection logic"""
        
        print 'transforming...'
        sql_fmt = '' # sql statement var
        if self.op == 'find':
            print 'it is a find_statement'
            sql_fmt = self.parse_find()
            
        if self.op == 'insert':
            print 'it is a insert_statement'
            sql_fmt = self.parse_insert()
            
        if self.op == 'update':
            print 'it is a update_statement'
            sql_fmt = self.parse_update()
        if self.op == 'remove':
            print 'it is a remove_statement'
            sql_fmt = self.parse_remove()
        if self.op == 'aggregate':
            print 'it is a aggregate_statement'
            sql_fmt = self.parse_aggreaget()
        if self.op == 'createIndex':
            print 'it is a createIndex_statement'
            sql_fmt = self.parse_createIndex()

        print 'transformation complete...'
        
        print sql_fmt
        
        print '{:*^88}'.format('end')


    def handle_string(self, astring):
        """a mongo args string -> a args dict. e.g: '{XXX: XXXX, ....}'->
        {XXX: XXXX, ....}
        """
        string_dict = {}
        if astring == '':
            return string_dict
        # key: value, e.g: value:value or value:exp ...
        STRING_PATTERN = r'\s*(?P<key>[$\w\'\"]*?)\s*:\s*(?P<value>[^{}\[\],]+|\[.*?\]|\{.*?\}),?\s*'
        
        string_m_iter = re.finditer(STRING_PATTERN, astring)
        print string_m_iter
        for str_m in string_m_iter:
            key  = str_m.group('key')
            value =  str_m.group('value')
            print '{0}==>{1}'.format(key, value)
      
            if value.startswith('['):
                value_list= []
                #VALUE_LIST_PATTERN = r'\s*([^{}\[\],]+|\[.*\]|\{.*\})\s*'
                VALUE_LIST_PATTERN = r'\s*(([$\w\'\"]*?)\s*:\s*([^{}\[\],]+|\{.*?\}|\[.*?\])),?\s*'
                value_m_iter = re.finditer(VALUE_LIST_PATTERN, value)
                for v_m in value_m_iter:
                    print 'v_m.group(1): '
                    print v_m.group(1)
                    value_list.append(self.handle_string(v_m.group(1)))
                    string_dict[key] = value_list
            elif value.startswith('{'):
                value_dict = self.handle_string(value)
                string_dict[key] = value_dict
            else:
                string_dict[key] = value
        return string_dict

    
    def handle_dict(self, adict):
        """a args dict -> a sql format string """
        fmt = []
        if adict == {}:
                return ''
        for field, value in adict.items():
            if isinstance(value, list):
                l_fmt = []
                for v in value:
                    l_fmt.append(self.handle_dict(v))
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
                    #add other operators' formats behind here
                    else:
                        d_fmt.append(self.handle_dict(v))
                d_fmt = ' and '.join(d_fmt)
                fmt.append(d_fmt)
            else:
                fmt.append('{0}={1}'.format(field, value))
            

        if len(adict) == 1:
            fmt = fmt[0]
            return fmt
        fmt = ' and '.join(fmt)
        return fmt
        

        
    def parse_find(self):
        """db.coll.find(...)->select ...from ...where...[order by|limit|skp]..."""

        #inner functions
        def handle_find_criteria(criteria_string):
            crite_fmt = []
            criteria_dict = self.handle_string(criteria_string)
            print 'find_critedia_dcit: '
            print criteria_dict
            crite_fmt = self.handle_dict(criteria_dict)
            print 'find_criteria_fmt: '
            print crite_fmt
            return crite_fmt

        
        def handle_projection(projection_string):
            proj_fmt = ''
            projection_dict = {}
            if projection_string == '':
                return proj_fmt
            PROJECTION_PATTERN = r'\s*([^{},]*?)\s*:\s*(\w+)\s*' #starting
            projection_m_iter = re.finditer(PROJECTION_PATTERN, projection_string)
            for  p_m in projection_m_iter:
                p_field = p_m.group(1)
                p_value = p_m.group(2)
                projection_dict[p_field] = p_value
                
            print 'projection_dict: '
            print projection_dict

            proj_fmt = ','.join([key for key in projection_dict.keys() if projection_dict[key] == '1'])
            print 'proj_fmt: '
            print proj_fmt
            return proj_fmt


        def handle_option_ops(opstring):
            """opsting -> option_dict -> option_fmt """
            option_fmt = ''
            sort_fmt = ''
            limit_fmt = ''
            skip_fmt = ''
            if opstring == '':
                return option_fmt
            option_dict = {}           
            OPTION_PATTERN = r'(\w+)\((.*?)\)'
            option_m_iter = re.finditer(OPTION_PATTERN, opstring)
            
            for o_m in option_m_iter:
                option_op_name = o_m.group(1)
                option_op_args = o_m.group(2)
                option_dict[option_op_name] = option_op_args
            print 'option_dict: '
            print option_dict

            for op, op_args in option_dict.items():
                if op == 'sort':
                    sort_fields = []
                    SORT_ARGS_PATTERN = r'\s*(.*?)\s*:\s*(1|0)\s*'
                    sort_args_m_iter = re.finditer(SORT_ARGS_PATTERN, op_args)
                    for sort_args_m in sort_args_m_iter:
                        sort_op_name = sort_args_m.group(1)
                        sort_op_value = sort_args_m.group(2)
                        if sort_op_value == '1':
                            sort_fields.append(str(sort_op_name) + ' asc ')
                           
                        if sort_op_value == '0':
                            sort_fields.append(str(sort_op_name) + ' desc ')
                            
                    for s_f in sort_fields:
                        sort_fmt = sort_fmt + str(s_f)
                        
                    sort_fmt = 'order by ' + sort_fmt
                                        
                if op == 'limit':
                    limit_fmt = 'limit {0}'.format(op_args)
                    
                if op == 'skip':
                    skip_fmt = 'skip {0}'.format(op_args)
                    
            option_fmt = sort_fmt + ' ' + limit_fmt + ' ' + skip_fmt
            print 'option_fmt: '
            print option_fmt
            return option_fmt
        #inner functions ending

        find_fmt = ''
        FIND_ARGS_PATTERN = r'\s*(?P<criteria>[^#]*)\s*#?\s*(?P<projection>.*)\s*$' #check here first, meta character '#'
        find_args_m = re.match(FIND_ARGS_PATTERN, self.op_args)
        criteria = find_args_m.group('criteria') # criteria string
        projection = find_args_m.group('projection') # projection string
        option_ops = self.option_ops
        print 'find_criteria_string: ', criteria
        print 'find_projection_string: ', projection
        print 'find_option_ops_string: ', option_ops
        print '-' * 66
        criteria_fmt = handle_find_criteria(criteria)
        projection_fmt = handle_projection(projection)
        option_ops_fmt = handle_option_ops(option_ops)
        print '-' * 66
        #print 'find_criteria_fmt: ' + criteria_fmt
        #print 'find_projection_fmt: ' + projection_fmt
        #print 'find_option_ops_fmt: ' + option_ops_fmt
        
        if projection_fmt == '':
            if criteria_fmt == '':
                find_fmt = 'select * from {0} {1}'.format(self.coll, option_ops_fmt)
                return find_fmt
            find_fmt = 'select * from {0} where {1} {2}'.format(self.coll, criteria_fmt, option_ops_fmt)
            return find_fmt
            
        else:
            if criteria_fmt == '':
                find_fmt = 'select {0} from {1} {2}'.format(projection_fmt, self.coll, option_ops_fmt)
                return find_fmt

            find_fmt = 'select {0} from {1} where {2} {3}'.format(projection_fmt, self.coll, criteria_fmt, option_ops_fmt)
            return find_fmt
                        

    def parse_insert(self, create = False):
        """db.coll.insert(...)->insert into table_name values ..."""
        
        # inner functions
        def handle_insert_args(astring):
            """a string -> list or dict
            if astring.startswith('['):
                args_list = []
                MUL_PATTERN = r'\s*(\{.*\})'
                m_iter = re.finditer(MUL_PATTERN, astring)
                for m_m in m_iter:
                    m_string = m_m.group(1)
                    args_list.append(handle_insert_args(m_string))
                return args_list
                    
            if astring.startswith('{'):
                arg_dict = {}
                SIN_PATTERN = r'\s*(\w+)\s*:\s*(\w+)\s*'
                s_iter = re.finditer(SIN_PATTERN, astring)
                for s_m in s_iter:
                    field = s_m.group(1)
                    value = s_m.group(2)
                    arg_dict[field] = value
                print 'arg_dict: '
                print arg_dict
                return arg_dict
            """
            insert_args_dict = {}
            if astring.startswith('{'):
                insert_args_dict = self.handle_string(astring)
                print 'insert_args_dict: '
                print insert_args_dict
                return insert_args_dict
            if astring.startswith('['):
                insert_args_dicts = []
                STRING_PATTERN = r'\s*(\{.*?\})\s*'
                string_m_iter = re.finditer(STRING_PATTERN, astring)
                for string_m in string_m_iter:
                    insert_arg = string_m.group(1)
                    insert_args_dicts.append(self.handle_string(insert_arg))
                print insert_args_dicts
                return insert_args_dicts
                
                   
                   

        def args_to_fmt(insert_args):
            """ list or dict -> format string """

            values_list = []
            fields_list = []
            if isinstance(insert_args, list):
                mul_list = []
                for value in insert_args:
                    for k, v in value.items(): 
                        mul_list.append(v)
                        if k not in fields_list:
                            fields_list.append(k)
                    values_list.append(mul_list)
                    return ','.join(fields_list), '({})'.format(','.join(mul_list)) # check here 12/10
            if isinstance(insert_args, dict):
                sin_list = [] 
                for k, v in insert_args.items():
                    sin_list.append(v)
                    fields_list.append(k)
                return ','.join(fields_list), '({})'.format(','.join(sin_list))
            
            
        #inner functions ending
       
            

        INSERT_ARGS_PATTERN = r'^(\[\s*(.*)\s*\]|^\{\s*(.*)\s*\})$'
        insert_args_m = re.match(INSERT_ARGS_PATTERN, self.op_args)
        insert_args = insert_args_m.group(0)
        print 'insert_args_string: ' + insert_args
        if not create :
            pass
        insert_fmt = ''
        fields_fmt ,insert_args_fmt = args_to_fmt(handle_insert_args(insert_args))
        
        print 'insert_args_fmt: '
        print insert_args_fmt
        insert_fmt = 'insert into {0}({1}) values {2}'.format(self.coll, fields_fmt, insert_args_fmt)
        return insert_fmt
        
        
    def parse_update(self):
        """db.coll.update(...)->update table_name ... / alter table table_name ..."""

        # inner functions
        def handle_operations(astring):
            """operations string -> operation format string"""
            OPERATIONS_PATTERN = r'\s*(?P<operation>[$\w]+)\s*:\s*(\{\s*(?P<field>\w+)\s*:\s*(?P<value>\w+)\s*\})\s*'
            operations_m_iter = re.finditer(OPERATIONS_PATTERN, astring)
            operation_fmt_list = []
            operation_fmt = ''
            for op_m in operations_m_iter:
                op = op_m.group(operation)
                field = op_m.group(field)
                value = op_m.group(value)
                if op == '$set':
                    operation_fmt = 'set {0}={1}'.format(field, value)
                if op == '$inc':
                    operation_fmt = 'set {0}={0}+{1}'.format(field, value)
                if op == '$unset':
                    operation_fmt = 'alter table {0} drop {1}'.format(self.coll, field)
                if op == '$push':
                    operation_fmt = 'alter table {0} add {1}, set {2}={3}'.format(self.coll, field,field, value)
                # other operations
                operation_fmt_list.append(operation_fmt)
            if len(operation_fmt_list) == 1:
                return operation_fmt_list[0]
            return ','.join(operation_fmt_list)
                            
            
            
        def handle_option(astring):
            """option string -> option fromat string"""
            OPTION_PATTERN = r'\s*\{\s*(?P<name>\w+)\s*:\s*(?P<value>\w+)\s*\}\s*'
            option_m = re.match(OPTION_PATTERN, astring)
            option_name = option_m.group(name)
            option_value = option_m.group(value)
            if option_value == 'true':
                return None
        def handle_update_criteria(astring):
            criteria_dict = self.handle_string(astring)
            crite_fmt = self.handle_dict(criteria_dict)
            return crite_fmt
        
        # inner functions definition ending    

        UPDATE_ARGS_PATTERN = r'^(?P<criteria>.*?),(?P<operations>.*),?\s*(?P<option>.*)?$'
        
        update_criteria = re.match(UPDATE_ARGS_PATTERN, self.op_args).group('criteria') # criteria string
        update_operations = re.match(UPDATE_ARGS_PATTERN, self.op_args).group('operations') # operation string
        update_option = re.match(UPDATE_ARGS_PATTERN, self.op_args).group('option') # option string
        print 'update_criteria_string: ' + update_criteria
        print 'update_operations_string: ' + update_operations
        print 'update_option_string: ' + update_option
        
        criteria_fmt = handle_update_criteria(update_criteria)
        operations_fmt = handle_operations(update_operations)
        #option_fmt = handle_option(update_option)
        print 'criteria_fmt: ' + criteria_fmt
        print 'operations_fmt: ' + operations_fmt
        
        if criteria_fmt == '':
            update_fmt = 'update {0} {1}'.format(self.coll, operations_fmt)
            return update_fmt
        update_fmt = 'update {0} {1} where {2}'.format(self.coll, operations_fmt, criteria_fmt)
        return update_fmt



    def parse_remove(self):
        """db.coll.remove(...)->delete from ..."""
        
        # inner functions
        def handle_option(astring):
            """option string -> option string format.
            As a  placeholder for  extending"""
            OPTION_PATTERN = r'\s*(?P<optional>\w+)\s*:\s*(?P<value>.*)\s*'
            option_m_iter = re.finditer(OPTION_PATTERN, astring)
            option_fmt = ''
            for op_m in option_m_iter:
                op_optional = op_m.group('optional')
                op_value = op_m.group('value')
                if op_optional == 'justOne':
                    pass
                if op_optional == 'writeConcern':
                    pass
                if op_optional == 'isolated':
                    pass
            return option_fmt
    
        def handle_remove_criteria(astring):
            crite_fmt = []
            criteria_dict = self.handle_string(astring)
            crite_fmt = self.handle_dict(criteria_dict)
            return crite_fmt
        # inner functions' definition ended                         
        
        REMOVE_ARGS_PATTERN = r'^(?P<criteria>.*),?(?P<option>.*)?$'
        remove_criteria = re.match(REMOVE_ARGS_PATTERN, self.op_args).group('criteria')
        remove_option = re.match(REMOVE_ARGS_PATTERN, self.op_args).group('option')
        print 'remove_criteria_string: ' + remove_criteria
        print 'remove_option_string: ' + remove_option
        
        criteria_fmt = handle_remove_criteria(remove_criteria)
        option_fmt = handle_option(remove_option)
        print 'remove_criteria_fmt: ' + criteria_fmt
        print 'remove_option_fmt: ' + option_fmt
        
        if criteria_fmt == '':
            remove_fmt = 'delete from {0}'.format(self.coll)
            return remove_fmt
        remove_fmt = 'delete from {0} where {1}'.format(self.coll, criteria_fmt)
        return remove_fmt
        
          
                
    def parse_aggregate(self):
        """mongo aggregate statement -> sql advanced select statement"""

        # inner functions
        def handle_match(astring):
            match_dict = self.handle_string(astring)
            match_fmt_string = self.handle_dict(match_dict)
            return match_fmt_string


        AGGREGATE_ARGS_PATTERN = r'\s*(?P<stagename>[$\w]+)\s*:\s*(?P<stageargs>.*)\s*,?'

        projection_fmt = ''
        options_fmt = ''
        aggregate_m_iter = re.finditer(AGGREGATE_ARGS_PATTERN, self.op_args)
        stage__dict = {}
        stage_name_list = []
        for a_m in aggregate_m_iter:
            stage_name = a_m.group('stagename')
            stage_args = a_m.group('stageargs')
            stage__dict[stage_name] = stage_args
            stage_name_list.append(stage_name)

        for stage_name, stage_args in stage_dict.items():
            if stage_name == '$match' and stage_name_list.index('$match') < stage_name_list.index('$group'):
                match_fmt = ' where ' + handle_match(stage_args)
                options_fmt = options_fmt + match_fmt

            if stage_name == '$group':
                pass
                
            if stage_name == '$match' and stage_name_list.index('$match') > stage_name_list.index('$group'):
                match_fmt = ' having ' + handle_match(stage_args)
                options_fmt = options_fmt + match_fmt
                
            if stage_name == '$projection':
                pass
            if stage_name == '$sort':
                pass
            if stage_name == '$limit':
                pass
            if stage_name == '$sum':
                pass
        aggregate_fmt = 'select {0} from {1}'.format(projection_fmt,self.coll, options_fmt)        
        return aggregate_fmt
    
    def parse_createIndex(self):
        pass
            
                
        
def main():
    t_list = []
    #t_list.append("db.test.find({'c': 1, $or:['a':{$lt: 1}, 'd': {$gt: 9}]}#{'a': 1, 'b': 0}).sort('a': 1).limit(4).skip(2)")
    
    #t_list.append("db.test.find({})")
    #t_list.append("db.test.update({'a': 1},{$set:{}, $inc:{}})")
    t_list.append("db.test.insert({'a': 1, 'b': 2})")
    t_list.append("db.test.insert([{'c': 2, 'd': 4},{'d':5, 'to': 3}])")
    #t_list.append("db.test.remove()")
    #t_list.append("db.test.remove({'a': 1})")
    #t_list.append("db.test.find({}, {'a': 1, 'b': 0}")
    count = 0
    for t_statement in t_list:
        t = Thread(target=Transformer, args=(t_statement,))
        t.start()
        
        count += 1
        print '{0:*^88}'.format('beginning' + str(count))


if __name__ == '__main__':
    main()
