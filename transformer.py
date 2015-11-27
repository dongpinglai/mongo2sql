#! /usr/bin/env python
# -*- encoding: utf-8 -*-
#created by: laidongping2006@sina.com 


import re


class Transformer(object):
    def __init__(self, m_statement):
        PATTERN = r'^db\.(?P<coll>\w+)\.(?P<op>\w+)\(\s*(?P<op_args>.*)\s*\).?(?P<option_ops>.*)?$'
        m = re.match(PATTERN, m_statement)
        if m:
            print 'got the match'
            print m.groups()
            self.db = 'database'
            self.coll = m.group(coll) # collection name string
            self.op = m.group(op) # the first function name string
            self.op_args = m.group(op_args) # the first function args string
            self.option_ops = m.group(option_ops) # other function string
            
        else:
            print 'input statament again'

    def transform(self):
        """ selection logic"""
        print 'transforming'
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
            
        print 'transformation completed'
        print sql_fmt



    def handle_string(self, astring):
        """a mongo args string -> a args dict. e.g: '{XXX: XXXX, ....}'->
        {XXX: XXXX, ....}
        """
        if astring == '':
            return {}
        STRING_PATTERN = r'(\s*(?P<key>[$\w]+)\s*:\s*(?P<value>.*)\s*,?)' # key: value, e.g: value:value or value:exp ...
        string_dict = {}
        string_m_iter = re.finditer(STRING_PATTERN, astring)
        for str_m in string_m_iter:
            key  = str_m.group(key)
            value =  str_m.group(value)
            if value.startswtih('['):
                value_list = []
                VALUE_LIST_PATTERN = r'\s*\{.*\}\s*,?' # {..},{...},...
                value_m_iter = re.finditer(VALUE_LIST_PATTERN, value)
                for v_m in value_m_iter:
                    value_list.append(handle_string(v_m.group(0)))
                value = value_list
            elif value.startswith('{'):
                value_dict = handle_string(value)
                value = value_dict
            else:
                value = value
            string_dict[key] = value
        return string_dict

    
    def handle_dict(self, adict):
        """a args dict -> a sql format string """
        fmt = []
        if adict == {}:
                return ''
        for field, value in crite_dict.items():
            if isinstance(value, list):
                l_fmt = []
                for v in value:
                    l_fmt.append(handle_dict(v))
                if field == '$or':
                    l_fmt = ' or '.join(l_fmt)
                elif field == '$and':
                    l_fmt = ' and '.join(l_fmt)
                fmt.append(l_fmt)
        
            elif isinstance(value, dict):
                d_fmt = []
                d_fmt.append(handle_dict(value))
                d_fmt = ' and '.join(d_fmt)
                fmt.append(d_fmt)
        
            elif field == '$lt':
                fmt.append('{0}<{1}'.format(field, value))
            elif field == '$lte':
                fmt.append('{0}<='.format(field, value))
            elif field == '$gt':
                fmt.append('{0}>{1}'.format(field, value))
            elif field == '$gte':
                fmt.append('{0}>={1}'.format(field, value))
            elif field == '$ne':
                fmt.append('{0}!={1}'.format(field, value))
            elif field == '$eq' or not field.startswith('$'):
                fmt.append('{0}={1}'.format(field, value))
            # add other operators' formats behind here
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
            crite_fmt = self.handle_dict(criteria_dict)
            return crite_fmt

        
        def handle_projection(projetion_string):
            proj_fmt = ''
            if projection_string == '':
                return proj_fmt
            PROJECTION_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
            projection_m_iter = re.finditer(PROJECTION_PATTERN, projection_string)
            for  p_m in projection_m_iter:
                p_field = p_m.group(1)
                p_value = p_m.group(2)
                projection_dict[p_field] = p_value 

            proj_fmt = ','.join([key for key in projection_dict if projection[key] == '1'])
            return proj_fmt


        def handle_option_ops(opstring):
            """opsting -> option_dict -> option_fmt """
            if opstring == '':
                option_fmt = ''
                return option_fmt
           
            OPTION_PATTERN = r'\.(\w+)\(\s*(.*)\s*\)'
            option_m_iter = re.finditer(OPTION_PATTERN, opstring)
            option_fmt = []
            for o_m in option_m_iter:
                option_op_name = o_m.group(1)
                option_op_args = o_m.group(2)
                option_dict[option_op_name] = option_op_args

            for op, op_args in option_dict.items():
                if op == 'sort':
                    
                    OP_ARGS_PATTERN = r'\s*(\w+)\s*:\s*(1|0)\s*'
                    op_args_m_iter = re.finditer(OP_ARGS_PATTERN, op_args)
                    for op_args_m in op_args_m_iter:
                        sort_op_name = op_args_m.group(1)
                        sort_op_value = op_args_m.group(2)

                        if sort_op_value == '1':
                           option_fmt.append('{0}'.format(sort_op_name))
                        if sort_op_value == '0':
                            option_fmt.append('{0} desc'.format(sort_op_name))
                    
                    option_fmt = 'order by' + ','.join(option_fmt)
                                        
                    
                if op == 'limit':
                    option_fmt = option_fmt + 'limit {0}'.format(op_args)
                if op == 'skip':
                    option_fmt = option_fmt + 'skip {0}'.format(op_args)
                    
            return option_fmt
        #inner functions ending

        find_fmt = ''
        FIND_ARGS_PATTERN = r'^(\{\s*(?P<criteria>.*)\s*\})?\s*,?\s*(\{\s*(?P<projection>.*)\s*\})?\s*,?\s*(\{\s*(?P<option>.*)\s*\})\s*$'
        criteria = re.(FIND_ARGS_PATTERN, self.op_args).group(criteria) # criteria string
        projection = re.match(FIND_ARGS_PATTERN, self.op_args).group(projection) # projection string
        option_ops = self.option_ops
        
        criteria_fmt = handle_find_criteria(criteria)
        projection_fmt = handle_projection(projection)
        option_ops_fmt = handle_option_ops(option_ops)
        
        if projection_fmt == '':
            if criteria_fmt == '':
                find_fmt = 'select * from {0} {1}'.format(self.coll, option_ops_fmt)
                return find_fmt
p
            find_fmt = 'select * from {0} where {1} {2}'.format(self.coll, criteria_fmt, option_ops_format)
            return find_fmt
            
        else:
            if criteria == '':
                find_fmt = 'select {0} from {1} {2}'.format(projection_fmt, self.coll, option_ops_fmt)
                return find_fmt

            find_fmt = 'select {0} from {1} where {2} {3}'.format(projection_fmt, self.coll, criteria_fmt, option_ops_fmt)
            return find_fmt
                        

    def parse_insert(self):
        """db.coll.insert(...)->insert into table_name values ..."""
        
        # inner functions
        def handle_insert_args(astring):
            """a string -> list or dict"""
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
                return arg_dict

        def args_to_fmt(insert_args):
            """ list or dict -> format string"""
            if isinstance(insert_args, list):
                mul_list = []
                for i in insert_args:
                    mul_list.append(args_to_fmt(i))
                return ','.join(mul_list)
            if isinstance(insert_args, dict):
                sin_list = [] 
                for k, v in insert_args.items:
                    sin_list.append(v)
                return '({0})'.format(','.join(sin_list))
        #inner functions ending
                
        INSERT_ARGS_PATTERN = r'^(\[\s*(.*)\s*\]|^\{\s*(.*)\s*\})$'
        insert_args_m = re.match(INSERT_ARGS_PATTERN, self.op_args)
        insert_args = insert_args_m.group(0)
        insert_args = handle_insert_args(insert_args)
        insert_fmt = args_to_fmt(insert_args)
        insert_fmt = 'insert into {0} values {1}'.format(self.coll, insert_fmt)
        return insert_fmt
        
        
    def parse_update(self):
        """db.coll.update(...)->update table_name ... / alter table table_name ..."""

        # inner functions
        def handle_operations(astring):
            """operations string -> operation format string"""
            OPERATTIONS_PATTERN = r'\s*(?P<operation>[$\w]+)\s*:\s*(\{\s*(?P<field>\w+)\s*:\s*(?P<value>\w+)\s*\})\s*'
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

        UPDATE_ARGS_PATTERN = r'^(?P<criteria>.*),(?P<operations>.*),?\s*(?P<option>.*)?$'
        
        update_criteria = re.match(UPDATE_ARGS_PATTERN, self.op_args).group(criteria) # criteria string
        update_operations = re.match(UPDATE_ARGS_PATTERN, self.op_args).group(operations) # operation string
        update_option = re.match(UPDATE_ARGS_PATTERN, self.op_args).group(option) # option string
        criteria_fmt = handle_update_criteria(update_criteria)
        operations_fmt = handle_operations(update_operations)
        option_fmt = handle_option(update_option)
        if criteria_fmt == '':
            update_fmt = 'update {0} {1}'.format(self.coll, operation_fmt)
            return update_fmt
        update_fmt = 'update {0} {1} where {2}'.format(self.coll, operations_fmt, criteria_fmt)
        return update_fmt



    def parse_remove(self):
        """db.coll.remove(...)->delete from ..."""
        
        # inner functions
        def handle_option(asrting):
            """option string -> option string format.
            As a  placeholder for  extending"""
            OPTION_PATTERN = r'\s*(?P<optional>\w+)\s*:\s*(?P<value>.*)\s*'
            option_m_iter = re.finditer(OPTION_PATTERN, astring)
            option_fmt = ''
            for op_m in option_m_iter:
                op_optional = op_m.group(optional)
                op_value = op_m.group(value)
                if op_optional == 'justOne':
                    pass
                if op_optional == 'writeConcern':
                    pass
                if op_optional == 'isolated':
                    pass
    
        def handle_remove_criteria(astring):
            crite_fmt = []
            criteria_dict = self.handle_string(astring)
            crite_fmt = self.handle_dict(criteria_dict)
            return crite_fmt
        # inner functions' definition ended                         
        
        REMOVE_ARGS_PATTERN = r'^(?P<criteria>.*),?(?P<option>.*)?$'
        remove_criteria = re.match(REMOVE_ARGS_PATTERN, self.op_args).group(criteria)
        remove_option = re.match(REMOVE_ARGS_PATTERN, self.op_args).group(option)
        
        criteria_fmt = handle_remove_criteria(remove_criteria)
        option_fmt = handle_option(remove_option)
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
            stage_name = a_m.group(stagename)
            stage_args = a_m.group(stageargs)
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
    t1 = Transformer("db.test.find({})")
    t_list.append(t1)
    t2 = Transformer("db.test.update({},{$set:{}, $inc:{}})")
    t_list.append(t2)
    for t in t_list:
        t.transform()



if __name__ == '__main__':
    main()
