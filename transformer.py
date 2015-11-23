#! /usr/bin/env python
# -*- encoding: utf-8 -*-
#created by: laidongping2006@sina.com 


import re


class Transformer(object):
    def _init__(self, m_statement):
        PATEERN = r'^db\.(\w)+\.(\w)+\(\s*(.*)\s*\)(.*)$'
        m = re.match(PATTERN, m_statement)
        if m:
            self.db = 'database'
            self.coll = m.group(1)
            self.op = m.group(2)
            self.op_args = m.group(3)
            self.option_ops = m.group(4)
        else:
            print 'input again'

    def transform(self):
        if self.op == 'find':
            self.parse_find()
        if self.op == 'insert':
            self.parse_insert()
        if self.op == 'update':
            self.parse_update()
        if self.op == 'remove':
            self.parse_remove()

    def parse_find(self):
        #criteria_dict = {}
        projection_dict = {}
        option_op_fmt = handle_option_ops(self.option_ops)
        
        FIND_ARGS_PATTERN = r'^(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})\s*$'
        criteria = re.match(FIND_ARGS_PATTERN, self.args).group(2) # criteria string
        projection = re.match(FIND_ARGS_PATTERN, self.args).group(4) # projection string

        if projection == '' or self.args == '':
            c_fmt = self.handle_criteria(criteria)
            if criteria == '':
                return 'select * from {0} {1}'.format(self.coll, option_op_fmt)

            return 'select * from {0} where {1} {2}'.format(self.coll, c_fmt, option_op_format)
        else:
            PROJECTION_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
            projection_m_iter = re.finditer(PROJECTION_PATTERN, projection)
            for  p_m in projection_m_iter:
                p_field = p_m.group(1)
                p_value = p_m.group(2)
                projection_dict[p_field] = p_value 

            p_fmt = ','.join([key for key in projection_dict if projection[key] == 1])
            if criteria == '':
                return 'select {0} from {1} {2}'.format(p_fmt, self.coll, option_op_fmt)

            c_fmt = self.handle_criteria(criteria)
            return 'select {0} from {1} where {2} {3}'.format(p_fmt, self.coll, c_fmt, option_op_fmt)

                    
       # else: #select [*|...] from ...[ where ...] [sort|limit|skip]
        #    option_op_fmt = handle_option_ops(self.option_ops)
         #   if  projection == '':
          #      c_fmt = self.handle_criteria(criteria)
           #     if criteria == '':
            #        return 'select * from {0}'
            



            
        def handle_option_ops(opsting):
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
                

    def parse_insert(self):
        INSERT_ARGS_PATTERN = r'(^\[\s*(.*)\s*\]$|^\{\s*(.*)\s*\}$)'
        insert_args_m = re.match(INSERT_ARGS_PATTERN, self.op_args)
        insert_args = insert_args_m.group(0)
        insert_args = handle_insert_args(insert_args)
        insert_fmt = args_to_fmt(insert_args)
        insert_fmt = 'insert into {0} values {1}'.format(self.coll, insert_fmt)
        return insert_fmt
        
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
                   
           
                

    def parse_update(self):
        UPDATE_ARGS_PATTERN = r'^(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})\s*$'
        update_criteria = re.match(UPDATE_ARGS_PATTERN, self.args).group(2) # criteria string
        update_operations = re.match(UPDATE_ARGS_PATTERN, self.args).group(4) # operation string
        option = re.match(UPDATE_ARGS_PATTERN, self.args).group(6) # option string
        criteria_fmt = self.handle_criteria(update_criteria)
        operations_fmt = handle_operations(update_operations)
        option_fmt = handle_option(option)
        update_fmt = 'update {0} set {1} where'.format(self.coll, operations_fmt, criteria_fmt)
        return update_fmt

        def handle_operations(astring):
            """operations string -> operation format string"""
            OPERATTIONS_PATTERN = r''
            
            pass
        def handle_option(astring):
            """option string -> option fromat string"""
            pass
        

    def parse_remove(self):
        pass
        

    def handle_criteria(self, astring, callback):
        """criteria string -> datastructures -> where format string"""
        
        PATTERN = r'(\s*(\$\w+)\s*:\s*(\[.*\])\s*|\s*(\w+)\s*:\s*(\{.*\})\s*|\s*(\w+)\s*:\s*(\w+)\s*)'
        criteria_dict = {}
        criteria_m = re.finditer(PATTERN, astring)
        for c_m in criteria_m:
            c_field  = c_m.group(1)
            c_value =  c_m.group(2)
            c_value = self.parse_criteria_value(c_value)
            criteria_dict[c_field] = c_value
        return callback(criteria_dict)
        
    def parse_criteria_value(c_value):
        """ value string -> datastructures"""
        if c_value.startswith('['): # e.g: {$or: [{}, ..]}
            c_value_list = []
            C_VALUE_LIST_PATTERN = r'\s*((.*)\s*,?)\s*'
            c_value_list_m = re.finditer(C_VALUE_LIST_PATTERN, c_value)
            for c_v_l_m in c_value_list_m:
                c_v_l = c_v_l_m.group(2)
                c_v_l = parse_criteria_value(c_v_l)
                c_value_list.append(c_v_l)
 
            return c_value_list

        elif c_value.startswith('{'): # e.g: {XXX:{$lt:XX}}
            c_value_dict = {}
            C_VALUE_DICT__PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
            c_value_dict_m = re.finditer(C_VALUE_DICT_PATTERN, c_value)
            for c_v_d_m in c_value_dict_m:
                c_v_field = c_v_d_m.group(1)
                c_v_value = c_v_d_m.group(2)
                c_v_value = parse_criteria_value(c_v_value)
                c_value_dict[c_v_field] = c_v_value
                    
            return c_value_dict

        else: # e.g: {XXX: XX}
            return c_value
                    

        
    def criteria_to_fmt(self, adict):
        """a dict -> format string"""
        for field, value in adict.items:
            fmt_list = []
            if isinstance(value, list):
                c_l = []
                for vl in value:
                    c_l.append(self.check_criteria(v1))
                if field == '$or':
                    c_l = ' or '.join(c_1)
                if field == '$and':
                    c_1 = ' and '.join(c_1)
                    
                fmt_list.append(c_l)
            if isinstance(value, dict):
                c_d = []
                for k, v in value.items():
                    if isinstance(v, dict):
                        c_d.append(self.check_criteria(v))
                    
                    else:
  
                        if k == '$lt':
                            c_d.append('{0}<{1}'.format(k, v))
                        if k == '$lte':
                            c_d.append('{0}<={1}'.format(k, v))
                        if k == '$gt':
                            c_d.append('{0}>{1}'.format(k, v))
                        if k == '$gte':
                            c_d.append('{0}>={1}'.format(k, v))
                        if k == '$eq':
                            c_d.append('{0}={1}'.format(k, v))
                        if k == '$ne':
                            c_d.append('{0}!={1}'.format(k, v))
                        # can add other operators from here

                c_d = ' and '.join(c_d)
                fmt_list.append(c_d)
            else:
                fmt_list.append('{0}={1}'.format(field, value))
        return ' and '.join(fmt_list)
           
        

                
    def parse_aggregate(self):
        pass
    
            
            
                
        
def main():
    t_list = []
    t1 = Transformer("db.test.find({})")
    t_list.append(t1)
    #t2 = Transformer("db.test.update({},{$set:{}, $inc:{}})")

    
    #t_list.append(t2)
    for t in t_list:
        t.transform()



if __name__ == '__main__':
    main()

    

    
