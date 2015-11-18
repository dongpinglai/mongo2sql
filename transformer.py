#! /usr/bin/env python
# -*- encoding: utf-8 -*-
#created by: laidongping2006@sina.com 


import re


class Transformer(object):
    def _init__(self, m_statement):
        PATEERN = r'db\.(\w)+\.(\w)+\(\s*(.*)\s*\)(.*)'
        m = re.match(PATTERN, m_statement)
        self.db = 'database'
        self.coll = m.group(1)
        self.op = m.group(2)
        self.op_args = m.group(3)
        self.option_ops = m.group(4)

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
        criteria_dict = {}
        projection_dict = {}
        option_dict = {}
        if self.option_ops == '':
            ARGS_PATTERN = r'^(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})?\s*,?\s*(\{\s*(.*)\s*\})\s*$'
                      
            criteria = re.match(ARGS_PATTERN, self.args).group(2) # criteria string
            projection = re.match(ARGS_PATTERN, self.args).group(4) # projection string
           # option = re.match(ARGS_PATTERN, self.args).group(6) # option string
            if projection == '':
                pass
            else:
                PROJECTION_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
                projection_m_iter = re.finditer(PROJECTION_PATTERN, option)
                for  p_m in projection_m_iter:
                    p_field = p_m.group(1)
                    p_value = p_m.group(2)
                    projection_dict[p_field] = p_value 
                p_fmt = ','.join(key for key in projection_dict if projection[key] == 1]
                if criteria == '':
                    return 'select {0} from {1}'.format(p_fmt, self.coll)

                #self.handle_criteria()

                    
        else:
            pass
            
    def parse_insert(self):
        pass

    def parse_update(self):
        pass

    def parse_remove(self):
        pass
        

    def handle_criteria_string_dict(self, astring):
        PATTERN = r'(\s*(\$\w+)\s*:\s*(\[.*\])\s*|\s*(\$\w+)\s*:\s*(\{.*\})\s*|\s*()\s*:\s*()\s*)'
        criteria_dict = {}
        pass
        
        
    def handle_criteria(adict):
        fmt = ''
        for key, value in adict.items:
            if isinstance(value, list):
              
                for v in value:
                   pass
                    
                fmt = ' ' + key.translate(None, '$') + ' '.join(v_list)
            elif isinstance(value, dict):
                v_dict = {}
                for v_key, v_value in value.iterms:
                    v_value = handle_criteria(v_value)
                    pass
            else:
                
            
            
                
        
    

    

    
