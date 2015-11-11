#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com
'''
input a mongodb statement, output metadata(db, coll, ops)
'''

import re


class Parser(object):
    def __init__(self, statment):
        
        parts = re.split(r'\.', statement)
        self.db = parts[0]
        self.coll = parts[1]
        self.ops = {}
        for part in parts[2:]: 
            m = re.match(r'(\w+)\s*\(\s*(.*)\s*\)\s*', part)
            op_name = m.group(1)
            op_args = m.group(2)
           
            self.ops['op_name'] = op_args
    
    def op_names_anylize(self):
        op_names = []
        for op_name in self.ops:
            op_names.append(op_name)
        
        return op_names
    
    def op_name_exists(self, op_name):
        if op_name not in self.ops:
            return False
        return True



class RParser(Parser):
    """parser mongo's find statement"""
    
    def args_anylize(self):
        ARGS_PATTERN = r'\s*(\{.*\})?\s*,?\s*(\{.*\})?\s*,?\s*(\{.*\})?\s*'
        CRITERIA_PATTERN = r'\s*(\w+)\s*:\s*(([\'"]).*(?(1)\3))\s*'|r'\s*(\w+)\s*:\s*(([\{]).*(?(1)\3))\s*'|r'\s*(\w+)\s*:\s*(([\[]).*(?(1)\3))\s*'
        PROJECTION_PARTTERN = r'\s*(\w+)\s*:\s*(w+)\s*'
        OPTION_PARTTERN = r'\s*(\w+)\s*:\s*(w+)\s*'

        self.criteria_dict = {}
        self.projection_dict = {}
        self.option_dict = {}
        args = self.ops['find']
        if args != '':
            args_m = re.match(ARGS_PATTERN, args)
            criteria = m.group(1)
            projection = m.group(2)
            option = m.group(3) 
            criteria_m = re.finditer(CRITERIA_PATTERN, crireria)
            projection_m = re.finditer(PROJECTION_PATTEN, projection)
            option_m = re.finditer(OPTION_PATTERN, option)
             
            for cm in criteria_m:
                c_field = criteria_m.group(1)
                c_value = criteria_m.group(2)
                self.criteria_dict[c_field] = c_value
            for pm in projection_m:
                p_field = projection_m.group(1)
                p_value = projection_m.group(2)
                self.projection_dict[p_field] = p_value
            for om in option_m:
                o_field = option_m.group(1)
                o_value = option_m.grouup(2)
                self.option_dict[o_field] = o_value
        
    

class CParser(Parser):
    """parse mongo's insert statment"""

    

    
