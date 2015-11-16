#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# created by: laidongping2006@sina.com at 12th Nov 2015
'''
input a mongodb statement, output metadata(db, coll, ops) etc.
To improve the module, patterns needn't to write in every class, can write as  global vars.
'''

import re


def parse_criteria_value(c_value):
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
      
        self.op_names = []
        for op_name in self.ops:
            self.op_names.append(op_name)
        
        
    def op_name_exists(self, op_name):
        if op_name not in self.ops:
            return False
        return True



class RParser(Parser):
    """parser mongo's find statement"""
    
    def parse_args(self):
        ARGS_PATTERN = r'\s*(.*)?\s*,?\s*(.*)?\s*,?\s*(.*)?\s*'
        CRITERIA_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        PROJECTION_PARTTERN = r'\s*(.*)\s*:\s*(\d*)\s*'
        OPTION_PARTTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        
        self.criteria_dict = {}
        self.projection_dict = {}
        self.option_dict = {}
        find_args = self.ops['find']
        if args != '':
            find_args_m = re.match(ARGS_PATTERN, find_args)
            find_criteria = find_args_m.group(1)
            projection = find_args_m.group(2)
            option = find_args_m.group(3) 

            criteria_m = re.finditer(CRITERIA_PATTERN, crireria)
            projection_m = re.finditer(PROJECTION_PATTEN, projection)
            option_m = re.finditer(OPTION_PATTERN, option)
             
            for cm in criteria_m:
                c_field = cm.group(1)
                c_value = cm.group(2)
                c_value = parse_criteria(c_value)
                self.criteria_dict[c_field] = c_value

            for pm in projection_m:
                p_field = pm.group(1)
                p_value = pm.group(2)
                self.projection_dict[p_field] = p_value

            for om in option_m:
                o_field = om.group(1)
                o_value = om.grouup(2)
                self.option_dict[o_field] = o_value
        
        if self.op_name_exists('sort'):
            self.sort_criteria_dict = {}
            sort_args = self.ops['sort']
            if sort_args != '':
                sort_criteria_m = re.finditer(CRITERIA_PATTERN, sort_args)
                for s_c_field, s_c_value in sort_criteria_m:
                    self.sort_criteria_dict[s_c_field] =  s_c_value

        if self.op_name_exists('limit'):
            self.limit_arg  = self.ops['limit']
                
    

class CParser(Parser):
    """parse mongo's insert statment"""

    def parse_args(self):
        DOCS_PATTERN = r'\s*(\w*)\s*:\s*(.*)\s*'
        self.docs_dict  ={}
        args = self.ops['insert']
        if args != '':
            docs_m = re.finditer(DOCS_PATTERN, args)
            for dm in docs_m:
                d_field = dm.group(1)
                d_value = dm.group(2)
                self.docs_dict[d_field] = d_value
                

class UParser(Parser):
    """pase mongo's update statment"""

    
    def parse_args(self):
        ARGS_PATTERN = r'\s*(.*)?\s*,?\s*(.*)\s*,?\s*(.*)?\s*'
        CRITERIA_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        OPERATION_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        OPTION_PATTERN = r'\s*(.*):\s*(.*)\s*'
        self.criteria_dict = {}
        self.operation_dict = {}
        self.option_dict = {}




        args = self.['update']
        if args !='':
            args_m = re.match(ARGS_PATTERN, args)
            criteria = args_m.group(1)
            operation = args_m.group(2)
            option = args_m.group(3)
            criteria_m = re.finditer(CRITERIA_PATTERN, criteria)
            operation_m = re.finditer(OPERATION_PATTERN, operation)
            option_m = re.finditer(OPTION_PATTERN, option)


            for cm in criteria_m:
                c_field = cm.group(1)
                c_value = cm.group(2)
                c_value = parse_criteria_value(c_value)
                self.criteria_dict[c_field] = c_value

            for opm in operation_m:
                op_field = opm.group(1)
                op_value = opm.group(2)
                self.operation_dict[op_field] = op_value

            for om in option_m:
                o_field = om.group(1)
                o_value = om.group(2)
                self.option_dict[o_field] = o_value


    
class DParser(Parser):
    """parse mongo's remove statment"""

    def parse_args(self):
    
        ARGS_PATTERN = r'\s*(.*)?\s*,?\s*(.*?)\s*'
        CRITERIA_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        OPTION_PATTERN = r'\s*(.*)\s*:\s*(.*)\s*'
        self.criteria_dict = {}
        self.option_dict = {}

        args = self.ops['remove']
        if args != '':
            args_m = re.match(ARGS_PATTERN, args)
            criteria = args_m.group(1)
            option = args_m.group(2)
            criteria_m = re.finditer(CRITERIA_PATTERN, criteria)
            option_m = re.finditer(OPTION_PATTERN, option)

            for cm in criteria_m:
                c_field = cm.group(1)
                c_value = cm.group(2)
                c_value = parse_criteria_value(c_value)
                self.criteria_dict[c_field] = c_value
            for om in option_m:
                o_field = om.group(1)
                o_value = om.group(2)
                self.option_dict[o_field] = o_value


class IndexParser(Parser):
    """ parse mongo's createIndex and mongo's druopIndex statement."""

   
    ARGS_PATTERN = r'\s*()?\s*,?\s*()?\s*'
    CRITERIA_PATTERN = r'\s*(.*)\s*:\s*(\w*)\s*'
    OPTION_PATTERN = r'\s*(.*)\s*:\s*(\w*)\s*'
    
    def parse_createIndex_args(self):
        self.c_criteria_dict = {}
        self.c_option_dict = {}
        c_args = self.ops['createIndex']
        if c_args != '':
            c_args_m = re.match(ARGS_PATTERN, c_args)
            c_criteria = c_args_m.group(1)
            c_option = c_args_m.group(2)
            c_criteria_m = re.finditer(CRITERIA_PATTERN, c_criteria)
            c_option_m = re.finditer(OPTION_PATTERN, c_option)

            for c_cm in c_criteria_m:
                c_c_field = c_cm.group(1)
                c_c_value = c_cm.group(2)
                c_c_value = parse_criteria_value(c_c_value)
                self.c_criteria_dict[c_c_field] = c_c_value
            for c_om in c_option_m:
                c_o_field = c_om.group(1)
                c_o_value = c_om.group(2)
                self.c_option_dict[c_o_field] = c_o_value
        

    def parse_dropIndex_args(self):
        self.dr_criteria_dict = {}
        dr_args = self.ops['dropIndex']
        if dr_args != '':
            dr_criteria_m = re.finditer(CRITERIA_PATTERN, dr_args)
            for dr_cm in dr_criteria_m:
                dr_c_field = dr_cm.group(1)
                dr_c_value = dr_cm.group(2)
                dr_c_value = parse_criteria_value(dr_c_value)
                self.dr_criteria_dict[dr_c_field] = dr_c_value



class AggregateParser(Parser):
    ''' parse mongo's aggregate statements'''

    def parse_args(self):
        pass
        

        
        
def main():
    pass




if __name__ === '__main__':
    main()
