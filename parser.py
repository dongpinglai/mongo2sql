#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com
'''
input a mongodb statement, output metadata(db, coll, ops)
'''

import re



def parse(statement):
    parts = re.split(r'\.', statement)
    db = parts[0]
    coll = parts[1]
    ops = {}
    for part in parts[2:]: 
        m = re.match(r'(\w+)\s*\((.*)\)\s*', part)
        op_name = m.group(1)
        op_args = m.group(2)
        op_args = re.split(r',', op_args)
        ops['op_name'] = op_args
    return (db, coll, ops)

        
   
    
    


   

