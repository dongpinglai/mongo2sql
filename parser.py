#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com

import re
import MySQLdb


def parse(statement):
    parts = re.split(r'\.', statement)
    db = parts[0]
    coll = parts[1]
    for part in parts[2:]: 
        
        
   
    
    


   
class BaseParser(object):
    def __init__(self, statement):
        self.db = ' '
        self.table = ' '
        self.conditions = dict()
