#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com

import re
import MySQLdb

coll_methods = ['insert', 'find', 'findOne', 'update',
                'drop', 'limit', 'createIndex', 'dropIndex',
                'dropIndexes', 'count', 'distinct', 'group',
                'remove', 'aggregate', 'mapReduce', 'ensureIndex',
                'skip', 'sort']

def parse(statement):
    parts = re.split(r'\.', statement)
    db = parts[0]
    coll = parts[1]
    methods = []
    criterias = []
    for part in parts[2:]:
        
        
   
    
    


   
class BaseParser(object):
    def __init__(self, statement):
        self.db = ' '
        self.table = ' '
        self.conditions = dict()
