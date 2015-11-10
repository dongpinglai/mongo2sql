#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com


coll_methods = ['insert', 'find', 'findOne', 'update',
                'drop', 'limit', 'createIndex', 'dropIndex',
                'dropIndexes', 'count', 'distinct', 'group',
                'remove', 'aggregate', 'mapReduce', 'ensureIndex',
                'skip', 'sort']


class Controler(object):
    def __init__(self, methods):
        self.methods = []
        for m in methods:
            self.methods.append(m)
        
    def method_len(self):
        return len(self.methods)
        


class SingleMethodControler(Controler):
    pass
