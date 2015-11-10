#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com

        
        
        
class SQLSchema(object):
    mapping_dict ={
        'select {0} from {1};': {'find'},
        'select {0} from {1} where {2};' {'find', 'sort'} 
   

    }


    def __init__(self, db, coll, ops):
        self.db = db
        self.table_name = coll
        self.ops = ops
    
    def get_op_names(self):
        op_names = []
        for op in self.ops:
            op_names.append(op)
        return op_names
    
   
               
    def map_ops(self):
        if 'find' in op_names:
            
           # anylizer
           #generator    
            
        if 'insert' in op_names:
            pass
        if 'update' in op_names:
            pass
        if 'remove' in op_names:
            pass
    def query_anylize(self, op_names):
       find_args = self.ops['find']
       operators = []
       for op in op_names[1:]:
           operators.append(op)
        #if args is []:
         #   fmt = 'select * from {0}'.formt(self.table_name)
          #  print fmt
           # return fmt
        criterias = find_args[0]
        projections = find_args[1]
        options = find_args[2]
        
            
            
        
        
    
    
    

