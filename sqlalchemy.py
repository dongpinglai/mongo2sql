#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# by: laidongping2006@sina.com


#?import inspect 
        
        
class SQLSchema(object):
   
    def __init__(self, ParserObj):
        self.parser = ParserObj
        #using inspect ParserObj to realize which Schema to choose here 
        #or writring another module use inspect to realize this function.
    
    def check_op_name(self,op_name):
        if op_name in self.op_names:
            return True
        return False
    
   
               
  
class RSchema(SQLSchema):
    """according to RParser's attributes selecting different sql statements."""

    def map_sql(self):
        db = self.parser.db
        table_name = self.parser.coll
        ops = self.parser.ops
        op_names = self.parser.op_names
        criteria_dict = self.parser.criteria_dict
        projection_dict = self.parser.projection_dcit
        option_dict = self.parser.option_dict

        projections = [key for key in projection_dict if projection_dict[key]=='1']
        #if 
        criterias = ['{0}={1}'.format(key, value) for key, value in criteria_dict]
        
            
        
        
    
    
    

