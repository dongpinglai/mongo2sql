#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# created by: laidongping2006@sina.com at 12th Nov 2015


import re

def criteria_schema(adict):
    pass
            



    
        
class SQLSchema(object):
   
    def __init__(self, ParserObj):
        self.parser = ParserObj
        #using inspect ParserObj to realize which Schema to choose here 
        #or writring another module use inspect to realize this function.
    
    def check_op_name(self,op_name):
        if op_name in self.op_names:
            return True
        return False

        
        
   
               
  
class RSQLSchema(SQLSchema):
    """according to RParser's attributes selecting different sql statements."""


    
    def map__to_sql(self):
        db = self.parser.db
        table_name = self.parser.coll
        ops = self.parser.ops
        op_names = self.parser.op_names
        criteria_dict = self.parser.criteria_dict
        projection_dict = self.parser.projection_dcit
        option_dict = self.parser.option_dict
    
        #'select ... from ... [where]...'    
        if not self.parser.op_name_exists('sort') and self.parser.self.parser.op_name_exists('limit'):
            if criteria_dict == {}: # 'select * from ...'
                if projection_dict == {}:
                    return 'select * from {0};'.format(table_name)
                else:
                    projections = [key for key in projection_dict if projection_dict[key]=='1']
                    projections = ','.join(projections)
                    return 'select {0} from {1};'.format(projection, table_name)
                    
            else: #'select ... from ... where ...'
                if projection_dict == {}: # 'select * from ... where '
                    for criteria_key, criteria_value in self.parser.criteria_dict.items():
                        

                        

                        
                   
                    pass
                   
               

                else: #select XXX from ....where ...'
                    pass
        
    
       


                    criterias = ['{0}={1}'.format(key, value) for key, value in criteria_dict]
        else: # 'select ... from ... where ... limit/sort ...'
            if criteria_dict == {}:
                if projection_dict == {}:
                    pass

            

            
        
    
    
    

def main():
    pass


if __name__ == '__main__':
    main()
