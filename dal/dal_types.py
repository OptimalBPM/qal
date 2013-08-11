'''
Created on Aug 17, 2010

@author: Nicklas Boerjesson
'''

from common.listhelper import CI_index

DB_MYSQL = 0
DB_POSTGRESQL = 1
DB_ORACLE = 2
DB_DB2 = 3
DB_SQLSERVER = 4


def unenumerate(value, _Type):
    return value[_Type]   

def db_types():
    return ['MySQL', 'PostgreSQL', 'Oracle','DB2','SQLserver'];

def string_to_db_type(_value):
   
    result = CI_index(db_types(),_value)
    if result > -1:
        return result
    else:
        raise Exception("string_to_db_type: Invalid database type.") 

        
    
def db_type_to_string(_DBType):
    try:
        return unenumerate(db_types(), _DBType)
    except:
        raise Exception("db_type_to_string: Invalid database type.")


    
    

