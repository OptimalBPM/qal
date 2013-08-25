'''
Created on Aug 17, 2010

@author: Nicklas Boerjesson
'''

from qal.common.listhelper import CI_index

DB_MYSQL = 0
DB_POSTGRESQL = 1
DB_ORACLE = 2
DB_DB2 = 3
DB_SQLSERVER = 4


def unenumerate(value, _Type):
    """Returns the value of a specific item"""
    return value[_Type]   

def db_types():
    """Returns a list of supported database engines"""
    return ['MySQL', 'PostgreSQL', 'Oracle','DB2','SQLserver'];

def string_to_db_type(_value):
    """Returns db_type constant matching the specified string"""
    result = CI_index(db_types(),_value)
    if result > -1:
        return result
    else:
        raise Exception("string_to_db_type: Invalid database type.") 

        
    
def db_type_to_string(_DBType):
    """Returns string matching the specified db_type constant"""""
    try:
        return unenumerate(db_types(), _DBType)
    except:
        raise Exception("db_type_to_string: Invalid database type.")
