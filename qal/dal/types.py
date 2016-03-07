"""
Created on Aug 17, 2010

@author: Nicklas Boerjesson
"""

from qal.common.listhelper import ci_index

DB_MYSQL = 0
DB_POSTGRESQL = 1
DB_ORACLE = 2
DB_DB2 = 3
DB_SQLSERVER = 4
DB_SQLITE = 5

def unenumerate(value, _type):
    """Returns the value of a specific item"""
    return value[_type]


def db_types():
    """Returns a list of supported database engines"""
    return ['MySQL', 'PostgreSQL', 'Oracle', 'DB2', 'SQLserver', "SQLite"]


def string_to_db_type(_value):
    """Returns db_type constant matching the specified string"""
    result = ci_index(db_types(), _value)
    if result > -1:
        return result
    else:
        raise Exception("string_to_db_type: Invalid database type:" + str(_value))


def db_type_to_string(_dbtype):
    """Returns string matching the specified db_type constant"""""
    try:
        return unenumerate(db_types(), _dbtype)
    except:
        raise Exception("db_type_to_string: Invalid database type:" + str(_dbtype))
