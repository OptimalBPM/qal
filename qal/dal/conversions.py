"""
Created on Sep 23, 2013

@author: Nicklas Boerjesson
"""

from datetime import datetime

from qal.dal.types import DB_MYSQL

"""
MySQL constants, made from: https://github.com/PyMySQL/PyMySQL/blob/master/pymysql/constants/FIELD_TYPE.py
"""
MySQL_DECIMAL = 0
MySQL_TINY = 1
MySQL_SHORT = 2
MySQL_LONG = 3
MySQL_FLOAT = 4
MySQL_DOUBLE = 5
MySQL_NULL = 6
MySQL_TIMESTAMP = 7
MySQL_LONGLONG = 8
MySQL_INT24 = 9
MySQL_DATE = 10
MySQL_TIME = 11
MySQL_DATETIME = 12
MySQL_YEAR = 13
MySQL_NEWDATE = 14
MySQL_VARCHAR = 15
MySQL_BIT = 16
MySQL_NEWDECIMAL = 246
MySQL_ENUM = 247
MySQL_SET = 248
MySQL_TINY_BLOB = 249
MySQL_MEDIUM_BLOB = 250
MySQL_LONG_BLOB = 251
MySQL_BLOB = 252
MySQL_VAR_STRING = 253
MySQL_STRING = 254
MySQL_GEOMETRY = 255

"""Translation methods for MySQL field codes into the SQL data types(qal.sql.types):
return ['integer', 'string', 'string(255)', 'string(3000)', 'float', 'serial', 'timestamp', 'boolean']

Arguably, they should be declared in qal.dal.types instead.
"""


def mysql_type_to_sql_type(_type_code):
    """
    Convert the internal MySQL type to the SQL type

    :param _type_code: A MySQL field constant
    """
    if _type_code in (
            MySQL_VARCHAR,
            MySQL_VAR_STRING,
            MySQL_STRING
    ):
        return 'string'
    elif _type_code in (
            MySQL_TINY_BLOB,
            MySQL_MEDIUM_BLOB,
            MySQL_LONG_BLOB,
            MySQL_BLOB,
    ):
        return "blob"
    elif _type_code in (
            MySQL_DECIMAL,
            MySQL_FLOAT,
            MySQL_DOUBLE,
            MySQL_NEWDECIMAL
    ):
        return "float"
    elif _type_code in (
            MySQL_TINY,
            MySQL_SHORT,
            MySQL_LONG,
            MySQL_LONGLONG,
            MySQL_INT24,
            MySQL_BIT
    ):
        return "integer"
    elif _type_code in (
            MySQL_DATE,
            MySQL_TIME,
            MySQL_DATETIME,
            MySQL_YEAR,
            MySQL_NEWDATE,
            MySQL_TIMESTAMP):
        return "timestamp"
    else:
        raise Exception("mysql_type_to_sql_type: _type_code \"" + str(_type_code) + "\"not supported")


def python_type_to_sql_type(_python_type):
    """
    Convert a python data type to ab SQL type.

    :param _python_type: A Python internal type
    """
    if _python_type == str:
        return 'string'
    elif _python_type == bytes:
        return "blob"
    elif _python_type == float:
        return "float"
    elif _python_type == int:
        return "integer"
    elif _python_type == datetime:
        return "datetime"
    elif _python_type == bool:
        return "boolean"
    else:
        raise Exception("python_type_to_sql_type: _type_code \"" + str(_python_type) + "\"not supported")


def parse_description(_descriptions, _db_type):
    """Convert field descriptions to field name- and field type-lists.

    :param _descriptions: A list of descriptions
    :param _db_type: The database type
    """
    _field_names = []
    _field_types = []

    for _column in _descriptions:
        _field_names.append(_column[0])
        if _db_type == DB_MYSQL:
            _field_types.append(mysql_type_to_sql_type(_column[1]))
        else:
            _field_types.append(_column[1])

    return _field_names, _field_types

