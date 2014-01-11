'''
Created on Oct 6, 2010

@author: Nicklas Boerjesson
'''

from qal.common.settings import UBPMSettings
from qal.dal.dal import Database_Abstraction_Layer
from qal.dal.types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER
import os



def get_default_dal(_db_type, _db_name = ""):
    """Returns a default database connection for the given db typ.
    Read from environment variable first, then assume files are in /config/subdirectory.
    # TODO: Fix so it uses \ on the windows platform. """
    cfg_Path = os.getenv('OPTIMAL_BPM_TESTCFG', os.path.dirname(os.path.realpath( __file__ )) + '/config/')
    print("Testing config path set to: "+ cfg_Path)
        
    cfg_MySQL      = cfg_Path + 'MySQL.conf'
    cfg_PostgreSQL = cfg_Path + 'PostgreSQL.conf'
    cfg_Oracle     = cfg_Path + 'Oracle.conf'
    cfg_DB2        = cfg_Path + 'DB2.conf'
    cfg_SQLServer  = cfg_Path + 'SQL_Server.conf'


    cfg_file = ''
    if   _db_type == DB_MYSQL:
        cfg_file = cfg_MySQL
    elif _db_type == DB_POSTGRESQL:
        cfg_file = cfg_PostgreSQL
    elif _db_type == DB_ORACLE:
        cfg_file = cfg_Oracle
    elif _db_type == DB_DB2:
        cfg_file = cfg_DB2
    elif _db_type == DB_SQLSERVER:
        cfg_file = cfg_SQLServer
    else:
        raise Exception("GetConnection: Invalid database type.") 
    
    settings = UBPMSettings(cfg_file)
    if settings.Parser.has_section("database"):
        if _db_name != "":
            settings.Parser.set("database", "database_name", _db_name)  
        return Database_Abstraction_Layer(settings)
    else:
        return None


def default_dal(_db_type):
    """ default_dal is a class decorator that sets the self.dal property of the class. """
    def decorator_dal(instance):
        if (_db_type == None):
            instance._dal = get_default_dal(DB_POSTGRESQL)
        else:
            instance._dal = get_default_dal(_db_type)
        return instance
    return decorator_dal
