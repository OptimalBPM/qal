"""
Created on Oct 6, 2010

@author: Nicklas Boerjesson
"""

import os

from qal.common.settings import UBPMSettings
from qal.dal.dal import DatabaseAbstractionLayer
from qal.dal.types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER


def get_default_dal(_db_type, _db_name=""):
    """Returns a default database connection for the given db typ.
    Read from environment variable first, then assume files are in /config/subdirectory.
    # TODO: Fix so it uses \ on the windows platform. """
    cfg_path = os.path.expanduser(
        os.getenv('OPTIMAL_BPM_TESTCFG', os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config')))
    print("Testing config path set to: " + cfg_path)

    cfg_mysql = os.path.join(cfg_path, "MySQL.conf")
    cfg_postgresql = os.path.join(cfg_path, "PostgreSQL.conf")
    cfg_oracle = os.path.join(cfg_path, "Oracle.conf")
    cfg_db2 = os.path.join(cfg_path, "DB2.conf")
    cfg_sqlserver = os.path.join(cfg_path, "SQL_Server.conf")

    if _db_type == DB_MYSQL:
        cfg_file = cfg_mysql
    elif _db_type == DB_POSTGRESQL:
        cfg_file = cfg_postgresql
    elif _db_type == DB_ORACLE:
        cfg_file = cfg_oracle
    elif _db_type == DB_DB2:
        cfg_file = cfg_db2
    elif _db_type == DB_SQLSERVER:
        cfg_file = cfg_sqlserver
    else:
        raise Exception("GetConnection: Invalid database type.")

    settings = UBPMSettings(cfg_file)
    if settings.parser.has_section("database"):
        if _db_name != "":
            settings.parser.set("database", "database_name", _db_name)
        _dal = DatabaseAbstractionLayer(settings)
        _dal.connect_to_db()
        return _dal
    else:
        return None


def default_dal(_db_type):
    """ default_dal is a class decorator that sets the self.dal property of the class. """

    def decorator_dal(instance):
        if _db_type is None:
            instance._dal = get_default_dal(DB_POSTGRESQL)
        else:
            instance._dal = get_default_dal(_db_type)

        return instance

    return decorator_dal
