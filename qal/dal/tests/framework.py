"""
Created on Oct 6, 2010

@author: Nicklas Boerjesson
"""
import json
import os

from qal.common.resources import Resources

from qal.dal.dal import DatabaseAbstractionLayer
from qal.dal.types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER


def get_default_dal(_db_type, _db_name=""):
    """Returns a default database connection for the given db typ.
    Read from environment variable first, then assume files are in /config/subdirectory.
    # TODO: Fix so it uses \ on the windows platform. """
    cfg_path = os.path.expanduser(
        os.getenv('OPTIMAL_BPM_TESTCFG', os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config')))
    print("Testing config path set to: " + cfg_path)

    if _db_type not in[DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER]:
        raise Exception("GetConnection: Invalid database type.")

    with open(os.path.join(cfg_path, "db_resources.json")) as f:
        _resources_list = json.load(f)

    _resources = Resources(_resources_list=_resources_list)
    _resource = _resources[_db_type]


    if _db_name:
            _resource.database["databaseName"] =_db_name
    _dal = DatabaseAbstractionLayer(_resource = _resource)
    return _dal


def default_dal(_db_type):
    """ default_dal is a class decorator that sets the self.dal property of the class. """

    def decorator_dal(instance):
        if _db_type is None:
            instance._dal = get_default_dal(DB_POSTGRESQL)
        else:
            instance._dal = get_default_dal(_db_type)

        return instance

    return decorator_dal
