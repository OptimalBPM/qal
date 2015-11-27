"""
Created on Sep 19, 2010

@author: Nicklas Boerjesson
@note: This module accesses the Parameter_* and Verb_* classes
from the SQL.py and generates meta data by analyzing its objects.
"""

# Import the entire SQL namespace to make it searchable and suppress warnings.
from qal.sql.sql import *  # @UnusedWildImport #IGNORE:W0401
from qal.sql.base import *
from qal.sql.remotable import *
from qal.dataset.flatfile import FlatfileDataset  # @UnusedWildImport #IGNORE:W0401
from qal.dataset.xpath import XpathDataset  # @UnusedWildImport #IGNORE:W0401
from qal.dataset.matrix import MatrixDataset  # @UnusedWildImport #IGNORE:W0401
from qal.dataset.spreadsheet import SpreadsheetDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.rdbms import RDBMSDataset # @UnusedWildImport #IGNORE:W0401

def list_parameter_classes():
    """List all parameter classes"""
    result = list()
    for k in globals().items():
        if (k[0][0:9]).lower() == 'parameter':
            result.append(k[0])
    return result


def list_verb_classes():
    """List all verb classes"""
    result = list()
    for k in globals().items():
        if (k[0][0:4]).lower() == 'verb':
            result.append(k[0])
    return result


def list_class_properties(_class_name, _result = None):
    """List properties for the class specified in _class_name"""
    _class_name = _class_name.rpartition(".")[2]
    if _result is None:
        _result = list()
    if _class_name in globals():
        for k in globals()[_class_name].__dict__.items():
            if not hasattr(k[1], '__call__') and k[0][0:1] != '_' and k[0] not in _result:
                _result.append(k[0])
        _mro_length = len(globals()[_class_name]().__class__.__mro__)
        if _mro_length > 2:
            for _mro_idx in range(1, _mro_length - 1):
                list_class_properties(globals()[_class_name]().__class__.__mro__[_mro_idx].__name__, _result)

    return _result


def find_class(_name, _raise_error=True):
    """Using name, find a class reference"""
    try:

        _object_instance = None
        _object_name = None
        for k in globals().items():
            if k[0].lower() == _name.lower():
                _object_instance = k[1]()
                _object_name = k[0]
                break
        # Python types
        if _name == "list":
            return [], "list"
    except Exception as e:
        raise Exception("sql_meta - find_class: Error looking for " + str(_name) + " : " + str(e))

    if _object_instance is None and _raise_error:
        raise Exception("sql_meta - find_class: Cannot find matching class - " + _name)

    return _object_instance, _object_name

