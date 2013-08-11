'''
Created on Sep 19, 2010

@author: Nicklas Boerjesson
@note: This module accesses the Parameter_ and Verb_ classes 
from the SQL.py and generates meta data by analyzing its objects.
'''

# Import the entire SQL namespace to make it searchable and supress warnings.
from .sql import * #@UnusedWildImport #IGNORE:W0401
from dataset.flatfile import Parameter_Flatfile_Dataset #@UnusedWildImport #IGNORE:W0401
from dataset.rdbms import Parameter_RDBMS_Dataset #@UnusedWildImport #IGNORE:W0401
from dataset.xml import Parameter_XML_Dataset #@UnusedWildImport #IGNORE:W0401
from dataset.matrix import Parameter_Matrix_Dataset #@UnusedWildImport #IGNORE:W0401

def list_parameter_classes():
    """List all parameter classes"""
    result = list()
    for k in globals().items():
        if (k[0][0:10]).lower() == 'parameter_':
            result.append(k[0]) 
    return result    
def list_verb_classes():
    """List all verb classes"""
    result = list()
    for k in globals().items():
        if (k[0][0:5]).lower() == 'verb_':
            result.append(k[0]) 
    return result    

def list_class_properties(_classname):
    """List properties for the class specified in _classname"""
    _classname = _classname.rpartition(".")[2]
    result = list()
    if (_classname in globals()):
        for k in globals()[_classname].__dict__.items():
            if not hasattr(k[1], '__call__') and k[0][0:1] != '_': 
                result.append(k[0])
                
        if len(globals()[_classname]().__class__.__mro__) > 2:
            result.extend(list_class_properties(globals()[_classname]().__class__.__mro__[1].__name__))
    
    return result

def find_class(_name, _raiserror = True):
    """Using name, find a class reference""" 
    Obj = None
    ObjName = None
    for k in globals().items():
        if k[0].lower() == _name.lower():
            Obj = k[1]()
            ObjName = k[0]
            break
    if (Obj == None and _raiserror):
        raise Exception("sql_meta - find_class: Cannot find matching class - " + _name)
    return Obj, ObjName
    
                
