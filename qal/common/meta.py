"""
Created on Sep 19, 2010

@author: Nicklas Boerjesson
@note: This module generates metadata by analyzing the objects of the supplied scope.
"""

from inspect import isclass


def list_prefixed_classes(_globals, _prefix, _exclude=[]):
    """
    List all classes in the scope with the provided prefix
    :param _globals: The scope to mine for classes
    :param _prefix: The prefix to filter by
    :param _exclude: Classes to explicitly exclude
    :return: A list of classes
    """
    _result = list()
    _prefix_length = len(_prefix)
    for k in _globals.items():
        if isclass(k[1]) and (k[0][0:_prefix_length]).lower() == _prefix and k[0] not in _exclude:
            _result.append(k[0])
    return _result

def list_suffixed_classes(_globals, _suffix, _exclude=[]):
    """
    List all classes in the scope with the provided prefix
    :param _globals: The scope to mine for classes
    :param _suffix: The _suffix to filter by
    :param _exclude: Classes to explicitly exclude
    :return: A list of classes
    """
    _result = list()
    _suffix_length = len(_suffix)
    for k in _globals.items():
        if isclass(k[1]) and (k[0][-_suffix_length:]).lower() == _suffix and k[0] not in _exclude:
            _result.append(k[0])
    return _result


def list_class_properties(_globals, _class_name, _result = None):
    """
    List properties for the class specified in _class_name
    :param _globals: The scope (globals()) where the class is defined
    :param _class_name: The name of the class to iterate
    :param _result:
    :return:
    """
    _class_name = _class_name.rpartition(".")[2]
    if _result is None:
        _result = list()
    if _class_name in _globals:
        for k in _globals[_class_name].__dict__.items():
            if not (callable(k[1]) or isinstance(k[1], staticmethod)) and k[0][0:1] != '_' and k[0] not in _result:
                _result.append(k[0])
        _mro_length = len(_globals[_class_name]().__class__.__mro__)
        if _mro_length > 2:
            for _mro_idx in range(1, _mro_length - 1):
                list_class_properties(_globals, _globals[_class_name]().__class__.__mro__[_mro_idx].__name__, _result)

    return _result


def find_class(_globals, _name, _raise_error=True):
    """Using class name, find a class reference"""
    try:

        _object_instance = None
        _object_name = None
        for k in _globals.items():
            if k[0].lower() == _name.lower():
                _object_instance = k[1]()
                _object_name = k[0]
                break
        # Python types
        if _name == "list":
            return [], "list"
    except Exception as e:
        raise Exception("meta - find_class: Error looking for " + str(_name) + " : " + str(e))

    if _object_instance is None and _raise_error:
        raise Exception("meta - find_class: Cannot find matching class - " + _name)

    return _object_instance, _object_name

def readattr(_obj, _name, _default = None):
    try:
        _value = getattr(_obj, _name)
    except AttributeError:
        return _default

    if _value is None:
        return _default
    else:
        return _value