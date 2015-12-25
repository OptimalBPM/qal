"""
Created on Sep 19, 2010

@author: Nicklas Boerjesson
@note: This module generates metadata by analyzing the objects of the supplied scope.
"""



def _json_add_child_properties(_globals, _class_name, _property_to_type):
    """Convenience function when building JSON Schema"""
    _properties = {}

    for _curr_property in list_class_properties(_globals, _class_name):

        _curr_type = _property_to_type(_curr_property)[0]

        if not isinstance(_curr_type, dict) and"$ref" not in _curr_type:
            _curr_type = {"type": _curr_type}


        _properties[_curr_property] = _curr_type

    return _properties


def list_prefixed_classes(_globals, _prefix, _exclude=[]):
    """List all classes in the scope with the provided prefix"""
    _result = list()
    _prefix_length = len(_prefix)
    for k in _globals.items():
        if  hasattr(k[1], "__dict__") and len(k[1].__dict__) > 0 \
            and (k[0][0:_prefix_length]).lower() == _prefix and k[0] not in _exclude:
            _result.append(k[0])
    return _result


def list_class_properties(_globals, _class_name, _result = None):
    """List properties for the class specified in _class_name"""
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
    """Using name, find a class reference"""
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

def set_dict_if_set(_dest, _attribute, _value):
    """Set a dict attribute if value is set"""
    if _value is not None:
        _dest[_attribute] = _value

def set_property_if_in_dict(_dest_obj, _property_name, _dict, _error_msg = None):
    """Set an object property is the same attribute exists in the dict. If _error is set, raise exception if missing."""
    if _property_name in _dict:
        _dest_obj.__dict__[_property_name] = _dict[_property_name]
    elif _error_msg is not None:
        raise Exception(_error_msg + " Attribute missing: " + _property_name)

