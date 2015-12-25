"""This modules holds convenience functions used when generating JSON and JSON Schema"""


from qal.common.meta import list_class_properties

__author__ = 'nibo'


def json_add_child_properties(_globals, _class_name, _property_to_type):
    """
    Convenience function when building JSON Schema
    :param _globals: The scope (globals()) where the class is defined
    :param _class_name: The name of the class to iterate
    :param _property_to_type: A callback that returns the JSON schema type of a property based on its name
    :return: A dict of properties
    """
    _properties = {}

    for _curr_property in list_class_properties(_globals, _class_name):

        _curr_type = _property_to_type(_curr_property)
        if _curr_type is not None:
            if not isinstance(_curr_type[0], dict) and "$ref" not in _curr_typ[0]:
                _curr_type = {"type": _curr_type[0]}

            _properties[_curr_property] = _curr_type[0]

    return _properties


def set_dict_if_set(_dest, _attribute, _value):
    """Set a dict attribute if value is set"""
    if _value is not None:
        _dest[_attribute] = _value


def set_property_if_in_dict(_dest_obj, _property_name, _dict, _default_value = None, _error_msg = None):
    """Set an object property is the same attribute exists in the dict. If _error is set, raise exception if missing."""
    if _property_name in _dict:
        _dest_obj.__dict__[_property_name] = _dict[_property_name]
    elif _default_value is not None:
        _dest_obj.__dict__[_property_name] = _default_value
    elif _error_msg is not None:
        raise Exception(_error_msg + " Attribute missing: " + _property_name)