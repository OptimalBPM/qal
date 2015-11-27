"""
Created on Nov 22, 2012

@author: Nicklas Boerjesson

"""
from csv import list_dialects
import json
import string

from qal.dataset.custom import CustomDataset
from qal.sql.base import ParameterBase
from qal.sql.meta import list_class_properties, list_parameter_classes, list_verb_classes, find_class
from qal.sql.types import sql_property_to_type, and_or, \
    constraint_types, index_types, verbs, expression_item_types, \
    condition_part, set_operator, tabular_expression_item_types, data_source_types, join_types, in_types
from qal.dal.types import db_types


class SQLJSON():
    """
    This class converts the SQL structure(see SQL.py) into a JSON-compatible dict and back.
    """

    """Encoding of the document"""
    encoding = 'utf-8'

    """The base path of the document. Allows for relative paths within."""
    base_path = None

    """The resources"""
    _resources = None

    # Debugging
    debuglevel = 2
    nestinglevel = 0
    def __init__(self):
        """
        Constructor
        """
        self.debuglevel = 2
        self.nestinglevel = 0

    def _print_nestinglevel(self, _value):
        """Prints the current nesting level. Not thread safe."""
        self._debug_print(_value + ' level: ' + str(self.nestinglevel), 4)


    def _get_up(self, _value):
        """Gets up one nesting level. Not thread safe."""
        self.nestinglevel -= 1
        self._print_nestinglevel("Leaving " + _value)

    def _go_down(self, _value):
        """Gets down one nesting level. Not thread safe."""
        self.nestinglevel += 1
        self._print_nestinglevel("Entering " + _value)
    def _debug_print(self, _value, _debuglevel=3):
        """Prints a debug message if the debugging level is sufficient."""
        if self.debuglevel >= _debuglevel:
            print(_value)
    def _child_array_of(self, _types):
        if len(_types) > 1:
            return {
                "type": "array",
                "minItems": 0,
                "items": {"type": "object", "oneOf": [{"$ref": x} for x in _types]},
                "uniqueItems": True
            }
        elif len(_types) == 1:
            return {
                "type": "array",
                "minItems": 0,
                "items": {"$ref": _types[0]},
                "uniqueItems": True
            }
        else:
            raise Exception("Internal error in _child_array_of: _types are empty.")

    def _get_child_types(self):
        _result = {}
        _result["datatypes"] = {"type": "string", "pattern": "(integer|string|string(\(.*\))|serial|timestamp)"}
        _result["db_types"] = {"type": "string", "enum": db_types()}
        _result["and_or"] = {"type": "string", "enum": and_or()}
        _result["in_types"] = {"type": "string", "enum": in_types()}
        _result["index_types"] = {"type": "string", "enum": index_types()}
        _result["constraint_types"] = {"type": "string", "enum": constraint_types()}
        _result["set_operator"] = {"type": "string", "enum": set_operator()}

        _result["csv_dialects"] = {"type": "string", "enum": list_dialects()}
        _result["join_types"] = {"type": "string", "enum": join_types()}

        _result["csv_dialects"] = {"type": "string", "enum": list_dialects()}
        _result["statement"] = {"type": "object", "oneOf": [{"$ref": "#/definitions/" + x} for x in verbs()]}
        _result["condition_part"] = {"type": "object",
                                     "oneOf": [{"$ref": "#/definitions/" + x} for x in condition_part()]}
        _result["tabular_expression_item"] = {"type": "object", "oneOf": [{"$ref": "#/definitions/" + x} for x in
                                                                          tabular_expression_item_types()]}
        _result["data_source_types"] = {"type": "object", "oneOf": [{"$ref": "#/definitions/" + x} for x in
                                                                    data_source_types()]}

        _result["Array_string"] = {"type" : "array", "items": {"type": "string"}}

        _result["Array_ParameterString"] = self._child_array_of(['#/definitions/ParameterString'])
        _result["Array_ParameterConstraint"] = self._child_array_of(['#/definitions/ParameterConstraint'])
        _result["Array_ParameterColumndefinition"] = self._child_array_of(['#/definitions/ParameterColumndefinition'])
        _result["Array_ParameterSource"] = self._child_array_of(['#/definitions/ParameterSource'])
        _result["Array_ParameterWhen"] = self._child_array_of(['#/definitions/ParameterWhen'])
        _result["Array_ParameterIdentifier"] = self._child_array_of(['#/definitions/ParameterIdentifier'])
        _result["Array_Statement"] = self._child_array_of(['#/definitions/statement'])
        _result["Array_ParameterOrderByItem"] = self._child_array_of(['#/definitions/ParameterOrderByItem'])
        _result["Array_ParameterCondition"] = self._child_array_of(['#/definitions/ParameterCondition'])
        _result["Array_ParameterField"] = self._child_array_of(['#/definitions/ParameterField'])
        _result["Array_ParameterAssignment"] = self._child_array_of(['#/definitions/ParameterAssignment'])
        _result["Array_expression_item"] = self._child_array_of(
            [{"$ref": "#/definitions/" + x} for x in expression_item_types()])
        _result["Array_tabular_expression_item"] = self._child_array_of(
            [{"$ref": "#/definitions/" + x} for x in tabular_expression_item_types()])
        _result["Array_list"] = self._child_array_of(['*'])

        return _result


    def _add_child_property(self, _class_name):

        _properties = {}

        for _curr_property in list_class_properties(_class_name):
            _type = sql_property_to_type(_curr_property, _json_ref='#/definitions/')[0]
            if "$ref" not in _type:
                _type = {"type": _type}
            _properties[_curr_property] = _type

        return _properties

    def generate_schema(self):
        """Generates an JSON schema based on the class structure in SQL.py"""

        _result = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "description": "QAL SQL Schema",
            "title": "Message",
            "type": "object",
            "version": "0.5",
            "properties": {"statement": {"$ref": "#/definitions/statement"}},
            "required": ["statement"],
            "definitions": {}
        }
        # First add types
        _result["definitions"].update(self._get_child_types())

        # First, Add parameter types
        for _curr_class in list_parameter_classes():
            _result["definitions"].update({_curr_class: {
                "type": "object",
                "properties": self._add_child_property(_curr_class)}})

        # Then add verbs.

        for _curr_class in list_verb_classes():
            _result["definitions"].update({_curr_class: {
                "type": "object",
                "properties": self._add_child_property(_curr_class)}})

        return _result

    def _list_to_dict(self, _list):
        _result = []
        for _curr_item in _list:
            if isinstance(_curr_item, ParameterBase) or isinstance(_curr_item, CustomDataset) :
                _result.append(self._object_to_dict(_curr_item))
            else:
                _result.append(_curr_item)
        return _result



    def _object_to_dict(self, _object):
        if _object is not None and _object != "":
            self._debug_print("_object_to_dict: Encoding " + _object.__class__.__name__)

            _content = None

            if hasattr(_object, "__dict__"):
                _content = {}
                for _curr_property_name, _curr_property_value in sorted(_object.__dict__.items()):
                    if not _curr_property_name.lower() in ['row_separator'] and \
                            not hasattr(_curr_property_value, '__call__') and _curr_property_name[0:1] != '_':

                        # Property is a list
                        if isinstance(_curr_property_value, list):
                            _content[_curr_property_name] = self._list_to_dict(_curr_property_value)
                        elif hasattr(_curr_property_value, 'as_sql'):
                            _content[_curr_property_name] = self._object_to_dict(_curr_property_value)
                        else:
                            _curr_type = sql_property_to_type(_curr_property_name,_json_ref="")
                            if str(_curr_property_value).isnumeric() and len(_curr_type) > 1:
                                _content[_curr_property_name] = _curr_type[1][_curr_property_value]
                            else:
                                if _curr_property_value is not None:
                                    _content[_curr_property_name] = str(_curr_property_value) # Do something about lowercase (_curr_type == "boolean"))
                                else:
                                    _content[_curr_property_name] = None
            elif isinstance(_object, list):

                _content = self._xml_encode_list(_document, _object_node, _object)
            else:
                _content = str(_object)

            return {_object.__class__.__name__: _content}


    def sql_structure_to_dict(self, _structure):
        """Translates an SQL structure into JSON"""

        # Recurse structure
        _json = self._object_to_dict(_structure)

        return {"statement": _json}

    def json_get_allowed_value(_value, _type):
        """Check if a value is allowed in a certain XML node"""


        if _value in _type[1] or _value == "":
            return _value
        # Check for correct string format.
        elif _value[0:8].lower() == "varchar(" and _value[8:len(value) - 1].isnumeric() and _value[len(value) - 1] == ")":
            return value
        else:
            raise Exception("json_get_allowed_value: " + str(_value) + " is not a valid value in a " + _type[0])

    def _parse_array_dict(self, _list, _parent_obj):
        self._go_down("_parse_array_dict")
        self._debug_print("_parse_array_dict: Parsing list")

        # Loop nodes and parse them.

        _result = []
        for _curr_item in _list:
            if isinstance(_curr_item, dict):
                for _curr_key, _curr_value in _curr_item.items():
                    _result.append(self._parse_attribute(_curr_key, _curr_value, _parent_obj))
            else:
                _result.append(_curr_item)

        self._get_up("_parse_array_dict")
        return _result

    def _parse_attribute(self, _attribute_name, _attribute_value, _parent_obj):
        self._go_down("_parse_class_json")

        self._debug_print("_parse_class_json: Parsing " + _attribute_name)


        # Check for base types.
        if _attribute_name.lower() in ['str', 'int', 'float', 'datetime']:
            return str(_attribute_value)

            # Find and instatiate the actual class.
        _obj, _obj_name = find_class(_attribute_name)

        if hasattr(_obj, 'as_sql'):
            _obj._parent = _parent_obj
            _obj._base_path = self.base_path
            self._debug_print(
                "_parse_class_json: Found matching Parameter class for " + _attribute_name + " : " + _obj_name)

        elif isinstance(_obj, list):
            # If this is a list, parse it and return.
            self._debug_print("_parse_class_json: Found matching list class for " + _attribute_name + " : " + _obj_name)
            return {_attribute_name : self._parse_array_dict(_attribute_value, _obj)}
        else:
            raise Exception("_parse_class_json: Could not find matching class : " + _obj_name)

        # Loop the object's properties
        for _curr_item_key, _curr_obj in _obj.__dict__.items():

            if _curr_item_key != 'row_separator':
                if _curr_item_key in _attribute_value:
                    _curr_value = _attribute_value[_curr_item_key]
                    self._debug_print("_parse_class_json: Parsing property " + _curr_item_key)

                    if isinstance(_curr_obj, list):
                        _obj.__dict__[_curr_item_key] = self._parse_array_dict(_curr_value, _obj)
                    else:
                        # Match the property to a type.
                        _curr_type = sql_property_to_type(_curr_item_key, _json_ref="")
                        if _curr_type[0].lower() in ['string', 'datatypes', 'boolean','integer', 'float', 'number']:
                            _obj.__dict__[_curr_item_key] = _curr_value
                        elif _curr_type[0:5] == 'verb_' or _curr_type[0:10] == 'parameter_':
                            raise Exception(
                                "_parse_class_xml_node: Strange VERB/PARAMETER happened parsing.. parent: " +
                                _parent.__class__.__name__ + "Class: " + _attribute_name + " Currtype: " + _curr_type)

                        elif len(_curr_type) > 1 and type(_curr_type[1]) == list:
                            # There are several possible types that can be children
                            if isinstance(_curr_value, dict):
                                _first_key, first_value = next(iter(_curr_value.items()))
                                _obj.__dict__[_curr_item_key] = self._parse_attribute(_first_key, first_value,
                                                                                      _curr_obj)
                            else:
                                # Base types doesn't have any children.
                                _obj.__dict__[_curr_item_key] = _curr_value

                        else:
                            _first_key, first_value = next(iter(_curr_value.items()))
                            _obj.__dict__[_curr_item_key] = self._parse_attribute(_first_key, first_value, _curr_obj)

        if self._resources and hasattr(_obj,
                                       'resource_uuid') and _obj.resource_uuid is not None and _obj.resource_uuid != '':
            _obj._resource = self._resources.get_resource(_obj.resource_uuid)
            self._debug_print("_parse_attribute: Added resource_uuid for " + _obj_name + ": " + _obj.resource_uuid,
                              1)

        self._get_up("_parse_attribute")
        return _obj


    def dict_to_sql_structure(self, _dict, _base_path = None):
        """Translates an XML file into a class structure"""



        if _base_path:
            self.base_path = os.path.dirname(_base_path)


        if "resources" in _dict:
            #_resources = _dict['resources']

            # Send XML here, since resources now uses lxml

            # self._resources = Resources(_resources_xml=_resources_node.toxml(), _base_path=_base_path)
            self._resources = None

        if "statement" in _dict:

            # There is always just one top verb
            for _curr_key, _curr_value in _dict["statement"].items():
                if _curr_key in verbs():
                    _result = self._parse_attribute(_curr_key, _curr_value, None)

            return _result
        else:
            raise Exception("json_to_sql_structure: No \"statement\"-attribute found.")