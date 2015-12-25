"""
Created on Nov 22, 2012

@author: Nicklas Boerjesson

"""
from csv import list_dialects

from qal.common.resources import Resources
from qal.dataset.xpath import xpath_data_formats
from qal.common.meta import list_prefixed_classes, find_class
from qal.common.schema import json_add_child_properties
from qal.common.recurse import Recurse
from qal.sql.types import sql_property_to_type, and_or, \
    constraint_types, index_types, verbs, set_operator, join_types, in_types, quoting_types, data_source_types
from qal.dal.types import db_types


# Imported for class resolution

from qal.sql.sql import *  # @UnusedWildImport #IGNORE:W0401
from qal.sql.base import * # @UnusedWildImport #IGNORE:W0401
from qal.dataset.custom import CustomDataset # @UnusedWildImport #IGNORE:W0401


# IMPORTANT, there should be imports of all qal.dataset.* modules above for generate schema to work

class SQLJSON(Recurse):
    """
    This class converts the SQL structure(see SQL.py) into a JSON-compatible dict and back.
    """

    """Encoding of the document"""
    encoding = 'utf-8'

    """The base path of the document. Allows for relative paths within."""
    base_path = None

    """The resources"""
    _resources = None


    def _child_array_of(self, _types):
        if len(_types) > 1 or isinstance(_types, dict):
            return {
                "type": "array",
                "minItems": 0,
                "items":_types,
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
        _result["quoting"] = {"type": "string", "enum": quoting_types()}

        _result["csv_dialects"] = {"type": "string", "enum": list_dialects()}
        _result["join_types"] = {"type": "string", "enum": join_types()}

        _result["xpath_data_format"] = {"type": "string", "enum": xpath_data_formats()}

        def make_one_of(_classes):
            return {"type": "object",
                    "anyOf":
                         [{"properties": {x: {"$ref": "#/definitions/" + x}}} for x in _classes]
                    }

        _result["statement"] = make_one_of(verbs())
        _result["condition_part"] = make_one_of(condition_part())
        _result["TabularExpressionItem"] = make_one_of(tabular_expression_item_types())
        _result["data_source_types"] = make_one_of(data_source_types())

        _result["ArrayString"] = {"type": "array", "items": {"type": "string"}}

        _result["ArrayParameterString"] = self._child_array_of(['#/definitions/ParameterString'])
        _result["ArrayParameterConstraint"] = self._child_array_of(['#/definitions/ParameterConstraint'])
        _result["ArrayParameterColumndefinition"] = self._child_array_of(['#/definitions/ParameterColumndefinition'])
        _result["ArrayParameterSource"] = self._child_array_of(['#/definitions/ParameterSource'])
        _result["ArrayParameterWhen"] = self._child_array_of(['#/definitions/ParameterWhen'])
        _result["ArrayParameterIdentifier"] = self._child_array_of(['#/definitions/ParameterIdentifier'])
        _result["ArrayStatement"] = self._child_array_of(['#/definitions/statement'])
        _result["ArrayParameterOrderByItem"] = self._child_array_of(['#/definitions/ParameterOrderByItem'])
        _result["ArrayParameterCondition"] = self._child_array_of(['#/definitions/ParameterCondition'])
        _result["ArrayParameterField"] = self._child_array_of(['#/definitions/ParameterField'])
        _result["ArrayParameterAssignment"] = self._child_array_of(['#/definitions/ParameterAssignment'])
        _result["ArrayExpressionItem"] = self._child_array_of(make_one_of(expression_item_types()))
        _result["ArrayTabularExpressionItem"] = self._child_array_of(make_one_of(tabular_expression_item_types()))
        _result["ArrayList"] = self._child_array_of(['*'])

        return _result



    def generate_schema(self):
        """Generates an JSON schema based on the class structure in SQL.py"""

        _result = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "description": "The JSON Schema for QAL SQL settings",
            "title": "QAL SQL JSON Schema",
            "type": "object",
            "version": "0.5",
            "properties": {
                "statement": {"$ref": "#/definitions/statement"},
                "resources": {"$ref": "qal://resources.json#/properties/resources"}
                           },
            "required": ["statement"],
            "definitions": {}
        }
        # First add types
        _result["definitions"].update(self._get_child_types())


        def json_sql_property_to_type(_curr_property):
            return sql_property_to_type(_curr_property, _json_ref='#/definitions/')


        _globals = globals()
        # First, Add parameter types
        for _curr_class in list_prefixed_classes(globals(), "parameter"):
            _result["definitions"].update({_curr_class: {
                "type": "object",
                "properties": json_add_child_properties(_globals, _curr_class, json_sql_property_to_type)}})

        # Then add verbs.

        for _curr_class in list_prefixed_classes(globals(), "verb"):
            _result["definitions"].update({_curr_class: {
                "type": "object",
                "properties": json_add_child_properties(_globals, _curr_class, json_sql_property_to_type)}})

        return _result

    def _list_to_dict(self, _list):
        _result = []
        for _curr_item in _list:
            if isinstance(_curr_item, ParameterBase) or isinstance(_curr_item, CustomDataset):
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
                            _curr_type = sql_property_to_type(_curr_property_name, _json_ref="")
                            if str(_curr_property_value).isnumeric() and len(_curr_type) > 1:
                                _content[_curr_property_name] = _curr_type[1][_curr_property_value]
                            else:
                                if _curr_property_value is not None:
                                    _content[_curr_property_name] = str(
                                        _curr_property_value)  # Do something about lowercase (_curr_type == "boolean"))
                                else:
                                    _content[_curr_property_name] = None
            elif isinstance(_object, list):

                _content = self._xml_encode_list(_document, _object_node, _object)
            else:
                _content = str(_object)

            return {_object.__class__.__name__: _content}

    def sql_structure_to_dict(self, _statement, _resources = None):
        """Translates an SQL structure into JSON"""

        # Recurse structure
        _statement = self._object_to_dict(_statement)
        if _resources is not None:

            return {"statement": _statement, "resources": _resources.as_json_dict()}
        else:
            return {"statement": _statement}

    def json_get_allowed_value(_value, _type):
        """Check if a value is allowed in a certain XML node"""

        if _value in _type[1] or _value == "":
            return _value
        # Check for correct string format.
        elif _value[0:8].lower() == "varchar(" and _value[8:len(value) - 1].isnumeric() and _value[
                    len(value) - 1] == ")":
            return value
        else:
            raise Exception("json_get_allowed_value: " + str(_value) + " is not a valid value in a " + _type[0])

    def _parse_array_dict(self, _list, _parent_obj, _destination = []):
        self._go_down("_parse_array_dict")
        self._debug_print("_parse_array_dict: Parsing list")

        # Loop nodes and parse them.

        for _curr_item in _list:
            if isinstance(_curr_item, dict):
                for _curr_key, _curr_value in _curr_item.items():
                    _destination.append(self._parse_attribute(_curr_key, _curr_value, _parent_obj))
            else:
                _destination.append(_curr_item)

        self._get_up("_parse_array_dict")
        return _destination

    def _parse_attribute(self, _attribute_name, _attribute_value, _parent_obj):
        self._go_down("_parse_class_json")

        self._debug_print("_parse_class_json: Parsing " + _attribute_name)


        # Check for base types.
        if _attribute_name.lower() in ['str', 'int', 'float', 'datetime']:
            return str(_attribute_value)

            # Find and instatiate the actual class.
        _obj, _obj_name = find_class(globals(), _attribute_name)

        if hasattr(_obj, 'as_sql'):
            _obj._parent = _parent_obj
            _obj._base_path = self.base_path
            self._debug_print(
                "_parse_class_json: Found matching Parameter class for " + _attribute_name + " : " + _obj_name)

        elif isinstance(_obj, list):
            # If this is a list, parse it and return.
            self._debug_print("_parse_class_json: Found matching list class for " + _attribute_name + " : " + _obj_name)
            return {_attribute_name: self._parse_array_dict(_attribute_value, _obj)}
        else:
            raise Exception("_parse_class_json: Could not find matching class : " + _obj_name)

        # Loop the object's properties
        for _curr_item_key, _curr_obj in _obj.__dict__.items():

            if _curr_item_key != 'row_separator':
                if _curr_item_key in _attribute_value:
                    _curr_value = _attribute_value[_curr_item_key]
                    self._debug_print("_parse_class_json: Parsing property " + _curr_item_key)

                    if isinstance(_curr_obj, list):
                        self._parse_array_dict(_curr_value, _obj, _curr_obj)
                    else:
                        # Match the property to a type.
                        _curr_type = sql_property_to_type(_curr_item_key, _json_ref="")
                        if _curr_type[0].lower() in ['string', 'datatypes', 'boolean', 'integer', 'float', 'number']:
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

    def dict_to_sql_structure(self, _dict, _base_path=None):
        """Translates an JSON dict into a class structure"""

        if _base_path:
            self.base_path = _base_path

        if "resources" in _dict:
            # _resources = _dict['resources']

            # Send XML here, since resources now uses lxml

            self._resources = Resources(_resources_json_dict=_dict["resources"], _base_path=_base_path)

        if "statement" in _dict:

            # There is always just one top verb
            for _curr_key, _curr_value in _dict["statement"].items():
                if _curr_key in verbs():
                    _result = self._parse_attribute(_curr_key, _curr_value, None)

            return _result
        else:
            raise Exception("json_to_sql_structure: No \"statement\"-attribute found.")
