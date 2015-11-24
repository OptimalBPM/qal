"""
Created on Nov 22, 2012

@author: Nicklas Boerjesson

"""
from csv import list_dialects

from qal.sql.meta import list_class_properties, list_parameter_classes, list_verb_classes
from qal.sql.types import sql_property_to_type, and_or, \
    constraint_types, index_types, verbs, expression_item_types, \
    condition_part, set_operator, tabular_expression_item_types, data_source_types
from qal.dal.types import db_types


class SQLJSON():
    """
    This class converts XML into a class structure(declare in SQL.py) that holds the statements.
    """

    """Encoding of the document"""
    encoding = 'utf-8'

    """The base path of the document. Allows for relative paths within."""
    base_path = None

    """The resources"""
    _resources = None

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
        _result["index_types"] = {"type": "string", "enum": index_types()}
        _result["constraint_types"] = {"type": "string", "enum": constraint_types()}
        _result["set_operator"] = {"type": "string", "enum": set_operator()}
        _result["data_source"] = {"type": "string", "enum": data_source_types()}
        _result["csv_dialects"] = {"type": "string", "enum": list_dialects()}

        _result["csv_dialects"] = {"type": "string", "enum": list_dialects()}
        _result["statement"] = {"type": "object", "oneOf": [{"$ref": "#/definitions/" + x} for x in verbs()]}
        _result["condition_part"] = {"type": "object",
                                     "oneOf": [{"$ref": "#/definitions/" + x} for x in condition_part()]}
        _result["tabular_expression_item"] = {"type": "object", "oneOf": [{"$ref": "#/definitions/" + x} for x in
                                                                          tabular_expression_item_types()]}

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
