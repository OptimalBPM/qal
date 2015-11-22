"""
Created on Nov 22, 2012

@author: Nicklas Boerjesson

"""
from csv import list_dialects
import os

from qal.sql.meta import list_class_properties, list_parameter_classes, list_verb_classes, find_class
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
        return {
            "type": "array",
            "minItems": 1,
            "items": {"type": "object", "oneOf": _types},
            "uniqueItems": True
        }


    def get_child_types(self):
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
        _result["statement"] = {"type": "object","oneOf": verbs()}
        _result["condition_part"] = {"type": "object","oneOf": condition_part()}
        _result["tabular_expression_item"] = {"type": "object","oneOf": tabular_expression_item_types()}

        _result["Array_ParameterString"] = self._child_array_of(['ParameterString'])
        _result["Array_ParameterConstraint"] = self._child_array_of(['ParameterConstraint'])
        _result["Array_ParameterColumndefinition"] = self._child_array_of(['ParameterColumndefinition'])
        _result["Array_ParameterSource"] = self._child_array_of(['ParameterSource'])
        _result["Array_ParameterWhen"] = self._child_array_of(['ParameterWhen'])
        _result["Array_ParameterIdentifier"] = self._child_array_of(['ParameterIdentifier'])
        _result["Array_Statement"] = self._child_array_of(['statement'])
        _result["Array_ParameterOrderByItem"] = self._child_array_of(['ParameterOrderByItem'])
        _result["Array_ParameterCondition"] = self._child_array_of(['ParameterCondition'])
        _result["Array_ParameterField"] = self._child_array_of(['ParameterField'])
        _result["Array_ParameterAssignment"] = self._child_array_of(['ParameterAssignment'])
        _result["Array_expression_item"] = self._child_array_of(expression_item_types())
        _result["Array_tabular_expression_item"] = self._child_array_of(tabular_expression_item_types())
        _result["Array_list"] = self._child_array_of(['*'])

        return _result

    def generate_schema(self):
        """Generates an JSON schema based on the class structure in SQL.py"""

        _result = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "description": "QAL SQL Schema",
            "title": "Message",
            "type": "object",
            "version": "0.5",
            "properties": {},
            "required": [],
            "definitions": {}
        }

        _result["definitions"].update(self.get_child_types())

        return _result
