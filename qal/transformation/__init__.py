"""The transformation module holds functionality for transforming data."""
__author__ = 'nibo'

from qal.common.meta import list_prefixed_classes
from qal.common.json import json_add_child_properties
from qal import __version__

from qal.transformation.merge import Merge
from qal.transformation.substitution import Substitution
from qal.transformation.mapping import Mapping
from qal.transformation.transform import Trim, IfEmpty, Cast, Replace, ReplaceRegex


def generate_schema():
    """Generates an JSON schema based on the class structure in SQL.py"""

    _result = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The JSON Schema for QAL resources",
        "title": "QAL Transformations",
        "type": "object",
        "version": __version__,
        "properties": {},
        "namespace": "qal",
        "definitions": {}
    }

    def _property_to_type(_property_name):
        if _property_name == "uuid":
            return [{
                "type": "string",
                "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
            }]
        elif _property_name == "mappings":
            return [{
                "type": "array",
                "items": {
                    "$ref": "#/definitions/Mapping"
                }
            }]
        elif _property_name == "substitution":
            return [{"$ref": "#/definitions/Substitution"}]

        elif _property_name == "resources":
            return [{
                "type": "array",
                "items": {
                    "$ref": "qal://resources.json#/definitions/Resource"
                }
            }]
        elif _property_name == "resources":
            return [{
                "type": "array",
                "items": {
                    "$ref": "qal://resources.json#/definitions/Resource"
                }
            }]
        elif _property_name in ["builtin_substitutions", "key_fields", "destination_log_level", "key_fields",
                                "source", "destination"]:
            # Disregard these fields
            return None
        elif _property_name in ["delete", "insert", "update", "is_key"]:
            # Disregard these fields
            return [{"type": "boolean"}]
        else:
            return [{"type": "string"}]

    # First, Add parameter types
    for _curr_class in list_prefixed_classes(globals(), "", _exclude=[]):
        _result["definitions"].update({_curr_class: {
            "type": "object",
            "properties": json_add_child_properties(globals(), _curr_class, _property_to_type)
        }
        })

    return _result
