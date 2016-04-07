"""
    Access functionality for resources.
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details. 
"""

from qal.common.meta import list_prefixed_classes
from qal.common.json import json_add_child_properties
from qal.common.recurse import Recurse
from qal import __version__



def resource_types():
    """Returns a list of the QAL-supported resource types"""
    return ["CUSTOM", "FLATFILE", "MATRIX", "XPATH", "RDBMS", "SPREADSHEET"]
    # TODO: See to it that spreadsheets are added as resource types everywhere

def generate_schema():
    """Generates an JSON schema based on the class structure in SQL.py"""

    _result = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The JSON Schema for QAL resources",
        "title": "QAL Resources",
        "type": "object",
        "version": __version__,
        "properties": {
            "resources":
                {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/Resource"
                        }
                }
        },
        "namespace": "qal",
        "required": ["resources"],
        "definitions": {

        }
    }

    def _property_to_type(_property_name):
        if _property_name == "uuid":
            return [{
                        "type": "string",
                        "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
                    }]
        else:
            return [{
                        "type": "string"
            }]
    # First, Add parameter types
    for _curr_class in list_prefixed_classes(globals(), "resource", _exclude=["Resources"]):
        _result["definitions"].update({_curr_class: {
            "type": "object",
            "properties": json_add_child_properties(globals(), _curr_class, _property_to_type),
            "additionalProperties": True
            }
        })

    return _result

class Resource(object):
    """The resource class represents a QAL resource.
    Could be any entity like a database server, flat file or a web page.
    Resources have a globally unique resource uuid"""

    uuid = None
    """The unique identifier of the resources. Used to access the resource, wherever its data is stored."""
    type = None
    """The type of the resources. Possible current values: "CUSTOM", "FLATFILE", "MATRIX", "XPATH", "RDBMS"."""
    caption = None
    """The "friendly" name of the resource. For example "Database BDD04"."""
    base_path = None
    """If this resource was read from a file, base path is the directory where the file was located.
    Makes it possible to deduce the absolute path from relative paths in the files,
    making them slightly more portable."""
    _excluded_fields = None
    """This private field lists the fields that aren't persistent and not part of the definition"""


    def __init__(self):
        self.uuid = None
        self.type = None
        self.name = None
        self.base_path = None
        self._excluded_fields = ["base_path"]


    def as_json_dict(self):
        """This function encode an JSON structure into resource objects. """
        _resource = {}

        # Loop data. Sorted to be predictable enough for testing purposes
        for _curr_data_key, _curr_data_value in sorted(self.__dict__.items()):
            if _curr_data_value is not None and\
                            _curr_data_key[0] != "_" and \
                            _curr_data_key not in self._excluded_fields:
                if isinstance(_curr_data_value, list):
                    _resource[_curr_data_key] = []
                    for _curr_item in _curr_data_value:
                        _resource[_curr_data_key].append(str(_curr_item))
                else:
                    _resource[_curr_data_key] = _curr_data_value

        return _resource



class Resources(Recurse):
    """
    The resource class provides access to resources available through either JSON-definitions or callback functions.
    """



    external_resources_callback = None
    """Callback method for external lookup"""

    base_path = None
    """If resource was loaded from a file, its path is stores here, useful when translating relative paths."""

    """Local list of resources"""
    _local_resources = None
    """Short cuts to the positions in the array"""
    _local_resources_short_cuts = None



    def __init__(self, _resources_list=None, _external_resources_callback=None,
                 _base_path=None):
        """
            The argument _resources_list is an dict from which local resources are parsed.
            The argument _external_resources_callback is a user provided callback function, not implemented.
            that has the same arguments as the get_resource-function.
        """
        super(Resources).__init__()
        self._local_resources = []
        self._local_resources_short_cuts = {}

        if _external_resources_callback:
            raise Exception("_external_resources_callback is not implemented")
        self.base_path = _base_path

        if _resources_list is not None:
            self.parse_json(_resources_list)

    def get_resource(self, uuid):
        """Returns the resource with the corresponding uuid"""

        _resource = None

        """Check local list"""
        if self._local_resources and uuid in self._local_resources_short_cuts:
            _resource = self._local_resources[self._local_resources_short_cuts[uuid]]

        """Lookup externally"""
        if (_resource is None) and self.external_resources_callback:
            _resource = self.external_resources_callback(uuid)

        if _resource is None:
            raise Exception("get_resource: Resource not found - uuid: " + uuid)
        else:
            return _resource

    def parse_json(self, _resources_list=None):
        """Parses a list structure into resource objects"""

        self._local_resources = []
        self._local_resources_short_cuts = {}
        for _curr_resource in _resources_list:
            if "uuid" in _curr_resource:
                self._debug_print("Resources.parse_json: Create new resource object")
                _new_resource = Resource()

                for _curr_resource_key, _curr_resource_value in _curr_resource.items():

                    if isinstance(_curr_resource_value, list):
                        _new_data = []
                        for _curr_item in _curr_resource_value:
                            _new_data.append(_curr_item)
                        _new_resource.__dict__[_curr_resource_key] = _new_data
                    else:
                        _new_resource.__dict__[_curr_resource_key] = _curr_resource_value

                _new_resource.base_path = self.base_path
                self[_new_resource.uuid] = _new_resource

                self._debug_print(
                    "parse_json: Append resource: " + str(_new_resource.caption) + " uuid: " + str(_new_resource.uuid) +
                    " type: " + str(_new_resource.type), 4)

    def as_json_dict(self):
        """This function encode resources structure into a JSON dict structure."""
        _result = []
        # Loop resources. Sorted to be predictable enough for testing purposes
        for _curr_resource in self._local_resources:
            _result.append(_curr_resource.as_json_dict())

        return _result


    def __getitem__(self, item):
        return self.get_resource(item)

    def __setitem__(self, key, value):
        self._local_resources.append(value)
        self._local_resources_short_cuts[key] =  len(self._local_resources) - 1