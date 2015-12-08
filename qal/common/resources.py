"""
    Access functionality for resources.
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details. 
"""
from urllib.parse import unquote
import os

from lxml import etree

from qal.common.xml_utils import XMLTranslation
from qal.common.meta import list_prefixed_classes, _json_add_child_properties
from qal.recurse import Recurse


def add_xml_subitem(_parent, _nodename, _nodetext):
    _curr_item = etree.SubElement(_parent, _nodename)
    _curr_item.text = _nodetext
    return _curr_item


def resource_types():
    """Returns a list of the QAL-supported resource types"""
    return ["CUSTOM", "FLATFILE", "MATRIX", "XPATH", "RDBMS"]

def generate_schema():
    """Generates an JSON schema based on the class structure in SQL.py"""

    _result = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The JSON Schema for QAL resources",
        "title": "QAL Resources",
        "type": "object",
        "version": "0.5",
        "properties": {"resources": {"$ref": "#/definitions/Resource"}},
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
            "properties": _json_add_child_properties(globals(), _curr_class, _property_to_type)}})

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
    """If this resource was read from an XML, base path is the directory where the XML was located.
    Makes it possible to deduce the absolute path from relative paths in the XML files,
    making them slightly more portable."""
    data = None
    """A dict of all the custom data that belongs to the resource. Access this way: _filename = data['file_name'].'"""

    def __init__(self):
        self.uuid = None
        self.type = None
        self.caption = None
        self.base_path = None
        self.data = {}

    def as_json_dict(self):
        """This function encode an XML structure into resource objects. Uses lxml as opposed to parse_xml()."""
        _resource = {}
        _resource["name"] = self.caption
        _resource["type"] = self.type
        _resource["uuid"] = self.uuid
        # Loop data. Sorted to be predictable enough for testing purposes
        for _curr_data_key, _curr_data_value in sorted(self.data.items()):
            if _curr_data_value is not None:
                if isinstance(_curr_data_value, list):
                    _resource[_curr_data_key] = []
                    for _curr_item in _curr_data_value:
                        _resource[_curr_data_key].append(str(_curr_item))
                else:
                    _resource[_curr_data_key] = _curr_data_value

        return _resource

    def as_xml_node(self):
        """This function encode an XML structure into resource objects. Uses lxml as opposed to parse_xml()."""
        _resource = etree.Element("resource")
        _resource.set("caption", self.caption)
        _resource.set("type", self.type)
        _resource.set("uuid", self.uuid)
        # Loop data. Sorted to be predictable enough for testing purposes
        for _curr_data_key, _curr_data_value in sorted(self.data.items()):
            if _curr_data_value is not None:
                if isinstance(_curr_data_value, list):
                    _item_parent = add_xml_subitem(_resource, _curr_data_key, "")
                    for _item in _curr_data_value:
                        add_xml_subitem(_item_parent, "item", str(_item))
                else:
                    add_xml_subitem(_resource, _curr_data_key, str(_curr_data_value))

        return _resource



class Resources(Recurse):
    """
    The resource class provides access to resources available through either XML-definitions or callback functions.
    """

    local_resources = None
    """Local list of resources"""
    external_resources_callback = None
    """Callback method for external lookup"""

    base_path = None
    """If resource was loaded from a file, its path is stores here, useful when translating relative paths."""

    def __init__(self, _resources_node=None, _resources_xml=None, _resources_json_dict=None, _external_resources_callback=None,
                 _base_path=None):
        """
            The argument _resources_node is an XML node from which local resources are parsed.
            The argument _external_resources_callback is a user provided callback function, not implemented.
            that has the same arguments as the get_resource-function.
        """
        super(Resources).__init__()
        self.local_resources = dict()
        if _external_resources_callback:
            raise Exception("_external_resources_callback is not implemented")
        if _base_path is not None:
            self.base_path = os.path.dirname(_base_path)
        else:
            self.base_path = None
        if _resources_node is not None or _resources_xml is not None:
            self.parse_xml(_resources_node, _resources_xml)
        if _resources_json_dict is not None:
            self.parse_json(_resources_json_dict)

    def get_resource(self, uuid):
        """Returns the resource with the corresponding uuid"""

        _resource = None

        """Check local list"""
        if self.local_resources and uuid in self.local_resources:
            _resource = self.local_resources[uuid]

        """Lookup externally"""
        if (_resource is None) and self.external_resources_callback:
            _resource = self.external_resources_callback(uuid)

        if _resource is None:
            raise Exception("get_resource: Resource not found - uuid: " + uuid)
        else:
            return _resource

    def parse_json(self, _resources_dict=None):
        """Parses an dict structure into resource objects"""



        self.local_resources = dict()

        for _curr_key, _curr_resource in _resources_dict.items():
            if "uuid" in _curr_resource:
                self._debug_print("Resources.parse_json: Create new resource object")
                _new_resource = Resource()
                _new_resource.uuid = _curr_resource["uuid"]
                _new_resource.type = _curr_resource["type"]
                _new_resource.caption = _curr_resource["name"]
                _new_resource.base_path = self.base_path

                for _curr_resource_key, _curr_resource_value in _curr_resource.items():
                    _new_data = []
                    if isinstance(_curr_resource_value, list):
                        for _curr_item in _curr_resource_value:
                            _new_data.append(_curr_item)
                        _new_resource.data[_curr_resource_key] = _new_data
                    else:
                        _new_resource.data[_curr_resource_key] = _curr_resource_value


                self.local_resources[_new_resource.uuid] = _new_resource
                self._debug_print(
                    "parse_xml: Append resource: " + _new_resource.caption + " uuid: " + _new_resource.uuid +
                    " type: " + _new_resource.type, 4)

    def parse_xml(self, _resources_node=None, _resources_xml=None):
        """Parses an XML structure into resource objects. Uses lxml."""

        if _resources_node is None:
            _parser = etree.XMLParser(remove_blank_text=True)
            _root_node = etree.fromstring(_resources_xml, _parser)
            if _root_node.tag != "resources":
                _resources_node = _root_node.find("resources")
            else:
                _resources_node = _root_node

        if _resources_node.base:
            self.base_path = os.path.dirname(unquote(_resources_node.base.replace('file:///', '')))

        self.local_resources = dict()

        for _curr_resource_node in _resources_node.findall("resource"):

            if _curr_resource_node.get("uuid") is not None:
                self._debug_print("parse_xml: Create new resource object")
                _new_resource = Resource()
                _new_resource.uuid = _curr_resource_node.get("uuid")
                _new_resource.type = _curr_resource_node.get("type")
                _new_resource.caption = _curr_resource_node.get("caption")
                _new_resource.base_path = self.base_path

                for _curr_resource_data in _curr_resource_node.findall("*"):

                    if len(_curr_resource_data.findall("item")) > 0:
                        _new_data = []
                        for _curr_item in _curr_resource_data.findall("item"):
                            _new_data.append(_curr_item.text)
                        _new_resource.data[str(_curr_resource_data.tag).lower()] = _new_data
                        self._debug_print("parse_xml: Add datas " + str(_curr_resource_data.tag).lower() + " " + str(
                            _new_resource.data[str(_curr_resource_data.tag).lower()]), 1)
                    else:
                        _new_resource.data[str(_curr_resource_data.tag).lower()] = _curr_resource_data.text
                        self._debug_print("parse_xml: Add data " + str(_curr_resource_data.tag).lower() + " " + str(
                            _new_resource.data[str(_curr_resource_data.tag).lower()]), 1)

                self.local_resources[_new_resource.uuid] = _new_resource
                self._debug_print(
                    "parse_xml: Append resource: " + _new_resource.caption + " uuid: " + _new_resource.uuid +
                    " type: " + _new_resource.type, 4)

    def as_xml_node(self):
        """This function encode resources structure into an XML structure."""
        _xml_node = etree.Element("resources")
        # Loop resources. Sorted to be predictable enough for testing purposes
        for _curr_resource_key, _curr_resource_value in sorted(self.local_resources.items()):
            _xml_node.append(_curr_resource_value.as_xml_node())

        return _xml_node

    def as_json_dict(self):
        """This function encode resources structure into an XML structure."""
        _result = {}
        # Loop resources. Sorted to be predictable enough for testing purposes
        for _curr_resource_key, _curr_resource_value in sorted(self.local_resources.items()):
            _result[_curr_resource_key] =_curr_resource_value.as_json_dict()

        return _result