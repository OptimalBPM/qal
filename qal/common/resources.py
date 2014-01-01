"""
    Access functionality for resources.
    
    :copyright: Copyright 2010-2013 by Nicklas Börjesson
    :license: BSD, see LICENSE for details. 
"""
from qal.common.xml_utils import XML_Translation

from lxml import etree


def add_xml_subitem(_parent, _nodename, _nodetext):
    _curr_item = etree.SubElement(_parent, _nodename)
    _curr_item.text = _nodetext
    return _curr_item


def resource_types():
    """Returns a list of the supported resource types"""
    return ["CUSTOM", "FLATFILE", "MATRIX", "XPATH", "RDBMS"]

class Resource(object):
    """The resource class represents a QAL resource. 
    Could be any entity like a database server, flat file or a web page.
    Resources have a globally unique resource uuid"""
    
    uuid = None
    type = None
    caption = None
    
    data = {}
    
    def __init__(self):
        self.uuid = None
        self.type = None
        self.caption = None    
        self.data = {}
        
    def as_xml_node(self):
        """This function encode an XML structure into resource objects. Uses lxml as opposed to parse_xml()."""
        _resource = etree.Element("resource")
        _resource.set("caption", self.caption)
        _resource.set("type", self.type)
        _resource.set("uuid", self.uuid)
        # Loop data. Sorted to be predictable enough for testing purposes
        for _curr_data_key, _curr_data_value in sorted(self.data.items()):
            add_xml_subitem(_resource, _curr_data_key, _curr_data_value) 
        
        
        return _resource
    

class Resources(XML_Translation):
    '''
    The resource class provides access to resources available through either XML-definitions or callback functions.
    '''
    
    """Local list of resources"""
    local_resources = None
    """Callback method for external lookup"""
    external_resources_callback = None


    def __init__(self, _resources_node=None, _resources_xml=None, _external_resources_callback=None):
        '''
        The argument _resources_node is an XML node from which local resources are parsed. 
        The argument _external_resources_callback is a user provided callback function 
        that has the same arguments as the get_resource-function.  
        '''
       
        if _resources_node != None or _resources_xml != None:
            self.parse_xml(_resources_node, _resources_xml)
                    
    def get_resource(self, _uuid):
        """Returns the resource with the corresponding uuid"""
        
        _resource = None
        
        """Check local list"""
        if self.local_resources and _uuid in self.local_resources:
            _resource = self.local_resources[_uuid]
        
        """Lookup externally"""
        if (_resource == None) and (self.external_resources_callback):
            _resource = self.external_resources_callback(_uuid)
        
        if _resource == None:
            raise Exception("get_resource: Resource not found - uuid: " + _uuid)
        else:
            return _resource
     
    
    
    def parse_xml(self, _resources_node=None, _resources_xml=None):
        """Parses an XML structure into resource objects. Uses lxml."""


        if _resources_node == None:
            _parser = etree.XMLParser(remove_blank_text=True)
            _root_node = etree.fromstring(_resources_xml, _parser) 
            if _root_node.tag != "resources":
                _resources_node = _root_node.find("resources")
            else:
                _resources_node = _root_node            
            
        
        
        self.local_resources = dict()
        
        for _curr_resource_node in _resources_node.findall("resource"):
            
            if _curr_resource_node.get("uuid") != None:
                self._debug_print("parse_xml: Create new resource object")
                _new_resource = Resource()
                _new_resource.uuid = _curr_resource_node.get("uuid")
                _new_resource.type = _curr_resource_node.get("type")
                _new_resource.caption = _curr_resource_node.get("caption")
                
                for _curr_resource_data in _curr_resource_node.findall("*"):
                    _new_data = []
                    if len(_curr_resource_data.findall("item")) > 0:
                        for _curr_item in _curr_resource_data.findall("item"):
                            _new_data.append(_curr_item.text)
                        _new_resource.data[str(_curr_resource_data.tag).lower()] = _new_data
                        self._debug_print("parse_xml: Add datas "+ str(_curr_resource_data.tag).lower() + " " +  str(_new_resource.data[str(_curr_resource_data.tag).lower()]) , 1)
                    else:
                        _new_resource.data[str(_curr_resource_data.tag).lower()] = _curr_resource_data.text
                        self._debug_print("parse_xml: Add data "+ str(_curr_resource_data.tag).lower() + " " + str(_new_resource.data[str(_curr_resource_data.tag).lower()]) , 1)
                        
            
                self.local_resources[_new_resource.uuid] = _new_resource
                self._debug_print("parse_xml: Append resource: "+_new_resource.caption + " uuid: " + _new_resource.uuid + " type: " + _new_resource.type  , 4)
    def as_xml_node(self):
        """This function encode resources structure into an XML structure."""
        _xml_node = etree.Element("resources")
        # Loop resources. Sorted to be predictable enough for testing purposes
        for _curr_resource_key, _curr_resource_value in sorted(self.local_resources.items()):
            _xml_node.append(_curr_resource_value.as_xml_node())
            
        return _xml_node