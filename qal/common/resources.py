"""
Created on Sep 13, 2013

@author: Nicklas Börjesson
@note: This module contains access functionality for resources.
"""
from qal.common.xml_utils import XML_Translation, xml_get_text, find_child_node


def resource_types():
    """Returns a list of the supported resource types"""
    return ["CUSTOM", "FLATFILE", "MATRIX", "XPATH", "RDBMS"]

class Resource(object):
    
    uuid = None
    type = None
    caption = None
    
    data = {}
    
    def __init__(self):
        self.uuid = None
        self.type = None
        self.caption = None    
        self.data = {}
    

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
        
        if _resources_node == None:
            _root_node = self.get_root_node('sql:statement', _resources_xml, _resources_node)
            _resources_node = find_child_node(_root_node, "resources")
        
        
        self.local_resources = dict()
        
        for _curr_resource_node in _resources_node.childNodes:
            
            if _curr_resource_node.nodeType == _curr_resource_node.ELEMENT_NODE and _curr_resource_node.getAttribute("uuid") != '':
                self._debug_print("parse_xml: Create new resource object")
                _new_resource = Resource()
                _new_resource.uuid = _curr_resource_node.getAttribute("uuid")
                _new_resource.type = _curr_resource_node.getAttribute("type")
                _new_resource.caption = _curr_resource_node.getAttribute("caption")
                
                for _curr_resource_data in _curr_resource_node.childNodes:
                    if _curr_resource_data.nodeType == _curr_resource_data.ELEMENT_NODE:
                        if _curr_resource_data.getElementsByTagName("item").length > 0:
                            _new_data = []
                            for _curr_item in _curr_resource_data.childNodes:
                                if _curr_item.nodeType == _curr_item.ELEMENT_NODE:
                                    _new_data.append(xml_get_text(_curr_item))
                            _new_resource.data[_curr_resource_data.nodeName.lower()] = _new_data
                            self._debug_print("parse_xml: Add datas "+ _curr_resource_data.nodeName.lower() + " " +  str(_new_resource.data[_curr_resource_data.nodeName.lower()]) , 1)
                                    
                        else:
                            _new_resource.data[_curr_resource_data.nodeName.lower()] = xml_get_text(_curr_resource_data)
                            self._debug_print("parse_xml: Add data "+ _curr_resource_data.nodeName.lower() + " " + _new_resource.data[_curr_resource_data.nodeName.lower()] , 1)
                        
            
                self.local_resources[_new_resource.uuid] = _new_resource
                self._debug_print("parse_xml: Append resource: "+_new_resource.caption + " uuid: " + _new_resource.uuid + " type: " + _new_resource.type  , 4)
    