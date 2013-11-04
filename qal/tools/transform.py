"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""

class Custom_Transformation(object):
    """
    This is the base class for all transformations
    """

    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        
        if _xml_node != None:

            self.load_from_xml_node(_xml_node)
        
        
    def load_from_xml_node(self, _xml_node):
        raise Exception("Custom_Transformation.load_from_xml_node : Not implemented in base class.")    
    
    def transform(self, _value):
        """Make transformation"""
        raise Exception("Custom_Transformation.transform : Not implemented in base class.")            


class Trim(Custom_Transformation):
    leftright = None
    
    def load_from_xml_node(self, _xml_node):
        self.leftright = _xml_node.text

    def transform(self, _value):
        """Make transformation"""
        if self.leftright == "left":
            return _value.lstrip()
        elif self.leftright =="right":
            return _value.rstrip()
        else:
            return _value.strip()
        
        raise Exception("Custom_Transformation.transform : Not implemented in base class.")           