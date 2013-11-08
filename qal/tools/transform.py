"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
from lxml import etree
from datetime import date, datetime



def isnone( _node):
    if _node == None or _node.text == None:
        return ''
    else:
        return _node.text  

class Custom_Transformation(object):
    """
    This is the base class for all transformations
    """
    
    """Order dictates when the transformation is run."""
    order = None
    
    def __init__(self, _xml_node = None):
        
        """
        Constructor
        """
        
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)
            
    def init_base_to_node(self, _name):
        _xml_node = etree.Element(_name)
        _xml_node.set("order", self.order)
        return _xml_node
            
    def as_xml_node(self):
        raise Exception("Custom_Transformation.as_xml_node : Should not be called. Not implemented in base class, use init_base_to_node().")      
        
    def load_from_xml_node(self, _xml_node = None):
        if _xml_node != None:
            self.order = _xml_node.get("order")
        else:
            raise Exception("Custom_Transformation.load_from_xml_node : Base class need a destination node.")  
    
    def transform(self, _value):
        """Make transformation"""
        raise Exception("Custom_Transformation.transform : Not implemented in base class.")            


class Trim(Custom_Transformation):
    """Trim returns a copy of the string in which all chars have been trimmed from the beginning and the end of the string (default whitespace characters).
    If the value parameter is set to either "beginning" or "end", only the left or right end of the string is trimmed, respectively."""
    value = None
    
    def load_from_xml_node(self, _xml_node):
        super(Trim, self ).load_from_xml_node(_xml_node)
        self.value = _xml_node.text
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("trim")
        _xml_node.text = self.value
        
        return _xml_node

    def transform(self, _value):
        """Make transformation"""
        if self.value == "beginning":
            return _value.lstrip()
        elif self.value =="end":
            return _value.rstrip()
        else:
            return _value.strip()
        
class Is_null(Custom_Transformation):
    """Is_null returns a specified value if the input value is NULL."""
    value = None
    
    def load_from_xml_node(self, _xml_node):
        super(Is_null, self ).load_from_xml_node(_xml_node)
        self.value = _xml_node.text
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("is_null")
        _xml_node.text = self.value
        
        return _xml_node

    def transform(self, _value):
        """Make transformation"""
        if _value == None:
            return self.value
        else:
            return _value
        
class Cast(Custom_Transformation):
    """ICasts a string to the specified type. The timestamp date format defaults to the ISO format if format_string is not set.\n
    Possible format string directives at : http://docs.python.org/3.2/library/datetime.html#strftime-strptime-behavior\n
    For example, 2013-11-06 22:05:42 is "%Y-%m-%d %H:%M:%S".
    """
    
    dest_type = None
    format_string = None
    
    def load_from_xml_node(self, _xml_node):
        super(Cast, self ).load_from_xml_node(_xml_node)
        self.dest_type = isnone(_xml_node.find("dest_type"))
        self.format_string = isnone(_xml_node.find("format_string"))
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("cast")
        etree.SubElement(_xml_node, "dest_type").text = self.dest_type
        etree.SubElement(_xml_node, "format_string").text = self.format_string
        
        return _xml_node

    def transform(self, _value):
        """Make cast"""
        if self.dest_type in ['string', 'string(255)', 'string(3000)']:
            return str(_value)
        elif self.dest_type in ['float']:
            return float(_value)
        elif self.dest_type in ['integer', 'serial']:
            return int(_value)
        elif self.dest_type in ['timestamp']:
            if self.format_string != None:
                
                return datetime.strptime(_value, self.format_string)
            else:
                return datetime.strptime(_value, "%Y-%m-%d %H:%M:%S")
        elif self.dest_type in ['boolean']:
            return bool(_value)

class Replace(Custom_Transformation):
    """Replace returns a copy of the string in which the occurrences of old have been replaced with new, optionally restricting the number of replacements to max."""
    old = None
    new = None
    max = None
    
    def load_from_xml_node(self, _xml_node):
        super(Replace, self ).load_from_xml_node(_xml_node)
        self.old = isnone(_xml_node.find("old"))
        self.new = isnone(_xml_node.find("new"))
        self.max = isnone(_xml_node.find("max"))
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("replace")
        etree.SubElement(_xml_node, "old").text = self.old
        etree.SubElement(_xml_node, "new").text = self.new
        etree.SubElement(_xml_node, "max").text = self.max
        
        return _xml_node

    def transform(self, _value):
        """Make transformation"""
        if self.max:
            return _value.replace(self.old, self.new, int(self.max))
        else:
            return _value.replace(self.old, self.new)
            