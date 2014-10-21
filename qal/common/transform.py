"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
@todo: 
* Add input type checks for better error messages
* Use format string for destination formatting as well (casting from any other primitive type to string)

"""
import re
from lxml import etree
from datetime import date, datetime
from qal.common.strings import empty_when_none


def isnone( _node):
    if _node is None or _node.text is None:
        return None
    else:
        return _node.text  

def perform_transformations(_input, _transformations):
    for _curr_transformation in _transformations:
        _input = _curr_transformation.transform(_input)
    return _input    
    
def make_transformation_array_from_xml_node(_xml_node, _substitution= None):
    _result = []
    for _curr_node in _xml_node:
        if _curr_node.tag == 'trim':
            _result.append(Trim(_xml_node=_curr_node, _substitution=_substitution))
        elif _curr_node.tag == 'IfEmpty':
            _result.append(IfEmpty(_xml_node=_curr_node, _substitution=_substitution))
        elif _curr_node.tag == 'cast':
            _result.append(Cast(_xml_node=_curr_node, _substitution=_substitution))
        elif _curr_node.tag == 'replace':
            _result.append(Replace(_xml_node=_curr_node, _substitution=_substitution))
    
    return _result
        
def make_transformations_xml_node(_transformations):
    _xml_node = etree.Element("transformations")
    for _curr_transformation in _transformations:
        _xml_node.append(_curr_transformation.as_xml_node())
    
    return _xml_node

class CustomTransformation(object):
    """
    This is the base class for all transformations
    """
    order = None
    """Order dictates when the transformation is run."""
    on_done = None
    """On done is an event, triggered when the transformation has been run. Conveys the resulting value or error message."""
    substitution = None
    """An optional instance of the substitution class. Usually shared by several transformations."""
    def __init__(self, _xml_node = None, _substitution = None):
        """
        Constructor
        """
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)

        if _substitution != None:
            self.substitution = _substitution

    def do_on_done(self, _value=None, _error=None):
        if self.on_done:
            self.on_done(_value, _error)
        return _value
            
    def init_base_to_node(self, _name):
        _xml_node = etree.Element(_name)
        _xml_node.set("order", empty_when_none(self.order))
        return _xml_node
            
    def as_xml_node(self):
        raise Exception("CustomTransformation.as_xml_node : Should not be called. Not implemented in base class, use init_base_to_node().")
        
    def load_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.order = _xml_node.get("order")
        else:
            raise Exception("CustomTransformation.load_from_xml_node : Base class need a destination node.")

    def transform(self, _value):
        try:
            _result = self._transform(_value)
            return self.do_on_done(_value=_result)
        except Exception as e:
            self.do_on_done(_error="Order: " + str(self.order) + ", " + str(e))
            raise

    def _transform(self, _value):
        """Make transformation"""
        raise Exception("CustomTransformation.transform : Not implemented in base class.")


class Trim(CustomTransformation):
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

    def _transform(self, _value):
        """Make transformation"""
        if _value is not None:
            if self.value == "beginning":
                return _value.lstrip()
            elif self.value == "end":
                return _value.rstrip()
            else:
                return _value.strip()
        
class IfEmpty(CustomTransformation):
    """IfEmpty returns a specified value if the input value is NULL."""
    value = None
    
    def load_from_xml_node(self, _xml_node):
        super(IfEmpty, self ).load_from_xml_node(_xml_node)
        self.value = _xml_node.text
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("IfEmpty")
        _xml_node.text =  self.value
        
        return _xml_node

    def _transform(self, _value):
        """Make transformation"""
        if _value is None or _value == "":
            if self.substitution is not None:
                return self.substitution.substitute(self.value)
            else:
                return self.value
        else:
            return _value
        
class Cast(CustomTransformation):
    """ICasts a string to the specified type. The timestamp date format defaults to the ISO format if format_string is not set.\n
    Possible format string directives at : http://docs.python.org/3.2/library/datetime.html#strftime-strptime-behavior\n
    For example, 2013-11-06 22:05:42 is "%Y-%m-%d %H:%M:%S".
    """
    
    dest_type = None
    """The destination type"""
    format_string = None
    """A format string where applicable"""
    
    def load_from_xml_node(self, _xml_node):
        super(Cast, self ).load_from_xml_node(_xml_node)
        self.dest_type = isnone(_xml_node.find("dest_type"))
        self.format_string = isnone(_xml_node.find("format_string"))
    
    def as_xml_node(self):
        _xml_node = self.init_base_to_node("cast")
        etree.SubElement(_xml_node, "dest_type").text = self.dest_type
        etree.SubElement(_xml_node, "format_string").text = self.format_string
        
        return _xml_node

    def _transform(self, _value):
        """Make cast"""
        try:
            if _value is None or _value=="":
                return _value
            if self.dest_type in ['string', 'string(255)', 'string(3000)']:
                if isinstance(_value, date):
                    if self.format_string is not None and self.format_string !="":
                        return _value.strftime(self.format_string)
                    else:
                        return _value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    return str(_value)
            else:
                if isinstance(_value, str):
                    # All other types will not work with quotations
                    _value = re.sub(r'^["\']|["\']$', '', _value)

            if self.dest_type in ['float']:
                return float(_value)
            elif self.dest_type in ['integer', 'serial']:
                return int(_value)
            elif self.dest_type in ['timestamp']:
                if self.format_string is not None and self.format_string !="":
                    return datetime.strptime(_value, self.format_string)
                else:
                    return datetime.strptime(_value, "%Y-%m-%d %H:%M:%S")
                
            elif self.dest_type in ['boolean']:
                return bool(_value)
            else:
                raise Exception("Invalid destination data type: " + str(self.dest_type))
                
        except Exception as e:
            raise Exception("Error in Cast.transform: " + str(e))

class Replace(CustomTransformation):
    """Replace returns a copy of the string in which the occurrences of old have been replaced with new, optionally restricting the number of replacements to max."""
    old = None
    """The old value"""
    new = None
    """The new value"""
    max = None
    """The max number of times to replace"""
    
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

    def _transform(self, _value):
        """Make replace transformation"""
        # It is a string operation, None will be handled as a string.
        if _value is None:
            _value = ""
        if self.old is None:
            raise Exception("Replace.transform: old value has to have a value.")
        else:
            _old = self.old

        if _value.find(_old) > -1:
            if self.new is None:
                _new = ""
            else:
                _new = self.new

            if self.substitution is not None and _value.find(_old) > -1:
                _new = self.substitution.substitute(_new)

            if self.max:
                return _value.replace(_old, str(_new), int(self.max))
            else:
                return _value.replace(_old, str(_new))
        else:
            return _value
            