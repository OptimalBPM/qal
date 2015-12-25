"""
    Helper library for XML operations and their debugging. 
    
    .. note::
        Will probably be deprecated or at least rewritten when QAL switches completely to lxml.

    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details. 
"""

from urllib.request import quote, unquote
from xml.dom.minidom import Text
from xml.dom.minidom import parseString

from qal.common.recurse import Recurse
from qal.sql.utils import check_for_param_content


def xml_set_cdata(_node, _value, _lowercase=False):
    """Helper to set character data in an XML tree"""
    if _value is not None and _value != "":
        sec = Text()
        if _value is str:
            _value = quote(_value)
        if _lowercase:  # Force lowercase.
            sec.data = _value.lower()
        else:
            sec.data = _value

        _node.appendChild(sec)


def xml_get_text(_node):
    """Helper function to get character data from an XML tree"""
    rc = list()
    for node in _node.childNodes:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return unquote(''.join(rc))


def xml_get_boolean(_node):
    """Helper function to get a boolean value from XSD:booleans defaults"""
    value = xml_get_text(_node)
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        raise Exception('xml_get_boolean: "' + value + '" is not a boolean value')


def xml_get_numeric(_node, _type=""):
    """Helper function to read a numeric value from the XML"""
    try:
        _value = xml_get_text(_node)
        if _type.lower() == "integer":
            number = int(_value)
        else:
            number = float(_value)
    except Exception:
        if str(_value).lower() == 'null':
            return 'NULL'
        if not check_for_param_content(_value):
            raise Exception("xml_get_numeric: Invalid numeric format: " + _value)
        else:
            return _value

    return number


def xml_base_type_value(_node, _typename):
    """Using the base type name, get appropriate data"""
    if _typename in ["int", "float"]:
        return xml_get_numeric(_node)
    elif _typename in ["str"]:
        return xml_get_text(_node)
    elif _typename in ["bool"]:
        return xml_get_boolean(_node)
    else:
        raise Exception("xml_base_type_value: Invalid base type: " + _typename)


def xml_get_allowed_value(_node, _type):
    """Check if a value is allowed in a certain XML node"""
    value = xml_get_text(_node)

    if value in _type[1] or value == "":
        return value
    # Check for correct string format.
    elif value[0:8].lower() == "varchar(" and value[8:len(value) - 1].isnumeric() and value[len(value) - 1] == ")":
        return value
    else:
        raise Exception(
            "xml_get_allowed_value: " + str(value) + " is not a valid value in a " + _type[0] + ". Node:" + str(_node))


def xml_find_non_text_child(_node):
    """Finds the first child that is not of the Text node type."""
    nodelist = _node.childNodes
    for node in nodelist:
        if node.nodeType != node.TEXT_NODE:
            return node

    return None


def find_child_node(_node, _nodename):
    """Find all child nodes of an XML tree"""
    for node in _node.childNodes:  # visit every node <bar />
        if node.nodeName.lower() == _nodename.lower():
            return node
    return None


class XMLTranslation(Recurse):
    """
    This is a base class for translations between XML structures and object structures. 
    """

    """Transcoding settings"""
    prefix_xml = 'xsi'
    prefix_schema = 'xsd'
    prefix_own = None

    namespace = None
    schema_uri = None
    encoding = 'utf-8'

    # Debugging

    def __init__(self):
        """
        Constructor
        """
        pass


    """Helper utilities"""

    def _add_own(self, _value):
        # Adds the set SQL-prefix.
        if self.prefix_own:
            return self.prefix_own + ':' + _value
        else:
            return _value

    def _strip_own(self, _value):
        ps_len = len(self.prefix_own + ':')
        if _value[0:ps_len] == self.prefix_own + ':':
            return _value[ps_len:len(_value)]
        else:
            return _value

    def get_root_node(self, _nodename, _xml="", _node=None):

        if _node is None:
            self._debug_print(self.__class__.__name__ + ".get_root_node : - XML being parsed:\n" + _xml)
            try:
                _doc = parseString(_xml)
            except Exception as e:
                raise Exception(self.__class__.__name__ + ".get_root_node : Exception parsing SQL:\n" + str(
                    e) + "\n XML: \n" + _xml)

            """ Find root node having _nodename."""
            for _curr_node in _doc.childNodes:
                if _curr_node.nodeName == self._add_own(_nodename):
                    return _curr_node
            # TODO: What happens if there are no child nodes
            # noinspection PyUnboundLocalVariable
            if _curr_node.nodeName != self._add_own(_nodename):
                raise Exception(
                    self.__class__.__name__ + '.get_root_node : "' + self._add_own(_nodename) + ' top node required.')
        else:
            if _node.nodeName != _nodename and _node.nodeName != self._add_own(_nodename):
                raise Exception(self.__class__.__name__ + '.get_root_node : "' + _nodename + '" top node required.')

        return _node


def xml_isnone(_node):
    if _node is None or _node.text is None:
        return None
    else:
        return _node.text