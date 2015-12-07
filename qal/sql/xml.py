"""
Created on Sep 21, 2010

@author: Nicklas Boerjesson

"""
from xml.dom.minidom import Document
from xml.sax.saxutils import escape
from csv import list_dialects
import os

from qal.dataset.xpath import xpath_data_formats
from qal.common.meta import list_class_properties, list_prefixed_classes, find_class
from qal.sql.types import sql_property_to_type, and_or, \
    constraint_types, index_types, verbs, set_operator, data_source_types, in_types, join_types, data_types, \
    quoting_types
from qal.dal.types import db_types
from qal.common.xml_utils import XMLTranslation, xml_base_type_value, find_child_node, xml_get_text, \
    xml_set_cdata, xml_get_numeric, xml_get_boolean, xml_get_allowed_value, xml_find_non_text_child
from qal.common.resources import Resources




# Imported for class resolution

from qal.sql.sql import *  # @UnusedWildImport #IGNORE:W0401
from qal.dataset.custom import CustomDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.flatfile import FlatfileDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.matrix import MatrixDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.rdbms import RDBMSDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.spreadsheet import SpreadsheetDataset # @UnusedWildImport #IGNORE:W0401
from qal.dataset.xpath import XpathDataset# @UnusedWildImport #IGNORE:W0401

def sql_property_to_xml_type(_propertyname):
    """Converts a SQL class' property name to an XML type"""
    result = sql_property_to_type(_propertyname)
    if result[0] in ['string', 'decimal', 'boolean', 'integer']:
        result[0] = "xsd:" + result[0]
    return result


class SQLXML(XMLTranslation):
    """
    This class converts XML into a class structure(declare in SQL.py) that holds the statements. 
    """

    encoding = 'utf-8'
    _resources = None
    base_path = None
    """The base path of the document. Allows for relative paths within."""

    def __init__(self):
        """
        Constructor
        """
        super(SQLXML, self).__init__()
        self.namespace = 'http://www.optimalbpm.se/XMLschema/DAL/SQL'
        self.schema_uri = 'http://www.optimalbpm.se/XMLschema/DAL/SQL.xsd'
        self.prefix_own = 'sql'
        self.debuglevel = 2
        self.nestinglevel = 0

    def _add_child_array_of(self, _document, _parent_node, _name, _types):

        _complex_node = _document.createElementNS(self.namespace, self.prefix_schema + ":complexType")
        _complex_node.setAttribute("name", _name)
        _sequence_node = _document.createElementNS(self.namespace, self.prefix_schema + ":choice")
        _sequence_node.setAttribute("minOccurs", '0')
        _sequence_node.setAttribute("maxOccurs", 'unbounded')
        _complex_node.appendChild(_sequence_node)
        if isinstance(_types, list):
            for currType in _types:
                _element_node = _document.createElementNS(self.namespace, self.prefix_schema + ":element")
                _element_node.setAttribute("name", currType)
                _element_node.setAttribute("type", currType)

                _sequence_node.appendChild(_element_node)
        else:
            _element_node = _document.createElementNS(self.namespace, self.prefix_schema + ":element")
            _element_node.setAttribute("name", "str")
            _element_node.setAttribute("type", _types)

            _sequence_node.appendChild(_element_node)

        _parent_node.appendChild(_complex_node)

    def _add_child_string_restriction(self, _document, _parent_node, _name, _enums, _pattern=None):
        _simple_node = _document.createElementNS(self.namespace, self.prefix_schema + ":simpleType")
        _simple_node.setAttribute("name", _name)
        _restriction_node = _document.createElementNS(self.namespace, self.prefix_schema + ":restriction")
        _restriction_node.setAttribute("base", self.prefix_schema + ":string")
        if _enums is not None:
            for _curr_value in _enums:
                _enumeration_node = _document.createElementNS(self.namespace, self.prefix_schema + ":enumeration")
                _enumeration_node.setAttribute("value", _curr_value)
                _restriction_node.appendChild(_enumeration_node)
        if _pattern is not None:
            _pattern_node = _document.createElementNS(self.namespace, self.prefix_schema + ":pattern")
            _pattern_node.setAttribute("value", _pattern)
            _restriction_node.appendChild(_pattern_node)

        _simple_node.appendChild(_restriction_node)
        _parent_node.appendChild(_simple_node)

    def _add_child_type_restriction(self, _document, _parent_node, _name, _enums):
        _simple_node = _document.createElementNS(self.namespace, self.prefix_schema + ":complexType")
        _simple_node.setAttribute("name", _name)
        _choice_node = _document.createElementNS(self.namespace, self.prefix_schema + ":choice")
        for _curr_value in _enums:
            _enumeration_node = _document.createElementNS(self.namespace, self.prefix_schema + ":element")
            _enumeration_node.setAttribute("name", _curr_value)
            _enumeration_node.setAttribute("type", _curr_value)
            _choice_node.appendChild(_enumeration_node)

        _simple_node.appendChild(_choice_node)
        _parent_node.appendChild(_simple_node)

    def _add_child_restrictions(self, _document, _parent_node):
        self._add_child_string_restriction(_document, _parent_node, "datatypes", None,
                                           "(integer|string|string(\(.*\))|serial|timestamp)")
        self._add_child_string_restriction(_document, _parent_node, "db_types", db_types())
        self._add_child_string_restriction(_document, _parent_node, "and_or", and_or())
        self._add_child_string_restriction(_document, _parent_node, "in_types", in_types())
        self._add_child_string_restriction(_document, _parent_node, "quoting", quoting_types())
        self._add_child_string_restriction(_document, _parent_node, "xpath_data_format", xpath_data_formats())
        self._add_child_string_restriction(_document, _parent_node, "index_types", index_types())
        self._add_child_string_restriction(_document, _parent_node, "constraint_types", constraint_types())
        self._add_child_string_restriction(_document, _parent_node, "set_operator", set_operator())
        self._add_child_string_restriction(_document, _parent_node, "csv_dialects", list_dialects())
        self._add_child_string_restriction(_document, _parent_node, "join_types", join_types())

        self._add_child_type_restriction(_document, _parent_node, "statement", verbs())
        self._add_child_type_restriction(_document, _parent_node, "condition_part", condition_part())
        self._add_child_type_restriction(_document, _parent_node, "data_source_types", data_source_types())
        self._add_child_type_restriction(_document, _parent_node, "TabularExpressionItem",
                                         tabular_expression_item_types())
        self._add_child_type_restriction(_document, _parent_node, "field_types",
                                         data_types())

        self._add_child_array_of(_document, _parent_node, 'ArrayString', 'xsd:string')
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterString', ['ParameterString'])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterConstraint', ['ParameterConstraint'])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterColumndefinition',
                                 ['ParameterColumndefinition'])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterSource', ['ParameterSource'])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterWhen', ['ParameterWhen'])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterIdentifier', ["ParameterIdentifier"])
        self._add_child_array_of(_document, _parent_node, 'ArrayStatement', ["statement"])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterOrderByItem', ["ParameterOrderByItem"])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterCondition', ["ParameterCondition"])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterField', ["ParameterField"])
        self._add_child_array_of(_document, _parent_node, 'ArrayParameterAssignment', ["ParameterAssignment"])
        self._add_child_array_of(_document, _parent_node, 'ArrayExpressionItem', expression_item_types())
        self._add_child_array_of(_document, _parent_node, 'ArrayTabularExpressionItem',
                                 tabular_expression_item_types())
        self._add_child_array_of(_document, _parent_node, 'ArrayList', '')

    def _add_child_property_node(self, _document, _parent_node, _property_name):
        _curr_node = _document.createElementNS(self.namespace, self.prefix_schema + ":element")
        _curr_node.setAttribute("name", _property_name)

        _curr_node.setAttribute("type", sql_property_to_xml_type(_property_name)[0])
        _curr_node.setAttribute("minOccurs", "0")
        _parent_node.appendChild(_curr_node)

    def _add_child_type_node(self, _document, _parent_node, _class_name):

        _complex_node = _document.createElementNS(self.namespace, self.prefix_schema + ":complexType")
        _complex_node.setAttribute("name", _class_name)

        _all_node = _document.createElementNS(self.namespace, self.prefix_schema + ":all")
        _complex_node.appendChild(_all_node)

        for _curr_property in list_class_properties(globals(), _class_name):
            self._add_child_property_node(_document, _all_node, _curr_property)

        _parent_node.appendChild(_complex_node)

    def _add_child_types(self, _document, _parentnode):

        # First add restrictions

        self._add_child_restrictions(_document, _parentnode)
        # First, Add parameter types
        for _curr_class in list_prefixed_classes(globals(), "parameter"):
            self._add_child_type_node(_document, _parentnode, _curr_class)

        # Then add verbs.

        for _curr_class in list_prefixed_classes(globals(), "verb"):
            self._add_child_type_node(_document, _parentnode, _curr_class)

        # Then add datasets.
        for _curr_class in data_source_types():
            self._add_child_type_node(_document, _parentnode, _curr_class)

    def generate_schema(self):
        """Generates an XML schema based on the class structure in SQL.py"""
        # Create the minidom document
        doc = Document()

        # Create the <wml> base element
        schema = doc.createElement(self.prefix_schema + ":schema")
        schema.setAttribute("xmlns:" + self.prefix_schema, 'http://www.w3.org/2001/XMLSchema')
        schema.setAttribute("targetNamespace", self.namespace)
        schema.setAttribute("xmlns", self.namespace)
        schema.setAttribute("elementFormDefault", 'qualified')
        doc.appendChild(schema)

        self._add_child_types(doc, schema)
        statement = doc.createElement(self.prefix_schema + ":element")
        statement.setAttribute("name", 'statement')
        statement.setAttribute("type", 'statement')
        schema.appendChild(statement)

        return doc

    def _parse_array_xml_node(self, _node, _list, _parent):
        self._go_down("_parse_array_xml_node")
        self._debug_print("_parse_array_xml_node: Parsing " + _node.nodeName)

        # Loop nodes and parse them.        

        for currNode in _node.childNodes:

            if currNode.nodeType != currNode.TEXT_NODE:
                # Do not handle text nodes, there should never be any in lists.
                resobj = self._parse_class_xml_node(currNode, None, _parent)
                _list.append(resobj)

        self._get_up("_parse_array_xml_node")
        return _list

    def _parse_class_xml_node(self, _node, _classname, _parent):
        self._go_down("_parse_class_xml_node")
        if _classname is None:
            _classname = _node.nodeName

        self._debug_print("_parse_class_xml_node: Parsing " + _classname)

        _stripped_classname = self._strip_own(_classname)

        # Check for base typesError in VerbCreateIndex, name is not set.
        if _stripped_classname.lower() in ['str', 'int', 'float', 'datetime']:
            return xml_base_type_value(_node, _stripped_classname)

            # Find and instatiate the actual class.
        _obj, _obj_name = find_class(globals(), _stripped_classname)

        if hasattr(_obj, 'as_sql'):
            _obj._parent = _parent
            _obj._base_path = self.base_path
            self._debug_print(
                "_parse_class_xml_node: Found matching Parameter class for " + _classname + " : " + _obj_name)

        elif isinstance(_obj, list):
            # If this is a list, parse it and return.
            self._debug_print("_parse_class_xml_node: Found matching list class for " + _classname + " : " + _obj_name)
            return self._parse_array_xml_node(_node, _obj, _obj)
        else:
            raise Exception("_parse_class_xml_node: Could not find matching class : " + _obj_name)

        # Loop the object's properties
        for _curr_itemkey, _curr_obj in _obj.__dict__.items():

            if _curr_itemkey != 'row_separator':

                _curr_node = find_child_node(_node, self._add_own(_curr_itemkey))
                if _curr_node is not None:

                    self._debug_print("_parse_class_xml_node: Parsing property " + _curr_itemkey)

                    if isinstance(_curr_obj, list):
                        _obj.__dict__[_curr_itemkey] = self._parse_array_xml_node(_curr_node, _curr_obj, _obj)
                    else:
                        # Match the property to a type.
                        currtype = sql_property_to_type(_curr_itemkey)
                        if currtype[0].lower() in ['string', 'datatypes']:
                            _obj.__dict__[_curr_itemkey] = xml_get_text(_curr_node)
                        elif currtype[0].lower() == 'boolean':
                            _obj.__dict__[_curr_itemkey] = xml_get_boolean(_curr_node)
                        elif currtype[0].lower() in ['integer', 'float', 'decimal']:
                            _obj.__dict__[_curr_itemkey] = xml_get_numeric(_curr_node, currtype[0])
                        elif currtype[0:5] == 'verb_' or currtype[0:10] == 'parameter_':
                            raise Exception(
                                "_parse_class_xml_node: Strange VERB/PARAMETER happened parsing.. parent: " +
                                _parent.__class__.__name__ + "Class: " + _classname + " Currtype: " + currtype)

                        elif len(currtype) > 1 and type(currtype[1]) == list:
                            _curr_child = xml_find_non_text_child(_curr_node)
                            if _curr_child:
                                _obj.__dict__[_curr_itemkey] = self._parse_class_xml_node(_curr_child, None, _obj)
                            else:
                                # Base types doesn't have any children.
                                _obj.__dict__[_curr_itemkey] = xml_get_allowed_value(_curr_node, currtype)

                        else:
                            _curr_child = xml_find_non_text_child(_curr_node)
                            if _curr_child:
                                _obj.__dict__[_curr_itemkey] = self._parse_class_xml_node(_curr_node, currtype[0], _obj)

        if self._resources and hasattr(_obj,
                                       'resource_uuid') and _obj.resource_uuid is not None and _obj.resource_uuid != '':
            _obj._resource = self._resources.get_resource(_obj.resource_uuid)
            self._debug_print("_parse_class_xml_node: Added resource_uuid for " + _obj_name + ": " + _obj.resource_uuid,
                              1)

        self._get_up("_parse_class_xml_node")
        return _obj

    def xml_to_sql_structure(self, _xml="", _node=None, _base_path=None):
        """Translates an XML file into a class structure"""
        _node = self.get_root_node('statement', _xml, _node)

        if _base_path:
            self.base_path = os.path.dirname(_base_path)

        _resources_node = find_child_node(_node, 'resources')

        if _resources_node:
            self._debug_print("xml_to_sql_structure: Found resources.")
            print("xml_to_sql_structure: Found resources.")
            # Send XML here, since resources now uses lxml      
            self._resources = Resources(_resources_xml=_resources_node.toxml(), _base_path=_base_path)

        _verb = xml_find_non_text_child(_node)
        if _verb is None:
            raise Exception('XMLToSQL: No Verb_*-node found.')
        _structure = self._parse_class_xml_node(_verb, None, None)

        return _structure

    def xml_file_to_sql(self, _xml_file_name, **kwargs):
        """Reads a specified XML fil and translates it into an SQL class structure."""
        # Read file
        _xml_file = open(_xml_file_name, 'r')
        _xml = _xml_file.read()
        # Encode parameters
        for _curr_name, _curr_value in kwargs.items():
            if _curr_value is None:
                _curr_value = 'NULL'
            _xml = _xml.replace('::Param=' + _curr_name + '::', escape(str(_curr_value)))
        _xml_file.close()
        return self.xml_to_sql_structure(_xml)

    def _xml_encode_list(self, _document, _node, _list):
        self._debug_print("_xml_encode_list: Encoding " + _node.nodeName)
        for _curr_item in _list:
            self._xml_encode_object(_document, _node, _curr_item)

    def _xml_encode_object(self, _document, _parent_node, _object):

        if _object is not None and _object != "":
            self._debug_print("_xml_encode_object: Encoding " + _object.__class__.__name__)
            _object_node = _document.createElement(self.prefix_own + ':' + _object.__class__.__name__)
            _parent_node.appendChild(_object_node)
            if hasattr(_object, "__dict__"):
                for _curr_property_name, currProperty in sorted(_object.__dict__.items()):
                    if not _curr_property_name.lower() in ['row_separator'] and \
                            not hasattr(currProperty, '__call__') and _curr_property_name[0:1] != '_':
                        # Create node for property
                        _curr_node = _document.createElement(self.prefix_own + ':' + _curr_property_name)

                        # Property is a list
                        if isinstance(currProperty, list):
                            self._xml_encode_list(_document, _curr_node, currProperty)
                        elif hasattr(currProperty, 'as_sql'):
                            self._xml_encode_object(_document, _curr_node, currProperty)
                        else:
                            _curr_type = sql_property_to_type(_curr_property_name)
                            if str(currProperty).isnumeric() and len(_curr_type) > 1:
                                xml_set_cdata(_curr_node, _curr_type[1][currProperty])
                            else:
                                xml_set_cdata(_curr_node, currProperty, (_curr_type == "boolean"))

                        _object_node.appendChild(_curr_node)

            elif isinstance(_object, list):

                self._xml_encode_list(_document, _object_node, _object)
            else:
                xml_set_cdata(_object_node, _object)

    def sql_structure_to_xml(self, _structure, _resources = None):
        """Translates an XML structure into XML"""
        # Create the minidom document
        _doc = Document()
        _doc.encoding = self.encoding
        # Create the root element "statement".
        _statement = _doc.createElement(self.prefix_own + ":statement")
        _statement.setAttribute("xmlns:" + self.prefix_xml, 'http://www.w3.org/2001/XMLSchema-instance')
        _statement.setAttribute(self.prefix_xml + ":schemaLocation", self.namespace + ' ' + self.schema_uri)
        _statement.setAttribute("xmlns:" + self.prefix_own, self.namespace)
        _doc.appendChild(_statement)

        # Recurse structure
        self._xml_encode_object(_doc, _statement, _structure)
        if _resources is not None:
            _statement.appendChild(_resources.as_xml_node())
        return _doc

