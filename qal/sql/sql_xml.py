'''
Created on Sep 21, 2010

@author: Nicklas Boerjesson

'''
from qal.sql.sql_meta import list_class_properties, list_parameter_classes, list_verb_classes, find_class
from qal.sql.sql_types import sql_property_to_type, and_or, \
    constraint_types, index_types, verbs, expression_item_types, \
    condition_part,set_operator, tabular_expression_item_types, data_source_types

from qal.dal.dal_types import db_types
from xml.dom.minidom import Document, parseString
from xml.sax.saxutils import escape
from qal.common.xml_utils import XML_Translation, xml_base_type_value, find_child_node, xml_get_text,\
    xml_set_cdata, xml_get_numeric, xml_get_boolean, xml_get_allowed_value, xml_find_non_text_child

# TODO : This is a wee bit risky, this means that the schema will be dynamic dependent on what the server supports.
# Good or Bad?
from csv import list_dialects


def sql_property_to_xml_type(_PropertyName):
    """Converts a SQL class' property name to an XML type"""
    result = sql_property_to_type(_PropertyName)
    if result[0] in ['string', 'decimal', 'boolean', 'integer']:
        result[0] = "xsd:" + result[0]
    return result


class SQL_XML(XML_Translation):
    """
    This class converts XML into a class structure(declare in SQL.py) that holds the statements. 
    """

    prefix_sql = 'sql'
    encoding = 'utf-8'

    def __init__(self):
        '''
        Constructor
        '''
        super(SQL_XML, self ).__init__()
        self.namespace = 'http://www.unifiedbpm.se/XMLschema/DAL/SQL'
        self.schema_uri = 'http://www.unifiedbpm.se/XMLschema/DAL/SQL.xsd'
        self.debuglevel = 2
        self.nestinglevel = 0
    
    def _add_ps(self, _value):
        # Adds the set SQL-prefix.
        return self.prefix_sql + ':' + _value
                
    def _strip_ps(self, _value):
        ps_len = len(self.prefix_sql + ':')
        if _value[0:ps_len] == self.prefix_sql + ':':
            return _value[ps_len:len(_value)]
        else:
            return _value
        
    
    def _add_child_array_of(self, _document, _parentNode, _name, _types):

        _complex_node = _document.createElementNS(self.namespace, self.prefix_schema + ":complexType") 
        _complex_node.setAttribute("name", _name)
        _sequence_node = _document.createElementNS(self.namespace, self.prefix_schema + ":choice") 
        _complex_node.appendChild(_sequence_node)
        for currType in _types: 
            _element_node = _document.createElementNS(self.namespace, self.prefix_schema + ":element")
            _element_node.setAttribute("name", currType)
            _element_node.setAttribute("type", currType)
            _element_node.setAttribute("minOccurs", '0')
            _element_node.setAttribute("maxOccurs", 'unbounded')
            _sequence_node.appendChild(_element_node)
        
        _parentNode.appendChild(_complex_node)
                
    def _add_child_string_restriction(self, _document, _parent_node, _name, _enums, _pattern = None):
        _simple_node = _document.createElementNS(self.namespace, self.prefix_schema + ":simpleType") 
        _simple_node.setAttribute("name", _name)
        _restriction_node = _document.createElementNS(self.namespace, self.prefix_schema + ":restriction") 
        _restriction_node.setAttribute("base", self.prefix_schema + ":string")
        if _enums != None: 
            for _curr_value in _enums:
                _enumeration_node = _document.createElementNS(self.namespace, self.prefix_schema + ":enumeration") 
                _enumeration_node.setAttribute("value", _curr_value)
                _restriction_node.appendChild(_enumeration_node)
        if _pattern != None:        
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
        self._add_child_string_restriction(_document, _parent_node, "datatypes", None, "(integer|string|string(\(.*\))|serial|timestamp)")   
        self._add_child_string_restriction(_document, _parent_node, "db_types", db_types())   
        self._add_child_string_restriction(_document, _parent_node, "and_or", and_or())   
        self._add_child_string_restriction(_document, _parent_node, "index_types", index_types())   
        self._add_child_string_restriction(_document, _parent_node, "constraint_types", constraint_types())
        self._add_child_string_restriction(_document, _parent_node, "set_operator", set_operator())
        self._add_child_string_restriction(_document, _parent_node, "data_source", data_source_types())
        self._add_child_string_restriction(_document, _parent_node, "csv_dialects", list_dialects())
                      
        self._add_child_type_restriction(_document, _parent_node, "statement", verbs())    
        self._add_child_type_restriction(_document, _parent_node, "condition_part", condition_part()) 
        self._add_child_type_restriction(_document, _parent_node, "tabular_expression_item", tabular_expression_item_types()) 
                    
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_String', ['Parameter_String'])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_Constraint', ['Parameter_Constraint'])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_ColumnDefinition', ['Parameter_ColumnDefinition'])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_Source', ['Parameter_Source'])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_WHEN', ['Parameter_WHEN'])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_Identifier', ["Parameter_Identifier"])
        self._add_child_array_of(_document, _parent_node, 'Array_Statement', ["statement"])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_ORDER_BY_item', ["Parameter_ORDER_BY_item"])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_Condition', ["Parameter_Condition"])
        self._add_child_array_of(_document, _parent_node, 'Array_Parameter_Field', ["Parameter_Field"])
        self._add_child_array_of(_document, _parent_node, 'Array_expression_item', expression_item_types())
        self._add_child_array_of(_document, _parent_node, 'Array_tabular_expression_item', tabular_expression_item_types())    
        self._add_child_array_of(_document, _parent_node, 'Array_list', '*')        

        
                
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
        
        for _curr_property in list_class_properties(_class_name):
            self._add_child_property_node(_document, _all_node, _curr_property)  
                      
        _parent_node.appendChild(_complex_node)
              
     
    def _add_child_types(self, _document, _parentNode):
           
        #First add restrictions
            
        self._add_child_restrictions(_document, _parentNode)       
        #First, Add parameter types
        for _curr_class in list_parameter_classes():
            self._add_child_type_node(_document, _parentNode, _curr_class)
           
        #Then add verbs.
            
        for _curr_class in list_verb_classes():
            self._add_child_type_node(_document, _parentNode, _curr_class)
            
    
    def generate_schema(self):
        """Generates an XML schema based on the class structure in SQL.py"""
        #Create the minidom document
        doc = Document();
        
        # Create the <wml> base element
        schema = doc.createElement(self.prefix_schema + ":schema")
        schema.setAttribute("xmlns:" + self.prefix_schema, 'http://www.w3.org/2001/XMLSchema')
        schema.setAttribute("targetNamespace", self.namespace)
        schema.setAttribute("xmlns", self.namespace)
        schema.setAttribute("elementFormDefault", 'qualified')
        doc.appendChild(schema) 
     
        self._add_child_types(doc, schema);
        statement = doc.createElement(self.prefix_schema + ":element")
        statement.setAttribute("name", 'statement')
        statement.setAttribute("type", 'statement')
        schema.appendChild(statement)
        
        return doc
    
    
    def _parse_array_xml_node(self, _node, _list):
        self._go_down("_parse_array_xml_node") 
        self._debug_print("_parse_array_xml_node: Parsing " + _node.nodeName)
        
        # Loop nodes and parse them.        
 
        for currNode in _node.childNodes:
            
            if currNode.nodeType != currNode.TEXT_NODE:
                # Do not handle text nodes, there should never be any in lists.
                resobj = self._parse_class_xml_node(currNode, None)
                _list.append(resobj)
                
        self._get_up("_parse_array_xml_node")    
        return _list
    
    def _parse_class_xml_node(self, _node, _classname=None):
        self._go_down("_parse_class_xml_node")
        if _classname == None:
            _classname = _node.nodeName
        
                                      
        self._debug_print("_parse_class_xml_node: Parsing " + _classname)

        _stripped_classname = self._strip_ps(_classname)
        
        # Check for base typesError in Verb_CREATE_INDEX, name is not set.
        if _stripped_classname.lower() in  ['str', 'int', 'float', 'datetime']:
            return xml_base_type_value(_node, _stripped_classname) 
        
        # Find the actual class.
        _obj, _obj_name = find_class(_stripped_classname)

        if hasattr(_obj, 'as_sql'):
            self._debug_print("Found matching Parameter class for " + _classname + " : " + _obj_name)
        elif isinstance(_obj, list):
            # If this is a list, parse it and return.
            self._debug_print("Found matching list class for " + _classname + " : " + _obj_name)
            return self._parse_array_xml_node(_node, _obj) 
        else:
            raise Exception("_parse_class_xml_node: Could not find matching class : " + _obj_name)
        
        # Loop the object's properties
        for _curr_itemkey, _curr_obj in _obj.__dict__.items():

            if _curr_itemkey != 'row_separator':
                _curr_node = find_child_node(_node, self._add_ps(_curr_itemkey))
                if _curr_node != None:
                    
                    self._debug_print("_parse_class_xml_node: Parsing property " + _curr_itemkey)
                    
                    if isinstance(_curr_obj, list):
                        _obj.__dict__[_curr_itemkey] = self._parse_array_xml_node(_curr_node, _curr_obj)
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
                                _obj.__dict__[_curr_itemkey] = self._parse_class_xml_node(_curr_node, _obj)
                           
                        elif len(currtype) > 1 and type(currtype[1]) == list:
                            _curr_child = xml_find_non_text_child(_curr_node)
                            if (_curr_child):
                                _obj.__dict__[_curr_itemkey] = self._parse_class_xml_node(_curr_child)    
                            else:
                                # Base types doesn't have any children.
                                _obj.__dict__[_curr_itemkey] = xml_get_allowed_value(_curr_node, currtype)
                                
                        else:
                            if _curr_node.hasChildNodes():
                                _obj.__dict__[_curr_itemkey] = self._parse_class_xml_node(_curr_node, currtype[0])  
               
                  
        self._get_up("_parse_class_xml_node")       
        return _obj

    def xml_to_sql_structure(self, _xml = "", _node = None ):
        """Translates an XML file into a class structure"""
        
        self._debug_print("xml_to_sql_structure - XML being parsed:\n" + _xml)
        if _node == None:
            try:
                _doc = parseString(_xml)
            except Exception as e:
                raise Exception("xml_to_sql_structure.write_step_log_entry: Exception parsing SQL:\n" + str(e) + "\n XML: \n" + _xml)
        
            # Find root node "statement".
            for _curr_node in _doc.childNodes:
                if _curr_node.nodeName == self._add_ps('statement'):
                    _node = _curr_node 
                else:
                    break 
                
            if _curr_node.nodeName != self._add_ps('statement'):
                raise Exception('XMLToSQL: "' + self._add_ps('statement') + ' top node required.')
        else:
            if _node.nodeName != 'statement' and _node.nodeName != self._add_ps('statement'):
                raise Exception('XMLToSQL: "' + 'statement" top node required.')

        
        _verb = xml_find_non_text_child(_node)
        if (_verb == None):
            raise Exception('XMLToSQL: No Verb_*-node found.') 

        _structure = self._parse_class_xml_node(_verb)        

                
        return _structure
    
    def xml_file_to_sql(self, _xml_file_name, **kwargs):
        """Reads a specified XML fil and translates it into an SQL class structure.""" 
        # Read file
        _xml_file = open(_xml_file_name, 'r')
        _xml = _xml_file.read()
        # Encode parameters
        for _curr_name, _curr_value in kwargs.items():
            if _curr_value == None:
                _curr_value = 'NULL'
            _xml = _xml.replace('::Param='+ _curr_name + '::', escape(str(_curr_value)))
        _xml_file.close()
        return self.xml_to_sql_structure(_xml)
    
    def _xml_encode_list(self, _document, _node, _list):
        self._debug_print("_xml_encode_list: Encoding " + _node.nodeName)
        for _curr_item in _list:
            self._xml_encode_object(_document, _node, _curr_item)

        
            
    
    def _xml_encode_object(self, _document, _parent_node, _object):
        
        if (_object != None and _object != ""):
            self._debug_print("_xml_encode_object: Encoding " + _object.__class__.__name__)
            _object_node = _document.createElement(self.prefix_sql + ':' + _object.__class__.__name__)
            _parent_node.appendChild(_object_node)                
            if hasattr(_object, "__dict__"):
                for _curr_property_name, currProperty in _object.__dict__.items():
                    if not _curr_property_name.lower() in ['row_separator'] and not hasattr(currProperty, '__call__') and _curr_property_name[0:1] != '_':
                        # Create node for property
                        _curr_node = _document.createElement(self.prefix_sql + ':' + _curr_property_name)
                               
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

    
    def sql_structure_to_xml(self, _structure):
        """Translates an XML structure into XML"""
        #Create the minidom document
        _doc = Document();
        _doc.encoding = self.encoding
        # Create the root element "statement".
        _statement = _doc.createElement(self.prefix_sql + ":statement")
        _statement.setAttribute("xmlns:" + self.prefix_xml, 'http://www.w3.org/2001/XMLSchema')
        _statement.setAttribute(self.prefix_xml + ":schemaLocation", self.namespace + ' ' + self.schema_uri)
        _statement.setAttribute("xmlns:" + self.prefix_sql, self.namespace)
        _doc.appendChild(_statement) 
       
        # Recurse structure
        self._xml_encode_object(_doc, _statement, _structure);
        
        return _doc


        
        
        
