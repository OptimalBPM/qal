'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

import io

from qal.dataset.custom import Custom_Dataset
from qal.common.parsing import parse_balanced_delimiters
from lxml import _elementpath
from lxml import etree
from xml.etree.ElementTree import SubElement    



def xpath_data_formats():
    return ["XML", "XHTML", "HTML"] # "UNTIDY_HTML"]

class XPath_Dataset(Custom_Dataset):
    """This class implements data formats that are possible to query via XPath.
    Currently XML and HTML are implemented(XHTML isn't tested). 
    Untidy HTML will be implemented using the Beautiful Soup library.
    """

    filename = None
    """The XPath of the "row" nodes of the data set"""
    rows_xpath = None
    """The data format"""
    xpath_data_format = None
    
    field_names = None
    field_types = None
    field_xpaths  = None

    def __init__(self, _filename = None, _rows_xpath = None,  _resource = None):

        """Constructor"""
        super(XPath_Dataset, self ).__init__()
        
        if _resource:
            self.read_resource_settings(_resource)
        else:
            if _filename != None: 
                self.filename = _filename
            else:
                self.filename = None                
            if _rows_xpath != None: 
                self.rows_xpath = _rows_xpath
            else:
                self.rows_xpath = None         


    def read_resource_settings(self, _resource):
  
        if _resource.type.upper() != 'XPATH':
            raise Exception("XPath_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename   =   _resource.data.get("filename")
        self.rows_xpath =   _resource.data.get("rows_xpath")
        self.xpath_data_format =  _resource.data.get("xpath_data_format")
        self.field_names = _resource.data.get("field_names")
        self.field_xpaths = _resource.data.get("field_xpaths")       
        self.field_types = _resource.data.get("field_types") 
        self.xpath_text_qualifier = _resource.data.get("xpath_text_qualifier") 
        self.encoding = _resource.data.get("encoding")  
              
    def file_to_tree(self, _data_format, _reference):
        print("format_to_tree : " + _data_format)
        if _data_format == 'HTML':
            from lxml import html
            return html.parse(_reference)
        if _data_format == 'XML':
            from lxml import etree
            return etree.parse(_reference)
        else:
            raise Exception("file_to_tree: " + _data_format + " is not supported")
        
    def _parse_obpm_xpath(self, _obpm_xpath):
        """Parse the trailing attribute identifier from the ubpm xpath string. ":" isn't allowed in xpath."""
        _parts = _obpm_xpath.split("::")
        if len(_parts) == 2:
            return _parts[0], _parts[1]
        else:
            return _obpm_xpath, None      
        
    def load(self):
        """Parse file, apply root XPath to iterate over and then collect field data via field_xpaths."""
        print("Loading : " + self.filename)
        self.log_load(self.filename)  
        try:
            _tree = self.file_to_tree(self.xpath_data_format, self.filename)
        except Exception as e:
            raise Exception("XPath_Dataset.load - error parsing " + self.xpath_data_format + " file : " + str(e))
        
        #_root_nodes = _tree.xpath("/html/body/form/table/tr[4]/td[2]/table/tr[2]/td/table[2]/tr/td/table/tr[10]/td/table/tr")
        _root_nodes = _tree.xpath(self.rows_xpath)
        _data = []
        for _curr_row in _root_nodes:
            _row_data = []
            for _field_idx in range(0, len(self.field_names)):
                _curr_path, _curr_attribute = self._parse_obpm_xpath(self.field_xpaths[_field_idx])
                print("_curr_path      :" + str(_curr_path))
                print("_curr_attribute :" + str(_curr_attribute))
                # Handle special case with only an attribute reference
                if _curr_path:
                    _item_data = _curr_row.xpath(_curr_path)
                else:
                    _row_data.append(self.cast_text_to_type(_curr_row.get(_curr_attribute), _field_idx))

                if len(_item_data) > 0:
                    if _curr_attribute:
                        _row_data.append(self.cast_text_to_type(_item_data[0].get(_curr_attribute), _field_idx))
                    else:
                        _row_data.append(self.cast_text_to_type(_item_data[0].text, _field_idx))
                else:
                    _row_data.append("")
            
            print(str(_row_data))
            _data.append(_row_data)
            
        self.data_table = _data
        
        return _data  
    
    def _find_next_match(self, _list, _start_idx, _match):
        for _curr_idx in range(_start_idx, len(_list)):
            if _list[_curr_idx] == _match:
                return _curr_idx
        return -1
    
    def _find_previous_match(self, _list, _start_idx, _match):
        for _curr_idx in range(_start_idx, 0, -1):
            if _list[_curr_idx] == _match:
                return _curr_idx
        return -1
    
    def _create_xpath_nodes(self, _node, _xpath):   
        """Used an xpath to create nodes that match the path and its conditions(names, attributes and so forth)"""
        #
        print("_create_xpath_nodes: " + str(_xpath))
        _curr_node = _node
        # Break up the string in its tokens
        _tokens =  list(_elementpath.xpath_tokenizer(_xpath))
        print(str(_tokens))   
        
        # Iterate through tokens, until we have gone through the entire XPath.
        _token_idx = 0 
        
        # But first, move past any root reference and conditions. 
        # TODO: This is a bit ugly, perhaps.
        if len(_tokens) > 1 and (_tokens[0][0] == "/") and (_tokens[1][1] == _node.tag):
            print("_create_xpath_nodes: Ignoring root node path and condition.")
            _token_idx+=1
            while _token_idx < len(_tokens) and _tokens[_token_idx][0] != "/": 
                _token_idx+=1
        
     
        while _token_idx < len(_tokens):
            print("_tokens[" + str(_token_idx) + "][0]:" + str(_tokens[_token_idx][0]))
            #Is this a new level?
            
            if _tokens[_token_idx][0] == "/":
                # Then the next is the name of the node
                _token_idx+=1                
            
            if _tokens[_token_idx][0] == "":

                _next_name = _tokens[_token_idx][1]

                #Is the next token a condition?
                
                if _token_idx + 1 < len(_tokens) and _tokens[_token_idx + 1][0] == "[" :
                    #It was, move on
                    _token_idx+=1
                    
                    #Find ending of condition
                    _end_cond_idx = self._find_next_match(_tokens, _token_idx, ("]",""))

                    if _end_cond_idx == -1: 
                        raise Exception("_create_xpath_nodes: Cannot find end of condition.\nXPath = " + _xpath) 
                    #Create relative path
                    _check_path = "".join((_a[0]+_a[1] for _a in _tokens[_token_idx -1:_end_cond_idx + 1]))

                    #Then check if it exists
                    print("_check_path:" + str(_check_path))
                    _found_nodes = _curr_node.xpath(_check_path)
                    
                    # Node found, move on
                    if _found_nodes and len(_found_nodes) == 1:
                        _token_idx+=1
                        _curr_node = _found_nodes[0]
                    else:
                        # If not found create node 
                        print("add node: " + str(_next_name)) 
                        _curr_node = SubElement(_curr_node, _next_name)
                        
                        # If they can be discerned(@id=value), add attributes
                        if _tokens[_token_idx + 3][0] == "=" and _tokens[_token_idx + 1][0] == "@":
                            print("set attribute to satisfy this path: " + str(_check_path)) 
                            _curr_node.set(_tokens[_token_idx + 2][1], str(_tokens[_token_idx + 4][0]).strip("\"'"))
                        
                    _token_idx+=5
                else:
                    _found_nodes = _curr_node.xpath(_next_name)
                    # Node found, move on
                    if len(_found_nodes) == 1:
                        _curr_node = _found_nodes[0]
                    else:
                        print("Create new node: " + str(_next_name))
                        _curr_node = SubElement(_curr_node, _next_name)

            _token_idx+=1
        return _curr_node
    
    def _parse_root_path(self, _root_path):
        """Extract information from the root path to get sufficient information 
        for specifying the destination and creating new nodes. \n 
        * If many xpaths in the string, it uses the first one 
        * Parses the root node name
        * Parses the row parent node path
        * Parses the row node name"""
        
        if _root_path == "":
            raise Exception("_parse_root_path: Root path cannot be empty")
        # Parse XPath tokens
        _root_xpath_tokens =  list(_elementpath.xpath_tokenizer(self.rows_xpath))
        print(str(_root_xpath_tokens))
        # Use only first path for destination
        try:
            _first_splitter = _root_xpath_tokens.index(('', '|'))
            print("_parse_root_path: Multiple XPaths, using first.")
            _split_xpath = "".join((_a[0]+_a[1] for _a in _root_xpath_tokens[0:_first_splitter]))
        except ValueError:
            _split_xpath = self.rows_xpath


        # Get the root node name
        if _root_xpath_tokens[0][0] == "/":
            _root_node_name = _root_xpath_tokens[1][1]
        else:
            raise Exception("_parse_root_path: It is necessary to have an absolute (\"/node\") top node name in the XPath")
        print("_root_node_name=" + str(_root_node_name))
        # Get the row node name

        _row_node_name_idx = self._find_previous_match(_root_xpath_tokens, len(_root_xpath_tokens) -1, ("/", ""))
        if _row_node_name_idx == -1:
            raise Exception("_parse_root_path: Cannot find start of condition.\nXPath = " + _root_path)
        else:
            _row_node_name = _root_xpath_tokens[_row_node_name_idx + 1][1]
            
        if _row_node_name_idx == 0:
            raise Exception("_parse_root_path: The row node cannot be the root node.\nXPath = " + _root_path)
        
        print("_row_node_name=" + str(_row_node_name))
        # Move on backward, what's left is the path to the parent of the row node, we need it to be able 
        # to create rows, as the path may not return a node to find a parent from
        _row_node_parent_xpath = "".join((_a[0]+_a[1] for _a in _root_xpath_tokens[0:_row_node_name_idx])) 
        
        print("_row_node_parent_xpath=" + str(_row_node_parent_xpath))
        
        return _root_node_name, _row_node_name, _row_node_parent_xpath
    
    
    def _structure_insert_row(self, _row_idx, _row_data):
        """Override parent to add XML handling"""
        self.super(XPath_Dataset, self)._structure_insert_row(_row_idx,_row_data)
        #self.data_table.insert(_row_idx,_row_data)
        
    def _structure_update_row(self, _row_idx, _row_data):
        self.super(XPath_Dataset, self)._structure_update_row(_row_idx,_row_data)
        #self.data_table[_row_idx] = _row_data

    def _structure_delete_row(self, _row_idx):
        self.super(XPath_Dataset, self)._structure_delete_row(_row_idx)
        #self.data_table.pop(_row_idx)



    def save(self, _apply_to = None, _do_not_insert = None, _do_not_update = None, _do_not_delete = None):
        """Use root XPath to find a node to iterate over and then add field data via field_xpaths, and save resulting file"""
        
        if _apply_to:
            _filename = _apply_to
        else:
            _filename = self.filename
        
        # Find the 
        _root_node_name, _row_node_name, _parent_xpath = self._parse_root_path(self.rows_xpath)
               
        # Load destination file    
        
        import os
        if _apply_to:
            
            if not os.path.exists(_filename):
                print("XPath_Dataset.save - Destination file does not exist, using source file for structure")
                _structure_file = self.filename
            else:
                _structure_file = _filename
            try:
                _tree = self.file_to_tree(self.xpath_data_format, _structure_file)
            except Exception as e:
                raise Exception("XPath_Dataset.save - error parsing " + self.xpath_data_format + " file : " + str(e))

        else:
            # Create a tree with root node based on the first  
            
            if _root_node_name != "":
                if self.encoding:
                    _encoding = self.encoding
                else:
                    _encoding = "UTF-8"
                          
                _tree = etree.parse(io.StringIO("<?xml version='1.0' ?>\n<" + _root_node_name + "/>")) 
            else:
                raise Exception("XPath_Dataset.save - rows_xpath("+ str(self.rows_xpath)+") must be absolute and have at least the name of the root node. Example: \"/root_node\" ")
        _top_node =_tree.getroot()
        
        # Where not existing, create a node structure up to the parent or the row nodes
        # from the information in the xpath.
        _row_node_parent = self._create_xpath_nodes(_top_node, _parent_xpath)

        
        # Sort and add index references. 
        # The reason for sorting is made to handle several levels of data in the right order, 
        # this way the structure is gradually built with the right attributes.
        _sorted_xpaths = [[self.field_xpaths.index(x), x] for x in sorted(self.field_xpaths)]
        print(str(_sorted_xpaths))
        print(str(self.data_table))
        
        # Create a list of row nodes which should not be deleted
        if not _do_not_delete:
            _spare_these = []
        
        _curr_path, _row_id_attribute = self._parse_obpm_xpath( _sorted_xpaths[0][1])
        if not _row_id_attribute and _apply_to:
            raise Exception("xpath.save(_apply_to=\""+ str(_filename) +"\")):\nCannot apply a dataset to an existing file without identifying attributes in the the row node level.")
        
        _log = []
        
        for _curr_row in self.data_table:
            _row_node = None
            if _row_id_attribute:
                # Create an XPath for finding existing row nodes 
                _row_xpath = _row_node_name + \
                    "[@"+ _row_id_attribute + "='" + str(_curr_row[_sorted_xpaths[0][0]]) + "']"
                # Lookup existing node
                _existing_node = _row_node_parent.xpath(_row_xpath)
                if len(_existing_node) > 1:
                    raise Exception("xpath.save: error: Non-unique key at "+ str(_row_xpath) + ", " + str(len(_existing_node)) + " matching nodes encountered.")
                elif len(_existing_node) == 0:
                    if _do_not_insert:
                        print("xpath.save(_do_not_insert=True): Skipping creating new node at "+ str(_row_xpath))
                    else:
                        _row_node = self._create_xpath_nodes(_row_node_parent, _row_xpath)
                        self.log_insert(_row_node.get(_row_id_attribute), etree.tostring(_row_node))
                else:
                    if _do_not_update:        
                        print("xpath.save(_do_not_delete=True): Skipping updating node at "+ str(_row_xpath))
                    else:
                        _row_node = _existing_node[0]

                _start_idx = 1
            else:
                # XML is empty, always create new row nodes
                _row_node = SubElement(_row_node_parent, _row_node_name)
                self.log_insert("N/A", etree.tostring(_row_node))
                _start_idx = 0
                
            if _do_not_delete:
                # The actual decision is later, this is just for logging
                print("xpath.save(_do_not_delete=True): Skipping deleting node at "+ str(_row_xpath))
            else:
                _spare_these.append(_row_node)          
                  
            if _row_node != None:
                for _field_idx in range(_start_idx, len(_sorted_xpaths)):
                    
                    # TODO: Optimize, generate a list of paths and attributes to use instead of calling _parse_obpm_xpath each time
                    _curr_path, _curr_attribute = self._parse_obpm_xpath( _sorted_xpaths[_field_idx][1])
                    _curr_node = self._create_xpath_nodes(_row_node, _curr_path)
                    print("_curr_path      :" + str(_curr_path))
                    print("_curr_attribute :" + str(_curr_attribute))
                    # Handle special case with only an attribute reference
                    _new_value = str(_curr_row[_sorted_xpaths[_field_idx][0]])
                    if _curr_attribute:
                        _curr_value = _curr_node.get(_curr_attribute)
                        if _curr_value != _new_value:
                            self.log_update_field(_curr_row[_sorted_xpaths[0][0]],_sorted_xpaths[_field_idx][1], _curr_value, _new_value)
                            _curr_node.set(_curr_attribute, _new_value)
                            
                        
                    else:
                        _curr_value = _curr_node.text
                        if _curr_node.text != _new_value:
                            self.log_update_field(_curr_row[_sorted_xpaths[0][0]],_sorted_xpaths[_field_idx][1], _curr_value, _new_value)
                            _curr_node.text = _new_value 
        
        if not _do_not_delete:
            # Delete all nodes not in source data
            for _delete_node in _row_node_parent.getchildren():
                if not(_delete_node in _spare_these):
                    if _row_id_attribute:
                        self.log_delete(_delete_node.get(_row_id_attribute), etree.tostring(_delete_node))
                    else:
                        self.log_delete('N/A',  etree.tostring(_delete_node))
                    _row_node_parent.remove(_delete_node)
        
        
                          
        _tree.write(_filename)
        self.log_save(_filename)  
        return _top_node, self._log
