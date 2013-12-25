'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

import io

from qal.nosql.custom import Custom_Dataset
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
        
    def _parse_ubpm_xpath(self, _ubpm_xpath):
        """Parse the trailing attribute identifier from the ubpm xpath string. ":" isn't allowed in xpath."""
        _parts = _ubpm_xpath.split("::")
        if len(_parts) == 2:
            return _parts[0], _parts[1]
        else:
            return _ubpm_xpath, None       
            
            
    

        
    def load(self):
        """Parse file, apply root XPath to iterate over and then collect field data via field_xpaths."""
        print("Loading : " + self.filename)
        
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
                _curr_path, _curr_attribute = self._parse_ubpm_xpath(self.field_xpaths[_field_idx])
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
    
    def _create_xpath_nodes(self, _node, _xpath):   
        """Used an xpath to create nodes that match the path(names, attributes and so forth)"""
        
        print("_create_xpath_nodes: " + str(_xpath))
        _curr_node = _node
        _tokens =  list(_elementpath.xpath_tokenizer(_xpath))
        print(str(_tokens))   
        _token_idx = 0 
        
        # Move past any root reference.
        if (_tokens[0][0] == "/") and (_tokens[1][1] == _node.tag):
            print("_create_xpath_nodes: Ignoring root node path."
            _token_idx+=1
            while _token_idx < len(_tokens) and _tokens[_token_idx][0] != "/": 
                _token_idx+=1
        
     
        while _token_idx < len(_tokens):
            print("_tokens[" + str(_token_idx) + "][0]:" + str(_tokens[_token_idx][0]))
            #Is this a new level?
            if _tokens[_token_idx][0] == "/":
                # Then the next is the name of the node
                _token_idx+=1
                _next_name = _tokens[_token_idx][1]

                #Is the next token a condition?
                
                if _token_idx < len(_tokens) and _tokens[_token_idx + 1][0] == "[" :
                    #It was, move on
                    _token_idx+=1
                    
                    #Then check if it exists
                    #Create relative path
                    
                    _check_path = "".join((_a[0]+_a[1] for _a in _tokens[_token_idx -1:_token_idx + 6]))
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
                            _curr_node.set(_tokens[_token_idx + 2][1], _tokens[_token_idx + 4][0])
                        
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
    
    def _prepare_root_path(self, _root_path):
        """To make the XPath usable for specifying the destination and creating new nodes, 
        if needed _prepare_root_path removes all conditions and if there are multiple paths, 
        only uses the first one. """
        

        # Use only first path for destination
        _split_xpath = _root_path.split(' | ')
        if len(_split_xpath) > 1:
            print("_prepare_root_path: Multiple XPaths, using first.")
            _relevant_path =  _split_xpath[0]
        else:
            _relevant_path =  _root_path

        # Remove all conditions
        _conditions, _no_conditions = parse_balanced_delimiters(_relevant_path, "[", "]", "'")



    def save(self, _save_as = None, _update = None, _delete = None):
        """Use root XPath to find a node to iterate over and then add field data via field_xpaths, and save resulting file"""
        
        if _save_as:
            _filename = _save_as
        else:
            _filename = self.filename
        
        print("Saving : " + _filename)    

        # Load destination file    
        
        import os
        if _update:
            
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
            _tokens =  list(_elementpath.xpath_tokenizer(self.rows_xpath))
            if len(_tokens) > 1 and _tokens[0][0] == "/" and _tokens[1][1] != "":
                if self.encoding:
                    _encoding = self.encoding
                else:
                    _encoding = "UTF-8"
                          
                _tree = etree.parse(io.StringIO("<?xml version='1.0' ?>\n<" + _tokens[1][1] + "/>")) 
            else:
                raise Exception("XPath_Dataset.save - rows_xpath("+ str(self.rows_xpath)+") must be absolute and have at least the name of the root node. Example: \"root_node\" ")

        # Where not existing, create a node structure from the information in the xpath.
        _root_node = self._create_xpath_nodes(_tree.getroot(), self.rows_xpath)

        print(etree.tostring(_tree.getroot()))
        # Sort and add indexes. Sorting is made to handle several levels of data in the right order.
        _sorted_xpaths = [[self.field_xpaths.index(x), x] for x in sorted(self.field_xpaths)]
        print(str(_sorted_xpaths))
        
        # Find parent of root path. 
        
        
        
        for _curr_row in self.data_table:
            # Find parent of root path. 
            
            for _field_idx in range(0, len(_sorted_xpaths)):
                
                _curr_path, _curr_attribute = self._parse_ubpm_xpath(_sorted_xpaths[_field_idx])
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
            
        return _data  
    

        