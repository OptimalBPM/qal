"""
Created on Sep 14, 2012

@author: Nicklas Boerjesson
"""

import io
import json

from lxml import _elementpath
from lxml import etree
from lxml.etree import SubElement

from qal.common.meta import readattr
from qal.common.strings import make_path_absolute
from qal.dataset.custom import CustomDataset
from qal.common.listhelper import find_next_match, find_previous_match


def xpath_data_formats():
    return ["XML", "XHTML", "HTML", "JSON"]  # "UNTIDY_HTML"]


class XpathDataset(CustomDataset):
    """This class implements data formats that are possible to query via XPath.
    Currently XML and HTML are implemented(XHTML isn't tested). 
    Untidy HTML will be implemented using the Beautiful Soup library.
    """

    filename = None
    """The name of the file to be loaded"""

    rows_xpath = None
    """The XPath to the "row" nodes of the data set. Ex: /books/book"""

    field_xpaths = None
    """A list of XPaths relative to the rows_xpath leading to each field. Ex: ["title", "id"]"""

    xpath_data_format = None
    """The data format. Can be either "XML", "XHTML" or "HTML"."""

    _structure_tree = None
    """A private reference to the node tree, when load is called with _add_node_ref=True, an instance
    is kept for more efficient merging with diff data"""

    _structure_row_node_parent = None
    """A private reference to the parent node of the row nodes in the structure """

    _structure_key_fields = None
    """A private list of key fields."""

    _structure_row_node_name = None
    """
        A private variable holding the tag name of the row node.
        
        .. todo::
            Should this be able to hold an XML?
    """

    _structure_top_node = None
    """
        A private variable holding the top node of the structure.
    """

    def __init__(self, _filename=None, _rows_xpath=None, _resource=None):
        """Constructor.

        :param str _filename: Name of the file to parser
        :param str _rows_xpath: Path to nodes holding the rows.
        :param Resource _resource : An instance of a :py:class: qal.common.resource.resource
        """
        super(XpathDataset, self).__init__()

        self.field_xpaths = []
        self._structure_key_fields = []

        if _resource:
            self.read_resource_settings(_resource)
        else:
            self.filename = _filename
            self.rows_xpath = _rows_xpath

    def read_resource_settings(self, _resource):
        """Reads settings from a resource"""

        self._base_path = readattr(_resource, "base_path")

        if _resource.type.upper() != 'XPATH':
            raise Exception(
                "XpathDataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename = readattr(_resource, "filename")
        self.rows_xpath = readattr(_resource, "rows_xpath")
        self.xpath_data_format = readattr(_resource, "xpath_data_format")
        self.field_names = readattr(_resource, "field_names")
        self.field_xpaths = readattr(_resource, "field_xpaths")
        self.field_types = readattr(_resource, "field_types")
        self.encoding = readattr(_resource, "encoding")

    def write_resource_settings(self, _resource):
        """Write settings to a resource"""

        _resource.type = 'XPATH'
        _resource.filename = self.filename
        _resource.rows_xpath = self.rows_xpath
        _resource.xpath_data_format = self.xpath_data_format
        _resource.field_names = self.field_names
        _resource.field_xpaths = self.field_xpaths
        _resource.field_types = self.field_types
        _resource.encoding = self.encoding

    @staticmethod
    def _file_to_tree(_data_format, _reference):
        """Reads a file and chooses the right parser to make it an lxml element tree"""
        print("format_to_tree : " + _data_format)
        if _data_format == 'HTML':
            from lxml import html

            return html.parse(_reference)
        if _data_format == 'XML':
            from lxml import etree

            return etree.parse(_reference)
        if _data_format == 'JSON':
            from lxml import etree
            from json_lxml import element
            with open(_reference, "r") as _f:
                _top_element = json.load(_f)
                return etree.ElementTree(element("top",_top_element))
        else:
            raise Exception("_file_to_tree: " + _data_format + " is not supported")

    @staticmethod
    def _structure_parse_qal_xpath(_qal_xpath):
        """Parse the trailing attribute identifier from the QAL xpath string. ":" isn't allowed in xpath.

        :param str _qal_xpath: QAL xpath string. Ex. "library/books/book | library/books"
        """
        _parts = _qal_xpath.split("::")
        if len(_parts) == 2:
            return _parts[0], _parts[1]
        else:
            return _qal_xpath, None

    def load(self, _add_node_ref=None):
        """Parse file, apply root XPath to iterate over and then collect field data via field_xpaths.
            
        :param bool add_node_ref: If set, add an extra column with references to the underlying XML nodes, \
        used to efficiently apply new data.
        """
        print("Loading : " + self.filename)
        self.log_load(self.filename)
        try:
            _tree = self._file_to_tree(self.xpath_data_format, make_path_absolute(self.filename, self._base_path))

        except Exception as e:
            raise Exception("XpathDataset.load - error parsing " + self.xpath_data_format + " file : " + str(e))

        # _root_nodes =
        # _tree.xpath("/html/body/form/table/tr[4]/td[2]/table/tr[2]/td/table[2]/tr/td/table/tr[10]/td/table/tr")
        _root_nodes = _tree.xpath(self.rows_xpath)
        if len(_root_nodes) > 0:
            self._structure_tree = _tree
            self._structure_row_node_parent = _root_nodes[0].getparent()
            self._structure_top_node = _tree.getroot()

        _data = []

        if self.field_xpaths is not None and len(self.field_xpaths) > 0:
            for _curr_row in _root_nodes:
                _row_data = []
                for _field_idx in range(0, len(self.field_names)):
                    _curr_path, _curr_attribute = self._structure_parse_qal_xpath(self.field_xpaths[_field_idx])
                    if _curr_path != "":
                        _found_node = _curr_row.xpath(_curr_path)
                        if len(_found_node) > 0:
                            _item_data = _found_node[0]
                        else:
                            _item_data = None
                    else:
                        _item_data = _curr_row

                    if _item_data is not None:
                        if _curr_attribute:
                            if _curr_attribute[0] == "@":
                                _row_data.append(
                                    self.cast_text_to_type(_item_data.get(_curr_attribute[1:]), _field_idx))
                            elif _curr_attribute.lower() == "xml":
                                _row_data.append(self.cast_text_to_type(_item_data.tostring(), _field_idx))
                            else:
                                raise Exception(
                                    "Param_XPath.load: Error, attribute references must have the ::@AttributeName- " +
                                    "format or ::XML. The attribute was \"" + _curr_attribute + "\"")
                        else:
                            _row_data.append(self.cast_text_to_type(_item_data.text, _field_idx))
                    else:
                        _row_data.append("")

                # Add reference to XML structure.
                if _add_node_ref:
                    _row_data.append(_curr_row)

                print(str(_row_data))
                _data.append(_row_data)

            self.data_table = _data

        return _data

    def _structure_create_xpath_nodes(self, _node, _xpath):
        """Used an xpath to create nodes that match the path and its conditions(names, attributes and so forth)"""

        print("_create_xpath_nodes: " + str(_xpath))
        _curr_node = _node
        # Break up the string in its tokens
        _tokens = list(_elementpath.xpath_tokenizer(_xpath))
        print(str(_tokens))

        # Iterate through tokens, until we have gone through the entire XPath.
        _token_idx = 0

        # But first, move past any root reference and conditions. 
        # TODO: This is a bit ugly, perhaps.
        if len(_tokens) > 1 and (_tokens[0][0] == "/") and (_tokens[1][1] == _node.tag):
            print("_create_xpath_nodes: Ignoring root node path and condition.")
            _token_idx += 1
            while _token_idx < len(_tokens) and _tokens[_token_idx][0] != "/":
                _token_idx += 1

        while _token_idx < len(_tokens):
            print("_tokens[" + str(_token_idx) + "][0]:" + str(_tokens[_token_idx][0]))
            # Is this a new level?

            if _tokens[_token_idx][0] == "/":
                # Then the next is the name of the node
                _token_idx += 1

            if _tokens[_token_idx][0] == "":

                _next_name = _tokens[_token_idx][1]

                # Is the next token a condition?

                if _token_idx + 1 < len(_tokens) and _tokens[_token_idx + 1][0] == "[":
                    # It was, move on
                    _token_idx += 1

                    # Find ending of condition
                    _end_cond_idx = self.find_next_match(_tokens, _token_idx, ("]", ""))

                    if _end_cond_idx == -1:
                        raise Exception("_create_xpath_nodes: Cannot find end of condition.\nXPath = " + _xpath)
                        # Create relative path
                    _check_path = "".join((_a[0] + _a[1] for _a in _tokens[_token_idx - 1:_end_cond_idx + 1]))

                    # Then check if it exists
                    print("_check_path:" + str(_check_path))
                    _found_nodes = _curr_node.xpath(_check_path)

                    # Node found, move on
                    if _found_nodes and len(_found_nodes) == 1:
                        _token_idx += 1
                        _curr_node = _found_nodes[0]
                    else:
                        # If not found create node 
                        print("add node: " + str(_next_name))
                        _curr_node = SubElement(_curr_node, _next_name)

                        # If they can be discerned(@id=value), add attributes
                        if _tokens[_token_idx + 3][0] == "=" and _tokens[_token_idx + 1][0] == "@":
                            print("set attribute to satisfy this path: " + str(_check_path))
                            _curr_node.set(_tokens[_token_idx + 2][1], str(_tokens[_token_idx + 4][0]).strip("\"'"))

                    _token_idx += 5
                else:
                    if isinstance(_curr_node, str):
                        raise Exception(
                            "_structure_create_xpath_nodes - Internal error: Node is a string, missing node ref, "
                            "was _add_node_ref used? \nData :" + _curr_node + " | " + _next_name + " | " + _xpath +
                            " | " + str(_tokens))
                    _found_nodes = _curr_node.xpath(_next_name)
                    # Node found, move on
                    if len(_found_nodes) == 1:
                        _curr_node = _found_nodes[0]
                    else:
                        print("Create new node: " + str(_next_name))
                        _curr_node = SubElement(_curr_node, _next_name)

            _token_idx += 1
        return _curr_node

    def _structure_parse_root_path(self, _root_path):
        """Extract information from the root path to get sufficient information 
        for specifying the destination and creating new nodes. \n 
        * If many XPaths in the string, it uses the first one 
        * Parses the root node name
        * Parses the row parent node path
        * Parses the row node name"""

        if _root_path == "":
            raise Exception("_parse_root_path: Root path cannot be empty")
        # Parse XPath tokens
        _root_xpath_tokens = list(_elementpath.xpath_tokenizer(self.rows_xpath))
        # Use only first path for destination
        # TODO: This part is never used, why?
        try:
            _first_splitter = _root_xpath_tokens.index(('', '|'))
            print("_parse_root_path: Multiple XPaths, using first.")
            _split_xpath = "".join((_a[0] + _a[1] for _a in _root_xpath_tokens[0:_first_splitter]))
        except ValueError:
            _split_xpath = self.rows_xpath

        # Get the root node name
        if _root_xpath_tokens[0][0] == "/":
            _root_node_name = _root_xpath_tokens[1][1]
        else:
            raise Exception(
                "_parse_root_path: It is necessary to have an absolute (\"/node\") top node name in the XPath")
        print("_root_node_name=" + str(_root_node_name))
        # Get the row node name

        _row_node_name_idx = find_previous_match(_root_xpath_tokens, len(_root_xpath_tokens) - 1, ("/", ""))
        if _row_node_name_idx == -1:
            raise Exception("_parse_root_path: Cannot find start of condition.\nXPath = " + _root_path)
        else:
            _row_node_name = _root_xpath_tokens[_row_node_name_idx + 1][1]

        if _row_node_name_idx == 0:
            raise Exception("_parse_root_path: The row node cannot be the root node.\nXPath = " + _root_path)

        print("_structure_row_node_name=" + str(_row_node_name))
        # Move on backward, what's left is the path to the parent of the row node, we need it to be able 
        # to create rows, as the path may not return a node to find a parent from
        _row_node_parent_xpath = "".join((_a[0] + _a[1] for _a in _root_xpath_tokens[0:_row_node_name_idx]))

        print("_row_node_parent_xpath=" + str(_row_node_parent_xpath))

        return _root_node_name, _row_node_name, _row_node_parent_xpath

    def _structure_init(self, _dataset):
        """Initializes the XML structure that data is to be applied to."""
        print("XpathDataset._structure_init")
        super(XpathDataset, self)._structure_init(_dataset)

        # Parse important information data from XPath 
        _root_node_name, self._structure_row_node_name, _parent_xpath = self._structure_parse_root_path(self.rows_xpath)

        # If the structure already loaded?
        if self._structure_row_node_parent is None:

            # If not try to load, or create file.    
            import os

            if os.path.exists(make_path_absolute(self.filename, self._base_path)):

                try:
                    self.load(_add_node_ref=True)
                except Exception as e:
                    raise Exception("XpathDataset.save - error parsing " + self.xpath_data_format + " file : " + str(e))
            else:
                # Create a tree with root node based on the first  

                if _root_node_name != "":
                    # noinspection PyUnusedLocal
                    if self.encoding:
                        _encoding = self.encoding
                    else:
                        _encoding = "UTF-8"
                    # TODO: Check why this is done, _tree isn't used
                    # noinspection PyUnusedLocal
                    _tree = etree.parse(io.StringIO("<?xml version='1.0' ?>\n<" + _root_node_name + "/>"))
                else:
                    raise Exception("XpathDataset.save - rows_xpath(" + str(
                        self.rows_xpath) + ") must be absolute and have at least the name of the root node. " +
                                           "Example: \"/root_node\" ")

        # If the structure there yet? It could be an XML file with only a top node. 
        if self._structure_row_node_parent is None:
            # If not existing, create a node structure up to the parent or the row nodes
            # from the information in the xpath.
            self._structure_top_node = self._structure_create_xpath_nodes(self._structure_top_node, self.rows_xpath)

    def _structure_populate_row(self, _node, _row_data):
        for _field_idx in range(len(self.field_xpaths)):
            _curr_path, _curr_attribute = self._structure_parse_qal_xpath(self.field_xpaths[_field_idx])
            _curr_node = self._structure_create_xpath_nodes(_node, _curr_path)
            _new_value = str(_row_data[_field_idx])

            print("_curr_path      :" + str(_curr_path))
            print("_curr_attribute :" + str(_curr_attribute))
            print("_new_value :" + str(_new_value))
            # TODO: Support XML?
            if _curr_attribute:
                _curr_node.set(_curr_attribute[1:], _new_value)
            else:
                _curr_node.text = _new_value

    def _structure_insert_row(self, _row_idx, _row_data, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add XML handling"""

            print("_row_data: " + str(_row_data))


            _new_element = etree.Element(self._structure_row_node_name)

            # Insert row node
            self._structure_row_node_parent.insert(_row_idx, _new_element)

            self._structure_populate_row(_new_element, _row_data)

        # Call parent
        super(XpathDataset, self)._structure_insert_row(_row_idx, _row_data)

    def _structure_update_row(self, _row_idx, _row_data, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add XML handling"""

            # Last column is a reference
            print("_row_data: " + str(_row_data))
            _row_node = self.data_table[_row_idx][len(self.data_table[_row_idx]) - 1]

            self._structure_populate_row(_row_node, _row_data)

        # Call parent
        super(XpathDataset, self)._structure_update_row(_row_idx, _row_data)

    def _structure_delete_row(self, _row_idx, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add XML handling"""
            _row_node = self.data_table[_row_idx][len(self.data_table[_row_idx]) - 1]
            self._structure_row_node_parent.remove(_row_node)
        # Call parent
        super(XpathDataset, self)._structure_delete_row(_row_idx)
        # self.data_table.pop(_row_idx)

    def save(self, _save_as=None):
        """Save the document"""

        if not _save_as:
            _save_as = make_path_absolute(self.filename, self._base_path)
        if self.xpath_data_format == "JSON":
            from json_lxml import value
            _json_out = value(self._structure_tree.getroot())
            with open(_save_as, "w") as _f:
                json.dump(_json_out,_f)
        else:
            self._structure_tree.write(_save_as)

        self.log_save(_save_as)
        return self._log
