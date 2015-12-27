"""
Created on Jan 26, 2014

@author: Nicklas Boerjesson
"""
import os
from qal.common.strings import empty_when_none
from qal.dataset.flatfile import FlatfileDataset
from qal.dataset.xpath import XpathDataset
from qal.tools.gui.frame_dataset_custom import FrameCustomFileDataset
from tkinter import Button, messagebox, SUNKEN, ttk, StringVar, IntVar, BooleanVar, filedialog
from tkinter.constants import LEFT, W
from qal.tools.gui.widgets_misc import make_entry

__author__ = 'Nicklas Boerjesson'

class FrameXPathDataset(FrameCustomFileDataset):
    """
        Holds an instance of, and visually represents, a XPath dataset.
        See qal.dataset.xpath.XPathDataset
    """

    def __init__(self, _master, _dataset = None, _relief = None, _is_destination=None):
        super(FrameXPathDataset, self ).__init__(_master, _dataset, _relief, _is_destination=_is_destination)
        if _dataset is None:
            self.dataset = XpathDataset()

    def on_select(self):
        """Brings up a selector dialog, prompting the user to select a file,
        the relative path if the file is then saved to the filename property.
        Also, the base path is set.
        """
        self.select_file(_default_extension = ".xml", _file_types=[('.xml files', '.xml'), ('all files', '.*')])

    def init_widgets(self):
        # file selector
        self.btn_file_select=Button(self, text="Select file",command=self.on_select)
        self.btn_file_select.grid(column=0, row=0, columnspan=2)

        # filename
        self.filename, self.e_filename, self.l_filename = make_entry(self, "File name: ", 1)

        # rows_xpath
        self.rows_xpath, self.e_rows_xpath, self.l_rows_xpath = make_entry(self, "Rows XPath: ", 2)

        # xpath_data_format
        self.xpath_data_format = StringVar()

        self.l_xpath_data_format = ttk.Label(self, text="Data format: :")
        self.l_xpath_data_format.grid(column=0, row=3)

        self.sel_xpath_data_format = ttk.Combobox(self, textvariable=self.xpath_data_format, state='readonly')
        self.sel_xpath_data_format['values'] = ["XML", "HTML"]
        self.sel_xpath_data_format.current(0)
        self.sel_xpath_data_format.grid(column=1, row=3)

        # xpath_text_qualifier
        self.xpath_text_qualifier, self.e_xpath_text_qualifier, self.l_xpath_text_qualifier = make_entry(self, "Text qualifier: ", 4)

        # encoding
        self.encoding, self.e_encoding, self.l_encoding = make_entry(self, "Encoding: ", 5)

        self.field_names = []
        self.field_xpaths = []
        self.field_types = []

    def read_from_dataset(self):
        super(FrameXPathDataset, self ).read_from_dataset()

        self.filename.set(empty_when_none(self.dataset.filename))
        self.base_path = os.path.dirname(self.dataset.filename)
        self.rows_xpath.set(empty_when_none(self.dataset.rows_xpath))
        self.xpath_data_format.set(empty_when_none(self.dataset.xpath_data_format))
        self.field_names = self.dataset.field_names
        self.field_xpaths = self.dataset.field_xpaths
        self.field_types = self.dataset.field_types
        self.encoding.set(empty_when_none(self.dataset.encoding))


    def write_to_dataset(self):
        super(FrameXPathDataset, self).write_to_dataset()

        if self.dataset is None:
            self.dataset = XpathDataset()

        self.dataset.filename = self.filename.get()
        self.dataset.rows_xpath = self.rows_xpath.get()
        self.dataset.xpath_data_format = self.xpath_data_format.get()
        self.dataset.field_names = self.field_names
        self.dataset.field_xpaths = self.field_xpaths
        self.dataset.field_types = self.field_types
        self.dataset.encoding = self.encoding.get()
        self.dataset.base_path = self.base_path


    def _recurse_paths(self,_parent_node, _exclude_len):
        """Create a list of all possible X paths under a node.
        :param _parent_node: The node from which to recurse.
        :param _exclude_len: Exclude _exclude_len number of characters. Used to remove unnecessary text.
        :return: a list of paths
        """
        _paths = []
        for _curr_node in _parent_node:
            _curr_path = self.dataset._structure_tree.getpath(_curr_node)[_exclude_len:]
            # Remove the first [0]
            _curr_offset = _curr_path.find("]") + 1
            if _curr_path[_curr_offset:] != "":
                _paths.append(_curr_path[_curr_offset + 1:])

            _child_nodes = self._recurse_paths(_curr_node, _exclude_len)
            if _child_nodes:
                _paths+=_child_nodes


        return _paths


    def reload(self):
        self.notify_task("Load XML "+ self.dataset.filename, 10)
        self.dataset.load()
        self.notify_task("Loaded XML "+ self.dataset.filename + ".", 100)


    def get_possible_references(self, _force=None):

        if self.references is None or _force == True:
            self.reload()
            if self.dataset._structure_tree:
                _rows_xpath = self.rows_xpath.get()
                self.references = sorted(set(self._recurse_paths(self.dataset._structure_tree.xpath(_rows_xpath), len(_rows_xpath))))

        return self.references
