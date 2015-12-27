"""
Created on Feb 5, 2014

@author: Nicklas Boerjesson
"""
from tkinter import IntVar, StringVar, ttk, BooleanVar
from tkinter.constants import LEFT, X, RIGHT
from qal.common.strings import bool_to_binary_int, binary_int_to_bool, empty_when_none
from qal.tools.gui.frame_list import FrameCustomItem

__author__ = 'nibo'



class FrameMapping(FrameCustomItem):
    """Holds and visualizes a Map between two columns of different datasets"""
    row_index = None

    is_key = None
    src_reference = None
    src_datatype = None
    src_cast_to = None
    dest_table = None
    curr_data = None
    curr_raw_data = None
    mapping = None

    preview = None
    dest_reference = None

    def __init__(self, _master, _mapping = None,
                 _on_get_source_references = None,
                 _on_get_destination_references = None,
                 _on_select = None):
        super(FrameMapping, self).__init__(_master)


        # Add monitored variables.
        self.is_key = BooleanVar()
        self.src_reference = StringVar()
        self.src_datatype = StringVar()
        self.curr_data = StringVar()

        self.result_cast_to = StringVar()
        self.preview = StringVar()

        self.dest_reference = StringVar()

        self.on_get_source_references = _on_get_source_references
        self.on_get_destination_references = _on_get_destination_references

        self.on_select = _on_select
        self.init_widgets()

        self.mapping = _mapping



        if _mapping is not None:
            self.mapping_to_gui()


    def mapping_to_gui(self):

        self.src_reference.set(str(empty_when_none(self.mapping.src_reference)))
        self.dest_reference.set(str(empty_when_none(self.mapping.dest_reference)))
        self.src_datatype.set(self.mapping.src_datatype)
        self.is_key.set(bool_to_binary_int(self.mapping.is_key))

    def gui_to_mapping(self):

        self.mapping.src_reference = self.src_reference.get()
        self.mapping.dest_reference = self.dest_reference.get()

        self.mapping.is_key = binary_int_to_bool(self.is_key.get())

    def reload_references(self):
        self.cb_source_ref['values'] = self.get_source_references()
        self.cb_dest_ref['values'] = self.get_destination_references()


    def get_source_references(self, _force = None):
        if self.on_get_source_references:
            return self.on_get_source_references(_force)

    def get_destination_references(self, _force = None):
        if self.on_get_destination_references:
            return self.on_get_destination_references( _force)


    def on_change_source_ref(self, *args):
        # reload dataset.
        pass


    def init_widgets(self):

        """Init all widgets"""

        # Source reference
        self.cb_source_ref = ttk.Combobox(self, textvariable=self.src_reference, state='normal')
        self.cb_source_ref['values'] = self.get_source_references()
        self.cb_source_ref.pack(side=LEFT, fill=X, expand=1)


        # Data type label
        self.l_data_type = ttk.Label(self, textvariable=self.src_datatype, width=8)
        self.src_datatype.set("Not set")

        self.l_data_type.pack(side=LEFT)

        # Dest reference
        self.cb_dest_ref = ttk.Combobox(self, textvariable=self.dest_reference, state='normal')
        self.cb_dest_ref['values'] = self.get_destination_references()
        self.cb_dest_ref.pack(side=RIGHT, fill=X, expand=1)

        # Is key field
        self.cb_is_key = ttk.Checkbutton(self, variable=self.is_key)
        self.cb_is_key.pack(side=RIGHT)

        # Current data
        self.l_data = ttk.Label(self, textvariable=self.curr_data)
        self.curr_data.set("No data")
        self.l_data.pack(side=RIGHT, fill=X, padx=5)






