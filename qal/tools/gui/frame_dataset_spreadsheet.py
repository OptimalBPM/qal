"""
Created on Jan 26, 2014

@author: Nicklas Boerjesson
"""
import os
from qal.common.strings import empty_when_none, bool_to_binary_int, binary_int_to_bool
from qal.dataset.flatfile import FlatfileDataset
from qal.dataset.spreadsheet import SpreadsheetDataset
from qal.dataset.xpath import XpathDataset
from qal.tools.gui.frame_dataset_custom import FrameCustomFileDataset
from tkinter import Button, messagebox, SUNKEN, ttk, StringVar, IntVar, BooleanVar
from tkinter.constants import LEFT, W
from qal.tools.gui.widgets_misc import make_entry

__author__ = 'Nicklas Boerjesson'


class FrameSpreadsheetDataset(FrameCustomFileDataset):
    """
        Holds an instance of, and visually represents, a spreadsheet dataset.
        See qal.dataset.spreadsheet.SpreadsheetDataset
    """

    def __init__(self, _master, _dataset=None, _relief=None, _is_destination=None):
        super(FrameSpreadsheetDataset, self).__init__(_master, _dataset, _relief, _is_destination=_is_destination)

        if _dataset is None:
            self.dataset = SpreadsheetDataset()

    def on_select(self):
        """Brings up a selector dialog, prompting the user to select a file,
        the relative path if the file is then saved to the filename property.
        Also, the base path is set.
        """
        self.select_file(_default_extension=".xlsx", _file_types=[('.xlsx files', '.xlsx'), ('all files', '.*')])

    def init_widgets(self):
        # file selector
        self.btn_file_select = Button(self, text="Select file", command=self.on_select)
        self.btn_file_select.grid(column=0, row=0, columnspan=2)

        # filename
        self.filename, self.e_filename, self.l_filename = make_entry(self, "File name: ", 1)


        # delimiter
        self.delimiter, self.e_delimiter, self.l_delimiter = make_entry(self, "Delimiter: ", 2)

        # has_header
        self.l_has_header = ttk.Label(self, text="Has header: ")
        self.l_has_header.grid(column=0, row=3, sticky=W)
        self.has_header = BooleanVar()
        self.e_has_header = ttk.Checkbutton(self, variable=self.has_header)
        self.e_has_header.grid(column=1, row=3, sticky=W)

        # sheet_name
        self.sheet_name, self.e_sheet_name, self.l_sheet_name = make_entry(self, "Sheet name: ", 4)

        # x_offset
        self.x_offset, self.e_x_offset, self.l_x_offset = make_entry(self, "X offset: ", 5)


        # y_offset
        self.y_offset, self.e_y_offset, self.l_y_offset = make_entry(self, "Y offset: ", 6)


    def read_from_dataset(self):
        super(FrameSpreadsheetDataset, self).read_from_dataset()

        self.filename.set(empty_when_none(self.dataset.filename))
        self.has_header.set(bool_to_binary_int(self.dataset.has_header))
        self.sheet_name.set(empty_when_none(self.dataset.sheet_name))
        self.x_offset.set(empty_when_none(self.dataset.x_offset))
        self.y_offset.set(empty_when_none(self.dataset.y_offset))


    def write_to_dataset(self):
        super(FrameSpreadsheetDataset, self).write_to_dataset()

        if self.dataset is None:
            self.dataset = SpreadsheetDataset()

        self.dataset.filename = self.filename.get()
        self.dataset.delimiter = self.delimiter.get()
        self.dataset.has_header = binary_int_to_bool(self.has_header.get())

        self.dataset.sheet_name = self.sheet_name.get()
        if self.x_offset.get() == "":
            self.dataset.x_offset = None
        else:
            self.dataset.x_offset = int(self.x_offset.get())

        if self.y_offset.get() == "":
            self.dataset.y_offset = None
        else:
            self.dataset.y_offset = int(self.y_offset.get())

    def reload(self):
        self.notify_task("Load spreadsheet " + self.dataset.filename, 10)
        self.dataset.load()
        self.notify_task("Loaded spreadsheet " + self.dataset.filename + ".", 100)

    def get_possible_references(self, _force=None):

        if not self.dataset.field_names or _force == True:
            self.reload()

        self.references = self.dataset.field_names

        return self.dataset.field_names




