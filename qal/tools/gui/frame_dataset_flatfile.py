"""
Created on Jan 26, 2014

@author: Nicklas Boerjesson
"""
import os
from qal.dataset.flatfile import FlatfileDataset
from qal.tools.gui.frame_dataset_custom import FrameCustomFileDataset
from tkinter import Button, messagebox, SUNKEN, ttk, StringVar, IntVar, BooleanVar, filedialog
from tkinter.constants import LEFT, W
from qal.tools.gui.widgets_misc import make_entry
from qal.common.strings import empty_when_none, bool_to_binary_int, binary_int_to_bool

__author__ = 'Nicklas Boerjesson'

class FrameFlatfileDataset(FrameCustomFileDataset):
    """
        Holds an instance of, and visually represents, a flatfile dataset.
        See qal.dataset.flatfile.FlatfileDataset
    """

    def __init__(self, _master, _dataset = None, _relief = None, _is_destination=None):
        super(FrameFlatfileDataset, self ).__init__(_master, _dataset, _relief, _is_destination=_is_destination)

        if _dataset is None:
            self.dataset = FlatfileDataset()

    def on_select(self):
        """Brings up a selector dialog, prompting the user to select a file,
        the relative path if the file is then saved to the filename property.
        Also, the base path is set.
        """
        self.select_file(_default_extension = ".csv", _file_types=[('.csv files', '.csv'), ('all files', '.*')])

    def init_widgets(self):
        # file selector
        self.btn_file_select=Button(self, text="Select file",command=self.on_select)
        self.btn_file_select.grid(column=0, row=0, columnspan=2)
        # filename
        self.filename, self.e_filename, self.l_filename = make_entry(self,"File name: ", 1)

        # delimiter
        self.delimiter, self.e_delimiter, self.l_delimiter = make_entry(self,"Delimiter: ", 2)

        # has_header
        self.l_has_header = ttk.Label(self, text="Has header: ")
        self.l_has_header.grid(column=0, row=3, sticky=W)
        self.has_header = BooleanVar()
        self.e_has_header = ttk.Checkbutton(self, variable=self.has_header)
        self.e_has_header.grid(column=1, row=3, sticky=W)

        # csv_dialect
        self.csv_dialect, self.e_csv_dialect, self.l_csv_dialect = make_entry(self,"CSV dialect: ", 4)

        # quoting
        self.quoting, self.e_quoting, self.l_quoting = make_entry(self, "Quoting: ", 5)

        # escapechar
        self.escapechar, self.e_escapechar, self.l_escapechar = make_entry(self, "Escape character: ", 6)

        # lineterminator
        self.lineterminator, self.e_lineterminator, self.l_lineterminator = make_entry(self, "Line terminator: ", 7)

        # quotechar
        self.quotechar, self.e_quotechar, self.l_quotechar = make_entry(self, "Quote character: ", 8)

        # skipinitialspace
        self.skipinitialspace, self.e_skipinitialspace, self.l_skipinitialspace = make_entry(self, "Skip initial space: ", 9)

    def read_from_dataset(self):
        super(FrameFlatfileDataset, self ).read_from_dataset()

        self.filename.set(empty_when_none(self.dataset.filename))
        self.delimiter.set(empty_when_none(self.dataset.delimiter))
        self.has_header.set(bool_to_binary_int(self.dataset.has_header))
        self.csv_dialect.set(empty_when_none(self.dataset.csv_dialect))
        self.quoting.set(empty_when_none(self.dataset.quoting))
        self.escapechar.set(empty_when_none(self.dataset.escapechar))
        self.lineterminator.set(empty_when_none(self.dataset.lineterminator))
        self.quotechar.set(empty_when_none(self.dataset.quotechar))
        self.skipinitialspace.set(empty_when_none(self.dataset.skipinitialspace))

    def write_to_dataset(self):
        super(FrameFlatfileDataset, self ).write_to_dataset()

        if self.dataset is None:
            self.dataset = FlatfileDataset()

        self.dataset.filename = self.filename.get()
        self.dataset.delimiter = self.delimiter.get()
        self.dataset.has_header = self.has_header.get()
        self.dataset.csv_dialect = self.csv_dialect.get()
        self.dataset.quoting = self.quoting.get()
        self.dataset.escapechar = self.escapechar.get()
        self.dataset.lineterminator = self.lineterminator.get()
        self.dataset.quotechar = self.quotechar.get()
        self.dataset.skipinitialspace = self.skipinitialspace.get()


    def reload(self):
        self.notify_task("Load file "+ self.dataset.filename, 10)
        self.dataset.load()
        self.notify_task("Loaded filed "+ self.dataset.filename + ".", 100)

    def get_possible_references(self, _force = None):

        if not self.dataset.field_names or _force == True:
            self.reload()

        self.references = self.dataset.field_names
        return self.dataset.field_names

    def check_reload(self):
        _filename = self.filename.get()
        if not os.path.isabs(_filename) and self.base_path is None:
            return "First save the merge. You use a relative path to the flatfile dataset."
        else:
            return False