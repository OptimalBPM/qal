"""
Created on Jan 26, 2014

@author: Nicklas Boerjesson
"""
import os
from tkinter.constants import SUNKEN
from qal.tools.gui.widgets_misc import BPMFrame
from tkinter import filedialog
__author__ = 'Nicklas Boerjesson'

class FrameCustomDataset(BPMFrame):
    """
         This class is the super class of all the dataset property GUI:s.
         Those that inherit add their own functionality.
         Also owns the QAL-datasets and the qal.dal connections in the application.
    """

    dataset = None
    """The qal.dataset"""
    references = None
    """A list of references, for example field names or XPaths"""
    base_path = None
    """If the dataset is file-based, the location of the file, this to handle relative paths."""
    on_columns_change = None
    """Points to a function that if set is to be called to notify when columns(references) are changes."""

    is_destination = None

    def __init__(self, _master, _dataset=None, _relief=SUNKEN, _is_destination=None):
        super(FrameCustomDataset, self ).__init__(_master, bd=1, relief=_relief)

        self.is_destination = _is_destination

        self.init_widgets()

        if _dataset:
            self.dataset = _dataset

            self.read_from_dataset()
        else:
            self._dataset = None


    def init_widgets(self):
        """
        This function is called when the frame is instantiated to allow for customized behaviour.
        """
        pass

    def read_from_dataset(self):
        """
        This function reads the data from the dataset into the GUI components
        """
        pass

    def write_to_dataset(self):
        """
        This function reads data from the GUI components into the dataset
        """
        pass

    def check_reload(self):
        """
        Checks if the component is able to reload its data.
        If there is no problems found, it return False, if any found, it returns a textual description of
        the reason.
        :return: a boolean or a string, False if not problems found, a problem description if found.
        """
        pass

    def reload(self):
        """
        Reloads the dataset.
        """

        raise Exception("This source does not support reloading data.")


    def get_possible_references(self, _force = None):
        """
        Returns a list of currently available references, references are for example table column names or XPaths.
        :param _force: If set, the dataset is reloaded to ensure that the references are relevant
        :return: a string list of references
        """
        raise Exception("This source does not support listing references.")


class FrameCustomFileDataset(FrameCustomDataset):

    def select_file(self, _default_extension, _file_types):
        """Brings up a selector dialog, prompting the user to select a file,
        the relative path if the file is then saved to the filename property.
        Also, the base path is set.
        """
        if self.is_destination is True:
            _filename = filedialog.asksaveasfilename(initialdir=os.path.dirname(self.filename.get()),
                                               defaultextension=_default_extension,
                                               filetypes=_file_types,
                                               title="Choose destination file")
        else:
            _filename = filedialog.askopenfilename(initialdir=os.path.dirname(self.filename.get()),
                                               defaultextension=_default_extension,
                                               filetypes=_file_types,
                                               title="Choose source file")
        if _filename:
            self.base_path = os.path.dirname(_filename)
            self.filename.set(os.path.relpath(_filename, self.base_path))