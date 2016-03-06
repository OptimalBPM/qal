"""
Created on Jan 8, 2012

@author: Nicklas Boerjesson
"""

from datetime import date

import os

import time

from qal.common.meta import readattr
from qal.dataset.custom import CustomDataset
from glob import glob

F_DIR = 0
F_NAME = 1
F_SIZE = 2
F_MODIFIED = 3
F_DATA = 4

class FilesDataset(CustomDataset):
    """The Files dataset holds list of files and adds references to their content data"""

    """The glob is a path to the file or files, it can contain wild cards"""
    glob_path = None
    include_data = None

    def __init__(self, _glob_path = None, _resource=None, _include_data=None):
        """
        Constructor
        """
        super(FilesDataset, self).__init__()
        self.field_names = ["directory", "filename", "filesize", "modified", "data"]
        self.field_types = ["string", "string", "integer", "string", "blob"]
        if _resource is not None:
            self.read_resource_settings(_resource)
        else:
            self.glob_path = _glob_path
            self.include_data = _include_data

    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'FILES':
            raise Exception(
                "FilesDataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self._base_path = readattr(_resource, "base_path")
        self.glob_path = readattr(_resource, "glob_path")
        self.include_data = readattr(_resource, "include_data")

    def load(self):
        """Load data. Not implemented as it is not needed in the matrix descendant"""
        _files = glob(os.path.join(self._base_path, self.glob_path))
        self.data_table = []
        for _curr_path in _files:
            (_mode, _ino, _dev, _nlink, _uid, _gid, _size, _atime, _mtime, _ctime) = os.stat(_curr_path)

            if self.include_data:
                with open(_curr_path, mode="rb") as f:
                    # TODO: Should this try to load text files as UTF-8 encoded files? File type map?
                    _new_row = [os.path.dirname(_curr_path), os.path.split(_curr_path)[1], _size, time.ctime(_mtime), f.read()];
            else:
                _new_row = [os.path.dirname(_curr_path), os.path.split(_curr_path)[1], _size, time.ctime(_mtime), None];

            self.data_table.append(_new_row)

        return self.data_table

    def save(self):
        """If load data is set Save the data in the dataset."""
        if self.include_data:
            for _curr_row in self.data_table:
                with open(os.path.join(_curr_row[F_DIR], _curr_row[F_NAME]), "wb") as f:
                    f.write(_curr_row[F_DATA])
