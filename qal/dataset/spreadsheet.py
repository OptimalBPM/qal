'''
Created on Dec 28, 2013

@author: Nicklas Boerjesson
'''
from qal.common.strings import make_path_absolute, bool_to_binary_int, string_to_bool

from qal.dataset.custom import CustomDataset
from qal.tools.discover import import_error_to_help

def none_to_zero(_value):
    if _value is None or _value is False:
        return 0
    else:
        return _value

class SpreadsheetDataset(CustomDataset):
 
    """The Spreadsheet dataset can read data from a spreadsheet, currently only Excel files, and store that data in its."""
    
    has_header = None
    """True if data begins with a header row containing field names."""
    filename = None
    """The name of the file."""
    field_names = None
    """The names of the fields(if applicable, see has_header)."""
    sheet_name = None
    """The name of the worksheet to use, typically "Sheet1"."""
    x_offset = None
    """The leftmost column where data is held"""
    y_offset = None
    """The topmost row where data is held, *including the header row*."""
    
    def read_resource_settings(self, _resource):
        self._base_path = _resource.base_path

        if _resource.type.upper() != 'SPREADSHEET':
            raise Exception("SpreadsheetDataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename = _resource.data.get("filename")
        self.delimiter = _resource.data.get("delimiter")
        if _resource.data.get("has_header"):
            self.has_header = string_to_bool(str(_resource.data.get("has_header")))
        else:
            self.has_header = None

        self.sheet_name = _resource.data.get("sheet_name")

        if _resource.data.get("x_offset") is not None:
            self.x_offset = int(_resource.data.get("x_offset"))
        else:
            self.x_offset = 0
        if _resource.data.get("y_offset") is not None:
            self.y_offset = int( _resource.data.get("y_offset"))
        else:
            self.y_offset = 0

    def write_resource_settings(self, _resource):
        _resource.type = 'SPREADSHEET'
        _resource.data.clear()
        _resource.data["filename"] = self.filename
        _resource.data["delimiter"] = self.delimiter
        _resource.data["has_header"] = self.has_header
        _resource.data["sheet_name"] = self.sheet_name
        _resource.data["x_offset"] = self.x_offset
        _resource.data["y_offset"] = self.y_offset
    
    def __init__(self, _filename = None, _has_header = None, _resource = None, _sheet_name = None, _x_offset = None, _y_offset = None):
        '''
        Constructor
        '''
        super(SpreadsheetDataset, self ).__init__()
        if _resource != None:
            self.read_resource_settings(_resource)        
        else:
  
            if _filename != None: 
                self.filename = _filename
            else:
                self.filename = None      
            if _has_header != None: 
                self.has_header = _has_header
            else:
                self.has_header = None
            if _sheet_name != None: 
                self.sheet_name = _sheet_name
            else:
                self.sheet_name = None

            if _x_offset != None:
                self.x_offset = _x_offset
            else:
                self.x_offset = None
            if _y_offset != None: 
                self.y_offset = _y_offset
            else:
                self.y_offset = None
                  
    def load_xls(self, _filename, _sheet_name, _x_offset, _y_offset, _header_offset):
        print("SpreadsheetDataset.load_xls: using xlrd")

        try:
            from xlrd import open_workbook
        except ImportError as _err:
            raise Exception(import_error_to_help(_module="xlrd", _err_obj=_err, _pip_package="xlrd",
                                                 _apt_package=None, _win_package=None))
        try:
            _workbook = open_workbook(_filename)
            _sheet = _workbook.sheet_by_name(_sheet_name)

            if _header_offset >0:
                _field_names = _sheet.row_values(rowx=_y_offset, start_colx=_x_offset, end_colx=_sheet.ncols)
            else:
                _field_names = list("Column_" + str(x) for x in range(_y_offset, _sheet.ncols))

            _data_table = []

            for _curr_y in range(_y_offset + _header_offset, _sheet.nrows):
                _data_table.append(_sheet.row_values(_curr_y, start_colx=_x_offset, end_colx=_sheet.ncols))

            return _data_table, _field_names

        except IOError as e:
            raise Exception("SpreadsheetDataset.load: Error reading file:" + str(e))

    def load_xlsx(self, _filename, _sheet_name, _x_offset, _y_offset, _header_offset):
        print("SpreadsheetDataset.load_xlsx: using openpyxl")

    def load(self):
        """Load data. """

        import os.path
        _extension = os.path.splitext(self.filename)[1]

        _x_offset = none_to_zero(self.x_offset)
        _y_offset = none_to_zero(self.y_offset)

        if self.has_header:
            _header_offset = 1
        else:
            _header_offset = 0

        _filename = make_path_absolute(self.filename, self._base_path)

        if _extension.lower() in [".xls"]:
            self.data_table, self.field_names = self.load_xls(_filename=_filename, _sheet_name=self.sheet_name,
                                           _x_offset=_x_offset, _y_offset=_y_offset,
                                           _header_offset = _header_offset)
            
            
        elif _extension.lower() == ".odt":
            # from ezodf import newdoc, Paragraph, Heading, Sheet
            raise Exception("SpreadsheetDataset.load: Open Document Spreadsheet not implemented yet.")

        else:
            raise Exception("SpreadsheetDataset.load: Unsupported file type \"" + _extension +"\"")
    def save_xlsx(self, _filename, _data_table, _number_of_rows, _number_of_columns,
                 _x_offset, _y_offset, _header_offset, _field_names):
        pass

    def save_xls(self, _filename, _data_table, _number_of_rows, _number_of_columns,
                 _x_offset, _y_offset, _header_offset, _field_names):
        print("SpreadsheetDataset.save_xls: using xlwt")

        try:
            from xlwt import Workbook
        except ImportError as _err:
            raise Exception(import_error_to_help(_module="xlwt",
                                                 _err_obj=_err,
                                                 _pip_package="xlwt",
                                                 _apt_package=None,
                                                 _win_package=None))

        _workbook = Workbook()
        _sheet = _workbook.add_sheet(sheetname="Sheet1", cell_overwrite_ok=True)

        if _header_offset > 0:
            for _curr_col_idx in range(0, _number_of_columns):
                _sheet.write(self.y_offset, _curr_col_idx + _x_offset, label=_field_names[_curr_col_idx])

        for _curr_row_idx in range(0, _number_of_rows + self.y_offset + _header_offset -1):
            for _curr_col_idx in range(0, _number_of_columns):
                _sheet.write(_curr_row_idx + _y_offset + _header_offset, _curr_col_idx + _x_offset,
                label=_data_table[_curr_row_idx][_curr_col_idx])

        _workbook.save(filename=_filename)

    def save(self, _save_as=None):
        """Save data"""
        _number_of_rows = len(self.data_table)
        if _number_of_rows > 0:
            _number_of_columns = len(self.data_table[0])
        print("SpreadsheetDataset.save: Filename='" + str(self.filename) + "', Delimiter='"+str(self.delimiter)+"'")
        import os.path
        _extension = os.path.splitext(self.filename)[1]
        if _save_as:
            _filename = _save_as
        else:
            _filename = make_path_absolute(self.filename, self._base_path)

        _x_offset = none_to_zero(self.x_offset)
        _y_offset = none_to_zero(self.y_offset)
        if self.y_offset is None:
            self.y_offset = 0
        if self.has_header:
            _header_offset = 1
        else:
            _header_offset = 0

        if _extension.lower() == ".xls":
            self.save_xls(_filename, _data_table=self.data_table, _number_of_rows=_number_of_rows,
                          _number_of_columns=_number_of_columns, _x_offset=self.x_offset,
                          _y_offset=self.y_offset, _header_offset=_header_offset, _field_names=self.field_names)

        elif _extension.lower() == ".xlsx":
            self.save_xlsx(_filename, _data_table=self.data_table, _number_of_rows=_number_of_rows,
                          _number_of_columns=_number_of_columns, _x_offset=self.x_offset,
                          _y_offset=self.y_offset, _header_offset=_header_offset, _field_names=self.field_names)


        

    
        