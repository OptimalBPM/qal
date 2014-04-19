'''
Created on Dec 28, 2013

@author: Nicklas Boerjesson
'''
from qal.common.strings import make_path_absolute, bool_to_binary_int, string_to_bool

from qal.dataset.custom import CustomDataset



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
        if _resource.type.upper() != 'SPREADSHEET':
            raise Exception("SpreadsheetDataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename = make_path_absolute(_resource.data.get("filename"), _resource.base_path)
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
                  
        
    def load(self):
        """Load data. Not implemented as it is not needed in the matrix descendant"""

        import os.path
        _extension = os.path.splitext(self.filename)[1]
        
        #TODO: Check file type
        if _extension.lower() in [".xls", ".xlsx"]:
            from xlrd import open_workbook
            try:
                _workbook = open_workbook(self.filename)
                _sheet = _workbook.sheet_by_name(self.sheet_name)
                if self.x_offset is None:
                    self.x_offset = 0
                    
                if self.y_offset is None:
                    self.y_offset = 0
              
                if self.has_header:
                    self.field_names = _sheet.row_values(rowx=0, start_colx=0, end_colx=_sheet.ncols)
                    _has_header_offset = 1
                else:
                    self.field_names = list("Column_" + str(x) for x in range(self.y_offset, _sheet.ncols))
                    _has_header_offset = 0

                self.data_table = []

                for _curr_y in range(self.y_offset + _has_header_offset, _sheet.nrows):
                    self.data_table.append(_sheet.row_values(_curr_y, start_colx= self.x_offset, end_colx= _sheet.ncols))

            except IOError as e:
                raise Exception("SpreadsheetDataset.load: Error reading file:" + str(e))
            
            
        elif _extension.lower() == ".odt":
            from ezodf import newdoc, Paragraph, Heading, Sheet
            raise Exception("SpreadsheetDataset.load: Open Document Spreadsheet not implemented yet.")

        else:
            raise Exception("SpreadsheetDataset.load: Unsupported file type \"" + _extension +"\"")
    
    def save (self):
        """Save data. Not implemented yet"""
        
        

    
        