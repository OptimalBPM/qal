'''
Created on Dec 28, 2013

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Custom_Dataset



class Spreadsheet_Dataset(Custom_Dataset):
 
    """The Spreadsheet dataset holds a spreadsheet, currently only XLS."""
    
    has_header = None
    """True if data begins with a header row containing field names."""
    filename = None
    """The name of the file."""
    field_names = None
    """The names of the fields(if applicable, see has_header)."""
    sheet_name = None
    """The name of the worksheet to use, typically "Sheet1"."""
    
    
    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'SPREADSHEET':
            raise Exception("Spreadsheet_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename =    _resource.data.get("filename")
        self.delimiter =   _resource.data.get("delimiter")
        self.has_header =  bool(_resource.data.get("has_header"))
        self.sheet_name = _resource.data.get("sheet_name")
        self.x_offset = _resource.data.get("x_offset")
        self.y_offset = _resource.data.get("y_offset")
    
    def __init__(self, _filename = None, _has_header = None, _resource = None, _sheet_name = None, _x_offset = None, _y_offset = None):
        '''
        Constructor
        '''
        super(Spreadsheet_Dataset, self ).__init__()
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
                    self.y_offset = 1
              
                if self.has_header:
                    self.field_names = _sheet.row_values(self.y_offset, start_colx=self.x_offset, end_colx= _sheet.ncols)
                    _has_header_offset = 1
                else:
                    _has_header_offset = 0

                self.data_table = []

                for _curr_y in range(self.y_offset + _has_header_offset, _sheet.nrows):
                    self.data_table.append(_sheet.row_values(_curr_y, start_colx=0, end_colx= _sheet.ncols))

            except IOError as e:
                raise Exception("Spreadsheet_Dataset.load: Error reading file:" + str(e))
            
            
        elif _extension.lower() == ".odt":
            from ezodf import newdoc, Paragraph, Heading, Sheet
            raise Exception("Spreadsheet_Dataset.load: Open Document Spreadsheet not implemented yet.")

        else:
            raise Exception("Spreadsheet_Dataset.load: Unsupported file type \"" + _extension +"\"")
    
    def save (self):
        """Save data. Not implemented yet"""
        
        

    
        