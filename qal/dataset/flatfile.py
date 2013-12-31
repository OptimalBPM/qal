'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Custom_Dataset

import csv

class Flatfile_Dataset(Custom_Dataset):
 
    """This class loads a flat file into an array."""
    delimiter = None
    filename = None
    has_header = None
    csv_dialect = None
    quoting = None
    escapechar = None
    lineterminator = None
    quotechar = "\""
    skipinitialspace = None
    
    field_names = None
    
    def __init__(self, _delimiter = None, _filename = None, _has_header = None, _csv_dialect = None, _resource = None, _quoting = None):
        """Constructor"""
        super(Flatfile_Dataset, self ).__init__() 
       
        if _resource != None:
            self.read_resource_settings(_resource)
        else:
            if _delimiter != None: 
                self.delimiter = _delimiter
            else:  
                self.delimiter = None    
            if _filename != None: 
                self.filename = _filename
            else:
                self.filename = None      
            if _has_header != None: 
                self.has_header = _has_header
            else:
                self.has_header = None
                  
            if _csv_dialect != None: 
                self.csv_dialect = _csv_dialect
            else:
                self.csv_dialect = None      
             
            if _quoting != None: 
                self.quoting = _quoting
            else:
                self.quoting = None      

        
    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'FLATFILE':
            raise Exception("Flatfile_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename =    _resource.data.get("filename")
        self.delimiter =   _resource.data.get("delimiter")
        self.has_header =  bool(_resource.data.get("has_header"))
        self.csv_dialect = _resource.data.get("csv_dialect")
        self.quoting    = _resource.data.get("quoting")
        self.escapechar    = _resource.data.get("escapechar")
        self.lineterminator = _resource.data.get("lineterminator")
        self.quotechar = _resource.data.get("quotechar") or '"'       
        self.skipinitialspace = _resource.data.get("skipinitialspace")
         
    def _quotestr_to_constants(self, _str):
        if _str == None:
            return csv.QUOTE_NONE
        elif _str.upper() == "MINIMAL":
            return csv.QUOTE_MINIMAL
        elif _str.upper() == "ALL":
            return csv.QUOTE_ALL
        elif _str.upper() == "NONNUMERIC":
            return csv.QUOTE_NONNUMERIC
        else:
            raise Exception("Error in _quotestr_to_constants: " + str(_str) + " is an invalid quotestr.")
            

    def load(self):
        """Load data"""
        print("Flatfile_Dataset.load: Filename='" + str(self.filename) + "', Delimiter='"+str(self.delimiter)+"'")
        
        _file = open(self.filename, 'r')
        _reader = csv.reader(_file, 
                             delimiter=self.delimiter, 
                             quoting=self._quotestr_to_constants(self.quoting),
                             quotechar = self.quotechar,
                             skipinitialspace = self.skipinitialspace
                             )
        _first_row = True
        self.data_table = []
        for _row in _reader:
            # Save header row if existing.
            if (_first_row and self.has_header == True):
                self.field_names = [_curr_col.replace("'", "").replace("\"", "") for _curr_col in _row]
                print("self.field_names :" + str(self.field_names))
                _first_row = False
            else:
                self.data_table.append(_row)
                    
            
        _file.close()    
        if (self.has_header == False):
            for _curr_idx in range(0,len(_reader[0])):
                self.field_names.append("Field_"+ str(_curr_idx))   
            
        return self.data_table   
    


    
    def save(self, _save_as = None):
        """Save data"""

        print("Flatfile_Dataset.save: Filename='" + str(self.filename) + "', Delimiter='"+str(self.delimiter)+"'")
        
        if _save_as:
            _filename = _save_as
        else:
            _filename = self.filename
            
        _file = open(_filename, 'w')

        # Work around silly line terminator default, has to be set if passed.
        if self.lineterminator:
            _writer = csv.writer(_file,                              
                             delimiter=self.delimiter, 
                             quoting=self._quotestr_to_constants(self.quoting),
                             quotechar = self.quotechar,
                             skipinitialspace = self.skipinitialspace,
                             lineterminator = self.lineterminator
                             )
        else:
            _writer = csv.writer(_file,                              
                             delimiter=self.delimiter, 
                             quoting=self._quotestr_to_constants(self.quoting),
                             quotechar = self.quotechar,
                             skipinitialspace = self.skipinitialspace
                             )

            
        if self.has_header == True:
            _writer.writerow(self.field_names)
    
            
        _writer.writerows(self.data_table)
            
        _file.close()