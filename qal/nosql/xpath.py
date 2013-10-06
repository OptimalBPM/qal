'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

from qal.nosql.custom import Custom_Dataset

def data_formats():
    return ['HTML1.0', 'XML', 'XHTML']

class XPath_Dataset(Custom_Dataset):
    """This class implements data formats that are possible to query via XPath.
    These include XML and HTML 1.0. Untidy HTML files are parsed using the Beautiful Soup 4 library.
    """
    
    
    
    
    
    
    _filename = None
    """The XPath of the "row" nodes of the data set"""
    _rows_xpath = None
    """Each field has an XPath going from the row nodes"""
    _field_xpaths = None
    """The corresponding field names"""
    _field_names = None
    """The corresponding field types"""
    _field_types = None
    
    def __init__(self, _filename = None, _rows_xpath = None, _field_xpaths = None, _field_names = None, _field_types = None, _resource = None):

        if _resource:
            self.read_resource_settings(_resource)
        else:
            if _filename != None: 
                self.filename = _filename
            else:
                self.filename = None                
            if _rows_xpath != None: 
                self.rows_xpath = _rows_xpath
            else:
                self.rows_xpath = None         
            if _field_xpaths != None: 
                self.field_xpaths = _field_xpaths
            else:
                self._field_xpaths = None         
            if _field_names != None: 
                self.field_names = _field_names
            else:
                self.field_names = None         
            if _field_types != None: 
                self.field_types = _field_types
            else:
                self.field_types = None         
                        
        """Constructor"""
        super(XPath_Dataset, self ).__init__()
        
    def read_resource_settings(self, _resource):
  
        if _resource.type.upper() != 'XPATH':
            raise Exception("XPath_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.rows_xpath =   _resource.data.get("rows_xpath")
        self.data_format =  _resource.data.get("data_format")
        
    def load(self):
        # TODO: Implement XPath parsing.
        import xml.etree.ElementTree as ET
        pass
        
        # TODO: Flatten result
        