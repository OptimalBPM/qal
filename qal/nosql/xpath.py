'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

from qal.nosql.custom import Custom_Dataset

def xpath_data_formats():
    return ["XML", "XHTML", "HTML", "UNTIDY_HTML"]

class XPath_Dataset(Custom_Dataset):
    """This class implements data formats that are possible to query via XPath.
    These include XML and HTML 1.0. Untidy HTML files are parsed using the Beautiful Soup 4 library.
    """

    filename = None
    """The XPath of the "row" nodes of the data set"""
    rows_xpath = None
    """The data format"""
    xpath_data_format = None

    def __init__(self, _filename = None, _rows_xpath = None,  _resource = None):

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

        """Constructor"""
        super(XPath_Dataset, self ).__init__()
        
    def read_resource_settings(self, _resource):
  
        if _resource.type.upper() != 'XPATH':
            raise Exception("XPath_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename   =   _resource.data.get("filename")
        self.rows_xpath =   _resource.data.get("rows_xpath")
        self.xpath_data_format_format =  _resource.data.get("xpath_data_format")
        
        
    def format_to_tree(self, _data_format, _data):
        if _data_format == 'HTML':
            import lxml.html
            return lxml.html.fromstring(_data)
            
        
    def load(self):
        # TODO: Implement XPath parsing.
        
        _data = self.get_data(self.filename)
        
        _tree = self.format_to_tree(self.xpath_data_format, _data)
        
        _root_nodes = _tree.xpath(self.rows_xpath)
        for _curr_row in _root_nodes:
            print(str(_curr_row))
        
        # TODO: Flatten result
        