'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

from qal.nosql.custom import Custom_Dataset

def xpath_data_formats():
    return ["XML", "XHTML", "HTML"] # "UNTIDY_HTML"]

class XPath_Dataset(Custom_Dataset):
    """This class implements data formats that are possible to query via XPath.
    Currently XML and HTML are implemented(XHTML isn't tested). 
    Untidy HTML will be implemented using the Beautiful Soup library.
    """

    filename = None
    """The XPath of the "row" nodes of the data set"""
    rows_xpath = None
    """The data format"""
    xpath_data_format = None
    
    field_names = None
    field_types = None
    field_xpaths  = None

    def __init__(self, _filename = None, _rows_xpath = None,  _resource = None):

        """Constructor"""
        super(XPath_Dataset, self ).__init__()
        
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


    def read_resource_settings(self, _resource):
  
        if _resource.type.upper() != 'XPATH':
            raise Exception("XPath_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        self.filename   =   _resource.data.get("filename")
        self.rows_xpath =   _resource.data.get("rows_xpath")
        self.xpath_data_format =  _resource.data.get("xpath_data_format")
        self.field_names = _resource.data.get("field_names")
        self.field_xpaths = _resource.data.get("field_xpaths")       
        self.field_types = _resource.data.get("field_types")  
              
    def file_to_tree(self, _data_format, _reference):
        print("format_to_tree : " + _data_format)
        if _data_format == 'HTML':
            from lxml import html
            return html.parse(_reference)
        if _data_format == 'XML':
            from lxml import etree
            return etree.parse(_reference)
        else:
            raise Exception("file_to_tree: " + _data_format + " is not supported")
            
    

        
    def load(self):
        """Parse file, apply root XPath to iterate over and then collect field data via field_xpaths."""
        print("Loading : " + self.filename)
        
        try:
            _tree = self.file_to_tree(self.xpath_data_format, self.filename)
        except Exception as e:
            raise Exception("XPath_Dataset.load - error parsing " + self.xpath_data_format + " file : " + str(e))
        
        #_root_nodes = _tree.xpath("/html/body/form/table/tr[4]/td[2]/table/tr[2]/td/table[2]/tr/td/table/tr[10]/td/table/tr")
        _root_nodes = _tree.xpath(self.rows_xpath)
        _data = []
        for _curr_row in _root_nodes:
            _row_data = []
            for _field_idx in range(0, len(self.field_names)):
                _item_data = _curr_row.xpath(self.field_xpaths[_field_idx])
                if len(_item_data) > 0:
                    
                    _row_data.append(self.cast_text_to_type(_item_data[0].text, _field_idx))
                else:
                    _row_data.append("")
            
            print(str(_row_data))
            _data.append(_row_data)
            
        return _data        

        