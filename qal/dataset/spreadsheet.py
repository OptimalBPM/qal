'''
Created on Dec 28, 2013

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Custom_Dataset

import os
from xlrd import Workbook

class Spreadsheet_Dataset(Custom_Dataset):
 
    """The Spreadsheet dataset holds a spreadsheet, currently only XLS."""
    
    _dal = None
    
    def __init__(self):
        '''
        Constructor
        '''
        super(Spreadsheet_Dataset, self ).__init__()
        
        
        
    def load(self):
        """Load data. Not implemented as it is not needed in the matrix descendant"""

        import os.path
        _extension = os.path.splitext(self.filename)[1]
        
        #TODO: Check file type
        if _extension.lower() in ["xls", "xlsx"]:
            from xlrd import open_workbook
            _workbook = open_workbook('my_workbook.xls')
        elif _extension.lower() == "odt":
            from ezodf import newdoc, Paragraph, Heading, Sheet
            raise Exception("Spreadsheet_Dataset.load: Open Document Spreadsheet not implemented yet.")

        else:
            raise Exception("Spreadsheet_Dataset.load: Unsupported file type \"" + _extension +"\"")
            
        pass
        
        pass
    
    def save (self):
        """Save data. Not implemented as it is not needed in the matrix descendant"""
        
        

    
        