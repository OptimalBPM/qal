'''
Created on Dec 28, 2013

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Custom_Dataset

class Spreadsheet_Dataset(Custom_Dataset):
 
    """The matrix dataset holds a two-dimensional array of data"""
    
    _dal = None
    
    def __init__(self):
        '''
        Constructor
        '''
        super(Spreadsheet_Dataset, self ).__init__()
        
        
        
    def load(self):
        """Load data. Not implemented as it is not needed in the matrix descendant"""
        pass
    
    def save (self):
        """Save data. Not implemented as it is not needed in the matrix descendant"""
        pass
    
        