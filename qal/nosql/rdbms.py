'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.nosql.custom import Custom_Dataset

class RDBMS_Dataset(Custom_Dataset):
 
    """This class represent an dataset from external RDBMS database server"""
    
    _dal = None
    
    def __init__(self):
        """Constructor"""
        super(RDBMS_Dataset, self ).__init__()
        
        
        
    def load(self):
        """Load data. Not implemented."""
        pass
        