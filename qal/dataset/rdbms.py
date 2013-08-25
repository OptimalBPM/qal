'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Parameter_Custom_Dataset

class Parameter_RDBMS_Dataset(Parameter_Custom_Dataset):
 
    """This class represent an dataset from external RDBMS database server"""
    
    _dal = None
    
    def __init__(self):
        """Constructor"""
        super(Parameter_RDBMS_Dataset, self ).__init__()
        
        
        
    def load(self):
        """Load data. Not implemented."""
        pass
        