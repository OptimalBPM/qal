'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.dataset.custom import Parameter_Custom_Dataset

class Parameter_RDBMS_Dataset(Parameter_Custom_Dataset):
 
    '''
    classdocs
    '''
    
    _dal = None
    
    def __init__(self):
        '''
        Constructor
        '''
        super(Parameter_RDBMS_Dataset, self ).__init__()
        
        
        
    def load(self):
        pass
        