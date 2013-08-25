'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''

from qal.dataset.custom import Parameter_Custom_Dataset

class Parameter_XML_Dataset(Parameter_Custom_Dataset):
    """This class implements an XML as datasource"""
    def __init__(self):
        """Constructor"""
        super(Parameter_XML_Dataset, self ).__init__()
        
        
        
    def load(self):
        pass
        