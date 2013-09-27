'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.nosql.custom import Custom_Dataset
from qal.common.resources import Resource
from qal.dal.dal import Database_Abstraction_Layer
from qal.dal.dal_types import string_to_db_type
#from qal.sql.sql import Verb_CREATE_TABLE


class RDBMS_Dataset(Custom_Dataset):
 
    """This class represent a dataset from an RDBMS database server
    It 
    """
    
    _dal = None
    _resource = None
    SQL = None
    field_types = None
    field_names = None
    
    def __init__(self, _resource = None, _SQL = None):
        """Constructor"""
        super(RDBMS_Dataset, self ).__init__()
        self._dal = Database_Abstraction_Layer()
        if _resource:
            self.connect_resource(_resource)
            
        if _SQL:
            self.SQL = _SQL
        
    def connect_resource(self, _resource, _dest = None):
        """Is this pointless? Assign resources to a DAL instance"""

        if _dest == None:
            _dest = self._dal
            self._resource = _resource
        
        _dest.read_resource_settings(_resource)
        _dest.connect_to_db()        

            
        
    def load(self):
        print("Querying using " + str(self._resource) + "  " + self._resource.caption + " Server type : " + self._resource.data.get("db_type"))
        
        self.data_table = self._dal.query(self.SQL)
        self.field_names = self._dal.field_names
        self.field_types = self._dal.field_types
        
        

