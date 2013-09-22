'''
Created on Sep 14, 2012

@author: Nicklas Boerjesson
'''


from qal.nosql.custom import Custom_Dataset
from qal.common.resources import Resource
from qal.dal.dal import Database_Abstraction_Layer
from qal.dal.dal_types import string_to_db_type


class RDBMS_Dataset(Custom_Dataset):
 
    """This class represent a dataset from an RDBMS database server
    It 
    """
    
    _dal = None
    _resource = None
    
    def __init__(self, _resource = None):
        """Constructor"""
        super(RDBMS_Dataset, self ).__init__()
        self._dal = Database_Abstraction_Layer()
        if _resource:
            self.parse_resource(_resource)
        
    def parse_resource(self, _resource):
        if _resource.type.upper() != 'RDBMS':
            raise Exception("RDBMS_Dataset.parse_resource error: Wrong resource type")
        
        self._dal.db_type =         string_to_db_type(_resource.data.get("db_type"))
        self._dal.db_server =       _resource.data.get("server")
        self._dal.db_databasename = _resource.data.get("database")
        self._dal.db_instance =     _resource.data.get("instance")
        self._dal.db_username =     _resource.data.get("username")
        self._dal.db_password =     _resource.data.get("password")
        self._dal.DB_Port =         _resource.data.get("DB_Port")
        self._dal.autocommit =      _resource.data.get("autocommit")
        
        
        self._dal.connect_to_db()
            
        
    def load(self):
        """Load data. Not implemented."""
        pass
        