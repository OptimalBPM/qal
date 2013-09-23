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
    SQL = None
    
    def __init__(self, _resource = None, _SQL = None):
        """Constructor"""
        super(RDBMS_Dataset, self ).__init__()
        self._dal = Database_Abstraction_Layer()
        if _resource:
            self.parse_resource(_resource)
            
        if _SQL:
            self.SQL = _SQL
        
    def parse_resource(self, _resource):
        if _resource.type.upper() != 'RDBMS':
            raise Exception("RDBMS_Dataset.parse_resource error: Wrong resource type")
        self._resource =            _resource
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
        print("Querying using " + str(self._resource) + "  " + self._resource.caption + " Server type : " + self._resource.data.get("db_type"))
        
        _rows = self._dal.query(self.SQL)
        
  
        for _row in _rows:
            for _col in _row:
                print(str(_col))
            
        """Load data. Not implemented."""
        pass
        