"""
Created on Sep 14, 2012

@author: Nicklas Boerjesson
"""

from qal.dal.dal_types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.sql_utils import db_specific_object_reference
from urllib.parse import quote
from datetime import datetime

DATASET_LOGLEVEL_NONE = 0
DATASET_LOGLEVEL_LOW = 1
DATASET_LOGLEVEL_MEDIUM = 2
DATASET_LOGLEVEL_DETAIL = 3
DATASET_LOGLEVEL_ALL = 4


class Custom_Dataset(object):
    """This is the base class for all (external) data sets in QAL-
    Note: The fields are named like this to not appear as parameters in automatic generators like sql_xml.
    """

    field_names = []
    field_types = []
    data_table = [] 


    def __init__(self):
        """Constructor""" 
        self.field_names = []
        self.data_table = []
        self._log = [] 
        self.log_level = DATASET_LOGLEVEL_MEDIUM
        
    def cast_text_to_type(self, _text, _field_idx):
        try:
            if  self.field_types[_field_idx] == "integer":
                return int(_text)
            if  self.field_types[_field_idx] == "float":
                return float(_text)
            else:
                return _text
        except Exception as e:
            raise Exception("cast_text_to_type raised an error for \"" + _text +"\": " + str(e) )
        
    def log_update_field(self, _row_key, _fieldref, _old_val, _new_val):
        if self.log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".update;"+quote(str(_row_key)) + ";"+quote(str(_fieldref)) + ";"+quote(str(_old_val)) + ";"+quote(str(_new_val)))

    
    def log_insert(self, _row_key, row_value):
        if self.log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".insert;"+quote(str(_row_key)) + ";"+quote(str(row_value)))

    def log_delete(self, _row_key, row_value):
        if self.log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".delete;"+quote(str(_row_key)) + ";"+quote(str(row_value)))
    def log_save(self, _filename):
        if self.log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".save;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()))
                
    def log_load(self, _filename):
        if self.log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".load;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()))
               
        
    def load(self):
        """Load the data"""
        raise Exception('Custom_Dataset.Load is not implemented in class: ' + self.classname)
        pass
    
    def save(self):
        """Save the data"""
        raise Exception('Custom_Dataset.Save is not implemented in class: ' + self.classname)
        pass
        
    
    def as_sql(self, _db_type):
        """Generate SQL
        Either through a union SQL or into a temp table."""
        _result = []
        
        _add_field_names = (len(self.data_table) > 0) and (len(self.field_names) == len(self.data_table[0]))
        
        
        # TODO: If is i a large number of rows, make an insert into a temp table instead.
        for _row in self.data_table:
            _curr_row = []
            for _col_idx in range(len(_row)):
                _str_col = ''
                _col = _row[_col_idx]
                if (_col.lower() == ''):
                    _str_col = 'NULL'                    
                elif (_col.lower() in ['true', 'false']):
                    if (_col.lower() == 'true'):
                        if (_db_type == DB_POSTGRESQL):
                            _str_col = 'TRUE'
                        else:
                            _str_col = '\'1\''
                    else:
                        if (_db_type == DB_POSTGRESQL):
                            _str_col = 'FALSE'
                        else:
                            _str_col = '\'0\''
                else:
                    _str_col = _col 
                            
                if (_add_field_names):                    
                    _curr_row.append(_str_col + ' AS ' + db_specific_object_reference(self.field_names[_col_idx], _db_type))
                else:
                    _curr_row.append(_str_col)
            
            _add_field_names = False
                
            if (_db_type == DB_DB2):
                _result.append("SELECT " + ",".join(_curr_row) +' FROM sysibm.sysdummy1')
            elif (_db_type == DB_ORACLE):
                _result.append("SELECT " + ",".join(_curr_row) +' FROM DUAL')
            else:
                _result.append("SELECT " + ",".join(_curr_row))
            
                    
        return str("\nUNION\n".join(_result)) 
