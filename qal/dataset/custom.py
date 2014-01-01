"""
Created on Sep 14, 2012

@author: Nicklas Boerjesson
"""
from urllib.parse import quote
from datetime import datetime


from qal.dal.dal_types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.sql_utils import db_specific_object_reference
from qal.tools.diff import compare 

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
    
    _log_level = DATASET_LOGLEVEL_MEDIUM


    def __init__(self):
        """Constructor""" 
        self.field_names = []
        self.data_table = []
        self._log = [] 
        self._log_level = DATASET_LOGLEVEL_MEDIUM
        
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
        
    def log_update_row(self, _row_key, _old_row, _new_row):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            _field_diffs = []
            for _field_idx in range(len(_new_row)):
                if _old_row[_field_idx] != _new_row[_field_idx]:
                    _field_diffs.append(self.field_names[_field_idx] + " : " + quote(str(_old_row[_field_idx])) + " =>" + quote(str(_new_row[_field_idx]))) 
                
            self._log.append(self.__class__.__name__ + ".update;"+quote(str(_row_key)) + ";"+";" +"|".join(_field_diffs))

    
    def log_insert(self, _row_key, row_value):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".insert;"+quote(str(_row_key)) + ";"+quote(str(row_value)))

    def log_delete(self, _row_key, row_value):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".delete;"+quote(str(_row_key)) + ";"+quote(str(row_value)))
    def log_save(self, _filename):
        if self._log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".save;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()))
                
    def log_load(self, _filename):
        if self._log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".load;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()))
               
        
    def load(self):
        """Load the data"""
        raise Exception('Custom_Dataset.Load is not implemented in class: ' + self.classname)
        pass
    
    def save(self):
        """Save the data to the underlying target."""
        raise Exception('Custom_Dataset.Save is not implemented in class: ' + self.classname)

    
    def _structure_insert_row(self, _row_idx, _row_data):
        """Inserts a row at _row_idx in the self.data_table, containing _row_data\
        Overridden by subclasses"""
        self.data_table.insert(_row_idx,_row_data)
        
    def _structure_update_row(self, _row_idx, _row_data):
        """Updates the row at _row_idx in the self.data_table with _row_data.\
        Overridden by subclasses"""
        self.data_table[_row_idx] = _row_data

    def _structure_delete_row(self, _row_idx):
        """Deletes a row at _row_idx from the self.data_table.\
        Overridden by subclasses"""
        self.data_table.pop(_row_idx)
        
    def _structure_init(self):
        """Initialize underlying structure before applying data to it.\
        Overridden by subclasses."""
        pass

        
    def _structure_apply_merge(self, _insert, _update, _delete, _sorted_dest):
        
        #print("_insert: " + str(_insert))
        #print("Before apply: " + str(_sorted_dest))
        
        
        self._structure_init()

        _insert_idx = 0
        _delete_idx = 0
        _update_idx = 0
        
        self.data_table = _sorted_dest
        # Loop the sorted destination dataset and apply changes
        for _curr_row_idx in range(0, len(self.data_table)):
            _actual_row_idx = _curr_row_idx - _delete_idx + _insert_idx
            # print("_actual_row_idx = _curr_row_idx - _delete_idx + _insert_idx = " + 
            #      str(_curr_row_idx) + " - " +str(_delete_idx) + " + " + str(_insert_idx))

            # Make deletes
            if _delete and _delete_idx < len(_delete):
                if _delete[_delete_idx][1] == _curr_row_idx:
                    #print("deleting row " + str(_delete[_delete_idx][1])) 
                    self._structure_delete_row(_actual_row_idx)
                    _delete_idx+=1
            # Make updates
            if _update and _update_idx < len(_update):
                if _update[_update_idx][1] == _curr_row_idx:
                    #print("updating row " + str(_update[_update_idx][1])) 
                    self.log_update_row(_actual_row_idx, self.data_table[_actual_row_idx], _update[_update_idx][2])
                    self._structure_update_row(_actual_row_idx, _update[_update_idx][2])
                    _update_idx+=1
                    
            # Make inserts
            if _insert and len(_insert) > 0:
                
                # TODO: This while should insert these in reverse instead.
                while _insert_idx < len(_insert) and _insert[_insert_idx][1] == _curr_row_idx:
                    #print("inserting row " + str(_insert[_insert_idx][1]))
                    self._structure_insert_row(_actual_row_idx, _insert[_insert_idx][2]) 
                    _insert_idx+=1
        
        #print("After apply:  " + str(_sorted_dest))            
        return self.data_table        
    
    def apply_new_data(self, _new_data_table, _key_fields):
        _delete, _insert, _update, _dest_sorted = compare(
                                                          _left = _new_data_table, 
                                                          _right = self.data_table, 
                                                          _key_columns = _key_fields, 
                                                          _full = True)
        self.data_table = self._structure_apply_merge(_insert, _update, _delete, _dest_sorted)
        
        return self.data_table
                            
                
        
    
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
