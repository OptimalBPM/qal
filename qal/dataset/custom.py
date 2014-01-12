"""
Created on Sep 14, 2012

@author: Nicklas Boerjesson
"""
from urllib.parse import quote
from datetime import datetime


from qal.dal.types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.utils import db_specific_object_reference
from qal.common.strings import empty_if_none
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
    """These are, if applicable, the field names of the dataset"""
    field_types = []
    """The data types of the fields"""
    data_table = [] 
    """The actual data, in the form of a two-dimensional array"""
    
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
        
    def log_update_row(self, _row_key, _old_row, _new_row, _comment = None):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            _field_diffs = []
            for _field_idx in range(len(_new_row)):
                if _old_row[_field_idx] != _new_row[_field_idx]:
                    _field_diffs.append(self.field_names[_field_idx] + " : " + quote(str(_old_row[_field_idx])) + " =>" + quote(str(_new_row[_field_idx]))) 
                
            self._log.append(self.__class__.__name__ + ".update;"+quote(str(_row_key)) + ";"+";" +"|".join(_field_diffs) + empty_if_none(";"+ _comment, _comment))

    
    def log_insert(self, _row_key, _row_data, _comment = None):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".insert;"+quote(str(_row_key)) + ";"+quote(str(_row_data)) + empty_if_none(";"+ _comment, _comment))

    def log_delete(self, _row_key, _row_data, _comment = None):
        if self._log_level >= DATASET_LOGLEVEL_DETAIL:
            self._log.append(self.__class__.__name__ + ".delete;"+quote(str(_row_key)) + ";"+quote(str(_row_data)) + empty_if_none(";"+ _comment, _comment))

    def log_save(self, _filename, _comment = None):
        if self._log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".save;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()) + empty_if_none(";"+ _comment, _comment))
                
    def log_load(self, _filename, _comment = None):
        if self._log_level >= DATASET_LOGLEVEL_LOW:
            self._log.append(self.__class__.__name__ + ".load;"+quote(str(_filename)) + ";" + str(datetime.now().isoformat()) + empty_if_none(";"+ _comment, _comment))
               
        
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
        

        if _insert:
            _insert_idx = len(_insert) - 1
        else:
            _insert_idx = -1
            
        if _delete:
            _delete_idx = len(_delete) - 1
        else:
            _delete_idx = -1
            
        if _update:
            _update_idx = len(_update) - 1
        else:
            _update_idx = -1

        
        self.data_table = _sorted_dest
        # Loop the sorted destination dataset backwards and apply changes
        for _curr_row_idx in range(len(self.data_table), -1, -1):
  
            # Make deletes
            if _delete_idx > -1:
                if _delete[_delete_idx][1] == _curr_row_idx:
                    #print("deleting row " + str(_delete[_delete_idx][1])) 
                    self._structure_delete_row(_curr_row_idx)
                    _delete_idx-= 1
            # Make updates
            if _update_idx > -1:
                if _update[_update_idx][1] == _curr_row_idx:
                    #print("updating row " + str(_update[_update_idx][1])) 
                    self._structure_update_row(_curr_row_idx, _update[_update_idx][2])
                    _update_idx-= 1
                    
            # Make inserts
            if _insert_idx > -1:
                
                # TODO: This while should insert these in reverse instead.
                if _insert[_insert_idx][1] == _curr_row_idx:
                    #print("inserting row " + str(_insert[_insert_idx][1]))
                    self._structure_insert_row(_curr_row_idx, _insert[_insert_idx][2]) 
                    _insert_idx-= 1
        
        #print("After apply:  " + str(_sorted_dest))            
        return self.data_table        
    
    def apply_new_data(self, _new_data_table, _key_fields):
        """This function applies a new data table unto the existing, matches are made using the key fields.
      
        
        :parameter 2D-list _new_data_table: A two-dimensional list contains the data. Must match the existing column-wise.
        :parameter list _key_fields: An array with the indices of the fields that should be used to match source rows to destination rows. 
        
        -- note:
            If there are different data types in the _new_data_table columns and the existing dataset.data_table, they will be considered different and be updated. 
            It is also possible that the keys will not match. So cast these before applying.
        """
         
        self._structure_key_fields = _key_fields
        
        # Some datasets needs to initialize the underlying structure to work, and work efficiently
        self._structure_init()        
        
        # Compare the source and destination table to generate diff sets
        _delete, _insert, _update, _dest_sorted = compare(
                                                          _left = _new_data_table, 
                                                          _right = self.data_table, 
                                                          _key_columns = _key_fields, 
                                                          _full = True)
        
        # Merge the data into the structure. 
        # Note: in RDBMS_Dataset, this means writing to the underlying database, since there is no in-memory structure.
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
