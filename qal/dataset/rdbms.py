'''
Created on Jan 8, 2012

@author: Nicklas Boerjesson
'''

from qal.dal.dal_types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.sql_utils import db_specific_object_reference
from qal.sql.sql import Verb_INSERT, Verb_DELETE, Verb_UPDATE, Parameter_Conditions
from qal.sql.sql import Parameter_Source, Parameter_Identifier, Parameter_Parameter,Parameter_Condition, Parameter_Assignment
from qal.dal.dal import Database_Abstraction_Layer
from qal.sql.sql_base import SQL_List

from qal.sql.sql_macros import make_insert_sql_with_parameters

from qal.dataset.custom import Custom_Dataset

class RDBMS_Dataset(Custom_Dataset):
 
    """The RDMBS Data set holds a two-dimensional array of data, typically representing a table in a database.
    If the data set is not a table but based on a more complex query, data will not be possibly to apply to it."""
    
    _dal = None
    
    def __init__(self, _resource = None):
        '''
        Constructor
        '''
        super(RDBMS_Dataset, self ).__init__()
        
        if _resource != None:
            self.read_resource_settings(_resource)        

    def _structure_init(self):
        """Initializes the SQL queries that data is to be applied to."""
        
        
        print("XPath_Dataset._structure_init")
        super(RDBMS_Dataset, self)._structure_init()


    def _rdbms_init_deletes(self, _delete_list):
        """Generates a Verb_DELETE instance populated with the indata"""
        

                 
        _source = Parameter_Source()
        _source.expression.append(Parameter_Identifier(self.dest_table))
        
        # Add the WHERE statement
        for _field_idx in self.key_fields:
            _new_cond = Parameter_Condition(_left = Parameter_Identifier(_identifier= self.dest_field_names[_field_idx]), 
                                            _right = Parameter_Parameter(_datatype = self.dest_field_types[_field_idx]), 
                                             _operator = '=', _and_or = 'AND')
            _source.conditions.append(_new_cond)
        
        # Make the Verb_DELETE skeleton
        _delete = Verb_DELETE()
        _delete.sources.append(_source)
        
        # Fetch the resource
        _dal = Database_Abstraction_Layer(_resource = self.dest_resource)
        self._structure_delete_sql = _delete.as_sql(_dal.db_type)


    
    def _rdbms_init_inserts(self, _insert_list):
        """Generates a Verb_INSERT instance populated with the indata"""

        # Create a DAL for the destination resource, also we need to know the database type       
                
        _dal = Database_Abstraction_Layer(_resource = self.dest_resource)   
        
        self._structure_insert_sql = make_insert_sql_with_parameters(self.dest_table, self.dest_field_names, _dal.db_type, self.dest_field_types)


        
    def _rdbms_init_updates(self, _update_list):
        """Generates DELETE and INSERT instances populated with the indata """
        
        # Add assignments to all fields except the key fields and add conditions for all key fields.
                
        _field_names_ex_keys = []
        _field_names_ex_keys_datatypes = []
        _key_field_names = []
        _key_field_datatypes = []
        
        # Create lists of field names and types excluding and including keys
        for _curr_field_idx in range(len(self.dest_field_names)):
            if _curr_field_idx in self.key_fields:
                _key_field_names.append(self.dest_field_names[_curr_field_idx])
                _key_field_datatypes.append(self.dest_field_types[_curr_field_idx])
            else:
                _field_names_ex_keys.append(self.dest_field_names[_curr_field_idx])
                _field_names_ex_keys_datatypes.append(self.dest_field_types[_curr_field_idx])
        
        
        _assignments = SQL_List("Parameter_Assignment")
        
        # Instantiate the assignments
                
        for _curr_field_idx in range(len(_field_names_ex_keys)):
            _left = Parameter_Identifier(_identifier = _field_names_ex_keys[_curr_field_idx])
            _right = Parameter_Parameter(_datatype = _field_names_ex_keys_datatypes[_curr_field_idx])
            _assignments.append(Parameter_Assignment(_left, _right))


        # Create the WHERE conditions.

        _conditions = Parameter_Conditions()    
        
        for _curr_field_idx in range(len(_key_field_names)):
            _left = Parameter_Identifier(_identifier = _key_field_names[_curr_field_idx])
            _right = Parameter_Parameter(_datatype = _key_field_datatypes[_curr_field_idx])
            _conditions.append(Parameter_Condition( _left, _right, _operator = "="))
        
        # Specify target table
            
        _table_identifier =  Parameter_Identifier(_identifier = self.dest_table)
        
        # Create Verb_UPDATE instance with all parameters
        
        _update = Verb_UPDATE(_table_identifier = _table_identifier, _conditions = _conditions, _assignments = _assignments)            
        
        # Generate the SQL with all parameter place holders
        
        self._structure_update_sql = self._update.as_sql(self._dal.db_type)        
       
   
             
    def _structure_insert_row(self, _row_idx, _row_data):
        """Override parent to add SQL handling"""
        _execute_many_data = self._extract_data_columns_from_diff_row(range(len(self.dest_field_names)), _row_data)
        
        # Create a DAL for the destination resource        
                
        _dal = Database_Abstraction_Layer(_resource = self.dest_resource)   
         
        # Apply and commit changes to the structure
        
        _dal.executemany(self._structure_insert_sql, _execute_many_data)  
        _dal.commit()
        # Call parent
        super(RDBMS_Dataset, self)._structure_insert_row(_row_idx,_row_data)
        
    def _structure_update_row(self, _row_idx, _row_data):
        """Override parent to add SQL handling"""
        # To satisfy the Verb_UPDATE instance, create a two-dimensional array, leftmost columns are data, rightmost are keys.
                 
        _field_idx_ex_keys = list(set(range(len(self.dest_field_names))) - set(self.key_fields))
        _execute_data = self._extract_data_columns_from_diff_row(_field_idx_ex_keys + self.key_fields, _row_data)
        
        

        
        # Apply and commit changes to the database        
        self._dal.executemany(self._structure_update_sql, _execute_data)  
        self._dal.commit()
        # Call parent
        super(RDBMS_Dataset, self)._structure_update_row(_row_idx,_row_data)

    def _structure_delete_row(self, _row_idx):
        """Override parent to add SQL handling"""
                
        # Extract the key data
        _key_values = self._extract_data_columns_from_diff_row(self.key_fields, self.data_table[_row_idx])
                # Make the deletes
        self._dal.executemany(self._structure_delete_sql, [_key_values])
        self._dal.commit()
        # Call parent
        super(RDBMS_Dataset, self)._structure_delete_row(_row_idx)
        #self.data_table.pop(_row_idx)
                
    def load(self):
        
        pass
    
    def save (self):
        """Save data. Not implemented as it is not needed in the RDBMS descendant"""
        pass
    