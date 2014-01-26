'''
Created on Jan 8, 2012

@author: Nicklas Boerjesson
'''

from qal.dal.types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.utils import db_specific_object_reference
from qal.sql.sql import Verb_INSERT, Verb_DELETE, Verb_UPDATE, Parameter_Conditions
from qal.sql.sql import Parameter_Source, Parameter_Identifier, Parameter_Parameter,Parameter_Condition, Parameter_Assignment
from qal.dal.dal import Database_Abstraction_Layer
from qal.sql.macros import select_all_skeleton
from qal.sql.base import SQL_List

from qal.sql.macros import make_insert_sql_with_parameters

from qal.dataset.custom import Custom_Dataset

class RDBMS_Dataset(Custom_Dataset):
 
    """The RDMBS Dataset holds a two-dimensional array of data, typically representing a table in a database.
    If the data set is not a table but based on a more complex query, data will not be possible to apply to it."""
    
    dal = None
    """An instance of the Database Abstraction Layer(DAL)"""
    table_name = None
    """If set, all data in table "table_name" is loaded in its entirety into the data_table."""
    query = None
    """If set, and table_name is not, then the SQL statement contained is executed. 
    It is a text string. 

    .. todo::
    Make it possible for it to be a backend agnostic qal.sql.sql.VERB_SELECT object parsed from an XML structure.""" 

    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'RDBMS':
            raise Exception("RDBMS_Dataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)
        
        self.dal = Database_Abstraction_Layer(_resource = _resource)
        
        self.table_name = _resource.data.get("table_name")
        
        self.query =   _resource.data.get("query")


    def write_resource_settings(self, _resource):
        """TODO: The RDBMS resource type is a special case, as it is both a database connection and/or a dataset definition.
        Should this be?"""

        if self.dal is None:
            # Clear, as the DAL is not going to
            _resource.type = 'RDBMS'
            _resource.data = {}
        else:
            # Let the DAL go first
            self.dal.write_resource_settings(_resource)

        # Add own parameters
        _resource.data["table_name"] = self.table_name
        _resource.data["query"] = self.query

    
    def __init__(self, _resource = None):
        '''
        Constructor
        '''
        super(RDBMS_Dataset, self ).__init__()
        
        if _resource != None:
            self.read_resource_settings(_resource)        

    def _structure_init(self):
        """Initializes the SQL queries that data is to be applied to, starts a database transaction"""
        self._rdbms_init_deletes()
        self._rdbms_init_updates()
        self._rdbms_init_inserts()
        
        self.dal.start()

        super(RDBMS_Dataset, self)._structure_init()

    def _extract_data_columns_from_diff_row(self, _field_indexes, _diff_row):    
        """Extracts columns specified in _field_indexes from _diff_list"""
        _result = []

        for _curr_field in _field_indexes:
            _result.append(_diff_row[_curr_field])
        return _result        
    

    def _rdbms_init_deletes(self):
        """Generates a Verb_DELETE instance populated with the indata"""
        
        _source = Parameter_Source()
        _source.expression.append(Parameter_Identifier(self.table_name))
        
        # Add the WHERE statement
        for _field_idx in self._structure_key_fields:
            _new_cond = Parameter_Condition(_left = Parameter_Identifier(_identifier= self.field_names[_field_idx]), 
                                            _right = Parameter_Parameter(_datatype = self.field_types[_field_idx]), 
                                             _operator = '=', _and_or = 'AND')
            _source.conditions.append(_new_cond)
        
        # Make the Verb_DELETE skeleton
        _delete = Verb_DELETE()
        _delete.sources.append(_source)
        
        # Fetch the resource
        self._structure_delete_sql = _delete.as_sql(self.dal.db_type)


    
    def _rdbms_init_inserts(self):
        """Generates a Verb_INSERT instance populated with the indata"""

        self._structure_insert_sql = make_insert_sql_with_parameters(self.table_name, self.field_names,self.dal.db_type, self.field_types)

        
    def _rdbms_init_updates(self):
        """Generates DELETE and INSERT instances populated with the indata """
        
        # Add assignments to all fields except the key fields and add conditions for all key fields.
                
        _field_names_ex_keys = []
        _field_names_ex_keys_datatypes = []
        _key_field_names = []
        _key_field_datatypes = []
        
        # Create lists of field names and types excluding and including keys
        for _curr_field_idx in range(len(self.field_names)):
            if _curr_field_idx in self._structure_key_fields:
                _key_field_names.append(self.field_names[_curr_field_idx])
                _key_field_datatypes.append(self.field_types[_curr_field_idx])
            else:
                _field_names_ex_keys.append(self.field_names[_curr_field_idx])
                _field_names_ex_keys_datatypes.append(self.field_types[_curr_field_idx])
        
        
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
            
        _table_identifier =  Parameter_Identifier(_identifier = self.table_name)
        
        # Create Verb_UPDATE instance with all parameters
        
        _update = Verb_UPDATE(_table_identifier = _table_identifier, _conditions = _conditions, _assignments = _assignments)            
        
        # Generate the SQL with all parameter place holders
        
        self._structure_update_sql = _update.as_sql(self.dal.db_type)        
       
   
             
    def _structure_insert_row(self, _row_idx, _row_data):
        """Override parent to add SQL handling"""
        _execute_many_data = self._extract_data_columns_from_diff_row(range(len(self.field_names)), _row_data)
        
         
        # Apply and commit changes to the structure
        
        self.dal.executemany(self._structure_insert_sql, [_execute_many_data])

        self.log_insert("N/A in RDBMS", _row_data, "Destination table: " + self.table_name)
        # Call parent
        super(RDBMS_Dataset, self)._structure_insert_row(_row_idx,_row_data, _no_logging= True)
        
    def _structure_update_row(self, _row_idx, _row_data):
        """Override parent to add SQL handling"""
        # To satisfy the Verb_UPDATE instance, create a two-dimensional array, leftmost columns are data, rightmost are keys.
                 
        _field_idx_ex_keys = list(set(range(len(self.field_names))) - set(self._structure_key_fields))
        _execute_data = self._extract_data_columns_from_diff_row(_field_idx_ex_keys + self._structure_key_fields, _row_data)
        
        # Apply and commit changes to the database        
        self.dal.executemany(self._structure_update_sql,  [_execute_data])  

        self.log_update_row(_row_idx, self.data_table[_row_idx], _row_data, "Destination table: " + self.table_name)
        # Call parent
        super(RDBMS_Dataset, self)._structure_update_row(_row_idx,_row_data, _no_logging= True)

    def _structure_delete_row(self, _row_idx):
        """Override parent to add SQL handling"""
                
        # Extract the key data
        _key_values = self._extract_data_columns_from_diff_row(self._structure_key_fields, self.data_table[_row_idx])
        # Make the deletes
        self.dal.executemany(self._structure_delete_sql, [_key_values])

        self.log_delete(_key_values, self.data_table[_row_idx], "Destination table: " + self.table_name)

        # Call parent
        super(RDBMS_Dataset, self)._structure_delete_row(_row_idx, _no_logging= True)
        #self.data_table.pop(_row_idx)
                
    def load(self):
        if self.table_name and self.table_name != "":
            """Query all values from a table from a RDBMS resource"""
            self.data_table = self.dal.query(select_all_skeleton(self.table_name).as_sql(self.dal.db_type))
            self.field_names = self.dal.field_names
            self.field_types = self.dal.field_types
        else:
            raise Exception("RDBMS_Dataset.load(): data_table must be set.")    
        
        return self.data_table
    
    def save (self):
        """Save data. Commits the transaction"""
        self.dal.commit()
    