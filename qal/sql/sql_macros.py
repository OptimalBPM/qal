"""
Created on Sep 26, 2013

@author: Nicklas Boerjesson
"""

from qal.sql.sql import Verb_CREATE_TABLE, Verb_INSERT, SQL_List, Parameter_ColumnDefinition,Parameter_Identifier


def make_column_definitions(_field_names, _field_types):
    _columns = SQL_List()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(Parameter_ColumnDefinition(_name = _field_names[_field_counter], _datatype = _field_types[_field_counter]))
    return _columns

def create_temporary_table(_table_name, _field_names, _field_types, _db_type):

    _columns = make_column_definitions(_field_names, _field_types)
    _CREATE = Verb_CREATE_TABLE(_name = _table_name, _columns = _columns)
    _CREATE._temporary = True
    return _CREATE.as_sql(_db_type)


def make_column_identifiers(_field_names):
    _columns = SQL_List()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(Parameter_Identifier(_identifier = _field_names[_field_counter]))
    return _columns

def make_insert_skeleton(_table_name, _field_names):
    _destination_identifier = Parameter_Identifier(_identifier = _table_name)
    _column_identifiers = make_column_identifiers(_field_names)
    return Verb_INSERT(_destination_identifier = _destination_identifier, _column_identifiers = _column_identifiers)

def copy_to_temp_table(_dal, _values, _field_names, _field_types, _table_name = None):
    """Move datatable into a temp table on the resource, return the table name. """
    if _table_name == None:
        _table_name = "test"
        
    # Always create temporary table even if it ends up empty.
    _create_table_sql = create_temporary_table(_table_name, _field_names, _field_types, _db_type = _dal.db_type)    
    
    print("Creating temporary " + _table_name + " table..\n"+_create_table_sql)
    _dal.execute(_create_table_sql)
    if len(_values) == 0:
        print("copy_to_temp_table: No source data, inserting no rows.")
    else:        
        _insert = make_insert_skeleton(_table_name = _table_name, _field_names = _field_names)
         
        _refs = []
        for _curr_idx in range(0, len(_field_names)):
            if _field_types[_curr_idx] in ["float", "integer"]:
                _refs.append("%d")
            else:
                _refs.append("%s")

#            else:
#                raise Exception("copy_to_temp_table -error: Invalid field type: " + _field_types[_curr_idx])
        
        _values_sql = "VALUES (" +", ".join(_refs) + ")"
        
        print("Inserting " + str(len(_values)) + " rows (" + str(len(_values[0])) + " columns)")
        _dal.executemany(_insert.as_sql(_dal.db_type) + _values_sql, _values)
        
    return _table_name
    
        
   
