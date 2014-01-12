"""
Created on Sep 26, 2013

@author: Nicklas Boerjesson
"""


from qal.sql.sql import Verb_CREATE_TABLE, Verb_SELECT, Verb_INSERT, Verb_DELETE, Verb_UPDATE, Verb_DROP_TABLE, SQL_List, \
    Parameter_ColumnDefinition,Parameter_Identifier, Parameter_Source
    
from qal.sql.utils import datatype_to_parameter


def make_column_definitions(_field_names, _field_types):
    _columns = SQL_List()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(Parameter_ColumnDefinition(_name = _field_names[_field_counter], _datatype = _field_types[_field_counter]))
    return _columns

def create_table_skeleton(_table_name, _field_names, _field_types):
    """Creates a sql for creating tables."""
    _columns = make_column_definitions(_field_names, _field_types)
    _CREATE = Verb_CREATE_TABLE(_name = _table_name, _columns = _columns)
    _CREATE._temporary = True
    return _CREATE


def make_column_identifiers(_field_names):
    """Create columns idenfiers from a list of field names."""
    _columns = SQL_List()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(Parameter_Identifier(_identifier = _field_names[_field_counter]))
    return _columns

def make_delete_skeleton(_table_name, _key_fields):
    _destination_identifier = Parameter_Identifier(_identifier = _table_name)
    
    _column_identifiers = make_column_identifiers(_key_fields)
    return Verb_DELETE()

def make_update_skeleton(_table_name):
    _table_identifier = Parameter_Identifier(_identifier = _table_name)
    return Verb_UPDATE(_table_identifier =  _table_identifier)

def make_insert_sql_with_parameters(_table_name, _field_names, _db_type, _field_types):
    """Make a prepared statement-type INSERT INTO ...VALUES-SQL."""
    _destination_identifier = Parameter_Identifier(_identifier = _table_name)
    _column_identifiers = make_column_identifiers(_field_names)
    _insert = Verb_INSERT(_destination_identifier = _destination_identifier, _column_identifiers = _column_identifiers)
    _refs = []
    # The VALUES-part of INSERT INTO looks the same on all platforms and we don't have to care about escaping.
    for _curr_idx in range(0, len(_field_names)):
        _refs.append(datatype_to_parameter(_db_type, _field_types[_curr_idx]))
    
    _values_sql = "VALUES (" +", ".join(_refs) + ")"
    
    return _insert.as_sql(_db_type) + _values_sql

def copy_to_table(_dal, _values, _field_names, _field_types, _table_name, _create_table = None, _drop_existing = None):
    """Move datatable into a table on the resource, return the table name. """
        
    if _drop_existing == True:
        try:
            _dal.execute(Verb_DROP_TABLE(_table_name).as_sql(_dal.db_type))
            _dal.commit()
        except Exception as e:
            print("copy_to_table - Ignoring error when dropping the table \"" + _table_name + "\": " + str(e)  )
                    
    if _create_table == True:    
        # Always create temporary table even if it ends up empty.
        _create_table_sql = create_table_skeleton(_table_name, _field_names, _field_types).as_sql(_dal.db_type)    
        print("Creating " + _table_name + " table..\n"+_create_table_sql)
        _dal.execute(_create_table_sql)
        _dal.commit()
        
    if len(_values) == 0:
        print("copy_to_table: No source data, inserting no rows.")
    else:        
        _insert_sql = make_insert_sql_with_parameters(_table_name, _field_names, _dal.db_type, _field_types)
        
        print("Inserting " + str(len(_values)) + " rows (" + str(len(_values[0])) + " columns)")
        _dal.executemany(_insert_sql, _values)
        _dal.commit()
        
    return _table_name

    
def select_all_skeleton(_table_name):
    """Returns a "SELECT * FROM _table_name"-structure. """
    _expression = Parameter_Identifier(_table_name)
    _source = Parameter_Source(_expression, _conditions = None, _alias = None, _join_type = None)
    _select = Verb_SELECT(_fields = None, _sources = [_source], _operator =  "AND")
    
    return _select
            
   
