'''
Created on Sep 26, 2013

@author: Nicklas Boerjesson
'''

from qal.sql.sql import Verb_CREATE_TABLE, Verb_INSERT, SQL_List, Parameter_ColumnDefinition,Parameter_Identifier
from qal.dal.dal import Database_Abstraction_Layer 

def make_column_definitions(_field_names, _field_types):
    _columns = SQL_List()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(Parameter_ColumnDefinition(_name = _field_names[_field_counter], _datatype = _field_types[_field_counter]))
    return _columns





def create_table(_table_name, _field_names, _field_types, _db_type):

    _columns = make_column_definitions(_field_names, _field_types)
    _CREATE = Verb_CREATE_TABLE(_name = _table_name, _columns = _columns)
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

def copy_to_temp_table(self, _dal, _values, _field_name, _field_types):
    """Move datatable into a temp table on the resource, return the table name. """
    
    _tmp_table_name = "#test"
    _insert = make_insert_skeleton(_field_name, _field_types)
    print(_insert.as_sql(_dal.db_type))
    _query = " VALUES (" +"%s, " * (len(self.field_names) - 1) + "%s)"
    print(_query)
    
        
    _values = []
    # _rows, _cols, _frames = numpy.nonzero(data)
    for _row in zip(self.data_table):
        pass
    #    _values.append((frame, row, col, data[row,col,frame]))
    
    #_dest_dal.executemany(_query, _values)
    
    """Use executemany"""
    pass

