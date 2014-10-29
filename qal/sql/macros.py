"""
Created on Sep 26, 2013

@author: Nicklas Boerjesson
"""

from qal.sql.sql import VerbCreateTable, VerbSelect, VerbInsert, VerbDelete, VerbUpdate, VerbDropTable, SqlList, \
    ParameterColumndefinition, ParameterIdentifier, ParameterSource

from qal.sql.utils import datatype_to_parameter


def make_column_definitions(_field_names, _field_types):
    """Based on a list of field names and field types, return a list of ParameterColumndefinition objects.

    :param _field_names: A list of field names
    :param _field_types: A list of field types
    :return: A list of ParameterColumndefinition
    """
    _columns = SqlList()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(
            ParameterColumndefinition(_name=_field_names[_field_counter], _datatype=_field_types[_field_counter]))
    return _columns


def create_table_skeleton(_table_name, _field_names, _field_types):
    """Creates a sql for creating tables.

    :param _table_name: The table name to use
    :param _field_names: The field(column) names
    :param _field_types: The types of the fields
    :return:
    """
    _columns = make_column_definitions(_field_names, _field_types)
    _CREATE = VerbCreateTable(_name=_table_name, _columns=_columns)
    return _CREATE


def make_column_identifiers(_field_names):
    """Create columns idenfiers from a list of field names.

    :param _field_names: A list of field names
    :return: A list of column identifies (instances of ParameterIdentifier)
    """

    _columns = SqlList()
    _field_counter = 0
    _curr_column = None
    for _field_counter in range(0, len(_field_names)):
        _columns.append(ParameterIdentifier(_identifier=_field_names[_field_counter]))
    return _columns


def make_delete_skeleton(_table_name, _key_fields):
    # TODO: Implement make_delete_skeleton

    """NOT IMPLEMENTED: Make a skeleton for an SQL DELETE statement object.

    :param _table_name:
    :param _key_fields:
    :return:
    """
    raise Exception("make_delete_skeleton isn't implemented")
    _destination_identifier = ParameterIdentifier(_identifier=_table_name)

    _column_identifiers = make_column_identifiers(_key_fields)
    return VerbDelete()


def make_update_skeleton(_table_name):
    """Make a skeleton for an SQL update object

    :param _table_name: The name of the table
    :return: An instance of VerbUpdate
    """
    _table_identifier = ParameterIdentifier(_identifier=_table_name)
    return VerbUpdate(_table_identifier=_table_identifier)


def make_insert_sql_with_parameters(_table_name, _field_names, _db_type, _field_types):
    """Make a prepared statement-type INSERT INTO ...VALUES-SQL.
    See: http://en.wikipedia.org/wiki/Prepared_statement

    :param _table_name: The table to insert into
    :param _field_names: Fields(column) names
    :param _db_type: The database backend type(qal.dal.types)
    :param _field_types: A list of field types(qal.sql.types)
    :return: Return a populated instance of an insert SQL
    """
    _destination_identifier = ParameterIdentifier(_identifier=_table_name)
    _column_identifiers = make_column_identifiers(_field_names)
    _insert = VerbInsert(_destination_identifier=_destination_identifier, _column_identifiers=_column_identifiers)
    _refs = []
    # The VALUES-part of INSERT INTO looks the same on all platforms and we don't have to care about escaping.
    for _curr_idx in range(0, len(_field_names)):
        _refs.append(datatype_to_parameter(_db_type, _field_types[_curr_idx]))

    _values_sql = "VALUES (" + ", ".join(_refs) + ")"

    return _insert.as_sql(_db_type) + _values_sql




def copy_to_table(_dal, _values, _field_names, _field_types, _table_name, _create_table=None, _drop_existing=None):
    """Copy a matrix of data into a table on the resource, return the table name.

    :param _dal: An instance of DAL(qal.dal.DAL)
    :param _values: The a list(rows) of lists(values) with values to be inserted
    :param _field_names: The name of the fields(columns)
    :param _field_types: The field types(qal.sql.types)
    :param _table_name: The name of the destination tables
    :param _create_table: Create the destination table based on _field_names, _field_types
    :param _drop_existing: If a table with the same name as the destination table already exists, drop it
    :return: The name of the destination table.
    """

    if _drop_existing == True:
        try:
            _dal.execute(VerbDropTable(_table_name).as_sql(_dal.db_type))
            _dal.commit()
        except Exception as e:
            print("copy_to_table - Ignoring error when dropping the table \"" + _table_name + "\": " + str(e))

    if _create_table == True:
        # Always create temporary table even if it ends up empty.
        _create_table_sql = create_table_skeleton(_table_name, _field_names, _field_types).as_sql(_dal.db_type)
        print("Creating " + _table_name + " table..\n" + _create_table_sql)
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


def select_all_skeleton(_table_name, _column_names=None):
    """Returns a "SELECT * FROM _table_name"-structure,
    if _column_names are specified, SELECT only from those columns

    :param _table_name: The name of the table to select from
    :param _column_names: Optionally, the name of the columns to select from
    :return: A populated instance of VerbSelect
    """
    _expression = ParameterIdentifier(_table_name)
    if (_column_names is None) or (_column_names == []):
        _fields = None
    else:
        _fields = make_column_identifiers(_column_names)
    _source = ParameterSource(_expression, _conditions=None, _alias=None, _join_type=None)
    _select = VerbSelect(_fields=_fields, _sources=[_source], _operator="AND")

    return _select
