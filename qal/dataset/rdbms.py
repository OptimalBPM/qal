"""
Created on Jan 8, 2012

@author: Nicklas Boerjesson
"""
from qal.dal.conversions import python_type_to_sql_type

from qal.dal.types import DB_DB2, DB_ORACLE, DB_POSTGRESQL
from qal.sql.utils import db_specific_object_reference
from qal.sql.sql import VerbInsert, VerbDelete, VerbUpdate, ParameterConditions
from qal.sql.sql import ParameterSource, ParameterIdentifier, ParameterParameter, ParameterCondition, \
    ParameterAssignment
from qal.dal.dal import DatabaseAbstractionLayer
from qal.sql.macros import select_all_skeleton, create_table_skeleton
from qal.sql.base import SqlList

from qal.sql.macros import make_insert_sql_with_parameters

from qal.dataset.custom import CustomDataset


class RDBMSDataset(CustomDataset):
    """The RDMBS Dataset holds a two-dimensional array of data, typically representing a table in a database.
    If the data set is not a table but based on a more complex query, data will not be possible to apply to it."""

    """An instance of the Database Abstraction Layer(DAL)"""
    _dal = None
    """If set, all data in table "table_name" is loaded in its entirety into the data_table."""
    table_name = None
    """If set, and table_name is not, then the SQL statement contained is executed. It is a text string."""
    query = None
    """
    TODO:Make it possible for it to be a backend agnostic qal.sql.sql.VerbSelect object parsed from an XML structure."""

    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'RDBMS':
            raise Exception(
                "RDBMSDataset.read_resource_settings.parse_resource error: Wrong resource type: " + _resource.type)

        self._dal = DatabaseAbstractionLayer(_resource=_resource)

        self.table_name = _resource.data.get("db_table_name")

        self.query = _resource.data.get("db_query")

    def write_resource_settings(self, _resource):
        """TODO: The RDBMS resource type is a special case, as it is both a database connection and/or a dataset definition.
        Should this be?"""

        if self._dal is None:
            # Clear, as the DAL is not going to
            _resource.type = 'RDBMS'
            _resource.data.clear()
        else:
            # Let the DAL go first
            self._dal.write_resource_settings(_resource)

        # Add own parameters
        _resource.data["db_table_name"] = self.table_name
        _resource.data["db_query"] = self.query

    def __init__(self, _resource=None):
        """
        Constructor
        """
        super(RDBMSDataset, self).__init__()

        if _resource is not None:
            self.read_resource_settings(_resource)

    def _structure_init(self, _dataset):

        """Initializes the SQL queries that data is to be applied to, starts a database transaction"""
        # If field types aren't set, that means that the table isn't created.
        # They would have been populated as part of the loading process
        if len(self.field_types) == 0:
            if len(_dataset) == 0:
                raise Exception("RDBMSDataset.structure_init error: The _dataset cannot be empty "
                                "for field types to be derived from it.")
            try:
                self.field_types = [python_type_to_sql_type(type(_curr_value)) for _curr_value in _dataset[0]]
                _create_table_sql = create_table_skeleton(self.table_name, self.field_names, self.field_types).as_sql(
                    self._dal.db_type)
                print("Creating " + self.table_name + " table..\n" + _create_table_sql)
                self._dal.execute(_create_table_sql)
            except Exception as e:
                raise Exception("Merge.execute: An error occurred creating destination table: " + str(e))

        self._rdbms_init_deletes()
        if len(self._structure_key_fields) <= len(self.field_names):
            self._rdbms_init_updates()

        self._rdbms_init_inserts()

        self._dal.start()

        super(RDBMSDataset, self)._structure_init(_dataset)

    @staticmethod
    def _extract_data_columns_from_diff_row(_field_indexes, _diff_row):
        """Extracts columns specified in _field_indexes from _diff_list"""
        _result = []

        for _curr_field in _field_indexes:
            _result.append(_diff_row[_curr_field])
        return _result

    def _rdbms_init_deletes(self):
        """Generates a VerbDelete instance populated with the structural indata"""

        _source = ParameterSource()
        _source.expression.append(ParameterIdentifier(self.table_name))

        # Add the WHERE statement
        for _field_idx in self._structure_key_fields:
            _new_cond = ParameterCondition(_left=ParameterIdentifier(_identifier=self.field_names[_field_idx]),
                                           _right=ParameterParameter(_datatype=self.field_types[_field_idx]),
                                           _operator='=', _and_or='AND')
            _source.conditions.append(_new_cond)

        # Make the VerbDelete skeleton
        _delete = VerbDelete()
        _delete.sources.append(_source)

        # Fetch the resource
        self._structure_delete_sql = _delete.as_sql(self._dal.db_type)

    def _rdbms_init_inserts(self):
        """Generates a VerbInsert instance populated with the structural indata"""

        self._structure_insert_sql = make_insert_sql_with_parameters(self.table_name, self.field_names,
                                                                     self._dal.db_type, self.field_types)

    def _rdbms_init_updates(self):
        """Generates DELETE and INSERT instances populated with the structural indata """

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

        _assignments = SqlList("ParameterAssignment")

        # Instantiate the assignments

        for _curr_field_idx in range(len(_field_names_ex_keys)):
            _left = ParameterIdentifier(_identifier=_field_names_ex_keys[_curr_field_idx])
            _right = ParameterParameter(_datatype=_field_names_ex_keys_datatypes[_curr_field_idx])
            _assignments.append(ParameterAssignment(_left, _right))

        # Create the WHERE conditions.

        _conditions = ParameterConditions()

        for _curr_field_idx in range(len(_key_field_names)):
            _left = ParameterIdentifier(_identifier=_key_field_names[_curr_field_idx])
            _right = ParameterParameter(_datatype=_key_field_datatypes[_curr_field_idx])
            _conditions.append(ParameterCondition(_left, _right, _operator="="))

        # Specify target table

        _table_identifier = ParameterIdentifier(_identifier=self.table_name)

        # Create VerbUpdate instance with all parameters

        _update = VerbUpdate(_table_identifier=_table_identifier, _conditions=_conditions, _assignments=_assignments)

        # Generate the SQL with all parameter place holders

        self._structure_update_sql = _update.as_sql(self._dal.db_type)

    def _structure_insert_row(self, _row_idx, _row_data, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add SQL handling"""
            _execute_many_data = self._extract_data_columns_from_diff_row(range(len(self.field_names)), _row_data)

            # Apply and commit changes to the structure

            self._dal.executemany(self._structure_insert_sql, [_execute_many_data])
            if not _no_logging:
                self.log_insert("N/A in RDBMS", _row_data, "Destination table: " + self.table_name)
        # Call parent
        super(RDBMSDataset, self)._structure_insert_row(_row_idx, _row_data, _no_logging=_commit)

    def _structure_update_row(self, _row_idx, _row_data, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add SQL handling"""
            # To satisfy the VerbUpdate instance, create a two-dimensional array,
            # leftmost columns are data, rightmost are keys.

            _field_idx_ex_keys = list(set(range(len(self.field_names))) - set(self._structure_key_fields))
            _execute_data = self._extract_data_columns_from_diff_row(_field_idx_ex_keys + self._structure_key_fields,
                                                                     _row_data)

            # Apply and commit changes to the database
            self._dal.executemany(self._structure_update_sql, [_execute_data])
            if not _no_logging:
                self.log_update_row(_row_idx, self.data_table[_row_idx], _row_data,
                                    "Destination table: " + self.table_name)
        # Call parent
        super(RDBMSDataset, self)._structure_update_row(_row_idx, _row_data, _no_logging=_commit)

    def _structure_delete_row(self, _row_idx, _commit=True, _no_logging=None):
        if _commit is True:
            """Override parent to add SQL handling"""

            # Extract the key data
            _key_values = self._extract_data_columns_from_diff_row(self._structure_key_fields,
                                                                   self.data_table[_row_idx])
            # Make the deletes
            self._dal.executemany(self._structure_delete_sql, [_key_values])
            if not _no_logging:
                self.log_delete(_key_values, self.data_table[_row_idx], "Destination table: " + self.table_name)

        # Call parent
        super(RDBMSDataset, self)._structure_delete_row(_row_idx, _no_logging=_commit)
        # self.data_table.pop(_row_idx)

    def load(self):
        if self.table_name and self.table_name != "":
            """Query all values from a table from a RDBMS resource"""
            if not self._dal.connected:
                self._dal.connect_to_db()

            self.data_table = self._dal.query(
                select_all_skeleton(self.table_name, self.field_names).as_sql(self._dal.db_type))
            self.field_names = self._dal.field_names
            self.field_types = self._dal.field_types
        else:
            raise Exception("RDBMSDataset.load(): data_table must be set.")

        return self.data_table

    def save(self):
        """Save data. Commits the transaction"""
        self._dal.commit()
