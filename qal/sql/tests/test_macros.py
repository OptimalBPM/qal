"""
Created on Sep 26, 2013

@author: Nicklas Boerjesson
"""
import unittest

from qal.sql.macros import create_table_skeleton, make_insert_sql_with_parameters, copy_to_table, select_all_skeleton
from qal.dal.types import DB_POSTGRESQL, DB_MYSQL, db_type_to_string, \
    DB_SQLSERVER
from qal.sql.types import DEFAULT_ROWSEP
from qal.dal.tests.framework import default_dal
from qal.sql.sql import ParameterIdentifier
from qal.sql.utils import handle_temp_table_ref, citate, db_specific_object_reference, db_specific_datatype

db_type = DB_POSTGRESQL
table_name = ""


@default_dal(db_type)
class Test(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        global table_name
        table_name = "#sqlmacro"

        """ Comparison SQL statements"""

        if db_type == DB_SQLSERVER:
            self.create_temporary_table_SQL = "CREATE TABLE "
        else:
            self.create_temporary_table_SQL = "CREATE TEMPORARY TABLE "

        self.create_temporary_table_SQL += citate(handle_temp_table_ref(table_name, db_type),
                                                  db_type) + " (" + DEFAULT_ROWSEP + \
                                                  db_specific_object_reference("column1", db_type) + " " + \
                                                  db_specific_datatype("string", db_type) + " NULL" + "," + \
                                                  DEFAULT_ROWSEP + \
                                                  db_specific_object_reference("column2", db_type) + " " + \
                                                  db_specific_datatype("string", db_type) + " NULL" + DEFAULT_ROWSEP +\
                                                  ")"

        self.make_insert_sql_with_parameters_SQL = "INSERT INTO " + citate(handle_temp_table_ref(table_name, db_type),
                                                                           db_type) \
                                                   + " (" + citate("column1", db_type) + ", " + citate("column2",
                                                   db_type) + ")" + DEFAULT_ROWSEP + "VALUES (%s, %s)"

        self.make_select_all_skeleton_SQL = "SELECT * FROM " + citate("test", db_type)
        self.make_select_all_skeleton_SQL_columns = "SELECT " + citate("Column1", db_type) + ", " + \
                                                    citate("Column2", db_type) + " FROM " + citate(
            "test", db_type)
        if db_type == DB_MYSQL:
            self.create_temporary_table_SQL += " ENGINE=InnoDB"

        """ Test data"""

        self.field_names = ["column1", "column2"]
        self.field_types = ["string", "string"]
        self.values = [["string_1_A", "string_1_B"], ["string_2_A", "string_2_B"], ["string_3_A", "string_3_B"]]

    def test_1_create_table_skeleton(self):
        self.assertEqual(create_table_skeleton(table_name, _field_names=self.field_names,
                                               _field_types=self.field_types).as_sql(db_type),
                         self.create_temporary_table_SQL)

    def test_2_make_insert_sql_with_parameters(self):
        _sql = make_insert_sql_with_parameters(table_name, self.field_names, db_type, self.field_types)
        print(_sql)
        self.assertEqual(_sql, self.make_insert_sql_with_parameters_SQL)

    def test_3_copy_to_temp_table(self):
        global table_name
        copy_to_table(_dal=self._dal, _values=self.values, _field_names=self.field_names, _field_types=self.field_types,
                      _table_name=table_name, _create_table=True)
        _rows = self._dal.query(
            "SELECT * FROM " + ParameterIdentifier(_identifier=table_name).as_sql(self._dal.db_type))
        self.assertEqual(_rows, self.values)

    def test_4_select_all_skeleton(self):
        self.assertEqual(select_all_skeleton("test").as_sql(db_type), self.make_select_all_skeleton_SQL)

    def test_5_select_all_skeleton_columns(self):
        self.assertEqual(select_all_skeleton("test", ["Column1", "Column2"]).as_sql(db_type),
                         self.make_select_all_skeleton_SQL_columns)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()