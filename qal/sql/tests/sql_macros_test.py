'''
Created on Sep 26, 2013

@author: Nicklas Boerjesson
'''
import unittest
from qal.sql.sql_macros import create_temporary_table, make_insert_skeleton,copy_to_temp_table
from qal.dal.dal_types import DB_POSTGRESQL
from qal.sql.sql_types import DEFAULT_ROWSEP
from qal.dal.tests.framework import default_dal
from qal.sql.sql import Parameter_Identifier
create_temporary_table_SQL = "CREATE TEMPORARY TABLE \"#test\" ("+DEFAULT_ROWSEP + "\
\"column1\" VARCHAR," + DEFAULT_ROWSEP +  "\
\"column2\" VARCHAR" + DEFAULT_ROWSEP + "\
)"

make_insert_skeleton_SQL = "INSERT INTO \"#test\" (\"column1\", \"column2\")"+DEFAULT_ROWSEP 

field_names = ["column1", "column2"]
field_types = ["string", "string"]
values = [("string_1_A", "string_1_B"),("string_2_A", "string_2_B"),("string_3_A", "string_3_B")]

@default_dal(DB_POSTGRESQL)
class Test(unittest.TestCase):

    def test_1_create_temporary_table_sql(self):
        self.assertEqual(create_temporary_table("#test", _field_names = field_names,
                     _field_types = field_types, _db_type = DB_POSTGRESQL), create_temporary_table_SQL)
        
    def test_2_make_insert_skeleton(self):
        
        self.assertEqual(make_insert_skeleton("#test", _field_names = field_names, ).as_sql(DB_POSTGRESQL), make_insert_skeleton_SQL)
       
    def test_3_copy_to_temp_table(self):
        print(copy_to_temp_table(_dal = self._dal, _values = values, _field_names = field_names, _field_types = field_types, _table_name = "#sqĺmac3"))
        _rows = self._dal.query("SELECT * FROM " + Parameter_Identifier(_identifier = "#sqĺmac3").as_sql(self._dal.db_type))
        self.assertEqual(_rows, values)
        


        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()