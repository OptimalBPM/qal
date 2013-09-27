'''
Created on Sep 26, 2013

@author: Nicklas Boerjesson
'''
import unittest
from qal.sql.sql_macros import create_table, make_insert_skeleton,copy_to_temp_table
from qal.dal.dal_types import DB_POSTGRESQL
from qal.sql.sql_types import DEFAULT_ROWSEP

create_table_SQL = "CREATE TABLE \"#test\" ("+DEFAULT_ROWSEP + "\
\"column1\" VARCHAR," + DEFAULT_ROWSEP +  "\
\"column2\" VARCHAR" + DEFAULT_ROWSEP + "\
)"

field_names = ["column1", "column2"]
field_types = ["string", "string"]
data = [["string_1_A", "string_1_B"],["string_2_A", "string_2_B"],["string_2_A", "string_2_B"]]

class Test(unittest.TestCase):


    def test_create_table_sql(self):
        self.assertEqual(create_table("#test", _field_names = field_names,
                     _field_types = field_types, _db_type = DB_POSTGRESQL), create_table_SQL)
        
    def test_make_insert_skeleton(self):
        print(make_insert_skeleton("#test", _field_names = field_names).as_sql(DB_POSTGRESQL))
        
    def test_copy_to_temp_table(self):
        print(copy_to_temp_table("#test", _field_names = field_names, data))
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()