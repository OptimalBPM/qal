'''
Created on Sep 26, 2013

@author: Nicklas Boerjesson
'''
import unittest
from qal.sql.sql_macros import create_table_skeleton, make_insert_skeleton,copy_to_table,select_all_skeleton
from qal.dal.dal_types import DB_POSTGRESQL, DB_MYSQL, db_type_to_string,\
    DB_SQLSERVER
from qal.sql.sql_types import DEFAULT_ROWSEP
from qal.dal.tests.framework import default_dal
from qal.sql.sql import Parameter_Identifier
from qal.sql.sql_utils import handle_temp_table_ref, citate, db_specific_object_reference, db_specific_datatype

db_type = DB_POSTGRESQL
table_name = "#sqlmacro"


""" Comparison SQL statements"""

if db_type == DB_SQLSERVER:
    create_temporary_table_SQL = "CREATE TABLE "
else:     
    create_temporary_table_SQL = "CREATE TEMPORARY TABLE " 
    
create_temporary_table_SQL+= citate(handle_temp_table_ref(table_name, db_type), db_type) + " ("+ DEFAULT_ROWSEP  \
 + db_specific_object_reference("column1", db_type) + " " +db_specific_datatype("string", db_type) + "," + DEFAULT_ROWSEP \
 + db_specific_object_reference("column2", db_type) + " " +db_specific_datatype("string", db_type) + DEFAULT_ROWSEP + ")"

make_insert_skeleton_SQL = "INSERT INTO " + citate(handle_temp_table_ref(table_name, db_type), db_type)  \
 +  " (" + citate("column1", db_type) + ", " + citate("column2", db_type) + ")"+DEFAULT_ROWSEP 


make_select_all_skeleton_SQL = "SELECT * FROM "+ citate("test", db_type)

if db_type == DB_MYSQL:
    create_temporary_table_SQL+= " ENGINE=InnoDB"
    
    
""" Test data"""

field_names = ["column1", "column2"]
field_types = ["string", "string"]
values = [["string_1_A", "string_1_B"],["string_2_A", "string_2_B"],["string_3_A", "string_3_B"]]


@default_dal(db_type)
class Test(unittest.TestCase):

    def test_1_create_table_skeleton(self):
        self.assertEqual(create_table_skeleton(table_name, _field_names = field_names,
                     _field_types = field_types).as_sql(db_type), create_temporary_table_SQL)
        
    def test_2_make_insert_skeleton(self):
        _sql = make_insert_skeleton(table_name, _field_names = field_names, ).as_sql(db_type)
        print(_sql)
        self.assertEqual(_sql, make_insert_skeleton_SQL)
       
    def test_3_copy_to_temp_table(self):
        copy_to_table(_dal = self._dal, _values = values, _field_names = field_names, _field_types = field_types, _table_name = table_name, _create_table= True)
        _rows = self._dal.query("SELECT * FROM " + Parameter_Identifier(_identifier = table_name).as_sql(self._dal.db_type))
        self.assertEqual(_rows, values)

    def test_4_select_all_skeleton(self):
        self.assertEqual(select_all_skeleton("test").as_sql(db_type), make_select_all_skeleton_SQL)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()