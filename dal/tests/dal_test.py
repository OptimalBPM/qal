'''
Created on May 20, 2010

@author: Nicklas Boerjesson
'''
import unittest

from dal.dal_types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER
from dal.tests.framework import get_default_dal 

def _connect_test(_db_type):
    dal = get_default_dal(_db_type, "")
    dal.close()
    return True
    
class DAL_tests(unittest.TestCase):
    global settings


    def test_connect_DB_MYSQL(self):
        self.assertEqual(_connect_test(DB_MYSQL), True, 'Connect_test failed: DB_MYSQL')
    def test_connect_DB_POSTGRESQL(self):
        self.assertEqual(_connect_test(DB_POSTGRESQL), True, 'Connect_test failed: DB_POSTGRESQL')
    def test_connect_DB_ORACLE(self):
        self.assertEqual(_connect_test(DB_ORACLE), True, 'Connect_test failed: DB_ORACLE')
    def test_connect_DB_DB2(self):
        self.assertEqual(_connect_test(DB_DB2), True, 'Connect_test failed: DB_DB2')
    def test_connect_DB_SQLSERVER(self):
        self.assertEqual(_connect_test(DB_SQLSERVER), True, 'Connect_test failed: DB_SQLSERVER')

    # This test can be used for debugging.
    def _test_run(self):

        dal = get_default_dal(DB_MYSQL,'dbupgrd')
        if 1==0:
            sql = "DROP TABLE  \"__VersionLog\""
            dal.execute(sql)
            sql = "DROP TABLE  \"__Application\""
            dal.execute(sql)
            sql = "DROP TABLE  \"TestTable1\""
            dal.execute(sql)
            sql = "DROP TABLE  \"TestTable2\""
            dal.execute(sql)
            sql = "DROP TABLE  \"TestTable3\""
            dal.execute(sql)
            dal.commit()
        sql = \
"""
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE AS NULLABLE, CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '__VersionLog'

"""
        _result_set = dal.query(sql)
        for item in _result_set:
            print(item)
        dal.close()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    