"""
Created on Oct 10, 2010

@author: Nicklas Boerjesson
@note: This tests require a testing environment to work.
@todo: There should be tests for listing tables and so forth.
"""
import unittest

from qal.dal.tests.framework import get_default_dal
from qal.sql.meta_queries import MetaQueries
from qal.dal.types import DB_POSTGRESQL


class TestSQLMetaQueries(unittest.TestCase):
    def check_list_column(self, _db_type):
        dal = get_default_dal(_db_type, "")
        columns = MetaQueries.table_info(dal, 'Table1')
        if columns == ['Table1ID', 'Table1Name', 'Table1Changed']:
            return True
        else:
            return False

    def _test_list_columns(self):
        # self.assertEqual(self.check_list_column(DB_MYSQL), True, 'ListColumns failed: DB_MYSQL')
        self.assertEqual(self.check_list_column(DB_POSTGRESQL), True, 'ListColumns failed: DB_POSTGRES')
        #        self.assertEqual(self.check_list_column(DB_ORACLE), True, 'ListColumns failed: DB_ORACLE')
        #        self.assertEqual(self.check_list_column(DB_DB2), True, 'ListColumns failed: DB_DB2')
        #        self.assertEqual(self.check_list_column(DB_SQLSERVER), True, 'ListColumns failed: DB_SQLSERVER')

        pass


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()