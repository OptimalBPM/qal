"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
"""

import unittest
from qal.common.listhelper import pretty_list

from qal.dataset.flatfile import FlatfileDataset
from qal.common.resources import Resources
from lxml import etree
from qal.common.diff import diff_files
from qal.dataset.spreadsheet import SpreadsheetDataset

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


_test_data= [
            [7369.0, 'SMITH', 'CLERK', 7902.0, '1980-12-17 00:00:00', 800.0, '', 20.0],
            [7499.0, 'ALLEN', 'SALE;SMAN', 7698.0, '1981-02-20 00:00:00', 1600.0, 300.0, 30.0],
            [7521.0, 'WARD', 'SALESMAN', 7698.0, '1981-02-22 00:00:00', 1250.0, 500.0, 30.0],
            [7566.0, 'JONES', 'MANAGER', 7839.0, '1981-04-02 00:00:00', 2975.0, '', 20.0],
            [7654.0, 'MARTIN', 'SALESMAN', 7698.0, '1981-09-28 00:00:00', 1250.0, 1400.0, 30.0],
            [7698.0, 'BLAKE', 'MANAGER', 7839.0, '1981-05-01 00:00:00', 2850.0, '', 30.0],
            [7782.0, 'CLARK', 'MANAGER', 7839.0, '1981-06-09 00:00:00', 2450.0, '', 10.0],
            [7788.0, 'SCOTT', 'ANALYST', 7566.0, '1982-12-09 00:00:00', 3000.0, '', 20.0],
            [7839.0, 'KING', 'PRESIDENT', '', '1981-11-17 00:00:00', 5000.0, '', 10.0],
            [7876.0, 'ADAMS', 'CLERK', 7788.0, '1983-01-12 00:00:00', '1100,5', '', 20.0],
            [7900.0, 'JAMES', 'CLERK', 7698.0, '1981-12-03 00:00:00', 950.0, '', 30.0],
            [7902.0, 'FORD', 'ANALYST', 7566.0, '1981-12-03 00:00:00', 3000.0, '', 20.0],
            [7934.0, 'MILLER', 'CLERK', 7782.0, '1982-01-23 00:00:00', 1300.0, '', 10.0]
            ]

def load_xml(_filename):
    return etree.parse(_filename)

class Test(unittest.TestCase):


    def test_1_Load_Save_xls(self, _has_header = None, _resource = None):
        _resources_node = load_xml(os.path.join(Test_Resource_Dir, "resources.xml")).find("resources")
        _resources = Resources(_resources_node = _resources_node)
        _da = SpreadsheetDataset(_resource= _resources.get_resource("{86470370-FF78-48A4-9759-A3BAE4EE22A1}"))
        _da.load()
        self.assertEqual(_da.data_table, _test_data, "test_1_Load_Save: Loaded data doesn't match")

        _da.save(_save_as=Test_Resource_Dir + "/excel_out.xls")

        _da.data_table = []
        _da.load()
        self.assertEqual(_da.data_table, _test_data, "test_1_Load_Save loading back: Loaded data doesn't match")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()