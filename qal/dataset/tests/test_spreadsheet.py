"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
"""

import unittest
from qal.common.listhelper import pretty_list

from qal.dataset.flatfile import Flatfile_Dataset
from qal.common.resources import Resources
from lxml import etree
from qal.tools.diff import diff_files
from qal.dataset.spreadsheet import Spreadsheet_Dataset

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources/'

def load_xml(_filename):
    return etree.parse(_filename)

class Test(unittest.TestCase):


    def test_1_Load_Save(self, _has_header = None, _resource = None):
        _resources_node = load_xml(Test_Resource_Dir + "resources.xml").find("resources")
        _resources = Resources(_resources_node = _resources_node)
        _da = Spreadsheet_Dataset(_resource= _resources.get_resource("{86470370-FF78-48A4-9759-A3BAE4EE22A1}"))
        _da.load()
        self.assertEqual(_da.data_table,
            [
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
        , "test_1_Load_Save: Data doesn't match")



        #_da.save(_save_as = Test_Resource_Dir + "/excel_out.csv")
        #pretty_list(_da.data_table)

        # _f_a = open(Test_Resource_Dir + "/excel_out.csv", "r")
        # _f_b = open(Test_Resource_Dir + "/excel_cmp.csv", "r")
        # _a = _f_a.read()
        # _b = _f_b.read()
        # _f_a.close()
        # _f_b.close()
        # self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")
        #
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()