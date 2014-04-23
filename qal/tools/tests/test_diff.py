"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.common.listhelper import pretty_list
from qal.dataset.flatfile import FlatfileDataset
from qal.tools.diff import compare


import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


class Diff_test(unittest.TestCase):


    def test_diff(self):
        """Test comparison of two data sets, fetched from .csv files."""
        _ff_source = FlatfileDataset(_filename = os.path.join(Test_Resource_Dir, "csv_source.csv"),
                                     _has_header = True, _delimiter = ";", _csv_dialect = "excel-tab",
                                     _quoting = "MINIMAL", _quotechar='"')
        _dataset_source = _ff_source.load()
        _ff_dest = FlatfileDataset(_filename = os.path.join(Test_Resource_Dir, "csv_dest_orig.csv"),
                                   _has_header = True, _delimiter = ";", _csv_dialect = "excel-tab",
                                   _quoting = "MINIMAL", _quotechar='"')
        _dataset_dest = _ff_dest.load()
        #print(str(_dataset_dest))
        _missing_left, _missing_right, _difference, _sorted = compare(_dataset_source, _dataset_dest, [0], True)
        self.assertEqual(_missing_left, [[9, 7, ['7844', 'TURNER', 'SALESMAN', '7698', '1981-09-08 00:00:00', '1500', '', '30']], 
                                         [2, 12, ['7999', 'BORJESSON', 'HACKER', '7839', '2013-01-01', '99999', '', '10']]], 'Missing left differs')
        self.assertEqual(_missing_right, [[6,6,['7782', 'CLARK', 'MANAGER', '7839', '1981-06-09 00:00:00', '2450', '', '10']], 
                                          [7,6,['7788', 'SCOTT', 'ANALYST', '7566', '1982-12-09 00:00:00', '3000', '', '20']]], 'Missing right differs')

        self.assertEqual(_difference,
                         [
                            [0, 0, ['7369', 'SMITH', 'CLERK', '7902', '1980-12-17 00:00:00', '800', '', '20'], ['7369', 'SMITH', 'CLERK', '7902', '1980-12-17 00:00:00', '700', '', '20']],
                            [1, 1, ['7499', 'ALLEN', 'SALE;SMAN', '7698', '1981-02-20 00:00:00', '1600', '300', '30'], ['7499', 'ALLEN', 'SALESMAN', '7698', '1981-02-20 00:00:00', '1600', '300', '30']],
                            [8, 6, ['7839', 'KING', 'PRESIDENT ', '', '1981-11-17 00:00:00', '5000', '', '10'], ['7839', 'KING', 'PRESIDENT', '', '1981-11-17 00:00:00', '4500', '', '10']],
                            [9, 8, ['7876', 'ADAMS', 'CLERK', '7788', '1983-01-12 00:00:00', '1100,5', '', '20'], ['7876', 'ADAMS', 'CLERK', '7788', '1983-01-12 00:00:00', '1100', '', '20']]
                        ], 'Difference differs')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
