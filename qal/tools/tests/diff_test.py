"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.nosql.flatfile import Flatfile_Dataset
from qal.tools.diff import compare

class Diff_test(unittest.TestCase):


    def testName(self):
        _ff_source = Flatfile_Dataset(_filename = "resources/csv_source.csv", _has_header = True, _delimiter = ";", _csv_dialect = "excel-tab", _quoting = "MINIMAL")
        _dataset_source = _ff_source.load()
        _ff_dest = Flatfile_Dataset(_filename = "resources/csv_dest.csv", _has_header = True, _delimiter = ";", _csv_dialect = "excel-tab", _quoting = "MINIMAL")
        _dataset_dest = _ff_dest.load()
        #print(str(_dataset_dest))
        _missing_left, _missing_right, _difference = compare(_dataset_source, _dataset_dest, [0], True)
        
        self.assertEqual(_missing_left, [['7999', 'BORJESSON', 'HACKER', '7839', '2013-01-01', '99999', '', '10']], 'Missing left differs')
        self.assertEqual(_missing_right, [['7782', 'CLARK', 'MANAGER', '7839', '1981-06-09 00:00:00', '2450', '', '10'], 
                                          ['7788', 'SCOTT', 'ANALYST', '7566', '1982-12-09 00:00:00', '3000', '', '20']], 'Missing right differs')
        self.assertEqual(_difference, [[['7369', 'SMITH', 'CLERK', '7902', '1980-12-17 00:00:00', '800', '', '20'], 
                                        ['7369', 'SMITH', 'CLERK', '7902', '1980-12-17 00:00:00', '700', '', '20'], 0, 0], 
                                       [['7839', 'KING', 'PRESIDENT ', '', '1981-11-17 00:00:00', '5000', '', '10'], 
                                        ['7839', 'KING', 'PRESIDENT', '', '1981-11-17 00:00:00', '4500', '', '10'], 8, 6], 
                                       [['7876', 'ADAMS', 'CLERK', '7788', '1983-01-12 00:00:00', '1100,5', '', '20'], 
                                        ['7876', 'ADAMS', 'CLERK', '7788', '1983-01-12 00:00:00', '1100', '', '20'], 10, 8]]
                                        , 'Difference differs')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()