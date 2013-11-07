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
        print(str(_dataset_dest))
        _missing_left, _missing_right, _difference = compare(_dataset_source, _dataset_dest, [0], True)
        print(str(_missing_left))
        print(str(_missing_right))
        print(str(_difference))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()