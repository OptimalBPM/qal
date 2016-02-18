"""
This modules holds CLI tests for optimal_sync.
There are not many as the functionality is covered in the libraries anyway.
"""

from io import StringIO, UnsupportedOperation
import os
import sys


__author__ = 'Nicklas Borjesson'

import unittest
from subprocess import check_output

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')

from qal.common.diff import diff_strings

class MyTestCase(unittest.TestCase):
    def test_merge_two_csv(self):
        # This test is not fully implemented yet.
        print("\n")

        _tmpstdout = StringIO()
        _tmpstderr = StringIO()

        _call = sys.executable + " " + os.path.abspath(os.path.join(Test_Script_Dir,"..", "optimal_sync.py") +
                                                       " -d " +
                                                       os.path.join(Test_Resource_Dir,"test_merge_two_files.json"))
        print("Call: " + _call)
        try:
            _res = check_output(_call, shell=True, cwd=Test_Script_Dir)
            print("Result : " + str(_res))
        except UnsupportedOperation as e:
            print("Error: " + str(e.args))
        except Exception as e:
            print("Error: " + str(e))
        with open(os.path.join(Test_Resource_Dir, "csv_out.csv")) as _f_out:
            _output = _f_out.read()
        with open(os.path.join(Test_Resource_Dir, "csv_cmp.csv")) as _f_cmp:
            _cmp = _f_cmp.read()
        self.assertEqual(_output, _cmp, diff_strings(_output, _cmp))
        print("out : " + str(_tmpstdout.getvalue()))
        print("err : " + str(_tmpstderr.getvalue()))
        print("Done")
        self.assertEqual(True, True)



if __name__ == '__main__':
    unittest.main()
