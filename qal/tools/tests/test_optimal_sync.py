"""
This modules holds CLI tests for optimal_sync.
There are not many as the functionality is covered in the libraries anyway.
"""

from io import StringIO, UnsupportedOperation
import os

__author__ = 'nibo'

import unittest
from subprocess import check_output


Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'


class MyTestCase(unittest.TestCase):
    def _test_merge_two_csv(self):
        # This test is not fully implemented yet.
        print(os.path.join(Test_Script_Dir, "..", "optimal_sync.py"))
        _tmpstdout = StringIO()
        _tmpstderr = StringIO()

        try:
            _res = check_output(cwd=Test_Script_Dir, args=[Test_Script_Dir + "/../optimal_sync.py", "-d", Test_Resource_Dir.replace(" ", "\ ") + "/test_merge_two_files.xml"],
                                stderr=_tmpstderr, shell=True)
            print("Result : " + str(_res))
        except UnsupportedOperation as e:
            print("Error: " + str(e.args))
        except Exception as e:
            print("Error: " + str(e))

        print([Test_Script_Dir + "/../optimal_sync.py", "-d", Test_Resource_Dir.replace(" ", "\ ") + "/test_merge_two_files.xml"])
        print("out : " + str(_tmpstdout.getvalue()))
        print("err : " + str(_tmpstderr.getvalue()))
        print("Done")
        self.assertEqual(True, True)



if __name__ == '__main__':
    unittest.main()
