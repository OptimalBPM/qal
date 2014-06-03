"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.tools.discover import import_error_to_help
from source.qal.tools.discover import platform_to_int, get_python_versions


class Discover_test(unittest.TestCase):
    

    def test_import_error_to_help(self):
        """Since this test is platform specific, failure is only possible on linux"""
        self.maxDiff = None
        if platform_to_int() == 0:
            self.assertEqual(import_error_to_help(_module="pymysql", _err_obj="No module named 'pymysql'",
                                                  _pip_package="pymysql3", _apt_package="python3-mysql",
                                                  _win_package="pymysql3.msi"),
                             "The python " +get_python_versions(_style="minor") + " module \"pymysql\" is not installed.\nRun sudo pip3 install pymysql3" +
                             " or sudo apt-get install python3-mysql")
        if platform_to_int() == 1:
            self.assertEqual(import_error_to_help(_module="pymysql", _err_obj="No module named 'pymysql'",
                                                  _pip_package="pymysql3", _apt_package="python3-mysql",
                                                  _win_package="pymysql3.msi"),
                             "The python " +get_python_versions(_style="minor") + " module \"pymysql\" is not installed.\nEither run pip install pymysql3" +
                             ", download and install pymysql3.msi or install from source.")
        else:
            self.assertTrue(True)



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()