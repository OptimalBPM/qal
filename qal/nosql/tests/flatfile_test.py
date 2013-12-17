'''
Created on Dec 17, 2013

@author: nibo
'''
import unittest

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

class Test(unittest.TestCase):


    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()