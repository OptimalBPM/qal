"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.tools.transform import Trim
from lxml import etree

class Transform_test(unittest.TestCase):


    def test_trim(self):
        """ Trim has no parameters"""
        _tree = etree.ElementTree()
        _tree.parse('test_merge_two_files.xml')
        _tree.find("./resources/resource[@uuid='source_uuid']")
        _trim = Trim()
        _result = _trim.transform(' test ')
        self.assertEqual(_result, 'test')
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()