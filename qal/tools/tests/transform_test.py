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
        _tree.parse('resources/test_merge_two_files.xml')
        _trim_node = _tree.find("mappings/field_mappings/field_mapping/transformations/trim")
        _trim = Trim(_trim_node)
        _result = _trim.transform(' test ')
        self.assertEqual(_result, ' test')
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()