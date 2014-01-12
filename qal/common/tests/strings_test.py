"""
    Tests for parsing
    
    :copyright: Copyright 2010-2013 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

import unittest
from qal.common.strings import parse_balanced_delimiters, empty_if_none

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

_cmp_parse_balanced_delimiters_list = ['4', '2', '2', '2', '10', "@name='sdf]'", "@id='test'"]
_cmp_parse_balanced_delimiters_clear = '/html/body/form/table/tr/td/table/tr/td/table/tr/td/table/tr/td/table'

class Test(unittest.TestCase):


    def test_1_XML_parsing(self):
        _result_list, _result_clear = parse_balanced_delimiters("/html/body/form/table/tr[4]/td[2]/table/tr[2]/td/table[2]/tr/td/table/tr[10]/td[@name='sdf]']/table[@id='test']/tr", "[", "]", "'")
        self.assertEqual(_result_list, _cmp_parse_balanced_delimiters_list, "Balanced delimiters differ")
        self.assertEqual(_result_clear, _cmp_parse_balanced_delimiters_clear, "Cleared string differ")
        
    def test_1_empty_if_none(self):
        self.assertEqual("", empty_if_none(";Test", None), "Should be empty")
        self.assertEqual(";Test", empty_if_none(";Test", "Test"), "Should not be empty")

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()