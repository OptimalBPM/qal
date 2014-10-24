"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.common.transform import Trim, Replace, IfEmpty, Cast, Replace_Regex
from lxml import etree
from datetime import datetime

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')

class Transform_test(unittest.TestCase):
    
    def _parse_xml(self, _filename):
        _parser = etree.XMLParser(remove_blank_text=True)
        _tree = etree.ElementTree()
        return _tree.parse(_filename, _parser)
        

    def test_1_trim(self):
        """Test trim transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/trim")
        _tested = Trim(_xml_def)
        _result = _tested.transform(' test ')
        self.assertEqual(_result, ' test', "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")
        
    def test_2_IfEmpty(self):
        """Test trim transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/IfEmpty")
        _tested = IfEmpty(_xml_def)
        _result = _tested.transform(None)
        self.assertEqual(_result, 'NULL', "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")
        
    def test_3_replace(self):
        """Test replace transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/replace")
        _tested = Replace(_xml_def)
        _result = _tested.transform("unneccessary cc")
        self.assertEqual(_result, "unnecessary cc", "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")

    def test_4_replace_regex(self):
        """Test replace transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/replace_regex")
        _tested = Replace_Regex(_xml_def)
        _result = _tested.transform("MILLER KING")
        self.assertEqual(_result, "KULLER KUNG", "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")
    def test_5_cast(self):
        """Test trim transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _xml_def = _tree.find("mappings/field_mappings/field_mapping[src_reference='hiredate']/transformations/cast[@order='5']")
        _tested = Cast(_xml_def)
        _result = _tested.transform("2010-01-02 00:00:01")
        self.assertEqual(_result, datetime.strptime("2010-01-02 00:00:01", "%Y-%m-%d %H:%M:%S"), "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")      


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()