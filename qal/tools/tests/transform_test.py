"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.tools.transform import Trim, Replace
from lxml import etree

class Transform_test(unittest.TestCase):
    
    def _parse_xml(self, _filename):
        _parser = etree.XMLParser(remove_blank_text=True)
        _tree = etree.ElementTree()
        return _tree.parse(_filename, _parser)
        

    def test_trim(self):
        """Test trim transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml('resources/test_merge_two_files.xml')
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/trim")
        _tested = Trim(_xml_def)
        _result = _tested.transform(' test ')
        self.assertEqual(_result, ' test', "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")
        
    def test_replace(self):
        """Test replace transformation input/output and XML encoding/decoding"""
        _tree = self._parse_xml('resources/test_merge_two_files.xml')
        _xml_def = _tree.find("mappings/field_mappings/field_mapping/transformations/replace")
        _tested = Replace(_xml_def)
        _result = _tested.transform("unneccessary cc")
        self.assertEqual(_result, "unnecessary cc", "Results differ")
        self.assertEqual(etree.tostring(_xml_def).strip(), etree.tostring(_tested.as_xml_node()), "XML in/out differ")        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()