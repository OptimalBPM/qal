"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.tools.merge import Merge 
from lxml import etree
class Merge_test(unittest.TestCase):
    
    def _parse_xml(self, _filename):
        _parser = etree.XMLParser(remove_blank_text=True)
        _tree = etree.ElementTree()
        return _tree.parse(_filename, _parser)
    
    def test_Merge(self):
        """Test merge"""
        _merge_xml = self._parse_xml('resources/test_merge_two_files.xml')
        _merge = Merge(_xml_node = _merge_xml)

        #print(str(etree.tostring(_merge.as_xml_node(), pretty_print=True)).replace("\\n", "\n"))
        #print(str(etree.tostring(_merge_xml, pretty_print=True)).replace("\\n", "\n"))
        
        print(etree.tostring(_merge.as_xml_node()))
        print(etree.tostring(_merge_xml))
        _merge.execute()
        





if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()