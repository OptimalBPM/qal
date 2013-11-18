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
    
    def test_Merge_files(self):
        """Test merge two files"""
        _merge_xml = self._parse_xml('resources/test_merge_two_files.xml')
        _merge = Merge(_xml_node = _merge_xml)
        self.assertEqual(etree.tostring(_merge.as_xml_node()), etree.tostring(_merge_xml), "Input/output XML does not match")

        _result = _merge.execute()
        #_merge.write_result('resources/csv_out.xml')
        
        self.assertEqual(_result, [['7369', '"SMITH"', '"CLERK"', '7902', '"1980-12-17 00:00:00"', '800', '', '20'], 
                                   ['7499', '"ALLEN"', '"SALESMAN"', '7698', '"1981-02-20 00:00:00"', '1600', '300', '30'], 
                                   ['7521', '"WARD"', '"SALESMAN"', '7698', '"1981-02-22 00:00:00"', '1250', '500', '30'], 
                                   ['7566', '"JONES"', '"MANAGER"', '7839', '"1981-04-02 00:00:00"', '2975', '', '20'], 
                                   ['7654', '"MARTIN"', '"SALESMAN"', '7698', '"1981-09-28 00:00:00"', '1250', '1400', '30'], 
                                   ['7698', '"BLAKE"', '"MANAGER"', '7839', '"1981-05-01 00:00:00"', '2850', '', '30'], 
                                   ['7788', '"SCOTT"', '"ANALYST"', '7566', '"1982-12-09 00:00:00"', '3000', '', '20'], 
                                   ['7782', '"CLARK"', '"MANAGER"', '7839', '"1981-06-09 00:00:00"', '2450', '', '10'], 
                                   ['7839', '"KING"', '"PRESIDENT "', '', '"1981-11-17 00:00:00"', '5000', '', '10'], 
                                   ['7876', '"ADAMS"', '"CLERK"', '7788', '"1983-01-12 00:00:00"', '"1100,5"', '', '20'], 
                                   ['7900', '"JAMES"', '"CLERK"', '7698', '"1981-12-03 00:00:00"', '950', '', '30'], 
                                   ['7902', '"FORD"', '"ANALYST"', '7566', '"1981-12-03 00:00:00"', '3000', '', '20'], 
                                   ['7934', '"MILLER"', '"CLERK"', '7782', '"1982-01-23 00:00:00"', '1300', '', '10']])
        





if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()