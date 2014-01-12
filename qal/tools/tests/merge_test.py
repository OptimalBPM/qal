"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import unittest
from qal.tools.merge import Merge 
from qal.common.listhelper import pretty_list
from qal.dataset.custom import DATASET_LOGLEVEL_DETAIL
from lxml import etree
from shutil import copyfile
import datetime

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

c_file_result = [
                ['7369', 'SMITH', 'CLERK', 7902, datetime.datetime(1980, 12, 17, 0, 0), 0.0, 800.0, 20.0], 
                ['7499', 'ALLEN', 'SALE;SMAN', 7698, datetime.datetime(1981, 2, 20, 0, 0), 300.0, 1600.0, 30.0], 
                ['7521', 'WARD', 'SALESMAN', 7698, datetime.datetime(1981, 2, 22, 0, 0), 500.0, 1250.0, 30.0], 
                ['7566', 'JONES', 'MANAGER', 7839, datetime.datetime(1981, 4, 2, 0, 0), 0.0, 2975.0, 20.0], 
                ['7654', 'MARTIN', 'SALESMAN', 7698, datetime.datetime(1981, 9, 28, 0, 0), 1400.0, 1250.0, 30.0], 
                ['7698', 'BLAKE', 'MANAGER', 7839, datetime.datetime(1981, 5, 1, 0, 0), 0.0, 2850.0, 30.0], 
                ['7782', 'CLARK', 'MANAGER', 7839, datetime.datetime(1981, 6, 9, 0, 0), 0.0, 2450.0, 10.0], 
                ['7788', 'SCOTT', 'ANALYST', 7566, datetime.datetime(1982, 12, 9, 0, 0), 0.0, 3000.0, 20.0], 
                ['7839', 'KING', 'PRESIDENT', '', datetime.datetime(1981, 11, 17, 0, 0), 0.0, 5000.0, 10.0], 
                ['7876', 'ADAMS', 'CLERK', 7788, datetime.datetime(1983, 1, 12, 0, 0), 0.0, 1100.5, 20.0], 
                ['7900', 'JAMES', 'CLERK', 7698, datetime.datetime(1981, 12, 3, 0, 0), 0.0, 950.0, 30.0], 
                ['7902', 'FORD', 'ANALYST', 7566, datetime.datetime(1981, 12, 3, 0, 0), 0.0, 3000.0, 20.0], 
                ['7934', 'MILLER', 'CLERK', 7782, datetime.datetime(1982, 1, 23, 0, 0), 0.0, 1300.0, 10.0]
                ]






class Merge_test(unittest.TestCase):
    
    def _parse_xml(self, _filename):
        _parser = etree.XMLParser(remove_blank_text=True)
        _tree = etree.ElementTree()
        return _tree.parse(_filename, _parser)
    
    def _test_Merge_files(self):
        
        """Test merge two files"""
        
        copyfile(Test_Resource_Dir + "/csv_dest_orig.csv", Test_Resource_Dir + "/csv_out.csv")
        
        _merge_xml = self._parse_xml(Test_Resource_Dir + "/test_merge_two_files.xml")
        _merge = Merge(_xml_node = _merge_xml)
        _merge.destination_log_level = DATASET_LOGLEVEL_DETAIL
        self.assertEqual(etree.tostring(_merge.as_xml_node()), etree.tostring(_merge_xml), "Input/output XML does not match")


        _result = _merge.execute()
        print("Source:\n" + pretty_list(_merge.source.data_table)) 
        print("Result:\n" + pretty_list(_merge.destination.data_table))
        print("Log:\n" + pretty_list(_merge.destination._log)) 
        
        #_merge.write_result('resources/csv_out.xml')
        self.assertEqual(_result, c_file_result, "Merge result differs")

    def test_Merge_tables(self):
        
        """Test merge two files"""
        _merge_xml = self._parse_xml(Test_Resource_Dir + "/test_merge_two_tables.xml")
        _merge = Merge(_xml_node = _merge_xml)
        self.assertEqual(etree.tostring(_merge.as_xml_node()), etree.tostring(_merge_xml), "Input/output XML does not match")

        _result = _merge.execute()

        self.assertEqual(_result, c_file_result)

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()