"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
"""

import unittest

from qal.dataset.rdbms import RDBMS_Dataset
from qal.common.resources import Resources
from lxml import etree
from qal.tools.diff import diff_files


import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

def load_xml(_filename):
    return etree.parse(_filename)

class Test(unittest.TestCase):


    def test_1_Load_Save(self, _has_header = None, _resource = None):
        _resources_node = load_xml(Test_Resource_Dir + "/resources.xml").find("resources")
        _resources = Resources(_resources_node = _resources_node)
        _d_pgsql = RDBMS_Dataset(_resource= _resources.get_resource("{1D62083E-88F7-4442-920D-0B6CC59BA2FF}"))
        _d_pgsql.load()
        print(str(_d_pgsql.data_table))
        
        _d_mysql = RDBMS_Dataset(_resource= _resources.get_resource("{DD34A233-47A6-4C16-A26F-195711B49B97}"))
        _d_mysql.load()

        print(str(_d_pgsql.data_table))        
        
        
        _d_mysql.apply_new_data(_d_pgsql.data_table, [2])
        
        _d_mysql.save()
        _d_mysql.load()

        #self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")
        
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()