"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
@note: The tests work against copies of xml_dest_in.xml
"""

import unittest
from shutil import copyfile

from qal.dataset.xpath import XPath_Dataset
from qal.common.resources import Resources
from lxml import etree
from qal.tools.diff import diff_files
import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

def load_xml(_filename):
    return etree.parse(_filename)




class Test(unittest.TestCase):


    def test_1_Load_Save_update_delete(self):
        """This tests checks so that(by the id-attribute):
        1. bk103 and bk105:s titles and bk112: price is corrected (_update=True)
        2. that bk103:s "test_tag" is unaffected by the update
        2. that bk110 is added
        3. bk103.3 is removed by _delete=True
        4. And that the visual layout of the destination file is kept. 
        """
        
        copyfile(Test_Resource_Dir + "/xml_dest_in.xml", Test_Resource_Dir + "/xml_out.xml")
        
        _resources_node = load_xml(Test_Resource_Dir + "/resources.xml").find("resources")
        _resources = Resources(_resources_node = _resources_node)
        _da = XPath_Dataset(_resource= _resources.get_resource("{969A610A-FCA6-4837-B33A-BAA8F13D8B70}"))
        _da.load()
        print(str(_da.data_table))
        _da.save(_apply_to = Test_Resource_Dir + "/xml_out.xml", _update = True, _delete = True)
        
        _f_a = open(Test_Resource_Dir + "/xml_out.xml", "r")
        _f_b = open(Test_Resource_Dir + "/xml_cmp.xml", "r")
        _a = _f_a.read()
        _b = _f_b.read()
        _f_a.close()
        _f_b.close()
        self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")
        
        #TODO: Add more tests. Especially against no existing destination and more complex files, more levels.
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()