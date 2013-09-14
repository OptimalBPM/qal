'''
Created on Sep 13, 2013

@author: Nicklas Boerjesson
@note: 

'''
import unittest
from qal.common.resources import Resources

import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'

class Test(unittest.TestCase):


    def test_1_XML_parsing(self):
        _xml_node = None
        
        f = open(Test_Resource_Dir +"/resources_test.xml","r")
        _str_xml_in = f.read()
        _resources = Resources()
        #_resources.debuglevel = 4
        _resources.parse_xml(_resources_xml=_str_xml_in)
        
        self.assertEqual(len(_resources.local_resources), 2, 'Resources are not as many as they should')
                        
        _test_resource = _resources.get_resource("{1D62083E-88F7-4442-920D-0B6CC59BA2FF}")
        self.assertIsNotNone(_test_resource, 'Resource data not found')        
        self.assertEqual(_test_resource.caption,"localhost_pg", 'Resource caption do not match')
        f.close()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()