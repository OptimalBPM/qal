"""
    Tests for resources
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""
import json

import unittest
import os
from distlib.util import get_resources_dests
from qal.common.diff import DictDiffer

from qal.common.resources import Resources

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


class Test(unittest.TestCase):
    def test_1_XML_parsing(self):
        f = open(Test_Resource_Dir + "/resources_test.xml", "r")
        _str_xml_in = f.read()
        f.close()
        _resources = Resources()
        # _resources.debuglevel = 4
        _resources.parse_xml(_resources_xml=_str_xml_in)

        self.assertEqual(len(_resources.local_resources), 5, 'Resources are not as many as they should')

        _test_resource = _resources.get_resource("{1D62083E-88F7-4442-920D-0B6CC59BA2FF}")
        self.assertIsNotNone(_test_resource, 'Resource data not found')
        self.assertEqual(_test_resource.caption, "localhost_pg", 'Resource caption do not match')


    def test_2_JSON_parsing(self):

        f = open(Test_Resource_Dir + "/_test_resource_in.json", "r")
        _dict_in = json.loads(f.read())
        f.close()
        _resources = Resources(_resources_json_dict=_dict_in)
        # _resources.debuglevel = 4



        _dict_out = _resources.as_json_dict()

        f_out = open(Test_Resource_Dir + "/_test_resource_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()


        self.assertEqual(len(_resources.local_resources), 5, 'Resources are not as many as they should')

        _test_resource = _resources.get_resource("{1D62083E-88F7-4442-920D-0B6CC59BA2FF}")
        self.assertIsNotNone(_test_resource, 'Resource data not found')
        self.assertEqual(_test_resource.caption, "localhost_pg", 'Resource caption do not match')
        f.close()

        _changes = DictDiffer.compare_documents(_dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()