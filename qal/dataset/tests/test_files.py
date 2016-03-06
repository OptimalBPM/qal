"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson

"""
import json

import unittest
from shutil import copyfile
import os

from qal.dataset.files import FilesDataset
from qal.common.resources import Resources
from qal.dataset.custom import DATASET_LOGLEVEL_DETAIL

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


class Test(unittest.TestCase):
    def __init__(self, _method_name='runTest'):
        self.maxDiff = None
        super(Test, self).__init__(_method_name)

    def test_load_into_db(self):
        """
        This test loads files into a structure and then back to a file
        """

        # Use the XML dest_in-file to have something to compare
        copyfile(Test_Resource_Dir + "/xml_dest_in.xml", Test_Resource_Dir + "/files_data_xml.xml")
        copyfile(Test_Resource_Dir + "/jpeg_source.jpg", Test_Resource_Dir + "/files_data_jpeg.jpg")
        _f_r = open(Test_Resource_Dir + "/resources.json", "r")
        _resources_list = json.load(_f_r)
        _resources = Resources(_resources_list=_resources_list, _base_path=Test_Resource_Dir)

        # Init tests
        _source = FilesDataset(_resource=_resources.get_resource("{42446be5-12a0-4781-aef6-04d52e6d47d6}"))
        _source._log_level = DATASET_LOGLEVEL_DETAIL
        _source.load()

        # Remove temporary source
        os.remove(Test_Resource_Dir + "/files_data_xml.xml")
        os.remove(Test_Resource_Dir + "/files_data_jpeg.jpg")

        # Write back
        _source.save()

        # Compare XML
        _f_a = open(Test_Resource_Dir + "/xml_dest_in.xml", "r")
        _f_b = open(Test_Resource_Dir + "/files_data_xml.xml", "r")
        _a = _f_a.read()
        _b = _f_b.read()
        _f_a.close()
        _f_b.close()
        self.assertEqual(_a, _b, "test_1_Load_Save: XML-File doesn't match")


        # Compare binary JPG
        _f_a = open(Test_Resource_Dir + "/jpeg_source.jpg", "rb")
        _f_b = open(Test_Resource_Dir + "/files_data_jpeg.jpg", "rb")
        _a = _f_a.read()
        _b = _f_b.read()
        _f_a.close()
        _f_b.close()
        self.assertEqual(_a, _b, "test_1_Load_Save: JPEG-File doesn't match")



if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()