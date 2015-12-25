"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
"""
import json

import unittest
import os

from lxml import etree

from qal.dataset.flatfile import FlatfileDataset
from qal.common.resources import Resources
from qal.common.listhelper import pretty_list
from qal.dataset.custom import DATASET_LOGLEVEL_DETAIL

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


def load_xml(_filename):
    return etree.parse(_filename)


class Test(unittest.TestCase):
    def test_1_Load_Save(self):
        _f_r = open(Test_Resource_Dir + "/resources.json", "r")
        _resources_list = json.load(_f_r)
        _resources = Resources(_resources_list=_resources_list, _base_path=Test_Resource_Dir)
        _da = FlatfileDataset(_resource=_resources["{86470370-FF78-48A4-9759-A3BAE4EE22FE}"])
        _da._log_level = DATASET_LOGLEVEL_DETAIL
        _da.load()
        print("Source: " + pretty_list(_da.data_table))
        _da.save(_save_as=os.path.join(Test_Resource_Dir, "csv_out.csv"))
        print("Log: " + pretty_list(_da._log))
        _f_a = open(os.path.join(Test_Resource_Dir, "csv_out.csv"), "r")
        _f_b = open(os.path.join(Test_Resource_Dir, "csv_cmp.csv"), "r")
        _a = _f_a.read()
        _b = _f_b.read()
        _f_a.close()
        _f_b.close()

        self.maxDiff = None
        self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()