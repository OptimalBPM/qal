"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
@note: The tests work against copies of xml_dest_in.xml
"""
import json

import unittest
from shutil import copyfile
import os

from lxml import etree

from qal.dataset.xpath import XpathDataset
from qal.common.resources import Resources
from qal.dataset.custom import DATASET_LOGLEVEL_DETAIL

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


def load_xml(_filename):
    return etree.parse(_filename)


class Test(unittest.TestCase):
    def __init__(self, _method_name='runTest'):
        self.maxDiff = None
        super(Test, self).__init__(_method_name)

    def test_1_apply_new_data(self):
        """This tests checks so that(by the id-attribute):
        1. bk103 and bk105:s titles and bk112: price  but not description is corrected (_update=True)
        2. that bk103:s "test_tag" is unaffected by the update
        2. that bk110 is added
        3. bk103.3 is removed by _delete=True
        4. And that the visual layout of the destination file is kept.
        5. That bk113 is added.
        """

        copyfile(Test_Resource_Dir + "/xml_dest_in.xml", Test_Resource_Dir + "/xml_out.xml")
        _f_r = open(Test_Resource_Dir + "/resources.json", "r")
        _resources_json_dict = json.load(_f_r)
        _resources = Resources(_resources_json_dict=_resources_json_dict, _base_path=Test_Resource_Dir)

        # xml_in
        _source = XpathDataset(_resource=_resources.get_resource("{969A610A-FCA6-4837-B33A-BAA8F13D8B70}"))
        _source._log_level = DATASET_LOGLEVEL_DETAIL
        _source.load(_add_node_ref=True)

        # xml_out
        _destination = XpathDataset(_resource=_resources.get_resource("{969A610A-FCA6-4837-B33A-BAA8F13D8B71}"))
        _destination._log_level = DATASET_LOGLEVEL_DETAIL
        _destination.load(_add_node_ref=True)
        _destination.apply_new_data(_source.data_table, [2], _insert=True, _update=True, _delete=True)

        print(str(_destination.data_table))
        _destination.save(_save_as=Test_Resource_Dir + "/xml_out.xml")

        _f_a = open(Test_Resource_Dir + "/xml_cmp.xml", "r")
        _f_b = open(Test_Resource_Dir + "/xml_out.xml", "r")
        _a = _f_a.read()
        _b = _f_b.read()
        _f_a.close()
        _f_b.close()
        self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")

        # TODO: Add more tests. Especially against no existing destination and more complex files, more levels.


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()