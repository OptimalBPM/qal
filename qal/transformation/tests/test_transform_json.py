"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""
import json
import unittest
from datetime import datetime
import os

from qal.common.diff import DictDiffer

from qal.transformation.transform import Trim, Replace, ReplaceRegex, IfEmpty, Cast

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


class TransformTest(unittest.TestCase):
    def load_json(self, _file_name):
        _f = open(_file_name, "r")
        _dict = json.load(_f)
        return _dict

    def test_1_trim(self):
        """Test trim transformation input/output and JSON encoding/decoding"""
        _dict_in = self.load_json(Test_Resource_Dir + "/test_transform.json")
        _json_def = _dict_in["mappings"][0]["transformations"][0]
        _tested = Trim(_json=_json_def)
        _result = _tested.transform(' test ')
        self.assertEqual(_result, ' test', "Results differ")

        _changes = DictDiffer.compare_documents(_json_def, _tested.as_json())
        if len(_changes) > 0:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "The input and output definitions doesn't match.")

    def test_2_IfEmpty(self):
        """Test trim transformation input/output and JSON encoding/decoding"""
        _dict_in = self.load_json(Test_Resource_Dir + "/test_transform.json")
        _json_def = _dict_in["mappings"][0]["transformations"][1]
        _tested = IfEmpty(_json=_json_def)
        _result = _tested.transform(None)
        self.assertEqual(_result, 'NULL', "Results differ")
        _changes = DictDiffer.compare_documents(_json_def, _tested.as_json())
        if len(_changes) > 0:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "The input and output definitions doesn't match.")


    def test_3_replace(self):
        """Test replace transformation input/output and JSON encoding/decoding"""
        _dict_in = self.load_json(Test_Resource_Dir + "/test_transform.json")
        _json_def = _dict_in["mappings"][0]["transformations"][2]
        _tested = Replace(_json=_json_def)
        _result = _tested.transform("unneccessary cc")
        self.assertEqual(_result, "unnecessary cc", "Results differ")
        _changes = DictDiffer.compare_documents(_json_def, _tested.as_json())
        if len(_changes) > 0:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "The input and output definitions doesn't match.")

    def test_4_replace_regex(self):
        """Test replace transformation input/output and JSON encoding/decoding"""
        _dict_in = self.load_json(Test_Resource_Dir + "/test_transform.json")
        _json_def = _dict_in["mappings"][1]["transformations"][2]
        _tested = ReplaceRegex(_json=_json_def)
        _result = _tested.transform("MILLER KING")
        self.assertEqual(_result, "KULLER KUNG", "Results differ")
        _changes = DictDiffer.compare_documents(_json_def, _tested.as_json())
        if len(_changes) > 0:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "The input and output definitions doesn't match.")

    def test_5_cast(self):
        """Test trim transformation input/output and JSON encoding/decoding"""
        _dict_in = self.load_json(Test_Resource_Dir + "/test_transform.json")
        _json_def = _dict_in["mappings"][4]["transformations"][1]
        _tested = Cast(_json=_json_def)
        _result = _tested.transform("2010-01-02 00:00:01")
        self.assertEqual(_result, datetime.strptime("2010-01-02 00:00:01", "%Y-%m-%d %H:%M:%S"), "Results differ")
        _changes = DictDiffer.compare_documents(_json_def, _tested.as_json())
        if len(_changes) > 0:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "The input and output definitions doesn't match.")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()