import json
import os
from jsonschema.validators import Draft4Validator
from qal.transformation import generate_schema

__author__ = 'nibo'

import unittest

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


class MyTestCase(unittest.TestCase):


    def test_json_schema(self):
        """This test will actually generate the schema for all classes in the transformation module"""
        _schema = generate_schema()
        f_out = open(os.path.join(Test_Resource_Dir, "../../../", "schema/transformation.json"), "w")
        json.dump(obj=_schema, fp=f_out, sort_keys=True, indent=4)
        f_out.close()

        Draft4Validator.check_schema(_schema)


if __name__ == '__main__':
    unittest.main()
