"""
    Tests for resources

    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""
import unittest
import jsonschema
from jsonschema.validators import RefResolver

from qal.schema.handler import qal_uri_handler


class Test(unittest.TestCase):
    def test_1_uri_handler(self):
        _test_schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                "resource":
                    {
                        "$ref": "qal://resources.json#/definitions/Resource"
                    }
            },
            "required": ["resource"],
        }
        _test_data = {
            "resource": {
                "uuid": "d03b44af-2887-4038-93fd-fbba5cbf5362",
                "name": "test",
                "base_path":"",
                "caption": "",
                "type": "",
                "data": ""
            }

        }
        _resolver = RefResolver(base_uri="",
                                handlers={"qal":qal_uri_handler}, referrer=None, cache_remote=False)
        jsonschema.validators.Draft4Validator(schema=_test_schema, resolver=_resolver).validate(_test_data)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
