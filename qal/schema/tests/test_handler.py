"""
    Tests for resources

    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""
import jsonschema

from qal.schema.handler import qal_uri_handler


class Test(unittest.TestCase):
    def test_1_uri_handler(self):
        _test_schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                "resource":
                    {
                        "$ref": "qal://resources.json/definitions/Resource"
                    }
            },
            "required": ["resource"],
        }
        _test_data = {
            "resource": {
                "uuid": "sdfölksdfölk",
                "name": "test",
                "base_path":"",
                "caption": "",
                "type": "",
                "data": ""
            }

        }
        _resolver = RefResolver(base_uri="",
                                handlers=qal_uri_handler, referrer=None, cache_remote=False)
        jsonschema.validators.Draft4Validator(schema=_test_schema, resolver=self.resolver).validate(_test_data)
        .validate(_data, _json_schema_obj)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
