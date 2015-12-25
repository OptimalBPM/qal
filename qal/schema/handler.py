"""
This module hold functionality for validating the qal schemas
"""
import json
import os
from urllib.parse import urlparse
import jsonschema
from jsonschema.validators import RefResolver, Draft4Validator

_mbe_schema_folder = os.path.join(os.path.dirname(__file__))
__author__ = 'nibo'

def qal_uri_handler(uri):
    """

    Handle the qal:// namespace references

    :param uri: The uri to handle
    :return: The schema
    """

    # Use urlparse to parse the file location from the URI
    _file_location = os.path.abspath(os.path.join(_mbe_schema_folder, urlparse(uri).netloc))

    # noinspection PyTypeChecker
    _schema_file = open(_file_location, "r", encoding="utf-8")
    _json = json.loads(_schema_file.read())
    # Cumbersome, but needs to close the file properly
    _schema_file.close()

    return _json

def check_against_qal_schema(_ref, _data):
    """ Check JSON against given schema
    :param _schema_name: The name of the schema; "resources"
    :param _data: The data to be validated
    """
    _resolver = RefResolver(base_uri="",
                        handlers={"qal":qal_uri_handler}, referrer=None, cache_remote=False)

    _schema = _resolver.resolve(_ref)
    Draft4Validator(schema=_schema[1], resolver=_resolver).validate(_data)