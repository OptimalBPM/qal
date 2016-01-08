"""
This module hold functionality for validating the qal schemas
"""
import json
import os
from urllib.parse import urlparse
import jsonschema
from jsonschema.validators import RefResolver, Draft4Validator

_qal_schema_folder = os.path.join(os.path.dirname(__file__))
__author__ = 'Nicklas Borjesson'

def qal_uri_handler(uri):
    """

    Handle the qal:// namespace references

    :param uri: The uri to handle
    :return: The schema
    """

    # Use urlparse to parse the file location from the URI
    _file_location = os.path.abspath(os.path.join(_qal_schema_folder, urlparse(uri).netloc))

    # noinspection PyTypeChecker
    _schema_file = open(_file_location, "r", encoding="utf-8")
    _json = json.loads(_schema_file.read())
    # Cumbersome, but needs to close the file properly
    _schema_file.close()

    return _json

def check_against_qal_schema(_ref, _data):
    """ Check JSON against given schema
    :param _ref: The name of the schema; "resources"
    :param _data: The data to be validated
    """
    # First create a resolver to resolve the current schema.
    _resolver = RefResolver(base_uri="",
                        handlers={"qal": qal_uri_handler}, referrer=None, cache_remote=True)

    _schema = _resolver.resolve(_ref)[1]
    _base_ref = urlparse(_ref).scheme + "://" + urlparse(_ref).netloc
    if _ref != _base_ref:
        _resolver.referrer = _resolver.resolve(_base_ref)[1]
    else:
        _resolver.referrer = _schema

    _resolver.store[""] = _resolver.referrer

    Draft4Validator(schema=_schema, resolver=_resolver).validate(_data)