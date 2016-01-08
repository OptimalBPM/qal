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

    _schema = _resolver.resolve(_ref)
    _referrer = _resolver.resolve(urlparse(_ref).scheme + "://" + urlparse(_ref).netloc)
    # Then a resolver that is used for validation (it has to have a resolved referrer)
    # TODO: This is not a very nice looking solution, make sure that there is no better solution.
    _resolver_2 = RefResolver(base_uri="", referrer=_referrer[1],
                        handlers={"qal": qal_uri_handler}, cache_remote=True)
    Draft4Validator(schema=_schema[1], resolver=_resolver_2).validate(_data)