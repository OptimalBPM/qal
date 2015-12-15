"""
This module hold functionality for validating the qal schemas
"""
_mbe_schema_folder = os.path.join(os.path.dirname(__file__))
__author__ = 'nibo'

def qal_uri_handler(self, uri):
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

