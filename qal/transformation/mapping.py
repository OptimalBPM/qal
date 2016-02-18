"""
Created on Oct 20, 2014

@author: Nicklas Boerjesson
"""

from qal.transformation.substitution import Substitution

from qal.common.strings import string_to_bool, empty_when_none
from qal.transformation.transform import make_transformations_json, make_transformation_array_from_json



class Mapping(object):
    is_key = None
    """If true, the mapping is a key field"""
    src_reference = None
    """A reference to the source data location within its dataset. Can be a field name, XPath or similar."""
    src_datatype = None
    """The source data data type"""
    dest_reference = None
    """A reference to the destination data location within its dataset. Can be a field name, XPath or similar."""
    substitution = None
    """An instance of the substitution class. Kept for maintaining things lite incrementors and similar."""

    def __init__(self, _substitution=None, _json=None):
        """
        Constructor
        """
        self.transformations = []

        if _substitution is None:
            self.substitution = Substitution()
        else:
            self.substitution = _substitution

        if _json is not None:
            self.load_from_json(_json)

    def load_from_json(self, _json):
        try:
            self.is_key = _json["is_key"]
            self.src_reference = _json["src_reference"]
            self.src_datatype = _json["src_datatype"]
            self.dest_reference = _json["dest_reference"]
            self.transformations = make_transformation_array_from_json(_json["transformations"], self.substitution)
        except KeyError as e:
            raise Exception("Mapping.load_from_json error loading configuration: Missing key: " + str(e) + ", data\n" +
                            str(_json))


    def as_json(self):
        return {
                "is_key": self.is_key,
                "src_reference": self.src_reference,
                "src_datatype": self.src_datatype,
                "dest_reference": self.dest_reference,
                "transformations": make_transformations_json(self.transformations)
            }

