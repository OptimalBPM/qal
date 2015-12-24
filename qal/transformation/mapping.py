"""
Created on Oct 20, 2014

@author: Nicklas Boerjesson
"""
from lxml import etree
from qal.transformation.substitution import Substitution

from qal.common.strings import string_to_bool, empty_when_none
from qal.transformation.transform import make_transformation_array_from_xml_node, make_transformations_xml_node, \
    make_transformations_json, make_transformation_array_from_json
from qal.common.xml_utils import xml_isnone


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

    def __init__(self, _xml_node=None, _substitution=None):
        """
        Constructor
        """
        self.transformations = []

        if _substitution is None:
            self.substitution = Substitution()
        else:
            self.substitution = _substitution

        if _xml_node is not None:
            self.load_from_xml_node(_xml_node)

    def load_from_xml_node(self, _xml_node):
        if _xml_node is not None:
            self.is_key = string_to_bool(xml_isnone(_xml_node.find("is_key")))
            self.src_reference = xml_isnone(_xml_node.find("src_reference"))
            self.src_datatype = xml_isnone(_xml_node.find("src_datatype"))
            self.dest_reference = xml_isnone(_xml_node.find("dest_reference"))
            self.transformations = make_transformation_array_from_xml_node(_xml_node.find("transformations"),
                                                                           self.substitution)

    def as_xml_node(self):

        _xml_node = etree.Element("field_mapping")
        etree.SubElement(_xml_node, "is_key").text = str(self.is_key)
        etree.SubElement(_xml_node, "src_reference").text = empty_when_none(self.src_reference)
        etree.SubElement(_xml_node, "src_datatype").text = empty_when_none(self.src_datatype)
        _xml_node.append(make_transformations_xml_node(self.transformations))
        etree.SubElement(_xml_node, "dest_reference").text = empty_when_none(self.dest_reference)

        return _xml_node

    def as_json(self):
        return {
                "is_key": self.is_key,
                "src_reference": self.src_reference,
                "src_datatype": self.src_datatype,
                "dest_reference": self.dest_reference,
                "transformations": make_transformations_json(self.transformations)
            }

    def load_from_json(self, _json):
        if _xml_node is not None:
            self.is_key = _json["is_key"]
            self.src_reference = _json["src_reference"]
            self.src_datatype = _json["src_datatype"]
            self.dest_reference = _json["dest_reference"]
            self.transformations = make_transformation_array_from_json(_json["transformations"],
                                                                           self.substitution)

