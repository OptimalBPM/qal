"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
@todo: 
* Add input type checks for better error messages
* Use format string for destination formatting as well (casting from any other primitive type to string)

"""
import re
from datetime import date, datetime

from qal.common.json import set_dict_if_set, set_property_if_in_dict

def perform_transformations(_input, _transformations):
    for _curr_transformation in _transformations:
        _input = _curr_transformation.transform(_input)
    return _input


def make_transformation_array_from_json(_json, _substitution=None):
    _result = []
    for _curr_transformation in _json:
        if _curr_transformation["type"] == 'trim':
            _result.append(Trim(_json= _curr_transformation, _substitution=_substitution))
        elif _curr_transformation["type"] == 'ifempty':
            _result.append(IfEmpty(_json= _curr_transformation, _substitution=_substitution))
        elif _curr_transformation["type"] == 'cast':
            _result.append(Cast(_json= _curr_transformation, _substitution=_substitution))
        elif _curr_transformation["type"] == 'replace':
            _result.append(Replace(_json= _curr_transformation, _substitution=_substitution))
        elif _curr_transformation["type"] == 'replace_regex':
            _result.append(ReplaceRegex(_json= _curr_transformation, _substitution=_substitution))
    return _result



def make_transformation_array_from_json(_json, _substitution=None):
    _result = []
    for _curr_item in _json:
        if _curr_item["type"] == 'trim':
            _result.append(Trim(_json=_curr_item, _substitution=_substitution))
        elif _curr_item["type"] == 'ifempty':
            _result.append(IfEmpty(_json=_curr_item, _substitution=_substitution))
        elif _curr_item["type"] == 'cast':
            _result.append(Cast(_json=_curr_item, _substitution=_substitution))
        elif _curr_item["type"] == 'replace':
            _result.append(Replace(_json=_curr_item, _substitution=_substitution))
        elif _curr_item["type"] == 'replace_regex':
            _result.append(ReplaceRegex(_json=_curr_item, _substitution=_substitution))
    return _result


def make_transformations_json(_transformations):
    _result = []
    for _curr_transformation in _transformations:
        _result.append(_curr_transformation.as_json())

    return _result



class CustomTransformation(object):
    """
    This is the base class for all transformations
    """
    order = None
    """Order dictates when the transformation is run."""
    on_done = None
    """On done is an event, triggered when the transformation has been run.
    Conveys the resulting value or error message."""
    substitution = None
    """An optional instance of the substitution class. Usually shared by several transformations."""

    def __init__(self, _json=None, _substitution=None):
        """
        Constructor
        """
        if _substitution is not None:
            self.substitution = _substitution


        if _json is not None:
            self.load_from_json(_json)

    def do_on_done(self, _value=None, _error=None):
        if self.on_done:
            self.on_done(_value, _error)
        return _value

    def init_base_dict(self, _name):
        _result = {}
        set_dict_if_set(_result, "type", _name)
        set_dict_if_set(_result, "order", self.order)
        return _result

    # noinspection PyPep8
    def as_json(self):

        raise Exception(
            "CustomTransformation.as_json : Should not be called. Not implemented in base class, use init_base_to_node().")

    def load_from_json(self, _json):
        if _json is not None:
            set_property_if_in_dict(self, "order",_json, "Transformations require an order attribute.")
        else:
            raise Exception("CustomTransformation.load_from_json : Base class need a destination node.")

    def transform(self, _value):
        try:
            _result = self._transform(_value)
            return self.do_on_done(_value=_result)
        except Exception as e:
            self.do_on_done(_error="Order: " + str(self.order) + ", " + str(e))
            raise

    def _transform(self, _value):
        """Make transformation"""
        raise Exception("CustomTransformation.transform : Not implemented in base class.")


class Trim(CustomTransformation):
    """
    Trim returns a copy of the string in which all chars have been trimmed from the beginning and the end of the
    string (default whitespace characters).
    If the value parameter is set to either "beginning" or "end", only the left or right end of the string is
    trimmed, respectively.
    """
    value = None

    def load_from_json(self, _json):
        super(Trim, self).load_from_json(_json)
        set_property_if_in_dict(self, "value", _json)

    def as_json(self):
        _result = self.init_base_dict("trim")
        set_dict_if_set(_result, "value", self.value)
        return _result


    def _transform(self, _value):
        """Make transformation"""
        if _value is not None:
            if self.value == "beginning":
                return _value.lstrip()
            elif self.value == "end":
                return _value.rstrip()
            else:
                return _value.strip()


class IfEmpty(CustomTransformation):
    """IfEmpty returns a specified value if the input value is NULL."""
    value = None


    def load_from_json(self, _json):
        super(IfEmpty, self).load_from_json(_json)
        set_property_if_in_dict(self, "value", _json)


    def as_json(self):
        _result = self.init_base_dict("ifempty")
        set_dict_if_set(_result, "value", self.value)
        return _result

    def _transform(self, _value):
        """Make transformation"""
        if _value is None or _value == "":
            if self.substitution is not None:
                return self.substitution.substitute(self.value)
            else:
                return self.value
        else:
            return _value


class Cast(CustomTransformation):
    """Casts a string to the specified type.
    The timestamp date format defaults to the ISO format if format_string is not set.\n
    Possible format string directives at : http://docs.python.org/3.2/library/datetime.html#strftime-strptime-behavior\n
    For example, 2013-11-06 22:05:42 is "%Y-%m-%d %H:%M:%S".
    """

    dest_type = None
    """The destination type"""
    format_string = None
    """A format string where applicable"""


    def load_from_json(self, _json):
        super(Cast, self).load_from_json(_json)
        set_property_if_in_dict(self, "dest_type", _json)
        set_property_if_in_dict(self, "format_string", _json)


    def as_json(self):
        _result = self.init_base_dict("cast")
        set_dict_if_set(_result, "dest_type", self.dest_type)
        set_dict_if_set(_result, "format_string", self.format_string)
        return _result


    def _transform(self, _value):
        """Make cast"""
        try:
            if _value is None or _value == "":
                return _value
            if self.dest_type in ['string', 'string(255)', 'string(3000)']:
                if isinstance(_value, date):
                    if self.format_string is not None and self.format_string != "":
                        return _value.strftime(self.format_string)
                    else:
                        return _value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    return str(_value)
            else:
                if isinstance(_value, str):
                    # All other types will not work with quotations
                    _value = re.sub(r'^["\']|["\']$', '', _value)

            if self.dest_type in ['float']:
                return float(_value)
            elif self.dest_type in ['integer', 'serial']:
                return int(_value)
            elif self.dest_type in ['timestamp']:
                if self.format_string is not None and self.format_string != "":
                    return datetime.strptime(_value, self.format_string)
                else:
                    return datetime.strptime(_value, "%Y-%m-%d %H:%M:%S")

            elif self.dest_type in ['boolean']:
                return bool(_value)
            else:
                raise Exception("Invalid destination data type: " + str(self.dest_type))

        except Exception as e:
            raise Exception("Error in Cast.transform: " + str(e))


class Replace(CustomTransformation):
    """Replace returns a copy of the string in which the occurrences of old have been replaced with new,
    optionally restricting the number of replacements to max."""
    old = None
    """The old value"""
    new = None
    """The new value"""
    max = None
    """The max number of times to replace"""

    def load_from_json(self, _json):
        super(Replace, self).load_from_json(_json)
        set_property_if_in_dict(self, "old",_json)
        set_property_if_in_dict(self, "new",_json)
        set_property_if_in_dict(self, "max",_json)


    def as_json(self):
        _result = self.init_base_dict("replace")
        set_dict_if_set(_result, "old", self.old)
        set_dict_if_set(_result, "new", self.new)
        set_dict_if_set(_result, "max", self.max)
        return _result

    def _transform(self, _value):
        """Make replace transformation"""
        # It is a string operation, None will be handled as a string.
        if _value is None:
            _value = ""
        if self.old is None:
            raise Exception("Replace.transform: old value has to have a value.")
        else:
            _old = self.old

        if _value.find(_old) > -1:
            if self.new is None:
                _new = ""
            else:
                _new = self.new

            if self.substitution is not None and _value.find(_old) > -1:
                _new = self.substitution.substitute(_new)

            if self.max:
                return _value.replace(_old, str(_new), int(self.max))
            else:
                return _value.replace(_old, str(_new))
        else:
            return _value


class ReplaceRegex(CustomTransformation):
    """
    ReplaceRegex returns a copy of the string in which the occurrences of old have been replaced with new,
    optionally restricting the number of replacements to max.
    """
    pattern = None
    """The old value"""
    new = None
    """The new value"""
    max = None
    """The max number of times to replace"""
    compiled_regex = None
    """The compiled regular expression"""

    def load_from_json(self, _json):
        super(ReplaceRegex, self).load_from_json(_json)
        set_property_if_in_dict(self, "pattern",_json)
        set_property_if_in_dict(self, "new",_json)
        set_property_if_in_dict(self, "max",_json)


    def as_json(self):
        _result = self.init_base_dict("replace_regex")
        set_dict_if_set(_result, "pattern", self.pattern)
        set_dict_if_set(_result, "new", self.new)
        set_dict_if_set(_result, "max", self.max)
        return _result

    def _transform(self, _value):
        """Make replace transformation"""
        # It is a string operation, None will be handled as a string.
        if _value is None:
            _value = ""
        if self.pattern is None:
            raise Exception("ReplaceRegex.transform: pattern has to have a value.")
        elif self.compiled_regex is None or self.compiled_regex.pattern != self.pattern:
            self.compiled_regex = re.compile(self.pattern)

        if self.compiled_regex.search(_value):
            if self.new is None:
                _new = ""
            else:
                _new = self.new

            if self.substitution is not None:
                _new = self.substitution.substitute(_new)

            if self.max:
                return self.compiled_regex.sub(_new, _value, int(self.max))
            else:
                return self.compiled_regex.sub(_new, _value)
        else:
            return _value
