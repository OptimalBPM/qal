"""
    Variable substitution functionality
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

import datetime
import uuid
from getpass import getuser


class Substitution(object):
    """The Substitution class is responsible for replacing and generating variables for a substitution session.
    Using a session one can, for example, keep track of an identity increment.
    """
    builtin_substitutions = None

    def __init__(self):
        self.builtin_substitutions = {
            "identity": self._builtin_identity,
            "uuid": self._builtin_uuid,
            "username": self._builtin_username,
            "curr_datetime": self._builtin_curr_datetime

        }


    def _builtin_username(self):
        """TODO: Secure this, this value depends on environment variables. On the other hand, the users doesn't need
        to use this function to insert a false user name.
        """
        return getuser()
    def _builtin_curr_datetime(self):
        return datetime.datetime.now()
    def _builtin_uuid(self):
        return uuid.uuid4()
    def _builtin_identity(self):
        """Returns an int identity value"""
        if not hasattr(self, "_builtin_identity_value"):
            self._builtin_identity_value = 0

        _result = self._builtin_identity_value
        self._builtin_identity_value += 1
        return _result

    def set_identity(self, _value):
        self._builtin_identity_value = int(_value)

    def replace(self, _input):
        """ The replace method scans _input for known substitution variables.
        It tries to keep the value within the same data type.
        :param _input: The substitution string, potentially holding substitution variables
        :return: Returns a value where all substitutions are made
        """

        for _var_name, _var_func in self.builtin_substitutions.items():
            _curr_subst = "::" + _var_name + "::"
            if _input == _curr_subst:
                # Its a single value, return value immidiately
                return _var_func()
            else:
                if _input.find(_curr_subst) > -1:
                    _input = _input.replace(_curr_subst, str(_var_func()))

        return _input








# Built-in variables