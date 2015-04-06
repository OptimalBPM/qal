"""
    Tests for substitutions
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""
import datetime
import unittest
import os
from uuid import UUID
from getpass import getuser

from source.qal.common.substitution import Substitution

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')

subst = Substitution()
subst.set_identity(0)


class Test(unittest.TestCase):
    def test_1_single_value(self):
        self.assertEqual(0, subst.substitute("::identity::"))

    def test_2_mix_value(self):
        self.assertEqual("Hello 1", subst.substitute("Hello ::identity::"))

    def test_3_no_value(self):
        self.assertEqual("Hello", subst.substitute("Hello"))

    def test_4_mix_multiple(self):
        self.assertEqual("Hello 2 " + getuser(), subst.substitute("Hello ::identity:: ::username::"))

    def test_5_datetime(self):
        _result = subst.substitute("::curr_datetime::")
        self.assertTrue(isinstance(_result, datetime.datetime))

    def test_6_uuid(self):
        _result = subst.substitute("::uuid::")
        self.assertTrue(isinstance(_result, UUID))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()