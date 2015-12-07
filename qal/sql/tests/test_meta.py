"""
Created on Sep 21, 2010

@author: Nicklas Boerjesson
@note: These tests have no real purpose since they in practice
only tests what objects there are in the SQL-unit.
The tests need to be redesigned to have a point.
Therefore, they have been commented out.

"""
import unittest

from qal.common.meta import list_parameter_classes, list_verb_classes, list_class_properties


class Test(unittest.TestCase):
    def _test_list_parameter_classes(self):
        self.assertEqual(list_parameter_classes(),
            ['ParameterConditions', 'ParameterCondition', 'ParameterConditions', 'ParameterString', 'ParameterBase',
             'ParameterFunction', 'ParameterExpression', 'ParameterIdentifier', 'ParameterColumndefinition',
             'ParameterCast', 'ParameterWhen', 'ParameterConstraint', 'ParameterNumeric', 'ParameterExpressionItem',
             'Parameter_DML', 'ParameterSource', 'ParameterCase', 'ParameterField'])

    def _test_list_verb_classes(self):
        self.assertEqual(list_verb_classes(),
            ['VerbCreateIndex', 'VerbCreateTable', 'VerbSelect', 'VerbDelete', 'VerbCustom'])

    def _testList_class_properties(self):
        self.assertEqual(list_class_properties('VerbCreateTable'), ['Name', 'Columns', 'Constraints'])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()