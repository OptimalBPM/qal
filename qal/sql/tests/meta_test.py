'''
Created on Sep 21, 2010

@author: Nicklas Boerjesson
@note: These tests have no real purpose since they in practice 
only tests what objects there are in the SQL-unit. 
The tests need to be redesigned to have a point.
Therefore, they have been commented out.

'''
import unittest
from qal.sql.meta import list_parameter_classes, list_verb_classes, list_class_properties

class Test(unittest.TestCase):


    def _test_list_parameter_classes(self):
        self.assertEqual(list_parameter_classes(),['Parameter_Conditions','Parameter_Condition', 'Parameter_Conditions', 'Parameter_String', 'Parameter_Base', 'Parameter_Function', 'Parameter_Expression', 'Parameter_Identifier', 'Parameter_ColumnDefinition', 'Parameter_Cast', 'Parameter_WHEN', 'Parameter_Constraint', 'Parameter_Numeric', 'Parameter_Expression_Item', 'Parameter_DML', 'Parameter_Source', 'Parameter_CASE', 'Parameter_Field'])
        
    def _test_list_verb_classes(self):
        self.assertEqual(list_verb_classes(), ['Verb_CREATE_INDEX', 'Verb_CREATE_TABLE', 'Verb_SELECT', 'Verb_DELETE', 'Verb_Custom'])
        
    def _testList_class_properties(self):
        self.assertEqual(list_class_properties('Verb_CREATE_TABLE'), ['Name', 'Columns', 'Constraints'])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDev']
    unittest.main()