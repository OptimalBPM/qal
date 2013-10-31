'''
Created on Sep 21, 2010

@author: Nicklas Boerjesson
@note: 
The xml_to_sql_structure-to-sql_structure_to_xml conversion is not at all times reversable as classes
have default values which may make the output file differ from the input unless all fields are set.

From test 6 and forward, also SQL:s for all platforms are being tested in the same test. This is done here instead of 
in the sql_test for convenience and that those tests use datasets.
'''
import unittest
from qal.sql.sql_xml import SQL_XML
from qal.sql.tests.sql_test import my_diff #, gen_simple_insert, gen_simple_select, gen_simple_create
from qal.dal.dal_types import db_types, DB_POSTGRESQL


import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'


def sql_for_all_databases(_SQLObj):  
    index = 0
    for name in db_types():
        print("SQL for the " + name + " database:")
        print("\n\r<code>")
        print(_SQLObj.as_sql(index))
        print("</code>\n\r")
        index = index + 1        
            
class class_SQL_Meta_XML_Test(unittest.TestCase):

    maxDiff = None
    
    def _compare_sql_files_for_all_db_types(self, _structure, _prefix, _overwrite = None):
        _db_types = db_types()
        for _curr_db_type_idx in range(len(_db_types)):
            _filename_part = _prefix +"_" + _db_types[_curr_db_type_idx] 
            
            if not _overwrite:
                f = open(Test_Resource_Dir +"/" + _filename_part + "_in.sql","r", newline = '')
                _str_sql_in = f.read()
                f.close()
                
                _str_sql_out = _structure.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir +"/" + _filename_part + "_out.sql","w")
                print(_str_sql_out, file=f_out)
                f_out.close()
                
                # TODO: FIXME: Using offsets to handle weird endline handling is not good.
                self.assertEqual(_str_sql_in[:-2], _str_sql_out[:-1],
                                 _filename_part + " differs from in-file.\n"+ my_diff(_str_sql_in, _str_sql_out))
            else:         
    
                _str_sql_out = _structure.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir +"/" + _filename_part + "_out.sql","w")
                f_in = open(Test_Resource_Dir +"/" + _filename_part + "_in.sql","w")
                print(_str_sql_out, file=f_out)
                print(_str_sql_out, file=f_in)
         
    
    
    # TODO: This test generates the SQL schema file. It sou
    def test_1_generate_xml_Schema(self):
        meta_xml = SQL_XML()
        f = open("SQL.xsd","w")
        meta_xml.schema_uri = '../SQL.xsd'
        print( meta_xml.generate_schema().toprettyxml(indent = '  '),file=f)
        f.close()
        # TODO: Compare to old XML Schema. Or don̈́'t. Only ties down development?
        
     
        
        # Test XML-to-Structure with a create table verb and back to XML. Should generate an identical file.
    def test_2_create_table(self):
        meta_xml = SQL_XML()
        

#        Generate structure from manual create. 
#        from test_sql import gen_simple_create
#        param = gen_simple_create()
#        meta_xml.schema_uri = '../SQL.xsd'
#        _XMLOut = meta_xml.sql_structure_to_xml(param)
#        _str_xml_out = _XMLOut.toxml()
               
#        meta_xml.debuglevel = 4
        f = open(Test_Resource_Dir +"/_test_CREATE_TABLE_in.xml","r")
        _str_xml_in = f.read()
        f.close()
        structure = meta_xml.xml_to_sql_structure(_str_xml_in)
        meta_xml.schema_uri = '../SQL.xsd'        
        _XMLOut = meta_xml.sql_structure_to_xml(structure)
        _str_xml_out = _XMLOut.toxml()


        
        f_out = open(Test_Resource_Dir +"/_test_CREATE_TABLE_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()
        
        self.assertEqual(_str_xml_in,_str_xml_out)
#        
#        print(my_diff(_str_xml_in,_str_xml_out))
        #self.assertEqual(_str_xml_in,_str_xml_out)
           
        # sql_for_all_databases(_XMLOut)
        
        
        # Test XML-to-Structure with a create table verb and back to XML. Should generate an identical file.
    def test_3_select(self):
        meta_xml = SQL_XML()
#        meta_xml.debuglevel = 4
        
#        Generate structure from manual SELECT. 
#         from test_sql import  gen_simple_select
#        param = gen_simple_select()
#        meta_xml.schema_uri = '../SQL.xsd'
#        _XMLOut = meta_xml.sql_structure_to_xml(param)
               
#         
        f = open(Test_Resource_Dir +"/_test_SELECT_in.xml","r")
        _str_xml_in = f.read()
        f.close()
        structure = meta_xml.xml_to_sql_structure(_str_xml_in)
        meta_xml.schema_uri = '../SQL.xsd'        
        _XMLOut = meta_xml.sql_structure_to_xml(structure)
        
        
        
        _str_xml_out = _XMLOut.toxml()
        f_out = open(Test_Resource_Dir +"/_test_SELECT_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()

        self.assertEqual(_str_xml_in,_str_xml_out)
        
#        if _str_xml_in != _str_xml_out:
#            print(my_diff(_str_xml_in, _str_xml_out))
#        else:
#            print("Identical!")
#        sql_for_all_databases(param)

    def test_4_insert(self):
        meta_xml = SQL_XML()
#        meta_xml.debuglevel = 4
        
#        Generate structure from manual SELECT.
#        from test_sql import gen_simple_insert
#        param = gen_simple_insert()
#        meta_xml.schema_uri = '../SQL.xsd'
#        _XMLOut = meta_xml.sql_structure_to_xml(param)

         
        f = open(Test_Resource_Dir +"/_test_INSERT_in.xml","r")
        _str_xml_in = f.read()
        f.close()
        structure = meta_xml.xml_to_sql_structure(_str_xml_in)
        meta_xml.schema_uri = '../SQL.xsd'        
        _XMLOut = meta_xml.sql_structure_to_xml(structure)
        
        
        
        _str_xml_out = _XMLOut.toxml()
        f_out = open(Test_Resource_Dir +"/_test_INSERT_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()
        

        self.assertEqual(_str_xml_in,_str_xml_out)
        
        
    def test_5_create_index(self):
        meta_xml = SQL_XML()
        meta_xml.schema_uri = '../SQL.xsd'      
#        meta_xml.debuglevel = 4
        
#        Generate structure from manual CREATE_INDEX. 
#        from dal.sql import Verb_CREATE_INDEX
#        structure = Verb_CREATE_INDEX('ind_Table1ID', "CLUSTERED", 'Table1', ['Table1Name', 'Table1Date'])
#         
        f = open(Test_Resource_Dir +"/_test_CREATE_INDEX_in.xml","r")
        _str_xml_in = f.read()
        f.close()
        structure = meta_xml.xml_to_sql_structure(_str_xml_in)


        _xml_out = meta_xml.sql_structure_to_xml(structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir +"/_test_CREATE_INDEX_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()
        

        self.assertEqual(_str_xml_in,_str_xml_out)


    
    def test_6_insert_matrix_csv(self):
        _meta_xml = SQL_XML()
        _meta_xml.schema_uri = '../../dal/SQL.xsd'      

        f = open(Test_Resource_Dir +"/_test_INSERT_matrix_csv_in.xml","r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in)

        # Add data matrix
        _structure.data.subsets[0].data_source.field_names = ['Column1', 'Column2']   
        _structure.data.subsets[0].data_source.data_table = [['Matrix11','Matrix12'],['Matrix21', 'Matrix22']]
        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_structure,"_test_INSERT_matrix_csv")#, _overwrite = True)
        
        
        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir +"/_test_INSERT_matrix_csv_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()
        
        f_comp = open(Test_Resource_Dir +"/_test_INSERT_matrix_csv_cmp.xml","r")
        _str_xml_comp = f_comp.read()


        self.assertEqual(_str_xml_comp[:-2],_str_xml_out[:-1], 'test_6_insert_matrix_csv: The generated XML file differs.\n'+ my_diff(_str_xml_comp, _str_xml_out))

    def test_7_delete(self):
        _meta_xml = SQL_XML()
        _meta_xml.schema_uri = '../../dal/SQL.xsd'      

        f = open(Test_Resource_Dir +"/_test_DELETE_in.xml","r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in)

        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_structure,"_test_DELETE", _overwrite = True)
        
        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir +"/_test_DELETE_out.xml","w")
        print(_str_xml_out, file=f_out)
        f_out.close()
        
        f_comp = open(Test_Resource_Dir +"/_test_DELETE_cmp.xml","r")
        _str_xml_comp = f_comp.read()


        self.assertEqual(_str_xml_comp[:-2],_str_xml_out[:-1], 'test_7_delete: The generated XML file differs.\n'+ my_diff(_str_xml_comp, _str_xml_out))

    def test_8_resource(self):
        # TODO: Describe the requirements for the test.
        
        _meta_xml = SQL_XML()
        _meta_xml.schema_uri = '../../dal/SQL.xsd'      

        f = open(Test_Resource_Dir +"/_test_SELECT_resource_in.xml","r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in)

        # Compare with all SQL flavours
        #self._compare_sql_files_for_all_db_types(_structure,"_test_DELETE", _overwrite = True)

        
        _sql_out = _structure.as_sql(DB_POSTGRESQL)
        print(_sql_out)
        _rows = _structure._dal.query(_sql_out)
        
        for _row in _rows:
            print(str(_row))
        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir +"/_test_SELECT_resource_out.xml","w")
        f_out.close()
        
#        f_comp = open(Test_Resource_Dir +"/_test_SELECT_resource_cmp.xml","r")
#        _str_xml_comp = f_comp.read()


#        self.assertEqual(_str_xml_comp[:-2],_str_xml_out[:-1], 'test_insert_matrix_csv: The generated XML file differs.\n'+ my_diff(_str_xml_comp, _str_xml_out))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()