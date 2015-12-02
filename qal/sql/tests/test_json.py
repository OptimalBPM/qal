"""
Created on Nov 22, 2015

@author: Nicklas Boerjesson

"""
import json
import os
import unittest

from jsonschema import Draft4Validator

from qal.common.diff import diff_strings, DictDiffer
from qal.dal.types import db_types, DB_POSTGRESQL
from qal.sql.json import SQLJSON
from qal.sql.xml import SQLXML

Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = os.path.join(Test_Script_Dir, 'resources')


def sql_for_all_databases(_sqlobj):
    index = 0
    for name in db_types():
        print("SQL for the " + name + " database:")
        print("\n\r<code>")
        print(_sqlobj.as_sql(index))
        print("</code>\n\r")
        index += 1


class ClassSQLMetaXMLTest(unittest.TestCase):
    maxDiff = None

    def _compare_sql_files_for_all_db_types(self, _structure, _prefix, _overwrite=None):
        _db_types = db_types()
        for _curr_db_type_idx in range(len(_db_types)):
            _filename_part = _prefix + "_" + _db_types[_curr_db_type_idx]

            if not _overwrite:
                f = open(Test_Resource_Dir + "/" + _filename_part + "_in.sql", "r", newline='')
                _str_sql_in = f.read()
                f.close()

                _str_sql_out = _structure.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir + "/" + _filename_part + "_out.sql", "w")
                print(_str_sql_out, file=f_out)
                f_out.close()

                # TODO: FIXME: Using offsets to handle weird endline handling is not good.
                self.assertEqual(_str_sql_in[:-2], _str_sql_out[:-1],
                                 _filename_part + " differs from in-file.\n" + diff_strings(_str_sql_in, _str_sql_out))
            else:

                _str_sql_out = _structure.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir + "/" + _filename_part + "_out.sql", "w")
                f_in = open(Test_Resource_Dir + "/" + _filename_part + "_in.sql", "w")
                print(_str_sql_out, file=f_out)
                print(_str_sql_out, file=f_in)

    """Test generation of JSON schema and compare with existing"""

    def test_1_generate_json_schema(self):

        meta_json = SQLJSON()
        print()
        f = open(os.path.join(Test_Resource_Dir, "../../", "JSON.json"), "w")
        _schema = meta_json.generate_schema()

        json.dump(_schema, f)
        f.close()

        Draft4Validator.check_schema(_schema)


        # Check schema



        # TODO: This test generates the JSON schema file, it should not change within patch versions, compare with previous
        # when implementation is done...




        # Test XML-to-Structure with a create table verb and back to XML. Should generate an identical file.

    def test_2_create_table(self):
        _meta_dict = SQLJSON()
        # _meta_dict.debuglevel = 4

        # Generate structure from manual create.
        # from qal.sql.tests.test_sql import gen_simple_create
        # _structure = gen_simple_create()

        f = open(Test_Resource_Dir + "/_test_CREATE_TABLE_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _structure = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_structure)

        f_out = open(Test_Resource_Dir + "/_test_CREATE_TABLE_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()
        _changes = DictDiffer.compare_documents(dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)

    # Test XML-to-Structure with a create table verb and back to XML. Should generate an identical file.
    def test_3_select(self):
        _meta_dict = SQLJSON()
        # _meta_dict.debuglevel = 4

        # Generate structure from manual create.
        # from qal.sql.tests.test_sql import  gen_simple_select
        # _structure = gen_simple_select()

        f = open(Test_Resource_Dir + "/_test_SELECT_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _structure = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_structure)

        f_out = open(Test_Resource_Dir + "/_test_SELECT_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()
        _changes = DictDiffer.compare_documents(dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)

    #        if _str_xml_in != _str_xml_out:
    #            print(diff_strings(_str_xml_in, _str_xml_out))
    #        else:
    #            print("Identical!")
    #        sql_for_all_databases(param)

    def test_4_insert(self):

        _meta_dict = SQLJSON()
        # _meta_dict.debuglevel = 4

        # Generate structure from manual create.
        #from qal.sql.tests.test_sql import gen_simple_insert
        #_structure = gen_simple_insert()

        f = open(Test_Resource_Dir + "/_test_INSERT_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _structure = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_structure)

        f_out = open(Test_Resource_Dir + "/_test_INSERT_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()
        _changes = DictDiffer.compare_documents(dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)

    def test_5_create_index(self):

        _meta_dict = SQLJSON()
        #_meta_dict.debuglevel = 4

        # Generate structure from manual create.
        #from qal.sql.sql import VerbCreateIndex
        #_structure = VerbCreateIndex('ind_Table1ID', "CLUSTERED", 'Table1', ['Table1Name', 'Table1Date'])

        f = open(Test_Resource_Dir + "/_test_CREATE_INDEX_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _structure = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_structure)

        f_out = open(Test_Resource_Dir + "/_test_CREATE_INDEX_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()
        _changes = DictDiffer.compare_documents(dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)



    def test_6_insert_matrix_csv(self):
        _meta_dict = SQLJSON()

        _meta_xml = SQLXML()
        _meta_xml.schema_uri = '../../SQL.xsd'

        f = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_in.xml", "r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in,
                                                    _base_path=Test_Resource_Dir + "/_test_INSERT_matrix_csv_in.xml")

        # Add data matrix
        _structure.data.subsets[0].data_source.field_names = ['Column1', 'Column2']
        _structure.data.subsets[0].data_source.data_table = [['Matrix11', 'Matrix12'], ['Matrix21', 'Matrix22']]
        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_structure, "_test_INSERT_matrix_csv")  # , _overwrite = True)

        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_out.xml", "w")
        print(_str_xml_out, file=f_out)
        f_out.close()

        f_comp = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_cmp.xml", "r")
        _str_xml_comp = f_comp.read()


        self.assertEqual(_str_xml_comp[:-2], _str_xml_out[:-1],
                         'test_6_insert_matrix_csv: The generated XML file differs.\n' + diff_strings(_str_xml_comp,
                                                                                                      _str_xml_out))

        _dict_out = _meta_dict.sql_structure_to_dict(_structure)

        f_out = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()
        _changes = DictDiffer.compare_documents(dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)

"""
    def test_7_delete(self):
        _meta_xml = SQLXML()
        _meta_xml.schema_uri = '../../SQL.xsd'

        f = open(Test_Resource_Dir + "/_test_DELETE_in.xml", "r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in, _base_path=Test_Resource_Dir + "/_test_DELETE_in.xml")

        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_structure, "_test_DELETE", _overwrite=True)

        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir + "/_test_DELETE_out.json", "w")
        print(_str_xml_out, file=f_out)
        f_out.close()

        f_comp = open(Test_Resource_Dir + "/_test_DELETE_cmp.xml", "r")
        _str_xml_comp = f_comp.read()

        self.assertEqual(_str_xml_comp[:-2], _str_xml_out[:-1],
                         'test_7_delete: The generated XML file differs.\n' + diff_strings(_str_xml_comp, _str_xml_out))

    def test_8_update(self):

        _meta_xml = SQLXML()
        _meta_xml.schema_uri = '../../SQL.xsd'

        f = open(Test_Resource_Dir + "/_test_UPDATE_in.xml", "r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in)

        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_structure, "_test_UPDATE", _overwrite=True)

        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir + "/_test_UPDATE_out.json", "w")
        print(_str_xml_out, file=f_out)
        f_out.close()

        f_comp = open(Test_Resource_Dir + "/_test_UPDATE_in.xml", "r")
        _str_xml_comp = f_comp.read()

        self.assertEqual(_str_xml_comp[:-2], _str_xml_out[:-1],
                         'test_8_update: The generated XML file differs.\n' + diff_strings(_str_xml_comp, _str_xml_out))

    def test_9_resource(self):
        # TODO: Describe the requirements for the test.

        _meta_xml = SQLXML()
        _meta_xml.schema_uri = '../../SQL.xsd'

        f = open(Test_Resource_Dir + "/_test_SELECT_resource_in.xml", "r")
        _str_xml_in = f.read()
        _structure = _meta_xml.xml_to_sql_structure(_str_xml_in,
                                                    _base_path=Test_Resource_Dir + "/_test_SELECT_resource_in.xml")

        # Compare with all SQL flavours
        # self._compare_sql_files_for_all_db_types(_structure,"_test_DELETE", _overwrite = True)

        _sql_out = _structure.as_sql(DB_POSTGRESQL)
        print(_sql_out)

        _rows = _structure._dal.query(_sql_out)

        for _row in _rows:
            print(str(_row))
        # Compare compare-file with XML output file
        _xml_out = _meta_xml.sql_structure_to_xml(_structure)
        # noinspection PyUnusedLocal
        _str_xml_out = _xml_out.toxml()
        f_out = open(Test_Resource_Dir + "/_test_SELECT_resource_out.json", "w")
        f_out.close()

        # f_comp = open(Test_Resource_Dir +"/_test_SELECT_resource_cmp.json","r")
        #        _str_xml_comp = f_comp.read()

        #        self.assertEqual(_str_xml_comp[:-2],_str_xml_out[:-1], 'test_insert_matrix_csv:
        # The generated XML file differs.\n'+ diff_strings(_str_xml_comp, _str_xml_out))


"""

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
