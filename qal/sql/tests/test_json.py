"""
Created on Nov 22, 2015

@author: Nicklas Boerjesson

"""
import json
import os
import unittest

from jsonschema import Draft4Validator, validate

from qal.common.diff import diff_strings, DictDiffer
from qal.common.resources import Resources
from qal.dal.dal import DatabaseAbstractionLayer
from qal.dal.types import db_types, DB_POSTGRESQL
from qal.schema.handler import check_against_qal_schema
from qal.sql.json import SQLJSON
from qal.sql.macros import copy_to_table
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


class ClassSQLMetaJSONTest(unittest.TestCase):
    maxDiff = None

    def _compare_sql_files_for_all_db_types(self, _statement, _prefix, _overwrite=None):
        _db_types = db_types()
        for _curr_db_type_idx in range(len(_db_types)):
            _filename_part = _prefix + "_" + _db_types[_curr_db_type_idx]
            print("Comparing for " + _filename_part)

            if not _overwrite:
                f = open(Test_Resource_Dir + "/" + _filename_part + "_in.sql", "r", newline='')
                _str_sql_in = f.read()
                f.close()

                _str_sql_out = _statement.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir + "/" + _filename_part + "_out.sql", "w")
                print(_str_sql_out, file=f_out)
                f_out.close()

                # TODO: FIXME: Using offsets to handle weird endline handling is not good.
                self.assertEqual(_str_sql_in, _str_sql_out,
                                 _filename_part + " differs from in-file.\n" + diff_strings(_str_sql_in, _str_sql_out))
            else:

                _str_sql_out = _statement.as_sql(_curr_db_type_idx)
                f_out = open(Test_Resource_Dir + "/" + _filename_part + "_out.sql", "w")
                f_in = open(Test_Resource_Dir + "/" + _filename_part + "_in.sql", "w")
                print(_str_sql_out, file=f_out)
                print(_str_sql_out, file=f_in)

    """Test generation of JSON schema and compare with existing"""

    def test_1_generate_json_schema(self):

        meta_json = SQLJSON()
        print()
        f = open(os.path.join(Test_Resource_Dir, "../../../schema/", "sql.json"), "w")
        _schema = meta_json.generate_schema()

        json.dump(obj=_schema, fp=f, sort_keys=True, indent=4)
        f.close()

        Draft4Validator.check_schema(_schema)


        # Check schema



        # TODO: This test generates the JSON schema file, it should not change within patch versions, compare with previous
        # when implementation is done...




        # Test XML-to-Structure with a create table verb and back to XML. Should generate an identical file.

    def validate_json_against_sql_schema(self, _dict):
        _f = open(os.path.join(Test_Resource_Dir, "../../../schema/", "sql.json"), "r")
        _schema = json.loads(_f.read())
        validate(_dict, _schema)


    def test_2_create_table(self):
        _meta_dict = SQLJSON()
        # _meta_dict.debuglevel = 4

        # Generate structure from manual create.
        # from qal.sql.tests.test_sql import gen_simple_create
        # _statement = gen_simple_create()

        f = open(Test_Resource_Dir + "/_test_CREATE_TABLE_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_CREATE_TABLE_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)



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
        # _statement = gen_simple_select()

        f = open(Test_Resource_Dir + "/_test_SELECT_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_SELECT_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)

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
        #_statement = gen_simple_insert()

        f = open(Test_Resource_Dir + "/_test_INSERT_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(dict_in)

        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_INSERT_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)
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
        #_statement = VerbCreateIndex('ind_Table1ID', "CLUSTERED", 'Table1', ['Table1Name', 'Table1Date'])

        f = open(Test_Resource_Dir + "/_test_CREATE_INDEX_in.json", "r")
        _dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(_dict_in, _base_path=Test_Resource_Dir)

        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_CREATE_INDEX_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)
        _changes = DictDiffer.compare_documents(_dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)



    def test_6_insert_matrix_csv(self):
        _meta_dict = SQLJSON()

        f = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_in.json", "r")
        dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(dict_in, _base_path=Test_Resource_Dir)
        # Add data matrix
        _statement.data.subsets[0].data_source.field_names = ['Column1', 'Column2']
        _statement.data.subsets[0].data_source.data_table = [['Matrix11', 'Matrix12'], ['Matrix21', 'Matrix22']]
        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_statement, "_test_INSERT_matrix_csv")  # , _overwrite = True)


        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        f_cmp = open(Test_Resource_Dir + "/_test_INSERT_matrix_csv_cmp.json", "r")
        _dict_cmp = json.loads(f_cmp.read())
        f_cmp.close()
        self.validate_json_against_sql_schema(_dict_out)
        _changes = DictDiffer.compare_documents(_dict_out, _dict_cmp)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False, "test_6_insert_matrix_csv: The generated XML file differs.")


    def test_7_delete(self):

        _meta_dict = SQLJSON()
        f = open(Test_Resource_Dir + "/_test_DELETE_in.json", "r")
        _dict_in = json.loads(f.read())
        f.close()
        _statement = _meta_dict.dict_to_sql_structure(_dict_in, _base_path=Test_Resource_Dir)

        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_statement, "_test_DELETE", _overwrite=True)

        _dict_out = _meta_dict.sql_structure_to_dict(_statement)


        f_out = open(Test_Resource_Dir + "/_test_DELETE_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)
        _changes = DictDiffer.compare_documents(_dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)


    def test_8_update(self):


        _meta_dict = SQLJSON()
        f = open(Test_Resource_Dir + "/_test_UPDATE_in.json", "r")
        _dict_in = json.loads(f.read())
        f.close()

        _statement = _meta_dict.dict_to_sql_structure(_dict_in, _base_path=Test_Resource_Dir)

        # Compare with all SQL flavours
        self._compare_sql_files_for_all_db_types(_statement, "_test_UPDATE", _overwrite=True)



        _dict_out = _meta_dict.sql_structure_to_dict(_statement)

        f_out = open(Test_Resource_Dir + "/_test_UPDATE_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()

        self.validate_json_against_sql_schema(_dict_out)
        _changes = DictDiffer.compare_documents(_dict_in, _dict_out)
        if len(_changes) == 0:
            self.assertTrue(True)
        else:
            DictDiffer.pretty_print_diff(_changes)
            self.assertTrue(False)


    def test_9_resource(self):
        # TODO: Describe the requirements for the test.
        
        # Init tables


        _meta_dict = SQLJSON()
        f = open(Test_Resource_Dir + "/_test_SELECT_resource_in.json", "r")
        _dict_in = json.loads(f.read())
        f.close()


        # Init tables

        _resources = Resources(_resources_list=_dict_in["resources"], _base_path=Test_Resource_Dir)
        _pg_dal = DatabaseAbstractionLayer(_resource=_resources["1D62083E-88F7-4442-920D-0B6CC59BA2FF"])
        copy_to_table(_dal=_pg_dal, _values=[[1, "DataPostgres"]],
                                    _field_names= ["table_postgresID", "table_postgresName"] ,
                                    _field_types=["integer", "string"], _table_name= "table_postgres",
                                    _create_table=True, _drop_existing=True)

        _pg_dal.close()
        _mysql_dal = DatabaseAbstractionLayer(_resource=_resources["DD34A233-47A6-4C16-A26F-195711B49B97"])
        copy_to_table(_dal=_mysql_dal, _values=[[1, "DataMySQL"]],
                                    _field_names=["table_mysqlID", "table_mysqlName"] ,
                                    _field_types=["integer", "string"], _table_name= "table_mysql",
                                    _create_table=True, _drop_existing=True)

        _mysql_dal.close()

        # Start testing

        _statement = _meta_dict.dict_to_sql_structure(_dict_in, _base_path=Test_Resource_Dir)
        _dict_out = _meta_dict.sql_structure_to_dict(_statement, _meta_dict._resources)

        f_out = open(Test_Resource_Dir + "/_test_SELECT_resource_out.json", "w")
        print(json.dumps(_dict_out), file=f_out)
        f_out.close()


        check_against_qal_schema("qal://sql.json", _dict_out)

        # Compare with all SQL flavours
        #self._compare_sql_files_for_all_db_types(_statement,"_test_SELECT_resource_out", _overwrite = True)

        _sql_out = _statement.as_sql(DB_POSTGRESQL)
        print(_sql_out)

        _rows = _statement._dal.query(_sql_out)
        _dict_cmp_data = [[1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'AUD      ', '29.1358 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'BND      ', '24.3550 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'CAD      ', '29.9598 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'CHF      ', '34.4377 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'CNY      ', '5.0313 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'DKK      ', '5.6530 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'EUR      ', '42.2219 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'GBP      ', '50.0854 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'HKD      ', '3.9914 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'IDR      ', '2.4461 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'INR      ', '0.4485 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'JPY      ', '31.8058 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'MYR      ', '9.6064 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'NOK      ', '5.1948 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'NZD      ', '25.6424 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'PHP      ', '0.6994 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'PKR      ', '0.2809 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'SEK      ', '4.8697 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'SGD      ', '24.7693 ', 'MSXML3: A Comprehensive Guide', 36.95], [1, 1, 'DataPostgres', 'DataMySQL', 'csv', 'USD      ', '31.0644 ', 'MSXML3: A Comprehensive Guide', 36.95]]
        print("Data:")
        for _row in _rows:
            print(str(_row))


        self.assertTrue(_rows == _dict_cmp_data)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
