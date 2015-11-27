"""
Created on May 23, 2010

@author: Nicklas Boerjesson
"""
import unittest

from qal.sql.types import *
from qal.dal.types import *
from qal.sql.sql import *
# from qal.tools.diff import diff_strings  # Use when debugging tests


r_create_table_mysql = "CREATE TABLE Table1 (" + DEFAULT_ROWSEP + "\
`Table1ID` INTEGER AUTO_INCREMENT NOT NULL," + DEFAULT_ROWSEP + "\
`Table1Name` VARCHAR(400) NULL," + DEFAULT_ROWSEP + "\
`Table1Changed` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL," + DEFAULT_ROWSEP + "\
CONSTRAINT `PK_Table1_Table1ID` PRIMARY KEY (Table1ID)," + DEFAULT_ROWSEP + "\
CONSTRAINT `FK_Table1_Table1ID_Table2_Table2ID` FOREIGN KEY (Table1ID) REFERENCES Table2(Table2ID)," + DEFAULT_ROWSEP + "\
CONSTRAINT `CK_Table1_Name` CHECK ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas'))," + DEFAULT_ROWSEP + "\
CONSTRAINT `UQ_Table1_Name` UNIQUE (Table1ID)" + DEFAULT_ROWSEP + "\
) ENGINE=InnoDB"

r_create_table_oracle = "CREATE TABLE \"Table1\" (" + DEFAULT_ROWSEP + "\
\"Table1ID\" integer NOT NULL," + DEFAULT_ROWSEP + "\
\"Table1Name\" VARCHAR2(400) NULL," + DEFAULT_ROWSEP + "\
\"Table1Changed\" TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NULL," + DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Tabl\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas'))," + DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")" + DEFAULT_ROWSEP + "\
)"

"""
#TODO: Should the following too be tested?
"+DEFAULT_ROWSEP + "\
CREATE SEQUENCE seq_Table1_Table1ID_DAL_serial"+DEFAULT_ROWSEP + "\
start with 1"+DEFAULT_ROWSEP + "\
increment by 1 "+DEFAULT_ROWSEP + "\
nomaxvalue;"+DEFAULT_ROWSEP + "\
CREATE TRIGGER tr_Table1_Table1ID_DAL_serial"+DEFAULT_ROWSEP + "\
BEFORE INSERT ON Table1 FOR EACH ROW BEGIN"+DEFAULT_ROWSEP +
"SELECT seq_Table1_Table1ID_DAL_serial.nextval INTO :new.id FROM dual;"+DEFAULT_ROWSEP + "\
END;"+DEFAULT_ROWSEP
"""

r_create_table_postgresql = "CREATE TABLE \"Table1\" (" + DEFAULT_ROWSEP + "\
\"Table1ID\" serial NOT NULL," + DEFAULT_ROWSEP + "\
\"Table1Name\" varchar(400) NULL," + DEFAULT_ROWSEP + "\
\"Table1Changed\" timestamp DEFAULT (current_timestamp) NULL," + DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Table2ID\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas'))," + DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")" + DEFAULT_ROWSEP + "\
)"

r_create_table_db2 = "CREATE TABLE \"Table1\" (" + DEFAULT_ROWSEP + "\
\"Table1ID\" INT GENERATED ALWAYS AS IDENTITY NOT NULL," + DEFAULT_ROWSEP + "\
\"Table1Name\" VARCHAR(400) NULL," + DEFAULT_ROWSEP + "\
\"Table1Changed\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL," + DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Table2ID\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\")," + DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas'))," + DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")" + DEFAULT_ROWSEP + "\
)"

r_create_table_sqlserver = "CREATE TABLE Table1 (" + DEFAULT_ROWSEP + "\
[Table1ID] int IDENTITY(1,1) NOT NULL," + DEFAULT_ROWSEP + "\
[Table1Name] varchar(400) NULL," + DEFAULT_ROWSEP + "\
[Table1Changed] DATETIME DEFAULT (GETDATE()) NULL," + DEFAULT_ROWSEP + "\
CONSTRAINT [PK_Table1_Table1ID] PRIMARY KEY (Table1ID)," + DEFAULT_ROWSEP + "\
CONSTRAINT [FK_Table1_Table1ID_Table2_Table2ID] FOREIGN KEY (Table1ID) REFERENCES Table2(Table2ID)," + DEFAULT_ROWSEP + "\
CONSTRAINT [CK_Table1_Name] CHECK ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas'))," + DEFAULT_ROWSEP + "\
CONSTRAINT [UQ_Table1_Name] UNIQUE (Table1ID)" + DEFAULT_ROWSEP + "\
)"


# noinspection PyPep8
r_SELECT_SQL = "SELECT (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS VARCHAR(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) AS Field1, (T2.CountryPrefix + '+' + T2.PhoneNumber) AS Field2 FROM testtable AS T1 JOIN testtable AS T2 ON ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) ORDER BY T1.Field1 desc, T2.Field1 asc" + DEFAULT_ROWSEP + "LIMIT 1"

# noinspection PyPep8
r_SELECT_DB_DB2 = "SELECT (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS VARCHAR(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" AS T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc" + DEFAULT_ROWSEP + "FETCH FIRST 1 ROWS ONLY "

# noinspection PyPep8
r_SELECT_postgresql = "SELECT (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" AS T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc" + DEFAULT_ROWSEP + "LIMIT 1"

# noinspection PyPep8
r_SELECT_oracle = "SELECT (T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS VARCHAR2(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) AND (ROWNUM < 2) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc"

# noinspection PyPep8
r_SELECT_SQL_Server = "SELECT TOP 1 (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) AS Field1, (T2.CountryPrefix + '+' + T2.PhoneNumber) AS Field2 FROM testtable AS T1 JOIN testtable AS T2 ON ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) ORDER BY T1.Field1 desc, T2.Field1 asc"

# noinspection PyPep8
r_UPDATE_my_sql = "SET" + DEFAULT_ROWSEP + "dest_column = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((col_1 = '1') AND (col_2 = '1'))"

# noinspection PyPep8
r_UPDATE_DB2 = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"

# noinspection PyPep8
r_UPDATE_postgresql = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"

# noinspection PyPep8
r_UPDATE_oracle = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"

# noinspection PyPep8
r_UPDATE_SQL_Server = "SET" + DEFAULT_ROWSEP + "dest_column = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((col_1 = '1') AND (col_2 = '1'))"


# Generate test objects.
def gen_simple_condition_1():
    _cond = ParameterCondition(_operator='>', _and_or='AND')
    _cond.left.append(ParameterNumeric(1.3, '+'))
    _cond.right.append(ParameterNumeric(2.4, '+'))

    return _cond


def gen_simple_condition_2():
    _cond = ParameterCondition(_operator='LIKE', _and_or='AND')
    _cond.left.append(ParameterIdentifier('firstname', 'C', 'T1'))
    _cond.right.append(ParameterString('%icklas', '+'))

    return _cond


def gen_simple_conditions():
    _cond = ParameterConditions()

    _cond.append(gen_simple_condition_1())
    _cond.append(gen_simple_condition_2())
    return _cond


def gen_complex_conditions():
    _cond = ParameterConditions()

    _cond.append(gen_simple_condition_1())
    _cond.append(gen_simple_condition_2())
    _cond.append(gen_simple_conditions())
    return _cond


def gen_simple_function():
    param = ParameterFunction(_name='Simple', _operator='C')
    param.parameters.append(gen_simple_cast())
    param.parameters.append(gen_simple_expression_2())
    return param


def gen_simpleexpression_1():
    param = ParameterExpression(_operator='+')
    param.expressionitems.append(ParameterIdentifier('CountryPrefix', 'C', 'T1'))
    param.expressionitems.append(ParameterString('+', 'C'))
    param.expressionitems.append(ParameterIdentifier('PhoneNumber', 'C', 'T1'))
    param.expressionitems.append(gen_simple_function())
    return param


def gen_simple_expression_2():
    param = ParameterExpression(_operator='+')
    param.expressionitems.append(ParameterIdentifier('CountryPrefix', 'C', 'T2'))
    param.expressionitems.append(ParameterString('+', 'C'))
    param.expressionitems.append(ParameterIdentifier('PhoneNumber', 'C', 'T2'))
    return param


def gen_simple_cast():
    param = ParameterCast(None, 'string(200)', 'C')
    param.expression.append(gen_simple_expression_2())
    return param


def gen_complex_expression():
    exp = ParameterExpression()

    param = ParameterExpression()
    param.expressionitems.append(exp)
    param.expressionitems.append(ParameterString('+', 'C', '\\'))
    param.expressionitems.append(ParameterIdentifier('PhoneNumber', 'C', 'T1'))
    return param


def gen_complex_function():
    param = ParameterFunction(_name='Test', _operator='+')
    param.parameters.append(gen_simpleexpression_1())
    param.parameters.append(gen_complex_expression())
    return param


def gen_simple_case():
    param = ParameterCase()
    when1 = ParameterWhen(gen_simple_conditions(), gen_simpleexpression_1())
    param.when_statements.append(when1)
    when2 = ParameterWhen(gen_simple_conditions(), gen_simple_expression_2())
    param.when_statements.append(when2)
    param.else_statement = gen_simple_expression_2()
    return param


def gen_simple_field_1():
    parameter = ParameterField(None, _alias='Field1')
    parameter.expression.append(gen_simpleexpression_1())
    return parameter


def gen_simple_field_2():
    _parameter = ParameterField(None, _alias='Field2')
    _parameter.expression.append(gen_simple_expression_2())
    return _parameter


def gen_simple_source_1():
    # _condlist = SqlList("ParameterCondition")
    _condlist = gen_simple_conditions()
    source = ParameterSource(None, _condlist, 'T1')
    source.expression.append(ParameterIdentifier('testtable', 'C'))
    return source


def gen_simple_source_2():
    # _condlist = SqlList("ParameterCondition")
    _condlist = gen_simple_conditions()
    source = ParameterSource(None, _condlist, 'T2')
    source.expression.append(ParameterIdentifier('testtable', 'C'))
    return source


def gen_simple_select():
    select = VerbSelect(_operator='C')
    select.top_limit = 1
    select.fields.append(gen_simple_field_1())
    select.fields.append(gen_simple_field_2())
    select.sources.append(gen_simple_source_1())
    select.sources.append(gen_simple_source_2())
    po1 = ParameterOrderByItem(_direction="desc")
    po1.expressionitems.append(ParameterIdentifier("Field1", _prefix="T1"))
    select.order_by.append(po1)
    po2 = ParameterOrderByItem(_direction="asc")
    po2.expressionitems.append(ParameterIdentifier("Field1", _prefix="T2"))
    select.order_by.append(po2)
    return select


def gen_simple_insert():
    insert = VerbInsert()
    insert.destination_identifier = ParameterIdentifier("test")
    insert.data = gen_simple_select()
    insert.column_identifiers.append(ParameterIdentifier("Table1ID"))
    insert.column_identifiers.append(ParameterIdentifier("Table1Name"))
    insert.column_identifiers.append(ParameterIdentifier("Table1Changed"))
    return insert


def gen_simple_update():

    _a_1 = ParameterCondition(ParameterIdentifier("col_1"), ParameterString("1"), "=")
    _a_2 = ParameterCondition(ParameterIdentifier("col_2"), ParameterString("1"), "=", "AND")
    _conditions = ParameterConditions()
    _conditions.append(_a_1)
    _conditions.append(_a_2)

    _assignments = SqlList()
    _assignments.append(ParameterAssignment(_left=ParameterIdentifier("dest_column"), _right=ParameterString("Hello")))

    _update = VerbUpdate(_table_identifier=ParameterIdentifier("test"), _assignments=_assignments,
                         _conditions=_conditions)
    return _update


def gen_simple_create():
    col1_constraint1 = ParameterConstraint('PK_Table1_Table1ID', "PRIMARY KEY", [ParameterIdentifier('Table1ID')])
    col1_constraint2 = ParameterConstraint('FK_Table1_Table1ID_Table2_Table2ID', "FOREIGN KEY",
                                           [ParameterIdentifier('Table1ID'), ParameterIdentifier('Table2'),
                                            ParameterIdentifier('Table2ID')])
    col1_constraint3 = ParameterConstraint('CK_Table1_Name', "CHECK", [ParameterIdentifier('Table1ID')])
    col1_constraint3.checkconditions = gen_simple_conditions()
    col1_constraint4 = ParameterConstraint('UQ_Table1_Name', "UNIQUE", [ParameterIdentifier('Table1ID')])
    # col1_constraint5 = ParameterConstraint('DF_Table1_name', C_DEFAULT, ['GETDATE()'])

    col1 = ParameterColumndefinition('Table1ID', 'serial', True)
    col2 = ParameterColumndefinition('Table1Name', 'string(400)', False)
    col3 = ParameterColumndefinition('Table1Changed', 'timestamp', False)
    col3.default = '::currdatetime::'
    # col4 = ParameterColumndefinition('Table1Date', 'datetime', False, 'NOW()')

    result = VerbCreateTable('Table1')
    result.columns.append(col1)
    result.columns.append(col2)
    result.columns.append(col3)
    result.constraints.append(col1_constraint1)
    result.constraints.append(col1_constraint2)
    result.constraints.append(col1_constraint3)
    result.constraints.append(col1_constraint4)
    # result.Constraints.append(col1_constraint5)

    return result


class ParameterTest(unittest.TestCase):
    def test_00_ParameterCondition_simple(self):
        self.maxDiff = None
        param = gen_simple_condition_1()
        paramclass = param.__class__.__name__
        _testvalue = '(1.3 > 2.4)'
        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _testvalue, paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    # noinspection PyPep8
    def test_01_ParameterCondition_complex(self):
        self.maxDiff = None
        param = gen_complex_conditions()
        paramclass = param.__class__.__name__
        _testvalue = "((1.3 > 2.4) AND (T1.firstname LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')))"
        _testvalue_Oracle = "((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')))"
        _testvalue_PostgreSQL = "((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')))"
        _testvalue_DB2 = "((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')))"

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _testvalue_Oracle, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _testvalue_PostgreSQL,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _testvalue_DB2, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')
    # noinspection PyPep8
    def test_02_ParameterExpression_simple(self):
        self.maxDiff = None
        param = gen_simpleexpression_1()
        _testvalue = "(T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS VARCHAR(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber)))"
        # noinspection PyUnusedLocal
        pipe_testvalue = "(T1.CountryPrefix || '+' || T1.PhoneNumber || Simple(CAST((T2.CountryPrefix || '+' || T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix || '+' || T2.PhoneNumber)))"
        _postgres_testvalue = "(T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\")))"
        _db2_testvalue = "(T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS VARCHAR(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\")))"
        _oracle_testvalue = "(T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS VARCHAR2(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\")))"
        _sql_server_testvalue = "(T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber)))"

        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgres_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sql_server_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    def test_03_ParameterCast_simple(self):
        self.maxDiff = None
        param = gen_simple_cast()
        _testvalue = "CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS VARCHAR(200))"
        _sql_server_testvalue = "CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200))"
        _db2_testvalue = "CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS VARCHAR(200))"
        _postgres_testvalue = "CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200))"
        _oracle_testvalue = "CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS VARCHAR2(200))"
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgres_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sql_server_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    # noinspection PyPep8
    def test_04_ParameterFunction_simple(self):
        self.maxDiff = None
        param = gen_simple_function()
        _testvalue = "Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS VARCHAR(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))"
        _sql_server_testvalue = "Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))"
        _db2_testvalue = "Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS VARCHAR(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))"
        _postgres_testvalue = "Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))"
        _oracle_testvalue = "Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS VARCHAR2(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))"
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgres_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sql_server_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    # noinspection PyPep8
    def test_05_ParameterCase(self):
        self.maxDiff = None
        param = gen_simple_case()
        _testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS VARCHAR(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T2.CountryPrefix + '+' + T2.PhoneNumber) else_statement (T2.CountryPrefix + '+' + T2.PhoneNumber) END"
        _postgresql_testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) THEN (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) THEN (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") END"
        _db2_testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS VARCHAR(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") END"
        _oracle_testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS VARCHAR2(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") END"
        _sql_server_testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T2.CountryPrefix + '+' + T2.PhoneNumber) else_statement (T2.CountryPrefix + '+' + T2.PhoneNumber) END"

        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')

        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgresql_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sql_server_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    def test_06_VerbSelect(self):
        self.maxDiff = None
        param = gen_simple_select()
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), r_SELECT_SQL, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), r_SELECT_oracle, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), r_SELECT_postgresql,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), r_SELECT_DB_DB2, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), r_SELECT_SQL_Server, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    def test_07_parameter_create_table(self):
        self.maxDiff = None
        param = gen_simple_create()
        paramclass = param.__class__.__name__

        print(param.as_sql(DB_MYSQL))
        print(r_create_table_mysql)
        self.assertEqual(param.as_sql(DB_MYSQL), r_create_table_mysql, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), r_create_table_oracle, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), r_create_table_postgresql,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), r_create_table_db2, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), r_create_table_sqlserver,
                         paramclass + '.as_sql(DB_SQLSERVER) failed.')
    # noinspection PyPep8
    def test_08_VerbInsert(self):
        self.maxDiff = None
        param = gen_simple_insert()

        _testvalue = "INSERT INTO test (Table1ID, Table1Name, Table1Changed)"
        quoted_testvalue = "INSERT INTO \"test\" (\"Table1ID\", \"Table1Name\", \"Table1Changed\")"
        paramclass = param.__class__.__name__  # self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue + DEFAULT_ROWSEP + r_SELECT_SQL,
                         paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), quoted_testvalue + DEFAULT_ROWSEP + r_SELECT_oracle,
                         paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), quoted_testvalue + DEFAULT_ROWSEP + r_SELECT_postgresql,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), quoted_testvalue + DEFAULT_ROWSEP + r_SELECT_DB_DB2,
                         paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _testvalue + DEFAULT_ROWSEP + r_SELECT_SQL_Server,
                         paramclass + '.as_sql(DB_SQLSERVER) failed.')
    # noinspection PyPep8
    def test_09_VerbUpdate(self):
        self.maxDiff = None
        param = gen_simple_update()

        _testvalue = "UPDATE test"
        quoted_testvalue = 'UPDATE "test"'
        paramclass = param.__class__.__name__  # self.assertEqual(param.as_sql(DB_MYSQL), _testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')

        self.assertEqual(param.as_sql(DB_MYSQL), _testvalue + DEFAULT_ROWSEP + r_UPDATE_my_sql,
                         paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), quoted_testvalue + DEFAULT_ROWSEP + r_UPDATE_oracle,
                         paramclass + '.as_sql(DB_ORACLE) failed.')

        self.assertEqual(param.as_sql(DB_POSTGRESQL), quoted_testvalue + DEFAULT_ROWSEP + r_UPDATE_postgresql,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), quoted_testvalue + DEFAULT_ROWSEP + r_UPDATE_DB2,
                         paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _testvalue + DEFAULT_ROWSEP + r_UPDATE_SQL_Server,
                         paramclass + '.as_sql(DB_SQLSERVER) failed.')
    # noinspection PyPep8
    def test_10_parameter_create_index(self):
        self.maxDiff = None
        param = VerbCreateIndex('ind_Table1ID', "CLUSTERED", 'Table1', ['Table1Name', 'Table1Date'])
        paramclass = param.__class__.__name__
        _mysql_testvalue = 'CREATE CLUSTERED INDEX `ind_Table1ID`' + param.row_separator + 'ON Table1(`Table1Name`, `Table1Date`)'
        _oracle_testvalue = 'CREATE CLUSTERED INDEX "ind_Table1ID"' + param.row_separator + 'ON Table1("Table1Name", "Table1Date")'
        _postgres_testvalue = 'CREATE CLUSTERED INDEX "ind_Table1ID"' + param.row_separator + 'ON Table1("Table1Name", "Table1Date")'
        _db2_testvalue = 'CREATE INDEX "ind_Table1ID"' + param.row_separator + 'ON Table1("Table1Name", "Table1Date")' + param.row_separator + 'CLUSTER'
        _sqlserver_testvalue = 'CREATE CLUSTERED INDEX [ind_Table1ID]' + param.row_separator + 'ON Table1([Table1Name], [Table1Date])'
        self.assertEqual(param.as_sql(DB_MYSQL), _mysql_testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgres_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sqlserver_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')

    def test_11_parameter_drop_table(self):
        self.maxDiff = None
        param = VerbDropTable('test')
        paramclass = param.__class__.__name__
        _mysql_testvalue = 'DROP TABLE test'
        _oracle_testvalue = 'DROP TABLE "test"'
        _postgres_testvalue = 'DROP TABLE "test"'
        _db2_testvalue = 'DROP TABLE "test"'
        _sqlserver_testvalue = 'DROP TABLE test'
        self.assertEqual(param.as_sql(DB_MYSQL), _mysql_testvalue, paramclass + '.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), _oracle_testvalue, paramclass + '.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), _postgres_testvalue,
                         paramclass + '.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), _db2_testvalue, paramclass + '.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), _sqlserver_testvalue, paramclass + '.as_sql(DB_SQLSERVER) failed.')


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()