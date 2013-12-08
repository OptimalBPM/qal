'''
Created on May 23, 2010

@author: Nicklas Boerjesson
'''
import unittest
import difflib
from qal.sql.sql_types import *
from qal.dal.dal_types import *
from qal.sql.sql import *

global r_create_table_mysql 
r_create_table_mysql = "CREATE TABLE Table1 ("+DEFAULT_ROWSEP + "\
`Table1ID` INTEGER AUTO_INCREMENT NOT NULL,"+DEFAULT_ROWSEP + "\
`Table1Name` VARCHAR(400) NULL,"+DEFAULT_ROWSEP + "\
`Table1Changed` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,"+DEFAULT_ROWSEP + "\
CONSTRAINT `PK_Table1_Table1ID` PRIMARY KEY (Table1ID),"+DEFAULT_ROWSEP + "\
CONSTRAINT `FK_Table1_Table1ID_Table2_Table2ID` FOREIGN KEY (Table1ID) REFERENCES Table2(Table2ID),"+DEFAULT_ROWSEP + "\
CONSTRAINT `CK_Table1_Name` CHECK ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')),"+DEFAULT_ROWSEP + "\
CONSTRAINT `UQ_Table1_Name` UNIQUE (Table1ID)"+DEFAULT_ROWSEP + "\
) ENGINE=InnoDB"

global r_create_table_oracle
r_create_table_oracle = "CREATE TABLE \"Table1\" ("+DEFAULT_ROWSEP + "\
\"Table1ID\" integer NOT NULL,"+DEFAULT_ROWSEP + "\
\"Table1Name\" VARCHAR2(400) NULL,"+DEFAULT_ROWSEP + "\
\"Table1Changed\" TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NULL,"+DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Tabl\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")"+DEFAULT_ROWSEP + "\
)"
#TODO: Should the following too be tested?
#"+DEFAULT_ROWSEP + "\
#CREATE SEQUENCE seq_Table1_Table1ID_DAL_serial"+DEFAULT_ROWSEP + "\
#start with 1"+DEFAULT_ROWSEP + "\
#increment by 1 "+DEFAULT_ROWSEP + "\
#nomaxvalue;"+DEFAULT_ROWSEP + "\
#CREATE TRIGGER tr_Table1_Table1ID_DAL_serial"+DEFAULT_ROWSEP + "\
#BEFORE INSERT ON Table1 FOR EACH ROW BEGIN"+DEFAULT_ROWSEP + "SELECT seq_Table1_Table1ID_DAL_serial.nextval INTO :new.id FROM dual;"+DEFAULT_ROWSEP + "\
#END;"+DEFAULT_ROWSEP 

global r_create_table_postgresql
r_create_table_postgresql = "CREATE TABLE \"Table1\" ("+DEFAULT_ROWSEP + "\
\"Table1ID\" serial NOT NULL,"+DEFAULT_ROWSEP + "\
\"Table1Name\" VARCHAR(400) NULL,"+DEFAULT_ROWSEP + "\
\"Table1Changed\" TIMESTAMP DEFAULT (current_timestamp) NULL,"+DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Table2ID\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")"+DEFAULT_ROWSEP + "\
)"

global r_create_table_db2 
r_create_table_db2 = "CREATE TABLE \"Table1\" ("+DEFAULT_ROWSEP + "\
\"Table1ID\" INT GENERATED ALWAYS AS IDENTITY NOT NULL,"+DEFAULT_ROWSEP + "\
\"Table1Name\" VARCHAR(400) NULL,"+DEFAULT_ROWSEP + "\
\"Table1Changed\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,"+DEFAULT_ROWSEP + "\
CONSTRAINT \"PK_Table1_Table1ID\" PRIMARY KEY (\"Table1ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"FK_Table1_Table1ID_Table2_Table2ID\" FOREIGN KEY (\"Table1ID\") REFERENCES \"Table2\"(\"Table2ID\"),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"CK_Table1_Name\" CHECK ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')),"+DEFAULT_ROWSEP + "\
CONSTRAINT \"UQ_Table1_Name\" UNIQUE (\"Table1ID\")"+DEFAULT_ROWSEP + "\
)"
global r_create_table_sqlserver
r_create_table_sqlserver = "CREATE TABLE Table1 ("+DEFAULT_ROWSEP + "\
[Table1ID] int IDENTITY(1,1) NOT NULL,"+DEFAULT_ROWSEP + "\
[Table1Name] varchar(400) NULL,"+DEFAULT_ROWSEP + "\
[Table1Changed] DATETIME DEFAULT (GETDATE()) NULL,"+DEFAULT_ROWSEP + "\
CONSTRAINT [PK_Table1_Table1ID] PRIMARY KEY (Table1ID),"+DEFAULT_ROWSEP + "\
CONSTRAINT [FK_Table1_Table1ID_Table2_Table2ID] FOREIGN KEY (Table1ID) REFERENCES Table2(Table2ID),"+DEFAULT_ROWSEP + "\
CONSTRAINT [CK_Table1_Name] CHECK ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')),"+DEFAULT_ROWSEP + "\
CONSTRAINT [UQ_Table1_Name] UNIQUE (Table1ID)"+DEFAULT_ROWSEP + "\
)" 

global r_SELECT_SQL 
r_SELECT_SQL= "SELECT (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) AS Field1, (T2.CountryPrefix + '+' + T2.PhoneNumber) AS Field2 FROM testtable AS T1 JOIN testtable AS T2 ON ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) ORDER BY T1.Field1 desc, T2.Field1 asc"+ DEFAULT_ROWSEP +"LIMIT 1"
global r_SELECT_DB_DB2
r_SELECT_DB_DB2 = "SELECT (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" AS T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc" + DEFAULT_ROWSEP + "FETCH FIRST 1 ROWS ONLY " 
global r_SELECT_postgresql
r_SELECT_postgresql = "SELECT (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" AS T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc"+ DEFAULT_ROWSEP +"LIMIT 1"
global r_SELECT_oracle
r_SELECT_oracle = "SELECT (T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))) AS Field1, (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS Field2 FROM \"testtable\" T1 JOIN \"testtable\" AS T2 ON ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) AND (ROWNUM < 2) ORDER BY T1.\"Field1\" desc, T2.\"Field1\" asc"
global r_SELECT_SQL_Server 
r_SELECT_SQL_Server= "SELECT TOP 1 (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) AS Field1, (T2.CountryPrefix + '+' + T2.PhoneNumber) AS Field2 FROM testtable AS T1 JOIN testtable AS T2 ON ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) WHERE ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) ORDER BY T1.Field1 desc, T2.Field1 asc"

global r_UPDATE_my_sql
r_UPDATE_my_sql = "SET"+DEFAULT_ROWSEP+ "dest_column = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((col_1 = '1') AND (col_2 = '1'))"
global r_UPDATE_DB2
r_UPDATE_DB2 = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"
global r_UPDATE_postgresql
r_UPDATE_postgresql = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"
global r_UPDATE_oracle
r_UPDATE_oracle = "SET" + DEFAULT_ROWSEP + "\"dest_column\" = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((\"col_1\" = '1') AND (\"col_2\" = '1'))"
global r_UPDATE_SQL_Server
r_UPDATE_SQL_Server = "SET" + DEFAULT_ROWSEP + "dest_column = 'Hello'" + DEFAULT_ROWSEP + "WHERE ((col_1 = '1') AND (col_2 = '1'))"

# Utils

def my_diff(a,b):
    result= '---------- String A-----------' + DEFAULT_ROWSEP
    result+= a + DEFAULT_ROWSEP
    result+= '---------- String B-----------' + DEFAULT_ROWSEP
    result+= b + DEFAULT_ROWSEP
 
    result+= '---------- Diff between A and B-----------' + DEFAULT_ROWSEP
    for line in difflib.context_diff(a,b):
        result+= line 
        
    return result
    

# Generate test objects.
def gen_simple_condition_1():
    _cond = Parameter_Condition(_operator = '>', _and_or = 'AND')
    _cond.left.append(Parameter_Numeric(1.3, '+'))
    _cond.right.append(Parameter_Numeric(2.4, '+'))

    return _cond

def gen_simple_condition_2():
    _cond = Parameter_Condition(_operator = 'LIKE', _and_or = 'AND')
    _cond.left.append(Parameter_Identifier('firstname', 'C', 'T1'))
    _cond.right.append(Parameter_String('%icklas', '+'))

    return _cond

def gen_simple_conditions():
    

    _cond = Parameter_Conditions()

    _cond.append(gen_simple_condition_1())
    _cond.append(gen_simple_condition_2())
    return _cond
    

def gen_complex_conditions():

    _cond = Parameter_Conditions()

    _cond.append(gen_simple_condition_1())
    _cond.append(gen_simple_condition_2())
    _cond.append(gen_simple_conditions())
    return _cond

def gen_simple_function():
    param = Parameter_Function(_name = 'Simple', _operator = 'C')
    param.parameters.append(gen_simple_cast())
    param.parameters.append(gen_simple_expression_2())
    return param
def gen_simpleexpression_1():
    param = Parameter_Expression(_operator = '+')
    param.expressionitems.append(Parameter_Identifier('CountryPrefix', 'C', 'T1'))
    param.expressionitems.append(Parameter_String('+', 'C'))
    param.expressionitems.append(Parameter_Identifier('PhoneNumber', 'C', 'T1'))
    param.expressionitems.append(gen_simple_function())
    return param

def gen_simple_expression_2():
    param = Parameter_Expression(_operator = '+')
    param.expressionitems.append(Parameter_Identifier('CountryPrefix', 'C', 'T2'))
    param.expressionitems.append(Parameter_String('+', 'C'))
    param.expressionitems.append(Parameter_Identifier('PhoneNumber', 'C', 'T2'))
    return param


def gen_simple_cast():
    
    param = Parameter_Cast(None, 'varchar(200)', 'C')
    param.expression.append(gen_simple_expression_2())
    return param


def gen_complex_expression():
    
    exp = Parameter_Expression()
    
    param = Parameter_Expression()
    param.expressionitems.append(exp)
    param.expressionitems.append(Parameter_String('+', 'C', '\\'))
    param.expressionitems.append(Parameter_Identifier('PhoneNumber', 'C', 'T1'))
    return param

def gen_complex_function():
    param = Parameter_Function(_name = 'Test', _operator = '+')
    param.parameters.append(gen_simpleexpression_1())
    param.parameters.append(gen_complex_expression())
    return param
    

def gen_simple_case():
    param = Parameter_CASE()
    when1 = Parameter_WHEN(gen_simple_conditions(), gen_simpleexpression_1())
    param.when_statements.append(when1)
    when2 = Parameter_WHEN(gen_simple_conditions(), gen_simple_expression_2())
    param.when_statements.append(when2)
    param.else_statement = gen_simple_expression_2()
    return param


def gen_simple_field_1():
    
    Parameter = Parameter_Field(None, _alias= 'Field1')
    Parameter.expression.append(gen_simpleexpression_1())
    return Parameter
def gen_simple_field_2():
    Parameter = Parameter_Field(None, _alias= 'Field2')
    Parameter.expression.append(gen_simple_expression_2())
    return Parameter 
def gen_simple_source_1():
    
    #_condlist = SQL_List("Parameter_Condition")
    _condlist = gen_simple_conditions()
    source = Parameter_Source(None, _condlist, 'T1')
    source.expression.append(Parameter_Identifier('testtable', 'C')) 
    return source

def gen_simple_source_2():
    #_condlist = SQL_List("Parameter_Condition")
    _condlist = gen_simple_conditions()
    source = Parameter_Source(None, _condlist, 'T2')
    source.expression.append(Parameter_Identifier('testtable', 'C')) 
    return source

def gen_simple_select(): 
    select = Verb_SELECT(_operator = 'C')
    select.top_limit = 1
    select.fields.append(gen_simple_field_1())
    select.fields.append(gen_simple_field_2())
    select.sources.append(gen_simple_source_1())
    select.sources.append(gen_simple_source_2())
    po1 = Parameter_ORDER_BY_item(_direction = "desc")
    po1.expressionitems.append(Parameter_Identifier("Field1", _prefix = "T1"))
    select.order_by.append(po1)
    po2 = Parameter_ORDER_BY_item(_direction = "asc")
    po2.expressionitems.append(Parameter_Identifier("Field1", _prefix = "T2"))
    select.order_by.append(po2)
    return select

def gen_simple_insert():
    insert = Verb_INSERT()
    insert.destination_identifier = Parameter_Identifier("test")
    insert.data = gen_simple_select()
    insert.column_identifiers.append(Parameter_Identifier("Table1ID"))
    insert.column_identifiers.append(Parameter_Identifier("Table1Name"))
    insert.column_identifiers.append(Parameter_Identifier("Table1Changed"))
    return insert

def gen_simple_update():
 
    _table_identifier = Parameter_Identifier("test")
    
    _a_1 = Parameter_Condition(Parameter_Identifier("col_1"), Parameter_String("1"), "=")
    _a_2 = Parameter_Condition(Parameter_Identifier("col_2"), Parameter_String("1"), "=", "AND")
    _conditions = Parameter_Conditions()
    _conditions.append(_a_1)
    _conditions.append(_a_2)
    
    
    _assignments = []
    _assignments.append(Parameter_Assignment(_left = Parameter_Identifier("dest_column"), _right = Parameter_String("Hello")))

    _update = Verb_UPDATE(_table_identifier =Parameter_Identifier("test"),_assignments = _assignments, _conditions = _conditions)
    return _update

def gen_simple_create(): 
  
    col1_constraint1 = Parameter_Constraint('PK_Table1_Table1ID', "PRIMARY KEY", [Parameter_Identifier('Table1ID')])
    col1_constraint2 = Parameter_Constraint('FK_Table1_Table1ID_Table2_Table2ID', "FOREIGN KEY", [Parameter_Identifier('Table1ID'), Parameter_Identifier('Table2'), Parameter_Identifier('Table2ID')])
    col1_constraint3 = Parameter_Constraint('CK_Table1_Name', "CHECK", [Parameter_Identifier('Table1ID')])
    col1_constraint3.checkconditions = gen_simple_conditions()
    col1_constraint4 = Parameter_Constraint('UQ_Table1_Name', "UNIQUE", [Parameter_Identifier('Table1ID')])
    #col1_constraint5 = Parameter_Constraint('DF_Table1_name', C_DEFAULT, ['GETDATE()'])
    
    col1 = Parameter_ColumnDefinition('Table1ID', 'serial', True)
    col2 = Parameter_ColumnDefinition('Table1Name', 'VARCHAR(400)', False)
    col3 = Parameter_ColumnDefinition('Table1Changed', 'timestamp', False)  
    col3.default = '::currdatetime::'
    #col4 = Parameter_ColumnDefinition('Table1Date', 'datetime', False, 'NOW()')  

    result = Verb_CREATE_TABLE('Table1')
    result.columns.append(col1)
    result.columns.append(col2)
    result.columns.append(col3)
    result.constraints.append(col1_constraint1)
    result.constraints.append(col1_constraint2)
    result.constraints.append(col1_constraint3)
    result.constraints.append(col1_constraint4)
    # result.Constraints.append(col1_constraint5)
   
    return result

    
class parameter_test(unittest.TestCase):


    def test_00_parameter_condition_simple(self):
        self.maxDiff = None
        param = gen_simple_condition_1()
        paramclass = param.__class__.__name__
        testvalue = '(1.3 > 2.4)'
        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), testvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), testvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_01_parameter_condition_complex(self):
        self.maxDiff = None
        param = gen_complex_conditions()
        paramclass = param.__class__.__name__
        testvalue = "((1.3 > 2.4) AND (T1.firstname LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')))"
        testvalue_Oracle = "((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')))"
        testvalue_PostgreSQL = "((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')))"
        testvalue_DB2 = "((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas') AND ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')))"

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), testvalue_Oracle, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), testvalue_PostgreSQL, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), testvalue_DB2, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_02_parameter_expression_simple(self):
        self.maxDiff = None
        param = gen_simpleexpression_1()
        testvalue = "(T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber)))"
        pipetestvalue = "(T1.CountryPrefix || '+' || T1.PhoneNumber || Simple(CAST((T2.CountryPrefix || '+' || T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix || '+' || T2.PhoneNumber)))"
        postgrestestvalue = "(T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\")))"
        db2testvalue = "(T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\")))"
        oracletestvalue = "(T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\")))"
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), oracletestvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), postgrestestvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), db2testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_03_parameter_cast_simple(self):
        self.maxDiff = None
        param = gen_simple_cast()
        testvalue = "CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200))"
        db2testvalue = "CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200))"
        postgrestestvalue = "CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200))"
        oracletestvalue = "CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS varchar(200))"
        paramclass = param.__class__.__name__
        
        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), oracletestvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), postgrestestvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), db2testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')
        
    def test_04_parameter_function_simple(self):
        self.maxDiff = None
        param = gen_simple_function()
        testvalue = "Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))"
        db2testvalue = "Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))"
        postgrestestvalue = "Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))"
        oracletestvalue = "Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))"
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), oracletestvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), postgrestestvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), db2testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')


    def test_05_parameter_case(self):
        self.maxDiff = None
        param = gen_simple_case()
        testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T1.CountryPrefix + '+' + T1.PhoneNumber + Simple(CAST((T2.CountryPrefix + '+' + T2.PhoneNumber) AS varchar(200)), (T2.CountryPrefix + '+' + T2.PhoneNumber))) WHEN ((1.3 > 2.4) AND (T1.firstname LIKE '%icklas')) THEN (T2.CountryPrefix + '+' + T2.PhoneNumber) else_statement (T2.CountryPrefix + '+' + T2.PhoneNumber) END"
        PostgreSQLtestvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) THEN (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" ILIKE '%icklas')) THEN (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") END"
        DB2testvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T1.\"CountryPrefix\" || '+' || T1.\"PhoneNumber\" || Simple(CAST((T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" || '+' || T2.\"PhoneNumber\") END"
        Oracletestvalue = "CASE WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T1.\"CountryPrefix\" + '+' + T1.\"PhoneNumber\" + Simple(CAST((T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") AS varchar(200)), (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\"))) WHEN ((1.3 > 2.4) AND (T1.\"firstname\" LIKE '%icklas')) THEN (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") else_statement (T2.\"CountryPrefix\" + '+' + T2.\"PhoneNumber\") END"
        paramclass = param.__class__.__name__

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), Oracletestvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
      
        self.assertEqual(param.as_sql(DB_POSTGRESQL), PostgreSQLtestvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), DB2testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_06_VERB_SELECT(self):
        self.maxDiff = None
        param = gen_simple_select()
        paramclass = param.__class__.__name__
 
        self.assertEqual(param.as_sql(DB_MYSQL), r_SELECT_SQL, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), r_SELECT_oracle, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), r_SELECT_postgresql, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), r_SELECT_DB_DB2, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), r_SELECT_SQL_Server, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_07_parameter_create_table(self):
        self.maxDiff = None
        param = gen_simple_create()
        paramclass = param.__class__.__name__
        self.assertEqual(param.as_sql(DB_MYSQL), r_create_table_mysql, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), r_create_table_oracle, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), r_create_table_postgresql, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), r_create_table_db2, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), r_create_table_sqlserver, paramclass +'.as_sql(DB_SQLSERVER) failed.')

    def test_08_VERB_INSERT(self):
        self.maxDiff = None
        param = gen_simple_insert()

        testvalue = "INSERT INTO test (Table1ID, Table1Name, Table1Changed)"
        quotedtestvalue = "INSERT INTO \"test\" (\"Table1ID\", \"Table1Name\", \"Table1Changed\")"
        paramclass = param.__class__.__name__#        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue + DEFAULT_ROWSEP + r_SELECT_SQL, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), quotedtestvalue + DEFAULT_ROWSEP + r_SELECT_oracle, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), quotedtestvalue + DEFAULT_ROWSEP + r_SELECT_postgresql, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), quotedtestvalue + DEFAULT_ROWSEP + r_SELECT_DB_DB2, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue + DEFAULT_ROWSEP + r_SELECT_SQL_Server, paramclass +'.as_sql(DB_SQLSERVER) failed.')
        
    def test_09_VERB_UPDATE(self):
        self.maxDiff = None
        param = gen_simple_update()

        testvalue = "UPDATE test"
        quotedtestvalue = 'UPDATE "test"'
        paramclass = param.__class__.__name__#        self.assertEqual(param.as_sql(DB_MYSQL), testvalue, paramclass +'.as_sql(DB_MYSQL) failed.')

        self.assertEqual(param.as_sql(DB_MYSQL), testvalue + DEFAULT_ROWSEP + r_UPDATE_my_sql, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), quotedtestvalue + DEFAULT_ROWSEP + r_UPDATE_oracle, paramclass +'.as_sql(DB_ORACLE) failed.')
        
        self.assertEqual(param.as_sql(DB_POSTGRESQL), quotedtestvalue + DEFAULT_ROWSEP + r_UPDATE_postgresql, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), quotedtestvalue + DEFAULT_ROWSEP + r_UPDATE_DB2, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), testvalue + DEFAULT_ROWSEP + r_UPDATE_SQL_Server, paramclass +'.as_sql(DB_SQLSERVER) failed.')
        


    def test_10_parameter_create_index(self):
        self.maxDiff = None
        param = Verb_CREATE_INDEX('ind_Table1ID', "CLUSTERED", 'Table1', ['Table1Name', 'Table1Date'])
        paramclass = param.__class__.__name__
        mysqltestvalue      = 'CREATE CLUSTERED INDEX `ind_Table1ID`' + param._row_separator + 'ON Table1(`Table1Name`, `Table1Date`)'
        oracletestvalue     = 'CREATE CLUSTERED INDEX "ind_Table1ID"' + param._row_separator + 'ON Table1("Table1Name", "Table1Date")'
        postgrestestvalue   = 'CREATE CLUSTERED INDEX "ind_Table1ID"' + param._row_separator + 'ON Table1("Table1Name", "Table1Date")'
        db2testvalue        = 'CREATE INDEX "ind_Table1ID"' + param._row_separator + 'ON Table1("Table1Name", "Table1Date")' + param._row_separator + 'CLUSTER'
        sqlservertestvalue  = 'CREATE CLUSTERED INDEX [ind_Table1ID]' + param._row_separator + 'ON Table1([Table1Name], [Table1Date])'
        self.assertEqual(param.as_sql(DB_MYSQL), mysqltestvalue, paramclass +'.as_sql(DB_MYSQL) failed.')
        self.assertEqual(param.as_sql(DB_ORACLE), oracletestvalue, paramclass +'.as_sql(DB_ORACLE) failed.')
        self.assertEqual(param.as_sql(DB_POSTGRESQL), postgrestestvalue, paramclass +'.as_sql(DB_POSTGRESQL) failed.')
        self.assertEqual(param.as_sql(DB_DB2), db2testvalue, paramclass +'.as_sql(DB_DB2) failed.')
        self.assertEqual(param.as_sql(DB_SQLSERVER), sqlservertestvalue, paramclass +'.as_sql(DB_SQLSERVER) failed.')
       

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()