"""
Created on Oct 3, 2010

@author: Nicklas Boerjesson
@note: This module contains functionality to retrieve metadata about databases and their objects.
@todo: These SQL:s could be moved into an XML file(VerbCustom).
The gain is yet somewhat marginal. They should not move in to the database due to versioning.  
"""
from builtins import classmethod

_table_info_postgresql = "SELECT a.attname AS COLUMN_NAME, t.typname AS DATA_TYPE,\
       CASE WHEN a.attlen > -1 THEN a.attlen  ELSE a.atttypmod END - 4 AS DATA_LENGTH, NOT a.attnotnull AS NULLABLE \
FROM pg_class c, pg_attribute a, pg_type t \
WHERE c.relname = ':TABLENAME'\
  AND a.attnum > 0\
  AND a.attrelid = c.oid\
  AND a.atttypid = t.oid \
ORDER BY a.attnum;"
  
_table_info_mysql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE AS NULLABLE, CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH \
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=':DATABASENAME' AND TABLE_NAME = ':TABLENAME'"

_table_info_oracle = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH FROM user_tab_columns \
WHERE table_name=':TABLENAME'"

_table_info_db2 = "SELECT NAME AS COLUMN_NAME, COLTYPE AS DATA_TYPE, NULLS AS NULLABLE, LENGTH AS DATA_LENGTH \
FROM SYSIBM.SYSCOLUMNS WHERE UPPER(TBNAME) = UPPER(':TABLENAME');"

_table_info_sqlserver = "SELECT \
clmns.name AS [COLUMN_NAME],\
usrt.name AS [DATA_TYPE],\
clmns.is_nullable AS [NULLABLE],\
usrt.max_length AS [DATA_LENGTH]\
FROM \
sys.tables AS tbl \
INNER JOIN sys.all_columns AS clmns ON clmns.object_id=tbl.object_id \
LEFT OUTER JOIN sys.types AS usrt ON usrt.user_type_id = clmns.user_type_id \
WHERE \
(tbl.name=':TABLENAME') \
ORDER BY clmns.column_id ASC"

_table_list_mysql_by_database_name =  _table_list_oracle_by_schema = table_list_sqlserver = ""


_table_list_postgresql_by_database_name = "SELECT  table_name \
FROM information_schema.tables \
WHERE table_type = 'BASE TABLE' \
    AND table_schema = 'public' \
    AND table_catalog = ':DATABASE' \
ORDER BY table_type, table_name "

_table_list_mysql_by_database_name = "SHOW TABLES IN :DATABASE"
_table_list_oracle_by_database_name = "select table_name from dba_tables;"
_table_list_db2_by_database_name = "SELECT NAME FROM SYSIBM.SYSTABLES WHERE Type = 'T';"
_table_list_mssql_by_database_name = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"

_table_list_db2_by_schema = "select NAME from sysibm.systables \
where CREATOR  = ':USER' \
and type = 'T';"

_table_list_oracle_by_schema = "select table_name from user_tables WHERE TABLESPACE_NAME = ':USER' AND table_name NOT LIKE 'DEF$_%'"



def _table_info_sql(_db_type, _table_name, _database_name):
    """Lists information about a table"""
    SQL = [_table_info_mysql, _table_info_postgresql, _table_info_oracle, _table_info_db2, _table_info_sqlserver][_db_type]
    SQL = SQL.replace(":TABLENAME", _table_name)
    SQL = SQL.replace(":DATABASENAME", _database_name)
    return SQL

def _table_list_sql_by_schema(_db_type, _user):
    """List tables in a schema"""
    if not(_db_type in [2,3]):
        raise Exception("table_list_sql_by_schema: Only DB2 and Oracle supported currently!") 
    
    SQL = [_table_list_mysql_by_database_name, _table_list_postgresql_by_database_name, _table_list_oracle_by_schema, _table_list_db2_by_schema, table_list_sqlserver][_db_type]
    SQL = SQL.replace(":USER", _user)
    return SQL

def _table_list_sql_by_database_name(_db_type, _database):
    """List tables in a specified database"""
    if not(_db_type in [0,1,3,4]):
        raise Exception("table_list_sql_by_database_name: Only MySQL, Postgres, DB2 and SQL Server is supported currently!")
    
    SQL = [_table_list_mysql_by_database_name, _table_list_postgresql_by_database_name, _table_list_oracle_by_database_name, _table_list_db2_by_database_name, _table_list_mssql_by_database_name][_db_type]
    SQL = SQL.replace(":DATABASE", _database)
    return SQL


class Meta_Queries(object):
    """The meta queries class collects methods for gathering meta data about a database."""

    @staticmethod
    def table_info(_dal, _table_name):
        """
        List column in the specified table
        :param _table_name: The name of the table to list columns for
        :return:
        """

        rows = _dal.query(_table_info_sql(_dal.db_type, _table_name, _dal.db_databasename))
        columns = list()
        for row in rows:
            columns.append(row[0])
        return columns   
     
    @staticmethod
    def table_list_by_schema(_dal, _schema_name):
        """List tables in the specified schema"""
        rows = _dal.query(_table_list_sql_by_schema(_dal.db_type, _schema_name))
        columns = list()
        for row in rows:
            columns.append(row[0])
        return columns    

    @staticmethod
    def table_list_by_database_name(_dal, _database_name):
        """List tables in the specified database"""
        """TODO: Refactor so either MySQL gets connection's name or all else goes by database name(which won't work)"""
        rows = _dal.query(_table_list_sql_by_database_name(_dal.db_type, _database_name))
        print("SQL:\n"+ _table_list_sql_by_database_name(_dal.db_type, _database_name) + "\n_database_name:" +
              _database_name + "\nrows: \n" + str(rows))
        columns = list()
        for row in rows:
            columns.append(row[0])
        return columns

    @staticmethod
    def oracle_all_sequences(_dal):
        """Oracle specific: List all sequences"""
        rows = _dal.query("SELECT SEQUENCE_NAME FROM USER_SEQUENCES")
        sequences = list()
        for row in rows:
            sequences.append(row[0])
        return sequences    
        