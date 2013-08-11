"""
The Database Abstraction Layer(DAL) package.
--------------------------------------------

DAL is designed to hide the differences between the different database backends that it supports.

Features:

1. Abstraction of connectivity

dal.py
------
The dal-module provides an API that strives to follow and eventually fulfill the DBAPI2.0 specification.
It does this by wrapping and simplifying use of the needed libraries.

Currently, it supports:

    Postgresql through py-postgresql
    MySQL through PyMySQL
    Microsoft SQL Server through pyodbc
    IBM DB/2 through pyodbc
    Oracle through pyodbc 
    
2. Abstraction of the SQL language.

sql.py
------

The sql-module contains an object structure that mimics a subset of the structure of the SQL language.
This object structure can be translated into an backend-specific SQL statement, 
which means that it can generate SQL that is adapted to the SQL-backend of the server.

3. XML schema, XML parsing and generation.

sql_xml.py
----------

The sql_xml module can generate an XML Schema (SQL.xsd) directly from the object structure of the sql module.
Following that schema, it can export or import that structure as XML files. 
    
This means that queries (the structure) can be saved and loaded files and databases.
Using its xml_file_to_sql function, they can even be called as functions with parameters. 

"""