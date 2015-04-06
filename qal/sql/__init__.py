"""
    *********************************************
    qal.sql is an abstraction of the SQL language
    ********************************************* 
    * Abstracts SQL to enable a single object structure to platform-independently generate SQL for each platform.
    * Can translate itself from and to an XML structure for portability and persistence.
    * Can implicitly join any qal.dataset from all kinds of sources directly on a target server(loads them into temp
    tables and joins then).
    * Currently available qal.dataset:s : flat file, matrix, XPath(XML/HTML/XHTML) and spreadsheet(Excel-formats/.odt).      
    * RBDMS Platforms (qal.dal.types) : MySQL, PostgreSQL, Oracle, DB2 and SQL server
    
    :copyright: Copyright 2010-2014 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""