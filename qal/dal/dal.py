"""
    ************************************
    qal.dal - Database Abstraction Layer
    ************************************
    
    The goal with DAL is to hide the connection-related differences between the most common database backends.
    
    :copyright: Copyright 2010-2013 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

from qal.dal.types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER, string_to_db_type, db_type_to_string
from qal.dal.conversions import parse_description, python_type_to_SQL_type
from source.qal.tools.discover import import_error_to_help


class DatabaseAbstractionLayer(object):
    """This class abstracts the different peculiarities of the different database backends with
    regards to connection details"""
    
    # Events
    
    on_connect = None
    
    # Properties
    
    connected = False
    
    db_connection = None

    db_type = None
    db_server = ''
    db_databasename = ''
    db_username = ''
    db_password = ''
    db_instance = ''
    db_driver = None
    db_autocommit = True
    db_port = None
    
    field_names = None
    field_types = None
    
    # Postgres transaction object
    _pg_xact = None
    
    def read_ini_settings(self, _ini_parser):
        """Read setting from the settings.Parser object"""
        if _ini_parser.Parser.has_option("database", "type"):
            self.db_type        = string_to_db_type(_ini_parser.Parser.get("database", "type"))
            self.db_server      = _ini_parser.Parser.get("database", "server")   
            self.db_databasename= _ini_parser.Parser.get("database", "database_name")   
            self.db_username    = _ini_parser.Parser.get("database", "username")
            self.db_password    = _ini_parser.Parser.get("database", "password")
            self.db_port        = _ini_parser.Parser.get("database", "port")
            self.db_autocommit     = _ini_parser.get("database", "autocommit", True)
            if _ini_parser.Parser.has_option("database", "instance"):
                self.db_instance    = _ini_parser.Parser.get("database", "instance")
        else:
            print("read_ini_settings: Settings not valid, not raising error.")
            
            
    def read_resource_settings(self, _resource):
        if _resource.type.upper() != 'RDBMS':
            raise Exception("DAL.read_resource_settings error: Wrong resource type - " + _resource.type)
        self.db_type =         string_to_db_type(_resource.data.get("db_type"))
        self.db_server =       _resource.data.get("db_server")
        self.db_databasename = _resource.data.get("db_databasename")
        self.db_instance =     _resource.data.get("db_instance")
        self.db_username =     _resource.data.get("db_username")
        self.db_password =     _resource.data.get("db_password")
        self.db_port =         _resource.data.get("db_port")
        self.db_autocommit =      _resource.data.get("db_autocommit")

    def write_resource_settings(self, _resource):
        _resource.type = 'RDBMS'
        _resource.data.clear()
        _resource.data["db_type"] = db_type_to_string(self.db_type)
        _resource.data["db_server"] = self.db_server
        _resource.data["db_databasename"] = self.db_databasename
        _resource.data["db_instance"] = self.db_instance
        _resource.data["db_username"] = self.db_username
        _resource.data["db_password"] = self.db_password
        _resource.data["db_port"] = self.db_port
        _resource.data["db_autocommit"] = self.db_autocommit

                       
    def connect_to_db(self):
        '''Connects to the database'''
        if self.db_type == DB_MYSQL:
            try:
                import pymysql
            except ImportError as _err:
                # TODO: Add python3-mysql when available or consider mysql-connector when available for python3
                raise Exception(import_error_to_help(_module="pymysql", _err_obj=_err, _pip_package="pymysql3",
                                                     _apt_package=None, _win_package=None))

            _connection = pymysql.connect (host = self.db_server,
                            db = self.db_databasename,
                            user = self.db_username,
                            passwd = self.db_password,
                            )
            

        elif self.db_type == DB_POSTGRESQL:

            try:
                import postgresql.driver as pg_driver
            except ImportError as _err:
                raise Exception(import_error_to_help(_module="postgresql.driver", _err_obj=_err,
                                                     _pip_package="py-postgresql",
                                                     _apt_package="python3-postgresql",
                                                     _win_package=None,
                                                     _import_comment="2014-04-16: If using apt-get, " +
                                                                     "check so version is > 1.0.3-2" +
                                                     " as there is a severe bug in the 1.02 version. " +
                                                     "See https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=724597"))

            if self.db_port == None or self.db_port == "" or self.db_port == 0:
                _port = 5432
            else:
                _port = self.db_port
            _connection = pg_driver.connect(host = self.db_server,
                                                database =  self.db_databasename, 
                                                user = self.db_username, 
                                                password = self.db_password,
                                                port = _port)
                            
        elif self.db_type in [DB_SQLSERVER, DB_DB2]:
            _connection_string = None
            try:
                import pyodbc
            except ImportError as _err:
                raise Exception(import_error_to_help(_module="pyodbc", _err_obj=_err,
                                                     _pip_package="pyodbc",
                                                     _apt_package="None",
                                                     _win_package=None,
                                                     _import_comment="2014-04-16: " +
                                                                     "No apt package (python3-pyodbc)"+
                                                                     " available at this time."))
            import platform

            #TODO: Investigate if there is any more adapting needed, platform.release() can also be used.

            if self.db_type == DB_SQLSERVER:
                if platform.system().lower() == 'linux':
                    _connection_string = "DRIVER=FreeTDS;SERVER=" + self.db_server + ";DATABASE=" + \
                                         self.db_databasename +";TDS VERSION=8.0;UID=" + self.db_username + ";PWD=" + \
                                         self.db_password + ";PORT="+str(self.db_port) + ";Trusted_Connection=no"
                elif platform.system().lower() == 'win32':
                    _connection_string = "Driver={SQL Server};Server=" + self.db_server + ";DATABASE=" + \
                                         self.db_databasename +";UID=" + self.db_username + ";PWD=" + self.db_password +\
                                         ";PORT="+str(self.db_port) + ";Trusted_Connection=no"
                else:
                    raise Exception("connect_to_db: ODBC connections on " + platform.system() + " not supported yet.")


            elif self.db_type == DB_DB2:

                if platform.system().lower() == 'linux':
                    drivername = "DB2"
                elif platform.system().lower() == 'win32':
                    drivername = "{IBM DATA SERVER DRIVER for ODBC - C:/PROGRA~1/IBM}"
                else:
                    raise Exception("connect_to_db: DB2 connections on " + platform.system() + " not supported yet.")

                # DSN-less?{IBM DB2 ODBC DRIVER} ?? http://www.webmasterworld.com/forum88/4434.htm
                _connection_string =  "Driver=" + drivername + ";Database=" + self.db_databasename +";hostname=" + \
                                      self.db_server + ";port="+str(self.db_port) + ";protocol=TCPIP; uid=" + \
                                      self.db_username + "; pwd=" + self.db_password
                #_connection_string = "DSN=" + self.db_server + ";UID=" + self.db_username + ";PWD=" + self.db_password
                _connection = pyodbc.connect(_connection_string, autocommit=self.db_autocommit)

                # DSN-less?{IBM DB2 ODBC DRIVER} ?? http://www.webmasterworld.com/forum88/4434.htm
                _connection_string =  "Driver=" + drivername + ";Database=" + self.db_databasename +";hostname=" + \
                                      self.db_server + ";port="+str(self.db_port) + ";protocol=TCPIP; uid=" + \
                                      self.db_username + "; pwd=" + self.db_password
                #_connection_string = "DSN=" + self.db_server + ";UID=" + self.db_username + ";PWD=" + self.db_password

            print("Connect to database using connection string:  " + _connection_string)
            _connection = pyodbc.connect(_connection_string, autocommit=self.db_autocommit)
        
        # cx_Oracle in python 3.X not checked yet.
        elif self.db_type == DB_ORACLE:
            try:
                import cx_Oracle
            except ImportError as _err:
                raise Exception(import_error_to_help(_module="cx_Oracle", _err_obj=_err,
                                                     _pip_package="cx_Oracle",
                                                     _apt_package="None",
                                                     _win_package="Download and install binary .msi package from " +
                                                                  "http://cx-oracle.sourceforge.net/ and install.",
                                                     _import_comment="2014-04-16: No python3-pyodbc available at" +
                                                                     " build time."))

            _connection_string = self.db_username + '/' +  self.db_password + '@' + self.db_server + ':' + \
                                 str(self.db_port) + '/' + self.db_instance
            print("Connect to database using connection string:  " + _connection_string)
            _connection = cx_Oracle.connect(_connection_string)
            _connection.autocommit=self.db_autocommit
                  
        else:
            raise Exception("connect_to_db: Invalid database type.")              
      
        
        self.db_connection = _connection
        
        if self.on_connect:
            self.on_connect() 
        self.connected = True
            
        return _connection
    
    
    
    def __init__(self, _settings = None, _resource = None):
        '''
        Init
          
        '''  
        if _settings != None:      
            self.settings = _settings
            self.read_ini_settings(_settings)

        if _resource != None:
            self.resource = _resource
            self.read_resource_settings(_resource)



    def select(self, params):
        pass
    
    def execute(self, _sql):
        """Execute the SQL statement, expect no dataset"""
        
        
        if self.db_type == DB_POSTGRESQL:
            self.db_connection.execute(_sql)
        else:
            cur = self.db_connection.cursor()
            cur.execute(_sql)
            
    def _make_positioned_params(self, _input):

            _arg_idx = 1
            _output =  _input
            
            # Simplest possible scan, prepared to handle the types differently.                        
            for _curr_idx in range(0, len(_input)):
                _chunk = _input[_curr_idx:_curr_idx +2]
                
                if _chunk == "%s":
                    _output = _output.replace(_chunk,"$" + str(_arg_idx), 1)           
                    _arg_idx+=1            
                elif _chunk == "%d":                    
                    _output = _output.replace(_chunk,"$" + str(_arg_idx), 1)           
                    _arg_idx+=1            

            return _output    
                
           
    def executemany(self, _sql, _values):
        """Execute the SQL statements , expect no dataset"""
        
       
        if self.db_type == DB_POSTGRESQL:
            # Change parameter type into Postgres positional ones, like  $1, $2 and so on.
            # TODO: Correctly handle other datatypes than string.
            _sql = self._make_positioned_params(_sql)

            _prepared = self.db_connection.prepare(_sql)
            print(_sql)
            for _row in _values:
                _prepared(*_row)
        else:
            cur = self.db_connection.cursor()
            cur.executemany(_sql, _values)
    

    def query(self, _sql):
        """Execute the SQL statement, get a dataset"""
        
        # py-postgres doesn't use the DB-API, as it doesn't work well-
        if self.db_type == DB_POSTGRESQL:
            _ps = self.db_connection.prepare(_sql)
            _res = _ps()
            if _ps.column_names != None:
                self.field_names = _ps.column_names 
                self.field_types = []                
                for _curr_type in _ps.column_types:
                    self.field_types.append(python_type_to_SQL_type(_curr_type))

        else:
            cur = self.db_connection.cursor()
            cur.execute(_sql)
            
            self.field_names, self.field_types = parse_description(cur.description, self.db_type);
            
            _res = cur.fetchall()
            
        # Untuple. TODO: This might need to be optimised, perhaps by working with the same array. 
        _results = [] 
        for _row in _res:
            _results.append(list(_row))
        
        return _results
   
    
   
    def close(self):
        """Close the database connection"""
        self.db_connection.close()
        
    def start(self):
        if self.db_type == DB_POSTGRESQL and self._pg_xact == None:
            self._pg_xact = self.db_connection.xact()
            self._pg_xact.start()
        else:
            self.execute("START TRANSACTION")
     
            
    def commit(self):
        """Commit the transaction"""
        if self.db_type == DB_POSTGRESQL:
            if self._pg_xact != None:
                self._pg_xact.commit()
                self._pg_xact = None
        else:
            self.db_connection.commit()

    def rollback(self):
        """Rollback the transaction"""
        if self.db_type == DB_POSTGRESQL:
            self._pg_xact.rollback()
            self._pg_xact = None
        self.db_connection.rollback()
    