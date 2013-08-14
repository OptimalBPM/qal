'''
Created on May 8, 2010

@author: Nicklas Boerjesson
'''

from qal.dal.dal_types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER, string_to_db_type

class Database_Abstraction_Layer(object):
    '''
    This class is a base class for all access classes.
    '''
    
    # Events
    
    on_connect = None
    
    # Properties
    
    db_connection = None
    settings = None
    db_type = None
    db_server = ''
    db_databasename = ''
    db_username = ''
    db_password = ''
    db_instance = ''
    db_driver = None
    db_autocommit = True
    def read_db_settings(self):
        self.db_type        = string_to_db_type(self.settings.Parser.get("database", "type"))
        self.db_server      = self.settings.Parser.get("database", "server")   
        self.db_databasename= self.settings.Parser.get("database", "database_name")   
        self.db_username    = self.settings.Parser.get("database", "username")
        self.db_password    = self.settings.Parser.get("database", "password")
        self.DB_Port        = self.settings.Parser.get("database", "port")
        self.autocommit     = self.settings.get("database", "autocommit", True)        
        if self.settings.Parser.has_option("database", "instance"):
            self.db_instance    = self.settings.Parser.get("database", "instance")
    def connect_to_db(self):
        
        if (self.db_type == DB_MYSQL):
            import pymysql
            Conn = pymysql.connect (host = self.db_server,
                            db = self.db_databasename,
                            user = self.db_username,
                            passwd = self.db_password,
                            )
            

        elif (self.db_type == DB_POSTGRESQL):
            import postgresql.driver.dbapi20  
            Conn = postgresql.driver.dbapi20.connect (host = self.db_server, 
                                                database =  self.db_databasename, 
                                                user = self.db_username, 
                                                password = self.db_password)
                            
        elif (self.db_type == DB_SQLSERVER):
            import pyodbc
            #TODO: Investigate if there is any more adapting needed, platform.release() can also be used. 
            import platform
            if platform.system().lower() == 'linux':
                connstr = "DRIVER=FreeTDS;SERVER=" + self.db_server + ";DATABASE=" + self.db_databasename +";TDS VERSION=8.0;UID=" + self.db_username + ";PWD=" + self.db_password + ";PORT="+self.DB_Port + ";Trusted_Connection=no"
            elif platform.system().lower() == 'windows':
                connstr = "Driver={SQL Server};Server=" + self.db_server + ";DATABASE=" + self.db_databasename +";UID=" + self.db_username + ";PWD=" + self.db_password + ";PORT="+self.DB_Port + ";Trusted_Connection=no"
            else:
                raise Exception("connect_to_db: ODBC connections on " + platform.system() + " not supported yet.")

            Conn = pyodbc.connect(connstr, autocommit=self.autocommit);      

        elif (self.db_type == DB_DB2):
            import pyodbc
            import platform
            if platform.system().lower() == 'linux':
                drivername = "DB2"
            elif platform.system().lower() == 'windows':
                drivername = "{IBM DATA SERVER DRIVER for ODBC - C:/PROGRA~1/IBM}"
            else:
                raise Exception("connect_to_db: DB2 connections on " + platform.system() + " not supported yet.")
            
            # DSN-less?{IBM DB2 ODBC DRIVER} ?? http://www.webmasterworld.com/forum88/4434.htm
            connstr =  "Driver=" + drivername + ";Database=" + self.db_databasename +";hostname=" + self.db_server + ";port="+self.DB_Port + ";protocol=TCPIP; uid=" + self.db_username + "; pwd=" + self.db_password
            #connstr = "DSN=" + self.db_server + ";UID=" + self.db_username + ";PWD=" + self.db_password 
            Conn = pyodbc.connect(connstr, autocommit=self.autocommit)
        
        # cx_Oracle in python 3.X not checked yet.
        elif (self.db_type == DB_ORACLE):
            import cx_Oracle
            connstr = self.db_username + '/' +  self.db_password + '@' + self.db_server + ':' + self.DB_Port + '/' + self.db_instance
            print(connstr)
            Conn = cx_Oracle.connect(connstr) 
            Conn.autocommit=self.autocommit
                  
        else:
            raise Exception("connect_to_db: Invalid database type.")              
      
        
        self.db_connection = Conn   
        if self.on_connect:
            self.on_connect()  
        return Conn 
    
    
    
    def init_db(self):
        self.read_db_settings()
        self.db_driver = self.connect_to_db()   
    
    def __init__(self, _settings):
        '''
        Init
          
        '''  
        if _settings != None:      
            self.settings = _settings
            self.init_db()


    def select(self, params):
        pass
    
    def execute(self, _SQL):
        if self.db_type == DB_POSTGRESQL:
            self.db_connection.execute(_SQL)
        else:
            cur = self.db_connection.cursor()
            cur.execute(_SQL)

    def query(self, _SQL):
        cur = self.db_connection.cursor()
        cur.execute(_SQL)
        return cur.fetchall()
        
   
    def close(self):
        self.db_connection.close()
            
    def commit(self):
        self.db_connection.commit()

    def rollback(self):
        self.db_connection.rollback()
    
    def connected(self):
        return self.db_connection.connected
