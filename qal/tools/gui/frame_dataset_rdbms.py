"""
Created on Oct 20, 2013

@author: Nicklas Boerjesson
"""
from qal.dataset.rdbms import RDBMSDataset


__author__ = 'Nicklas Boerjesson'

from tkinter import Button, messagebox, SUNKEN, ttk, StringVar, IntVar, BooleanVar
from tkinter.constants import LEFT, W
from qal.tools.gui.frame_dataset_custom import FrameCustomDataset
from qal.tools.gui.widgets_misc import BPMFrame, make_entry

from qal.dal.types import DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER
from qal.dal.dal import DatabaseAbstractionLayer
from qal.sql.types import data_types
from qal.common.discover import discover_database_servers
from qal.sql.meta_queries import MetaQueries

from lxml import etree

from qal.common.strings import empty_when_none


discovered_servers = []


def add_xml_subitem(_parent, _nodename, _nodetext):
    _curr_item = etree.SubElement(_parent, _nodename)
    _curr_item.text = str(_nodetext)
    return _curr_item


class FrameRDBMSDataset(FrameCustomDataset):
    """
        Holds an instance of, and visually represents, a RDBMS dataset.
        See qal.dataset.rdbms.RDBMSDataset
    """

    search_address = None
    subnet_ip = None
    possible_references = None

    def __init__(self, _master, _dataset=None, _relief=None, _is_destination=None):
        super(FrameRDBMSDataset, self).__init__(_master, _dataset, _relief, _is_destination)
        search_address = None
        subnet_ip = None
        possible_references = None

        if _dataset is None:
            self.dataset = RDBMSDataset()
            self.dataset._dal = DatabaseAbstractionLayer()

    def read_from_dataset(self):
        super(FrameRDBMSDataset, self).read_from_dataset()

        self.cb_type.current(empty_when_none(self.dataset._dal.db_type))
        self.server.set(empty_when_none(self.dataset._dal.server))
        self.database.set(empty_when_none(self.dataset._dal.databasename))
        self.username.set(empty_when_none(self.dataset._dal.username))
        self.password.set(empty_when_none(self.dataset._dal.password))
        self.port.set(empty_when_none(self.dataset._dal.port))
        self.instance.set(empty_when_none(self.dataset._dal.instance))
        if self.dataset._dal.autocommit is None:
            self.autocommit.set(True)
        else:
            self.autocommit.set(empty_when_none(self.dataset._dal.autocommit))
        self.table_name.set(empty_when_none(self.dataset.table_name))

    def write_to_dataset(self):
        super(FrameRDBMSDataset, self).write_to_dataset()

        self.dataset._dal.db_type = self.cb_type.current()
        self.dataset._dal.server = self.server.get()
        self.dataset._dal.databasename = self.database.get()
        self.dataset._dal.username = self.username.get()
        self.dataset._dal.password = self.password.get()
        self.dataset._dal.autocommit = self.autocommit.get()
        self.dataset._dal.instance = self.instance.get()
        self.dataset.table_name = self.table_name.get()

        try:
            self.dataset._dal.port = self.port.get()
        except:
            pass


    def check_reload(self):
        if self.dataset._dal is None or self.dataset._dal.connected is False:
            return "You need to connect to a database"
        if self.dataset.table_name is None:
            return "You need to choose a table"
        else:
            return False


    def nmap_string_to_db_type(self, _value):
        """
        Nmap searches return a string for each database platform, convert these into a qal database type enumeration.
        :return: and integer representing the brand of database from qal,dal.types
        """
        if _value[0:9] == "Microsoft":
            return DB_SQLSERVER
        if _value[0:10] == "PostgreSQL":
            return DB_POSTGRESQL
        if _value[0:5] == "MySQL":
            return DB_MYSQL
        if _value[0:6] == "Oracle":
            return DB_ORACLE
        if _value[0:7] == "IBM DB2":
            return DB_DB2

    def on_select_auto_server(self, *args):
        """When a database server is selected from the Nmap results, used its settings."""
        if self.cb_auto_server.current() != None:
            self.cb_type.current(self.nmap_string_to_db_type(discovered_servers[self.cb_auto_server.current()][2]))
            self.server.set(discovered_servers[self.cb_auto_server.current()][0])




    def set_widget_visibility(self, _widget, _visible):
        """Sets whether a grid is visible or not"""
        if _visible:
            _widget.grid()
        else:
            _widget.grid_remove()

    def set_visibility(self, _dbtype):
        """Sets visibility of all widgets"""
        self.set_widget_visibility(self.l_server,
                                   (_dbtype in [DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER]))
        self.set_widget_visibility(self.cb_auto_server,
                                   (_dbtype in [DB_MYSQL, DB_POSTGRESQL, DB_ORACLE, DB_DB2, DB_SQLSERVER]))
        self.set_widget_visibility(self.l_port, (_dbtype in [DB_ORACLE, DB_DB2, DB_SQLSERVER]))
        self.set_widget_visibility(self.e_port, (_dbtype in [DB_ORACLE, DB_DB2, DB_SQLSERVER]))
        self.set_widget_visibility(self.l_instance, (_dbtype in [DB_ORACLE, DB_DB2, DB_SQLSERVER]))
        self.set_widget_visibility(self.e_instance, (_dbtype in [DB_ORACLE, DB_DB2, DB_SQLSERVER]))

    def on_type_change(self, *args):
        """Event handler for when database type is changed"""
        self.set_visibility(self.cb_type.current())

    def on_cb_address_dropdown(self, *args):
        """Event handler for server list dropdown"""
        global discovered_servers
        if len(discovered_servers) == 0:
            self.notify_task('Scanning network for RDBMS...', -1)
            try:
                discovered_servers = discover_database_servers(self.subnet_ip.get())
                self.notify_task('Done.', 0)
            except Exception as e:
                self.notify_task('Nmap scan error: ' + str(e), 0)
                raise e

        _tmp_server_list = []
        for _curr_server in discovered_servers:
            if _curr_server[1] == '':
                _tmp_server_list.append(_curr_server[0] + "" + " (" + _curr_server[2] + ")")
            else:
                _tmp_server_list.append(_curr_server[1] + "" + " (" + _curr_server[2] + ")")
        self.cb_auto_server['values'] = _tmp_server_list

    def on_cb_table_dropdown(self):
        """When the user clicks the drop down, populate it"""
        if self.dataset._dal is not None:
            self.cb_table_names['values'] = self.get_table_list(self.dataset._dal)
        else:
            self.cb_table_names['values'] = []


    def on_cb_database_dropdown(self, *args):
        """Event handler for database list dropdown (not implemented)"""
        print("Reload databases not implemented yet")

    def do_on_table_change(self, *args):
        """This function is called when the user changes the selected table, reloads field names(references)"""
        self.dataset.table_name = self.table_name.get()
        self.dataset.field_names = []
        try:
            self.reload()
        except:
            pass

        print("self.dataset.field_names after reload:" + str(self.dataset.field_names))
        if self.on_columns_change:
            self.on_columns_change()

    def Connect_to_database(self, *args):
        """Creates a connection using credentials from the GUI"""
        if self.dataset._dal.connected:
            print("Disconnect current connection(" + self.dataset._dal.db_server + ")")
            self.dataset._dal.close()

        self.write_to_dataset()
        try:
            self.dataset._dal.connect_to_db()
            self.on_cb_table_dropdown()
        except Exception as e:
            messagebox.showerror("Error connecting to database", str(e))
            raise


    def init_widgets(self):
        """Initialize all widgets"""

        self.l_auto_server = ttk.Label(self, text="Scan and select server: ")
        self.l_auto_server.grid(column=0, row=0, sticky=W)

        self.auto_server = StringVar()
        self.cb_auto_server = ttk.Combobox(self, textvariable=self.auto_server, state='normal', validate="none",
                                           postcommand=self.on_cb_address_dropdown)
        self.cb_auto_server.grid(column=1, row=0, sticky=W)
        self.auto_server.trace('w', self.on_select_auto_server)

        self.server, self.e_server, self.l_server = make_entry(self, "Server(IP/host name): ", 1)
        self.server.set('localhost')

        self.l_type = ttk.Label(self, text="Type: ", anchor=W)
        self.l_type.grid(column=0, row=2, sticky=W)

        self.type = StringVar()
        self.cb_type = ttk.Combobox(self, textvariable=self.type, state='readonly')
        self.cb_type['values'] = ('MY SQL', 'PostgreSQL', 'Oracle', 'IBM DB2', 'SQL Server')
        self.cb_type.current(0)
        self.cb_type.grid(column=1, row=2, sticky=W)

        self.l_port = ttk.Label(self, text="Port: ")
        self.l_port.grid(column=0, row=3, sticky=W)

        self.port = IntVar()
        self.e_port = ttk.Entry(self, textvariable=self.port)
        self.e_port.grid(column=1, row=3, sticky=W)

        self.instance, self.e_instance, self.l_instance = make_entry(self, "Instance: ", 4)


        # Credentials
        self.username, self.e_username, self.l_username = make_entry(self, "User name: ", 5)
        self.username.set("")
        # Password
        self.password, self.e_password, self.l_password = make_entry(self, "Password: ", 6)

        # Database name
        self.l_database = ttk.Label(self, text="Database: ")
        self.l_database.grid(column=0, row=7, sticky=W)

        self.database = StringVar()
        self.cb_database = ttk.Combobox(self, textvariable=self.database, state='normal',
                                        postcommand=self.on_cb_database_dropdown, text="")
        self.cb_database.grid(column=1, row=7, sticky=W)

        self.autocommit = BooleanVar()
        self.autocommit.set(True)
        self.l_autocommit = ttk.Label(self, text="Autocommit: ")
        self.l_autocommit.grid(column=0, row=8, sticky=W)
        self.c_autocommit = ttk.Checkbutton(self, variable=self.autocommit)
        self.c_autocommit.grid(column=1, row=8, sticky=W)

        self.btn_connect = Button(self, text="Connect", command=self.Connect_to_database)
        self.btn_connect.grid(column=1, row=9, sticky=W)

        self.table_name = StringVar()
        self.cb_table_names = ttk.Combobox(self, textvariable=self.table_name, state='normal',
                                           postcommand=self.on_cb_table_dropdown, text="")
        self.cb_table_names.grid(column=1, row=10, sticky=W)
        self.table_name.trace('w', self.do_on_table_change)

        # Add handling when type is changed
        self.type.trace('w', self.on_type_change)

        self.set_visibility(0)

    def get_table_list(self, _dal):
        """List all tables for the current database"""
        if _dal:
            if not _dal.connected:
                _dal.connect_to_db()
            self.notify_task("Loading tables in " + _dal.databasename + ".", 10)
            _result = MetaQueries.table_list_by_database_name(_dal, _dal.databasename)
            self.notify_task("Loaded tables in " + _dal.databasename + ".", 100)
            return _result
        else:
            self.notify_task("Could not load tables.", 0)
            return []

    def get_column_list(self, _dal, _table_name):
        """List all available columns"""
        if _dal:
            if not _dal.connected:
                _dal.connect_to_db()
            self.notify_task("Loading columns in " + _table_name + ".", 10)
            _result = MetaQueries.table_info(_dal,_table_name)
            self.notify_task("Loaded columns in " + _table_name + ".", 100)
            return _result
        else:
            self.notify_task("Could not load columns in " + _table_name, 0)
            return []

    def reload(self):
        if self.dataset.table_name is not None:
            self.notify_task("Load table from " + self.dataset.table_name, 10)
            self.dataset.load()
            self.notify_task("Loaded table " + self.dataset.table_name, 100)
        else:
            self.notify_task("Cannot reload, please select table.", 0)


    def get_possible_references(self, _force=None):
        if (not self.possible_references) or (len(self.possible_references) == 0) or _force == True:
            self.possible_references = self.get_column_list(self.dataset._dal, self.table_name.get())
        return self.possible_references

