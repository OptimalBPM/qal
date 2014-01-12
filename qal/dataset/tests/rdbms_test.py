"""
Created on Dec 17, 2013

@author: Nicklas Boerjesson
"""

import unittest

from qal.dataset.rdbms import RDBMS_Dataset
from qal.dataset.custom import DATASET_LOGLEVEL_DETAIL
from qal.common.resources import Resources
from qal.dal.dal import Database_Abstraction_Layer
from lxml import etree



import os
Test_Script_Dir = os.path.dirname(__file__)
Test_Resource_Dir = Test_Script_Dir + '/resources'



def load_xml(_filename):
    return etree.parse(_filename)

class Test(unittest.TestCase):


    def test_1_Load_Save(self, _has_header = None, _resource = None):
        _resources_node = load_xml(Test_Resource_Dir + "/resources.xml").find("resources")
        _resources = Resources(_resources_node = _resources_node)
        
        _d_pgsql = RDBMS_Dataset(_resource= _resources.get_resource("{1D62083E-88F7-4442-920D-0B6CC59BA2FF}"))
        _d_pgsql.load()
        print("source:\n" + str(_d_pgsql.data_table))

        print("Staging destination")
        _staging_dal = Database_Abstraction_Layer(_resource= _resources.get_resource("{DD34A233-47A6-4C16-A26F-195711B49B97}"))
        _staging_dal.execute("DROP TABLE table_mysql;")
        _staging_dal.execute("CREATE TABLE table_mysql (table_mysqlID integer, table_mysqlName varchar(200), table_mysqlChanged timestamp);")
        _staging_dal.execute("INSERT INTO table_mysql (table_mysqlID, table_mysqlName, table_mysqlChanged) VALUES (1, 'MySQL', '2001-01-01');")
        _staging_dal.execute("INSERT INTO table_mysql (table_mysqlID, table_mysqlName, table_mysqlChanged) VALUES (2, 'MySQL', '2001-01-02');")
        _staging_dal.execute("INSERT INTO table_mysql (table_mysqlID, table_mysqlName, table_mysqlChanged) VALUES (3, 'MySQL', '2001-01-04');")
        
        _staging_dal.commit()
        _staging_dal.close()
        
        _d_mysql = RDBMS_Dataset(_resource= _resources.get_resource("{DD34A233-47A6-4C16-A26F-195711B49B97}"))
        _d_mysql._log_level = DATASET_LOGLEVEL_DETAIL
        _d_mysql.load()

        print("dest:\n" + str(_d_mysql.data_table))
                
        _d_mysql.apply_new_data(_d_pgsql.data_table, [2])
        
        _d_mysql.save()
        _d_mysql.load()
        
        print(str(_d_mysql._log))
        
        _d_pgsql.dal.close()
        _d_mysql.dal.close()

        #self.assertEqual(_a, _b, "test_1_Load_Save: Files are not equal")
        
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()