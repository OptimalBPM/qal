'''
Created on Jan 8, 2012

@author: Nicklas Boerjesson
'''

from qal.dal.dal_types import DB_DB2,DB_ORACLE, DB_POSTGRESQL
from qal.sql.sql_utils import db_specific_object_reference
from datetime import date, datetime
from .custom import Parameter_Custom_Dataset

class Parameter_Matrix_Dataset(Parameter_Custom_Dataset):
 
    '''
    classdocs
    '''
    
    _dal = None
    
    def __init__(self):
        '''
        Constructor
        '''
        super(Parameter_Matrix_Dataset, self ).__init__()
        
        
        
    def load(self):
        pass

    def as_sql(self, _db_type):
        # TODO: Review this code, as it is almost copied from the custom class. 
        #TODO: Add check here and in custom so that it is impossible to not fill all field_names when used.
        _result = []
        _add_field_names = (len(self.data_table) > 0) and (len(self.field_names) == len(self.data_table[0]))
        for _row in self.data_table:
            _curr_row = []
            for _col_idx in range(len(_row)):
                _str_col = ''
                _col = _row[_col_idx]
                if (_col == None):
                    _str_col = 'NULL'                    
                elif (isinstance(_col, bool)):
                    if (_col == True):
                        if (_db_type == DB_POSTGRESQL):
                            _str_col = 'TRUE'
                        else:
                            _str_col = '\'1\''
                    elif (_col == False):
                        if (_db_type == DB_POSTGRESQL):
                            _str_col = 'FALSE'
                        else:
                            _str_col = '\'0\''
                elif ((isinstance(_col, int)) or isinstance(_col, float)):
                    _str_col = str(_col)
                elif ((isinstance(_col, date)) or isinstance(_col, date)):
                    _str_col = "'" + _col.isoformat(' ') + "'"
                else:
                    _str_col = "'" + str(_col) + "'"
            
                if (_add_field_names):                    
                    _curr_row.append(_str_col + ' AS ' + db_specific_object_reference(self.field_names[_col_idx], _db_type))
                else:
                    _curr_row.append(_str_col)
                    
            _add_field_names = False
            
            if (_db_type == DB_DB2):
                _result.append("SELECT " + ",".join(_curr_row) +' FROM sysibm.sysdummy1')
            elif (_db_type == DB_ORACLE):
                _result.append("SELECT " + ",".join(_curr_row) +' FROM DUAL')
            else:
                _result.append("SELECT " + ",".join(_curr_row))
            
        return str("\nUNION\n".join(_result))

        