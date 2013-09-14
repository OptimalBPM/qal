"""
Created on Sep 12, 2013

@author: Nicklas Boerjesson
"""
from datetime import time
from postgresql.types.io.stdlib_datetime import seconds_in_day

""" 
This module contains all the base classes for the SQL-related code.
The reason is simply to make things more manageable and split things up a bit.
"""

from qal.sql.sql_types import DEFAULT_ROWSEP


class Parameter_Base(object): 
    """This class is a base class for all parameter classes."""
    _row_separator = DEFAULT_ROWSEP 
    _parent = None

    def __init__(self, _row_separator = None):
        super(Parameter_Base, self ).__init__()
        if _row_separator != None: 
            self._row_separator = _row_separator
            
    def as_sql(self, _db_type): 
        """Generate SQL for specified database engine"""
        raise Exception(self.__class__.__name__ + ".as_sql() is not implemented")
 

class Parameter_Remotable(object): 
    """This class is a auxilliary class for all parameter classes that is remotable. 
    That is, they can fetch their data from, or perform their actions at, a different location than the parent class.
    If they return data, the data will be held in the temporary table, where it can be joined with or otherwise managed.
    """
    """The temporary table name is used by owners to reference the data correctly."""
    temporary_table_name = None
    resource_uuid = None
    
            
    def prepare(self, _temporary_table_name_prefix = "t_"):
        """The prepare function checks whether the resource GUID is set. 
        If so, it fetches the data and puts it into a dataset
        The temporary table name is automatically generated based on the temporary table_name prefix.
        Its default is "t_". """
        
        print("_______________________DDDDD_______________prepare: sdfsdfsdfds" + self.resource_uuid)
        
        """Make connection to resource defined the resource_uuid"""
        pass
        """Run query/load to get the result set"""
        pass
        """Generate temporary table name(cannot be more than 8 characters due to some database backend limitations)"""
        #TODO: See if this should be smarter
        self.temporary_table_name = _temporary_table_name_prefix + str(time.second) + str(time.microsecond)[0:2] 
        
        pass
        """Create temporary table using column names and types from result set"""
        pass
        """Insert data into temporary table""" 
        pass

 

 
class SQL_List(list):
    """This is the base class for lists of class instances."""    

    def __init__(self, _itemclasses = None):
        super(SQL_List, self ).__init__()
        if _itemclasses != None:
            self._itemclasses = _itemclasses

    def as_sql(self, _db_type): 
        """Generate SQL for specified database engine"""
        result = '' 
        for _item in self:
#            TODO: Check if the below list functionality is ever needed/wanted.             
#            result+= DEFAULT_ROWSEP + '--****************' + str(self.itemclasses) + ' number ' + index +'****************************' + DEFAULT_ROWSEP 
            if hasattr(_item, 'as_sql'):
                result+= _item.as_sql(_db_type)
            else:
                result+= _item

        return result  
    
    
   
class Parameter_Expression_Item(Parameter_Base):
    """The superclass of all classes that can be considered part of an expression"""
    operator = 'C'  
    
    def __init__(self,_operator = None):
        super(Parameter_Expression_Item, self ).__init__()
        if _operator != None:
            self.operator = _operator
        else:
            self.operator = 'C' 
    

