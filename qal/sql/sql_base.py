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
from qal.common.resources import resource_types
from qal.dal.dal_types import db_type_to_string
from qal.dal.dal import Database_Abstraction_Layer



class Parameter_Base(object): 
    """This class is a base class for all parameter classes."""
    _row_separator = DEFAULT_ROWSEP 
    _parent = None

    def __init__(self, _row_separator = None):
        super(Parameter_Base, self ).__init__()
        if _row_separator != None: 
            self._row_separator = _row_separator
            
    def _generate_sql(self, _db_type): 
        """Generate SQL for specified database engine"""
        raise Exception(self.__class__.__name__ + "._generate_sql() is not implemented")
    
    def as_sql(self, _db_type):
        
        if hasattr(self,'resource_uuid') and self.resource_uuid and self._check_need_prepare():
            return self._bring_into_context(_db_type)
        else:
            return self._generate_sql(_db_type)
        
        
 

class Parameter_Remotable(object): 
    """This class is a auxilliary class for all parameter classes that is remotable. 
    That is, they can fetch their data from, or perform their actions at, a different location than the parent class.
    If they return data, the data will be held in the temporary table, where it can be joined with or otherwise managed.
    """
    """The temporary table name is used by owners to reference the data correctly."""
    temporary_table_name = None
    resource_uuid = None
    
            
    def _bring_into_context(self, _db_type):
        """The _bring_into_context function checks whether the resource GUID is set and if resources need fetching
        If so, it fetches the data and puts it into a dataset, which is then inserted into the parents context.
        """
        
        print(self.__class__.__name__ + "._bring_into_context: Resource_uuid: " + str(self.resource_uuid))        

        
        print(self.__class__.__name__ + "._bring_into_context: Needs preparing, preparing for resource_uuid: " + str(self.resource_uuid))
        
        if not self._resource:
            raise Exception(self.__class__.__name__ + "._bring_into_context: _resource not cached")
        
        """Make connection to resource defined by the resource_uuid"""
        if self._resource.type == 'rdbms':
            _source_dal = Database_Abstraction_Layer(_resource = self._resource)
            _data = _source_dal.execute(self._generate_sql(_db_type))
        elif self._resource.type in ["CUSTOM", "FLATFILE", "MATRIX", "XML"]:
            # Use all NoSQL-datasets

            _dataset = None
            if _dataset:
                _data = _dataset.load()
        
                  
        else:
            raise Exception(self.__class__.__name__ + "._bring_into_context - Error: Invalid resource type : " + str(self._resource.type))    
        
        # This is imported locally, to not interfere with the structure.
        from qal.sql.sql_macros import copy_to_temp_table 
        _table_name = copy_to_temp_table(_dal = self._dal_ , _values =_data, _field_names = _source_dal.field_names, _field_types = _source_dal.field_types, _table_name = None)

        
        
        
        print(self.__class__.__name__ + "._bring_into_context: Using "+ self._resource.caption +", resource_uuid: " + str(self.resource_uuid))

        """Generate temporary table name(cannot be more than 8 characters due to some database backend limitations)"""

        pass
        """Create temporary table using column names and types from result set"""
        pass
        """Insert data into temporary table""" 
        pass
        return None

    def _check_need_prepare(self):
        """
        Checks context, whether a _bring_into_context is needed. It is needed if:
        1. If the nearest parent with resource has a different ID
        2. If no parent has a resourceID
        
        It is NOT needed if the nearest parent with resource has the same ID.
        """
        _curr_parent = self._parent
        while _curr_parent:
            print(self.__class__.__name__ + "._check_need_prepare: level up" + str(_curr_parent))
            if hasattr(_curr_parent, 'resource_uuid') and _curr_parent.resource_uuid:
                if self.resource_uuid == _curr_parent.resource_uuid:
                    return False
                else:
                    print(self.__class__.__name__ + "._check_need_prepare: Different resource uuid found:" + str(_curr_parent.resource_uuid) + " (own: " +str(self.resource_uuid)+ ")")
                    return True
            _curr_parent = _curr_parent._parent

        return True
                
        
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
    

