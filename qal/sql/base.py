"""
Created on Sep 12, 2013

@author: Nicklas Boerjesson
"""


""" 
This module contains all the base classes for the SQL-related code.
The reason is simply to make things more manageable and split things up a bit.
"""

from qal.sql.types import DEFAULT_ROWSEP




class Parameter_Base(object): 
    """This class is a base class for all parameter classes."""
    _row_separator = DEFAULT_ROWSEP
    """The default row separator for SQL generation."""
    _parent = None
    """The parent of the object"""
    _base_path = None
    """The base path of the XML file from which it was loaded, used for relative paths."""

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
    

