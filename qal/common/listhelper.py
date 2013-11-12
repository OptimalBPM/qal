'''
Created on Oct 10, 2010

@author: Nicklas Boerjesson
@note: Helper functions for list types
'''

def CI_index(_List, _value):
    """Finds an item in a list"""
    if _value != None:
        for index in range(len(_List)):
            if _List[index].lower() == _value.lower():
                return index
    
    return -1;

def unenumerate(value, _Type):
    """Returns the value of a specific type""" 
    return value[_Type]   
