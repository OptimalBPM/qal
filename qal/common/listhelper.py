"""
    Functions for handling lists.
    
    :copyright: Copyright 2010-2013 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

def CI_index(_List, _value):
    """Case-insensitively finds an item in a list"""
    if _value != None:
        for index in range(len(_List)):
            if _List[index].lower() == _value.lower():
                return index
    
    return -1;

def unenumerate(value, _Type):
    """Returns the value of a specific type""" 
    return value[_Type]   

def find_next_match(_list, _start_idx, _match):
    """Finds next _match from _start_idx"""
    for _curr_idx in range(_start_idx, len(_list)):
        if _list[_curr_idx] == _match:
            return _curr_idx
    return -1

def find_previous_match(_list, _start_idx, _match):
    """Finds previous _match from _start_idx"""
    for _curr_idx in range(_start_idx, 0, -1):
        if _list[_curr_idx] == _match:
            return _curr_idx
    return -1
