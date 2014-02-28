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

def pretty_list(_array):
    """Returns proper array data representation syntax, but with each row on a new text row, to make it more readable and usable."""
    
    def _handle_types(_data):
        if isinstance(_data, list):
            return str(_data)
        else:
            if isinstance(_data, str):
                return "['" + _data + "']"    
            else:
                return "[" + str(_data) + "]"       
    
    _result = ""
    if _array is None:
        return False
    elif len(_array)==1:
        return "[\n" + _handle_types(_array[0]) + "\n]"
    elif len(_array) > 0:

        for _row_idx in range(len(_array) - 1) :
            _result+= _handle_types(_array[_row_idx]) + ",\n"

        _result+= _handle_types(_array[_row_idx + 1]) + "\n"
        return "[\n" + _result + "]"
    else:
        return "[]"
    
    
        
