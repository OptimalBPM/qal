"""
Created on Sep 1, 2013

@author: Nicklas Boerjesson
@note:  The functions are just stubs, so far. This module will contain code for doing what 
GNU diffutils does for files, but with datasets.

"""

def cmp_key_columns(_left, _right, _key_columns):
    for _curr_key_column in _key_columns:
        if _left[_curr_key_column] < _right[_curr_key_column]:
            return -1
        elif _left[_curr_key_column] > _right[_curr_key_column]:
            return 1

    return 0

def match_all_columns(_left, _right):
    for _curr_column in range(1, len(_right)):
        if _left[_curr_column] != _right[_curr_column]:
            return False

    return True   

def compare(_left, _right, _key_columns, _full):
    """ The compare function takes two structurally identical 2-dimensional matrices,
        _left and _right, matches them using the columns in _key_colums,
        and returns a tuple of the results.
        
        The first two results are _missing_left, and _missing_right, 
        which indicates if any rows are missing completely in either dataset.
        
        If the _full parameter is True, also the values in the rows are compared, 
        and the third result, _different is populated with a list of rows
        where the values differ.
    """    
    _missing_left = []
    _missing_right = []
    _difference = []

    # Order _left and _right using key columns
    if len(_key_columns) == 1:   
        _left_s = sorted(_left, key=lambda d: ( d[_key_columns[0]]))
        _right_s = sorted(_right, key=lambda d: ( d[_key_columns[0]]))
    elif len(_key_columns) == 2:   
        _left_s = sorted(_left, key=lambda d: ( d[_key_columns[0]], d[_key_columns[1]]))
        _right_s = sorted(_right, key=lambda d: ( d[_key_columns[0]], d[_key_columns[1]]))
    elif len(_key_columns) == 3:   
        _left_s = sorted(_left, key=lambda d: ( d[_key_columns[0]], d[_key_columns[1]], d[_key_columns[2]]))
        _right_s = sorted(_right, key=lambda d: ( d[_key_columns[0]], d[_key_columns[1]], d[_key_columns[2]]))
    else:
        raise Exception("Err..sorry, only 3 key columns are supported currently, too tired to make it dynamic. :-)")
        
    # From top, loop data sets, compare all rows
    _left_idx = _right_idx = 0
    _left_len = len(_left_s)
    _right_len = len(_right_s)
    while _left_idx < _left_len and _right_idx < _right_len:
        #print("_left_idx :" + str(_left_idx) + " value: "+str(_left_s[_left_idx][_key_columns[0]]) + " | _right_idx : "  + str(_right_idx)+ " value: "+str(_right_s[_right_idx][_key_columns[0]]))
        _cmp_res = cmp_key_columns(_left_s[_left_idx], _right_s[_right_idx], _key_columns)
        #print("_cmp_res :" + str(_cmp_res))
        if _cmp_res < 0:
            #print("_missing_right.append " + str(_left_s[_left_idx]))
            _missing_right.append(_left_s[_left_idx])
            _left_idx+= 1
        elif _cmp_res > 0:
            #print("_missing_left.append " + str(_right_s[_right_idx]))
            _missing_left.append(_right_s[_right_idx])            
            _right_idx+= 1
        else:
            # Keys are the same and _full is set, check all data 

            if _full == True:
                if match_all_columns(_left_s[_left_idx], _right_s[_right_idx]) != True:
                    # Differing columns found, add _row to _different
                    _difference.append([_left_s[_left_idx], _right_s[_right_idx], _left_idx, _right_idx])
            _left_idx+= 1
            _right_idx+= 1
            
    # Add remainders to missing
    if _left_idx < _left_len:
        # print("_missing_right.append (post) " + str(_left_s[_left_idx: _left_len]))
        _missing_right+= _left_s[_left_idx: _left_len]
    if _right_idx < _right_len:
        # print("_missing_left.append (post)" + str(_right_s[_right_idx: _right_len]))
        _missing_left+=_right_s[_right_idx: _right_len]
                
            
            
            
            
    
    
    return _missing_left, _missing_right, _difference


def diff_to_text(_missing_left, _missing_right, _different):
    """Creates a textual representation of the differences"""
    _diff_text = ""
        
    return _diff_text


