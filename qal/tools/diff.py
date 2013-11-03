"""
Created on Sep 1, 2013

@author: Nicklas Boerjesson
@note:  The functions are just stubs, so far. This module will contain code for doing what 
GNU diffutils does for files, but with datasets.

"""



def compare(self, _left, _right, _key_columns, _full):
    """ The compare function takes two structurally identical 2-dimensional matrices,
        _left and _right, matches them using the columns in _key_colums,
        and returns a tuple of the results.
        
        The first two results are _missing_left, and _missing_right, 
        which indicates if any rows are missing completely in either dataset.
        
        If the _full parameter is True, also the values in the rows are compared, 
        and the third result, _different is populated with a list of rows
        where the values differ.
    """    
    _missing_left = _missing_right = _difference = []
    
    # Order _left and _right using key columns
    
    
    # From top, loop data sets, compare all rows
    
        # Keys differ, compare _right with next row from _left to see who is missing
        
            # Keys for _left[+1] = _right, add row to _missing_left
            
            # Keys for _left[+1] != _right, add row to _missing_right
            
        # Keys are the same and _full is set, check all data 
            
            # Differing columns found, add _row to _different
    
    
    return _missing_left, _missing_right, _difference


def diff_to_text(_missing_left, _missing_right, _different):
    """Creates a textual representation of the differences"""
    _diff_text = ""
        
    return _diff_text


