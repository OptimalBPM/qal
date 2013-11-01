"""
Created on Sep 1, 2013

@author: Nicklas Boerjesson
@note:  The functions are just stubs, so far. This module will contain code for doing what 
GNU diffutils does for files, but with datasets.

"""

from qal.sql.sql import Verb_DELETE, Parameter_Source 


def compare(self, _left, _right, _key_columns, _full):
    """ The compare function takes two structurally identical n-dimensional matrices,
        _left and _right, matches them using the columns in _key_colums,
        and returns a tuple of the results.
        
        The first two results are _missing_left, and _missing_right, 
        which indicates if any rows are missing completely in either dataset.
        
        If the _full parameter is True, also the values in the rows are compared, 
        and the third result, _difference is populated with a list of rows
        where the values differ.
    """    
    _missing_left = _missing_right = _difference = []
    
    return _missing_left, _missing_right, _difference

def diff_to_text(_missing_left, _missing_right, _difference):
    """Creates a textual representation of the differences"""
    _diff_text = ""
        
    return _diff_text

def generate_deletes(_table_name, _id_columns, _delete_list):
    """Generates a Verb_DELETE instance populated with the indata"""
    
    """Create SELECTs and put them in a UNION:ed set"""
    
    """Put the set in an insert and add joins on the ID columns """
        
    _source = Parameter_Source("""_expression = None, _conditions = None, _alias = '', _join_type = None""")
    _deletes = Verb_DELETE("""_sources = None, _operator = None""")
    _deletes.sources.append(_source)
    return _deletes

def generate_inserts(_table_name, _id_columns, _delete_list):
    """Generates a Verb_INSERT instance populated with the indata"""
    pass

def generate_updates(_table_name, _id_columns, _delete_list):
    """Generates DELETE and INSERT instances populated with the indata
    @todo: Obviously a VERB_UPDATE will be better, implement that when test servers are back up."""     
    pass
