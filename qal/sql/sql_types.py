'''
Created on Sep 20, 2010

@author: Nicklas Boerjesson
@note: This module defines the basic types used in SQL. There are also some helper functions.
'''


"""Constants"""

DEFAULT_ROWSEP = chr(13)

from csv import list_dialects

"""
 Global types:
 All type declarations have a enumeration array that must be kept up to date!
"""
def constraint_types():
    """Returns a list of the supported constraint types"""
    return ['NOT NULL', 'UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY', 'CHECK', 'DEFAULT']
    

def index_types():
    """Returns a list of the supported index types"""
    return ['UNIQUE', 'CLUSTERED', 'NONCLUSTERED'];


def data_types():
    """Returns a list of the supported data types"""
    # @note: string(3000) is added to give some leeway for DB2s default table page size of 4000. 
    return ['integer', 'string', 'string(255)', 'string(3000)', 'float', 'serial', 'timestamp', 'boolean']

def data_source_types():
    """Returns a list of the supported data source types"""
    return ['Flatfile_Dataset', 'XML_Dataset', 'RDBMS_Dataset', 'Matrix_Dataset']

def boolean():
    """Returns a list of the supported boolean values"""
    return ['true', 'false']

def and_or():
    """Returns a list of the supported logical operators"""
    return ['AND', 'OR']

def set_operator():
    """Returns a list of the supported set operators"""
    return ['UNION', 'INTERSECT', 'DIFFERENCE']

def join_types():
    """Returns a list of the supported join types"""
    return ['INNER','LEFT OUTER', 'RIGHT OUTER', 'FULL OUTER', 'CROSS']

def expression_item_types():
    """Returns a list of the supported expression types""" 
    return ['Verb_SELECT','Parameter_Expression',
                'Parameter_String','Parameter_Numeric',
                'Parameter_Identifier','Parameter_Cast',
                'Parameter_Function', 'Parameter_IN', 'Parameter_NoSQL', 'Parameter_CASE', 'Parameter_Set']
    
def tabular_expression_item_types(): 
    """Returns a list of the supported tabular expression types""" 
    return ['Verb_SELECT','Parameter_NoSQL', 'Parameter_Set']

def in_types():
    """Returns a list of what is supported in a IN-statement""" 
    return ['Verb_SELECT', 'Parameter_String'] 

        
    
def condition_part(): 
    """Returns a list of the supported condition parts""" 
    return ['Parameter_Conditions','Parameter_Condition', 'Parameter_Expression'] + expression_item_types() 


def sql_property_to_type(_property_name):
    """Translates a property name to a type, like decimal or string.
    Property names in the SQL.py class structure are chosen to not collide."""
    
    _property_name = _property_name.lower()
    
    # Basic types
    
    if _property_name in ['name','default','tablename','alias','default','operator',\
                        'identifier','escape_character','string_value','operator','sql_mysql',\
                        'sql_postgresql','sql_oracle','sql_db2','sql_sqlserver','row_separator',\
                        'prefix', 'direction', 'operator','table', 'parameters', 'delimiter',\
                        'filename', 'target_table', 'resource_uuid', 'temporary_table_name',\
                        'temporary_table_name_prefix']:
        return ['string']
    
    elif _property_name == 'numeric_value':
        return ['decimal']

    elif _property_name in ['notnull', 'has_header']:
        return ['boolean']    
    
    elif _property_name == 'top_limit':
        return ['integer']    
    
    # Simple types
    
    elif _property_name == 'datatype':
        return ['datatypes', data_types()]
    
    elif _property_name == 'set_operator':
        return ['set_operator', set_operator()]
    
    elif _property_name == 'join_type':
        return ['join_type', join_types()]

    elif _property_name == 'data':
        return ['tabular_expression_item', tabular_expression_item_types()]
    
    elif _property_name == 'subsets':
        return ['Array_tabular_expression_item', tabular_expression_item_types()]    

    elif _property_name == 'and_or':
        return ['and_or', and_or()]
    
    elif _property_name == 'constraint_type':
        return ['constraint_types', constraint_types()]
    
    elif _property_name == 'index_type':
        return ['index_types', index_types()]
    
    elif _property_name in ['expression','parameters','result','else_statement','orderby','expressionitems']:
        return ['Array_expression_item', expression_item_types()]
    
    elif _property_name in ['in_values']:
        return ['in_types', in_types()]   
    
    elif _property_name in ['data_source']:
        return ['data_source_types', data_source_types()]   

    elif _property_name in ['left','right']:
        return ['condition_part', condition_part()]
    
    # Complex types
    elif _property_name in ['checkconditions','conditions']:
        return ['Array_Parameter_Condition']

    elif _property_name in ['columnnames']:
        return ['Array_string']
        
    elif _property_name == 'columns':
        return ['Array_Parameter_ColumnDefinition']
    
    elif _property_name == 'destination_identifier':
        return ['Parameter_Identifier']

    elif _property_name in ['column_identifiers','references']:
        return ['Array_Parameter_Identifier']
        
    elif _property_name == 'constraints':
        return ['Array_Parameter_Constraint']
    
     
    elif _property_name == 'fields':
        return ['Array_Parameter_Field']  

    elif _property_name == 'select':
        return ['Verb_SELECT']
     
    elif _property_name == 'sources':
        return ['Array_Parameter_Source']  
    
    elif _property_name == 'csv_dialect':
        return ['file_types', list_dialects()] 

    
    elif _property_name == 'when_statements':
        return ['Array_Parameter_WHEN']  
    elif _property_name == 'order_by':
        return ['Array_Parameter_ORDER_BY_item']  
    else:
        raise Exception("sql_property_to_type: Unrecognized property:" + _property_name)
    
def verbs(): 
    """Returns a list of the supported verb types""" 
    return ['Verb_CREATE_TABLE','Verb_CREATE_INDEX', 'Verb_SELECT','Verb_Custom', 'Verb_INSERT']  
   
