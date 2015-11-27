"""
Created on Sep 20, 2010

@author: Nicklas Boerjesson
@note: This module defines the basic types used in SQL. There are also some helper functions.
"""

"""Constants"""

DEFAULT_ROWSEP = chr(13)

from csv import list_dialects

from qal.dataset.xpath import xpath_data_formats

"""
 Global types:
 All type declarations have a enumeration array that must be kept up to date!
"""


def constraint_types():
    """Returns a list of the supported constraint types"""
    return ['NOT NULL', 'UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY', 'CHECK', 'DEFAULT']


def index_types():
    """Returns a list of the supported index types"""
    return ['UNIQUE', 'CLUSTERED', 'NONCLUSTERED']


def quoting_types():
    """Returns a list of the supported quoting modes"""
    return ['QUOTE_MINIMAL', 'QUOTE_ALL', 'QUOTE_NONE']


def data_types():
    """Returns a list of the supported data types"""
    # @note: string(3000) is added to give some leeway for DB2s default table page size of 4000. 
    # TODO: Investigate BLOB support
    return ['integer', 'string', 'string(255)', 'string(3000)', 'float', 'serial', 'timestamp', 'boolean']


def data_source_types():
    """Returns a list of the supported data source types"""
    return ['FlatfileDataset', 'XpathDataset', 'MatrixDataset', 'SpreadsheetDataset', 'RDBMSDataset']


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
    return ['', 'INNER', 'LEFT OUTER', 'RIGHT OUTER', 'FULL OUTER', 'CROSS']


def expression_item_types():
    """Returns a list of the supported expression types"""
    return ['VerbSelect', 'ParameterExpression',
            'ParameterString', 'ParameterNumeric',
            'ParameterIdentifier', 'ParameterCast',
            'ParameterFunction', 'ParameterIn', 'ParameterDataset', 'ParameterCase', 'ParameterSet']


def tabular_expression_item_types():
    """Returns a list of the supported tabular expression types"""
    return ['VerbSelect', 'ParameterDataset', 'ParameterSet']


def in_types():
    """Returns a list of what is supported in a IN-statement"""
    return ['VerbSelect', 'ParameterString']


def condition_part():
    """Returns a list of the supported condition parts"""
    return ['ParameterConditions', 'ParameterCondition'] + expression_item_types()

def make_json_type_ref(_json_ref, type_name):

    _result = {}


def sql_property_to_type(_property_name, _json_ref = None):
    """Translates a property name to a type, like decimal or string.
    Property names in the SQL.py class structure are chosen to not collide."""

    _property_name = _property_name.lower()

    def _handle_json_ref(_name, _types = None):
        if _types:
            if _json_ref is not None and _json_ref != "" :
                return [{"$ref": _json_ref + _name}, _types]
            else:
                return [_name, _types]
        else:
            if _json_ref is not None and _json_ref != "":
                return [{"$ref": _json_ref + _name}]
            else:
                return [_name]



    # TODO: Break out JSON class ref handling into function

    # Basic types

    if _property_name in ['name', 'default', 'tablename', 'alias', 'default', 'operator',
                          'identifier', 'escape_character', 'string_value', 'operator', 'sql_mysql',
                          'sql_postgresql', 'sql_oracle', 'sql_db2', 'sql_sqlserver', 'row_separator',
                          'prefix', 'direction', 'operator', 'table', 'delimiter',
                          'filename', 'target_table', 'resource_uuid', 'temporary_table_name',
                          'temporary_table_name_prefix', 'rows_xpath', "quotechar", "skipinitialspace", ]:
        return ['string']

    elif _property_name == 'numeric_value':
        if _json_ref is not None:
            return ['number']
        else:
            return ['decimal']

    elif _property_name in ['notnull', 'has_header']:
        return ['boolean']

    elif _property_name in ['top_limit']:
        return ['integer']

    elif _property_name == 'quoting':
        return _handle_json_ref('quoting', quoting_types())

    # Simple types

    elif _property_name == 'datatype':
        return _handle_json_ref('datatypes', data_types())

    elif _property_name == 'set_operator':
        return _handle_json_ref('set_operator', set_operator())

    elif _property_name == 'join_type':
        return _handle_json_ref('join_types', join_types())

    elif _property_name == 'data':
        return _handle_json_ref('tabular_expression_item', tabular_expression_item_types())

    elif _property_name == 'subsets':
        return _handle_json_ref('Array_tabular_expression_item', tabular_expression_item_types())

    elif _property_name == 'and_or':
        return _handle_json_ref('and_or', and_or())

    elif _property_name == 'constraint_type':
        return _handle_json_ref('constraint_types', constraint_types())

    elif _property_name == 'index_type':
        return _handle_json_ref('index_types', index_types())

    elif _property_name == 'xpath_data_format':
        return _handle_json_ref('xpath_data_format', xpath_data_formats())

    elif _property_name in ['expression', 'parameters', 'result', 'else_statement', 'orderby', 'expressionitems']:
        return _handle_json_ref('Array_expression_item', expression_item_types())

    elif _property_name in ['in_values']:
        return _handle_json_ref('in_types', in_types())

    elif _property_name in ['data_source']:
        return _handle_json_ref('data_source_types', data_source_types())

    elif _property_name in ['left', 'right']:
        return _handle_json_ref('condition_part', condition_part())


    # Complex types
    elif _property_name in ['checkconditions', 'conditions']:
        return _handle_json_ref('Array_ParameterCondition')

    elif _property_name in ['columnnames']:
        return _handle_json_ref('Array_string')

    elif _property_name == 'columns':
        return _handle_json_ref('Array_ParameterColumndefinition')


    elif _property_name in ['destination_identifier', 'table_identifier']:
        return _handle_json_ref('ParameterIdentifier')

    elif _property_name in ['column_identifiers', 'references']:
        return _handle_json_ref('Array_ParameterIdentifier')

    elif _property_name == 'constraints':
        return _handle_json_ref('Array_ParameterConstraint')

    elif _property_name == 'fields':
        return _handle_json_ref('Array_ParameterField')

    elif _property_name == 'select':
        return _handle_json_ref('VerbSelect')

    elif _property_name == 'sources':
        return _handle_json_ref('Array_ParameterSource')

    elif _property_name == 'csv_dialect':
        return _handle_json_ref('file_types', list_dialects())

    elif _property_name == 'when_statements':
        return _handle_json_ref('Array_ParameterWhen')

    elif _property_name == 'order_by':
        return _handle_json_ref('Array_ParameterOrderByItem')

    elif _property_name == 'assignments':
        return _handle_json_ref('Array_ParameterAssignment')
    else:
        raise Exception("sql_property_to_type: Unrecognized property:" + _property_name)


def verbs():
    """Returns a list of the supported verb types"""
    return ['VerbCreateTable', 'VerbCreateIndex', 'VerbSelect', 'VerbCustom', 'VerbInsert']
