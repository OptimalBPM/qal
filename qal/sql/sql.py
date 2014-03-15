"""
    ***********************************************************
    qal.sql.sql holds the class structure representation of SQL
    ***********************************************************
    .. note:: 
        * The mySQL DDL implementation defaults to using innoDB, since stuff like foreign keys and other very important security features are lacking from myISAM.
        * All ParameterBase descendants property names are named in a specific way, so that one from that name can discern what types are allowed. For example: sources means that it is a list of ParameterSource.
        * Parameter_* means that it is some form of input, Verb_* means that this statement can be executed stand alone.
        * No parameters can be required in either Verb_* or Parameter_* classes. If they are, the classes cannot be inspected by qal.sql.meta.list_class_properties.


    :warning: Changes and new classes must satisfy both the import/export of data structures and schema generation.
    :copyright: Copyright 2010-2013 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

from qal.sql.types import DEFAULT_ROWSEP, expression_item_types,tabular_expression_item_types
from qal.dal.types import DB_POSTGRESQL, DB_MYSQL, DB_ORACLE, DB_DB2 # , DB_SQLSERVER 

from qal.sql.base import ParameterBase, SqlList, ParameterExpressionItem
from qal.sql.remotable import ParameterRemotable


from qal.sql.utils import add_operator, parenthesise, oracle_add_escape, add_comma, make_operator, check_for_param_content,\
                                none_as_sql, error_on_blank, comma_separate, make_function, db_specific_operator, db_specific_object_reference,\
                                citate,check_not_null, curr_user, db_specific_datatype, curr_datetime, add_comma_rs, oracle_create_auto_increment,\
                                handle_temp_table_ref, datatype_to_parameter
from qal.sql.types import condition_part
from qal.dataset.flatfile import Flatfile_Dataset
from qal.dataset.xpath import XPath_Dataset
from qal.dataset.matrix import Matrix_Dataset
from qal.dal.types import DB_SQLSERVER
from qal.common.strings import make_path_absolute



class ParameterExpression(ParameterExpressionItem):
    """Holds an expression"""
    
    expressionitems = None
    
    def __init__(self, _expressionitems = None,_operator = None): 
        super(ParameterExpression, self ).__init__(_operator)
        
        if _expressionitems != None:
            self.expressionitems = _expressionitems
        else:
            self.expressionitems = SqlList()
                   
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine""" 
        result = ''
        #Loop expressions
        for index, item in enumerate(self.expressionitems):
            # Some want || instrad of + for concatenation 
            if (_db_type == DB_POSTGRESQL or _db_type == DB_DB2):
                result+= add_operator(index, make_operator(item.operator, True)) + item.as_sql(_db_type) 
            else:
                result+= add_operator(index, make_operator(item.operator, False)) + item.as_sql(_db_type) 
        if len(self.expressionitems) > 1:
            return parenthesise(result)
        else:
            return result
        
    
class ParameterString(ParameterExpressionItem):
    """Holds a string parameter/value"""
    
    string_value = ''  
    escape_character = ''  
    
    def __init__(self, _string_value = '', _operator = None, _escape_character = None):
        super(ParameterString, self ).__init__(_operator)
        self.string_value = _string_value  
        if (_escape_character != None):
            self.escape_character = _escape_character
        else:
            self.escape_character = ''
        
    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        if _db_type != DB_ORACLE:
            return "'" + self.string_value + "'"
        else:
            return oracle_add_escape("'" + self.string_value + "'", self.escape_character)  

    
class ParameterNumeric(ParameterExpressionItem):
    """Holds a numeric parameter/value"""
    numeric_value = ''
    
    def __init__(self, _numeric_value = '', _operator = None):
        super(ParameterNumeric, self ).__init__(_operator)
        self.numeric_value = _numeric_value  
    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        #Check if it is a valid float
        try:
            float(self.numeric_value)
        except ValueError:
            if (str(self.numeric_value).lower() != 'null') and (check_for_param_content(self.numeric_value) == False):
                raise Exception("A numeric parameter must be numeric. Value:" + self.numeric_value)
        return str(self.numeric_value)   
    # TODO: Check if any flavors allow other decimal signs.

class ParameterParameter(ParameterExpressionItem):
    """Holds a parameter to be used in prepared statements."""
    
    datatype = ''  
  
    def __init__(self, _datatype = '', _operator = None):
        super(ParameterParameter, self ).__init__(_operator)
        self.datatype = _datatype  
    

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
       
        # This is temporary, awaiting proper PyMySQL support for %d.
        return datatype_to_parameter(_db_type, self.datatype)

    
class ParameterIn(ParameterExpressionItem):
    """Hold an SQL IN-statement"""
    in_values = None
    
    def __init__(self, _in_values = '', _operator = None):
        super(ParameterIn, self ).__init__(_operator)
        self.in_values = _in_values 
    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        # Handle strings separately. Somewhat non-pythonic, but the alternative would be far more complicated.
        if (type(self.in_values) is ParameterString):
            return "IN " + parenthesise(self.in_values.string_value) 
        else: 
            return "IN " + parenthesise(self.in_values.as_sql(_db_type))   

    
    

        

class ParameterDataset(ParameterExpressionItem, ParameterRemotable):
    """Holds a dataset from an external, non-SQL source"""
    data_source = None

    
    def __init__(self, _data_source = None):

            
        if (_data_source != None):
            self.data_source = _data_source
        else:
            self.data_source = None
        
        self.resource_uuid = None
            
    def dataset_to_sql(self):
        """Generate SQL for specified database engine """
        
        # TODO: Is this used?
        if (self._dataset.loaded):
        
            _SQL = ''
            
            print("in dataset_to_sql" + str(self._dataset))
            
            return _SQL
        else:
            raise Exception('Dataset_To_SQL: Dataset not loaded.'); 
        
    def _generate_sql(self, _db_type):
        if (self.data_source):

            """Handle relative filename"""
            if hasattr(self.data_source, 'filename'):
                self.data_source.filename = make_path_absolute(self.data_source.filename, self._base_path)

            self.data_source.load()
            return "("+ self.data_source.as_sql(_db_type) + ")"       
        else:
            raise Exception('ParameterDataset.as_sql : data_source not set.');

class ParameterIdentifier(ParameterExpressionItem):
    """Holds an identifier(column-, table or other reference)"""
    identifier = ''
    prefix = ''
    
    def __init__(self, _identifier = '', _operator = None, _prefix = None):
        super(ParameterIdentifier, self ).__init__(_operator)
        self.identifier = _identifier  
        if _prefix != None:
            self.prefix = _prefix
        else:
            self.prefix = '' 
            
                                  
    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        # Add prefix if set
        if self.prefix != '':
            _tmp_prefix = self.prefix + '.'
        else:
            _tmp_prefix = ''
        # Add quote if needed
        if _db_type in (DB_POSTGRESQL, DB_DB2, DB_ORACLE):
            return _tmp_prefix + '"' + str(handle_temp_table_ref(self.identifier, _db_type)) + '"'
        else:    
            return _tmp_prefix + str(handle_temp_table_ref(self.identifier, _db_type))


class ParameterCast(ParameterExpressionItem):
    
    """A Cast() converts an expression to a specified datatype.
    Properties:
    * expression is a list of expression items(sql_types.expression_item_types()).
    * datatype is a string containing the datatype(as defined in sql_types.data_types())

    Example: http://192.168.0.210/mediawiki/index.php/Unified_BPM_Database_Abstraction_Layer#ParameterCast.28ParameterExpressionItem.29"""
    
    expression = ''
    datatype = ''
    
    def make_cast(self, _value):
        """Generate the CAST syntax"""
        return str('CAST' + parenthesise(_value + ' AS ' + self.datatype))   
    
    def __init__(self, _expression = None, _datatype = None,_operator = None):
        super(ParameterCast, self ).__init__(_operator)
        if _expression != None:
            self.expression = _expression
        else:
            self.expression = SqlList(expression_item_types())
        
        self.datatype = _datatype
    
    def _generate_sql(self, _DB_Type):
        """Generate SQL for specified database engine"""
        return self.make_cast(self.expression.as_sql(_DB_Type))    

class ParameterFunction(ParameterExpressionItem):
    """Holds an SQL function call"""
    parameters = None
    name = ''
    
    def __init__(self, _parameters = None, _name = '',_operator = None): 
        super(ParameterFunction, self ).__init__(_operator)
        
        if _parameters != None:
            self.parameters = _parameters
        else:
            self.parameters = SqlList(expression_item_types())
        
        self.name = _name    


    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        result = ''
        for index, item in enumerate(self.parameters):
            result+= add_comma(index, item.as_sql(_db_type))
        return make_function(self.name, result)


class ParameterWhen(ParameterBase):
    """Holds a WHEN statement"""
    conditions = None
    result = None
    
    def __init__(self, _conditions = None, _result = None):
        super(ParameterWhen, self ).__init__()
        self.conditions = _conditions
        self.result = _result  
        
    def _generate_sql(self,_db_type):
        """Generate SQL for specified database engine"""
        return 'WHEN ' + self.conditions.as_sql(_db_type) + ' THEN ' + self.result.as_sql(_db_type)

    
class ParameterCase(ParameterExpressionItem):
    """Holds a CASE statement (see ParameterWhen)"""
    when_statements = None
    else_statement = None
    
    def __init__(self, _when_statements = None, _else_statement = None,_operator = None): 
        super(ParameterCase, self ).__init__(_operator)
        
        if _when_statements != None:
            self.when_statements = _when_statements
        else:
            self.when_statements = SqlList(expression_item_types())
        
        self.else_statement = _else_statement

    def _generate_sql(self, _DB_Type):
        """Generate SQL for specified database engine"""
        result = 'CASE'
        for item in self.when_statements:
            result+= ' ' + item.as_sql(_DB_Type)
        if self.else_statement != None:
            result+= ' else_statement ' + self.else_statement.as_sql(_DB_Type)            
        result+= ' END'
        return result
        
 
        return result
class ParameterSet(ParameterBase):
    
    """This class holds a set. 
    In SQL, that means more than one tabular datasets is combined using a set operator like UNION."""
    # expression could be any table-valued expression
    subsets     = None
    set_operator = None
     
    def __init__(self, _subsets = None, _set_operator = None):
        super(ParameterBase, self ).__init__()
        
        # One can either have tabular source data, or an SQL expression 

        if _subsets != None:
            self.subsets = _subsets
        else:
            self.subsets= SqlList(tabular_expression_item_types())
        if _set_operator != None:
            self.set_operator = _set_operator
        else:
            self.set_operator = None                
    def _generate_sql(self,_db_type):
        """Generate SQL for specified database engine"""
        _sqls = []
        # Loop all expressions into a list. 
        [_sqls.append(none_as_sql(x,_db_type,'')) for x in self.subsets]
        # Separate them with operators.
        return ('\n'+self.set_operator + '\n').join(_sqls)     
    
class ParameterSource(ParameterBase, ParameterRemotable):
    """This class hold a source of data that can be used with a FROM or JOIN-statement."""
    # expression could be any table-valued expression
    expression     = None
    
    conditions     = None
    alias          = ''
    join_type      = None
    def __init__(self, _expression = None, _conditions = None, _alias = '', _join_type = None):
        super(ParameterSource, self ).__init__()
        
        # One can either have tabular source data i union:ed sets, a sub select, an SQL identifier or a noSQL data set. 

        if _expression != None:
            self.expression = _expression
        else:
            self.expression = SqlList(expression_item_types())
            
        if _conditions != None:
            self.conditions = _conditions  
        else:
            self.conditions = ParameterConditions()
            
        self.alias = _alias
        
        self.join_type = _join_type
        self.resource_uuid = None
        
    def _generate_sql(self,_db_type):
        """Generate SQL for specified database engine"""
        _sql = none_as_sql(self.expression,_db_type, '')
    
        return _sql
    
class ParameterOrderByItem(ParameterExpression):
    """This class holds an order by-statement"""
    #Sort direction, ASC or DESC
    direction = None
    
    def __init__(self, _expressionitems = None, _direction = None):
        super(ParameterOrderByItem, self ).__init__(_expressionitems)
        self.direction = _direction

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        return super(ParameterOrderByItem, self )._generate_sql(_db_type) + " " + self.direction
        
     
class VerbSelect(ParameterExpressionItem, ParameterRemotable):
    """This class holds a SELECT statement. """
    fields = None
    sources = None
    order_by = None
    top_limit = None
    
    # Internal variables
    _post_verb = ''
    _post_sql = ''
    
    # TODO: Implement GROUP BY and HAVING-support.
    #GROUP_BY = ''
    #HAVING = ''
    
    def __init__(self, _fields = None, _sources = None, _operator = None, _order_by = None): 
        super(VerbSelect, self ).__init__(_operator)
        
        if _fields != None:
            self.fields = _fields
        else:
            self.fields = SqlList("ParameterIdentifier")
            
        if _sources != None:
            self.sources = _sources
        else:
            self.sources = SqlList("ParameterSource")

        if _order_by != None:
            self.order_by = _order_by
        else:
            self.order_by = SqlList("ParameterExpressionItem")
            
        self.top_limit = None
        self.resource_uuid = None
        
    def add_limit(self, _db_type):
        """Generate SQL for specified database engine for limits on number of rows (TOP/LIMIT/FETCH FIRST)"""
        if self.top_limit != None and int(self.top_limit) > 0:
            if (_db_type in [DB_MYSQL, DB_POSTGRESQL]):
                    self._post_sql = 'LIMIT ' + str(int(self.top_limit))
            elif (_db_type in [DB_DB2]):
                    self._post_sql = 'FETCH FIRST ' + str(int(self.top_limit)) + ' ROWS ONLY '         
            # Oracles' solution is not handled here, but in the as_sql-WHERE handling.
            elif (_db_type != DB_ORACLE): 
                self._post_verb = 'TOP ' + str(int(self.top_limit)) + ' '
            

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        if len(self.sources) > 0:        
            self.add_limit(_db_type)
            
        # Add select and its fields
        result = 'SELECT ' + self._post_verb
        if (self.fields != None) and (len(self.fields) > 0):
            for index, item in enumerate(self.fields):
                result+= add_comma(index, item.as_sql(_db_type))
        else:
            result+='*'
            
        
        # Add FROM
        if len(self.sources) > 0:
            result+= ' FROM '
            result+= self.sources[0].as_sql(_db_type) 
            if  self.sources[0].alias != None and self.sources[0].alias != '':
                if (_db_type != DB_ORACLE):
                    result+= ' AS ' + self.sources[0].alias
                else:
                    result+= ' ' + self.sources[0].alias
                
        # DB2 needs a dummy FROM, even if there is no source table. 
        elif (_db_type == DB_DB2):  
            result+= ' FROM sysibm.sysdummy1 '
        # Oracle, too, needs a dummy FROM, even if there is no source table. 
        elif (_db_type == DB_ORACLE):  
            result+= ' FROM dual '
         
        # Add joins
        if len(self.sources) > 1:
            for index, item in enumerate(self.sources):

                if index > 0:
                    """As as_sql is not called, as_sql has to be called explicitly."""
                    item.as_sql(_db_type)
                    if item.join_type:
                        result+= ' '+ item.join_type
                    result+= ' JOIN ' + none_as_sql(item.expression, _db_type, _error = 'VerbSelect: Joins must contain a statement or a reference to a table.')
                    result+= ' AS ' + error_on_blank(item.alias,'VerbSelect: Joins must have aliases.')
                    if item.join_type != "CROSS":
                        result+= ' ON ' + none_as_sql(item.conditions,_db_type, _error = 'VerbSelect: Joins must have conditions.')
        
        # Add WHERE 
        if len(self.sources) > 0:
            _num_conds = len(self.sources[0].conditions)
            if _num_conds > 0:
                result+= ' WHERE ' + self.sources[0].conditions.as_sql(_db_type)
                
            # Add Oracles TOP/LIMIT solution
            # TODO: Check out: Would this ever be applicable without sources?
            if (_db_type == DB_ORACLE) :
                _num_conds = len(self.sources[0].conditions)
                if _num_conds > 0:
                    result+= ' AND '
                else:
                    result+= ' WHERE '
                #This is a *very* ugly solution, but the only way I could find out.
                if (self.top_limit > 0):
                    result+= '(ROWNUM < ' + str(int(self.top_limit) + 1)+ ')'
    
        
        if len(self.order_by) > 0:
            orderresult = ''
            for currItem in self.order_by:
                if orderresult == '':
                    orderresult= ' ORDER BY '
                else:
                    orderresult+= ', '
                    
                orderresult+= currItem.as_sql(_db_type)
            result+= orderresult
        
        if self._post_sql != '':
            result+= DEFAULT_ROWSEP + self._post_sql
        
        self._post_sql = ''
        self._post_verb = ''

        return result
    
    def append_field_identifier(self, _identifier):
        """Helper to append field identifier classes to field list only using names"""
        _ident = ParameterIdentifier(_identifier)

        self.fields.append(_ident)


class ParameterCondition(ParameterBase):
    """This class holds a condition, that is a comparison (IF A=B)"""
    left = None
    right = None
    operator =''
    and_or = '' 
    
    def __init__(self, _left = None, _right = None, _operator = '', _and_or = ''):
        super(ParameterCondition, self ).__init__()
        if _left != None:
            self.left     = _left
        else:
            self.left     = SqlList(condition_part())
        if _right != None:
            self.right     = _right
        else:
            self.right     = SqlList(condition_part())

        self.operator =  _operator  
        self.and_or    = _and_or
        
    def as_sql(self, _db_type, _index = 0):
        """Generate SQL for specified database engine (index if for handling when it is the first in a list of conditions)"""
        """Note that the ParameterBase.as_sql is not called, this is because 1) Conditions are in context 2) Needs _index argument"""
        # TODO: Handle ILIKE for PostgreSQL.
        if (_index != 0):
            error_on_blank(self.and_or ,'ParameterCondition: Conditions must be separated by a logical operator. "and_or" is not set.')
            _result = ' ' + self.and_or + ' '
                
        else:
            _result = ''
        
        _result+= parenthesise(self.left.as_sql(_db_type)  + ' ' + db_specific_operator(self.operator, _db_type) + ' ' + self.right.as_sql(_db_type))  
        return _result

class ParameterAssignment(ParameterBase):
    """This class holds a assignment, that is assign the result of the expression to the right to the identifier on the left."""
    left = None
    right = None

    
    def __init__(self, _left = None, _right = None):
        super(ParameterAssignment, self ).__init__()
        if _left != None:
            self.left     = _left
        else:
            self.left     = None
        if _right != None:
            self.right     = _right
        else:
            self.right     = None

    def as_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        # TODO: Handle ILIKE for PostgreSQL.
        
        _result = self.left.as_sql(_db_type)  + ' = ' + self.right.as_sql(_db_type)  
        return _result
        

class ParameterConditions(SqlList):
    """This class holds a list of condition or list of conditions ((A=B AND C=D) OR (E=F))"""
    def __init__(self):
        super(ParameterConditions, self ).__init__(["ParameterCondition", "ParameterConditions"])
   
   
    def get_first_and_or(self):
        """Return the first and/or to know if it should add its own
        (If it is the first condition there is no point in adding its or, but rather the parent conditions' operator."""
        if (len(self)) > 0:
            _first_item = self[0]
            if (hasattr(_first_item, "get_first_and_or")):
                return _first_item.get_first_and_or()
            else:
                return ' ' + _first_item.and_or + ' '          
        else:
            raise "ParameterConditions: Invalid structure - Cannot get and_or operator from empty list of conditions."
        
        
    def as_sql(self, _db_type, _parent_index = 0):
        """Generate SQL for specified database engine"""
        _result = ''  

        for _index, _item in enumerate(self):
            _result+= _item.as_sql(_db_type, _index)
            
        
        _result = parenthesise(_result)
            
        if (_parent_index != 0):
            _result = self.get_first_and_or() + _result   
        
        return _result

class ParameterField(ParameterBase):
    """Holds a field definition (SELECT _FIELD1 AS _FIELD) """
    expression = None
    alias = ''
    
    def __init__(self, _expression = None, _alias = ''):
        super(ParameterField, self ).__init__()
        if _expression == None:
            self.expression = SqlList(expression_item_types())
        else:
            self.expression = _expression   
        self.alias      = _alias
    
    def _generate_sql(self, _DB_Type):
        """Generate SQL for specified database engine"""
        if self.alias != '':
            return self.expression.as_sql(_DB_Type) + ' AS ' + self.alias
        else:
            return self.expression.as_sql(_DB_Type)
               
class ParameterConstraint(ParameterBase):
    """Hold a key constraint declaration"""
    name = ''
    constraint_type = None
    references = None # First key is considered local in a FK.
    checkconditions = None
    
    def __init__(self, _name  = '', _constraint_type = None, _references = None, _checkconditions = None):
        super(ParameterConstraint, self ).__init__()
        self.name = _name  
        self.constraint_type = _constraint_type
        if _references != None:
            self.references = _references
        else:
            self.references = SqlList()
            
        if _checkconditions != None :
            self.checkconditions = _checkconditions
        else:
            self.checkconditions = SqlList(["ParameterCondition", "ParameterConditions"])

                
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        result = 'CONSTRAINT ' + db_specific_object_reference(self.name, _db_type) + ' ' + self.constraint_type
        if (self.constraint_type == "CHECK"):
            result+= ' ' + self.checkconditions.as_sql(_db_type)
        if (self.constraint_type == "FOREIGN KEY"):
            result+=' ' + parenthesise(self.references[0].as_sql(_db_type))  + ' REFERENCES ' + citate(self.references[1].identifier, _db_type) + parenthesise(self.references[2].as_sql(_db_type)) 
        if (self.constraint_type == "PRIMARY KEY"):
            result+=' ' + parenthesise(comma_separate(self.references, _db_type)) 
        if (self.constraint_type == "UNIQUE"):
            result+=' ' + parenthesise(comma_separate(self.references, _db_type))  
        if (self.constraint_type == "DEFAULT"):
            result+=' ' + parenthesise(self.references[0].as_sql(_db_type))              
        return result 
         
        
class VerbCreateIndex(ParameterBase):
    """Holds a statement for creating database indices"""
    name = ''
    index_type = None
    tablename = ''
    columnnames = None


    
    def __init__(self, _name = '', _index_type = None, _tablename = '', _columnnames = None):
        super(VerbCreateIndex, self ).__init__()
        self.name = _name  
        self.index_type = _index_type
        self.tablename = _tablename
        if _columnnames != None:
            self.columnnames = _columnnames
        else:
            self.columnnames = SqlList('string')
          
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        check_not_null("VerbCreateIndex", [[self.name, "name"],[self.index_type, "index_type"], [self.tablename, "tablename"]])
        # Handle DB2s strange deviation #1
        if (_db_type == DB_DB2 and (self.index_type == "CLUSTERED" or self.index_type == "NONCLUSTERED")):
            result = 'CREATE INDEX ' + db_specific_object_reference(self.name, _db_type) + chr(13)
        else:
            result = 'CREATE ' + self.index_type + ' INDEX ' + db_specific_object_reference(self.name, _db_type) + chr(13)
        
        result+= 'ON '+ self.tablename + '('  
        for index, item in enumerate(self.columnnames):
            result+= add_comma(index, db_specific_object_reference(item, _db_type))
        result+= ')'
        # Handle DB2s strange deviation #2
        if (_db_type == DB_DB2 and self.index_type == "CLUSTERED"):
            result+=  chr(13) + 'CLUSTER'
        return result    

  
        
class ParameterColumndefinition(ParameterBase):
    """Holds a physical table column definition (not to confused with ParameterField which is a reference to one) """
    name = ''
    datatype = ''
    notnull = None
    default = ''
    
    def __init__(self, _name = '', _datatype = '', _notnull = None, _default = ''):
        super(ParameterColumndefinition, self ).__init__()
        self.name = _name
        self.datatype = _datatype
        self.notnull = _notnull
        self.default = _default
        
  
        

    def _generate_sql(self, _db_type, _mysql_pk = False):
        """Generate SQL for specified database engine"""
        result = db_specific_object_reference(self.name, _db_type) + ' ' + db_specific_datatype(self.datatype, _db_type) 
        if (_db_type == DB_MYSQL and _mysql_pk == True and self.datatype.lower() == 'serial'):
            result+= ' PRIMARY KEY'
            
        if (self.notnull == True and _db_type != DB_ORACLE ):
            result+= ' NOT NULL'
            
        if (self.default != ''):
            
            tmp_default = self.default
            tmp_default = tmp_default.replace('::curruser::', curr_user(_db_type))
            tmp_default = tmp_default.replace('::currdatetime::', curr_datetime(_db_type))
            if _db_type not in (DB_MYSQL, DB_DB2):
                result+= ' DEFAULT ' + parenthesise(tmp_default)
            else:
                result+= ' DEFAULT ' + tmp_default
                
        if (self.notnull == True and _db_type == DB_ORACLE ):
            result+= ' NOT NULL'
        
        if (self.notnull == False):
            result+= ' NULL'

        return result
    
     

class ParameterDDL(ParameterBase):
    """Parent class for SQL DDL(Data Definition Language) statement classes"""
    _post_sql = ''
    def __init__(self,_operator = None):
        super(ParameterDDL, self ).__init__()
        self._post_sql = ''
        
        
class VerbCreateTable(ParameterDDL):
    """Holds a CREATE TABLE statement"""
    name = ''
    columns = None
    constraints = None
    _post_statements = None
    def __init__(self, _name = None, _columns = None, _constraints = None):
        super(VerbCreateTable, self ).__init__()
        if _name != None:
            self.name = _name  
        else:
            self.name = ''
        
        if _columns != None:
            self.columns    = _columns
        else:
            self.columns    = SqlList("ParameterColumndefinition")
                               
        if _constraints != None:        
            self.constraints    = _constraints
        else:
            self.constraints = SqlList(["ParameterConstraint", "ParameterConstraints"])
        
        self._post_statements = list()
        
        
    def get_post_statements(self):
        """Return post statements. That is statements run after the main SQL"""
        return self._post_statements    
    
    def make_columns(self, _db_type):
        """Generate DDL for all columns"""
        result = ''
        for index, item in enumerate(self.columns):
            if (item.datatype.lower() == 'serial'):
                if (_db_type == DB_ORACLE):
                    self._post_statements.append(oracle_create_auto_increment(self, item))
                if (_db_type == DB_MYSQL and len(self.constraints) == 0): # TODO: This might have to really look for primary key constraints to be safe.
                    result+= add_comma_rs(index, item._generate_sql(_db_type, True), self._row_separator)
                else: 
                    result+= add_comma_rs(index, item.as_sql(_db_type), self._row_separator)
            else:      
                result+= add_comma_rs(index, item.as_sql(_db_type), self._row_separator) 
        return result
        
    def make_constraints(self, _db_type):
        """Generate SQL for all constraints"""
        result = ''
        for index, item in enumerate(self.constraints):
            result+= add_comma_rs(index, item.as_sql(_db_type), self._row_separator)   
        return result


    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        self._post_statements = []
        if self.name[0] == "#":
            if _db_type in [DB_POSTGRESQL, DB_MYSQL]:
                _result = "CREATE TEMPORARY TABLE "
            elif _db_type == [DB_DB2]:
                _result = "DECLARE GLOBAL TEMPORARY TABLE "
            elif _db_type == [DB_ORACLE]:
                _result = "CREATE GLOBAL TEMPORARY TABLE "
            else:
                _result = "CREATE TABLE "
            
            _result+= citate(self.name[1:len(self.name)], _db_type) + " (" + self._row_separator
        else:
            _result = "CREATE TABLE "
            _result+= citate(self.name, _db_type) + " (" + self._row_separator
            
        _result+= self.make_columns(_db_type) 
        if (len(self.constraints) > 0):
            _result+= ',' + self._row_separator + self.make_constraints(_db_type) 
        _result+= self._row_separator + ')' 
        
  
        if _db_type == DB_MYSQL:
            _result+= ' ENGINE=InnoDB'
                 
        return _result
    
class VerbInsert(ParameterBase):
    """This class holds an INSERT statement"""
    
    destination_identifier = None
    column_identifiers = None
    data = None
    
    def __init__(self, _destination_identifier = None, _column_identifiers = None, _select = None):
        super(VerbInsert, self ).__init__()
        if _destination_identifier != None:
            self.destination_identifier = _destination_identifier  
        else:
            self.destination_identifier = None
        
        if _column_identifiers != None:
            self.column_identifiers    = _column_identifiers
        else:
            self.column_identifiers    = SqlList("ParameterIdentifier")
                               
        if _select != None:        
            self.data = _select
        else:
            self.data = None            

    def makeIdentifiers(self, _db_type):
        """Generate SQL for the identifiers"""
        result = ''
        for currIndex, currIdent  in enumerate(self.column_identifiers):
            if currIndex > 0:
                result = result + ', '
            result = result + currIdent.as_sql(_db_type)
        return result     
        
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        if len(self.column_identifiers) > 0:
            result = 'INSERT INTO '+ self.destination_identifier.as_sql(_db_type) + ' (' + self.makeIdentifiers(_db_type) + ')' + DEFAULT_ROWSEP
            # Add data and remove parenthesis. Removing them here could certainly be seen as ugly but the reason 
            # is that MySQL cannot handle them. It would break everything else any other way.
            if self.data:
                tmpSQL = self.data.as_sql(_db_type)
                if isinstance(self.data, ParameterSet):
                    result = result +  tmpSQL
                else:
                    result = result +  tmpSQL.lstrip('(').rstrip(')')
        else:
            raise Exception('VerbInsert.as_sql: No column_identifiers specified!')
                   
        return result  
    
class VerbUpdate(ParameterBase):
    """This class holds an INSERT statement"""
    
    table_identifier = None
    assignments = None
    conditions = None

    def __init__(self, _table_identifier = None, _assignments = None, _conditions = None):
        super(VerbUpdate, self ).__init__()
        if _table_identifier != None:
            self.table_identifier = _table_identifier  
        else:
            self.table_identifier = None
        
        if _assignments != None:
            self.assignments    = _assignments
        else:
            self.assignments    = SqlList("ParameterAssignment")
                               
        if _conditions != None:        
            self.conditions = _conditions
        else:
            self.conditions = ParameterConditions()

    def _generate_assignments_sql(self, _db_type):
        _results = [] 
        for _curr_assignment in self.assignments:
            _results.append( _curr_assignment.as_sql(_db_type))
        
        return ", ".join(_results)
        
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        if len(self.assignments) > 0:
            _result = 'UPDATE '+ self.table_identifier.as_sql(_db_type) + DEFAULT_ROWSEP
            _result+= 'SET' + DEFAULT_ROWSEP + self._generate_assignments_sql(_db_type)+ DEFAULT_ROWSEP 
            _result+= 'WHERE ' + self.conditions.as_sql(_db_type)
        else:
            raise Exception('.as_sql(_db_type) + DEFAULT_ROWSEP.as_sql: No column_identifiers specified!')
                   
        return _result  
    
class VerbDropTable(ParameterBase):
    """This class holds a DELETE statement"""
    name = None
    """
       The name of the table to be dropped.
    """

    
    def __init__(self, _name = None): 
        super(VerbDropTable, self ).__init__()
            
        if _name != None:
            self.name = _name
        else:
            self.name = None
        

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        return 'DROP TABLE ' + citate(self.name, _db_type)

class VerbDelete(ParameterBase):
    """This class holds a DELETE statement"""
    sources = None
    """
       @warning: Important! The first source must have a ParameterIdentifier as expression that specifies the target table.
    """

    
    def __init__(self, _sources = None, _operator = None): 
        super(VerbDelete, self ).__init__()
        
            
        if _sources != None:
            self.sources = _sources
        else:
            self.sources = SqlList("ParameterSource")
        

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        result = ''
        # Add FROM
        if len(self.sources) > 0:
            # Extract target table from expression in first source.
            if (len(self.sources[0].expression) > 0 and
                isinstance(self.sources[0].expression[0], ParameterIdentifier)):
                if (_db_type == DB_POSTGRESQL):
                    result+= 'DELETE FROM ' + self.sources[0].expression[0].as_sql(_db_type) + ' ' + self.sources[0].alias + DEFAULT_ROWSEP
                else:
                    result+= 'DELETE ' +self.sources[0].expression[0].as_sql(_db_type)+ DEFAULT_ROWSEP

            else:
                raise Exception("Error from VerbDelete.as_sql: Could not find identifier in first source expression.")

            if (_db_type != DB_POSTGRESQL):
                result+= 'FROM  '
                result+= self.sources[0].as_sql(_db_type) 
                if self.sources[0].alias != '':
                    if (_db_type != DB_ORACLE):
                        result+= ' AS ' + self.sources[0].alias
                    else:
                        result+= ' ' + self.sources[0].alias
        
        # Add JOINS/USING
        if len(self.sources) > 1:
            for index, item in enumerate(self.sources):
                if index > 0:
                    if (_db_type != DB_POSTGRESQL): 
                        result+= ' JOIN ' + none_as_sql(item.expression, _db_type, _error = 'VerbDelete: Joins must contain a statement or a reference to a table.')
                    else:
                        result+= ' USING ' + none_as_sql(item.expression, _db_type, _error = 'VerbDelete(for Postgresql): Joins must contain a statement or a reference to a table.')
                    result+= ' AS ' + error_on_blank(item.alias,'VerbDelete: Joins must have aliases.')
                    if (_db_type != DB_POSTGRESQL): 
                        result+= ' ON ' + none_as_sql(item.conditions,_db_type, _error = 'VerbDelete: Joins must have conditions.')
        
        # Add WHERE 
        if len(self.sources) > 0:
            _num_conds = len(self.sources[0].conditions)
            if (_db_type == DB_POSTGRESQL) and (len(self.sources)>1):
                _num_conds+= len(self.sources[1].conditions)
            if (_num_conds > 0):
                result+= ' WHERE '
                if len(self.sources[0].conditions) > 0: 
                    result+=  self.sources[0].conditions.as_sql(_db_type)
                if (_db_type == DB_POSTGRESQL) and (len(self.sources)>1) and (len(self.sources[1].conditions) > 0):
                    result+= ' ' + self.sources[1].conditions.as_sql(_db_type)
                    if len(self.sources) > 2:
                        raise Exception('VerbDelete: To be able to generalize functionality, only one join is allowed due to Postgresql proprietary DELETE FROM .. USING syntax.')

         
        return result
    
    def append_field_identifier(self, _identifier):
        """Helper function to add ParameterIdentifier instances using just the field names"""
        _ident = ParameterIdentifier(_identifier)

        self.fields.append(_ident)

    
    
      
class VerbCustom(ParameterDDL):
    """This class holds custom statements (written, not generated) for all platforms.
    This is for when what is currenctly implementet do not suffice"""
   
    sql_mysql = ''
    sql_postgresql = ''
    sql_oracle = ''
    sql_db2 = ''
    sql_sqlserver = ''

    def _generate_sql(self, _db_type):
        """Return the specified SQLs for each database engine"""
        return [self.sql_mysql, self.sql_postgresql, self.sql_oracle, self.sql_db2, self.sql_sqlserver][_db_type]
        
    def __init__(self):
        super(VerbCustom, self ).__init__()
        self.sql_mysql = ''
        self.sql_postgresql = ''
        self.sql_oracle = ''
        self.sql_db2 = ''
        self.sql_sqlserver = ''
    
        