"""
Created on May 23, 2010

@author: Nicklas Boerjesson
@note: 
* The mySQL DDL implementation defaults to using innoDB, since stuff like foreign keys and other very important security features are lacking from myISAM.
* All Parameter_Base descendants property names are named in a specific way, so that one from that name can discern what types are allowed.
For example: sources means that it is a list of Parameter_Source.
* Parameter_ means that it is some form of input, Verb_ means that this statement can be executed stand alone.
 
@warning: Changes and new classes must satisfy both the import/export of data structures and schema generation.

"""

from qal.sql.sql_types import DEFAULT_ROWSEP, expression_item_types,tabular_expression_item_types
from qal.dal.dal_types import DB_POSTGRESQL, DB_MYSQL, DB_ORACLE, DB_DB2 # , DB_SQLSERVER 

from qal.sql.sql_base import Parameter_Base, Parameter_Remotable, SQL_List, Parameter_Expression_Item


from qal.sql.sql_utils import add_operator, parenthesise, oracle_add_escape, add_comma, make_operator, check_for_param_content,\
                                none_as_sql, error_on_blank, comma_separate, make_function, db_specific_operator, db_specific_object_reference,\
                                 citate,check_not_null, curr_user, db_specific_datatype, curr_datetime, add_comma_rs, oracle_create_auto_increment
from qal.sql.sql_types import condition_part
from qal.nosql.flatfile import Flatfile_Dataset
from qal.nosql.rdbms import RDBMS_Dataset
from qal.nosql.xml import XML_Dataset
from qal.nosql.matrix import Matrix_Dataset



class Parameter_Expression(Parameter_Expression_Item):
    """Holds an expression"""
    
    expressionitems = None
    
    def __init__(self, _expressionitems = None,_operator = None): 
        super(Parameter_Expression, self ).__init__(_operator)
        
        if _expressionitems != None:
            self.expressionitems = _expressionitems
        else:
            self.expressionitems = SQL_List()
                   
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
        
    
class Parameter_String(Parameter_Expression_Item):
    """Holds a string parameter/value"""
    
    string_value = ''  
    escape_character = ''  
    
    def __init__(self, _string_value = '', _operator = None, _escape_character = None):
        super(Parameter_String, self ).__init__(_operator)
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

    
class Parameter_Numeric(Parameter_Expression_Item):
    """Holds a numeric parameter/value"""
    numeric_value = ''
    
    def __init__(self, _numeric_value = '', _operator = None):
        super(Parameter_Numeric, self ).__init__(_operator)
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
    
class Parameter_IN(Parameter_Expression_Item):
    """Hold an SQL IN-statement"""
    in_values = None
    
    def __init__(self, _in_values = '', _operator = None):
        super(Parameter_IN, self ).__init__(_operator)
        self.in_values = _in_values 
    
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        # Handle strings separately. Somewhat non-pythonic, but the alternative would be far more complicated.
        if (type(self.in_values) is Parameter_String):
            return "IN " + parenthesise(self.in_values.string_value) 
        else: 
            return "IN " + parenthesise(self.in_values.as_sql(_db_type))   

    
    

        

class Parameter_NoSQL(Parameter_Expression_Item, Parameter_Remotable):  
    """Holds a dataset from an external, non-SQL source"""
    data_source = None

    
    def __init__(self, _data_source = None):

            
        if (_data_source != None):
            self.data_source = _data_source
        else:
            self.data_source = None
            
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
           
            self.data_source.load()
            return "("+ self.data_source.as_sql(_db_type) + ")"       
        else:
            raise Exception('Parameter_NoSQLas_sql : data_source not set.');

class Parameter_Identifier(Parameter_Expression_Item):
    """Holds an identifier(column-, table or other reference)"""
    identifier = ''
    prefix = ''
    
    def __init__(self, _identifier = '', _operator = None, _prefix = None):
        super(Parameter_Identifier, self ).__init__(_operator)
        self.identifier = _identifier  
        if _prefix != None:
            self.prefix = _prefix
        else:
            self.prefix = '' 
            
    
    def _generate_sql(self, _db_type, _is_source_table = False):
        """Generate SQL for specified database engine"""
        # Add prefix if set
        if self.prefix != '':
            _tmp_prefix = self.prefix + '.'
        else:
            _tmp_prefix = ''
        # Add quote if needed
        if _db_type in (DB_POSTGRESQL, DB_DB2, DB_ORACLE) and not _is_source_table:
            return  _tmp_prefix + '"' + str(self.identifier) + '"'
        else:    
            return _tmp_prefix + str(self.identifier)


class Parameter_Cast(Parameter_Expression_Item):
    
    """A Cast() converts an expression to a specified datatype.
    Properties:
    * expression is a list of expression items(sql_types.expression_item_types()).
    * datatype is a string containing the datatype(as defined in sql_types.data_types())

    Example: http://192.168.0.210/mediawiki/index.php/Unified_BPM_Database_Abstraction_Layer#Parameter_CAST.28Parameter_Expression_Item.29"""
    
    expression = ''
    datatype = ''
    
    def make_cast(self, _value):
        """Generate the CAST syntax"""
        return str('CAST' + parenthesise(_value + ' AS ' + self.datatype))   
    
    def __init__(self, _expression = None, _datatype = None,_operator = None):
        super(Parameter_Cast, self ).__init__(_operator)
        if _expression != None:
            self.expression = _expression
        else:
            self.expression = SQL_List(expression_item_types())
        
        self.datatype = _datatype
    
    def _generate_sql(self, _DB_Type):
        """Generate SQL for specified database engine"""
        return self.make_cast(self.expression.as_sql(_DB_Type))    

class Parameter_Function(Parameter_Expression_Item):
    """Holds an SQL function call"""
    parameters = None
    name = ''
    
    def __init__(self, _parameters = None, _name = '',_operator = None): 
        super(Parameter_Function, self ).__init__(_operator)
        
        if _parameters != None:
            self.parameters = _parameters
        else:
            self.parameters = SQL_List(expression_item_types())
        
        self.name = _name    


    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        result = ''
        for index, item in enumerate(self.parameters):
            result+= add_comma(index, item.as_sql(_db_type))
        return make_function(self.name, result)


class Parameter_WHEN(Parameter_Base):
    """Holds a WHEN statement"""
    conditions = None
    result = None
    
    def __init__(self, _conditions = None, _result = None):
        super(Parameter_WHEN, self ).__init__()
        self.conditions = _conditions
        self.result = _result  
        
    def _generate_sql(self,_db_type):
        """Generate SQL for specified database engine"""
        return 'WHEN ' + self.conditions.as_sql(_db_type) + ' THEN ' + self.result.as_sql(_db_type)

    
class Parameter_CASE(Parameter_Expression_Item):
    """Holds a CASE statement (see Parameter_When)"""
    when_statements = None
    else_statement = None
    
    def __init__(self, _when_statements = None, _else_statement = None,_operator = None): 
        super(Parameter_CASE, self ).__init__(_operator)
        
        if _when_statements != None:
            self.when_statements = _when_statements
        else:
            self.when_statements = SQL_List(expression_item_types())
        
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
class Parameter_Set(Parameter_Base): 
    
    """This class holds a set. 
    In SQL, that means more than one tabular datasets is combined using a set operator like UNION."""
    # expression could be any table-valued expression
    subsets     = None
    set_operator = None
     
    def __init__(self, _subsets = None, _set_operator = None):
        super(Parameter_Base, self ).__init__()
        
        # One can either have tabular source data, or an SQL expression 

        if _subsets != None:
            self.subsets = _subsets
        else:
            self.subsets= SQL_List(tabular_expression_item_types())
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
    
class Parameter_Source(Parameter_Base, Parameter_Remotable):
    """This class hold a source of data that can be used with a FROM or JOIN-statement."""
    # expression could be any table-valued expression
    expression     = None
    
    conditions     = None
    alias          = ''
    join_type      = None
    def __init__(self, _expression = None, _conditions = None, _alias = '', _join_type = None):
        super(Parameter_Source, self ).__init__()
        
        # One can either have tabular source data, or an SQL expression 

        if _expression != None:
            self.expression = _expression
        else:
            self.expression = SQL_List(expression_item_types())
            
        if _conditions != None:
            self.conditions = _conditions  
        else:
            self.conditions = Parameter_Conditions()
            
        self.alias = _alias
        
        self.join_type = _join_type
        
    def _generate_sql(self,_db_type):
        """Generate SQL for specified database engine"""
        _sql = none_as_sql(self.expression,_db_type, '')
    
        return _sql
    
class Parameter_ORDER_BY_item(Parameter_Expression): 
    """This class holds an order by-statement"""
    #Sort direction, ASC or DESC
    direction = None
    
    def __init__(self, _expressionitems = None, _direction = None):
        super(Parameter_ORDER_BY_item, self ).__init__(_expressionitems)
        self.direction = _direction

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        return super(Parameter_ORDER_BY_item, self )._generate_sql(_db_type) + " " + self.direction
        
     
class Verb_SELECT(Parameter_Expression_Item, Parameter_Remotable):
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
        super(Verb_SELECT, self ).__init__(_operator)
        
        if _fields != None:
            self.fields = _fields
        else:
            self.fields = SQL_List("Parameter_Identifier")
            
        if _sources != None:
            self.sources = _sources
        else:
            self.sources = SQL_List("Parameter_Source")

        if _order_by != None:
            self.order_by = _order_by
        else:
            self.order_by = SQL_List("Parameter_Expression_Item")
            
        self.top_limit = None
        
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
        for index, item in enumerate(self.fields):
            result+= add_comma(index, item.as_sql(_db_type))
        
        # Add FROM
        if len(self.sources) > 0:
            result+= ' FROM '
            result+= self.sources[0].as_sql(_db_type) 
            if self.sources[0].alias != '':
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
                    result+= ' JOIN ' + none_as_sql(item.expression, _db_type, _error = 'Verb_SELECT: Joins must contain a statement or a reference to a table.')
                    result+= ' AS ' + error_on_blank(item.alias,'Verb_SELECT: Joins must have aliases.')
                    result+= ' ON ' + none_as_sql(item.conditions,_db_type, _error = 'Verb_SELECT: Joins must have conditions.')
        
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
        _ident = Parameter_Identifier(_identifier)

        self.fields.append(_ident)


class Parameter_Condition(Parameter_Base):
    """This class holds a condition, that is a comparison (IF A=B)"""
    left = None
    right = None
    operator =''
    and_or = '' 
    
    def __init__(self, _left = None, _right = None, _operator = '', _and_or = ''):
        super(Parameter_Condition, self ).__init__()
        if _left != None:
            self.left     = _left
        else:
            self.left     = SQL_List(condition_part())
        if _right != None:
            self.right     = _right
        else:
            self.right     = SQL_List(condition_part())

        self.operator =  _operator  
        self.and_or    = _and_or
        
    def _generate_sql(self, _db_type, _index = 0):
        """Generate SQL for specified database engine (index if for handling when it is the first in a list of conditions)"""
        # TODO: Handle ILIKE for PostgreSQL.
        if (_index != 0):
            _result = ' ' + self.and_or + ' '
        else:
            _result = ''
        
        _result+= parenthesise(self.left.as_sql(_db_type)  + ' ' + db_specific_operator(self.operator, _db_type) + ' ' + self.right.as_sql(_db_type))  
        return _result

class Parameter_Conditions(SQL_List):
    """This class holds a list of condition or list of conditions ((A=B AND C=D) OR (E=F))"""
    def __init__(self):
        super(Parameter_Conditions, self ).__init__(["Parameter_Condition", "Parameter_Conditions"])
   
   
    def get_first_and_or(self):
        """Return the forst and/or to know if it should add its own
        (If it is the first condition there is no point in adding its or, but rather the parent conditions' operator."""
        if (len(self)) > 0:
            _first_item = self[0]
            if (hasattr(_first_item, "get_first_and_or")):
                return _first_item.get_first_and_or()
            else:
                return ' ' + _first_item.and_or + ' '          
        else:
            raise "Parameter_Conditions: Invalid structure - Cannot get and_or operator from empty list of conditions."
        
        
    def _generate_sql(self, _db_type, _parent_index = 0):
        """Generate SQL for specified database engine"""
        _result = ''  

        for _index, _item in enumerate(self):
            _result+= _item.as_sql(_db_type, _index)
            
        
        _result = parenthesise(_result)
            
        if (_parent_index != 0):
            _result = self.get_first_and_or() + _result   
        
        return _result

class Parameter_Field(Parameter_Base):
    """Holds a field definition (SELECT _FIELD1 AS FIELD_) """
    expression = None
    alias = ''
    
    def __init__(self, _expression = None, _alias = ''):
        super(Parameter_Field, self ).__init__()
        if _expression == None:
            self.expression = SQL_List(expression_item_types())
        else:
            self.expression = _expression   
        self.alias      = _alias
    
    def _generate_sql(self, _DB_Type):
        """Generate SQL for specified database engine"""
        if self.alias != '':
            return self.expression.as_sql(_DB_Type) + ' AS ' + self.alias
        else:
            return self.expression.as_sql(_DB_Type)
               
class Parameter_Constraint(Parameter_Base):  
    """Hold a key constraint declaration"""
    name = ''
    constraint_type = None
    references = None # First key is considered local in a FK.
    checkconditions = None
    
    def __init__(self, _name  = '', _constraint_type = None, _references = None, _checkconditions = None):
        super(Parameter_Constraint, self ).__init__()
        self.name = _name  
        self.constraint_type = _constraint_type
        if _references != None:
            self.references = _references
        else:
            self.references = SQL_List()
            
        if _checkconditions != None :
            self.checkconditions = _checkconditions
        else:
            self.checkconditions = SQL_List(["Parameter_Condition", "Parameter_Conditions"])

                
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
         
        
class Verb_CREATE_INDEX(Parameter_Base):
    """Holds a statement for creating database indices"""
    name = ''
    index_type = None
    tablename = ''
    columnnames = None


    
    def __init__(self, _name = '', _index_type = None, _tablename = '', _columnnames = None):
        super(Verb_CREATE_INDEX, self ).__init__()
        self.name = _name  
        self.index_type = _index_type
        self.tablename = _tablename
        if _columnnames != None:
            self.columnnames = _columnnames
        else:
            self.columnnames = SQL_List('string')
          
    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        check_not_null("Verb_CREATE_INDEX", [[self.name, "name"],[self.index_type, "index_type"], [self.tablename, "tablename"]])
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

  
        
class Parameter_ColumnDefinition(Parameter_Base):
    """Holds a physical table column definition (not to confused with Parameter_Field which is a reference to one) """
    name = ''
    datatype = ''
    notnull = None
    default = ''
    
    def __init__(self, _name = '', _datatype = '', _notnull = None, _default = ''):
        super(Parameter_ColumnDefinition, self ).__init__()
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
    
     

class Parameter_DDL(Parameter_Base): 
    """Parent class for SQL DDL(Data Definition Language) statement classes"""
    _post_sql = ''
    def __init__(self,_operator = None):
        super(Parameter_DDL, self ).__init__()
        self._post_sql = ''
        
        
class Verb_CREATE_TABLE(Parameter_DDL):
    """Holds a CREATE TABLE statement"""
    name = ''
    columns = None
    constraints = None
    _post_statements = None
    def __init__(self, _name = None, _columns = None, _constraints = None):
        super(Verb_CREATE_TABLE, self ).__init__()
        if _name != None:
            self.name = _name  
        else:
            self.name = ''
        
        if _columns != None:
            self.columns    = _columns
        else:
            self.columns    = SQL_List("Parameter_ColumnDefinition") 
                               
        if _constraints != None:        
            self.constraints    = _constraints
        else:
            self.constraints = SQL_List(["Parameter_Constraint", "Parameter_Constraints"])
        
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
                    result+= add_comma_rs(index, item.as_sql(_db_type, True), self._row_separator)
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
        result = 'CREATE TABLE '+ citate(self.name, _db_type) + ' (' + self._row_separator
        result+= self.make_columns(_db_type) 
        if (len(self.constraints) > 0):
            result+= ',' + self._row_separator + self.make_constraints(_db_type) 
        result+= self._row_separator + ')' 
        
  
        if _db_type == DB_MYSQL:
            result+= ' ENGINE=InnoDB'
                 
        return result
    
class Verb_INSERT(Parameter_Base):
    """This class holds an INSERT statement"""
    
    destination_identifier = None
    column_identifiers = None
    data = None
    
    def __init__(self, _destination_identifier = None, _column_identifiers = None, _select = None):
        super(Verb_INSERT, self ).__init__()
        if _destination_identifier != None:
            self.destination_identifier = _destination_identifier  
        else:
            self.destination_identifier = None
        
        if _column_identifiers != None:
            self.column_identifiers    = _column_identifiers
        else:
            self.column_identifiers    = SQL_List("Parameter_Identifier") 
                               
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
            tmpSQL = self.data.as_sql(_db_type)
            if isinstance(self.data, Parameter_Set):
                result = result +  tmpSQL
            else:
                result = result +  tmpSQL.lstrip('(').rstrip(')')
        else:
            raise Exception('Verb_INSERT.as_sql: No column_identifiers specified!')
                   
        return result  
    
class Verb_DELETE(Parameter_Base):
    """This class holds a DELETE statement"""
    sources = None
    """
       @warning: Important! The first source must have a parameter_identifier that specifies the target table.
    """

    
    def __init__(self, _sources = None, _operator = None): 
        super(Verb_DELETE, self ).__init__(_operator)
        
            
        if _sources != None:
            self.sources = _sources
        else:
            self.sources = SQL_List("Parameter_Source")
        

    def _generate_sql(self, _db_type):
        """Generate SQL for specified database engine"""
        
        result = ''
        # Add FROM
        if len(self.sources) > 0:
            # Extract target table from expression in first source.
            if (len(self.sources[0].expression) > 0 and
                isinstance(self.sources[0].expression[0], Parameter_Identifier)):
                if (_db_type == DB_POSTGRESQL):
                    result+= 'DELETE FROM ' + self.sources[0].expression[0].as_sql(_db_type) + ' ' + self.sources[0].alias + DEFAULT_ROWSEP
                else:
                    result+= 'DELETE ' +self.sources[0].expression[0].as_sql(_db_type)+ DEFAULT_ROWSEP

            else:
                raise Exception("Error from Verb_DELETE.as_sql: Could not find identifier in first source expression.")

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
                        result+= ' JOIN ' + none_as_sql(item.expression, _db_type, _error = 'Verb_DELETE: Joins must contain a statement or a reference to a table.')
                    else:
                        result+= ' USING ' + none_as_sql(item.expression, _db_type, _error = 'Verb_DELETE(for Postgresql): Joins must contain a statement or a reference to a table.') 
                    result+= ' AS ' + error_on_blank(item.alias,'Verb_DELETE: Joins must have aliases.')
                    if (_db_type != DB_POSTGRESQL): 
                        result+= ' ON ' + none_as_sql(item.conditions,_db_type, _error = 'Verb_DELETE: Joins must have conditions.')
        
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
                        raise Exception('Verb_DELETE: To be able to generalize functionality, only one join is allowed due to Postgresql proprietary DELETE FROM .. USING syntax.')

         
        return result
    
    def append_field_identifier(self, _identifier):
        """Helper function to add Parameter_Identifier instances using just the field names"""
        _ident = Parameter_Identifier(_identifier)

        self.fields.append(_ident)

    
    
      
class Verb_Custom(Parameter_DDL):
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
        super(Verb_Custom, self ).__init__()
        self.sql_mysql = ''
        self.sql_postgresql = ''
        self.sql_oracle = ''
        self.sql_db2 = ''
        self.sql_sqlserver = ''
    
        