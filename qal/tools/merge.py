"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""


from qal.sql.sql_macros import copy_to_table 

class Merge(object):
    """
    The merge class takes two datasets and merges them together.
    """


    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        if _xml_node:
            self.load_from_xml_node(_xml_node)
        
        
    def load_from_xml_node(self, _xml_node):
        pass
    
    def _generate_deletes(self,_table_name, _id_columns, _delete_list):
        """Generates a Verb_DELETE instance populated with the indata"""
        
        """Create SELECTs and put them in a UNION:ed set"""
        
        """Put the set in an insert and add joins on the ID columns """
            
        #_source = Parameter_Source("""_expression = None, _conditions = None, _alias = '', _join_type = None""")
        #_deletes = Verb_DELETE("""_sources = None, _operator = None""")
        #_deletes.sources.append(_source)
        #return _deletes
        pass
    
    def _generate_inserts(self, _table_name, _id_columns, _delete_list):
        """Generates a Verb_INSERT instance populated with the indata"""
        #copy_to_table
        pass
    
    def _generate_updates(self, _table_name, _id_columns, _delete_list):
        """Generates DELETE and INSERT instances populated with the indata
        @todo: Obviously a VERB_UPDATE will be better, implement that when test servers are back up."""     
        pass        