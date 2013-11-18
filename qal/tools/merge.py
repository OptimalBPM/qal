"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""


from qal.sql.sql_macros import copy_to_table
from qal.common.resources import Resources
from qal.tools.transform import make_transformation_array_from_xml_node, make_transformations_xml_node
from qal.tools.diff import compare
from qal.nosql.flatfile import Flatfile_Dataset
from qal.nosql.xpath import XPath_Dataset
from qal.sql.sql_macros import select_all_skeleton
from qal.dal.dal import Database_Abstraction_Layer
from lxml import etree

def isnone( _node):
    if _node == None or _node.text == None:
        return None
    else:
        return _node.text  
    
class Field_Mapping(object):
    is_key = None 
    src_column = None
    src_data_type = None
    src_cast_to = None
    result_cast_to = None
    dest_column = None
    transformations = []
    
    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)
        else:
            raise Exception("Field_Mapping.load_from_xml_node: \"None\" is not a valid Merge node.")     
    
    def load_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.is_key = isnone(_xml_node.find("is_key"))
            self.src_column = isnone(_xml_node.find("src_column"))
            self.src_data_type = isnone(_xml_node.find("src_data_type"))
            self.src_cast_to = isnone(_xml_node.find("src_cast_to"))
            self.result_cast_to = isnone(_xml_node.find("result_cast_to"))
            self.dest_column = isnone(_xml_node.find("dest_column"))
            self.transformations = make_transformation_array_from_xml_node(_xml_node.find("transformations"))
         
            
    def as_xml_node(self):

        _xml_node = etree.Element("field_mapping")        
        etree.SubElement(_xml_node, "is_key").text = self.is_key
        etree.SubElement(_xml_node, "src_column").text = self.src_column
        etree.SubElement(_xml_node, "src_data_type").text = self.src_data_type
        etree.SubElement(_xml_node, "src_cast_to").text = self.src_cast_to
        _xml_node.append(make_transformations_xml_node(self.transformations))
        etree.SubElement(_xml_node, "result_cast_to").text = self.result_cast_to
        etree.SubElement(_xml_node, "dest_column").text = self.dest_column

        

        return _xml_node

class Merge(object):
    mappings = []
    key_fields = []
    source_table = None
    dest_table = None
    resources = None
    """
    The merge class takes two datasets and merges them together.
    """


    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)
        else:
            raise Exception("Merge.load_from_xml_node: \"None\" is not a valid Merge node.")                  
    
    
    def _field_mappings_as_xml_node(self):
        _xml_node = etree.Element("field_mappings")
        for _curr_mapping in self.mappings:
            _xml_node.append(_curr_mapping.as_xml_node())
        
        return _xml_node    

    def _table_mappings_as_xml_node(self):
        _xml_node = etree.Element("table_mappings")
        etree.SubElement(_xml_node, "source_table").text = self.source_table
        etree.SubElement(_xml_node, "dest_table").text = self.dest_table
        
        return _xml_node    
    
    def _mappings_as_xml_node(self):
        _xml_node = etree.Element("mappings")
        _xml_node.append(self._field_mappings_as_xml_node())
        _xml_node.append(self._table_mappings_as_xml_node())
        return _xml_node
    
    def _settings_as_xml_node(self):
        _xml_node = etree.Element("settings")
        etree.SubElement(_xml_node, "insert").text = self.insert
        etree.SubElement(_xml_node, "update").text = self.update
        etree.SubElement(_xml_node, "delete").text = self.delete
        return _xml_node    
    
    
    def as_xml_node(self):
        _xml_node = etree.Element('merge')
        _xml_node.append(self._mappings_as_xml_node())
        _xml_node.append(self._settings_as_xml_node())
        _xml_node.append(self.resources.as_xml_node())
        return _xml_node        
        
    def load_field_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            _mapping_idx = 0
            for _curr_mapping in _xml_node.findall("field_mapping"):
                _new_mapping = Field_Mapping(_xml_node = _curr_mapping)
                self.mappings.append(_new_mapping)
                if _new_mapping.is_key == "True":
                    self.key_fields.append(_mapping_idx)  
                _mapping_idx+= 1              
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'field_mappings'-node.")   

    def load_table_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.source_table = isnone(_xml_node.find("source_table"))
            self.dest_table = isnone(_xml_node.find("dest_table"))
        else:
            raise Exception("Merge.load_table_mappings_from_xml_node: Missing 'table_mappings'-node.")   

    def load_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.load_field_mappings_from_xml_node(_xml_node.find("field_mappings"))
            self.load_table_mappings_from_xml_node(_xml_node.find("table_mappings"))
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'mappings'-node.")   
        

    def load_settings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.insert = isnone(_xml_node.find("insert"))
            self.update = isnone(_xml_node.find("update"))
            self.delete = isnone(_xml_node.find("delete"))     
        else:
            raise Exception("Merge.load_settings_from_xml_node: Missing 'settings'-node.")     
               
    def load_from_xml_node(self, _xml_node):

        if _xml_node != None:           
            self.load_mappings_from_xml_node(_xml_node.find("mappings"))
            self.load_settings_from_xml_node(_xml_node.find("settings"))
            self.resources = Resources(_resources_node= _xml_node.find("resources"))
        else:
            raise Exception("Merge.load_from_xml_node: \"None\" is not a valid Merge node.")                  
    
    def _rdbms_generate_deletes(self, _delete_list):
        """Generates a Verb_DELETE instance populated with the indata"""
        
        """Create SELECTs and put them in a UNION:ed set"""
        
        """Put the set in an insert and add joins on the ID columns """
            
        #_source = Parameter_Source("""_expression = None, _conditions = None, _alias = '', _join_type = None""")
        #_deletes = Verb_DELETE("""_sources = None, _operator = None""")
        #_deletes.sources.append(_source)
        #return _deletes
        pass
    
    def _rdbms_generate_inserts(self, _insert_list):
        """Generates a Verb_INSERT instance populated with the indata"""
        #copy_to_table
        pass
    
    def _rdbms_generate_updates(self, _delete_list):
        """Generates DELETE and INSERT instances populated with the indata
        @todo: Obviously a VERB_UPDATE will be better, implement that when test servers are back up."""     
        pass    
    
    def load_file_dataset_from_resource(self, _resource):
        if _resource.type.upper() == "FLATFILE":
            return Flatfile_Dataset(_resource = _resource).load()
        elif _resource.type.upper() == "XPATH":
            return Flatfile_Dataset(_resource = _resource).load()
        else: 
            raise Exception("load_file_dataset_from_resource: Unsupported source resource type: " + str(_resource.type.upper()))
        
    def load_rdbms_dataset_from_resource(self, _resource, _table_name):
        _dal = Database_Abstraction_Layer(_resource = _resource)
        return _dal.query(select_all_skeleton(_table_name).as_sql(_dal.db_type))
    
    def _apply_merge_to_dataset(self, _insert, _update, _delete, _sorted_dest):
        
        #print("_insert: " + str(_insert))
        #print("Before apply: " + str(_sorted_dest))
        
        _do_delete = str(self.delete).lower() == "true"
        _do_update = str(self.update).lower() == "true"
        _do_insert = str(self.insert).lower() == "true"
        
        _insert_idx = 0
        _delete_idx = 0
        _update_idx = 0
        # Loop the sorted destination dataset and apply changes
        for _curr_row_idx in range(0, len(_sorted_dest)):
            _actual_row_idx = _curr_row_idx - _delete_idx + _insert_idx
            # print("_actual_row_idx = _curr_row_idx - _delete_idx + _insert_idx = " + 
            #      str(_curr_row_idx) + " - " +str(_delete_idx) + " + " + str(_insert_idx))
            if _do_delete and _delete_idx < len(_delete):
                if _delete[_delete_idx][1] == _curr_row_idx:
                    #print("deleting row " + str(_delete[_delete_idx][1])) 
                    _sorted_dest.pop(_actual_row_idx)
                    _delete_idx+=1
        
            if _do_update and _update_idx < len(_update):
                if _update[_update_idx][1] == _curr_row_idx:
                    #print("updating row " + str(_update[_update_idx][1])) 
                    _sorted_dest[_actual_row_idx] = _update[_update_idx][2]
                    _update_idx+=1
                    
        
            if _do_insert:
                
                # TODO: This while should insert these in reverse instead.
                while _insert_idx < len(_insert) and _insert[_insert_idx][1] == _curr_row_idx:
                    #print("inserting row " + str(_insert[_insert_idx][1])) 
                    _sorted_dest.insert(_actual_row_idx,_insert[_insert_idx][2])
                    _insert_idx+=1
        
        #print("After apply:  " + str(_sorted_dest))            
        return _sorted_dest        
                    
                
                
    
    def _xpath_generate_updates(self, _update):
        pass
    def _xpath_generate_deletes(self, _delete):
        pass
    def _xpath_generate_inserts(self, _insert):
        pass    
    
    def execute(self):
        """Merge the datasets"""
        
        # Load source resource
        _source_resource = self.resources.get_resource('source_uuid')
        # Get source data set
        if _source_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX", "XPATH"]:
            _source_dataset = self.load_file_dataset_from_resource(_source_resource)
        elif _source_resource.type.upper() in ["RDBMS"]:
            _source_dataset = self.load_rdbms_dataset_from_resource(_source_resource, self.source_table)
        else: 
            raise Exception("execute: Invalid source resource type: " + str(_source_resource.type.upper()))

        # Load destination resource
        _dest_resource = self.resources.get_resource('dest_uuid')
        # Get destination data set
        if _dest_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX", "XPATH"]:
            _dest_dataset = self.load_file_dataset_from_resource(_dest_resource)
        elif _dest_resource.type.upper() in ["RDBMS"]:
            _dest_dataset = self.load_rdbms_dataset_from_resource(_dest_resource, self.dest_table)
        else: 
            raise Exception("execute: Invalid destination resource type:" + str(_dest_resource.type.upper()))
        
        # Compare data sets.
        _delete, _insert, _update, _dest_sorted = compare(
                                                          _left = _source_dataset, 
                                                          _right =_dest_dataset, 
                                                          _key_columns = self.key_fields, 
                                                          _full = True)
        
        if _dest_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX"]:
            pass
            #print("delete "  + str(_delete))
            # Update first, insert can then insert at position instead of looking up.
            return self._apply_merge_to_dataset(_insert, _update, _delete, _dest_sorted)            
        elif _dest_resource.type.upper() in ["XPATH"]:
            self._xpath_generate_updates(_update)
            self._xpath_generate_deletes(_delete)
            self._xpath_generate_inserts(_insert)        
        else:
            self._rdbms_generate_updates(_update)
            self._rdbms_generate_deletes(_delete)
            self._rdbms_generate_inserts(_insert)
        
        
        # Merge updates into destination data set
        print("_update : " + str(_update))
        
        # Merge inserts into destination data set
        print("_insert : " + str(_insert))
        
        # Merge delete into destination data set 
        print("_delete : " + str(_delete))
        

    def write_result_csv(self, _file_output = None):
        """ if _file_output:
            self.
        f = open('resources/csv_out.xml', w)
        f.write(_result)"""