"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""



from qal.common.resources import Resources
from qal.tools.transform import make_transformation_array_from_xml_node, make_transformations_xml_node, perform_transformations
from qal.tools.diff import compare
from qal.dataset.flatfile import Flatfile_Dataset
from qal.dataset.xpath import XPath_Dataset
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
    src_datatype = None
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
            self.src_datatype = isnone(_xml_node.find("src_datatype"))
            self.src_cast_to = isnone(_xml_node.find("src_cast_to"))
            self.result_cast_to = isnone(_xml_node.find("result_cast_to"))
            self.dest_column = isnone(_xml_node.find("dest_column"))
            self.transformations = make_transformation_array_from_xml_node(_xml_node.find("transformations"))
         
            
    def as_xml_node(self):

        _xml_node = etree.Element("field_mapping")        
        etree.SubElement(_xml_node, "is_key").text = self.is_key
        etree.SubElement(_xml_node, "src_column").text = self.src_column
        etree.SubElement(_xml_node, "src_datatype").text = self.src_datatype
        etree.SubElement(_xml_node, "src_cast_to").text = self.src_cast_to
        _xml_node.append(make_transformations_xml_node(self.transformations))
        etree.SubElement(_xml_node, "result_cast_to").text = self.result_cast_to
        etree.SubElement(_xml_node, "dest_column").text = self.dest_column

        

        return _xml_node

class Merge(object):
    mappings = []
    key_fields = []
    source_table = None
    source_field_types = None
    dest_table = None
    dest_field_types = None
    resources = None
    dest_dataset_log_level = None
    
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


    
    def _extract_data_columns_from_diff_row(self, _field_indexes, _diff_row):    
        """Extracts columns specified in _field_indexes from _diff_list"""
        _result = []

        for _curr_field in _field_indexes:
            _result.append(_diff_row[2][_curr_field])
        return _result        
    
    
    
    def loaded_dataset_from_resource(self, _resource):
        """Get a dataset from a file resource"""
        if _resource.type.upper() == "FLATFILE":
            _ffds =  Flatfile_Dataset(_resource = _resource)
            try:
                _ffds.load()
            except IOError:
                print("loaded_dataset_from_resource: Flatfile_Dataset - File node found, will write to new file.")
            return _ffds, _ffds.field_names
            
        elif _resource.type.upper() == "XPATH":
            _ffds =  XPath_Dataset(_resource = _resource)
            try:
                _ffds.load()
            except IOError:
                print("loaded_dataset_from_resource: XPath_Dataset - File node found, will write to new file.")
            return _ffds, _ffds.field_names
        else: 
            raise Exception("loaded_dataset_from_resource: Unsupported source resource type: " + str(_resource.type.upper()))
        
    def load_rdbms_dataset_from_resource(self, _resource, _table_name):
        """Query all values from a table from a RDBMS resource"""
        _dal = Database_Abstraction_Layer(_resource = _resource)
        return _dal.query(select_all_skeleton(_table_name).as_sql(_dal.db_type)), _dal.field_names, _dal.field_types
  
    
    def _load_resources(self):
        # Load source resource
        self.source_resource = self.resources.get_resource('source_uuid')
        # Get source data set
        if self.source_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX", "XPATH"]:
            self.source_dataset, self.source_field_names = self.loaded_dataset_from_resource(self.source_resource)
        elif self.source_resource.type.upper() in ["RDBMS"]:
            self.source_dataset, self.source_field_names, self.source_field_types = self.load_rdbms_dataset_from_resource(self.source_resource, self.source_table)
        else: 
            raise Exception("execute: Invalid source resource type: " + str(self.source_resource.type.upper()))

        # Load destination resource
        self.dest_resource = self.resources.get_resource('dest_uuid')
        # Get destination data set
        if self.dest_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX", "XPATH"]:
            self.dest_dataset, self.dest_field_names = self.loaded_dataset_from_resource(self.dest_resource)
        elif self.dest_resource.type.upper() in ["RDBMS"]:
            self.dest_dataset, self.dest_field_names, self.dest_field_types = self.load_rdbms_dataset_from_resource(self.dest_resource, self.dest_table)
        else: 
            raise Exception("execute: Invalid destination resource type:" + str(self.dest_resource.type.upper()))
        
        if self.dest_dataset_log_level:
            self.dest_dataset._log_level = self.dest_dataset_log_level
             
    def _make_shortcuts(self):
        """Make a list of which source column index maps to which destination column index""" 
        _shortcuts = []       

        # Make mapping
        for _curr_mapping in self.mappings:
            _src_idx  = self.source_field_names.index(_curr_mapping.src_column)
            _dest_idx = self.dest_field_names.index(_curr_mapping.dest_column)
            _shortcuts.append([_src_idx, _dest_idx, _curr_mapping])
            
        return _shortcuts
         

    def _remap_and_transform(self):
        """Create a remapped source data set that has the same data in the same columns as the destination data set.
        Also applies transformations."""
        _shortcuts = self._make_shortcuts()

        _mapped_source = []
        # Loop all rows in the source data set
        for _curr_idx in range(0, len(self.source_dataset.data_table)):
            # Create an empty row with None-values to fill later
            _curr_mapped = []
            _curr_mapped.extend(None for x in self.dest_field_names)
            
            _curr_row = self.source_dataset.data_table[_curr_idx]
            # Loop all the shortcuts to remap the data from the source structure into the destinations 
            # structure while applying transformations.
            for _curr_shortcut in _shortcuts:
                try:
                    _value = perform_transformations(_curr_row[_curr_shortcut[0]], _curr_shortcut[2].transformations)
                except Exception as e:
                    raise Exception("Merge._remap_and_transform:\nError in applying transformations for row " + 
                                    str(_curr_idx) + ", column \"" + self.dest_field_names[_curr_shortcut[0]] + 
                                    "\":\n" + str(e))
                # Set the correct field in the destination data set
                _curr_mapped[_curr_shortcut[1]] = _value
            _mapped_source.append(_curr_mapped)
        
        return _mapped_source
           
    def execute(self):
        
        # Load resources
        self._load_resources()
        
        # Create a remapped source dataset, perform transformations
        _mapped_source = self._remap_and_transform()
        

        """Merge the datasets"""
        if self.dest_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX"]:
            _merged_dataset = self.dest_dataset.apply_new_data(_mapped_source, self.key_fields)
        self.dest_dataset.save()
        

        return _merged_dataset

        """        
        if self.dest_resource.type.upper() in ["CUSTOM", "FLATFILE", "MATRIX"]:
            
            # Save to relevant data format
                   
                
                    
        else:
            self._rdbms_apply_updates(_update)
            self._rdbms_apply_deletes(_delete)
            self._rdbms_apply_inserts(_insert)
            
            #return self.load_rdbms_dataset_from_resource(self.dest_resource, self.dest_table)
        
        
        # Merge updates into destination data set
        print("_update : " + str(_update))
        
        # Merge inserts into destination data set
        print("_insert : " + str(_insert))
        
        # Merge delete into destination data set 
        print("_delete : " + str(_delete))
        """
        
        
    def write_result_csv(self, _file_output = None):
        """ if _file_output:
            self.
        f = open('resources/csv_out.xml', w)
        f.write(_result)"""