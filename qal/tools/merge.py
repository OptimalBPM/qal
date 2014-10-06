"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""

from lxml import etree

from qal.common.resources import Resources, Resource
from qal.common.strings import string_to_bool, empty_when_none
from qal.dataset.xpath import XpathDataset
from qal.tools.transform import make_transformation_array_from_xml_node, make_transformations_xml_node, perform_transformations
from qal.dataset.factory import dataset_from_resource


def isnone( _node):
    if _node == None or _node.text == None:
        return None
    else:
        return _node.text  
    
class Mapping(object):
    is_key = None 
    src_reference = None
    src_datatype = None
    dest_reference = None
    transformations = None
    
    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        self.transformations = []
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)



    def load_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.is_key = string_to_bool(isnone(_xml_node.find("is_key")))
            self.src_reference = isnone(_xml_node.find("src_reference"))
            self.src_datatype = isnone(_xml_node.find("src_datatype"))
            self.dest_reference = isnone(_xml_node.find("dest_reference"))
            self.transformations = make_transformation_array_from_xml_node(_xml_node.find("transformations"))
         
            
    def as_xml_node(self):

        _xml_node = etree.Element("field_mapping")        
        etree.SubElement(_xml_node, "is_key").text = str(self.is_key)
        etree.SubElement(_xml_node, "src_reference").text = empty_when_none(self.src_reference)
        etree.SubElement(_xml_node, "src_datatype").text = empty_when_none(self.src_datatype)
        _xml_node.append(make_transformations_xml_node(self.transformations))
        etree.SubElement(_xml_node, "dest_reference").text = empty_when_none(self.dest_reference)

        

        return _xml_node

class Merge(object):
    mappings = None
    key_fields = None
    source = None
    destination = None
    resources = None
    destination_log_level = None
    insert = None
    delete = None
    update = None
    """
    The merge class takes two datasets and merges them together.
    """


    def __init__(self, _xml_node = None):
        """
        Constructor
        """
        self.mappings = []
        self.key_fields = []
        if _xml_node != None:
            self.load_from_xml_node(_xml_node)

    
    def _field_mappings_as_xml_node(self):
        _xml_node = etree.Element("field_mappings")
        for _curr_mapping in self.mappings:
            _xml_node.append(_curr_mapping.as_xml_node())
        
        return _xml_node    


    def _mappings_as_xml_node(self):
        _xml_node = etree.Element("mappings")
        _xml_node.append(self._field_mappings_as_xml_node())
        return _xml_node
    
    def _settings_as_xml_node(self):
        _xml_node = etree.Element("settings")
        etree.SubElement(_xml_node, "insert").text = str(self.insert)
        etree.SubElement(_xml_node, "update").text = str(self.update)
        etree.SubElement(_xml_node, "delete").text = str(self.delete)
        return _xml_node    
    
    
    def as_xml_node(self):
        _xml_node = etree.Element('merge')
        _xml_node.append(self._mappings_as_xml_node())
        _xml_node.append(self._settings_as_xml_node())

        if self.resources is None:
            self.resources = Resources()

        if self.source is not None:
            try:
                _source_resource = self.resources.get_resource('source_uuid')
            except:
                _source_resource = None
            if _source_resource is None:
                _source_resource = Resource()
                _source_resource.uuid='source_uuid'
                _source_resource.caption = "source"
                self.resources.local_resources['source_uuid'] = _source_resource
            self.source.write_resource_settings(_source_resource)
        if self.destination is not None:
            try:
                _dest_resource = self.resources.get_resource('dest_uuid')
            except:
                _dest_resource = None
            if _dest_resource is None:
                _dest_resource = Resource()
                _dest_resource.uuid='dest_uuid'
                _dest_resource.caption = "destination"
                self.resources.local_resources['dest_uuid'] = _dest_resource

            self.destination.write_resource_settings(_dest_resource)

        """ TODO: Handle remotely defined resources properly. This way, they are included in the XML, which perhaps isn't
            right. Perhaps get_resource should return a tuple with a source parameter.
        """

        if self.source is not None or self.destination is not None:
            # If either aren't set, anything in resources are likely to be residuals from earlier.
            # However, there could be old resources left in one of them.
            _xml_node.append(self.resources.as_xml_node())
        return _xml_node        
        
    def load_field_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            _mapping_idx = 0
            for _curr_mapping in _xml_node.findall("field_mapping"):
                _new_mapping = Mapping(_xml_node = _curr_mapping)
                self.mappings.append(_new_mapping)
                if _new_mapping.is_key == True:
                    self.key_fields.append(_mapping_idx)  
                _mapping_idx+= 1              
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'field_mappings'-node.")   

    def load_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.load_field_mappings_from_xml_node(_xml_node.find("field_mappings"))
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'mappings'-node.")   
        

    def load_settings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.insert = string_to_bool(isnone(_xml_node.find("insert")))
            self.update = string_to_bool(isnone(_xml_node.find("update")))
            self.delete = string_to_bool(isnone(_xml_node.find("delete")))
        else:
            raise Exception("Merge.load_settings_from_xml_node: Missing 'settings'-node.")     
               
    def load_from_xml_node(self, _xml_node):

        if _xml_node != None:           
            self.load_mappings_from_xml_node(_xml_node.find("mappings"))
            self.load_settings_from_xml_node(_xml_node.find("settings"))
            self.resources = Resources(_resources_node= _xml_node.find("resources"))
            self.source = dataset_from_resource(self.resources.get_resource('source_uuid'))
            self.destination = dataset_from_resource(self.resources.get_resource('dest_uuid'))

        else:
            raise Exception("Merge.load_from_xml_node: \"None\" is not a valid Merge node.")                  


    def _mappings_to_fields(self, _dataset,  _use_dest = True):

        _dataset.field_names = []
        _dataset.field_types = []
        self.key_fields = []
        if hasattr(_dataset, "field_xpaths"):
            _dataset.field_xpaths = []

        for _curr_mapping_idx in range(len(self.mappings)):
            _curr_mapping = self.mappings[_curr_mapping_idx]
            if _use_dest:
                _curr_source_ref = _curr_mapping.dest_reference
            else:
                _curr_source_ref = _curr_mapping.src_reference
            _dataset.field_names.append(_curr_source_ref)
            if hasattr(_dataset, "filename"):
                _dataset.field_types.append("string")
                if hasattr(_dataset, "field_xpaths"):
                    _dataset.field_xpaths.append(_curr_source_ref)

    def _load_datasets(self):
        
        # Load source_dataset
        try:
            self.source.load()
        except Exception as e:
            raise Exception("Merge._load_datasets: Failed loading data for source data set.\n" + \
                            "Check your mappings and other settings.\n" + \
                            "Dataset: " + str(self.source.__class__.__name__) + "\n"+ \
                            "Error: " + str(e))
        if self.source.field_names is None or len(self.source.field_names) == 0:
            self._mappings_to_fields(self.source, False)
        
        # Load destination dataset
        try:
            self.destination.load()
        except Exception as e:
            raise Exception("Merge._load_datasets: Failed loading data for destination data set.\n" + \
                            "Check your mappings and other settings.\n" + \
                            "Dataset: " + str(self.destination.__class__.__name__) + "\n"+ \
                            "Error: " + str(e))
        if self.destination.field_names is None or len(self.destination.field_names) == 0:
            self._mappings_to_fields(self.destination, True)

        if self.destination_log_level:
            self.destination._log_level = self.destination_log_level

             
    def _make_shortcuts_readd_keys(self):
        """Make a list of which source column index maps to which destination column index and readd keys"""
        _shortcuts = []
        self.key_fields = []

        # Make mapping
        for _curr_mapping in self.mappings:
            _src_idx  = self.source.field_names.index(_curr_mapping.src_reference)
            if _curr_mapping.is_key:
                self.key_fields.append(_src_idx)
            _dest_idx = self.destination.field_names.index(_curr_mapping.dest_reference)
            _shortcuts.append([_src_idx, _dest_idx, _curr_mapping])
            
        return _shortcuts
         

    def _remap_and_transform(self):
        """Create a remapped source data set that has the same data in the same columns as the destination data set.
        Also applies transformations and remaps keys."""
        _shortcuts = self._make_shortcuts_readd_keys()

        _mapped_source = []
        # Loop all rows in the source data set
        for _curr_idx in range(0, len(self.source.data_table)):
            # Create an empty row with None-values to fill later
            _curr_mapped = []
            _curr_mapped.extend(None for x in self.destination.field_names)
            
            _curr_row = self.source.data_table[_curr_idx]
            # Loop all the shortcuts to remap the data from the source structure into the destinations 
            # structure while applying transformations.
            for _curr_shortcut in _shortcuts:
                try:
                    _value = perform_transformations(_curr_row[_curr_shortcut[0]], _curr_shortcut[2].transformations)
                except Exception as e:
                    raise Exception("Merge._remap_and_transform:\nError in applying transformations for row " + 
                                    str(_curr_idx) + ", column \"" + self.destination.field_names[_curr_shortcut[0]] + 
                                    "\":\n" + str(e))
                # Set the correct field in the destination data set
                _curr_mapped[_curr_shortcut[1]] = _value
            _mapped_source.append(_curr_mapped)

        # Remap keys to match the fields in _mapped_source
        _mapped_keys = []
        for _curr_key_fields_idx in range(len(self.key_fields)):
            for _curr_shortcut in _shortcuts:
                if self.key_fields[_curr_key_fields_idx]==_curr_shortcut[0]:
                    _mapped_keys.append(_curr_shortcut[1])

        return _mapped_source, _mapped_keys
           
    def execute(self, _commit = True):
        """
        Execute the merge and return the results.
        :param _commit: Actually save the result
        :return: The merged dataset, the destination log, deletes, inserts, updates
        """
        
        # Load resources
        self._load_datasets()
        
        # Create a remapped source dataset, perform transformations
        _mapped_source, _mapped_keys = self._remap_and_transform()
        

        """Merge the datasets"""
        _merged_dataset, _deletes, _inserts, _updates = self.destination.apply_new_data(_mapped_source, _mapped_keys, _insert=self.insert, _update = self.update, _delete=self.delete, _commit=_commit)

        if _commit:
            self.destination.save()
        
        return _merged_dataset, self.destination._log, _deletes, _inserts, _updates

