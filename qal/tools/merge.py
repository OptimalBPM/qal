"""
Created on Nov 3, 2013

@author: Nicklas Boerjesson
"""

from lxml import etree

from qal.common.resources import Resources, Resource
from qal.common.strings import string_to_bool
from qal.common.mapping import Mapping

from qal.common.transform import perform_transformations, IfEmpty, Replace
from qal.dataset.factory import dataset_from_resource

from qal.common.xml_utils import xml_isnone

class Merge(object):
    mappings = None
    key_fields = None
    source = None
    destination = None
    resources = None
    destination_log_level = None
    post_execute_sql = None
    insert = None
    delete = None
    update = None
    """
    The merge class takes two datasets and merges them together.
    """

    def __init__(self, _xml_node=None):
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
        etree.SubElement(_xml_node, "post_execute_sql").text = self.post_execute_sql
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
                _source_resource.uuid = 'source_uuid'
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
                _dest_resource.uuid = 'dest_uuid'
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
                _new_mapping = Mapping(_xml_node=_curr_mapping)
                self.mappings.append(_new_mapping)
                if _new_mapping.is_key == True:
                    self.key_fields.append(_mapping_idx)
                _mapping_idx += 1
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'field_mappings'-node.")

    def load_mappings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.load_field_mappings_from_xml_node(_xml_node.find("field_mappings"))
        else:
            raise Exception("Merge.load_field_mappings_from_xml_node: Missing 'mappings'-node.")


    def load_settings_from_xml_node(self, _xml_node):
        if _xml_node != None:
            self.insert = string_to_bool(xml_isnone(_xml_node.find("insert")))
            self.update = string_to_bool(xml_isnone(_xml_node.find("update")))
            self.delete = string_to_bool(xml_isnone(_xml_node.find("delete")))
            self.post_execute_sql = xml_isnone(_xml_node.find("post_execute_sql"))
        else:
            raise Exception("Merge.load_settings_from_xml_node: Missing 'settings'-node.")

    def load_from_xml_node(self, _xml_node):

        if _xml_node != None:
            self.load_mappings_from_xml_node(_xml_node.find("mappings"))
            self.load_settings_from_xml_node(_xml_node.find("settings"))
            self.resources = Resources(_resources_node=_xml_node.find("resources"))
            self.source = dataset_from_resource(self.resources.get_resource('source_uuid'))
            self.destination = dataset_from_resource(self.resources.get_resource('dest_uuid'))

        else:
            raise Exception("Merge.load_from_xml_node: \"None\" is not a valid Merge node.")


    def _mappings_to_fields(self, _dataset, _use_dest=True):

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
            if _curr_source_ref is not None and _curr_source_ref != "":
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
                            "Dataset: " + str(self.source.__class__.__name__) + "\n" + \
                            "Error: " + str(e))
        if self.source.field_names is None or len(self.source.field_names) == 0:
            self._mappings_to_fields(self.source, False)




        # Load destination dataset
        try:
            self.destination.load()
        except Exception as e:
            raise Exception("Merge._load_datasets: Failed loading data for destination data set.\n" + \
                            "Check your mappings and other settings.\n" + \
                            "Dataset: " + str(self.destination.__class__.__name__) + "\n" + \
                            "Error: " + str(e))
        if self.destination.field_names is None or len(self.destination.field_names) == 0:
            self._mappings_to_fields(self.destination, True)

        if self.destination_log_level:
            self.destination._log_level = self.destination_log_level


    def _make_shortcuts_readd_keys(self, _mappings, _source_dataset, _destination_dataset):
        """Make a list of which source column index maps to which destination column index and readd keys"""
        _shortcuts = []
        _key_fields = []

        # Make mapping
        for _curr_mapping in _mappings:
            if _curr_mapping.src_reference is not None and _curr_mapping.src_reference != "":
                _src_idx = _source_dataset.field_names.index(_curr_mapping.src_reference)
                if _curr_mapping.is_key:
                    _key_fields.append(_src_idx)
            else:
                _src_idx = None

            _dest_idx = _destination_dataset.field_names.index(_curr_mapping.dest_reference)
            _shortcuts.append([_src_idx, _dest_idx, _curr_mapping])

        return _shortcuts, _key_fields



    def _remap(self, _shortcuts, _key_fields, _source_dataset, _destination_dataset):
        """Create a remapped source data set that has the same data in the same columns as the destination data set.
        Also  and remaps keys."""

        _mapped_source = []
        # Loop all rows in the source data set
        for _curr_row in _source_dataset.data_table:
            # Create an empty row with None-values to fill later
            _curr_mapped = []
            _curr_mapped.extend(None for x in _destination_dataset.field_names)


            # Loop all the shortcuts to remap the data from the source structure into the destinations 
            # structure while applying transformations.
            for _curr_shortcut in _shortcuts:
                # Set the correct field in the destination data set

                if _curr_shortcut[0] is not None:
                    _curr_mapped[_curr_shortcut[1]]  = _curr_row[_curr_shortcut[0]]
                else:
                    # The destination column did not exist in the source? Fill with None for now.
                    _curr_mapped[_curr_shortcut[1]]  = None

            _mapped_source.append(_curr_mapped)

        # Remap keys to match the fields in _mapped_source
        _mapped_keys = []
        for _curr_key_field in _key_fields:
            for _curr_shortcut in _shortcuts:
                if _curr_key_field == _curr_shortcut[0]:
                    _mapped_keys.append(_curr_shortcut[1])

        return _mapped_source, _mapped_keys

    def clear_log(self):
        if self.destination is not None:
            self.destination.clear_log()


    def set_max_identities_for_mappings(self, _shortcuts, _source_dataset, _destination_dataset):
        """Loop through all mapping that uses identity """

        def if_empty(_value):
            if _value == "":
                return 0
            else:
                try:
                    return int(_value)
                except ValueError:
                    return 0

        def find_max(_curr_shortcut):

            if _curr_shortcut[1] is not None:
                _dest_max = max([if_empty(x[_curr_shortcut[1]]) for x in _destination_dataset.data_table])
                if _curr_shortcut[0] is not None:
                    _src_max = max([if_empty(x[_curr_shortcut[0]]) for x in _source_dataset.data_table])
                    if _src_max > _dest_max:
                        return _src_max

                return _dest_max
            else:
                return 0

        _result = {}
        for _curr_shortcut in _shortcuts:
            _curr_mapping = _curr_shortcut[2]
            for _curr_transformation in _curr_mapping.transformations:
                if (isinstance(_curr_transformation, IfEmpty) and
                                _curr_transformation.value is not None and
                                _curr_transformation.value == "::identity::") \
                    or (isinstance(_curr_transformation, Replace) and
                                _curr_transformation.new is not None and
                                _curr_transformation.new.find("::identity::") > 1):

                    _curr_mapping.substitution.set_identity(find_max(_curr_shortcut) + 1)
                    break

    def apply_modifications(self, _shortcuts, _data_table):


        _transformation_shortcuts = []
        # Make a list of columns with transformations
        for _curr_shortcut in _shortcuts:
            _curr_mapping = _curr_shortcut[2]
            if len(_curr_mapping.transformations) > 0:
                _transformation_shortcuts.append(_curr_shortcut)

        for _curr_row in _data_table:
            for _curr_shortcut in _transformation_shortcuts:
                _curr_mapping = _curr_shortcut[2]
                try:
                    _curr_row[_curr_shortcut[1]] = perform_transformations(_curr_row[_curr_shortcut[1]], _curr_mapping.transformations)
                except Exception as e:
                    raise Exception("Transformation:\nError in applying transformations for row " +
                                    str(_data_table.index(_curr_row)) + ", column \"" + str(_curr_mapping.dest_reference) +
                                    "\":\n" + str(e))

        return _data_table

    def execute(self, _commit=True):
        """
        Execute the merge and return the results.
        :param _commit: Actually save the result
        :return: The merged dataset, the destination log, deletes, inserts, updates
        """

        # Load both source and destination data
        self._load_datasets()

        # Make shortcuts between source and destination and add key fields
        _shortcuts, self.key_fields = self._make_shortcuts_readd_keys(self.mappings, self.source, self.destination)

        # Create a remapped version of the source dataset data table to match the column order of the destination
        _mapped_source, _mapped_keys = self._remap( _shortcuts, self.key_fields, self.source, self.destination)

        # If there are any identity references, find and set identity seed to the highest value in
        # either source or destination. If there aren't any, it will start att zero.
        self.set_max_identities_for_mappings(_shortcuts, self.source, self.destination)

        # Apply substitutions, transformations
        _mapped_source = self.apply_modifications(_shortcuts, _mapped_source)

        """Merge the datasets"""
        _merged_dataset, _deletes, _inserts, _updates = self.destination.apply_new_data(_mapped_source, _mapped_keys,
                                                                                        _insert=self.insert,
                                                                                        _update=self.update,
                                                                                        _delete=self.delete,
                                                                                        _commit=_commit)

        if _commit:
            self.destination.save()

            if self.post_execute_sql is not None:
                self.destination.dal.query(self.post_execute_sql)
                self.destination.dal.commit()

        return _merged_dataset, self.destination._log, _deletes, _inserts, _updates

