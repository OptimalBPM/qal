"""
Created on Sep 30, 2013

@author: Nicklas Boerjesson
"""
from qal.dal.dal import DatabaseAbstractionLayer
from qal.dal.types import DB_SQLITE


class ParameterRemotable(object):
    """This class is an auxilliary class for all parameter classes that are remotable. 
    That is, they can fetch their data from, or perform their actions at, a different location than the parent class.
    If they return data, the data will be held in the temporary table, where it can be joined with or otherwise managed.
    """
    """The temporary table name is used by owners to reference the data correctly. It is prefixed by an underscore to 
    not be shown in the external structure.(it is possible that it will be removed)"""
    _temporary_table_name = None
    """The resource object, usually loaded by the JSON importer"""
    _resource = None
    """The classes' DAL connection."""
    _dal = None
    """The nearest parent that is in another database context than self"""
    _top_parent = None
    """This value identifies what context this part of the query resides in"""
    resource_uuid = None

    # TODO: Some of these members should really be public.

    def _bring_into_context(self):
        """The _bring_into_context function checks whether the resource GUID is set and if resources need fetching
        If so, it fetches the data and puts it into a dataset, which is then inserted into the parents context.
        The parent is referenced to as the out-of-context parent.
        """

        print(self.__class__.__name__ + "._bring_into_context: Needs preparing, preparing for resource_uuid: " + str(
            self.resource_uuid))

        if not self._resource:
            raise Exception(self.__class__.__name__ + "._bring_into_context - Error: _resource not cached")

        # Generate random temp table name, max 1 (#) + 8 alphanumeric characters
        import random
        import string

        char_set = string.ascii_lowercase + string.digits
        # Generate
        _tmp_table_name = '#' + ''.join(random.sample(string.ascii_lowercase * 1, 1)) + ''.join(random.sample(char_set * 7, 7))
        from qal.sql.sql import ParameterIdentifier

        if self._resource.type.upper() == 'RDBMS':
            if not self._dal:
                """Make connection to resource defined by the resource_uuid"""
                self._dal = DatabaseAbstractionLayer(_resource=self._resource)
                print(self.__class__.__name__ + "._bring_into_context: Connected to: "+ self._resource.server +
                      ", database name: " + self._resource.databasename + ", resource_uuid: " + str(
                    self._resource.uuid))

            _source_sql = self._generate_sql(self._dal.db_type)
            from qal.sql.sql import ParameterSource

            if isinstance(self, ParameterSource):
                print("is ParameterSource")
                if isinstance(self.expression[0], ParameterIdentifier):

                    _source_sql = 'SELECT * FROM ' + self.expression[0].as_sql(self._dal.db_type)
                    if _tmp_table_name[1] == "#":
                        self.expression[0].identifier = _tmp_table_name[1:len(_tmp_table_name)]
                    else:
                        self.expression[0].identifier = _tmp_table_name

            _data = self._dal.query(_source_sql)
            _field_names = self._dal.field_names
            _field_types = self._dal.field_types

        elif self._resource.type.upper() in ["FLATFILE"]:

            from qal.dataset.flatfile import FlatfileDataset

            self._data_source = FlatfileDataset(_resource=self._resource)

            _data = self._data_source.load()
            _field_names = self._data_source.field_names
            _field_types = ["string"] * len(_field_names)

        elif self._resource.type.upper() in ["FILES"]:

            from qal.dataset.files import FilesDataset

            self._data_source = FilesDataset(_resource=self._resource)

            _data = self._data_source.load()
            _field_names = self._data_source.field_names
            _field_types = self._data_source.field_types

        elif self._resource.type.upper() in ["XPATH"]:
            from qal.dataset.xpath import XpathDataset

            self._data_source = XpathDataset(_resource=self._resource)

            _data = self._data_source.load()
            _field_names = self._data_source.field_names
            _field_types = self._data_source.field_types

        else:
            raise Exception(self.__class__.__name__ + "._bring_into_context - Error: Invalid resource type : " + str(
                self._resource.type))

        """Are we not at the top, i.e. the destination of the data"""
        if self._parent:
            """Does the top parent have a connection? If so, that's where the data should go."""
            if self._top_parent._dal:
                _destination_dal = self._top_parent._dal
            else:
                """If it doesn't, create one to the resource, and use that."""
                _destination_dal = DatabaseAbstractionLayer(_resource=self._top_parent._resource)
                """Also set the parents _dal, since we are using temporary tables, they need to be in the same context."""
                self._top_parent._dal = _destination_dal
        else:
            _destination_dal = self._dal

        """ The sql_macros library is imported locally, to not interfere with the qal.sql.* structure."""
        from qal.sql.macros import copy_to_table

        """Copy the data into the parents' context so the parent can access it."""
        _table_name = copy_to_table(_dal=_destination_dal, _values=_data, _field_names=_field_names,
                                    _field_types=_field_types, _table_name=_tmp_table_name, _create_table=True)

        _ident = ParameterIdentifier(_identifier=_table_name)
        return _ident.as_sql(_destination_dal.db_type)

    def _check_need_prepare(self):
        """
        Checks context, whether a call to _bring_into_context is needed. It is needed if:
        1. If none of the parents have the same resource ID
        2. If no parent has a resourceID

        It is NOT needed if the nearest parent with resource has the same ID.  ???

        """
        # The top should never be brought into context
        if self._parent is None:
            return False
        else:
            _curr_parent = self._parent
            _result = True
            while _curr_parent:
                self._top_parent = _curr_parent
                print(self.__class__.__name__ + "._check_need_prepare: level up" + str(_curr_parent))
                if hasattr(_curr_parent, 'resource_uuid') and _curr_parent.resource_uuid:
                    if self.resource_uuid == _curr_parent.resource_uuid and _curr_parent._dal is not None:
                        self._dal = _curr_parent._dal
                        print(self.__class__.__name__ + "._check_need_prepare: Matching resource uuid found:" + str(
                        _curr_parent.resource_uuid))
                        _result = False

                _curr_parent = _curr_parent._parent



            return _result
