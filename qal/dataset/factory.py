"""
Created on Jan 12, 2014

@author: Nicklas Boerjesson
"""
from qal.dataset.files import FilesDataset
from qal.dataset.flatfile import FlatfileDataset
from qal.dataset.xpath import XpathDataset
from qal.dataset.rdbms import RDBMSDataset
from qal.dataset.spreadsheet import SpreadsheetDataset


def dataset_from_resource(_resource):
    """Create a qal dataset from a resource"""
    if _resource.uuid is None:
        raise Exception("CustomDataset.check_resource: A resource must have an uuid")
    if _resource.type is None:
        raise Exception("CustomDataset.check_resource: A resource must have a resource type, resource id: " + str(_resource.uuid))
    try:
        if _resource.type.upper() == "FLATFILE":
            _ds = FlatfileDataset(_resource=_resource)
        elif _resource.type.upper() == "XPATH":
            _ds = XpathDataset(_resource=_resource)
        elif _resource.type.upper() == "RDBMS":
            _ds = RDBMSDataset(_resource=_resource)
        elif _resource.type.upper() == "SPREADSHEET":
            _ds = SpreadsheetDataset(_resource=_resource)
        elif _resource.type.upper() == "FILES":
            _ds = FilesDataset(_resource=_resource)
        else:
            raise Exception("qal.dataset.factory.dataset_from_resource: Unsupported source resource type: " + str(
                _resource.type.upper()) + " for resource id: " + str(_resource.uuid))
    except Exception as e:
        raise Exception(
            "qal.dataset.factory.dataset_from_resource: Failed loading resource for " + str(_resource.type) + ".\n" +
            "Resource: " + str(_resource.caption) + "(" + str(_resource.uuid) + ")\n" +
            "Error: \n" + str(e))

    return _ds


if __name__ == '__main__':
    pass