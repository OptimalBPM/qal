"""
Created on Jan 12, 2014

@author: Nicklas Boerjesson
"""
from qal.dataset.flatfile import Flatfile_Dataset
from qal.dataset.xpath import XPath_Dataset
from qal.dataset.rdbms import RDBMS_Dataset
from qal.dataset.spreadsheet import Spreadsheet_Dataset


def dataset_from_resource(_resource):
    """Create a qal dataset from a resource"""
    try:
        if _resource.type.upper() == "FLATFILE":
            _ds =  Flatfile_Dataset(_resource = _resource)
        elif _resource.type.upper() == "XPATH":
            _ds =  XPath_Dataset(_resource = _resource)
        elif _resource.type.upper() == "RDBMS":
            _ds =  RDBMS_Dataset(_resource = _resource)
        elif _resource.type.upper() == "SPREADSHEET":
            _ds =  Spreadsheet_Dataset(_resource = _resource)
        else: 
            raise Exception("qal.dataset.factory.dataset_from_resource: Unsupported source resource type: " + str(_resource.type.upper()))
    except Exception as e:
        raise Exception("qal.dataset.factory.dataset_from_resource: Failed loading resource for " + _ds.__class__.__name__ + ".\n" + \
                        "Resource: " + str(_resource.caption)+ "(" + str(_resource.uuid) + ")\n"+ \
                        "Error: " + str(e))
    return _ds    

if __name__ == '__main__':
    pass