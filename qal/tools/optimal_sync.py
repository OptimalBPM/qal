#!/usr/bin/env python3


"""
    ************
    Optimal Sync
    ************
    
    Optimal Sync is a tool that helps with creating and scripting replication jobs.
    Is is a stand-alone tool facilitates data replication and merging with some basic transformation features. 
    It uses a definition file and an SQL compliant database for the Transformation.
    
    :copyright: Copyright 2010-2015 by Nicklas Boerjesson
    :license: BSD, see LICENSE for details.
"""

# Version and release information used by Sphinx for documentation and setuptools for package generation.

__version__ = '0.9'
__release__ = '0.9.0'
__copyright__ = '2010-2014, Nicklas Boerjesson'

import json
from qal.transformation.merge import Merge

import sys, getopt
sys.path.append("../../")


from qal.tools.gui.main_tk_replicator import ReplicatorMain


_help_msg = """
Usage: optimal_sync.py [OPTION]... -d [DEFINITION FILE]... -l [LOG LEVEL]
Merge data in using a definition file.

This stand-alone tool facilitates data replication and merging with some basic transformation features. 
It uses a definition file and an SQL compliant database for the Transformation.

    -d, --definitionfile    Provide the path to an JSON definition file to describe the Transformation
    -e,                     Initialize editor
    -l, --log_level         Log level
    
    --help     display this help and exit
    --version  output version information and exit

Always back up your data!

"""


def init(_definitionfile):
    """Loads the definition file and extracts settings"""
    pass

def main():
    """Main program function"""
    
    _definitionfile = None
    _edit = None
    _log_level = None
    
    """Parse arguments"""
    try:
        _opts = None
        _args = None
        _opts, _args = getopt.getopt(sys.argv[1:],"ed:l:",["help","version","definitionfile=*.json", "log_level="])
    except getopt.GetoptError as err:
        print (str(err)+ "\n" +_help_msg + "\n Arguments: " + str(_args))
        sys.exit(2)

    if _opts:
        for _opt, _arg in _opts:

            if _opt == '-e':
                # Do not run the Transformation, start the editor instead
                _edit = True

            elif _opt in ("-d", "--definitionfile"):
                _definitionfile = _arg
            elif _opt in ("-l", "--log_level"):
                _log_level = _arg
            elif _opt == '--help':
                print(_help_msg)
                sys.exit()
            elif _opt == '--version':
                print(__version__)
                sys.exit()

        if _definitionfile:
            """Load merge"""
            print(_definitionfile)
            with open(_definitionfile, "r") as f:
                _merge = Merge(_json= json.load(f))
        else:
            """Create empty"""
            _merge = Merge()

        if _log_level:
            _merge.destination_log_level = int(_log_level)

        if _edit or not _definitionfile:
            """If the users wants to edit or haven't specified a definition file, start the editor"""
            # Bring up the GUI
            ReplicatorMain(_merge=_merge, _filename=_definitionfile)
        else:
            """Otherwise execute the transformation"""
            _dataset, _log, _deletes, _inserts, _updates =_merge.execute()
            print(str(_log))
    else:
        print("Error: No options provided.\n" + _help_msg)


if __name__ == '__main__':
    main()