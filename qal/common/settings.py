'''
Contains functionality for reading settings from ini-files

:copyright: Copyright 2010-2013 by Nicklas Börjesson
:license: BSD, see LICENSE for details. 
'''

import configparser

class UBPMSettings(object):
    """This class is responsible for reading settings from ini-files and holding them in memory."""
    
    global Parser
    
    def reload(self,filename):
        """Reload all information"""
        self.Parser.read(filename)
        
    def get(self, _section, _option, _default = None):
        """Get a certain option"""
        if (_default != None) and (not self.Parser.has_option(_section, _option)):
            return _default
        else:
            return self.Parser.get(_section, _option)

    
    def __init__(self, filename):
        """Constructor"""
        self.Parser = configparser.ConfigParser()
        if (filename != ''):
            # TODO: Add better config support.
            self.reload(filename)
            
        