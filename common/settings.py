'''
Created on May 8, 2010

@author: Nicklas Boerjesson
'''

import configparser

class UBPMSettings(object):
    global Parser
    
    '''
    This class is responsible for reading settings from the ini-files and holding them in memory.
    '''
    
    def reload(self,filename):
        self.Parser.read(filename)
        
    def get(self, _section, _option, _default = None):
        if (_default != None) and (not self.Parser.has_option(_section, _option)):
            return _default
        else:
            return self.Parser.get(_section, _option)

    
    def __init__(self, filename):
        '''
        Constructor
        '''
        self.Parser = configparser.ConfigParser()
        if (filename != ''):
            # TODO: Add better config support.
            self.reload(filename)
            
        