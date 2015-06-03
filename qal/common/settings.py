"""
Contains functionality for reading settings from ini-files

:copyright: Copyright 2010-2014 by Nicklas Boerjesson
:license: BSD, see LICENSE for details.
"""

import configparser


class BPMSettings(object):
    """This class is responsible for reading settings from ini-files and holding them in memory."""

    parser = None

    def reload(self, filename):
        """Reload all information"""
        self.parser.read(filename)

    def get(self, _section, _option, _default=None):
        """Get a certain option"""
        if self.parser.has_section(_section):
            if not self.parser.has_option(_section, _option):
                return _default
            else:
                return self.parser.get(_section, _option)
        else:
            return _default

    def __init__(self, filename):
        """Constructor"""
        self.parser = configparser.ConfigParser()
        if filename != '':
            # TODO: Add better config support.
            self.reload(filename)
