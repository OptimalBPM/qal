__author__ = 'nibo'

class Recurse(object):
    """This class provides helper functions for recursion
    """
    debuglevel = 2
    nestinglevel = 0

    def _print_nestinglevel(self, _value):
        """Prints the current nesting level. Not thread safe."""
        self._debug_print(_value + ' level: ' + str(self.nestinglevel), 4)

    def _get_up(self, _value):
        """Gets up one nesting level. Not thread safe."""
        self.nestinglevel -= 1
        self._print_nestinglevel("Leaving " + _value)

    def _go_down(self, _value):
        """Gets down one nesting level. Not thread safe."""
        self.nestinglevel += 1
        self._print_nestinglevel("Entering " + _value)

    def _debug_print(self, _value, _debuglevel=3):
        """Prints a debug message if the debugging level is sufficient."""
        if self.debuglevel >= _debuglevel:
            print(_value)