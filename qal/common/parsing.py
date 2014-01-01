"""
    Contains miscellaneous parsing functions.
    
    :copyright: Copyright 2010-2013 by Nicklas Börjesson
    :license: BSD, see LICENSE for details. 
"""

def parse_balanced_delimiters(_input, _d_left, _d_right, _text_qualifier):
    """Removes all balanced delimiters. Handles multiple levels and ignores those contained within text delimiters."""
    
    # Depth is deep into recursion we are
    _depth = 0
    
    # Balance is a list of all balanced delimiters
    _balanced = []
    
    # Cleared is a string without the balanced delimiters
    _cleared = ""
    
    # If in text mode all delimiters are ignored, since they are in text. Also, _text delimiters are escaped.
    _text_mode = False
    
    # Start- and end positions refers to start and end of balanced delimiters. 
    _start_pos = None
    # End pos implicitly also mean start of "clean" area.
    _clear_start_pos = 0
    
    for _curr_idx in range(len(_input)):
        _curr_char = _input[_curr_idx]


        if _curr_char == _text_qualifier:
            _text_mode = not _text_mode
        elif _text_mode == False:
            if _curr_char == _d_left:
                if _depth == 0:
                    _start_pos = _curr_idx
                    _cleared+=_input[_clear_start_pos:_curr_idx]
                    _clear_start_pos = None
                    
                _depth+=1
            elif _curr_char == _d_right:
                _depth-=1
            if _start_pos and _depth == 0  and _start_pos < _curr_idx:
                _balanced.append(_input[_start_pos +1:_curr_idx])
                _start_pos = None
                _clear_start_pos = _curr_idx + 1 
                
            
    if _cleared == "":
        return _balanced, _input
    else:
        return _balanced, _cleared

