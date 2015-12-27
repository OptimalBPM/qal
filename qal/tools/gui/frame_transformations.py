"""
Created on Feb 25, 2014

@author: Nicklas Boerjesson
"""
from tkinter import ttk
from tkinter.constants import W
from qal.common.strings import empty_when_none
from qal.transformation.transform import Cast, Trim, IfEmpty, Replace, ReplaceRegex
from qal.tools.gui.frame_list import FrameCustomItem
from qal.tools.gui.widgets_misc import Selector, make_entry

__author__ = 'nibo'


def transformation_to_type(_transformation):
    """
    Gets a string description on transformation classes.
    :param _transformation: The instance to be described.
    :return:
    """
    if isinstance(_transformation, Cast):
        return "Cast", "Cast value from one type to another"
    elif isinstance(_transformation, Trim):
        return "Trim", "Trim characters"
    elif isinstance(_transformation, IfEmpty):
        return "If empty", "IfEmpty, return value"
    elif isinstance(_transformation, Replace):
        return "Replace", "Replace old value with new value max number of times"
    elif isinstance(_transformation, ReplaceRegex):
        return "Replace regex", "Replace pattern with new value max number of times"

def type_to_transformation(_type):
    """
    Returns a class instance.
    :param _type: The string that needs to be instantiated.
    :return: A class instance.
    """
    if _type == "Cast":
        return Cast
    elif _type == "Trim":
        return Trim
    elif _type == "If empty":
        return IfEmpty
    elif _type == "Replace":
        return Replace
    elif _type == "Replace regex":
        return ReplaceRegex
    else:
        raise Exception("Error in type_to_transformation: " + str(_type) + " is an invalid type.")


class FrameTransformationCustom(FrameCustomItem):
    """
    This is the superclass to all the transformation GUI classes.
    """
    l_type = None
    """The label widget that indicates the type of transformation"""
    l_output = None
    """Label that shows the data after the tranformation"""

    transformation = None
    """The transformation class instance"""
    fr_parameters = None
    """The frame that holds the parameters"""


    def __init__(self, _master, _transformation=None):

        super(FrameTransformationCustom, self).__init__(_master)

        self.transformation = _transformation

        self.init_main_gui()

        self.init_gui()
        # If a transformation is a assigned,
        if self.transformation:
            self.transformation_to_gui()

        # Initialise result fields using grid positioning
        # TODO: Should maybe grids not be used here, this is fairly ugly and prevents a proper reload.
        _last = len(self.winfo_children()) + 2
        self.l_result_caption = ttk.Label(self, text="Result :")
        self.l_result_caption.grid(column=0, row=_last, columnspan=1, sticky=W)
        self.l_result_label = ttk.Label(self, text="No transformation done")
        self.l_result_label.grid(column=1, row=_last, columnspan=1, sticky=W)
        self.transformation.on_done = self.on_done

    def on_done(self, _value, _error):
        """Is called when the transformation is done, used to indicate if any errors have occurred."""
        if _error:
            self.l_result_label["text"] = _error
            self.l_result_label["foreground"] = "red"
        else:
            self.l_result_label["foreground"] = "black"
            self.l_result_label["text"] = _value

    def init_main_gui(self):
        """Initilize the common labels"""
        # TODO: Should not this be in init_gui()?
        _type, _desc = transformation_to_type(self.transformation)
        l_type = ttk.Label(self, text=str(_type) + ":  (" + str(_desc) + ")")
        l_type.grid(column=0, row=0, columnspan=2, sticky=W)
        self.grid_columnconfigure(index=1, weight=1)

    def transformation_to_gui(self):
        """ Implemented in subclasses; populate the GUI widgets from self.transformation."""
        raise Exception("transformation_to_gui() is not implemented in " + self.__class__.__name__)

    def init_gui(self):
        """ Implemented in subclasses; initializes the GUI widgets."""
        raise Exception("init_gui() is not implemented in " + self.__class__.__name__)

    def gui_to_transformation(self):
        """ Implemented in subclasses; write widget values to self.transformation."""
        raise Exception("gui_to_transformation() is not implemented in " + self.__class__.__name__)

    def on_change(self, _current_value, _current_index):
        """ Implemented in subclasses; Triggered when value is changed."""
        raise Exception("onchange() is not implemented in " + self.__class__.__name__)


class FrameTransformationCast(FrameTransformationCustom):
    """ Visually represents a Cast transformation.
        See qal.tools.transform.Cast
    """

    def init_gui(self):
        _dest_types = ['string', 'string(255)', 'string(3000)', 'float', 'integer', 'serial', 'timestamp', 'boolean']
        self.sel_destination_type = Selector(_master=self, _caption="Destination type", _values=_dest_types,
                                             _onchange=self.on_change)
        self.sel_destination_type.grid(column=0, row=1, columnspan=2)
        self.format_string, self.e_format_string, self.l_format_string = make_entry(self, "Format string", 2,
                                                                                    _split_sticky=True)


    def transformation_to_gui(self):
        self.sel_destination_type.set_but_do_not_propagate(empty_when_none(self.transformation.dest_type))
        self.format_string.set(empty_when_none(self.transformation.format_string))

    def gui_to_transformation(self):
        self.transformation.dest_type = self.sel_destination_type.value.get()
        self.transformation.format_string = self.format_string.get()

    def on_change(self, _current_value, _current_index):
        print('Cast dest_type changed')


class FrameTransformationTrim(FrameTransformationCustom):
    """ Visually represents a Trim transformation.
        See qal.tools.transform.Trim
    """

    def init_gui(self):
        _wheres = ['beginning', 'end', 'both']
        self.sel_where = Selector(_master=self, _caption="Trim where", _values=_wheres, _onchange=self.on_change)
        self.sel_where.grid(column=0, row=1, columnspan=2)

    def transformation_to_gui(self):
        self.sel_where.set_but_do_not_propagate(empty_when_none(self.transformation.value))

    def gui_to_transformation(self):
        self.transformation.value = self.sel_where.value.get()

    def on_change(self, _current_value, _current_index):
        print('Trim where changed')


class FrameTransformationIfEmpty(FrameTransformationCustom):
    """ Visually represents a IfEmpty transformation.
        See qal.tools.transform.IfEmpty
    """

    def init_gui(self):
        self.value, self.e_value, self.l_value = make_entry(self, "Value if empty", 1, _split_sticky=True)

    def transformation_to_gui(self):
        self.value.set(empty_when_none(self.transformation.value))

    def gui_to_transformation(self):
        self.transformation.value = self.value.get()

    def on_change(self, _current_value, _current_index):
        print('IfEmpty value changed')


class FrameTransformationReplace(FrameTransformationCustom):
    """ Visually represents a Replace transformation.
        See qal.tools.transform.Replace
    """

    def init_gui(self):
        self.old, self.e_old, self.l_old = make_entry(self, "Old value", 1, _split_sticky=True)
        self.new, self.e_new, self.l_new = make_entry(self, "New value", 2, _split_sticky=True)
        self.max, self.e_max, self.l_max = make_entry(self, "Max times", 3, _split_sticky=True)

    def transformation_to_gui(self):
        self.old.set(empty_when_none(self.transformation.old))
        self.new.set(empty_when_none(self.transformation.new))
        self.max.set(empty_when_none(self.transformation.max))

    def gui_to_transformation(self):
        self.transformation.old = self.old.get()
        self.transformation.new = self.new.get()
        self.transformation.max = self.max.get()

    def on_change(self, _current_value, _current_index):
        print('Replace value changed')

class FrameTransformationReplaceRegex(FrameTransformationCustom):
    """ Visually represents a ReplaceRegex transformation.
        See qal.tools.transform.ReplaceRegex
    """

    def init_gui(self):
        self.pattern, self.e_pattern, self.l_pattern = make_entry(self, "Pattern", 1, _split_sticky=True)
        self.new, self.e_new, self.l_new = make_entry(self, "New value", 2, _split_sticky=True)
        self.max, self.e_max, self.l_max = make_entry(self, "Max times", 3, _split_sticky=True)

    def transformation_to_gui(self):
        self.pattern.set(empty_when_none(self.transformation.pattern))
        self.new.set(empty_when_none(self.transformation.new))
        self.max.set(empty_when_none(self.transformation.max))

    def gui_to_transformation(self):
        self.transformation.pattern = self.pattern.get()
        self.transformation.new = self.new.get()
        self.transformation.max = self.max.get()

    def on_change(self, _current_value, _current_index):
        print('Replace regex value changed')
