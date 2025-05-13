"""
The sorting module contains utility functions used to sort pipeline input and
output.
"""
import re

from .. import logging

LOG = logging.get_logger(__name__)

__all__ = ["natural_sort", "numeric_sort", "natural_sort_key", "numeric_sort_key"]


def natural_sort_key(x, _nsre=re.compile(r"([0-9]+)")):
    """Key to order in natural order with the sort function"""
    return [
        int(text) if text.isdigit() else text.lower() for text in re.split(_nsre, x)
    ]


def natural_sort(input_list):
    """
    Sort a list in natural order, eg.

    >>> natural_sort(["session10", "session10a", "session9", "session1"])
    ['session1', 'session9', 'session10', 'session10a']
    """
    return sorted(input_list, key=natural_sort_key)


def numeric_sort_key(s, _nsre=re.compile("([0-9]+)")):
    """Key to order in numeric order with the sort function.
    Split a string by numbers.
    """
    return [int(text) if text.isdigit() else text for text in re.split(_nsre, s)]


def numeric_sort(input_list):
    """
    Sort a list numerically, eg.

    >>> numeric_sort(['9,11,13,15', '11,13', '9'])
    ['9', '9,11,13,15', '11,13']
    """
    return sorted(input_list, key=numeric_sort_key)
