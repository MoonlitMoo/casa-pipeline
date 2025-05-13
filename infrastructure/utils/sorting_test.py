"""
Tests for the sorting.py module.
"""
import pytest

from .sorting import natural_sort, numeric_sort, natural_sort_key, numeric_sort_key


test_params_natural_sort = (
    (["session10", "session10a", "session9", "session1"],
     ["session1", "session9", "session10", "session10a"]),
    (["V-1.2.10", "V-1.2.1"], ["V-1.2.1", "V-1.2.10"]),
    (["V-1.2.10", "v-1.2.10", "V-1.2.1"],
     ["V-1.2.1", "V-1.2.10", "v-1.2.10"]),  # Edge case 1
    (["v-1.2.10", "V-1.2.10", "V-1.2.1"],
     ["V-1.2.1", "v-1.2.10", "V-1.2.10"])  # Edge case 2
)


@pytest.mark.parametrize("input_list, expected", test_params_natural_sort)
def test_natural_sort(input_list, expected):
    """Test natural_sort()

    Natural sort orders a list of strings taking into account the numerical
    values included in the string. It is case insensitive in terms of sorting
    with respect to the characters.
    """
    assert natural_sort(input_list) == expected


test_params_numeric_sort = (
    (["9,11,13,15", "11,13", "9"], ["9", "9,11,13,15", "11,13"]),
    (["9,11,13,15", "11,13", "10"], ["9,11,13,15", "10", "11,13"]),
    (["V-1.2.1", "V-1.2.10", "v-1.2.1"], ["V-1.2.1", "V-1.2.10", "v-1.2.1"])
)


@pytest.mark.parametrize("input_list, expected", test_params_numeric_sort)
def test_numeric_sort(input_list, expected):
    """Test numeric_sort()

    Numeric sort orders a list of strings taking into account the numerical
    values included in the string.
    """
    assert numeric_sort(input_list) == expected


test_params_natural_sort_key = (
    ("session9", ["session", 9, ""]),
    ("session10", ["session", 10, ""]),
    ("session10a", ["session", 10, "a"]),
    ("spw10", ["spw", 10, ""]),
    ("spw100", ["spw", 100, ""]),
    ("spw1", ["spw", 1, ""]),
    ("Spw10", ["spw", 10, ""]),
)


@pytest.mark.parametrize("input_list, expected", test_params_natural_sort_key)
def test_natural_sort_key(input_list, expected):
    """Test natural_sort_key()

    This test the sorting key defined to obtain a natural sort order. It
    splits a string into a list of elements defined by being sets
    of digits or other types of characters. The digits are converted to
    integers and the strings to lower case to allow a natural sorting when
    using this list as the sort key in the sorted function.

    In its current implementation natural_sort returns an empty string element
    at the beginning/end if the input string starts/ends with a number.
    """
    assert natural_sort_key(input_list) == expected


test_params_numeric_sort_key = (
    ("9,11,13", ["", 9, ",",  11, ",", 13, ""]),
    ("9,11", ["", 9, ",",  11, ""]),
    ("V1.2", ["V", 1, ".", 2, ""]),
    ("v1.2", ["v", 1, ".", 2, ""]),
)


@pytest.mark.parametrize("input_list, expected", test_params_numeric_sort_key)
def test_numeric_sort_key(input_list, expected):
    """Test numeric_sort_key()

    This test the sorting key defined to obtain a natural sort order. The main
    difference with natural_sort_key() is that it is case sensitive for string
    components.

    In its current implementation natural_sort returns an empty string element
    at the beginning/end if the input string starts/ends with a number.
    """
    assert numeric_sort_key(input_list) == expected
