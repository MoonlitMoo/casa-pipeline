"""
utils_test.py : Unit tests for "hsd/tasks/common/utils.py".

Unit tests for "hsd/tasks/common/utils.py"

Test Cases:
    1. MJD 56839.91646527777  -> 2014/7/1 21:59:42.599999
        This is a kind of boundary test that checks if the function
        property handles the date and time with microsecond accuracy.
    2. MJD 56839.91647013888  -> 2014/7/1 21:59:43.019999
        This is a test that checks if the function properly handles
        the sub-second value which is less than 0.1 where there
        are zeros after decimal point.
    3. MJD 60414              -> 2024/4/14 0:0:0.0
    4. MJD 60414.9999999999   -> 2024/4/14 23:59:59.999991
    5. MJD 60414.999999999985 -> 2024/4/14 23:59:59.999999
        These three test cases correspond to check if the bug
        reported to PIPE-2156 is fixed. As shown in the test #3,
        MJD 60414 is 2024/4/14, and it should remain the same as long
        as an integer part is 60414 (only hour/minute/second part are
        affected by the change of decimal place). However, qa.splitdate
        interpreted the values in tests #4 and #5 as 2024/4/15, which
        is obviously wrong. These tests intend to make sure the
        functions are not suffered from the bug.
"""
import datetime

import pytest

from .utils import mjd_to_datetime, mjd_to_datestring

test_cases = [
    (56839.91646527777, datetime.datetime(2014, 7, 1, 21, 59, 42, 599999)),
    (56839.91647013888, datetime.datetime(2014, 7, 1, 21, 59, 43, 19999)),
    (60414, datetime.datetime(2024, 4, 14, 0, 0, 0)),
    (60414.9999999999, datetime.datetime(2024, 4, 14, 23, 59, 59, 999991)),
    (60414.999999999985, datetime.datetime(2024, 4, 14, 23, 59, 59, 999999))
]


@pytest.mark.parametrize('mjd, expected', test_cases)
def test_mjd_to_datetime(mjd: float, expected: datetime.datetime):
    result = mjd_to_datetime(mjd)
    assert result.year == expected.year
    assert result.month == expected.month
    assert result.day == expected.day
    assert result.hour == expected.hour
    assert result.minute == expected.minute
    assert result.second == expected.second
    assert result.microsecond == expected.microsecond


test_cases = [
    (56839.91646527777, 'Tue Jul  1 21:59:42 2014 UTC'),
    (56839.91647013888, 'Tue Jul  1 21:59:43 2014 UTC'),
    (60414, 'Sun Apr 14 00:00:00 2024 UTC'),
    (60414.9999999999, 'Sun Apr 14 23:59:59 2024 UTC'),
    (60414.999999999985, 'Sun Apr 14 23:59:59 2024 UTC')
]


@pytest.mark.parametrize('mjd, expected', test_cases)
def test_mjd_to_datestring(mjd: float, expected: str):
    result = mjd_to_datestring(mjd, 'd')
    assert result == expected

    mjd_sec = mjd * 86400
    result = mjd_to_datestring(mjd_sec, 's')
    assert result == expected

    # default unit is 's'
    result = mjd_to_datestring(mjd_sec)
    assert result == expected
