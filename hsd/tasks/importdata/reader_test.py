"""
reader_test.py : Unit tests for "hsd/tasks/importdata/reader.py".

Unit tests for "hsd/tasks/importdata/reader.py"

Please see hsd/tasks/common/utils_test.py for detailed explanation
on the test cases.
"""
import pytest

from .reader import mjdsec2str

test_cases = [
    (56839.91646527777, '2014/7/1/21:59:42.599999'),
    (56839.91647013888, '2014/7/1/21:59:43.019999'),
    (60414, '2024/4/14/0:0:0.0000000'),
    (60414.9999999999, '2024/4/14/23:59:59.9999912'),
    (60414.999999999985, '2024/4/14/23:59:59.9999987')
]


@pytest.mark.parametrize('mjd, expected', test_cases)
def test_mjdsec2str(mjd: float, expected: str):
    # 1 day = 24hour * 60min * 60sec = 86400sec
    mjdsec = mjd * 86400
    result = mjdsec2str(mjdsec)
    print(f'result {result} expected {expected}')
    # split string into two parts: year~second and microsecond
    # year~second: must be identical
    # microsecond: should be compared as float value with 1e-6 accuracy
    result_datetime, result_microsec = result.split('.')
    expected_datetime, expected_microsec = expected.split('.')

    assert result_datetime == expected_datetime
    result_usec = float(f'0.{result_microsec}')
    expected_usec = float(f'0.{expected_microsec}')
    if expected_usec == 0:
        assert result_usec == expected_usec
    else:
        diff = abs((result_usec - expected_usec) / expected_usec)
        assert diff < 1e-6
