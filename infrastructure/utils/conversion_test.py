import collections
import datetime

import pytest

from .conversion import commafy, dequote, flatten, format_datetime, format_timedelta, mjd_seconds_to_datetime,\
    range_to_list, safe_split, unix_seconds_to_datetime, _parse_antenna, _parse_field, _parse_spw


DomainMock = collections.namedtuple('DomainMock', ['id', 'name'])
AntennaMock = DomainMock
FieldMock = DomainMock


@pytest.mark.parametrize("inp, kwargs, expected", [
    ([], {}, ""),
    (['a'], {}, "'a'"),
    (['a', 'b'], {}, "'a' and 'b'"),
    (['a', 'b', 'c'], {}, "'a', 'b' and 'c'"),
    (["a", "b", "c"], {}, "'a', 'b' and 'c'"),
    (['a', 'b'], {'quotes': False}, "a and b"),
    (['25', '27', '29'], {'multi_prefix': 's'}, "s '25', '27' and '29'"),
    (['a', 'b', 'c'], {'separator': '; '}, "'a'; 'b' and 'c'"),
    (['a', 'b', 'c'], {'conjunction': '&'}, "'a', 'b' & 'c'"),
])
def test_commafy(inp, kwargs, expected):
    """Test commafy()"""
    assert commafy(inp, **kwargs) == expected


@pytest.mark.parametrize("inp, expected", [
    ('Field \'1\'', "Field 1"),
    ("Field \"1\"", "Field 1"),
    ('"Field 1", "Field 2"', "Field 1, Field 2"),
    ("'Field 1', 'Field 2'", "Field 1, Field 2"),
])
def test_dequote(inp, expected):
    """Test dequote()"""
    assert dequote(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ([1, 2], [1, 2]),
    ([[1], [2]], [1, 2]),
    ([[[1], [2]], [3]], [1, 2, 3]),
    ('abcd', ['a', 'b', 'c', 'd']),
    (['ab', 'cd'], ['ab', 'cd']),
])
def test_flatten(inp, expected):
    """Test flatten()"""
    it = flatten(inp)
    for el in expected:
        assert next(it) == el


def test_flatten_empty():
    """Test flatten() with empty input"""
    with pytest.raises(StopIteration):
        next(flatten([]))
    with pytest.raises(StopIteration):
        next(flatten(''))


@pytest.mark.parametrize("inp, kwargs, expected", [
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {}, '2020-01-01 12:34:56'),
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {'dp': 5}, '2020-01-01 12:34:56.00001'),
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {'dp': 6}, '2020-01-01 12:34:56.000007'),
])
def test_format_datetime(inp, kwargs, expected):
    """Test format_datetime()"""
    assert format_datetime(inp, **kwargs) == expected


def test_format_datetime_raises_exception_too_high_precision():
    """Test format_datetime() when requesting too high precision"""
    with pytest.raises(ValueError):
        format_datetime(datetime.datetime(2020, 1, 1, 12, 34, 56, 7), dp=7)


@pytest.mark.parametrize("inp, kwargs, expected", [
    (datetime.timedelta(days=1), {}, '1 day, 0:00:00'),
    (datetime.timedelta(days=9, seconds=8, microseconds=7, milliseconds=6), {'dp': 5}, '9 days, 0:00:08.00601'),
    (datetime.timedelta(days=9, seconds=8, microseconds=7, milliseconds=6), {'dp': 6}, '9 days, 0:00:08.006007'),
])
def test_format_timedelta(inp, kwargs, expected):
    """Test format_timedelta()"""
    assert format_timedelta(inp, **kwargs) == expected


def test_format_timedelta_raises_exception_too_high_precision():
    """Test format_timedelta() when requesting too high precision"""
    with pytest.raises(ValueError):
        format_timedelta(datetime.timedelta(days=1), dp=7)


@pytest.mark.parametrize("inp, expected", [
    ([1, 2], [datetime.datetime(1858, 11, 17, 0, 0, 1), datetime.datetime(1858, 11, 17, 0, 0, 2)]),
    ([1, 1.5], [datetime.datetime(1858, 11, 17, 0, 0, 1), datetime.datetime(1858, 11, 17, 0, 0, 1, 500000)]),
])
def test_mjd_seconds_to_datetime(inp, expected):
    """Test mjd_seconds_to_datetime()"""
    assert mjd_seconds_to_datetime(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ('', []),
    ('1', [1]),
    ('1,2', [1, 2]),
    ('1~3,5,7~9', [1, 2, 3, 5, 7, 8, 9]),
])
def test_range_to_list(inp, expected):
    """Test range_to_list()"""
    assert range_to_list(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ("", ['']),
    ("Field 1", ["Field 1"]),
    ("Field 1, 'Field 2,3'", ['Field 1', "'Field 2,3'"]),
    ('"Field 1,2" , Field 3', ['"Field 1,2"', 'Field 3']),
])
def test_safe_split(inp, expected):
    """Test safe_split()"""
    assert safe_split(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ([1, 1.5], [datetime.datetime(1970, 1, 1, 0, 0, 1), datetime.datetime(1970, 1, 1, 0, 0, 1, 500000)]),
])
def test_unix_seconds_to_datetime(inp, expected):
    """Test unix_seconds_to_datetime()"""
    assert unix_seconds_to_datetime(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ((None, None), TypeError),
    ((None, [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('*', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('0', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0]),
    (('1', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [1]),
    # it seems that _parse_antenna doesn't check availability of selected id
    (('3', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [3]),
    (('0~1', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('0,1', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    # check if returned list is sorted
    (('1,0', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    # check if unique list is returned
    (('0~1,1', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('Test00', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0]),
    (('Test01', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [1]),
    (('Test01,0', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('Test03', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), []),
    (('Test*', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0, 1]),
    (('Test00,0', [AntennaMock(id=0, name='Test00'), AntennaMock(id=1, name='Test01')]), [0]),
])
def test__parse_antenna(inp, expected):
    """Test _parse_antenna"""
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            _parse_antenna(*inp)
    else:
        assert _parse_antenna(*inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ((None, None), TypeError),
    ((None, [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('*', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('0', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0]),
    (('1', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [1]),
    # it seems that _parse_field doesn't check availability of selected id
    (('3', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [3]),
    (('0,1', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('0~1', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    # check if returned list is sorted
    (('1,0', [FieldMock(id=0, name='Test00'), FieldMock(id=1, name='Test01')]), [0, 1]),
    # check if unique list is returned
    (('0~1,1', [FieldMock(id=0, name='Test00'), FieldMock(id=1, name='Test01')]), [0, 1]),
    (('Test*', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('TestNW*', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0]),
    (('TestSE*', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [1]),
    (('TestSW*', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), []),
    (('TestSE*,0', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0, 1]),
    (('TestNW*,0', [FieldMock(id=0, name='TestNW00'), FieldMock(id=1, name='TestSE01')]), [0]),
])
def test__parse_field(inp, expected):
    """Test _parse_field"""
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            _parse_field(*inp)
    else:
        assert _parse_field(*inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ((None, None), None),
    (('', [0, 1, 2]), [(0, set()), (1, set()), (2, set())]),
    (('*', [0, 1, 2]), [(0, set()), (1, set()), (2, set())]),
    (('1', [0, 1, 2]), [(1, set())]),
    (('<2', [0, 1, 2]), [(0, set()), (1, set())]),
    # it seems that _parse_spw doesn't check availability of selected id
    (('3', [0, 1, 2]), [(3, set())]),
    # operator '>' is not supported
    (('>1', [0, 1, 2]), Exception),
    (('0,2', [0, 1, 2]), [(0, set()), (2, set())]),
    (('1~2', [0, 1, 2]), [(1, set()), (2, set())]),
    # _parse_spw doesn't sort the result
    (('1~2,0', [0, 1, 2]), [(1, set()), (2, set()), (0, set())]),
    # check if unique list is returned
    (('1~2,1', [0, 1, 2]), [(1, set()), (2, set())]),
    # channel selection
    (('0:0~6^2,2:6~38^4', [0, 1, 2]), [(0, set((0, 2, 4, 6))), (2, set((6, 10, 14, 18, 22, 26, 30, 34, 38)))]),
])
def test__parse_spw(inp, expected):
    """Test _parse_spw"""
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            _parse_spw(*inp)
    else:
        result = _parse_spw(*inp)
        if expected is None:
            assert result is None
            return

        assert len(result) == len(expected)
        for rs, ex in zip(result, expected):
            id = getattr(rs, 'spw', rs)
            chan = getattr(rs, 'channels', set())
            assert id == ex[0]
            assert chan == ex[1]
