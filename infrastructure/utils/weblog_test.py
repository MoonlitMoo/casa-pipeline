"""
Tests for the weblog.py module.
"""
import datetime
import pytest
from .. import casa_tools
from pipeline.infrastructure.tablereader import MeasurementSetReader
from .weblog import (
    OrderedDefaultdict,
    merge_td_columns,
    total_time_on_target_on_source,
    total_time_on_source
)


# # Tests that depend on the pipeline-testdata repository
TEST_DATA_PATH = casa_tools.utils.resolve('pl-unittest/casa_data')
# Skip tests if CASA cannot resolve to an absolute path
skip_data_tests = not TEST_DATA_PATH.startswith('/')

# Create decorator with reason to skip tests
skip_if_no_data_repo = pytest.mark.skipif(
    skip_data_tests,
    reason="The repo pipeline-testdata is not set up for the tests"
)

MS_NAME_DC = casa_tools.utils.resolve("pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms")


def test_OrderedDefaultdict_insertion():
    my_list = OrderedDefaultdict(list)
    my_list[2] = [1, 2, 3]
    for i in [0, 1, 3, 120, -1, -40]:
        assert my_list[i] == []
    assert my_list[2] == [1, 2, 3]


def test_OrderedDefaultdict_typical_use_case():
    """Typical use case in our codebase"""
    my_list = OrderedDefaultdict(list)
    my_list[2] = [1, 2, 3]
    my_list[2].extend([4, 5])
    assert my_list[2] == [1, 2, 3, 4, 5]


merge_td_columns_test_params = [
    [
        [
            (1, 2, 3),
            (1, 2, 4)
        ],
        [
            ('<td rowspan="2">1</td>', '<td rowspan="2">2</td>', '<td>3</td>'),
            ('', '', '<td>4</td>')
        ]
    ],
    [
        [
            (1, 2, 3),
            (1, 2, 4),
            (2, 2, 3)
        ],
        [
            ('<td rowspan="2">1</td>', '<td rowspan="3">2</td>', '<td>3</td>'),
            ('', '', '<td>4</td>'),
            ('<td>2</td>', '', '<td>3</td>')
        ]
    ],
    [
        [
            (1, 2, 3),
            (1, 2, 4),
            (2, 2, 3)
        ],
        [
            ('<td rowspan="2">1</td>', '<td rowspan="3">2</td>', '<td>3</td>'),
            ('', '', '<td>4</td>'),
            ('<td>2</td>', '', '<td>3</td>')
        ]
    ],
]

@pytest.mark.parametrize("params, expected", merge_td_columns_test_params)
def test_merge_td_columns(params, expected):
    assert merge_td_columns(params) == expected

merge_td_columns_num_to_merge_test_params = [
    [
        0,
        [(1, 1, 1), (1, 1, 1)],
        [('<td>1</td>', '<td>1</td>', '<td>1</td>'),
         ('<td>1</td>', '<td>1</td>', '<td>1</td>')]

    ],
    [
        1,
        [(1, 1, 1), (1, 1, 1)],
        [('<td rowspan="2">1</td>', '<td>1</td>', '<td>1</td>'),
         ('', '<td>1</td>', '<td>1</td>')]

    ],
    [
        2,
        [(1, 1, 1), (1, 1, 1)],
        [('<td rowspan="2">1</td>', '<td rowspan="2">1</td>', '<td>1</td>'),
         ('', '', '<td>1</td>')]

    ],
    [
        3,
        [(1, 1, 1), (1, 1, 1)],
        [('<td rowspan="2">1</td>', '<td rowspan="2">1</td>', '<td rowspan="2">1</td>'),
         ('', '', '')]

    ],
    [
        20,
        [(1, 1, 1), (1, 1, 1)],
        [('<td rowspan="2">1</td>', '<td rowspan="2">1</td>', '<td rowspan="2">1</td>'),
         ('', '', '')]

    ],
    [
        -1,
        [(1, 1, 1), (1, 1, 1)],
        [('<td>1</td>', '<td>1</td>', '<td>1</td>'),
         ('<td>1</td>', '<td>1</td>', '<td>1</td>')]

    ],
]

@pytest.mark.parametrize("num_to_merge, params, expected", merge_td_columns_num_to_merge_test_params)
def test_merge_td_columns_num_to_merge(num_to_merge, params, expected):
    assert merge_td_columns(params, num_to_merge=num_to_merge) == expected

def test_merge_td_columns_vertical_align():
    expected_output = [
        ('<td rowspan="2" style="vertical-align:middle;">1</td>',
         '<td rowspan="2" style="vertical-align:middle;">1</td>',
         '<td rowspan="2" style="vertical-align:middle;">1</td>'),
        ('', '', '')
    ]
    assert merge_td_columns([(1, 1, 1), (1, 1, 1)], vertical_align=True) == expected_output


@skip_if_no_data_repo
def test_total_time_on_target_on_source():
    MS = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    assert total_time_on_target_on_source(MS) == datetime.timedelta(0, 40, 320000)
    assert total_time_on_target_on_source(MS, autocorr_only=True) == datetime.timedelta(0, 40, 320000)


@skip_if_no_data_repo
def test_total_time_on_source():
    MS = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    assert total_time_on_source(MS.scans) == datetime.timedelta(0, 40, 319999)

## PIPE-876 - There are some remaining functions to test which could not be included in this ticket:
# * get_logrecords: It requires a result object to work which is complex and expensive to mock.
# * get_intervals: It requires a context object and a CalApplication object which are also complex and expensive to
#  mock.
# The strategy of loading a (relocatable) context to perform some tests was considered but this context can become
#  outdated as the pipeline is developed and the tests would be meaningless unless the object is updated. This update
#  can be expensive in terms of computing time or human maintenance.