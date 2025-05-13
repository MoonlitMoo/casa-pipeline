import collections
import pytest
import unittest.mock as mock

import pipeline.domain.antennaarray as antennaarray
import pipeline.domain.field as field
import pipeline.hsd.tasks.baseline.detection as detection
import pipeline.infrastructure.casa_tools as casa_tools

SpectralWindowMock = collections.namedtuple(
    'SpectralWindowMock',
    ['id']
)

MeasurementSetMock = collections.namedtuple(
    'MeasurementSetMock',
    ['get_fields', 'get_spectral_windows', 'antenna_array', 'start_time']
)

ANTENNA_ARRAY = antennaarray.AntennaArray(
    name='ALMA',
    position=casa_tools.measures.observatory('ALMA'),
    antennas=[]
)

FIELD_0 = field.Field(
    field_id=0,
    name='TestField0',
    source_id=0,
    time=[],
    direction=casa_tools.measures.direction()
)

MS_INSTANCE = MeasurementSetMock(
    get_fields=mock.MagicMock(
        return_value=[FIELD_0]
    ),
    get_spectral_windows=mock.MagicMock(
        return_value=[SpectralWindowMock(id=0), SpectralWindowMock(id=1)]
    ),
    antenna_array=ANTENNA_ARRAY,
    start_time=casa_tools.measures.epoch()
)


@pytest.mark.parametrize(
    'window, expected',
    [
        # list inputs
        ([0, 100], {0: [[0, 100]], 1: [[0, 100]]}),
        ([[0, 100]], {0: [[0, 100]], 1: [[0, 100]]}),
        # dict inputs (accept None)
        ({0: [0, 100]}, {0: [[0, 100]], 1: []}),
        ({0: [0, 100], 1: [[20, 70], [80, 90]]}, {0: [[0, 100]], 1: [[20, 70], [80, 90]]}),
        ({0: None, 1: [20, 70]}, {0:  None, 1: [[20, 70]]}),
        ({0: [0, 100], 1: None}, {0: [[0, 100]], 1: None}),
        ({0: None, 1: None}, {0: None, 1: None}),
        # dict string inputs (accept None)
        ('{0: [0, 100]}', {0: [[0, 100]], 1: []}),
        ('{0: [0, 100], 1: [[20, 70], [80, 90]]}', {0: [[0, 100]], 1: [[20, 70], [80, 90]]}),
        ('{0: None, 1: [20, 70]}', {0:  None, 1: [[20, 70]]}),
        ('{0: [0, 100], 1: None}', {0: [[0, 100]], 1: None}),
        ('{0: None, 1: None}', {0: None, 1: None}),
    ]
)
def test_channel_mode(window, expected):
    ms = MS_INSTANCE
    science_windows = [spw.id for spw in ms.get_spectral_windows()]
    parser = detection.LineWindowParser(ms, window)
    parser.parse(field_id=0)
    result = dict((i, parser.get_result(i)) for i in science_windows)
    print(f'result {result}, expected {expected}')
    assert result == expected
