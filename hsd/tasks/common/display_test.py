import numpy as np
import pytest

import pipeline.hsd.tasks.common.display as display

@pytest.mark.parametrize(
    'frequencies, channels, expected',
    [
        # interpolation, scalar input
        ([100., 101., 102], 0.5, 100.5),
        # interpolation, list input
        ([100., 101., 102.], [0, 0.5, 1, 1.25, 2], [100., 100.5, 101., 101.25, 102.]),
        # interpolation, LSB
        ([102., 101., 100.], [0, 0.5, 1, 1.25, 2], [102., 101.5, 101., 100.75, 100.]),
        # extrapolation, scalar input
        ([100., 101., 102.], -1, 99.),
        # extrapolation, list input
        ([100., 101., 102.], [-0.5, 2.5], [99.5, 102.5])
    ]
)
def test_ch_to_freq(frequencies, channels, expected):
    interpolated = display.ch_to_freq(channels, frequencies)
    assert np.allclose(interpolated, expected)
