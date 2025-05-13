import pytest
import numpy as np
import numpy.testing as nt
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.tablereader import MeasurementSetReader
from .mswrapper import MSWrapper, calc_vk


# # Tests that depend on the pipeline-testdata repository
TEST_DATA_PATH = casa_tools.utils.resolve('pl-unittest/casa_data')
# Skip tests if CASA cannot resolve to an absolute path
skip_data_tests = not TEST_DATA_PATH.startswith('/')
# Create decorator with reason to skip tests
skip_if_no_data_repo = pytest.mark.skipif(
    skip_data_tests,
    reason="The repo pipeline-testdata is not set up for the tests"
)


MS_NAME = casa_tools.utils.resolve("pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms")
MS_NAME_DC = casa_tools.utils.resolve("pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms")
SCAN_ID = 10
SPW_ID = 0


@skip_if_no_data_repo
def test_create_averages_from_ms_fail_as_expected():
    """This tests just checks that the function can be called. It will raise a numpy.AxisError exception
    because the 'corrected_data' column is not populated in the unprocessed test dataset.
    """
    ms = MeasurementSetReader.get_measurement_set(MS_NAME)
    with pytest.raises(np.AxisError):
        wrapper = MSWrapper.create_averages_from_ms(ms.name, SCAN_ID, SPW_ID, 1)


@skip_if_no_data_repo
def test_create_averages_from_ms_works():
    ms = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    wrapper = MSWrapper.create_averages_from_ms(ms.name, SCAN_ID, SPW_ID, 1)
    assert wrapper.V.dtype == [
        ('antenna', '<i4'),
        ('corrected_data', '<c16', (2, 128)),
        ('time', '<f8'),
        ('sigma', '<c16', (2, 128)),
        ('chan_freq', '<f8', (128,)),
        ('resolution', '<f8', (128,))
    ]


@skip_if_no_data_repo
def test_create_averages_from_ms_produces_comparable_corrected_data():
    """This test checks that the code implemented in PIPE-687 gives equivalent values to
    those of the old implementation.
    The test may be deleted in the future once the output is validated.
    The corrected_data column should be the same if the perantave is set to False and the
    complex conjugate if the parameter is set to True.
    """
    ms = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    wrapper_mimic_old = MSWrapper.create_averages_from_ms(ms.name, SCAN_ID, SPW_ID, 1, perantave=False)
    old_wrapper = MSWrapper.create_from_ms(ms.name, SCAN_ID, SPW_ID)
    V_old = calc_vk(old_wrapper)
    nt.assert_array_almost_equal(
        wrapper_mimic_old.V['corrected_data'].real,
        V_old['corrected_data'].real,
        6  # decimal places
    )
    nt.assert_array_almost_equal(
        wrapper_mimic_old.V['corrected_data'].imag,
        V_old['corrected_data'].imag,
        6  # decimal places
    )


@skip_if_no_data_repo
def test_create_averages_from_ms_produces_comparable_sigma():
    """This test checks that the code implemented in PIPE-687 gives equivalent values to
    those of the old implementation given the change in the normalization.
    The test may be deleted in the future once the output is validated.
    The sigma column should be proportional to the old value of sigma.
    """
    ms = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    wrapper_mimic_old = MSWrapper.create_averages_from_ms(ms.name, SCAN_ID, SPW_ID, 1, perantave=False)
    old_wrapper = MSWrapper.create_from_ms(ms.name, SCAN_ID, SPW_ID)
    V_old = calc_vk(old_wrapper)
    real_factor = np.nanmean(wrapper_mimic_old.V['sigma'].real/V_old['sigma'].real)
    imag_factor = np.nanmean(wrapper_mimic_old.V['sigma'].imag/V_old['sigma'].imag)
    nt.assert_almost_equal(real_factor, imag_factor, 6)
