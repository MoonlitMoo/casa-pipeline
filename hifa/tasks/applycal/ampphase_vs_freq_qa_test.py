import pytest
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.tablereader import MeasurementSetReader

from .ampphase_vs_freq_qa import score_all_scans, Outlier


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
SCAN_ID = 10
SPW_ID = 0
# This is the list of outliers before applying the changes of PIPE-401
OUTLIERS_OLD = [
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={2}, pol={0}, num_sigma=46.45925775941749, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={5}, pol={0}, num_sigma=50.427459137967354, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={6}, pol={0}, num_sigma=81.1260146003469, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={7}, pol={0}, num_sigma=46.58214904798901, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={10}, pol={0}, num_sigma=53.79134545208964, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={4}, pol={1}, num_sigma=71.97263166794389, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={7}, pol={1}, num_sigma=181.57460781948814, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={8}, pol={1}, num_sigma=59.71412966520033, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={10}, pol={1}, num_sigma=44.56921373526944, reason={'phase_vs_freq.slope'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={3}, pol={0}, num_sigma=66.38239011759642, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={6}, pol={0}, num_sigma=78.72179148843134, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={7}, pol={0}, num_sigma=500.397537042222, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={4}, pol={1}, num_sigma=84.51872790161062, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={7}, pol={1}, num_sigma=139.932017593615, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False}),
    Outlier(vis={'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'}, intent={'TARGET'}, scan={SCAN_ID}, spw={SPW_ID},
            ant={10}, pol={1}, num_sigma=84.22975284611235, reason={'phase_vs_freq.intercept'}, phase_offset_gt90deg={False})
]


@skip_if_no_data_repo
def test_score_all_scans():
    ms = MeasurementSetReader.get_measurement_set(MS_NAME_DC)
    score = score_all_scans(ms, 'TARGET')
    assert score == []



