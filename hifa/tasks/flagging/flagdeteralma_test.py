import pytest
import numpy as np
import numpy.testing as nt
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.tablereader import MeasurementSetReader
from .flagdeteralma import (get_partialpol_spws, load_partialpols_alma, get_partialpol_flag_cmd_params,
                            convert_params_to_commands)


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
MS_NAME_ALT = casa_tools.utils.resolve("pl-unittest/uid___A002_Xcfc232_X2eda_test.ms")
PIPE1028DATA = casa_tools.utils.resolve("pl-unittest/PIPE-1028-data.npz")
OUTPUT_PARTIALPOL = [
"antenna='DA48&&DA50' spw='27:0~959' timerange='2019/12/26/00:53:32.112~2019/12/26/00:53:38.160' reason='partialpol'",
"antenna='DA54&&DV02' spw='27:0~959' timerange='2019/12/26/00:53:32.112~2019/12/26/00:53:38.160' reason='partialpol'",
"antenna='DV02&&DV14' spw='27:0~959' timerange='2019/12/26/00:53:32.112~2019/12/26/00:53:38.160' reason='partialpol'",
"antenna='DV08&&DV21' spw='27:0~959' timerange='2019/12/26/00:53:32.112~2019/12/26/00:53:38.160' reason='partialpol'",
"antenna='DA48&&DA50' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DA54&&DV02' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DA60&&DV22' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DA61&&DV19' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DV02&&DV14' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DV08&&DV21' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DV18&&DV21' spw='27:0~959' timerange='2019/12/26/00:53:38.160~2019/12/26/00:53:44.208' reason='partialpol'",
"antenna='DA48&&DA50' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DA54&&DV02' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DA60&&DV22' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DV02&&DV14' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DV08&&DV13' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DV08&&DV21' spw='27:0~959' timerange='2019/12/26/00:53:44.208~2019/12/26/00:53:50.256' reason='partialpol'",
"antenna='DA48&&DA50' spw='27:0~959' timerange='2019/12/26/00:53:50.256~2019/12/26/00:53:56.304' reason='partialpol'",
"antenna='DA54&&DV02' spw='27:0~959' timerange='2019/12/26/00:53:50.256~2019/12/26/00:53:56.304' reason='partialpol'",
"antenna='DA60&&DV22' spw='27:0~959' timerange='2019/12/26/00:53:50.256~2019/12/26/00:53:56.304' reason='partialpol'",
"antenna='DV02&&DV14' spw='27:0~959' timerange='2019/12/26/00:53:50.256~2019/12/26/00:53:56.304' reason='partialpol'",
"antenna='DV18&&DV21' spw='27:0~959' timerange='2019/12/26/00:53:50.256~2019/12/26/00:53:56.304' reason='partialpol'"
]


@skip_if_no_data_repo
def test_get_partialpol_spws_gets_correct_spw_list():
    """Test that partialpol gets the correct spws as mentioned in PIPE-1028"""
    spws, ddids = get_partialpol_spws(MS_NAME)
    spws_alt, ddids_alt = get_partialpol_spws(MS_NAME_ALT)
    assert spws == [0]
    assert spws_alt == [18]


@skip_if_no_data_repo
def test_science_spw_included_in_get_partialpol_spws():
    ms_alt = MeasurementSetReader.get_measurement_set(MS_NAME_ALT)
    science_spw_ids = [s.id for s in ms_alt.get_spectral_windows()]
    assert np.isin(science_spw_ids, get_partialpol_spws(MS_NAME_ALT))


@skip_if_no_data_repo
def test_get_partialpol_spws_gets_correct_datadescids_list():
    spws, ddids = get_partialpol_spws(MS_NAME)
    spws_alt, ddids_alt = get_partialpol_spws(MS_NAME_ALT)
    assert ddids == [0]
    assert ddids_alt == [18]


@skip_if_no_data_repo
def test_load_partialpols_alma_no_data():
    ms_alt = MeasurementSetReader.get_measurement_set(MS_NAME_ALT)
    assert load_partialpols_alma(ms_alt) == []


# Test the partial polarization routine
ant1 = np.array([0, 0, 1])
ant2 = np.array([1, 2, 2])
time = np.array([5.03825887e+09, 5.03825887e+09, 5.03825887e+09])
interval = np.array([10.08, 10.08, 10.08])
test_params_get_partialpol_flag_cmd_params = [
    (np.array([[[0, 1, 0]], [[0, 0, 0]]]),
     [{"ant1": 0, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [0]}]),
    (np.array([[[0, 1, 0],[0, 1, 0]], [[0, 0, 0],[0, 0, 0]]]),
     [{"ant1": 0, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [0, 1]}]),
    (np.array([[[0, 1, 0],[0, 1, 0]], [[0, 1, 0],[0, 0, 0]]]),
     [{"ant1": 0, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [1]}]),
    (np.array([[[0, 1, 0],[0, 1, 0],[0, 1, 0]], [[0, 1, 0],[0, 0, 0],[0, 0, 0]]]),
     [{"ant1": 0, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [1, 2]}]),
    (np.array([[[0, 1, 0],[0, 1, 0],[0, 1, 0]], [[0, 1, 0],[0, 0, 0],[0, 1, 1]]]),
     [{"ant1": 0, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [1]},
      {"ant1": 1, "ant2": 2, "time": 5.03825887e+09, "interval": 10.08, "channels": [2]},]),
]


@pytest.mark.parametrize("flags, expected", test_params_get_partialpol_flag_cmd_params)
def test_get_partialpol_flag_cmd_params(flags, expected):
    assert get_partialpol_flag_cmd_params(flags, ant1, ant2, time, interval) == expected


@skip_if_no_data_repo
def test_params_amd_commands_for_real_data():
    """
    This test is not a unit test as it combines get_partialpol_flag_cmd_params and convert_params_to_commands.
    It is a test on real data with partial polarization. The data correspond to the spw 27 of the reference
    dataset mentioned in PIPEREQ-70: uid://A002/Xe5ce70/X8b7
    """
    raw_data = np.load(PIPE1028DATA)
    flags = raw_data['flags']
    ant1 = raw_data['ant1']
    ant2 = raw_data['ant2']
    time  = raw_data['time']
    interval = raw_data['interval']
    ant_id_map = {
        0: 'DA41', 1: 'DA42', 2: 'DA43', 3: 'DA45', 4: 'DA46', 5: 'DA47', 6: 'DA48', 7: 'DA50', 8: 'DA51', 9: 'DA52',
        10: 'DA53',11: 'DA54', 12: 'DA55', 13: 'DA56', 14: 'DA57', 15: 'DA58', 16: 'DA60', 17: 'DA61', 18: 'DA62',
        19: 'DA63', 20: 'DA64', 21: 'DA65', 22: 'DV01', 23: 'DV02', 24: 'DV04', 25: 'DV05', 26: 'DV08', 27: 'DV10',
        28: 'DV11', 29: 'DV12', 30: 'DV13', 31: 'DV14', 32: 'DV15', 33: 'DV16', 34: 'DV17', 35: 'DV18', 36: 'DV19',
        37: 'DV20', 38: 'DV21', 39: 'DV22', 40: 'DV23', 41: 'DV24', 42: 'DV25'
    }
    params = get_partialpol_flag_cmd_params(flags, ant1, ant2, time, interval)
    updated_params = [{**d, "spw": 27, "time_unit": 's'} for d in params]
    assert len(OUTPUT_PARTIALPOL) == len(updated_params)
    commands = convert_params_to_commands(None, updated_params, ant_id_map=ant_id_map)
    assert commands == OUTPUT_PARTIALPOL


test_params_convert_params_to_commands = [
    ([{"ant1": 0,
       "ant2": 2,
       "time": 5.03825887e+09,
       "interval": 10.08,
       "channels": [1, 2],
       "spw":18,
       "time_unit": "s"}],
     ["antenna='CM01&&CM03' spw='18:1~2' timerange='2018/07/14/04:21:04.960~2018/07/14/04:21:15.040' reason='partialpol'"]),
    ([{"ant1": 1,
       "ant2": 2,
       "time": 5.03825887e+09,
       "interval": 10.08,
       "channels": [1, 2, 3, 4, 6, 7, 8],
       "spw":18,
       "time_unit": "s"}],
     ["antenna='CM02&&CM03' spw='18:1~4;6~8' timerange='2018/07/14/04:21:04.960~2018/07/14/04:21:15.040' reason='partialpol'"]),
    ([{"ant1": 1,
       "ant2": 2,
       "time": 5.03825887e+09,
       "interval": 10.08,
       "channels": [1, 2, 3, 4, 6, 7, 8, 12],
       "spw":18,
       "time_unit": "s"}],
     ["antenna='CM02&&CM03' spw='18:1~4;6~8;12' timerange='2018/07/14/04:21:04.960~2018/07/14/04:21:15.040' reason='partialpol'"]),
]

@skip_if_no_data_repo
@pytest.mark.parametrize("input_dict, expected", test_params_convert_params_to_commands)
def test_convert_params_to_commands(input_dict, expected):
    ms_alt = MeasurementSetReader.get_measurement_set(MS_NAME_ALT)
    assert convert_params_to_commands(ms_alt, input_dict) == expected


