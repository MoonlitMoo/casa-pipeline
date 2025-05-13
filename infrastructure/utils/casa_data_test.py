from datetime import datetime
import re
import pytest
from .. import casa_tools
from .casa_data import (
    get_file_md5,
    get_iso_mtime,
    get_solar_system_model_files,
    get_filename_info,
    get_object_info_string,
    IERSInfo,
    from_mjd_to_datetime
)


# Validation of ISO 8601 strings
REGEX_ISO8601 = (
    r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T'
    r'(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'
)
match_iso8601 = re.compile(REGEX_ISO8601).match


def validate_iso8601(value: str) -> bool:
    """Check if a string follows a valid ISO 8601 format"""
    return True if match_iso8601(value) is not None else False


# # Tests that depend on the pipeline-testdata repository
TEST_DATA_PATH = casa_tools.utils.resolve('pl-unittest/casa_data')
# Skip tests if CASA cannot resolve to an absolute path
skip_data_tests = not TEST_DATA_PATH.startswith('/')

# Create decorator with reason to skip tests
skip_if_no_data_repo = pytest.mark.skipif(
    skip_data_tests,
    reason="The repo pipeline-testdata is not set up for the tests"
)


SOLAR_SYSTEM_MODELS_PATH = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels')
SS_CALLISTO = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Callisto_Tb.dat')
SS_CERES = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Ceres_Tb.dat')
SS_CERES_FD = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Ceres_fd_time.dat')


@skip_if_no_data_repo
def test_md5_hash_string_solar_system_models():
    assert get_file_md5(SS_CALLISTO) == "d943f9e40d13c433213de24a01a488d8"
    assert get_file_md5(SS_CERES) == "91a16285f38c9ed05e18cbf3d3573463"
    assert get_file_md5(SS_CERES_FD) == "a6200c8a75195bb43f80c3576907a12e"


@skip_if_no_data_repo
def test_get_iso_mtime():
    # At the moment we only check that the format is roughly an ISO date
    iso_mtime = get_iso_mtime(SS_CALLISTO)
    assert validate_iso8601(iso_mtime)


@skip_if_no_data_repo
def test_retrieve_corresponding_solar_system_models_with_one_file():
    callisto_ss = get_solar_system_model_files("Callisto", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert len(callisto_ss) == 1


@skip_if_no_data_repo
def test_retrieve_corresponding_solar_system_models_with_two_files():
    ceres_ss = get_solar_system_model_files("Ceres", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert len(ceres_ss) == 2


@skip_if_no_data_repo
def test_get_filename_info():
    ceres_info = get_filename_info(SS_CERES)
    ceres_fd_info = get_filename_info(SS_CERES_FD)
    callisto_info = get_filename_info(SS_CALLISTO)
    assert callisto_info["MD5"] == "d943f9e40d13c433213de24a01a488d8"
    assert ceres_info["MD5"] == "91a16285f38c9ed05e18cbf3d3573463"
    assert ceres_fd_info["MD5"] == "a6200c8a75195bb43f80c3576907a12e"
    assert validate_iso8601(callisto_info["mtime"])


@skip_if_no_data_repo
def test_info_string_solar_system_models_with_one_file():
    info_string = get_object_info_string("Callisto", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert info_string.startswith('Solar System models used for Callisto => {"Callisto')
    assert "d943f9e40d13c433213de24a01a488d8" in info_string
    assert "mtime" in info_string
    assert len(info_string.split("MD5")) == 2
    assert info_string.endswith('}}')


@skip_if_no_data_repo
def test_info_string_solar_system_models_with_two_files():
    info_string = get_object_info_string("Ceres", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert info_string.startswith('Solar System models used for Ceres => {"Ceres')
    assert "91a16285f38c9ed05e18cbf3d3573463" in info_string
    assert "a6200c8a75195bb43f80c3576907a12e" in info_string
    assert "mtime" in info_string
    assert len(info_string.split("MD5")) == 3
    assert info_string.endswith('}}')


GEODETIC_PATH = casa_tools.utils.resolve('pl-unittest/casa_data/geodetic')
WRONG_GEODETIC_PATH = casa_tools.utils.resolve('pl-unittest/casa_data/geodetic_wrong')


@skip_if_no_data_repo
def test_IERSInfo_class_creation():
    IERSInfo(iers_path=GEODETIC_PATH, load_on_creation=False)


@skip_if_no_data_repo
def test_IERSInfo_class_loads_data():
    IERSInfo(iers_path=GEODETIC_PATH)


@skip_if_no_data_repo
def test_IERSInfo_get_IERS_version_method():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    version_IERSeop2000 = iers_info.get_IERS_version("IERSeop2000")
    assert version_IERSeop2000 == '0001.0144'
    version_IERSpredict = iers_info.get_IERS_version("IERSpredict")
    assert version_IERSpredict == '0623.0351'


@skip_if_no_data_repo
def test_IERSInfo_get_IERSeop2000_last_entry_method():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    mjd = iers_info.get_IERS_last_entry()
    assert mjd == 59184.0


@skip_if_no_data_repo
def test_get_IERS_info():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    assert len(iers_info.info["versions"]) == 2
    assert iers_info.info["versions"]["IERSeop2000"] == '0001.0144'
    assert iers_info.info["versions"]["IERSpredict"] == '0623.0351'
    assert iers_info.info["IERSeop2000_last_MJD"] == 59184.0


@skip_if_no_data_repo
def test_validate_date_method():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    assert iers_info.validate_date(datetime(2020, 12, 1, 0, 0))
    assert not iers_info.validate_date(datetime(2021, 12, 1, 0, 0))
    assert iers_info.validate_date(datetime(2019, 12, 1, 0, 0))


@skip_if_no_data_repo
def test_string_representation_for_IERS_info():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    assert str(iers_info) == (
        'IERS table information => {"versions": {"IERSpredict": "0623.0351", '
        '"IERSeop2000": "0001.0144"}, "IERSeop2000_last_MJD": 59184.0, '
        '"IERSeop2000_last": "2020-12-01 00:00:00", "IERSpredict_last": "2021-04-25 00:00:00"}'
    )

@skip_if_no_data_repo
def test_date_message_type():
    iers_info = IERSInfo(iers_path=GEODETIC_PATH)
    # Before the end of IERSeop2000
    assert iers_info.date_message_type(datetime(2019, 12, 1, 0, 0)) == "GOOD"
    # At the end of IERSeop2000
    assert iers_info.date_message_type(datetime(2020, 12, 1, 0, 0)) == "GOOD"
    # Between the end of IERSeop2000 and IERSeop2000+3 months
    assert iers_info.date_message_type(datetime(2020, 12, 2, 0, 0)) == "INFO"
    # Between IERSeop2000 + 3 months and the end of IERSpredict
    assert iers_info.date_message_type(datetime(2021, 3, 25, 0, 0)) == "WARN"
    # At the end of IERSpredict
    assert iers_info.date_message_type(datetime(2021, 4, 25, 0, 0)) == "WARN"
    # After the end of IERSpredict
    assert iers_info.date_message_type(datetime(2021, 12, 1, 0, 0)) == "CRITICAL"


def test_date_message_type_when_data_is_not_found():
    iers_info = IERSInfo(iers_path=WRONG_GEODETIC_PATH)
    assert iers_info.date_message_type(datetime(2020, 12, 1, 0, 0)) == 'CRITICAL'


def test_from_mjd_to_datetime():
    assert from_mjd_to_datetime(59184.0) == datetime(2020, 12, 1, 0, 0)


def test_IERSInfo_when_data_is_not_found():
    IERSInfo(iers_path=WRONG_GEODETIC_PATH)


def test_get_IERS_info_when_data_is_not_found():
    iers_info = IERSInfo(iers_path=WRONG_GEODETIC_PATH)
    assert len(iers_info.info["versions"]) == 2
    assert iers_info.info["versions"]["IERSeop2000"] == 'NOT FOUND'
    assert iers_info.info["versions"]["IERSpredict"] == 'NOT FOUND'
    assert iers_info.info["IERSeop2000_last_MJD"] == 'NOT FOUND'
    assert iers_info.info["IERSeop2000_last"] is None


def test_validate_date_method_when_data_is_not_found():
    iers_info = IERSInfo(iers_path=WRONG_GEODETIC_PATH)
    assert not iers_info.validate_date(datetime(2020, 12, 1, 0, 0))
    assert not iers_info.validate_date(datetime(2021, 12, 1, 0, 0))
    assert not iers_info.validate_date(datetime(2019, 12, 1, 0, 0))


def test_string_representation_for_IERS_info_when_data_is_not_found():
    iers_info = IERSInfo(iers_path=WRONG_GEODETIC_PATH)
    assert str(iers_info) == (
        'IERS table information => {"versions": {"IERSpredict": "NOT FOUND", '
        '"IERSeop2000": "NOT FOUND"}, "IERSeop2000_last_MJD": "NOT FOUND", '
        '"IERSeop2000_last": null, "IERSpredict_last": null}'
    )
