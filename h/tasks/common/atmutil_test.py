"""Test module for atmutil.py."""
import math
import pytest
from typing import Tuple, Union

import numpy as np

from pipeline.infrastructure import casa_tools

from .atmutil import init_at, calc_airmass, calc_transmission
from .atmutil import get_dry_opacity, get_wet_opacity
from .atmutil import test
from .atmutil import get_spw_spec, get_median_elevation, get_transmission
from .atmutil import AtmType

defaultAtm = dict(humidity=20.0, temperature=270.0, pressure=560.0,
                  atmtype=AtmType.midLatitudeWinter, altitude=5000.0,
                  fcenter=100.0, nchan=4096, resolution=0.001)
vis = 'uid___A002_X85c183_X36f.ms'

def __update_atmparam(in_param: dict) -> dict:
    """
    Merge test specific atmospheric parameters with default ones.
    
    Args:
        in_param: A dictionary that specifies non-default parameters
            to initialize atmospheric model. The key is parameter
            name and value is corresponding parameter value. See
            parameters of atmutil.init_at for available parameters.

    Returns:
        A dictionary of atmospheric parameters to be used in init_at.
    """
    atmparam = defaultAtm.copy()
    atmparam.update(in_param)
    return atmparam
    

@pytest.mark.parametrize("in_param",
                         ({}, dict(humidity=10.0),
                          dict(pressure=590.0),
                          dict(atmtype=AtmType.midLatitudeSummer),
                          dict(atmtype=AtmType.tropical),
                          dict(atmtype=AtmType.subarcticSummer),
                          dict(atmtype=AtmType.midLatitudeWinter),
                          dict(fcenter=350.0), dict(nchan=128),
                          dict(resolution=0.000015)) )
def test_init_at(in_param: dict):
    """
    Test init_at.
    
    Initialize atmosphere with various parameter values and validate
    parameters set to casa atmosphere tool.
    
    Args:
        in_param: A dictionary that specifies non-default parameters
            to initialize atmospheric model. The key is parameter
            name and value is corresponding parameter value. See
            parameters of atmutil.init_at for available parameters.
    """
    myat = casa_tools.atmosphere
    myqa = casa_tools.quanta
    # Merge default and test parameters.
    in_atmparam = __update_atmparam(in_param)
    # Invoke init_at with a given parameter set
    init_at(myat, **in_atmparam)
    # Obtain parameters set to atmosphere tool and compare it with inputs
    atmparams = myat.getBasicAtmParms()
    altitude = myqa.getvalue(myqa.convert(atmparams[1], 'm'))
    temperature = myqa.getvalue(myqa.convert(atmparams[2], 'K'))
    pressure = myqa.getvalue(myqa.convert(atmparams[3], 'mbar'))
    humidity = atmparams[5]
    nchan = myat.getNumChan(0)
    resolution = myqa.getvalue(myqa.convert(myat.getChanSep(0), 'GHz'))
    fcenter = myqa.getvalue(myqa.convert(myqa.add(myat.getChanFreq(0,0), myat.getChanFreq(nchan-1)), 'GHz'))*0.5
    assert np.allclose(altitude, in_atmparam['altitude'])
    assert np.allclose(temperature, in_atmparam['temperature'])
    assert np.allclose(pressure, in_atmparam['pressure'])
    assert np.allclose(humidity, in_atmparam['humidity'])
    assert np.allclose(nchan, in_atmparam['nchan'])
    assert np.allclose(resolution, in_atmparam['resolution'])
    assert np.allclose(fcenter,  in_atmparam['fcenter'])
    

@pytest.mark.parametrize("elevation, expected_airmass",
                         ((1.0, 57.29868849855063),
                          (15., 3.8637033051562737),
                          (30., 2.0),
                      (45., math.sqrt(2.0)),
                          (60., 1.1547005383792515),
                          (75., 1.035276180410083),
                          (90., 1.0))
                         )
def test_calc_airmass(elevation: float, expected_airmass: float):
    """
    Test atmutil.calc_airmass for various elevation.
    
    Args:
        elevation: Input elevation.
        expected_airmass: Expected air mass for the elevation.
    """
    airmass = calc_airmass(elevation)
    assert np.allclose(airmass, expected_airmass, rtol=1.e-5, atol=0.0)


@pytest.mark.parametrize("in_param, expected",
                         ( ((1.0, 0.15, 0.10), 0.7788007830714049),
                           ((2.0, 0.15, 0.10), 0.6065306597126334),
                           ((2.0, np.array([0.075, 0.10]), np.array([0.05, 0.15])),
                            np.array([0.7788007830714049, 0.6065306597126334]))
                         ))
def test_calc_transmission(in_param: Tuple[float, Union[float, np.ndarray],
                                           Union[float, np.ndarray]],
                           expected: Union[float, np.ndarray]):
    """
    Test calc_transmission.
    
    Args:
        in_param: A tuple of (airmass, dry_opacity, wet_opacity) to be used
            in calc_transmission.
        
        expected: Expected return values. 
    """
    transmission = calc_transmission(in_param[0], in_param[1],
                                     in_param[2])
    assert np.allclose(transmission, expected, rtol=1.e-5, atol=0.0)


@pytest.mark.parametrize("in_param, expected",
                         ( ({}, 48.135443881030234),
                          (dict(humidity=10.0), 48.13438655147836),
                          (dict(pressure=590.0), 71.9255021742992),
                          (dict(atmtype=AtmType.midLatitudeSummer), 48.183451347527715),
                          (dict(atmtype=AtmType.tropical), 49.17959626425098),
                          (dict(atmtype=AtmType.subarcticSummer), 47.17390377363715),
                          (dict(atmtype=AtmType.midLatitudeWinter), 48.135443881030234),
                          (dict(fcenter=350.0), 99.28050952919153),
                          (dict(nchan=128), 1.395500829957058),
                          (dict(resolution=0.000015), 44.65368805886007)
                          ))
def test_get_dry_opacity(in_param: dict, expected: float):
    """
    Test get_dry_opacity.
    
    Args:
        in_param: A dictionary that specifies non-default parameters
            to initialize atmospheric model. The key is parameter
            name and value is corresponding parameter value. See
            parameters of atmutil.init_at for available parameters.
        expected: Expected sum of dry opacity.
    """
    myat = casa_tools.atmosphere
    in_atmparam = __update_atmparam(in_param)
    init_at(myat, **in_atmparam)
    dry_arr = get_dry_opacity(myat)
    assert np.allclose(np.sum(dry_arr), expected, rtol=1.e-5, atol=0.0)

@pytest.mark.parametrize("in_param, expected",
                         ( ({}, 45.44895888932035),
                          (dict(humidity=10.0), 22.740003586706898),
                          (dict(pressure=590.0), 48.32597615284148),
                          (dict(atmtype=AtmType.midLatitudeSummer), 45.54326843570722),
                          (dict(atmtype=AtmType.tropical), 45.57186180675734),
                          (dict(atmtype=AtmType.subarcticSummer), 45.45064451055434),
                          (dict(atmtype=AtmType.midLatitudeWinter), 45.44895888932035),
                          (dict(fcenter=350.0), 1040.320478812408),
                          (dict(nchan=128), 1.4198806069942795),
                          (dict(resolution=0.000015), 45.436160024392436)
                          ))
def test_get_wet_opacity(in_param: dict, expected: float):
    """
    Test get_wet_opacity.
    
    Args:
        in_param: A dictionary that specifies non-default parameters
            to initialize atmospheric model. The key is parameter
            name and value is corresponding parameter value. See
            parameters of atmutil.init_at for available parameters.
        expected: Expected sum of wet opacity.
    """
    myat = casa_tools.atmosphere
    in_atmparam = __update_atmparam(in_param)
    init_at(myat, **in_atmparam)
    dry_arr = get_wet_opacity(myat)
    assert np.allclose(np.sum(dry_arr), expected, rtol=1.e-5, atol=0.0)

@pytest.mark.parametrize("in_param, expected",
                         (((1.0, 90.0), 0.9813479717655601),
                          ((1.0, 30.0), 0.9630496887893027),
                          ((1.5, 90.0), 0.9778805819476384),
                          ))
def test_test(in_param: Tuple[float, float], expected: float):
    """
    Test method, test.
    
    Args:
        in_param: A tuple of (pwv, elevation) to be used to invoke method, test.
        expected: Expected mean of transmission.
    """
    transmission = test(in_param[0], in_param[1])
    assert np.allclose(np.mean(transmission), expected, rtol=1.e-5, atol=0.0)

@pytest.mark.skip(reason='need_vis')
@pytest.mark.parametrize("spwid, expected",
                         ((15, (114.68215, 128, 0.015625)),
                          (17, (100.95000, 4080, -0.000488281))
                          ))
def test_get_spw_spec(spwid: int, expected: Tuple[float, int, float]):
    """
    Test get_spw_spec.
    
    Arg:
        spwid: A spwctral window ID to get spw specification.
        expected: An expected spw specification in a tuple of
            (the center frequency, nchan, resolution). 
    """
    fcenter, nchan, resolution = get_spw_spec(vis, spwid)
    assert np.allclose(fcenter, expected[0], rtol=1.e-8, atol=0.0)
    assert nchan == expected[1]
    assert np.allclose(resolution, expected[2], rtol=1.e-5, atol=0.0)

@pytest.mark.skip(reason='need_vis')
@pytest.mark.parametrize("antid, expected",
                         ((0, 51.137000119425466),
                          (1, 51.13678069960953),
                          (2, 51.13635643503322)
                          ))
def test_get_median_elevation(antid: int, expected: float):
    """
    Test get_median_elevation.
    
    Args:
        antid: Antenna ID to calculate median elevation.
        expected: An expected median elevation.
    """
    elevation = get_median_elevation(vis, antid)
    assert np.allclose(elevation, expected, rtol=1.e-5, atol=0.0)

@pytest.mark.skip(reason='need_vis')
@pytest.mark.parametrize("antid, spwid, expected",
                         ((0, 15, 0.8433381888309945),
                          (1, 17, 0.9683929298207594)
                          ))
def test_get_transmission(antid: int, spwid: int, expected: float):
    """
    Test get_transmission.
    
    Args:
        antid: An Antenna ID to execute.
        spwid: A spectral window ID to execute.
        expected: An expected mean transmission.
    """
    f, transmission = get_transmission(vis, antid, spwid)
    assert np.allclose(np.mean(transmission), expected)