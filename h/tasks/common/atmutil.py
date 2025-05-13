"""
Utility module to calculate the atmospheric transmission.

Examples:
    Calculate using metadata in MeasurementSet
    >>> (freq, trans) = atmutil.get_transmission('M100.ms', antenna_id=1, spw_id=18, doplot=True)
"""
import math
import os
from typing import Union, Tuple, Optional

import casatools
import matplotlib.pyplot as plt
import numpy as np

import pipeline.extern.adopted as adopted
from pipeline.infrastructure import casa_tools, get_logger

LOG = get_logger(__name__)


class AtmType(object):
    """Atmosphere type enum class."""

    tropical = 1
    midLatitudeSummer = 2
    midLatitudeWinter = 3
    subarcticSummer = 4
    subarcticWinter = 5


def init_at(at: casatools.atmosphere, humidity: float = 20.0,
            temperature: float = 270.0, pressure: float = 560.0,
            atmtype: AtmType = AtmType.midLatitudeWinter, altitude: float = 5000.0,
            fcenter: float = 100.0, nchan: float = 4096, resolution: float = 0.001):
    """
    Initialize atmospheric profile and spectral window setting.

    Initialize atmospheric profile and spectral window setting in CASA
    atmosphere tool using input antenna site parameters, the center
    frequency, and the width of a frequency range.

    Args:
        at: CASA atmosphere tool instance to initialize.
        humidity: The relative humidity at the ground (unit: %).
        temperature: The temperature at the ground (unit: K).
        pressure: The pressure at the ground (unit: mbar).
        atmtype: An AtmType enum that defines a type of atmospheric profile.
        altitude: The altitude of antenna site to calculate atmospheric
            transmission (unit: m).
        fcenter: Center frequency of the frequency range (unit: GHz).
        nchan: Number of channels in the frequency range.
        resolution: Frequency resolution (unit: GHz/ch).
     """
    myqa = casa_tools.quanta
    at.initAtmProfile(humidity=humidity,
                      temperature=myqa.quantity(temperature, 'K'),
                      altitude=myqa.quantity(altitude, 'm'),
                      pressure=myqa.quantity(pressure, 'mbar'),
                      atmType=atmtype)
    fwidth = nchan * resolution
    at.initSpectralWindow(nbands=1,
                          fCenter=myqa.quantity(fcenter, 'GHz'),
                          fWidth=myqa.quantity(fwidth, 'GHz'),
                          fRes=myqa.quantity(resolution, 'GHz'))


def init_atm(at: casatools.atmosphere, altitude: float = 5000.0, temperature: float = 270.0, pressure: float = 560.0,
             max_altitude: float = 48.0, humidity: float = 20.0, delta_p: float = 10.0, delta_pm: float = 1.2,
             h0: float = 2.0, atmtype: int = AtmType.midLatitudeWinter):
    """
    Initialize atmospheric profile in CASA for a given location described by
    input atmosphere parameters.

    Args:
        at: CASA atmosphere tool instance to initialize.
        altitude: Altitude of the location (unit: m).
        temperature: Temperature at the ground (unit: K).
        pressure: Pressure at the ground (unit: mbar).
        max_altitude: Top height of atmospheric profile (unit: km).
        humidity: Relative humidity at the ground (unit: %).
        delta_p: Initial step of pressure (unit: mbar).
        delta_pm: Multiplicative factor of pressure steps.
        h0: Scale height of water vapor (unit: km).
        atmtype: AtmType enum that defines a type of atmospheric profile.
    """
    myqa = casa_tools.quanta
    at.initAtmProfile(altitude=myqa.quantity(altitude, 'm'),
                      temperature=myqa.quantity(temperature, 'K'),
                      pressure=myqa.quantity(pressure, 'mbar'),
                      maxAltitude=myqa.quantity(max_altitude, 'km'),
                      humidity=humidity,
                      dP=myqa.quantity(delta_p, 'mbar'),
                      dPm=delta_pm,
                      h0=myqa.quantity(h0, 'km'),
                      atmType=atmtype)


def init_spw(at: casatools.atmosphere, fcenter: float = 100.0, nchan: float = 4096, resolution: float = 0.001):
    """
    Initialize spectral window setting in CASA atmosphere tool using spectral window frequencies.

    Args:
        at: CASA atmosphere tool instance to initialize.
        fcenter: Center frequency of spectral window (unit: GHz).
        nchan: Number of channels in spectral window.
        resolution: Resolution of spectral window (unit: GHz).
    """
    myqa = casa_tools.quanta
    fwidth = nchan * resolution
    at.initSpectralWindow(nbands=1,
                          fCenter=myqa.quantity(fcenter, 'GHz'),
                          fWidth=myqa.quantity(fwidth, 'GHz'),
                          fRes=myqa.quantity(resolution, 'GHz'))


def calc_airmass(elevation: float = 45.0) -> float:
    """
    Calculate the relative airmass of a given elevation angle.

    Args:
        elevation: An angle of elevation (unit: degree).

    Returns:
        The relative airmass to the one at zenith.
    """
    return 1.0 / math.cos(math.radians(90.0 - elevation))


def calc_transmission(airmass: float, dry_opacity: Union[float, np.ndarray],
                      wet_opacity: Union[float, np.ndarray]) -> Union[float, np.ndarray]:
    """
    Calculate total atmospheric transmission.

    Calculate total atmospheric transmission from the zenith opacities and
    relative airmass.

    Args:
        airmass: The relative airmass to the zenith one.
        dry_opacity: The integrated zenith dry opacity.
        wet_opacity: The integrated zenith wet opacity.

    Returns:
        The atmospheric transmission.
    """
    return np.exp(-airmass * (dry_opacity + wet_opacity))


def get_dry_opacity(at: casatools.atmosphere) -> np.ndarray:
    """
    Obtain the integrated zenith opacity of dry species.

    Args:
        at: Atmosphere tool instance initialized by a spectral window and site
            parameter settings.

    Returns:
        An array of the zenith integrated opacity of dry species for each
        channel of the first spectral window.
    """
    dry_opacity_result = at.getDryOpacitySpec(0)
    dry_opacity = np.asarray(dry_opacity_result[1])
    return dry_opacity


def get_wet_opacity(at: casatools.atmosphere) -> np.ndarray:
    """
    Obtain the integrated zenith opacity of wet species.

    Args:
        at: Atmosphere tool instance initialized by a spectral window and site
            parameter settings.

    Returns:
        An array of the zenith integrated opacity of wet species for each
        channel of the first spectral window.
    """
    wet_opacity_result = at.getWetOpacitySpec(0)
    wet_opacity = np.asarray(wet_opacity_result[1]['value'])
    return wet_opacity


def test(pwv: float = 1.0, elevation: float = 45.0) -> np.ndarray:
    """
    Calculate atmospheric transmission and generate a plot.

    Calculate atmospheric transmission of a given PWV and elevation angle.
    The default parameter values of init_at function are used to initialize
    atmospheric and spectral window settings. A plot of atmospheric
    transmission, wet and dry zenith opacities of each channel of the spectral
    window is also generated.

    Args:
        pwv: The zenith water vapor column for forward radiative transfer
            calculation (unit: mm).
        elevation: The angle of elevation (unit: degree).

    Returns:
        An array of atmospheric transmission of each channel of the spectral window.
    """
    myat = casa_tools.atmosphere
    myqa = casa_tools.quanta
    init_at(myat)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))
    frequency = myqa.getvalue(myqa.convert(myat.getSpectralWindow(), 'GHz'))

    airmass = calc_airmass(elevation)

    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)

    plot(frequency, dry_opacity, wet_opacity, transmission)

    return transmission


def plot(frequency: np.ndarray, dry_opacity: np.ndarray,
         wet_opacity: np.ndarray, transmission: np.ndarray):
    """
    Generate a plot of atmospheric transmission, wet and dry opacities.

    Generate a twin axes plot of atmospheric transmission, wet and dry
    opacities by matplotlib.

    Args:
        frequency: An array of frequency values.
        dry_opacity: The integrated dry opacity at each frequency.
        wet_opacity: The integrated wet opacity at each frequency.
        transmission: The atmospheric transmission at each frequency.
    """
    plt.clf()
    a1 = plt.gcf().gca()
    plt.plot(frequency, dry_opacity, label='dry')
    plt.plot(frequency, wet_opacity, label='wet')
    plt.legend(loc='upper left', bbox_to_anchor=(0., 0.5))
    a2 = a1.twinx()
    a2.yaxis.set_major_formatter(plt.NullFormatter())
    a2.yaxis.set_major_locator(plt.NullLocator())
    plt.gcf().sca(a2)
    plt.plot(frequency, transmission, 'm-')
    M = transmission.min()
    Y = 0.8
    ymin = (M - Y) / (1.0 - Y)
    ymax = transmission.max() + (1.0 - transmission.max()) * 0.1
    plt.ylim([ymin, ymax])


def get_spw_spec(vis: str, spw_id: int) -> Tuple[float, int, float]:
    """
    Calculate spectral setting of a spectral window.

    Calculate the center frequency, number of channels, and channel resolution
    of a spectral window in a MeasurementSet. The values can be passed to
    init_at function to initialize spectral window setting in atmosphere tool.

    Args:
        vis: Path to MeasurementSet.
        spw_id: A spectral window ID to select.

    Returns:
        A three element tuple of the center frequency in GHz, number of
        channels, and resolution in GHz.
    """
    with casa_tools.TableReader(os.path.join(vis, 'SPECTRAL_WINDOW')) as mytb:
        nrow = mytb.nrows()
        if spw_id < 0 or spw_id >= nrow:
            raise RuntimeError('spw_id {} is out of range'.format(spw_id))
        nchan = mytb.getcell('NUM_CHAN', spw_id)
        chan_freq = mytb.getcell('CHAN_FREQ', spw_id)

    center_freq = (chan_freq.min() + chan_freq.max()) / 2.0
    resolution = chan_freq[1] - chan_freq[0]

    # Hz -> GHz
    toGHz = 1.0e-9
    center_freq *= toGHz
    resolution *= toGHz

    return center_freq, nchan, resolution


def get_median_elevation(vis: str, antenna_id: int) -> float:
    """
    Calculate the median elevation of pointing directions of an antenna.

    Calculate the median elevation of pointing directions of an antenna in a
    MeasurementSet. The pointing directions are obtained from the DIRECTION
    column in POINTING subtable. Only supports DIRECTION column in AZELGEO
    coordinate frame and the unit in radian.

    Args:
        vis: Path to MeasurementSet.
        antenna_id: The antenna ID to select.

    Returns:
        The median of elevation of selected antenna (unit: degree).
        Returns 45.0 if DIRECTION is not in AZELGEO or the value is invalid
        i.e. not within (0.0, 90.0].

    Raises:
        RuntimeError: An error when DIRECTION column has unsupported coodinate
            unit.
    """
    with casa_tools.TableReader(os.path.join(vis, 'POINTING')) as mytb:
        tsel = mytb.query('ANTENNA_ID == {}'.format(antenna_id))
        # default elevation
        elevation = 45.0
        try:
            if tsel.nrows() > 0:
                colkeywords = tsel.getcolkeywords('DIRECTION')
                if colkeywords['MEASINFO']['Ref'] == 'AZELGEO':
                    if not (colkeywords['QuantumUnits'] == 'rad').all():
                        raise RuntimeError('The unit must be radian. Got {}'.format(str(colkeywords['QuantumUnits'])))
                    elevation_list = tsel.getcol('DIRECTION')[1][0]
                    median_elevation = math.degrees(np.median(elevation_list))

                    # check if the value is in reasonable range
                    if 0.0 < median_elevation and median_elevation <= 90.0:
                        elevation = median_elevation
                    else:
                        LOG.attention(
                            f'Encountered invalid median elevation value, {median_elevation:.4g}deg. '
                            f'Use {elevation:.2g}deg instead.'
                        )
        finally:
            tsel.close()

    return elevation


def get_representative_elevation(vis, antenna_id: int) -> float:
    """Return representative elevation value computed from phasecenter.

    Representative elevation value is defined as the median elevation
    of the first science field during the observation.
    If there are multiple observations (OBSERVATION rows) in the MS,
    start/end times of the observation are determined by the min/max
    of time ranges of all observations.

    Args:
        vis: Path to MeasurementSet.
        antenna_id: The antenna ID to select.

    Returns:
        Elevation value in degree.
    """
    myqa = casa_tools.quanta
    myme = casa_tools.measures

    # collect necessary information using msmd tool
    with casa_tools.MSMDReader(vis) as mymsmd:
        mposition = mymsmd.antennaposition(antenna_id)
        target_field = mymsmd.fieldsforintent('OBSERVE_TARGET*')[0]
        time_range = mymsmd.timerangeforobs(0)
        for i in range(1, mymsmd.nobservations()):
            another_time_range = mymsmd.timerangeforobs(i)
            if myqa.lt(another_time_range['begin']['m0'], time_range['begin']['m0']):
                time_range['begin'] = another_time_range['begin']
            if myqa.gt(another_time_range['end']['m0'], time_range['end']['m0']):
                time_range['end'] = another_time_range['end']
        start_time = myqa.convert(time_range['begin']['m0'], 's')['value']
        end_time = myqa.convert(time_range['end']['m0'], 's')['value']
        mdirection = mymsmd.phasecenter(target_field)

    # convert phasecenter to AZEL and return elevation in degree
    def _elevation_at(time_in_sec: float) -> float:
        myme.done()
        mepoch = myme.epoch('UTC', myqa.quantity(time_in_sec, 's'))
        myme.doframe(mepoch)
        myme.doframe(mposition)
        azel = myme.measure(mdirection, 'AZELGEO')
        myme.done()
        elevation = myqa.convert(azel['m1'], 'deg')['value']
        return elevation

    # compute elevation value at 1 min (=60sec) interval
    # and take median of computed list of elevations
    elevation_list = [_elevation_at(t) for t in np.arange(start_time, end_time, 60)]
    median_elevation = np.median(elevation_list)

    return median_elevation


def get_altitude(vis: str) -> float:
    """Return observatory-dependent altitude in meter.

    This method gets observatory name from MS, and return
    appropriate altitude value depending on the observatory
    name.

    Args:
        vis: Path to MeasurementSet.

    Returns:
        Altitude value in meter. It only supports ALMA, VLA, and NRO.
        Otherwise, the method returns default value, 5000m.
    """
    with casa_tools.MSMDReader(vis) as mymsmd:
        observatory = mymsmd.observatorynames()[0]

    if observatory.find('ALMA') != -1 or observatory.find('ACA') != -1:
        altitude = 5059
    elif observatory.find('VLA') != -1:
        altitude = 2124
    elif observatory.find('NRO') != -1 or observatory.find('Nobeyama') != -1:
        altitude = 1300
    else:
        # default value is 5000m
        altitude = 5000

    return altitude


def get_transmission(vis: str, antenna_id: int = 0, spw_id: int = 0,
                     doplot: bool = False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate atmospheric transmission of an antenna and spectral window.

    Calculate the atmospheric transmission of a given spectral window for an
    elevation angle corresponding to pointings of a given antenna in a
    MeasurementSet.

    Args:
        vis: Path to MeasurementSet.
        spw_id: A spectral window ID.
        antenna_id: The antenna ID.
        doplot: If True, plot the atmospheric transmission and opacities.

    Returns:
        A tuple of 2 arrays. The first one is an array of frequencies in the
        unit of GHz, and the other is the atmospheric transmission at each
        frequency.
    """
    center_freq, nchan, resolution = get_spw_spec(vis, spw_id)

    return get_transmission_for_range(vis, center_freq, nchan, resolution, antenna_id, doplot)


def get_transmission_for_range(vis: str, center_freq: float, nchan: int, resolution: float, antenna_id: int = 0, doplot: bool = False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate atmospheric transmission covering a range of frequency.

    A number of channels, a resolution of a frequency range, and
    the median of elevations in all pointings of the selected antenna
    are used to calculate the atmospheric transmission.
    The atmospheric profile is constructed by default site parameters
    of the function, init_at.
    The median of zenith water vapor column (pwv) is used to calculate
    the transmission.

    Args:
        vis: Path to MeasurementSet.
        center_freq: Center frequency of the frequency range (unit: GHz).
        nchan: Number of channels in the frequency range.
        resolution: Frequency resolution (unit: GHz/ch).
        antenna_id: The antenna ID.
        doplot: If True, plot the atmospheric transmission and opacities.

    Returns:
        A tuple of 2 arrays. The first one is an array of frequencies in the
        unit of GHz, and the other is the atmospheric transmission at each
        frequency.
    """
    # first try computing elevation value from phasecenter
    # if it fails, inspect POINTING table
    try:
        elevation = get_representative_elevation(vis, antenna_id)
    except Exception:
        elevation = get_median_elevation(vis, antenna_id)

    # get median PWV using Todd's script
    if get_table_nrow(vis+'/ASDM_CALATMOSPHERE') or get_table_nrow(vis+'/ASDM_CALWVR'):
        # PIPE-2212: Ensure that PWV is checked only if the relevant subtable exists and is not empty.
        # This prevents open table tool istance from being left behind by adopted.getMedianPWV().
        pwv, _ = adopted.getMedianPWV(vis=vis)
    else:
        # fallback to pwv=0.0
        pwv = 0.0

    altitude = get_altitude(vis)

    myat = casa_tools.atmosphere
    myqa = casa_tools.quanta
    init_at(myat, fcenter=center_freq, nchan=nchan, resolution=resolution,
            altitude=altitude)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

    airmass = calc_airmass(elevation)
    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)
    frequency = myqa.convert(myat.getSpectralWindow(0), "GHz")['value']

    if doplot:
        plot(frequency, dry_opacity, wet_opacity, transmission)

    myat.done()
    return frequency, transmission


def get_table_nrow(table_name: str) -> Optional[int]:
    """Get the number of rows in a specified table.

    This function checks if the table exists, logs a debug message if it does not, and returns None.
    If the table exists, it opens the table, retrieves the number of rows, and returns that number.

    Args:
        table_name (str): The name of the table to check.

    Returns:
        Optional[int]: The number of rows in the table, or None if the table does not exist.
    """
    if not os.path.exists(table_name):
        LOG.debug('%s does not exist.', table_name)
        return

    with casa_tools.TableReader(table_name) as mytb:
        nrow = mytb.nrows()

    return nrow
