import os
import shutil
from typing import Union, Dict, Tuple

import astropy.io.fits as apfits
import pytest

from .. import casa_tools
from .positioncorrection import do_wide_field_pos_cor, calc_zd_pa

test_params_fits = [(casa_tools.utils.resolve('pl-unittest/VLASS1.1.ql.T19t20.J155950+333000.fits'),
                     {'unit': 'deg', 'value': -107.6183},
                     {'unit': 'deg', 'value': 33.90049},
                     ({'unit': 'deg', 'value': 239.9617912649343},
                      {'unit': 'deg', 'value': 33.49999737118265})
                     ),  # VLASS 1.1
                    (casa_tools.utils.resolve('pl-unittest/VLASS1.2.ql.T17t06.J041750+243000.fits'),
                     {'unit': 'deg', 'value': -107.61833},
                     {'unit': 'deg', 'value': 33.90049},
                     ({'unit': 'deg', 'value': 64.45982059345977},
                      {'unit': 'deg', 'value': 24.49997953261358})
                     )]  # VLASS 1.2

test_params_func = [({'unit': 'deg', 'value': 239.9617912649343},
                     {'unit': 'deg', 'value': 33.49999737118265},
                     {'unit': 'deg', 'value': -107.6183},
                     {'unit': 'deg', 'value': 33.90049},
                     {'m0': {'unit': 'd', 'value': 58089.82306510417}, 'refer': 'UTC', 'type': 'epoch'},
                    (0.29819920860195787,
                     1.4473218710956612)
                     ),
                    ({'unit': 'deg', 'value': 64.45982059345977},
                     {'unit': 'deg', 'value': 24.49997953261358},
                     {'unit': 'deg', 'value': -107.6183},
                     {'unit': 'deg', 'value': 33.90049},
                     {'m0': {'unit': 'd', 'value': 58089.82306510417}, 'refer': 'UTC', 'type': 'epoch'},
                    (2.0880275133156623,
                     -0.2674689095474299)
                     )]


@pytest.mark.parametrize('fitsname, obs_long, obs_lat, expected', test_params_fits)
def test_do_wide_field_corr(fitsname: str, obs_long: Dict[str, Union[str, float]],
                            obs_lat: Dict[str, Union[str, float]],
                            expected: Tuple[Dict, Dict], epsilon: float = 1.0e-9):
    """Test do_wide_field_corr()

    This utility function downloads a FITS image and applies wide field position
    correction to the image reference coordinates (CRVAL1 and CRVAL2). The tested
    quantities are the corrected RA and Dec values in the FITS header.

    If url is not provided, or not available, then assume file already exists in
    current folder.

    The default tolerance (epsilon) value is equivalent to about 0.01 milliarcs.
    """
    # Check if FITS file exists
    try:
        local_fitsname = os.path.basename(fitsname)
        shutil.copyfile(fitsname, local_fitsname)
    except IOError:
        print("FITS file is not accessible ({})".format(fitsname))

    # Correct the copied FITS file
    do_wide_field_pos_cor(fitsname=local_fitsname, obs_long=obs_long, obs_lat=obs_lat)

    # Obtain corrected reference coordinates
    with apfits.open(local_fitsname, mode='readonly') as hdulist:
        header = hdulist[0].header
        ra_deg_head = casa_tools.quanta.convert({'value': header['crval1'], 'unit': header['cunit1']}, 'deg')
        dec_deg_head = casa_tools.quanta.convert({'value': header['crval2'], 'unit': header['cunit2']}, 'deg')

    # Clean up
    os.remove(local_fitsname)

    # Compute relative error
    ra_expected = casa_tools.quanta.convert(expected[0], 'deg')['value']
    dec_expected = casa_tools.quanta.convert(expected[1], 'deg')['value']

    delta_ra = (ra_deg_head['value'] - ra_expected) / ra_expected
    delta_dec = (dec_deg_head['value'] - dec_expected) / dec_expected

    # Check results
    assert abs(delta_ra) < epsilon and abs(delta_dec) < epsilon


@pytest.mark.parametrize('ra, dec, obs_long, obs_lat, date_time, expected', test_params_func)
def test_calc_zd_pa(ra: Dict, dec: Dict, obs_long: Dict, obs_lat: Dict, date_time: Dict,
                    expected: Tuple[float, float], epsilon: float = 1.0e-9):
    """Test calc_zd_pa()

    This utility function tests the mathematical correctness of the zenith distance and parallactic
    angle calculation function.
    """
    # Compute correction
    zd, pa = calc_zd_pa(ra=ra, dec=dec, obs_long=obs_long, obs_lat=obs_lat, date_time=date_time)

    delta_zd = (zd - expected[0]) / expected[0]
    delta_pa = (pa - expected[1]) / expected[1]

    assert abs(delta_zd) < epsilon and abs(delta_pa) < epsilon
