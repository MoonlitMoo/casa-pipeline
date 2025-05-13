"""
Tests for the hifv/heuristics/standard.py module.
"""
import pytest

from . standard import Standard

sources = ('Mars', 'Jupiter', 'Uranus', 'Neptune', 'Pluto',
           'Io', 'Europa', 'Ganymede', 'Callisto', 'Titan',
           'Triton', 'Ceres', 'Pallas', 'Vesta', 'Juno',
           'Victoria', 'Davida')

test_params = [(source, 'Butler-JPL-Horizons 2012') for source in sources]
test_params.append(('3C286', 'Perley-Butler 2017'))
test_params.append(('3C48', 'Perley-Butler 2017'))
test_params.append(('3C138', 'Perley-Butler 2017'))
test_params.append(('3C147', 'Perley-Butler 2017'))


@pytest.mark.parametrize("source, expectedmodel", test_params)
def test_standard(source, expectedmodel):
    """Test standard() heuristics class

    This heuristics function takes a source field name and returns the appropriate model.

    """
    s = Standard()

    assert s.calculate(source) == expectedmodel
