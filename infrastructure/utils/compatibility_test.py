import numpy as np
import scipy

from . import compatibility


def test_get_func_compute_mad():
    """Test scipy.stats.median_abs(olute)_deviation to ensure compatibility between py3.6 and py3.8."""
    mad_func = compatibility.get_scipy_function_for_mad()

    # generate fixed random array
    np.random.seed(1234567)
    array_length = 100
    array_list = np.random.rand(2, array_length)

    # expected values based on casa-6.4.1-12-pipeline-2022.2.0.64 (py3.6)
    expected_mad_list = [
        0.3234774,
        0.3683437,
    ]

    # relatively loose test using np.allclose with default setting
    print(f'scipy version {scipy.__version__}')
    for arr, expected_mad in zip(array_list, expected_mad_list):
        mad = mad_func(arr)
        print(f'arr={arr[:10].tolist()}')
        print(f'mad={mad}, expected={expected_mad}')
        assert np.allclose(mad, expected_mad)
