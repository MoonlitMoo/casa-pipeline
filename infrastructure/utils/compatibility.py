"""Compatibility management."""
import functools
from typing import Callable

import scipy


def get_scipy_function_for_mad() -> Callable:
    """Return function to compute median absolute deviation (MAD).

    This absorbs the API difference bewteen SciPy versions including
    function name and scale parameter value.

    Raises:
        NotImplementedError: SciPy version is too old (lower than 1.3.0)

    Returns:
        function: function to compute MAD
    """
    # assuming X.Y.Z style version string
    scipy_version = scipy.version.full_version
    versioning = map(int, scipy_version.split('.'))
    major = next(versioning)
    minor = next(versioning)
    if major > 1 or (major == 1 and minor >= 5):
        # The 'normal' scale corresponds to the numerical value
        # scipy.stats.norm().ppf(0.75), which is approximately
        # 0.67449. It is supposed to be compatible with the result
        # obtained by scipy.stats.median_absolute_deviation (of version
        # 1.4.1) with default parameters, scale=1.4826. But there is
        # subtle difference between them. As an experiment, I empirically
        # derived the scale value that reproduced fully compatible result.
        # Here is a fine-tuned value:
        #
        # scale = 0.6744907594765952
        #
        # The experiment is based on "test_get_func_compute_mad" test
        # defined in rasterscan_test.py.
        scale = 'normal'
        mad_function = scipy.stats.median_abs_deviation
        return functools.partial(mad_function, scale=scale)
    elif major == 1 and minor >= 3:
        scale = 1.4826
        mad_function = scipy.stats.median_absolute_deviation
        return functools.partial(mad_function, scale=scale)
    else:
        raise NotImplementedError('No MAD function available in scipy. Use scipy 1.3 or higher.')
