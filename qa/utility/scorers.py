#!/usr/bin/env python

'''Generic scorer classes.'''

import numpy as np
from scipy.special import erf


class erfScorer(object):

    def __init__(self, good_level, bad_level, lowest_score=0.0):
        """
        Error function based scorer between two levels with
        optional lower score limit so that the QA score ranges
        from lowest_score to 1.0.
        """

        assert 0.0 <= lowest_score < 1.0, "Erf scorer lowest score must be in [0,1)."

        self.good_level = good_level
        self.bad_level = bad_level
        self.lowest_score = lowest_score
        self.slope = 6.0 / np.sqrt(2.0) / (good_level - bad_level)
        self.offset = 3.0 / np.sqrt(2.0) * (1.0 - 2.0 * good_level / (good_level - bad_level))

    def __call__(self, x):
        return (1.0 - self.lowest_score) * (erf(x * self.slope + self.offset) + 1.0) / 2.0 + self.lowest_score


class gaussScorer(object):
    """
    Gauss function based scorer using the difference to the
    center value as the metric.
    """

    def __init__(self, x0, sigma):
        self.x0 = x0
        self.sigma = sigma

    def __call__(self, x):
        return np.exp(-4.0 * np.log(2.0) * np.power((x - self.x0) / self.sigma, 2))


class linScorer(object):
    """
    Linear function scorer to map a metric range to a score
    range. Used for piecewise linear sections.
    """

    def __init__(self, metric_low, metric_high, score_low, score_high):

        # Avoid division by zero and enforce ordering of metric values.
        # The inverse mapping to higher scores for lower metric values
        # can still be done by inverting the low and high score values.
        # This way the check for valid metric values in the __call__
        # method is simpler. So score_low is the score for metric_low
        # and score_high is the one for metric_high.
        assert metric_low < metric_high, "Linear scorer metric low value must be lower than high value."

        self.metric_low = metric_low
        self.metric_high = metric_high
        self.score_low = score_low
        self.score_high = score_high

    def __call__(self, metric):

        assert self.metric_low <= metric <= self.metric_high, f"Metric must be in [{self.metric_low}, {self.metric_high}]."

        return (self.score_high - self.score_low) / (self.metric_high - self.metric_low) * (metric - self.metric_low) + self.score_low
