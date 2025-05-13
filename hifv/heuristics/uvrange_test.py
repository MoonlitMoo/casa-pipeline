"""
Tests for the hifv/heuristics/uvrange.py module.
"""
import pytest
from decimal import Decimal

from pipeline.h.tasks.common import commonfluxresults
import pipeline.domain as domain
from pipeline.domain.measures import FluxDensity

from . uvrange import uvrange

# Create three different test pipeline Flux objects
result1 = [commonfluxresults.FluxCalibrationResults('test1ms.ms')]
result2 = [commonfluxresults.FluxCalibrationResults('test2ms.ms')]
result3 = [commonfluxresults.FluxCalibrationResults('test3ms.ms')]
m1 = domain.FluxMeasurement(0, FluxDensity(1), origin='test1', uvmin=Decimal('0.0'), uvmax=Decimal('0.0'))
m2 = domain.FluxMeasurement(0, FluxDensity(2), origin='test2', uvmin=Decimal('100.0'), uvmax=Decimal('0.0'))
m3 = domain.FluxMeasurement(0, FluxDensity(3), origin='test3', uvmin=Decimal('10.0'), uvmax=Decimal('100.0'))
result1[0].measurements[0].append(m1)
result2[0].measurements[0].append(m2)
result3[0].measurements[0].append(m3)

test_params = [(result1, ''), (result2, '>100.0lambda'), (result3, '10.0~100.0lambda')]


@pytest.mark.parametrize("result, expecteduvrange", test_params)
def test_uvrange(result, expecteduvrange):
    """Test uvrange() heuristics function

    This utility function takes a flux result object and determines what the
    uvrange string should be.  A blank string, greater than value (>), or value~value can be returned
    in each of the examples.  This function is primarily used in VLASS processing.

    """
    assert uvrange(result, 0) == expecteduvrange
