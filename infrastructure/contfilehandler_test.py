from unittest.mock import Mock
import pytest

from .. import domain
from . import casa_tools
from . import contfilehandler

to_topo_test_params = (
    ('214.5.0~215.5GHz;215.6~216.1GHz LSRK',
     [casa_tools.utils.resolve('pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms')],
     ['3'], '0', Mock(spec=domain.ObservingRun, **{'virtual2real_spw_id.return_value': 0, 'get_ms.return_value': 'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'}),
     (['214.4948696490~215.5104946490GHz;215.6042446490~216.1198696490GHz TOPO'],
      ['55~119;16~48'],
      {'unit': 'GHz', 'value': 1.5})),
    ('230.4~230.7GHz;231.5~231.6GHz;232.3~232.4GHz SOURCE',
     [casa_tools.utils.resolve('pl-unittest/uid___A002_Xcfc232_X2eda_test.ms')],
     ['3'], 18, Mock(spec=domain.ObservingRun, **{'virtual2real_spw_id.return_value': 18, 'get_ms.return_value': 'uid___A002_Xcfc232_X2eda_test.ms'}),
     (['230.4733757345~230.7038444845GHz;231.5026726095~231.6042351095GHz;232.3024772970~232.4040397970GHz TOPO'],
      ['0~235;1054~1157;1873~1976'],
      {'unit': 'GHz', 'value': 0.5}))
    )

@pytest.mark.parametrize("selection, msnames, fields, spw_id, observing_run, result", to_topo_test_params)
def test_to_topo(selection, msnames, fields, spw_id, observing_run, result):
    """
    Test ContFileHandler::to_topo()
    """

    cfh = contfilehandler.ContFileHandler('cont.dat')
    assert cfh.to_topo(selection, msnames, fields, spw_id, observing_run) == result
