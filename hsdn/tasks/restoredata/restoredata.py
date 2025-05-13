"""
Restore task module for NRO data, based on h_restoredata.

The restore data module provides a class for reimporting, reflagging, and
recalibrating a subset of the ASDMs belonging to a member OUS, using pipeline
flagging and calibration data products.
"""
import os
from typing import List, Dict, Optional

import pipeline.h.tasks.restoredata.restoredata as restoredata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline import Context
from pipeline.h.tasks.applycal import ApplycalResults
from pipeline.hsd.tasks.applycal import applycal
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.basetask import ResultsList
from . import ampcal
from ..importdata import importdata as importdata

LOG = infrastructure.get_logger(__name__)


class NRORestoreDataInputs(restoredata.RestoreDataInputs):
    """NRORestoreDataInputs manages the inputs for the NRORestoreData task."""

    reffile = vdp.VisDependentProperty(default='')
    caltable = vdp.VisDependentProperty(default='')
    hm_rasterscan = vdp.VisDependentProperty(default='time')

    def __init__(self, context: Context, vis: List[str] = None, caltable: vdp.VisDependentProperty = None,
                 reffile: vdp.VisDependentProperty = None, products_dir: str = None,
                 copytoraw: vdp.VisDependentProperty = None, rawdata_dir: str = None, output_dir: str = None,
                 hm_rasterscan: Optional[str] = None):
        """
        Initialise the Inputs, initialising any property values to those given here.

        Args:
            context: the pipeline Context state object
            vis: the ASDMs(s) for which data is to be restored
            caltable: VisDependentProperty object, calibration table data
            reffile: VisDependentProperty object, a scale file
            products_dir: the directory of archived pipeline products
            copytoraw: copy the required data products from products_dir to rawdata_dir
            rawdata_dir: the raw data directory for ASDM(s) and products
            output_dir: the working directory for the restored data
            hm_rasterscan: Heuristics method for raster scan analysis
        """
        super(NRORestoreDataInputs, self).__init__(context, vis=vis, products_dir=products_dir,
                                                   copytoraw=copytoraw, rawdata_dir=rawdata_dir,
                                                   output_dir=output_dir)

        self.caltable = caltable
        self.reffile = reffile
        self.hm_rasterscan = hm_rasterscan


class NRORestoreDataResults(restoredata.RestoreDataResults):
    """Results object of NRORestoreData."""

    def __init__(self, importdata_results: ResultsList = None, applycal_results: ResultsList = None,
                 ampcal_results: ResultsList = None, flagging_summaries: List[Dict[str, str]] = None):
        """
        Initialise the results objects.

        Args:
            importdata_results: results of importdata
            applycal_results: results of applycal
            ampcal_results: results of ampcal
            flagging_summaries: summaries of flagdata
        """
        super(NRORestoreDataResults, self).__init__(importdata_results, applycal_results, flagging_summaries)
        self.ampcal_results = ampcal_results

    def merge_with_context(self, context: Context):
        """
        Merge results with context.

        Args:
            context: Context object
        """
        super(NRORestoreDataResults, self).merge_with_context(context)

        # set amplitude scaling factor to ms domain objects
        if isinstance(self.applycal_results, ResultsList):
            for result in self.applycal_results:
                self._merge_ampcal(context, result)
        else:
            self._merge_ampcal(context, self.applycal_results)

    def _merge_ampcal(self, context: Context, applycal_results: ApplycalResults):
        """
        Merge results of applycal with context.

        Args:
            context: Context object
            applycal_results: results of applycal
        """
        for calapp in applycal_results.applied:
            msobj = context.observing_run.get_ms(name=os.path.basename(calapp.vis))
            if not hasattr(msobj, 'k2jy_factor'):
                for _calfrom in calapp.calfrom:
                    if _calfrom.caltype == 'amp' or _calfrom.caltype == 'gaincal':
                        LOG.debug('Adding k2jy factor to {0}'.format(msobj.basename))
                        # k2jy gaincal table
                        k2jytable = _calfrom.gaintable
                        k2jy_factor = {}
                        with casa_tools.TableReader(k2jytable) as tb:
                            spws = tb.getcol('SPECTRAL_WINDOW_ID')
                            antennas = tb.getcol('ANTENNA1')
                            params = tb.getcol('CPARAM').real
                            nrow = tb.nrows()
                        for irow in range(nrow):
                            spwid = spws[irow]
                            antenna = antennas[irow]
                            param = params[:, 0, irow]
                            npol = param.shape[0]
                            antname = msobj.get_antenna(antenna)[0].name
                            dd = msobj.get_data_description(spw=int(spwid))
                            if dd is None:
                                continue
                            for ipol in range(npol):
                                polname = dd.get_polarization_label(ipol)
                                k2jy_factor[(spwid, antname, polname)] = 1.0 / (param[ipol] * param[ipol])
                        msobj.k2jy_factor = k2jy_factor
            LOG.debug('msobj.k2jy_factor = {0}'.format(getattr(msobj, 'k2jy_factor', 'N/A')))


@task_registry.set_equivalent_casa_task('hsdn_restoredata')
class NRORestoreData(restoredata.RestoreData):
    """Restore flagged and calibrated data produced during a previous pipeline run and archived on disk."""

    Inputs = NRORestoreDataInputs

    def prepare(self):
        """Prepare results."""
        inputs = self.inputs
        LOG.debug('prepare inputs = {0}'.format(inputs))

        # run prepare method in the parent class
        results = super(NRORestoreData, self).prepare()
        ampcal_results = self.ampcal_results

        # apply baseline table and produce baseline-subtracted MSs
        # apply final flags for baseline-subtracted MSs

        results = NRORestoreDataResults(results.importdata_results, results.applycal_results, ampcal_results,
                                        results.flagging_summaries)
        return results

    def _do_importasdm(self, sessionlist: List[str], vislist: List[str]):
        """
        Execute importasdm.

        Args:
            sessionlist: a list of sessions
            vislist: a list of vis
        """
        inputs = self.inputs
        # NROImportDataInputs operate in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.

        LOG.debug('_do_importasdm inputs = {0}'.format(inputs))

        container = vdp.InputsContainer(importdata.NROImportData, inputs.context, vis=vislist,
                                        output_dir=None, hm_rasterscan=inputs.hm_rasterscan)
        importdata_task = importdata.NROImportData(container)
        return self._executor.execute(importdata_task, merge=True)

    def _do_applycal(self):
        """Execute applycal."""
        inputs = self.inputs
        LOG.debug('_do_applycal inputs = {0}'.format(inputs))

        # Before applycal, sensitively (amplitude) correction using k2jycal task and
        # a scalefile (=reffile) given by Observatory. This is the special operation for NRO data.
        # If no scalefile exists in the working directory, skip this process.
        if os.path.exists(inputs.reffile):
            container = vdp.InputsContainer(ampcal.SDAmpCal, inputs.context, reffile=inputs.reffile)
        else:
            LOG.info('No scale factor file exists. Skip scaling.')
            container = vdp.InputsContainer(ampcal.SDAmpCal, inputs.context)
        LOG.debug('ampcal container = {0}'.format(container))
        ampcal_task = ampcal.SDAmpCal(container)
        self.ampcal_results = self._executor.execute(ampcal_task, merge=True)

        # SDApplyCalInputs operates in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.
        container = vdp.InputsContainer(applycal.SerialSDApplycal, inputs.context)
        applycal_task = applycal.SerialSDApplycal(container)
        LOG.debug('_do_applycal container = {0}'.format(container))
        return self._executor.execute(applycal_task, merge=True)
