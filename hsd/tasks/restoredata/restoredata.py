"""
Restore task module for single dish data, based on h_restoredata.

The restore data module provides a class for reimporting, reflagging, and
recalibrating a subset of the ASDMs belonging to a member OUS, using pipeline
flagging and calibration data products.
"""
import os

import pipeline.h.tasks.restoredata.restoredata as restoredata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from .. import applycal
from ..importdata import importdata as importdata
from typing import List, Dict, Optional, TYPE_CHECKING # typing.List/Dict is obsolete in Python 3.9, but we need to use it to support 3.6

if TYPE_CHECKING:
    from pipeline.hsd.tasks.applycal import SDApplycalResults
    from pipeline.hsd.tasks.importdata import SDImportDataResults
    from pipeline.infrastructure.launcher import Context


LOG = infrastructure.get_logger(__name__)


class SDRestoreDataInputs(restoredata.RestoreDataInputs):
    """SDRestoreDataInputs manages the inputs for the SDRestoreData task."""

    asis = vdp.VisDependentProperty(default='SBSummary ExecBlock Annotation Antenna Station Receiver Source CalAtmosphere CalWVR')
    ocorr_mode = vdp.VisDependentProperty(default='ao')
    hm_rasterscan = vdp.VisDependentProperty(default='time')

    def __init__(self, context: 'Context', copytoraw: Optional[bool] = None, products_dir: Optional[str] = None,
                 rawdata_dir: Optional[str] = None, output_dir: Optional[str] = None, session: Optional[str] = None,
                 vis: List[str] = None, bdfflags: Optional[bool] = None, lazy: Optional[bool] = None,
                 asis: Optional[str] = None, ocorr_mode: Optional[str] = None, hm_rasterscan: Optional[str] = None):
        """
        Initialise the Inputs, initialising any property values to those given here.

        Args:
            context: the pipeline Context state object
            copytoraw: copy the required data products from products_dir to rawdata_dir
            products_dir: the directory of archived pipeline products
            rawdata_dir: the raw data directory for ASDM(s) and products
            output_dir: the working directory for the restored data
            session: the parent session of each vis
            vis: the ASDMs(s) for which data is to be restored
            bdfflags: set the BDF flags
            lazy: use the lazy filler to restore data
            asis: Creates verbatim copies of the ASDM tables in the output MS.
                  The value given to this option must be a list of table names separated by space characters.
            hm_rasterscan: Heuristics method for raster scan analysis
        """
        super(SDRestoreDataInputs, self).__init__(context, copytoraw=copytoraw, products_dir=products_dir,
                                                  rawdata_dir=rawdata_dir, output_dir=output_dir, session=session,
                                                  vis=vis, bdfflags=bdfflags, lazy=lazy, asis=asis,
                                                  ocorr_mode=ocorr_mode)

        self.hm_rasterscan = hm_rasterscan


class SDRestoreDataResults(restoredata.RestoreDataResults):
    """Results object of SDRestoreData."""

    def __init__(self, importdata_results: 'SDImportDataResults', applycal_results: 'SDApplycalResults',
                 flagging_summaries: List[Dict[str, str]]):
        """
        Initialise the results objects.

        Args:
            importdata_results: results of importdata
            applycal_results: results of applycal
            flagging_summaries: summaries of flagdata
        """
        super(SDRestoreDataResults, self).__init__(importdata_results, applycal_results, flagging_summaries)

    def merge_with_context(self, context: 'Context'):
        """
        Call same method of superclass and _merge_k2jycal().

        Args:
            context: the pipeline Context state object
        """
        super(SDRestoreDataResults, self).merge_with_context(context)

        # set k2jy factor to ms domain objects
        if isinstance(self.applycal_results, basetask.ResultsList):
            for result in self.applycal_results:
                self._merge_k2jycal(context, result)
        else:
            self._merge_k2jycal(context, self.applycal_results)

    def _merge_k2jycal(self, context: 'Context', applycal_results: 'SDApplycalResults'):
        """
        Merge k2jycal caltable into context.

        Jy/K factors are retrieved from caltables and are attached to
        measurementset domain object.

        Args:
            context: the pipeline Context state object
            applycal_results: results object of applycal
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


@task_registry.set_equivalent_casa_task('hsd_restoredata')
class SDRestoreData(restoredata.RestoreData):
    """Restore flagged and calibrated data produced during a previous pipeline run and archived on disk."""

    Inputs = SDRestoreDataInputs

    def prepare(self):
        """Call prepare method of superclass, create Results ofject."""
        # run prepare method in the parent class
        results = super(SDRestoreData, self).prepare()

        # apply baseline table and produce baseline-subtracted MSs

        # apply final flags for baseline-subtracted MSs

        sdresults = SDRestoreDataResults(results.importdata_results,
                                         results.applycal_results,
                                         results.flagging_summaries)

        return sdresults

    def _do_importasdm(self, sessionlist: List[str], vislist: List[str]):
        """
        Execute importasdm task.

        Args:
            sessionlist: session list of pipeline
            vislist: MeasurementSet list of pipeline
        """
        inputs = self.inputs
        # SDImportDataInputs operate in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.
        container = vdp.InputsContainer(importdata.SerialSDImportData, inputs.context, vis=vislist, session=sessionlist,
                                        save_flagonline=False, lazy=inputs.lazy, bdfflags=inputs.bdfflags,
                                        asis=inputs.asis, ocorr_mode=inputs.ocorr_mode, hm_rasterscan=inputs.hm_rasterscan)
        importdata_task = importdata.SerialSDImportData(container)
        return self._executor.execute(importdata_task, merge=True)

    def _do_applycal(self):
        """Execute applycal task."""
        inputs = self.inputs
        # SDApplyCalInputs operates in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.
        container = vdp.InputsContainer(applycal.SerialSDApplycal, inputs.context)
        applycal_task = applycal.SerialSDApplycal(container)
        return self._executor.execute(applycal_task, merge=True)
