"""
Importdata tasks for observation data of Nobeyama.

This task loads the specified visibility data into the pipeline
context unpacking and / or converting it as necessary.

Created on Dec 4, 2017

@author: kana
"""
import os
from typing import Any, Dict, List, Optional

import pipeline.hsd.tasks.importdata.importdata as sd_importdata
import pipeline.infrastructure as infrastructure
from pipeline.domain.fluxmeasurement import FluxMeasurement
from pipeline.domain.measurementset import MeasurementSet
from pipeline.domain.singledish import MSReductionGroupDesc
from pipeline.infrastructure import casa_tools, task_registry
from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


class NROImportDataInputs(sd_importdata.SDImportDataInputs):
    """Class of inputs of NROImportData.

    This class extends importdata.ImportDataInputs.
    """

    def __init__(self, context: Context, vis: Optional[List[str]]=None, output_dir: Optional[str]=None, session: Optional[List[str]]=None,
                 datacolumns: Optional[Dict]=None, overwrite: Optional[bool]=None, nocopy: Optional[bool]=None, createmms: Optional[str]=None,
                 hm_rasterscan: Optional[str] = None):
        """Initialise NROImportDataInputs class.

        Args:
            context: pipeline context
            vis: list of input visibility data
            output_dir: path of output directory
            session: list of visibility data sessions
            overwrite: flag of overwrite existing files on import
            nocopy: flag of disable copying of MS to working directory
            createmms: operation string to create an MMS
            hm_rasterscan: Heuristics method for raster scan analysis
        """
        # no-op parameters for MS
        asis = ''
        process_caldevice = False
        bdfflags = False
        lazy = False
        ocorr_mode = 'ao'
        save_flagonline = False

        super(NROImportDataInputs, self).__init__(context, vis=vis, output_dir=output_dir, asis=asis,
                                                  process_caldevice=process_caldevice, session=session,
                                                  overwrite=overwrite, nocopy=nocopy, save_flagonline=save_flagonline,
                                                  bdfflags=bdfflags, lazy=lazy, createmms=createmms,
                                                  ocorr_mode=ocorr_mode, datacolumns=datacolumns,
                                                  hm_rasterscan=hm_rasterscan)


class NROImportDataResults(sd_importdata.SDImportDataResults):
    """NROImportDataResults is an equivalent class with ImportDataResults.

    Purpose of NROImportDataResults is to replace QA scoring associated
    with ImportDataResults with single dish specific QA scoring, which
    is associated with this class.

    ImportDataResults holds the results of the ImportData task. It contains
    the resulting MeasurementSet domain objects and optionally the additional
    SetJy results generated from flux entries in Source.xml.
    """

    def __init__(self, mses: Optional[List[MeasurementSet]]=None, reduction_group_list: Optional[List[Dict[int, MSReductionGroupDesc]]]=None,
                 datatable_prefix: Optional[str]=None, setjy_results: Optional[List[FluxMeasurement]]=None):
        """Initialise NROImportDataResults class.

        Args:
            mses: list of MeasurementSet domain objects
            reduction_group_list: list of dictionaries that consist of reduction group IDs (key) and MSReductionGroupDesc (value)
            datatable_prefix: table name prefix of MeasurementSet
            setjy_results: the flux results generated from Source.xml
        """
        super(NROImportDataResults, self).__init__(mses=mses, reduction_group_list=reduction_group_list,
                                                   datatable_prefix=datatable_prefix, setjy_results=setjy_results)

    def merge_with_context(self, context: Context):
        """Override method of basetask.Results.merge_with_context.

        Args:
            context: pipeline context
        """
        super(NROImportDataResults, self).merge_with_context(context)
        # Set observatory information
        for ms in self.mses:
            if ms.antenna_array.name in ('NRO',):
                context.project_summary.telescope = 'NRO'
                context.project_summary.observatory = 'Nobeyama Radio Observatory'
                break


@task_registry.set_equivalent_casa_task('hsdn_importdata')
@task_registry.set_casa_commands_comment('Import Nobeyama MeasurementSets.')
class NROImportData(sd_importdata.SerialSDImportData):
    """NRO Data import execution task.

    This class extends importdata.ImportData class, and methods execute main logics depends on it.
    """

    Inputs = NROImportDataInputs

    def prepare(self, **parameters: Dict[str, Any]) -> NROImportDataResults:
        """Prepare job requests for execution.

        Args:
            parameters: the parameters to pass through from the superclass.
        Returns:
            NROImportDataResults : result object
        """
        # the input data should be MSes
        # TODO: check data type
        # get results object by running super.prepare()
        results = super(NROImportData, self).prepare()
        myresults = NROImportDataResults(mses=results.mses, reduction_group_list=results.reduction_group_list,
                                         datatable_prefix=results.datatable_prefix, setjy_results=results.setjy_results)
        myresults.origin = results.origin
        myresults.msglist = results.msglist
        return myresults

    def analyse(self, result: NROImportDataResults) -> NROImportDataResults:
        """Get version information for merge2 and set it to MS of result.

        Args:
            result: Result object
        Returns:
            NROImportDataResults : result object
        """
        for ms in result.mses:
            with casa_tools.TableReader(os.path.join(ms.name, 'OBSERVATION')) as tb:
                col = 'RELEASE_DATE'
                release_dates = tb.getcol(col)
                unit = tb.getcolkeywords(col)['QuantumUnits'][0]
            if len(release_dates) > 0:
                release_date = release_dates[0]
                qa = casa_tools.quanta
                merge2_version = qa.time(qa.quantity(release_date, unit), form='ymd')
                if isinstance(merge2_version, list):
                    merge2_version = merge2_version[0]
            else:
                merge2_version = 'N/A'
            ms.merge2_version = merge2_version
        return result
