"""
Classes of Regression Framework.

The regression module contains base classes and plugin registry for the
pipeline's regression test value extractor framework.

This module contains two classes:

    * RegressionExtractor: the base class for extractor plug-ins
    * RegressionExtractorRegistry: the registry and manager for plug-ins

Tasks provide and register their own extractors that each extends
RegressionExtractor. These extractor plug-ins analyse the results of a task,
extracting pertinent values and writing them to a dict. The keys of the
output dict identify the value; the values of the dict are the extracted
values themselves.

The pipeline QA framework is activated whenever a Results instance is accepted
into the pipeline context. The pipeline QA framework operates by calling
is_handler_for(result) on each registered QAPlugin, passing it the the accepted
Results instance for inspection. QAPlugins that claim to handle the Result are
given the Result for processing. In this step, the QA framework calls
QAPlugin.handle(context, result), the method overridden by the task-specific
QAPlugin.
"""
import abc
import collections
import os.path
import re
from collections import OrderedDict
from typing import List, Union

from pipeline.domain.measures import FluxDensityUnits
from pipeline.h.tasks.applycal.applycal import ApplycalResults
from pipeline.h.tasks.common.commonfluxresults import FluxCalibrationResults
from pipeline.hif.tasks.applycal.ifapplycal import IFApplycal
from pipeline.hifa.tasks.fluxscale.gcorfluxscale import GcorFluxscale
from pipeline.hifa.tasks.gfluxscaleflag.resultobjects import GfluxscaleflagResults
from pipeline.hsd.tasks.applycal.applycal import SDApplycal
from pipeline.hsd.tasks.baselineflag.baselineflag import SDBLFlagResults
from pipeline.hsd.tasks.baselineflag.baselineflag import SDBLFlag
from pipeline.hsd.tasks.imaging.imaging import SDImaging
from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResults
from pipeline.infrastructure.basetask import Results, ResultsList, StandardTaskTemplate
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.taskregistry import task_registry
from pipeline.infrastructure import logging
from pipeline.hsd.tasks.restoredata.restoredata import SDRestoreDataResults, SDRestoreData
from pipeline.hsdn.tasks.restoredata.restoredata import NRORestoreDataResults, NRORestoreData
from pipeline.hifv.tasks.fluxscale.fluxboot import Fluxboot, FluxbootResults
from pipeline.hifv.tasks.fluxscale.solint import Solint, SolintResults
from pipeline.hifv.tasks.priorcals import Priorcals
from pipeline.hifv.tasks.priorcals.resultobjects import PriorcalsResults
from pipeline.hifv.tasks.setmodel.vlasetjy import VLASetjy
from pipeline.h.tasks.common.commonfluxresults import FluxCalibrationResults
from pipeline.hifv.tasks.statwt.statwt import Statwt, StatwtResults
from pipeline.hifv.tasks.importdata.importdata import VLAImportData, VLAImportDataResults
from pipeline.hifv.tasks.flagging.flagdetervla import FlagDeterVLA, FlagDeterVLAResults
from pipeline.hifv.tasks.testBPdcals.testBPdcals import testBPdcals, testBPdcalsResults
from pipeline.hifv.tasks.semiFinalBPdcals.semiFinalBPdcals import semiFinalBPdcals, semiFinalBPdcalsResults
from pipeline.hifv.tasks.finalcals.finalcals import Finalcals, FinalcalsResults
from pipeline.hifv.tasks.finalcals.applycals import Applycals
from pipeline.hifv.tasks.flagging.checkflag import Checkflag, CheckflagResults
from pipeline.hifv.tasks.flagging.targetflag import Targetflag, TargetflagResults
from pipeline.domain.measurementset import MeasurementSet

LOG = logging.get_logger(__name__)


class RegressionExtractor(object, metaclass=abc.ABCMeta):
    """The mandatory base class for all regression test result extractors."""

    # the Results class this handler is expected to handle
    result_cls = None
    # if result_cls is a list, the type of classes it is expected to contain
    child_cls = None
    # the task class that generated the results, or None if it should handle
    # all results of this type regardless of which task generated it
    generating_task = None

    def is_handler_for(self, result:Union[Results, ResultsList]) -> bool:
        """
        Return True if this RegressionExtractor can process the Result.

        :param result: the task Result to inspect
        :return: True if the Result can be processed
        """
        # if the result is not a list or the expected results class,
        # return False
        if not isinstance(result, self.result_cls):
            return False

        # this is the expected class and we weren't expecting any
        # children, so we should be able to handle the result
        if self.child_cls is None and (self.generating_task is None
                                       or result.task is self.generating_task
                                       or ( hasattr(self.generating_task, 'Task') and result.task is self.generating_task.Task) ):
            return True

        try:
            if all([isinstance(r, self.child_cls) and
                    (self.generating_task is None or r.task is self.generating_task)
                    for r in result]):
                return True
            return False
        except:
            # catch case when result does not have a task attribute
            return False

    @abc.abstractmethod
    def handle(self, result:Results) -> OrderedDict:
        """
        [Abstract] Extract values for testing.

        This method should return a dict of

        {'applycal.new_flags.science': 0.34,
         'applycal.new_flags.bandpass': 0.53}

        :param result:
        :return:
        """
        raise NotImplemented


class RegressionExtractorRegistry(object):
    """
    The registry and manager of the regression result extractor framework.

    The responsibility of the RegressionResultRegistry is to pass Results to
    RegressionExtractors that can handle them.
    """

    def __init__(self):
        """Constractor of this class."""
        self.__plugins_loaded = False
        self.__handlers = []

    def add_handler(self, handler: RegressionExtractor) -> None:
        """
        Push RegressionExtractor into handlers list __handler.

        Args:
            handler: RegressionExtractor
        """
        task = handler.generating_task.__name__ if handler.generating_task else 'all'
        child_name = ''
        if hasattr(handler.child_cls, '__name__'):
            child_name = handler.child_cls.__name__
        elif isinstance(handler.child_cls, collections.abc.Iterable):
            child_name = str([x.__name__ for x in handler.child_cls])
        container = 's of %s' % child_name
        s = '{}{} results generated by {} tasks'.format(handler.result_cls.__name__, container, task)
        LOG.debug('Registering {} as new regression result handler for {}'.format(handler.__class__.__name__, s))
        self.__handlers.append(handler)


    def handle(self, result:Union[Results, ResultsList]) -> OrderedDict:
        """
        Extract values from corresponding Extractor object of Result object.

        Args:
            result: Results or ResultsList[Results]
        Return:
            Dict of extracted values
        """
        if not self.__plugins_loaded:
            for plugin_class in get_all_subclasses( RegressionExtractor ):
                self.add_handler(plugin_class())
            self.__plugins_loaded = True

        # this is the list which will contain extracted values tuples
        extracted = {}

        # Process leaf results first
        if isinstance(result, collections.abc.Iterable):
            for r in result:
                d = self.handle(r)
                extracted = union(extracted, d)

        # process the group-level results.
        for handler in self.__handlers:
            if handler.is_handler_for(result):
                LOG.debug('{} extracting regression results for {}'.format(handler.__class__.__name__,
                                                                           result.__class__.__name__))
                d = handler.handle(result)
                extracted = union(extracted, d)

        return extracted


def union(d1: dict, d2: dict) -> OrderedDict:
    """
    Return the union of two dicts.
    
    It raises an exception if duplicate keys are detected in the input dicts.

    Args:
        d1, d2: dict for unioning
    Returns:
        OrderedDict unioned two dict
    """
    intersection = key_intersection(d1, d2)
    if intersection:
        raise ValueError('Regression keys are duplicated: {}'.format(intersection))
    # dict keys and values should be strings, so ok to shallow copy
    # OrderedDict is used to store results in processing order.
    u = OrderedDict(d1)
    u.update(d2)
    return u


def key_intersection(d1: dict, d2: dict) -> set:
    """
    Compare keys of two dicts, returning duplicate keys.

    Args:
        d1, d2: dict for comparison
    Returns:
        duplicated keys dict
    """
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    return d1_keys.intersection(d2_keys)


# default RegressionExtractorRegistry initialization
registry = RegressionExtractorRegistry()


class FluxcalflagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifa_gfluxscaleflag.

    The extracted values are:
       - the number of flagged rows before this task
       - the number of flagged rows after this task
       - QA score
    """

    result_cls = GfluxscaleflagResults
    child_cls = None

    def handle(self, result:GfluxscaleflagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: GfluxscaleflagResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, result.task)

        summaries_by_name = {s['name']: s for s in result.cafresult.summaries}

        num_flags_before = summaries_by_name['before']['flagged']

        if 'after' in summaries_by_name:
            num_flags_after = summaries_by_name['after']['flagged']
        else:
            num_flags_after = num_flags_before

        d = OrderedDict()
        d['{}.num_rows_flagged.before'.format(prefix)] = int(num_flags_before)
        d['{}.num_rows_flagged.after'.format(prefix)] = int(num_flags_after)

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class GcorFluxscaleRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifa_gcorfluxscale.

    The extracted values are:
       - Stokes I for each field and spw, in Jy
    """

    result_cls = FluxCalibrationResults
    child_cls = None
    generating_task = GcorFluxscale

    def handle(self, result:FluxCalibrationResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FluxCalibrationResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for field_id, measurements in result.measurements.items():
            for m in measurements:
                key = '{}.field_{}.spw_{}.I'.format(prefix, field_id, m.spw_id)
                d[key] = str(m.I.to_units(FluxDensityUnits.JANSKY))

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class ApplycalRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for applycal tasks.

    The extracted values are:
       - the number of flagged and scanned rows before this task
       - the number of flagged and scanned rows after this task
       - QA score
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = IFApplycal

    def handle(self, result:ApplycalResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: ApplycalResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)
        d = OrderedDict()

        try:
            summaries_by_name = {s['name']: s for s in result.summaries}
            num_flags_before = summaries_by_name['before']['flagged']
            num_flags_after = summaries_by_name['applycal']['flagged']
            
            d['{}.num_rows_flagged.before'.format(prefix)] = int(num_flags_before)
            d['{}.num_rows_flagged.after'.format(prefix)] = int(num_flags_after)

            flag_summary_before = summaries_by_name['before']
            for scan_id, v in flag_summary_before['scan'].items():
                d['{}.scan_{}.num_rows_flagged.before'.format(prefix, scan_id)] = int(v['flagged'])

            flag_summary_after = summaries_by_name['applycal']
            for scan_id, v in flag_summary_after['scan'].items():
                d['{}.scan_{}.num_rows_flagged.after'.format(prefix, scan_id)] = int(v['flagged'])

            qa_entries = extract_qa_score_regression(prefix, result)
            d.update(qa_entries)

        except: 
            d['{}.results'.format(prefix)] = None
            LOG.info("Some information missing from ApplycalResults. Omitting from resgression comparison.")

        return d


class SDApplycalRegressionExtractor(ApplycalRegressionExtractor):
    """
    Regression test result extractor for sd_applycal.

    It extends ApplycalRegressionExtractor in order to use the same extraction logic.
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = SDApplycal


class VLAApplycalRegressionExtractor(ApplycalRegressionExtractor):
    """
    Regression test result extractor for hifv_applycals

    It extends ApplycalRegressionExtractor in order to use the same extraction logic.
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = Applycals


class CheckflagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_checkflag

    Extract flagging summaries from CheckflagResults
    """

    result_cls = CheckflagResults
    child_cls = None
    generating_task = Checkflag

    def handle(self, result: CheckflagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: CheckflagResults object

        Returns:
            OrderedDict[str, float]
        """
        d = OrderedDict()
        prefix = get_prefix(result, self.generating_task)
        summaries_by_name = {s['name']: s for s in result.summaries}

        try:
            num_flags_before = summaries_by_name['before']['flagged']
            d['{}.num_rows_flagged.before'.format(prefix)] = int(num_flags_before)
        except: 
            d['{}.num_rows_flagged.before'.format(prefix)] = None

        try: 
            num_flags_after = summaries_by_name['after']['flagged']
            d['{}.num_rows_flagged.after'.format(prefix)] = int(num_flags_after)
        except: 
            d['{}.num_rows_flagged.after'.format(prefix)] = None


        if 'before' in summaries_by_name:
            flag_summary_before = summaries_by_name['before']

            for scan_id, v in flag_summary_before['scan'].items():
                d['{}.scan_{}.num_rows_flagged.before'.format(prefix, scan_id)] = int(v['flagged'])
        
        if 'after' in summaries_by_name:
            flag_summary_after = summaries_by_name['after']

            for scan_id, v in flag_summary_after['scan'].items():
                d['{}.scan_{}.num_rows_flagged.after'.format(prefix, scan_id)] = int(v['flagged'])

        return d


class TargetflagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_targetflag

    Extract flagging summaries from TargetflagResults
    """

    result_cls = TargetflagResults
    child_cls = None
    generating_task = Targetflag

    def handle(self, result: TargetflagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: TargetflagResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        summaries_by_name = {s['name']: s for s in result.summarydict}

        num_flags_summary = summaries_by_name['Summary']['flagged']

        d = OrderedDict()
        d['{}.num_rows_flagged.summary'.format(prefix)] = int(num_flags_summary)

        flag_summary = summaries_by_name['Summary']
        for scan_id, v in flag_summary['scan'].items():
            d['{}.scan_{}.num_rows_flagged.summary'.format(prefix, scan_id)] = int(v['flagged'])

        return d


class FluxbootRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_fluxboot.

    The extracted values are:
       - the flux_densities value (single spw)
       - spectral index list values (spix, curvature, delta, gamma coefficients)

    """

    result_cls = FluxbootResults
    child_cls = None
    generating_task = Fluxboot

    def handle(self, result: FluxbootResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FluxbootResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        flux_densities = result.flux_densities
        spindex = result.spindex_results

        d = OrderedDict()

        for i, spw in enumerate(sorted(result.spws)): 
            # There may be spws without a solution, which aren't included here, so don't index by spw
            d['{}.flux_densities.spw_{}'.format(prefix, spw)] = flux_densities[i][0]

        d['{}.spindex'.format(prefix)] = float(spindex[0]['spix'])
        d['{}.curvature'.format(prefix)] = float(spindex[0]['curvature'])
        d['{}.delta'.format(prefix)] = float(spindex[0]['delta'])
        d['{}.gamma'.format(prefix)] = float(spindex[0]['gamma'])

        return d


class SolintRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_solint.

    The extracted values are:
       - long solution interval per band
       - short solution interval per band
       - shortsol2 variable interval per band

    """

    result_cls = SolintResults
    child_cls = None
    generating_task = Solint

    def handle(self, result: SolintResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SolintResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for band, value in result.longsolint.items():
            d['{}.longsolint.{}-band'.format(prefix, band)] = value
            d['{}.short_solint.{}-band'.format(prefix, band)] = result.short_solint[band]
            d['{}.shortsol2.{}-band'.format(prefix, band)] = result.shortsol2[band]

        return d


class PriorcalsRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_priorcals

    The extracted values are:
       - Opacities per spw
    """

    result_cls = PriorcalsResults
    child_cls = None
    generating_task = Priorcals

    def handle(self, result: PriorcalsResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: PriorcalResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for idx, opacity in enumerate(result.oc_result.opacities):
            spw = result.oc_result.spw[idx]
            d['{}.opacity.spw_{}'.format(prefix, spw)] = opacity

        return d


class VLASetjyRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_vlasetjy

    The extracted values are:
       - Stokes I flux value per field
    """

    result_cls = FluxCalibrationResults
    child_cls = None
    generating_task = VLASetjy

    def handle(self, result: FluxCalibrationResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FluxCalibrationResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for field, fluxlist in result.measurements.items():
            flux_I = float(fluxlist[0].I.value)
            d['{}.flux.I'.format(prefix)] = flux_I

        return d


class VLAStatwtRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifv_statwt

    The extracted values are:
       - mean and variance
    """

    result_cls = StatwtResults
    child_cls = None
    generating_task = Statwt

    def handle(self, result: StatwtResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: StatwtResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        d['{}.mean'.format(prefix)] = result.jobs[0]['mean']
        d['{}.variance'.format(prefix)] = result.jobs[0]['variance']

        return d


class VLAImportDataRegressionExtractor(RegressionExtractor):
    """
        Regression test result extractor for hifv_importdata

        The extracted values are:
           - number of antennas :)
           - max integration time
        """

    result_cls = VLAImportDataResults
    child_cls = None
    generating_task = VLAImportData

    def handle(self, result: VLAImportDataResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: VLAImportDataResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        d['{}.numantennas'.format(prefix)] = len(result.mses[0].antennas)
        d['{}.vla_max_integration_time'.format(prefix)] = result.mses[0].get_vla_max_integration_time()

        return d


class FlagDeterVLARegressionExtractor(RegressionExtractor):
    """
        Regression test result extractor for hifv_flagdata

        The extracted values are:
           - number of flags per category
        """

    result_cls = FlagDeterVLAResults
    child_cls = None
    generating_task = FlagDeterVLA

    def handle(self, result: FlagDeterVLAResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FlagDeterVLAResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        flag_categories = [y['name'] for y in result.summaries]

        for i, summary in enumerate(result.summaries):
            d['{}.flagged.{}'.format(prefix, flag_categories[i])] = int(summary['flagged'])

        return d


class testBPdcalsRegressionExtractor(RegressionExtractor):
    """
        Regression test result extractor for hifv_testBPdcals

        The extracted values are:
           - number of flagged solutions per band for bandpass and delay
        """

    result_cls = testBPdcalsResults
    child_cls = None
    generating_task = testBPdcals

    def handle(self, result: testBPdcalsResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: testBPdcalsResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for band, flagdict in result.flaggedSolnApplycalbandpass.items():
            d['{}.bandpass.flagged.{}-band'.format(prefix, band)] = int(flagdict['all']['flagged'])

        for band, flagdict in result.flaggedSolnApplycaldelay.items():
            d['{}.delay.flagged.{}-band'.format(prefix, band)] = int(flagdict['all']['flagged'])

        return d


class semiFinalBPdcalsRegressionExtractor(RegressionExtractor):
    """
        Regression test result extractor for hifv_semiFinalBPdcals

        The extracted values are:
           - number of flagged solutions per band for bandpass and delay
        """

    result_cls = semiFinalBPdcalsResults
    child_cls = None
    generating_task = semiFinalBPdcals

    def handle(self, result: semiFinalBPdcalsResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: semiFinalBPdcalsResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for band, flagdict in result.flaggedSolnApplycalbandpass.items():
            d['{}.bandpass.flagged.{}-band'.format(prefix, band)] = int(flagdict['all']['flagged'])

        for band, flagdict in result.flaggedSolnApplycaldelay.items():
            d['{}.delay.flagged.{}-band'.format(prefix, band)] = int(flagdict['all']['flagged'])

        return d


class FinalcalsRegressionExtractor(RegressionExtractor):
    """
        Regression test result extractor for hifv_finalcals

        The extracted values are:
           - number of flagged solutions for bandpass and delay
        """

    result_cls = FinalcalsResults
    child_cls = None
    generating_task = Finalcals

    def handle(self, result: FinalcalsResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FinalcalsResults object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        d['{}.bandpass.flagged'.format(prefix)] = int(result.flaggedSolnApplycalbandpass['all']['flagged'])

        d['{}.delay.flagged'.format(prefix)] = int(result.flaggedSolnApplycaldelay['all']['flagged'])

        return d


class SDBLFlagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_blfrag.

    The extracted values are:
       - the number of flagged and scanned rows before this task
       - the number of flagged and scanned rows after this task
    """

    result_cls = SDBLFlagResults
    child_cls = None
    generating_task = SDBLFlag

    def handle(self, result:SDBLFlagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDBLFlagResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for summary in result.outcome['flagdata_summary']:
            name = prop = None
            for k, v in summary.items():
                if 'name' == k:
                    name = v
                elif isinstance(v, dict):
                    prop = v
            if name is not None and prop is not None:
                d['{}.num_rows_flagged.{}'.format(prefix, name)] = int(prop['flagged'])
                for scan_id, v in prop['scan'].items():
                    d['{}.scan_{}.num_rows_flagged.{}'.format(prefix, scan_id, name)] = int(v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class SDImagingRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_imaging.

    The extracted values are:
        - QA score
    """

    result_cls = SDImagingResults
    child_cls = None
    generating_task = SDImaging

    def handle(self, result:SDImagingResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDImagingResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class SDRestoredataRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_restoredata.

    The extracted values are:
        - the number of flagged and scanned rows after this task
    """

    result_cls = SDRestoreDataResults
    child_cls = None
    generating_task = SDRestoreData

    def handle(self, result:SDRestoreDataResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDRestoreDataResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for session, mses in result.flagging_summaries.items():
            for ms_name, ms in mses.items():
                for target_name, target in ms.items():
                    if isinstance(target, dict):
                        d['{}.target_{}.num_rows_flagged'.format(prefix, target_name)] = int(target['flagged'])
                        for scan_id, v in target['scan'].items():
                            d['{}.target_{}.scan_{}.num_rows_flagged'.format(prefix, target_name, scan_id)] = int(v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class NRORestoredataRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hsdn_restoredata.

    The extracted values are:
        - the number of flagged and scanned rows after this task
    """

    result_cls = NRORestoreDataResults
    child_cls = None
    generating_task = NRORestoreData

    def handle(self, result: NRORestoreDataResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: NRORestoreDataInputs object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for session, mses in result.flagging_summaries.items():
            for ms_name, ms in mses.items():
                for target_name, target in ms.items():
                    if isinstance(target, dict):
                        d['{}.target_{}.num_rows_flagged'.format(prefix, target_name)] = int(target['flagged'])
                        for scan_id, v in target['scan'].items():
                            d['{}.target_{}.scan_{}.num_rows_flagged'.format(prefix, target_name, scan_id)] = int(
                                v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


def get_prefix(result:Results, task:StandardTaskTemplate) -> str:
    """
    Return a string used to prefix string of rows of result text.

    Args:
        result: Result object
        task: Task object
    
    Returns:
        prefix string
    """
    # A value of result.inputs['vis'] of some classes (ex: SDImagingResults) is a list object
    res_vis = result.inputs['vis'][0] if isinstance(result.inputs['vis'], list) else result.inputs['vis']
    vis, _ = os.path.splitext(os.path.basename(res_vis))
    casa_task = task_registry.get_casa_task(task)
    prefix = 's{}.{}.{}'.format(result.stage_number, casa_task, vis)
    return prefix


def extract_qa_score_regression(prefix:str, result:Results) -> OrderedDict:
    """
    Create QA strings are properties of result, and insert them to OrderedDict.

    Args:
        prefix: Prefix string
        result: Result object

    Returns:
        OrderedDict
    """
    d = OrderedDict()
    for qa_score in result.qa.pool:
        metric_name = qa_score.origin.metric_name
        # Remove all non-word characters (everything except numbers and letters)
        metric_name = re.sub(r"[^\w\s]", '', metric_name)
        # Replace all runs of whitespace with a single dash
        metric_name = re.sub(r"\s+", '-', metric_name)

        metric_score = qa_score.origin.metric_score
        score_value = qa_score.score

        d['{}.qa.metric.{}'.format(prefix, metric_name)] = metric_score
        d['{}.qa.score.{}'.format(prefix, metric_name)] = score_value
    return d


def extract_regression_results(context: Context) -> List[str]:
    """
    Extract regression result and return logs.

    Args:
        context: Context object
    
    Returns:
        String list for logging
    """
    unified = OrderedDict()
    for results_proxy in context.results:
        results = results_proxy.read()
        unified = union(unified, registry.handle(results))

    # return unified
    return ['{}={}'.format(k, v)for k, v in unified.items()]


def get_all_subclasses(cls: RegressionExtractor) -> List[RegressionExtractor]:
    """
    Get all subclasses from RegressionExtractor classes tree recursively.

    Args:
        cls: root class of subclasses
    
    Returns:
        list of RegressionExtractor
    """
    subclasses = cls.__subclasses__()
    for subclass in subclasses:
        subclasses += get_all_subclasses(subclass)
    return subclasses


# TODO enable runtime comparisons?
