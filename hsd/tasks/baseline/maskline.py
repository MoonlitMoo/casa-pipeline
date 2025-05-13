"""Task to create channel mask for baseline subtraction."""
import os
import time

from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.domain.datatable import DataTableIndexer
from pipeline.infrastructure import casa_tools
from . import simplegrid
from . import detection
from . import validation
from .. import common
from ..common import utils

from .typing import LineWindow

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.domain.singledish import MSReductionGroupDesc, MSReductionGroupMember

LOG = infrastructure.get_logger(__name__)

NoData = common.NoData


class MaskLineInputs(vdp.StandardInputs):
    """Inputs for mask creation task."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    window = vdp.VisDependentProperty(default=[])
    windowmode = vdp.VisDependentProperty(default='replace')
    edge = vdp.VisDependentProperty(default=(0, 0))
    broadline = vdp.VisDependentProperty(default=True)
    clusteringalgorithm = vdp.VisDependentProperty(default='hierarchy')

    @property
    def group_desc(self) -> 'MSReductionGroupDesc':
        """Return reduction group description."""
        return self.context.observing_run.ms_reduction_group[self.group_id]

    @property
    def reference_member(self) -> 'MSReductionGroupMember':
        """Return reference member of reduction group.

        The first member in the list is returned.
        """
        return self.group_desc[self.member_list[0]]

    def __init__(self,
                 context: 'Context',
                 iteration: int,
                 group_id: int,
                 member_list: List[int],
                 window: Optional[LineWindow] = None,
                 windowmode: Optional[str] = None,
                 edge: Optional[Tuple[int, int]] = None,
                 broadline: Optional[bool] = None,
                 clusteringalgorithm: Optional[str] = None) -> None:
        """Construct MaskLineInputs instance.

        Args:
            context: Pipeline context
            iteration: Iteration counter for baseline/blflag loop
            group_id: Reduction group id.
            member_list: List of member indices for the reduction group members
            window: Manual line window. Defaults to None, which means that no user-defined
                    line window is given.
            windowmode: Line window handling mode. 'replace' exclusively uses manual line window
                        while 'merge' merges manual line window into automatic line detection
                        and validation result. Defaults to 'replace' if None is given.
            edge: Edge channels to exclude. Defaults to None, which means that all channels
                  are processed.
            broadline: Detect broadline component or not. Defaults to True if None is given.
            clusteringalgorithm: Clustering algorithm to use. Allowed values are 'kmean',
                                 'hierarchy', or 'both'. Defaults to 'hierarchy' if None is given.
        """
        super(MaskLineInputs, self).__init__()

        self.context = context
        self.iteration = iteration
        self.group_id = group_id
        self.member_list = member_list
        self.window = window
        self.windowmode = windowmode
        self.edge = edge
        self.broadline = broadline
        self.clusteringalgorithm = clusteringalgorithm


class MaskLineResults(common.SingleDishResults):
    """Results class to hold the result of mask creation task."""

    def __init__(self,
                 task: Optional[Type[basetask.StandardTaskTemplate]] = None,
                 success: Optional[bool] = None,
                 outcome: Any = None) -> None:
        """Construct MaskLineResults instance.

        Args:
            task: Task class that produced the result.
            success: Whether task execution is successful or not.
            outcome: Outcome of the task execution.
        """
        super(MaskLineResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        No specific merge operation is done.

        Args:
            context: Pipeline context.
        """
        super(MaskLineResults, self).merge_with_context(context)

    def _outcome_name(self) -> str:
        """Return string representing the outcome.

        Returns:
            Empty string
        """
        return ''


class MaskLine(basetask.StandardTaskTemplate):
    """Task to create channel mask for baseline subtraction.

    MaskLine task creates channel mask by performing spectral
    line detection (DetectLine task) and validation of detected
    lines (ValidateLine task).
    """

    Inputs = MaskLineInputs

    def prepare(self) -> MaskLineResults:
        """Create channel mask."""
        context = self.inputs.context

        start_time = time.time()

        iteration = self.inputs.iteration
        group_id = self.inputs.group_id
        member_list = self.inputs.member_list
        group_desc = self.inputs.group_desc
        reference_member = self.inputs.reference_member
        reference_data = reference_member.ms
        reference_antenna = reference_member.antenna_id
        reference_field = reference_member.field_id
        reference_spw = reference_member.spw_id
        duplicated_member_mses = [group_desc[i].ms for i in member_list]
        # list of unique MS object in member list in the order that appears in context
        unique_member_mses = [ms for ms in context.observing_run.measurement_sets if ms in duplicated_member_mses]
        #dt_dict: key = origin_ms name, value = DataTable instance
        dt_dict = dict((os.path.basename(ms.origin_ms),
                        DataTable(utils.get_data_table_path(context, ms)))
                       for ms in unique_member_mses)
        t0 = time.time()
        # index_dict: key = origin_ms name, value = row IDs of DataTable
        index_dict = utils.get_index_list_for_ms2(dt_dict, group_desc, member_list)
        t1 = time.time()
        LOG.info('Elapsed time for generating index_dict: {0} sec'.format(t1 - t0))

        LOG.debug('index_dict=%s', index_dict)
        # debugging
        t0 = time.time()
        indexer = DataTableIndexer(context)
        def _g():
            for ms in unique_member_mses:
                origin_basename = os.path.basename(ms.origin_ms)
                if origin_basename in index_dict:
                    for i in index_dict[origin_basename]:
                        yield indexer.perms2serial(origin_basename, i)
        # index_list stores serial DataTable row IDs of all group members
        index_list = numpy.fromiter(_g(), dtype=numpy.int64)
        LOG.debug('index_list=%s', index_list)
        t1 = time.time()
        LOG.info('Elapsed time for generating index_list: {0} sec'.format(t1 - t0))
        # LOG.trace('all(spwid == {}) ? {}', spwid_list[0], numpy.all(dt.getcol('IF').take(index_list) == spwid_list[0]))
        # LOG.trace('all(fieldid == {}) ? {}', field_list[0], numpy.all(dt.getcol('FIELD_ID').take(index_list) == field_list[0]))
        if len(index_list) == 0:
            # No valid data
            outcome = {'detected_lines': [],
                       'cluster_info': {},
                       'flag_digits': {},
                       'grid_table': None}
            result = MaskLineResults(task=self.__class__,
                                     success=True,
                                     outcome=outcome)

            return result

        window = self.inputs.window
        windowmode = self.inputs.windowmode
        LOG.debug('{}: window={}, windowmode={}'.format(self.__class__.__name__, window, windowmode))
        edge = self.inputs.edge
        broadline = self.inputs.broadline
        clusteringalgorithm = self.inputs.clusteringalgorithm
        beam_size = casa_tools.quanta.convert(reference_data.beam_sizes[reference_antenna][reference_spw], 'deg')['value']
        observing_pattern = reference_data.observing_pattern[reference_antenna][reference_spw][reference_field]

        # parse window
        parser = detection.LineWindowParser(reference_data, window)
        parser.parse(reference_field)
        parsed_window = parser.get_result(reference_spw)

        LOG.debug('Members to be processed:')
        for (m, f, a, s) in utils.iterate_group_member(group_desc, member_list):#itertools.izip(vis_list, field_list, antenna_list, spwid_list):
            v = m.name
            LOG.debug('MS "%s" Field %s Antenna %s Spw %s', os.path.basename(v), f, a, s)

        # gridding size
        grid_size = beam_size

        # simple gridding
        t0 = time.time()
        gridding_inputs = simplegrid.SDSimpleGridding.Inputs(context, group_id, member_list, parsed_window,
                                                             windowmode)
        gridding_task = simplegrid.SDSimpleGridding(gridding_inputs)
        gridding_result = self._executor.execute(gridding_task, merge=False,
                                                 datatable_dict=dt_dict,
                                                 index_list=index_list)
        # gridded spectrum of each grid position x ncube and corrsponding grdi_table
        spectra = gridding_result.outcome['spectral_data']
        grid_table = gridding_result.outcome['grid_table']
        t1 = time.time()

        # return empty result if grid_table is empty
        if len(grid_table) == 0:  # or len(spectra) == 0:
            LOG.warning(
                'Line detection/validation will not be done since grid table is empty. Maybe all the data are flagged out in the previous step.')
            outcome = {'detected_lines': [],
                       'cluster_info': {},
                       'flag_digits': {},
                       'grid_table': None}
            result = MaskLineResults(task=self.__class__,
                                     success=True,
                                     outcome=outcome)

            return result

        LOG.trace('len(grid_table)=%s, spectra.shape=%s', len(grid_table), numpy.asarray(spectra).shape)
        LOG.trace('grid_table=%s', grid_table)
        LOG.debug('PROFILE simplegrid: elapsed time is %s sec', t1 - t0)

        # line finding
        t0 = time.time()
        detection_inputs = detection.DetectLine.Inputs(context, group_id, parsed_window, windowmode,
                                                       edge, broadline)
        line_finder = detection.DetectLine(detection_inputs)
        detection_result = self._executor.execute(line_finder, merge=False,
                                                  datatable_dict=dt_dict,
                                                  grid_table=grid_table,
                                                  spectral_data=spectra)
        # detected line channels for each grid position x ncube (grid_table row)
        detect_signal = detection_result.signals
        t1 = time.time()

        LOG.trace('detect_signal=%s', detect_signal)
        LOG.debug('PROFILE detection: elapsed time is %s sec', t1-t0)

        # line validation
        t0 = time.time()
        validator_cls = validation.ValidationFactory(observing_pattern)
        validation_inputs = validator_cls.Inputs(context, group_id, member_list,
                                                 iteration, grid_size,
                                                 grid_size, parsed_window, windowmode,
                                                 edge,
                                                 clusteringalgorithm=clusteringalgorithm)
        validator_task = validator_cls(validation_inputs)
        LOG.trace('len(index_list)=%s', len(index_list))
        validation_result = self._executor.execute(validator_task, merge=False,
                                                   datatable_dict=dt_dict,
                                                   index_list=index_list,
                                                   grid_table=grid_table,
                                                   detect_signal=detect_signal)
        lines = validation_result.outcome['lines']
        if 'channelmap_range' in validation_result.outcome:
            channelmap_range = validation_result.outcome['channelmap_range']
        else:
            channelmap_range = validation_result.outcome['lines']
        cluster_info = validation_result.outcome['cluster_info']
        flag_digits  = validation_result.outcome['flag_digits']

        # export datatables
        for datatable in dt_dict.values():
            datatable.exportdata(minimal=True)
        t1 = time.time()

        # LOG.debug('lines=%s'%(lines))
        LOG.debug('PROFILE validation: elapsed time is %s sec', t1-t0)

        # LOG.debug('cluster_info=%s'%(cluster_info))

        end_time = time.time()
        LOG.debug('PROFILE execute: elapsed time is %s sec', end_time-start_time)

        outcome = {'detected_lines': lines,
                   'channelmap_range': channelmap_range,
                   'cluster_info': cluster_info,
                   'flag_digits': flag_digits,
                   'grid_table': grid_table}
        result = MaskLineResults(task=self.__class__,
                                 success=True,
                                 outcome=outcome)

        return result

    def analyse(self, result: MaskLineResults) -> MaskLineResults:
        """Analyse results instance generated by prepare.

        Do nothing.

        Returns:
            MaskLineResults instance
        """
        return result
