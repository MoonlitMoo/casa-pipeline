"""Task to perform line validation based on clustering analysis."""
import collections
import math
import time
from math import sqrt
from numbers import Integral
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union

import numpy
import numpy.linalg as LA
import scipy.cluster.hierarchy as HIERARCHY
import scipy.cluster.vq as VQ

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.domain.datatable import DataTableIndexer
from . import rules
from .. import common
from ..common import utils

from .typing import ClusteringResult, DetectedLineList, LineWindow

if TYPE_CHECKING:
    from pipeline.domain.singledish import MSReductionGroupDesc, MSReductionGroupMember
    from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


class ValidateLineInputs(vdp.StandardInputs):
    """Inputs class for line validation tasks."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    edge = vdp.VisDependentProperty(default=(0, 0))
    nsigma = vdp.VisDependentProperty(default=3.0)
    xorder = vdp.VisDependentProperty(default=-1.0)
    yorder = vdp.VisDependentProperty(default=-1.0)
    broad_component = vdp.VisDependentProperty(default=False)
    clusteringalgorithm = vdp.VisDependentProperty(default=rules.ClusterRule['ClusterAlgorithm'])

    @property
    def group_desc(self) -> 'MSReductionGroupDesc':
        """Return reduction group instance of the current group."""
        return self.context.observing_run.ms_reduction_group[self.group_id]

    @property
    def reference_member(self) -> 'MSReductionGroupMember':
        """Return the first reduction group member instance in the current group."""
        return self.group_desc[self.member_list[0]]

    @property
    def windowmode(self) -> str:
        """Return windowmode value. Defaults to 'replace'."""
        return getattr(self, '_windowmode', 'replace')

    @windowmode.setter
    def windowmode(self, value: str) -> None:
        """Set windowmode value.

        Args:
            value: Either 'replace' or 'merge'

        Raises:
            ValueError: Invalid windowmode value
        """
        if value not in ['replace', 'merge']:
            raise ValueError("linewindowmode must be either 'replace' or 'merge'.")
        self._windowmode = value

    def __init__(self,
                 context: 'Context',
                 group_id: int,
                 member_list: List[int],
                 iteration: int,
                 grid_ra: float,
                 grid_dec: float,
                 window: Optional[LineWindow] = None,
                 windowmode: Optional[str] = None,
                 edge: Optional[Tuple[int, int]] = None,
                 nsigma: Optional[float] = None,
                 xorder: Optional[int] = None,
                 yorder: Optional[int] = None,
                 broad_component: Optional[bool] = None,
                 clusteringalgorithm: Optional[str] = None) -> None:
        """Construct ValidateLineInputs instance.

        Args:
            context: Pipeline context
            group_id: Reduction group ID
            member_list: List of reduction group member IDs
            iteration: Iteration counter for baseline/blflag loop
            grid_ra: Horizontal (longitudinal) spacing of spatial grids.
                     The value should be the one without declination correction.
            grid_dec: Vertical (latitudinal) spacing of spatial grids.
            window: Manual line window. Defaults to None, which means that no user-defined
                    line window is given.
            windowmode: Line window handling mode. 'replace' exclusively uses manual line window
                        while 'merge' merges manual line window into automatic line detection
                        and validation result. Defaults to 'replace' if None is given.
            edge: Edge channels to exclude. Defaults to None, which means that all channels
                  are processed.
            nsigma: Threshold for iterative N-sigma clipping. No iterative clipping is done if
                    nsigma is None or negative value. Defaults to 3 if None is given.
            xorder: Polynomial order for two-dimensional fitting of line properties
                    along horizontal (longitudinal) axis. The order is automatically determined
                    if None or negative value is given.
            yorder: Polynomial order for two-dimensional fitting of line properties
                    along vertical (latitudinal) axis. The order is automatically determined
                    if None or negative value is given.
            broad_component: Process broad component if True. Defaults to False if None is given.
            clusteringalgorithm: Clustering algorithm to use. Allowed values are 'kmean',
                                 'hierarchy', or 'both'. Defaults to 'hierarchy' if None is given.
        """
        super(ValidateLineInputs, self).__init__()

        self.context = context
        self.group_id = group_id
        self.member_list = member_list
        self.iteration = iteration
        self.grid_ra = grid_ra
        self.grid_dec = grid_dec
        self.window = window
        self.windowmode = windowmode
        self.edge = edge
        self.nsigma = nsigma
        self.xorder = xorder
        self.yorder = yorder
        self.broad_component = broad_component
        self.clusteringalgorithm = clusteringalgorithm


class ValidateLineResults(common.SingleDishResults):
    """Results class to hold the result of line validation."""

    def __init__(self,
                 task: Optional[Type[basetask.StandardTaskTemplate]] = None,
                 success: Optional[bool] = None,
                 outcome: Any = None) -> None:
        """Construct ValidateLineResults instance.

        Args:
            task: Task class that produced the result.
            success: Whether task execution is successful or not.
            outcome: Outcome of the task execution.
        """
        super(ValidateLineResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        No specific merge operation is done.

        Args:
            context: Pipeline context.
        """
        super(ValidateLineResults, self).merge_with_context(context)

    def _outcome_name(self) -> str:
        """Return string representing the outcome.

        Returns:
            Empty string
        """
        return ''


class ValidateLineSinglePointing(basetask.StandardTaskTemplate):
    """Line validation task for single/multi pointing observation.

    This class is for single-pointing or multi-pointing (collection of
    fields with single-pointing).
    """

    Inputs = ValidateLineInputs

    def prepare(self,
                datatable_dict: dict,
                index_list: List[int],
                grid_table: Any = None,
                detect_signal: Optional[dict] = None):
        """Perform line validation for single/multi pointing observation.

        Accept all detected lines without clustering analysis.

        Args:
            datatable_dict: Dictionary holding datatable instance per MS.
            index_list: List of consecutive datatable row numbers.
            grid_table: Not used
            detect_signal: List of detected lines per spatial position. Its format is
                           as follows.

                detect_signal = {
                    ID1: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    [LineStartChannel2, LineEndChannel2],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]],
                    IDn: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]]
                }

        Returns:
            ValidateLineResults instance, which contains list of line parameters
            with validation results. The list is stored in outcome['lines'] and
            its format is as follows:

                [LineCenter, LineWidth, Validity]

            where Validity is boolean value that indicates whether or not the
            detected line is validated. OK (validated) for True while NG for False.
        """
        window = self.inputs.window
        windowmode = self.inputs.windowmode

        assert detect_signal is not None

        # indexer translates serial index into per-MS index
        indexer = DataTableIndexer(self.inputs.context)

        # for Pre-Defined Spectrum Window
        if windowmode == 'replace' and (window is None or len(window) > 0):
            LOG.info(f'Skip line validation: windowmode="{windowmode}", window="{window}"')
            lines = _to_validated_lines(detect_signal)
            # TODO: review whether this relies on order of dictionary values.
            signal = list(detect_signal.values())[0]
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                datatable.putcell('MASKLIST', row, signal[2])
                datatable.putcell('NOCHANGE', row, False)
            outcome = {'lines': lines,
                       'channelmap_range': lines,
                       'cluster_info': {},
                       'flag_digits': {} }
            result = ValidateLineResults(task=self.__class__,
                                         success=True,
                                         outcome=outcome)

            return result

        # Dictionary for final output
        lines = []

        # register manually specified line windows to lines
        if window is not None:
            for w in window:
                center = float(sum(w)) / 2
                width = max(w) - min(w)
                lines.append([center, width, True])

        LOG.info('Accept all detected lines without clustering analysis.')

        iteration = self.inputs.iteration

        # First cycle
        #if len(grid_table) == 0:
        if iteration == 0:
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                mask_list = datatable.getcell('MASKLIST', row)
                no_change = datatable.getcell('NOCHANGE', row)
                #LOG.debug('DataTable = %s, detect_signal = %s, OldFlag = %s' % (mask_list, detect_signal[row][2], no_change))
                # datatable.putcell('MASKLIST', row, detect_signal[row][2])
                datatable.putcell('MASKLIST', row, detect_signal[0][2])
                datatable.putcell('NOCHANGE', row, False)

        # Iteration case
        else:
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                mask_list = datatable.getcell('MASKLIST', row)
                no_change = datatable.getcell('NOCHANGE', row)
                #LOG.debug('DataTable = %s, detect_signal = %s, OldFlag = %s' % (mask_list, detect_signal[0][2], no_change))
                if mask_list == detect_signal[0][2]:
                    #if type(no_change) != int:
                    if no_change < 0:
                        # 2013/05/17 TN
                        # Put iteration itself instead to subtract 1 since
                        # iteration counter is incremented *after* the
                        # baseline subtraction in refactorred code.
                        #datatable.putcell('NOCHANGE',row,iteration - 1)
                        datatable.putcell('NOCHANGE', row, iteration)
                else:
                    datatable.putcell('MASKLIST', row, detect_signal[0][2])
                    datatable.putcell('NOCHANGE', row, False)
        outcome = {'lines': lines,
                   'channelmap_range': lines,
                   'cluster_info': {},
                   'flag_digits': {} }
        result = ValidateLineResults(task=self.__class__,
                                     success=True,
                                     outcome=outcome)

        return result

    def analyse(self, result: ValidateLineResults) -> ValidateLineResults:
        """Analyse results instance generated by prepare.

        Do nothing.

        Returns:
            ValidateLineResutls instance
        """
        return result


class ValidateLineRaster(basetask.StandardTaskTemplate):
    """Line validation task for OTF raster observation."""

    Inputs = ValidateLineInputs

    CLUSTER_WHITEN = 1.0

    # as of 2017/7/4 Valid=0.5, Marginal=0.35, Questionable=0.2
    # should be Valid=0.7, Marginal=0.5, Questionable=0.3
    Valid = rules.ClusterRule['ThresholdValid']
    Marginal = rules.ClusterRule['ThresholdMarginal']
    Questionable = rules.ClusterRule['ThresholdQuestionable']
    MinFWHM = rules.LineFinderRule['MinFWHM']
    #MaxFWHM = rules.LineFinderRule['MaxFWHM']
    #Clustering_Algorithm = rules.ClusterRule['ClusterAlgorithm']
    DebugOutName = '%05d' % (int(time.time())%100000)
    DebugOutVer = [0, 0]

    @property
    def MaxFWHM(self) -> int:
        """Return maximum FWHM to consider for line validation.

        Max FWHM is 1/3 of total number of channels excluding edge channels
        specified by inputs.edge.

        Returns:
            Maximum FWHM in number of channels
        """
        num_edge = sum(self.inputs.edge)
        spw = self.inputs.reference_member.spw
        nchan = spw.num_channels
        return int(max(0, nchan - num_edge) // 3)

    def validate_cluster(
        self,
        clustering_result: ClusteringResult,
        index_list: List[int],
        detect_signal: dict,
        PosList: numpy.ndarray,
        Region2: numpy.ndarray
    ) -> Tuple[dict, List[List[Union[int, bool]]], List[List[Union[int, bool]]], numpy.ndarray]:
        """Validate cluster detected by clustering analysis.

        This method validates clusters detected in line center vs line width space.
        Validation utilizes spatial distribution of lines associated with the cluster.
        Property of validated lines are interpolated in two-dimensional space and
        set to each spatial data point.

        Args:
            clustering_result: Clustering result
            index_list: List of consecutive datatable row numbers
            detect_signal: List of detected lines per spatial position. Its format is
                           as follows.

                detect_signal = {
                    ID1: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    [LineStartChannel2, LineEndChannel2],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]],
                    IDn: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]]
                }
            PosList: List of pointings (RA and Dec) of ON_SOURCE data
            Region2: List of line properties (line width and line center)
                     for each data

        Returns:
            4-tuple of final line properties for each ON_SOURCE pointings (RealSignal),
            line property of detected clusters (lines), line property for plotting
            (channelmap_range), and flag per validation stage for each cluster (cluster_flag).

            cluster_flag is data for plotting clustering analysis results.
            It stores GridCluster quantized by given thresholds.
            it is defined as integer array and one digit is assigned to
            one clustering stage in each integer value:

                1st digit: detection
                2nd digit: validation
                3rd digit: smoothing
                4th digit: final

            If GridCluster value exceeds any threshold, corresponding
            digit is incremented. For example, flag 3210 stands for,

                value didn't exceed any thresholds in detection, and
                exceeded one (out of three) threshold in validation, and
                exceeded two (out of three) thresholds in smoothing, and
                exceeded three (out of four) thresholds in final.
        """
        # input parameters
        grid_ra = self.inputs.grid_ra
        grid_dec = self.inputs.grid_dec
        broad_component = self.inputs.broad_component
        xorder = self.inputs.xorder
        yorder = self.inputs.yorder

        # decompose clustering result
        (Ncluster, Bestlines, BestCategory, Region) = clustering_result

        # Calculate Parameters for Gridding
        DecCorrection = 1.0 / math.cos(PosList[1][0] / 180.0 * 3.141592653)
        grid_ra *= DecCorrection
        wra = PosList[0].max() - PosList[0].min()
        wdec = PosList[1].max() - PosList[1].min()
        cra = PosList[0].min() + wra/2.0
        cdec = PosList[1].min() + wdec/2.0
        # 2010/6/11 +1.0 -> +1.01: if wra is n x grid_ra (n is a integer), int(wra/grid_ra) is not n in some cases because of the lack of accuracy.
        nra = 2 * (int((wra/2.0 - grid_ra/2.0)/grid_ra) + 1) + 1
        ndec = 2 * (int((wdec/2.0 - grid_dec/2.0)/grid_dec) + 1) + 1
        x0 = cra - grid_ra/2.0 - grid_ra*(nra-1)/2.0
        y0 = cdec - grid_dec/2.0 - grid_dec*(ndec-1)/2.0
        LOG.debug('Grid = %s x %s\n', nra, ndec)
        if 'grid' not in self.cluster_info:
            self.cluster_info['grid'] = {
                'ra_min': x0,
                'dec_min': y0,
                'grid_ra': grid_ra,
                'grid_dec': grid_dec
            }
        # Create Space for storing the list of spectrum (ID) in the Grid
        # 2013/03/27 TN
        # Grid2SpectrumID stores index of index_list instead of row numbers
        # that are held by index_list.
        Grid2SpectrumID = [[[] for y in range(ndec)] for x in range(nra)]
        for i in range(len(PosList[0])):
            Grid2SpectrumID[int((PosList[0][i] - x0)/grid_ra)][int((PosList[1][i] - y0)/grid_dec)].append(i)

        # Sort lines and Category by LineCenter: lines[][0]
        LineIndex = numpy.argsort([line[0] for line in Bestlines[:Ncluster]])
        lines = [Bestlines[i] for i in LineIndex]
        print('Ncluster, lines: {} {}'.format(Ncluster, lines))
        print('LineIndex: {}'.format(LineIndex))

        ### 2011/05/17 anti-scaling of the line width
        for Nc in range(Ncluster):
            lines[Nc][1] *= self.CLUSTER_WHITEN

        LineIndex2 = numpy.argsort(LineIndex)
        print('LineIndex2: {}'.format(LineIndex2))
        print('BestCategory: {}'.format(BestCategory))

        category = [LineIndex2[bc] for bc in BestCategory]

        ######## Clustering: Detection Stage ########
        ProcStartTime = time.time()
        LOG.info('Clustering: Detection Stage Start')

        (GridCluster, GridMember, cluster_flag) = self.detection_stage(Ncluster, nra, ndec, x0, y0, grid_ra, grid_dec, category,
                                                         Region, detect_signal)

        ProcEndTime = time.time()
        LOG.info('Clustering: Detection Stage End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))

        ######## Clustering: Validation Stage ########
        ProcStartTime = time.time()
        LOG.info('Clustering: Validation Stage Start')

        (GridCluster, GridMember, lines, cluster_flag) = self.validation_stage(GridCluster, GridMember, lines, cluster_flag)

        ProcEndTime = time.time()
        LOG.info('Clustering: Validation Stage End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))

        ######## Clustering: Smoothing Stage ########
        # Rating:  [0.0, 0.4, 0.5, 0.4, 0.0]
        #          [0.4, 0.7, 1.0, 0.7, 0.4]
        #          [0.5, 1.0, 6.0, 1.0, 0.5]
        #          [0.4, 0.7, 1.0, 0.7, 0.4]
        #          [0.0, 0.4, 0.5, 0.4, 0.0]
        # Rating = 1.0 / (Dx**2 + Dy**2)**(0.5) : if (Dx, Dy) == (0, 0) rating = 6.0

        ProcStartTime = time.time()
        LOG.info('Clustering: Smoothing Stage Start')

        (GridCluster, lines, cluster_flag) = self.smoothing_stage(GridCluster, lines, cluster_flag)

        ProcEndTime = time.time()
        LOG.info('Clustering: Smoothing Stage End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))

        ######## Clustering: Final Stage ########
        ProcStartTime = time.time()
        LOG.info('Clustering: Final Stage Start')

        # create virtual index_list
        (RealSignal, lines, channelmap_range, cluster_flag) = self.final_stage(GridCluster, GridMember, Region, Region2,
                                                                 lines, category, grid_ra, grid_dec, broad_component,
                                                                 xorder, yorder, x0, y0, Grid2SpectrumID, index_list,
                                                                 PosList, cluster_flag)

        ProcEndTime = time.time()
        LOG.info('Clustering: Final Stage End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))

        return RealSignal, lines, channelmap_range, cluster_flag

    def prepare(self,
                datatable_dict: dict,
                index_list: numpy.ndarray,
                grid_table: List[Union[int, float, numpy.ndarray]],
                detect_signal: collections.OrderedDict
    ) -> ValidateLineResults:
        """Validate spectral lines detected by detection module.

        As a first step, the method performs clustering analysis on detected
        lines in line width vs line center space. Detected clusters are then
        analyzed and set True/False flag based on the spatial distribution
        of the cluster members in celestial coordinate. Finally, cluster
        line properties are interpolated in two-dimensional celestial space
        and are distributed to each ON_SOURCE data point.

        Sigma clipping iterations will be applied if inputs.nsigma is positive

        Args:
            datatable_dict: Dictionary holding datatable instance per MS.
            index_list: List of consecutive datatable row numbers. Defaults to None.
            grid_table: Metadata for gridding. See simplegrid.py for detail.
            detect_signal: List of detected lines per spatial position. Its format is
                           as follows.

                detect_signal = {
                    ID1: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    [LineStartChannel2, LineEndChannel2],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]],
                    IDn: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]]
                }

        Returns:
            ValidateLineResults instance, which contains list of line parameters
            with validation results. The list is stored in outcome['lines'] and
            its format is as follows:

                [LineCenter, LineWidth, Validity]

            where Validity is boolean value that indicates whether or not the
            detected line is validated. OK (validated) for True while NG for False.
        """
        window = self.inputs.window
        windowmode = self.inputs.windowmode
        LOG.debug('{}: window={}, windowmode={}'.format(self.__class__.__name__, window, windowmode))

        # indexer translates serial index into per-MS index
        indexer = DataTableIndexer(self.inputs.context)

        # for Pre-Defined Spectrum Window
        if windowmode == 'replace' and (window is None or len(window) > 0):
            LOG.info(f'Skip line validation: windowmode="{windowmode}", window="{window}"')
            lines = _to_validated_lines(detect_signal)
            # TODO: review whether this relies on order of dictionary values.
            signal = list(detect_signal.values())[0]
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                datatable.putcell('MASKLIST', row, signal[2])
                datatable.putcell('NOCHANGE', row, False)
            outcome = {'lines': lines,
                       'channelmap_range': lines,
                       'cluster_info': {},
                       'flag_digits': {} }
            result = ValidateLineResults(task=self.__class__,
                                         success=True,
                                         outcome=outcome)

            return result

        manual_window = []
        manual_range = []
        # register manually specified line windows to lines
        if window is not None:
            for w in window:
                center = float(sum(w)) / 2
                width = max(w) - min(w)
                manual_window.append([center, width, True, 0.0])
            manual_range.extend(window)

        iteration = self.inputs.iteration

        origin_vis, _row = indexer.serial2perms(index_list[0])
        self.nchan = datatable_dict[origin_vis].getcell('NCHAN', _row)
        self.nsigma = self.inputs.nsigma

        ProcStartTime = time.time()
        LOG.info('2D fit the line characteristics...')

        #tSFLAG = datatable.getcol('FLAG_SUMMARY')
        Totallines = 0
        RMS0 = 0.0
        lines = []
        self.cluster_info = {}
        self.flag_digits = {'detection': 1, 'validation': 10,
                            'smoothing': 100, 'final': 1000}

        # RASTER CASE
        # Read data from Table to generate ID -> RA, DEC conversion table
        Region = []
        dummy = []
        flag = 1
        Npos = 0

        # 2017/7/4 clean-up detect_signal
        LOG.trace('Before: Detect_Signal = %s', detect_signal)
        detect_signal = self.clean_detect_signal(detect_signal)
        LOG.trace('After: Detect_Signal = %s', detect_signal)

        for row in range(len(grid_table)):
            # detect_signal[row][2]: [[LineStartChannelN, LineEndChannelN, Binning],[],,,[]]
            if len(detect_signal[row][2]) != 0 and detect_signal[row][2][0][0] != -1:
                Npos += 1
                for line in detect_signal[row][2]:
                    # Check statistics flag. tSFLAG[row]==1 => Valid Spectra 2008/1/17
                    # Bug fix 2008/5/29
                    #if (line[0] != line[1]) and ((len(grid_table) == 0 and tSFLAG[row] == 1) or len(grid_table) != 0):
                    # refering tSFLAG is not correct
                    if line[0] != line[1]: #and tSFLAG[row] == 1:
                        #2014/11/28 add Binning info into Region
                        Region.append([row, line[0], line[1], detect_signal[row][0], detect_signal[row][1], flag, line[2]])
                        ### 2011/05/17 make cluster insensitive to the line width: Whiten
                        dummy.append([float(line[1] - line[0]) / self.CLUSTER_WHITEN, 0.5 * float(line[0] + line[1])])
        Region2 = numpy.array(dummy) # [FullWidth, Center]
        ### 2015/04/22 save Region to file for test
        if infrastructure.logging.logging_level == infrastructure.logging.LOGGING_LEVELS['trace'] or \
           infrastructure.logging.logging_level == infrastructure.logging.LOGGING_LEVELS['debug']:
            self.DebugOutVer[0] += 1
            with open('ClstRegion.%s.%02d.txt' % (self.DebugOutName, self.DebugOutVer[0]), 'w') as fp:
                for i in range(len(Region)):
                    fp.writelines('%d %f %f %f %f %d %d\n' % (Region[i][0], Region[i][1], Region[i][2], Region[i][3],
                                                              Region[i][4], Region[i][5], Region[i][6]))

        del dummy
        LOG.debug('Npos = %s', Npos)
        # 2010/6/9 for non-detection
        if Npos == 0 or len(Region2) == 0:
            if len(manual_range) == 0:
                signal = [[-1, -1]]
            else:
                signal = manual_range
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                datatable.putcell('MASKLIST', row, signal)
                datatable.putcell('NOCHANGE', row, False)
            outcome = {'lines': manual_window,
                       'channelmap_range': manual_window,
                       'cluster_info': {},
                       'flag_digits': {} }
            result = ValidateLineResults(task=self.__class__,
                                         success=True,
                                         outcome=outcome)

            return result

        # 2008/9/20 Dec Effect was corrected
        def _g(colname):
            for i in index_list:
                origin_vis, j = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                yield datatable.getcell(colname, j)
        ras = numpy.fromiter(_g('OFS_RA'), dtype=numpy.float64)
        decs = numpy.fromiter(_g('OFS_DEC'), dtype=numpy.float64)
        PosList = numpy.asarray([ras, decs])
        ProcEndTime = time.time()
        LOG.info('Clustering: Initialization End: Elapsed time = %s sec', ProcEndTime - ProcStartTime)


        ######## Clustering: K-mean Stage ########
        # K-mean Clustering Analysis with LineWidth and LineCenter
        # Max number of protect regions are SDC.SDParam['Cluster']['MaxCluster'] (Max lines)
        ProcStartTime = time.time()
        LOG.info('Clustering Analysis Start')

        # Bestlines: [[center, width, T/F],[],,,[]]
        clustering_algorithm = self.inputs.clusteringalgorithm
        LOG.info('clustering algorithm is \'%s\'', clustering_algorithm)
        clustering_results = collections.OrderedDict()
        if clustering_algorithm == 'kmean':
            #(Ncluster, Bestlines, BestCategory, Region) = self.clustering_kmean(Region, Region2)
            clustering_results[clustering_algorithm] = self.clustering_kmean(Region, Region2)
        elif clustering_algorithm == 'hierarchy':
            #(Ncluster, Bestlines, BestCategory, Region) = self.clustering_hierarchy(Region, Region2, nThreshold=rules.ClusterRule['ThresholdHierarchy'], method='single')
            clustering_results[clustering_algorithm] = self.clustering_hierarchy(Region, Region2, nThreshold=rules.ClusterRule['ThresholdHierarchy'],
                                                                                 nThreshold2=rules.ClusterRule['ThresholdHierarchy2'], method='single')
        elif clustering_algorithm == 'both':
            clustering_results['kmean'] = self.clustering_kmean(Region, Region2)
            clustering_results['hierarchy'] = self.clustering_hierarchy(Region, Region2, nThreshold=rules.ClusterRule['ThresholdHierarchy'],
                                                                        nThreshold2=rules.ClusterRule['ThresholdHierarchy2'], method='single')
        else:
            LOG.error('Invalid clustering algorithm: {}'.format(clustering_algorithm))

        ProcEndTime = time.time()
        LOG.info('Clustering Analysis End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))
        # 2017/8/15 for non-detection after cleaninig
        #if Ncluster == 0:
        if sum([r[0] for r in clustering_results.values()]) == 0:
            if len(manual_range) == 0:
                signal = [[-1, -1]]
            else:
                signal = manual_range
            for i in index_list:
                origin_vis, row = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                datatable.putcell('MASKLIST', row, signal)
                datatable.putcell('NOCHANGE', row, False)
            outcome = {'lines': manual_window,
                       'channelmap_range': manual_window,
                       'cluster_info': {},
                       'flag_digits': {} }
            result = ValidateLineResults(task=self.__class__,
                                         success=True,
                                         outcome=outcome)
            return result

        ### 2011/05/17 anti-scaling of the line width
        Region2[:, 0] = Region2[:, 0] * self.CLUSTER_WHITEN
        # validate cluster
        assert clustering_algorithm in ['kmean', 'hierarchy', 'both']
        validated = [
            self.validate_cluster(v,
                                  index_list,
                                  detect_signal,
                                  PosList,
                                  Region2)
            for k, v in clustering_results.items()
        ]
        # Merge results from multiple clustering analysises
        # If more than one results exist, minimum contents of RealSignal will be merged
        # and remaining items (PolList) will be lost
        # Original RealSignal data will be stored in validated[x][0]
        (RealSignal, lines, channelmap_range, cluster_flag) = self._merge_cluster_result(validated)
        self.cluster_info['cluster_flag'] = cluster_flag

        # Merge masks if possible
        ProcStartTime = time.time()
        LOG.info('Clustering: Merging Start')
        # RealSignal should have all row's as its key
        tmp_index = 0
        for vrow in index_list:
            if vrow in RealSignal:
                signal = self.__merge_lines(RealSignal[vrow][2], self.nchan)
                signal.extend(manual_range)
            else:
                signal = manual_range
                if len(signal) == 0:
                    signal = [[-1, -1]]
                #RealSignal[row] = [PosList[0][tmp_index], PosList[1][tmp_index], signal]
            tmp_index += 1

            origin_vis, row = indexer.serial2perms(vrow)
            datatable = datatable_dict[origin_vis]

            # In the following code, access to MASKLIST and NOCHANGE columns
            # is direct to underlying table object instead of access via
            # datatable object's method since direct access is faster.
            # Note that MASKLIST [[-1,-1]] represents that no masks are
            # available, while NOCHANGE -1 indicates NOCHANGE is False.
            #tMASKLIST = datatable.getcell('MASKLIST',row)
            #tNOCHANGE = datatable.getcell('NOCHANGE',row)
            tMASKLIST = datatable.getcell('MASKLIST', row)
            if len(tMASKLIST) == 0 or tMASKLIST[0][0] < 0:
                tMASKLIST = []
            else:
                tMASKLIST = tMASKLIST.tolist()  # list(tMASKLIST)
            tNOCHANGE = datatable.getcell('NOCHANGE', row)
            if tMASKLIST == signal:
                if tNOCHANGE < 0:
                    # 2013/05/17 TN
                    # Put iteration itself instead to subtract 1 since iteration
                    # counter is incremented *after* baseline subtraction
                    # in refactorred code.
                    datatable.putcell('NOCHANGE', row, iteration)
            else:
                datatable.putcell('MASKLIST', row, signal)
                datatable.putcell('NOCHANGE', row, -1)
        del RealSignal
        ProcEndTime = time.time()
        LOG.info('Clustering: Merging End: Elapsed time = %s sec', (ProcEndTime - ProcStartTime))

        lines.extend(manual_window)
        channelmap_range.extend(manual_window)

        outcome = {'lines': lines,
                   'channelmap_range': channelmap_range,
                   'cluster_info': self.cluster_info,
                   'flag_digits': self.flag_digits }
        result = ValidateLineResults(task=self.__class__,
                                     success=True,
                                     outcome=outcome)

        return result

    def analyse(self, result: ValidateLineResults) -> ValidateLineResults:
        """Analyse results instance generated by prepare.

        Do nothing.

        Returns:
            ValidateLineResutls instance
        """
        return result

    def _merge_cluster_info(
        self,
        algorithm: str,
        cluster_score: List[List[int]],
        detected_lines: numpy.ndarray,
        cluster_property: List[List[Union[int, bool]]],
        cluster_scale: float) -> None:
        """Merge information on clustering analysis into "cluster_info" attribute.

        Merges args into "cluster_info" attribute according to the following rule:

            - cluster_score for kmean takes priority over the one for hierarchy
            - detected_lines is registered only once (since detected_lines is an
              input for clustering analysis and should be the same among clustering
              algorithm)
            - cluster_property is accumulated
            - cluster_scale is registered only once (since the value is shared
              among clustering algorithm)

        Args:
            algorithm: Clustering algorithm. Either 'kmean' or 'hierarchy'.
            cluster_score: Cluster score vs number of clusters
            detected_lines: List of line properties per grid position
            cluster_property: List of properties (line width, line center) for
                              each detected clusters
            cluster_scale: Scaling factor
        """
        actions = {
            'cluster_score': ('kmean', cluster_score),
            'detected_lines': ('skip', detected_lines),
            'cluster_property': ('append', cluster_property),
            'cluster_scale': ('skip', cluster_scale)
        }
        for key, (action, value) in actions.items():
            if key not in self.cluster_info:
                self.cluster_info[key] = value
            elif action == 'append':
                self.cluster_info[key].extend(value)
            elif action == algorithm:
                self.cluster_info[key] = value

    def _merge_cluster_result(
        self,
        result_list: List[Tuple[dict, List[List[Union[int, bool]]], List[List[Union[int, bool]]], numpy.ndarray]]
    ) -> Tuple[dict, List[List[Union[int, bool]]], List[List[Union[int, bool]]], numpy.ndarray]:
        """Merge multiple clustering analysis results into one.

        Take union on detected clusters. If length of result_list is 1, simply return
        the first item.

        Args:
            result_list: List of clustering analysis result

        Returns:
            Merged result
        """
        if len(result_list) == 1:
            return tuple(result_list[0])

        merged_RealSignal = collections.defaultdict(lambda: [None, None, []])
        for r in result_list:
            RealSignal = r[0]
            for k, v in RealSignal.items():
                merged_RealSignal[k][2].extend(v[2])
        merged_lines = [l for r in result_list for l in r[1]]
        merged_channelmap_ranges = [l for r in result_list for l in r[2]]
        merged_flag = numpy.concatenate([r[3] for r in result_list], axis=0)

        return merged_RealSignal, merged_lines, merged_channelmap_ranges, merged_flag

    def clean_detect_signal(self, detect_signal: dict) -> dict:
        """Exclude false detections based on the detection rate.

        Spectra in each grid positions are splitted into 3 groups along time series.
        Group of spectra is then combined to 1 spectrum. So, one grid position has
        3 combined spectra.
        Suppose that the real signal is correlated but the error is not, we can
        clean false signals (not correlated) in advance of the validation stage.

        Args:
            detect_signal: List of detected lines per spatial position. Its format is
                           as follows.

                detect_signal = {
                    ID1: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    [LineStartChannel2, LineEndChannel2],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]],
                    IDn: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]]
                }

        Returns:
            detect_signal after cleaning
        """
        # grouping by position
        Gthreshold = 1.0 / 3600.
        # TODO: review whether the following relies on a specific order of keys.
        DSkey = list(detect_signal.keys())
        PosGroup = []
        # PosGroup: [[ID,ID,ID],[ID,ID,ID],...,[ID,ID,ID]]
        for ID in list(detect_signal.keys()):
            if ID in DSkey:
                del DSkey[DSkey.index(ID)]
                DStmp = DSkey[:]
                PosGroup.append([ID])
                for nID in DStmp:
                    if abs(detect_signal[ID][0] - detect_signal[nID][0]) < Gthreshold and abs(detect_signal[ID][1] - detect_signal[nID][1]) < Gthreshold:
                        del DSkey[DSkey.index(nID)]
                        PosGroup[-1].append(nID)
        LOG.debug('clean_detect_signal: PosGroup = %s', PosGroup)
        for PList in PosGroup:
            if len(PList) > 2:
                data = collections.OrderedDict()
                for i in PList:
                    data[i] = detect_signal[i][2]
                # threshold 0.7: identical line is detected in all 3 data: strict checking
                # threshold 0.6: identical line is detected in 2 data out of 3
                ret = self.clean_detect_line(data.copy(), threshold=0.7)
                for i in PList:
                    detect_signal[i][2] = ret[i]

        return detect_signal

    def clean_detect_line(self, data: collections.OrderedDict, threshold: float = 0.6) -> dict:
        """Exclude false detection by comparing three signals.

        Select only line candidates with good possibility by checking all spectra
        taken at the same position. If list of detected signals per spectrum, data,
        is given, this method evaluates detection rate of each signal where identity
        of signals is checked by CheckLineIdentity method. If the rate exceeds threshold,
        the signal is marked as "true signal". Default threshold for "true signal" is
        0.6 (60%).

        Meaning of default value is interpreted as follows. Suppose we have three sets
        of signals. If identical lines are found in two out of three signals, detection
        rate is 2/3 or 0.66666.... Therefore, default value corresponds to the condition
        that the signal is true detection if it is found in two out of three signals.
        Larger threshold such as 0.7 is more strict check, which effectively requires
        that the signal must be found in all three.

        Args:
            data: List of properties of detected lines to be examined. Format is
                  as follows

                      {ID1: [[LineStart, LineEnd, Binning],,,],
                       ID2: [[LineStart, LineEnd, Binning],,,],
                       ...
                       IDn: [[LineStart, LineEnd, Binning],,,]}
            threshold: threshold for the exclusion. Ranges between 0 and 1.
                       Defalts to 0.6.

        Returns:
            List of lines that are regarded as "true detection". For false
            detection, dict value will be [-1, -1, 1].
        """
        ret = {}
        NSP = float(len(data))
        for ID in list(data.keys()):
            ret[ID] = []
        for ID in list(data.keys()):
            for i in range(len(data[ID])):
                if data[ID][i][0] != -1:
                    ref = data[ID][i][:]
                    tmp = []
                    # if binning > 1: expected number of lines in the grid doubles, i.e., two different offsets
                    for nID in list(data.keys()):
                        Lines = [0.0, 0.0, 0.0]
                        for j in range(len(data[nID])):
                            # check binning is the same, and overlap
                            if data[nID][j][2] == ref[2] and self.CheckLineIdentity(ref, data[nID][j], overlap=0.7):
                                Lines[0] += data[nID][j][0]
                                Lines[1] += data[nID][j][1]
                                Lines[2] += 1.0
                                # clear identical line
                                data[nID][j] = [-1, -1, 1]
                        if Lines[2] > 0:
                            tmp.append([nID, [Lines[0]/Lines[2], Lines[1]/Lines[2], ref[2]]])
                    if len(tmp) / NSP >= threshold:
                        for nID, ref in tmp:
                            ret[nID].append(ref)
        for ID in list(data.keys()):
            if len(ret[ID]) == 0:
                ret[ID].append([-1, -1, 1])

        return ret

    def CheckLineIdentity(self, old: List[float], new: List[float], overlap: float = 0.7) -> bool:
        """Check if the given set of line ranges overlap.

        True if the overlap of two lines is greater than the threshold.

        1L          1R          1L         1R          1L        1R       1L       1R
         [          ]           [          ]            [         ]       [         ]
         xxxxxxxxxxxx           xxxxxxxxxxxx            xxxxxxxxxxx       xxxxxxxxxxx
        oooooooooooooo           oooooooooooo          ooooooooooo         ooooooooo
        [            ]           [          ]          [         ]         [       ]
        2L          2R          2L         2R          2L        2R       2L       2R

        True if Num(x) / Num(o) >= overlap

        Args:
            old: Reference line range. [left, right, binning]
            new: Comparing line range. [left, right, binning]
            overlap: Threshold for overlap. Ranges between 0 and 1. Defaults to 0.7.

        Returns:
            Whether or not two lines overlap.
        """
        if(old[0] <= new[0] < old[1] or \
           old[0] < new[1] <= old[1] or \
           new[0] <= old[0] < new[1] or \
           new[0] < old[1] <= new[1]) and \
           (min(old[1], new[1]) - max(old[0], new[0]) + 1) / float(max((old[1] - old[0] + 1), (new[1] - new[0] + 1))) >= overlap:
            #print 'Identical', old, new
            return True
        else:
            return False

    def clustering_kmean(self, Region: DetectedLineList, Region2: numpy.ndarray) -> ClusteringResult:
        """Perform k-mean clustering analysis on detected lines.

        Perform k-mean clustering analysis on detected lines with various
        pre-defined number of clusters. Best number of clusters are
        determined by the scoring scheme based on the distance between
        origin of the cluster and the data regarded as a member of the
        cluster.

        Args:
            Region: List of line properties with associated spatial coordinate.
                    Format is as follows.

                [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]

            Region2: List of line properties (line width and line center)
                     for each data. Format is as follows.

                [[Width, Center],[],[],,,[]]

        Returns:
            4-tuple representing clustering results, number of clusters,
            list of cluster properties (Center, Width/WHITEN, T/F, ClusterRadius),
            List of category indices indicating which lines belong to what
            cluster, and list of line properties with associated spatial
            coordinate (which is same format as Region).
        """
        # Region = [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]
        # Region2 = [[Width, Center],[],[],,,[]]
        MedianWidth = numpy.median(Region2[:, 0])
        LOG.trace('MedianWidth = %s', MedianWidth)

        MaxCluster = int(rules.ClusterRule['MaxCluster'])
        LOG.info('Maximum number of clusters (MaxCluster) = %s', MaxCluster)

        # Determin the optimum number of clusters
        BestScore = -1.0
        # 2010/6/15 Plot the score along the number of the clusters
        ListNcluster = []
        ListScore = []
        ListBestScore = []
        converged = False
        #
        elapsed = 0.0
        self.DebugOutVer[1] += 1
        for Ncluster in range(1, MaxCluster + 1):
            index0=len(ListScore)
            # Fix the random seed 2008/5/23
            numpy.random.seed((1234, 567))
            # Try multiple times to supress random selection effect 2007/09/04
            for Multi in range(min(Ncluster+1, 10)):
                codebook, diff = VQ.kmeans(Region2, Ncluster, iter=50)
                # codebook = [[clstCentX, clstCentY],[clstCentX,clstCentY],,[]] len=Ncluster
                # diff <= distortion
                NclusterNew = 0
                LOG.trace('codebook=%s', codebook)
                # Do iteration until no merging of clusters to be found
                while(NclusterNew != len(codebook)):
                    category, distance = VQ.vq(Region2, codebook)
                    # category = [c0, c0, c1, c2, c0,,,] c0,c1,c2,,, are clusters which each element belongs to
                    # category starts with 0
                    # distance = [d1, d2, d3,,,,,] distance from belonging cluster center
                    LOG.trace('Cluster Category&Distance %s, distance = %s', category, distance)

                    # remove empty line in codebook
                    codebook = codebook.take([x for x in range(0, len(codebook)) if any(category==x)], axis=0)
                    NclusterNew = len(codebook)

                    # Clear Flag
                    for i in range(len(Region)):
                        Region[i][5] = 1

                    Outlier = 0.0
                    lines = []
                    for Nc in range(NclusterNew):
                        ### 2011/05/17 Strict the threshold, clean-up each cluster by nsigma clipping/flagging
                        ValueList = distance[(category == Nc).nonzero()[0]]
                        Stddev = ValueList.std()
                        Threshold = ValueList.mean() + Stddev * self.nsigma
                        del ValueList
                        LOG.trace('Cluster Clipping Threshold = %s, Stddev = %s', Threshold, Stddev)
                        for i in ((distance * (category == Nc)) > Threshold).nonzero()[0]:
                            Region[i][5] = 0 # set flag to 0
                            Outlier += 1.0
                        # Calculate Cluster Characteristics
                        MaxDistance = max(distance * ((distance < Threshold) * (category == Nc)))
                        indices = [x for x in range(len(category)) if category[x] == Nc and Region[x][5] != 0]
                        properties = Region2.take(indices, axis=0)
                        median_props = numpy.median(properties, axis=0)
                        lines.append([median_props[1], median_props[0], True, MaxDistance])
                    MemberRate = (len(Region) - Outlier)/float(len(Region))
                    MeanDistance = (distance * numpy.transpose(numpy.array(Region))[5]).mean()
                    LOG.trace('lines = %s, MemberRate = %s', lines, MemberRate)

                    # 2010/6/15 Plot the score along the number of the clusters
                    ListNcluster.append(Ncluster)
                    Score = self.clustering_kmean_score(MeanDistance, MedianWidth, NclusterNew, MemberRate)
                    ListScore.append(Score)
                    LOG.debug('NclusterNew = %s, Score = %s', NclusterNew, Score)

                    ### 2017/07/06 save Score to file for test
                    if infrastructure.logging.logging_level == infrastructure.logging.LOGGING_LEVELS['trace'] or \
                       infrastructure.logging.logging_level == infrastructure.logging.LOGGING_LEVELS['debug']:
                        with open('ClstProp.%s.%02d.txt' % (self.DebugOutName, self.DebugOutVer[1]), "wa") as fp:
                            fp.writelines('%d,%f,%f,%f,%f,%s\n' % (NclusterNew, Score, MeanDistance, MedianWidth,
                                                                   MemberRate, lines))

                    if BestScore < 0 or Score < BestScore:
                        BestNcluster = NclusterNew
                        BestScore = Score
                        BestCategory = category.copy()
                        BestCodebook = codebook.copy()
                        BestRegion = Region[:]
                        Bestlines = lines[:]

            ListBestScore.append(min(ListScore[index0:]))
            LOG.debug('Ncluster = %s, BestScore = %s', NclusterNew, ListBestScore[-1])
            # iteration end if Score(N) < Score(N+1),Score(N+2),Score(N+3)
            #if len(ListBestScore) > 3 and \
            #   ListBestScore[-4] <= ListBestScore[-3] and \
            #   ListBestScore[-4] <= ListBestScore[-2] and \
            #   ListBestScore[-4] <= ListBestScore[-1]:
            if len(ListBestScore) >= 10:
                LOG.info('Determined the Number of Clusters to be %s', BestNcluster)
                converged = True
                break

        if converged is False:
            LOG.warning('Clustering analysis not converged. Number of clusters may be greater than upper limit'
                        ' (MaxCluster=%s)', MaxCluster)

        cluster_info = {
            'algorithm': 'kmean',
            'cluster_score': [ListNcluster, ListScore],
            'detected_lines': Region2,
            'cluster_property': Bestlines,  # [[Center, Width/WHITEN, T/F, ClusterRadius],[],,,[]]
            'cluster_scale': self.CLUSTER_WHITEN
        }
        self._merge_cluster_info(**cluster_info)
        #SDP.ShowClusterScore(ListNcluster, ListScore, ShowPlot, FigFileDir, FigFileRoot)
        #SDP.ShowClusterInchannelSpace(Region2, Bestlines, self.CLUSTER_WHITEN, ShowPlot, FigFileDir, FigFileRoot)
        LOG.info('Final: Ncluster = %s, Score = %s, lines = %s', BestNcluster, BestScore, Bestlines)
        LOG.debug('Category = %s, CodeBook = %s', category, BestCodebook)

        return (BestNcluster, Bestlines, BestCategory, BestRegion)

    def clustering_hierarchy(
        self,
        Region: DetectedLineList,
        Region2: numpy.ndarray,
        nThreshold: float = 3.0,
        nThreshold2: float = 4.5,
        method: str = 'single'
    ) -> ClusteringResult:
        """Perform hierarchical clustering analysis on detected lines.

        Perform hierarchical clustering analysis that is a "bottom-up"
        approach to configure the clusters that best represents the
        distribution of the detected line properties. It starts with
        the small clusters and combine them until certain condition
        is met.

        method = 'ward'    : Ward's linkage method
                 'single'  : nearest point linkage method
                 'complete': farthest point linkage method
                 'average' : average linkage method
                 'centroid': centroid/UPGMC linkage method
                 'median'  : median/WPGMC linkage method
        1st threshold is set to nThreshold x stddev(distance matrix)
        2nd threshold is set to nThreshold2 x stddev of sub-cluster distance matrix
        in:
            self.Data -> Region2
            self.Ndata
        out:
            self.Threshold
            self.Nthreshold
            self.Category
            self.Ncluster

        Args:
            Region: List of line properties with associated spatial coordinate.
                    Format is as follows.

                [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]

            Region2: List of line properties (line width and line center)
                     for each data. Format is as follows.

                [[Width, Center],[],[],,,[]]
            nThreshold: Threshold factor for the hierarchical clustering analysis.
                        It is used as a multiplicative factor for stddev of
                        initial distance matrix.
            nThreshold2: Another threshold factor for the hierarchical clustering analysis.
                        It is used as a multiplicative factor for stddev of
                        sub-cluster distance matrix.
            method: Method name for linkage method of the hierarchical clustering analysis.

        Returns:
            4-tuple representing clustering results, number of clusters,
            list of cluster properties (Center, Width/WHITEN, T/F, ClusterRadius),
            List of category indices indicating which lines belong to what
            cluster, and list of line properties with associated spatial
            coordinate (which is same format as Region).
        """
        Data = self.set_data(Region2, ordering=[0, 1])  # Data: numpy[[width, center],[w,c],,,]
        Repeat = 3  # Number of artificial detection points to normalize the cluster distance
        # Calculate LinkMatrix from given data set
        if method.lower() == 'single': # nearest point linkage method
            H_Clustering = HIERARCHY.single
        elif method.lower() == 'complete': # farthest point linkage method
            H_Clustering = HIERARCHY.complete
        elif method.lower() == 'average': # average linkage method
            H_Clustering = HIERARCHY.average
        elif method.lower() == 'centroid': # centroid/UPGMC linkage method
            H_Clustering = HIERARCHY.centroid
        elif method.lower() == 'median': # median/WPGMC linkage method
            H_Clustering = HIERARCHY.median
        else: # Ward's linkage method: default
            H_Clustering = HIERARCHY.ward
        # temporaly add artificial detection points to normalize the cluster distance
        tmp = numpy.zeros((Data.shape[0]+Repeat*2, Data.shape[1]), float)
        tmp[Repeat*2:] = Data.copy()
        for i in range(Repeat):
            tmp[i] = [self.nchan//2, 0]
            tmp[Repeat+i] = [self.nchan//2, self.nchan-1]
        #LOG.debug('tmp[:10] = {}', tmp[:10])
        tmpLinkMatrix = H_Clustering(tmp)
        MedianDistance = numpy.median(tmpLinkMatrix.T[2])
        MeanDistance = tmpLinkMatrix.T[2].mean()
        Stddev = tmpLinkMatrix.T[2].std()
        del tmp, tmpLinkMatrix
        LOG.debug('MedianDistance = %s, MeanDistance = %s, Stddev = %s', MedianDistance, MeanDistance, Stddev)
        LOG.debug('Ndata = %s', Data.shape[0])

        # Divide data set into several clusters
        # LinkMatrix[n][2]: distance between two data/clusters
        # 1st classification
        LinkMatrix = H_Clustering(Data)
        #MedianDistance = numpy.median(LinkMatrix.T[2])
        #Stddev = LinkMatrix.T[2].std()
        Nthreshold = nThreshold
        #Threshold = MedianDistance + Nthreshold * Stddev
        Threshold = MeanDistance + Nthreshold * Stddev
        Category = HIERARCHY.fcluster(LinkMatrix, Threshold, criterion='distance')
        Ncluster = Category.max()
        LOG.debug('nThreshold = %s, nThreshold2 = %s, method = %s', nThreshold, nThreshold2, method)
        LOG.debug('Init Threshold = %s, Init Ncluster = %s', Threshold, Ncluster)
        print('Init Threshold: {}'.format(Threshold), end=' ')
        print('\tInit Ncluster: {}'.format(Ncluster))

        IDX = numpy.array([x for x in range(len(Data))])
        for k in range(Ncluster):
            C = Category.max()
            NewData = Data[Category==(k+1)] # Category starts with 1 (not 0)
            if(len(NewData) < 2):
                print('skip(%d): %d' % (k, len(NewData)))
                continue # LinkMatrix = ()
            NewIDX = IDX[Category==(k+1)]
            LinkMatrix = H_Clustering(NewData) # selected linkage method
            #print LinkMatrix
            MedianDistance = numpy.median(LinkMatrix.T[2])
            MeanDistance = LinkMatrix.T[2].mean()
            #print 'MedianD', MedianDistance
            Stddev = LinkMatrix.T[2].std()
            LOG.debug('MedianDistance = %s, MeanDistance = %s, Stddev = %s', MedianDistance, MeanDistance, Stddev)
            NewThreshold = MeanDistance + nThreshold2 * Stddev
            LOG.debug('Threshold(%s): %s', k, NewThreshold)
            print('Threshold(%d): %.1f' % (k, NewThreshold), end=' ')
            NewCategory = HIERARCHY.fcluster(LinkMatrix, NewThreshold, criterion='distance')
            NewNcluster = NewCategory.max()
            LOG.debug('NewCluster(%s): %s', k, NewNcluster)
            print('\tNewNcluster(%d): %d' % (k, NewNcluster), end=' ')
            print('\t# of Members(%d): %d: ' % (k, ((Category == k+1)*1).sum()), end=' ')
            for kk in range(NewNcluster):
                print(((NewCategory == kk+1)*1).sum(), end=' ')
            print('')
            if NewNcluster > 1:
                for i in range(len(NewData)):
                    if NewCategory[i] > 1:
                        Category[NewIDX[i]] = C + NewCategory[i] - 1
        Ncluster = Category.max() # update Ncluster

        (Region, Range, Stdev, Category) = self.clean_cluster(Data, Category, Region, nThreshold2, 2) # nThreshold, NumParam
        # 2017/7/25 ReNumbering is done in clean_cluster
        #for i in range(len(Category)):
        #    #if Category[i] > Ncluster: Region[i][5] = 0 # flag out cleaned data
        #    Category[i] -= 1 # Category starts with 1 -> starts with 0 (to align kmean)
        Bestlines = []
        Ncluster = len(Range)
        for j in range(Ncluster):
            Bestlines.append([Range[j][1], Range[j][0], True, Range[j][4]])
        LOG.info('Final: Ncluster = %s, lines = %s', Ncluster, Bestlines)

        cluster_info = {
            'algorithm': 'hierarchy',
            'cluster_score': [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]],
            'detected_lines': Region2,
            'cluster_property': Bestlines,  # [[Center, Width, T/F, ClusterRadius],[],,,[]]
            'cluster_scale': self.CLUSTER_WHITEN
        }
        self._merge_cluster_info(**cluster_info)

        return (Ncluster, Bestlines, Category, Region)

    def set_data(self, Observation: numpy.ndarray, ordering: Union[str,List[int]] = 'none') -> numpy.ndarray:
        """Transpose axes of two-dimensional array data.

        Observation: numpy.array([[val1, val2, val3,..,valN],
                                  [val1, val2, val3,..,valN],
                                   ........................
                                  [val1, val2, val3,..,valN]], float)
        where N is a max dimensions of parameter space
            ordering: 'none' or list of ordering of columns
              e.g., ordering=[2,3,1,0] => [col3,col2,col0,col1]

        self.Data: Observation data
        self.NumParam: Number of dimensions to be used for Clustering Analysis
        self.Factor: Set default Whitening factor (to be 1.0)

        Args:
            Observation: Two-dimensional array data
            ordering: Axis order for output array

        Raises:
            ValueError: Given array is not two-dimensional

        Returns:
            Transposed array
        """
        if ordering != 'none':
            NumParam = len(ordering)
            OrderList = ordering
        else:
            NumParam = len(Observation[0])
            OrderList = list(range(NumParam))
        if isinstance(Observation, list):
            Obs = numpy.array(Observation, float)
        else:
            Obs = Observation.copy()
        if len(Obs.shape) == 2:
            Data = numpy.zeros((Obs.shape[0], NumParam), float)
            for i in range(Obs.shape[0]):
                for j in range(NumParam):
                    Data[i][j] = Obs[i][OrderList[j]]
            Factor = numpy.ones(NumParam, float)
            Ndata = len(Data)
        else:
            LOG.error("Data should be 2-dimensional. {}-dimensional data was given".format(len(Obs.shape)))
            raise ValueError('Data should be 2-dimensional!')
        del Obs, OrderList
        return Data

    def clean_cluster(self,
                      Data: numpy.ndarray,
                      Category: List[int],
                      Region: DetectedLineList,
                      Nthreshold: float,
                      NumParam: int
    ) -> Tuple[List[Union[int, float, bool]], numpy.ndarray, numpy.ndarray, List[int]]:
        """Clean-up cluster by eliminating outliers.

         Radius = StandardDeviation * nThreshold (circle/sphere)

        Args:
            Data: List of cluster properties with associated spatial coordinate.
            Category: Input category list representing membership information
            Region: List of line properties with associated spatial coordinate.
                    Format is as follows.

                [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]

            Nthreshold: Threshold factor for detecting outlier
            NumParam: Number of cluster properties

        Returns:
            4-tuple of the following values.
                Region: flag information is added
                Range: Range[Ncluster][5]: [ClusterCenterX, ClusterCenterY, 0, 0, Threshold]
                Stdev: Stdev[Ncluster][5]: [ClusterStddevX, ClusterStddevY, 0, 0, 0]
                Category: renumbered category
        """
        IDX = numpy.array([x for x in range(len(Data))])
        Ncluster = Category.max()
        C = Ncluster + 1
        ValidClusterID = []
        ValidRange = []
        ValidStdev = []
        ReNumber = {}
        Range = numpy.zeros((C, 5), float)
        Stdev = numpy.zeros((C, 5), float)
        for k in range(Ncluster):
            NewData = Data[Category == k+1].T
            NewIDX = IDX[Category == k+1]
            for i in range(NumParam):
                Range[k][i] = NewData[i].mean()
                Stdev[k][i] = NewData[i].std()
            if(NumParam == 4):
                Tmp = ((NewData - numpy.array([[Range[k][0]], [Range[k][1]], [Range[k][2]], [Range[k][3]]]))**2).sum(axis=0)**0.5
            elif(NumParam == 3):
                Tmp = ((NewData - numpy.array([[Range[k][0]], [Range[k][1]], [Range[k][2]]]))**2).sum(axis=0)**0.5
            else: # NumParam == 2
                Tmp = ((NewData - numpy.array([[Range[k][0]], [Range[k][1]]]))**2).sum(axis=0)**0.5
            Threshold = numpy.median(Tmp) + Tmp.std() * Nthreshold
            #Threshold = Tmp.mean() + Tmp.std() * Nthreshold
            Range[k][4] = Threshold
            LOG.trace('Threshold(%s) = %s', k, Threshold)
            Out = NewIDX[Tmp > Threshold]
            #if (len(NewIDX) - len(Out)) < 6: # max 3 detections for each binning: detected in two binning pattern
            if (len(NewIDX) - len(Out)) < 3: # max 3 detections for each binning: detected in at least one binning pattern: sensitive to very narrow lines
                LOG.trace('Non Cluster: %s', len(NewIDX))
                for i in NewIDX:
                    #self.Category[i] = C
                    Region[i][5] = 0
                ReNumber[k+1] = 0
                continue
            LOG.trace('Out Of Cluster (%s): %s', k, len(Out))
            if len(Out > 0):
                for i in Out:
                    #Category[i] = C
                    Region[i][5] = 0
            ReNumber[k+1] = len(ValidClusterID)
            ValidClusterID.append(k)
        for k in ValidClusterID:
            ValidRange.append(Range[k])
            ValidStdev.append(Stdev[k])
        LOG.debug('ReNumbering Table: %s', ReNumber)
        for j in range(len(Category)): Category[j] = ReNumber[Category[j]]
        #return (Region, Range, Stdev)
        return (Region, numpy.array(ValidRange), numpy.array(ValidStdev), Category)

    def clustering_kmean_score(self, MeanDistance: float, MedianWidth: float, Ncluster: int, MemberRate: float) -> float:
        """Compute score of the clusters.

        Args:
            MeanDistance: Mean distance from the center of the cluster
            MedianWidth: Median value of the line width
            Ncluster: Number of clusters
            MemberRate: Fraction of the members that belong to any cluster

        Returns:
            Score of the cluster
        """
        # Rating
        ### 2011/05/12 modified for (distance==0)
        ### 2014/11/28 further modified for (distance==0)
        ### 2017/07/05 modified to be sensitive to MemberRate
        # (distance * numpy.transpose(Region[5])).mean(): average distance from each cluster center
        return(math.sqrt(MeanDistance**2.0 + (MedianWidth/2.0)**2.0) * (Ncluster+ 1.0/Ncluster) * ((1.0 - MemberRate) * 100.0 + 1.0))

    def detection_stage(
        self,
        Ncluster: int,
        nra: int, ndec: int,
        ra0: float, dec0: float,
        grid_ra: float, grid_dec: float,
        category: List[int],
        Region: DetectedLineList,
        detect_signal: dict
    ) -> Tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]:
        """Classify cluster members by their location on celestial coordinate.

        This method implements the first phase of cluster validation process,
        and is so-called "Detection Stage". It combines clustering analysis
        result with spatial information of each detected line, and classify
        cluster members by their location on celestial coordinate.

        Args:
            Ncluster: Number of clusters
            nra: Number of horizontal grids on the sky
            ndec: Number of vertical grids on the sky
            grid_ra: Physical size of the horizontal grid in degree
            grid_dec: Physical size of the vertical grid in degree
            category: List of cluster membership indices
            Region: List of line properties with associated spatial coordinate.
                    Format is as follows.

                [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]

            detect_signal: List of detected lines per spatial position. Its format is
                           as follows.

                detect_signal = {
                    ID1: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    [LineStartChannel2, LineEndChannel2],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]],
                    IDn: [RA, DEC, [[LineStartChannel1, LineEndChannel1],
                                    ...,
                                    [LineStartChannelN, LineEndChannelN]]]
                }

        Returns:
            3-tuple of the following numpy arrays.

                - Three dimensional array representing spatial distribution of
                  each cluster detected by the clustering analysis.
                - Number of spectra at each grid position. Value of the array ranges
                  from 0 to 3 (either of 0, 1, 2, 3) but usually it is 3.
                - array data for plotting clustering analysis results for detection stage
        """
        # Create Grid Parameter Space (Ncluster * nra * ndec)
        MinChanBinSp = 50.0
        BinningVariation = 1 + int(math.ceil(math.log(self.nchan/MinChanBinSp)/math.log(4)))
        GridClusterWithBinning = numpy.zeros((Ncluster, BinningVariation, nra, ndec), dtype=numpy.float32)
        #GridCluster = numpy.zeros((Ncluster, nra, ndec), dtype=numpy.float32)
        GridMember = numpy.zeros((nra, ndec))

        # Set the number of spectra belong to each gridding positions
        for row in range(len(detect_signal)):
            GridMember[int((detect_signal[row][0] - ra0)/grid_ra)][int((detect_signal[row][1] - dec0)/grid_dec)] += 1

        for i in range(len(category)):
            if Region[i][5] == 1: # valid spectrum
                # binning = 4**n
                n = int(math.log(Region[i][6])/math.log(4.) + 0.1)
                # if binning larger than 1, detection is done twice: m=>0.5
                if n == 0: m = 1.0 # case binning=1
                else: m = 0.5
                try:
                    #2014/11/28 Counting is done for each binning separately
                    GridClusterWithBinning[category[i]][n][int((Region[i][3] - ra0)/grid_ra)][int((Region[i][4] - dec0)/grid_dec)] += m
                    #GridCluster[category[i]][int((Region[i][3] - ra0)/grid_ra)][int((Region[i][4] - dec0)/grid_dec)] += 1.0
                except IndexError:
                    pass
        GridCluster = GridClusterWithBinning.max(axis=1)
        LOG.trace('GridClusterWithBinning = %s', GridClusterWithBinning)
        LOG.trace('GridCluster = %s', GridCluster)
        LOG.trace('GridMember = %s', GridMember)
        del GridClusterWithBinning
        # 2013/05/29 TN
        # cluster_flag is data for plotting clustering analysis results.
        # It stores GridCluster quantized by given thresholds.
        # it is defined as integer array and one digit is assigned to
        # one clustering stage in each integer value:
        #
        #     1st digit: detection
        #     2nd digit: validation
        #     3rd digit: smoothing
        #     4th digit: final
        #
        # If GridCluster value exceeds any threshold, corresponding
        # digit is incremented. For example, flag 3210 stands for,
        #
        #     value didn't exceed any thresholds in detection, and
        #     exceeded one (out of three) threshold in validation, and
        #     exceeded two (out of three) thresholds in smoothing, and
        #     exceeded three (out of four) thresholds in final.
        #
        #self.cluster_info['cluster_flag'] = numpy.zeros(GridCluster.shape, dtype=numpy.uint16)
        threshold = [1.5, 0.5]
        cluster_flag = numpy.zeros(GridCluster.shape, dtype=numpy.uint16)
        flag_digit = self.flag_digits['detection']
        cluster_flag = self.__update_cluster_flag(cluster_flag, 'detection', GridCluster, threshold, flag_digit)

        return (GridCluster, GridMember, cluster_flag)

    def validation_stage(
        self,
        GridCluster: numpy.ndarray,
        GridMember: numpy.ndarray,
        lines: List[List[Union[float, bool]]],
        cluster_flag: numpy.ndarray
    ) -> Tuple[numpy.ndarray, numpy.ndarray, List[List[Union[float, bool]]], numpy.ndarray]:
        """Validate clusters by their detection fraction on each grid.

        This method implements the second phase of cluster validation process,
        and is so-called "Validation Stage". It sets validation flag based on
        the detection fraction on each grid.

        Cluster is validated if number of spectrum which contains feature belongs
        to the cluster is greater or equal to the half number of spectrum in the Grid.
        Normally, 3 spectra are created for each grid positions, therefore,
        GridMember[ra][dec] = 3 for most of the cases.

        Normalized validity can be

            - 1/3 (0.2<V) -> only one detection -> Qestionable
            - 2/3 (0.5<V)-> two detections -> Marginal
            - 3/3 (0.7<V)-> detected for all spectra -> Valid

        Questionable lines are flagged.

        Args:
            GridCluster: Three dimensional array representing spatial distribution of
                         each cluster detected by the clustering analysis
            GridMember: Number of spectra at each grid position
            lines: List of line properties with validity flag
            cluster_flag: array data for plotting clustering analysis results

        Returns:
            4-tuple of the following arrays.

                - Updated GridCluster normalized by GridMember
                - Number of lines detected at each grid position (return GridMember as is)
                - List of line properties with updated validity flag (Questionable lines are
                  flagged out)
                - array data for plotting clustering analysis results for validation stage
        """
        # Validated if number of spectrum which contains feature belongs to the cluster is greater or equal to
        # the half number of spectrum in the Grid
        # Normally, 3 spectra are created for each grid positions,
        # therefore, gridmember[ra][dec] = 3 for most of the cases.
        # Normalized validity can be
        # 1/3 (0.2<V) -> only one detection -> Qestionable
        # 2/3 (0.5<V)-> two detections -> Marginal
        # 3/3 (0.7<V)-> detected for all spectra -> Valid
        # ThresholdValid should be 0.5 -> 0.7 in the future

        (Ncluster, nra, ndec) = GridCluster.shape
        MinChanBinSp = 50.0
        BinningVariation = 1 + int(math.ceil(math.log(self.nchan/MinChanBinSp)/math.log(4)))

        for Nc in range(Ncluster):
            LOG.trace('GridCluster[Nc]: %s', GridCluster[Nc])
            LOG.trace('Gridmember: %s', GridMember)
            for x in range(nra):
                for y in range(ndec):
                    if GridMember[x][y] == 0: GridCluster[Nc][x][y] = 0.0
                    elif GridMember[x][y] == 1 and GridCluster[Nc][x][y] > 0.9:
                        GridCluster[Nc][x][y] = 1.0
                    ### 2014/11/28 Binning valiation is taken into account in the previous stage
                    # normarize validity
                    #else: GridCluster[Nc][x][y] /= float(GridMember[x][y])
                    else: GridCluster[Nc][x][y] = min(GridCluster[Nc][x][y] / float(GridMember[x][y]), 1.0)

            if ((GridCluster[Nc] > self.Questionable)*1).sum() == 0: lines[Nc][2] = False

        threshold = [self.Valid, self.Marginal, self.Questionable]
        flag_digit = self.flag_digits['validation']
        cluster_flag = self.__update_cluster_flag(cluster_flag, 'validation', GridCluster, threshold, flag_digit)
        LOG.trace('GridCluster %s', GridCluster)
        LOG.trace('GridMember %s', GridMember)
        self.GridClusterValidation = GridCluster.copy()

        return (GridCluster, GridMember, lines, cluster_flag)

    def smoothing_stage(
        self,
        GridCluster: numpy.ndarray,
        lines: List[List[Union[float, bool]]],
        cluster_flag: numpy.ndarray
    ) -> Tuple[numpy.ndarray, List[List[Union[float, bool]]], numpy.ndarray]:
        """Smooth cluster distribution.

        This method implements the third phase of cluster validation process,
        and is so-called "Smoothing Stage". It applies smoothing to spatial
        cluster distribution and update validity flag of detected lines
        according to the smoothed cluster distribution.

        Smoothing kernel is given by,

            6.0 if Dx=0 and Dy=0,
            1.0 / (Dx**2 + Dy**2) if abs(Dx) + abs(Dy) < 4, and
            0 otherwise

        where Dx and Dy is distance from the center pixel. It looks like below.

            [ 0.0,  0.2, 0.25,  0.2,  0.0]
            [ 0.2,  0.5,  0.1,  0.5,  0.2]
            [0.25,  0.1,  6.0,  0.1, 0.25]
            [ 0.2,  0.5,  0.1,  0.5,  0.2]
            [ 0.0,  0.2, 0.25,  0.2,  0.0]

        Lines with too low detection value is flagged.

        Args:
            GridCluster: Three dimensional array representing spatial distribution of
                         each cluster detected by the clustering analysis
            lines: List of line properties with validity flag
            cluster_flag: array data for plotting clustering analysis results

        Returns:
            3-tuple of updated cluster information.

              - Smoothed GridCluster
              - List of line properties with updated validity flag
              - Updated cluster_flag (flags for smoothing stage is appended)
        """
        # Rating:  [0.0, 0.4, 0.5, 0.4, 0.0]
        #          [0.4, 0.7, 1.0, 0.7, 0.4]
        #          [0.5, 1.0, 6.0, 1.0, 0.5]
        #          [0.4, 0.7, 1.0, 0.7, 0.4]
        #          [0.0, 0.4, 0.5, 0.4, 0.0]
        # Rating = 1.0 / (Dx**2 + Dy**2)**(0.5) : if (Dx, Dy) == (0, 0) rating = 6.0
        # NewRating[0.0, 0.2, 0.3, 0.2, 0.0]
        #          [0.2, 0.5, 1.0, 0.5, 0.2]
        #          [0.3, 1.0, 6.0, 1.0, 0.3]
        #          [0.2, 0.5, 1.0, 0.5, 0.2]
        #          [0.0, 0.2, 0.3, 0.2, 0.0]
        # NewRating = 1.0 / (Dx**2 + Dy**2) : if (Dx, Dy) == (0, 0) rating = 6.0
        (Ncluster, nra, ndec) = GridCluster.shape
        GridScore = numpy.zeros((2, nra, ndec), dtype=numpy.float32)
        LOG.trace('GridCluster = %s', GridCluster)
        LOG.trace('lines = %s', lines)
        for Nc in range(Ncluster):
            if lines[Nc][2] != False:
                GridScore[:] = 0.0
                for x in range(nra):
                    range_x = list(range(-min(2, x), 0)) + list(range(1, min(3, nra-x)))
                    for y in range(ndec):
                        range_y = list(range(-min(2, y), 0)) + list(range(1, min(3, ndec-y)))
                        # TN refactoring
                        # split smoothing loop
                        # dx = 0 and dy = 0
                        GridScore[0][x][y] += 6.0 * GridCluster[Nc][x][y]
                        GridScore[1][x][y] += 6.0
                        # dx = 0
                        for dy in range_y:
                            ny = y + dy
                            #Rating = 1.0 / abs(dy)
                            Rating = 1.0 / abs(dy*dy)
                            GridScore[0][x][y] += Rating * GridCluster[Nc][x][ny]
                            GridScore[1][x][y] += Rating
                        # dy = 0
                        for dx in range_x:
                            nx = x + dx
                            #Rating = 1.0 / abs(dx)
                            Rating = 1.0 / abs(dx*dx)
                            GridScore[0][x][y] += Rating * GridCluster[Nc][nx][y]
                            GridScore[1][x][y] += Rating
                        # dx != 0 and dy != 0
                        for dx in range_x:
                            for dy in range_y:
                                if (abs(dx) + abs(dy)) <= 3:
                                    (nx, ny) = (x + dx, y + dy)
                                    #Rating = 1.0 / sqrt(dx*dx + dy*dy)
                                    Rating = 1.0 / (dx*dx + dy*dy)
                                    GridScore[0][x][y] += Rating * GridCluster[Nc][nx][ny]
                                    GridScore[1][x][y] += Rating
                LOG.trace('Score :  GridScore[%s][0] = %s', Nc, GridScore[0])
                LOG.trace('Rating:  GridScore[%s][1] = %s', Nc, GridScore[1])
                #GridCluster[Nc] = GridScore[0] / GridScore[1]
                GridCluster[Nc] = GridScore[0] / GridScore[1] * 2.0 # for single valid detection
            if ((GridCluster[Nc] > self.Questionable)*1).sum() < 0.1: lines[Nc][2] = False

        threshold = [self.Valid, self.Marginal, self.Questionable]
        flag_digit = self.flag_digits['smoothing']
        cluster_flag = self.__update_cluster_flag(cluster_flag, 'smoothing', GridCluster, threshold, flag_digit)
        LOG.trace('threshold = %s', threshold)
        LOG.trace('GridCluster = %s', GridCluster)

        return (GridCluster, lines, cluster_flag)

    def final_stage(
        self,
        GridCluster: numpy.ndarray,
        GridMember: numpy.ndarray,
        Region: DetectedLineList,
        Region2: numpy.ndarray,
        lines: List[List[Union[int, bool]]],
        category: List[int],
        grid_ra: float,
        grid_dec: float,
        broad_component: bool,
        xorder: int,
        yorder: int,
        x0: float,
        y0: float,
        Grid2SpectrumID: List[List[int]],
        index_list: List[int],
        PosList: numpy.ndarray,
        cluster_flag: numpy.ndarray
    ) -> Tuple[collections.OrderedDict, List[List[Union[int, bool]]], List[List[Union[int, bool]]], numpy.ndarray]:
        """Distribute validated lines to each observed spectra.

        This method implements the final phase of cluster validation process,
        and is so-called "Final Stage". It performs two-dimensional least-square
        fitting of line properties (center, width) on the grid configured onto
        the celestial plane, and distribute those properties to each observed
        spectra using least-square solution at their associated position.

        Args:
            GridCluster: Three dimensional array representing spatial distribution of
                         each cluster detected by the clustering analysis
            GridMember: Number of spectra at each grid position
            Region: List of line properties with associated spatial coordinate.
                    Format is as follows.

                [[row, chan0, chan1, RA, DEC, flag, Binning],[],[],,,[]]

            Region2: List of line properties (line width and line center)
                     for each data
            lines: List of line properties with validity flag
            category: List of cluster membership indices
            grid_ra: Horizontal (longitudinal) spacing of spatial grids.
                     The value should be the one with declination correction.
            grid_dec: Vertical (latitudinal) spacing of spatial grids.
            broad_component: Process broad component or not. Not used.
            xorder: Order of the polynomial for horizontal fitting.
                    If it is -1, order is automatically determined inside
                    the method (max 5).
            yorder: Order of the polynomial for vertical fitting.
                    If it is -1, order is automatically determined inside
                    the method (max 5).
            x0: Horizontal position of the bottom left corner
            y0: Vertical position of the bottom left corner
            Grid2SpectrumID: Index mapping between serial list of gridded
                             spectra and two-dimensional grid positions
            index_list: List of consecutive datatable row numbers.
            PosList: List of pointings (RA and Dec) of ON_SOURCE data
            cluster_flag: array data for plotting clustering analysis results

        Returns:
            4-tuple of the following data.

              - List of validated line ranges distributed to each observed spectrum
                using the least-square fitting of line parameters
              - List of line properties with updated validity flag
              - List of line properties dedicated to plotting
              - Updated cluster_flag (flags for smoothing stage is appended)
        """
        (Ncluster, nra, ndec) = GridCluster.shape
        xorder0 = xorder
        yorder0 = yorder

        LOG.trace('GridCluster = %s', GridCluster)
        LOG.trace('GridMember = %s', GridMember)
        LOG.trace('lines = %s', lines)
        LOG.info('Ncluster=%s', Ncluster)

        # Dictionary for final output
        RealSignal = collections.OrderedDict()

        HalfGrid = 0.5 * sqrt(grid_ra*grid_ra + grid_dec*grid_dec)

        MinFWHM = self.MinFWHM
        MaxFWHM = self.MaxFWHM

        # for Channel Map velocity range determination 2014/1/12
        channelmap_range = []
        for i in range(len(lines)):
            channelmap_range.append(lines[i][:])

        for Nc in range(Ncluster):
            LOG.trace('------00------ Exec for Nth Cluster: Nc=%s', Nc)
            LOG.trace('lines[Nc] = %s', lines[Nc])
            if lines[Nc][2] == False: continue
            Plane = (GridCluster[Nc] > self.Marginal) * 1
            if Plane.sum() == 0:
                lines[Nc][2] = False
                channelmap_range[Nc][2] = False
                continue
            Original = GridCluster[Nc].copy()
            # Clear GridCluster Nc-th plane
            GridCluster[Nc] *= 0.0

            # Clean isolated grids
            MemberList, Realmember = self.CleanIsolation(nra, ndec, Original, Plane, GridMember)
            if len(MemberList) == 0: continue

            # Blur each SubCluster with the radius of sqrt(Nmember/Pi) * ratio
            ratio = rules.ClusterRule['BlurRatio']
            # Set-up SubCluster
            for Ns in range(len(MemberList)):  # Ns: SubCluster number
                LOG.trace('------01------ Ns=%s', Ns)
                SubPlane = numpy.zeros((nra, ndec), dtype=numpy.float32)
                for (x, y) in MemberList[Ns]: SubPlane[x][y] = Original[x][y]
                ValidPlane, BlurPlane = self.DoBlur(Realmember[Ns], nra, ndec, SubPlane, ratio)

                LOG.debug('GridCluster.shape = %s', list(GridCluster.shape))
                LOG.trace('Original %s', Original)
                LOG.debug('SubPlane.shape = %s', list(SubPlane.shape))
                LOG.trace('SubPlane %s', SubPlane)
                LOG.debug('BlurPlane.shape = %s', list(BlurPlane.shape))
                LOG.trace('BlurPlane %s', BlurPlane)
                LOG.trace('ValidPlane %s', ValidPlane)
                LOG.trace('GridClusterV %s', self.GridClusterValidation[Nc])

                # 2D fit for each Plane
                # Use the data for fit if GridCluster[Nc][x][y] > self.Valid
                # Not use for fit but apply the value at the border if GridCluster[Nc][x][y] > self.Marginal

                (ylen, xlen) = self.GridClusterValidation[Nc].shape
                # 2017/9/21 GridClusterValidation should not be used for order determination
                # 0 <= xorder0,yorder0 <= 5, swap xorder0 and yorder0
                if xorder < 0: xorder0 = max(min(numpy.sum(ValidPlane, axis=0).max()-1, 5), 0)
                if yorder < 0: yorder0 = max(min(numpy.sum(ValidPlane, axis=1).max()-1, 5), 0)
                LOG.trace('(X,Y)order, order0 = (%s, %s) (%s, %s)', xorder, yorder, xorder0, yorder0)

                # clear Flag
                for i in range(len(category)):
                    Region[i][5] = 1

                #while ExceptionLinAlg:
                FitData = []
                LOG.trace('------02-1---- category=%s, len(category)=%s, ClusterNumber(Nc)=%s, SubClusterNumber(Ns)=%s', category, len(category), Nc, Ns)
                #Region format:([row, line[0], line[1], RA, DEC, flag])
                dummy = [tuple(Region[i][:5]) for i in range(len(category))
                         if category[i] == Nc and Region[i][5] == 1 and
                         SubPlane[int((Region[i][3] - x0)/grid_ra)][int((Region[i][4] - y0)/grid_dec)] > self.Valid]
                LOG.trace('------02-2----- len(dummy)=%s, dummy=%s', len(dummy), dummy)
                if len(dummy) == 0: continue

                # same signal can be detected in a single row with multiple binning
                # in that case, take maximum width as a representative width (Lmax-Lmin)
                (Lrow, Lmin, Lmax, LRA, LDEC) = dummy[0]
                for i in range(1, len(dummy)):
                    if Lrow == dummy[i][0]:
                        Lmin, Lmax = (min(Lmin, dummy[i][1]), max(Lmax, dummy[i][2]))
                    else:
                        FitData.append([Lmin, Lmax, LRA, LDEC, 1])
                        (Lrow, Lmin, Lmax, LRA, LDEC) = dummy[i]
                FitData.append([Lmin, Lmax, LRA, LDEC, 1])
                del dummy
                # FitData format: [Chan0, Chan1, RA, DEC, flag]
                LOG.trace('FitData = %s', FitData)

                # TN refactoring
                # Comment out the following if statement since
                # 1) len(FitData) is always greater than 0.
                #    Lee the code just above start of iteration.
                # 2) SingularMatrix is always False in this
                #    loop. Exit the loop whenever SingularMatrix
                #    is set to False. See code below.
                #if len(FitData) == 0 or SingularMatrix: break

                SingularMatrix = False
                for iteration in range(3):  # iteration loop for 2D fit sigma flagging
                    LOG.trace('------03------ iteration=%s', iteration)
                    # effective components of FitData
                    effective = [i for i in range(len(FitData)) if FitData[i][4] == 1]

                    # assertion
                    assert len(effective) > 0

                    # prepare data for SVD fit
                    # 2017/9/26 Repeat solver.find_good_solution until not through exception by reducing xorder and yorder
                    SVDsolver = True
                    while(1):
                        LOG.trace('2D Fit Order: xorder0=%s yorder0=%s', xorder0, yorder0)
                        if(SVDsolver):
                            # Instantiate SVD solver
                            solver = SVDSolver2D(xorder0, yorder0)
                            # prepare data for SVD fit
                            xdata = numpy.array([FitData[i][2] for i in effective], dtype=numpy.float64)
                            ydata = numpy.array([FitData[i][3] for i in effective], dtype=numpy.float64)
                            lmindata = numpy.array([FitData[i][0] for i in effective], dtype=numpy.float64)
                            lmaxdata = numpy.array([FitData[i][1] for i in effective], dtype=numpy.float64)
                        else:
                            # 2017/9/28 old code is incerted for validation purpose
                            # make arrays for coefficient calculation
                            # Matrix    MM x A = B  ->  A = MM^-1 x B
                            M0 = numpy.zeros((xorder0 * 2 + 1) * (yorder0 * 2 + 1), dtype=numpy.float64)
                            M1 = numpy.zeros((xorder0 * 2 + 1) * (yorder0 * 2 + 1), dtype=numpy.float64)
                            B0 = numpy.zeros((xorder0 + 1) * (yorder0 + 1), dtype=numpy.float64)
                            B1 = numpy.zeros((xorder0 + 1) * (yorder0 + 1), dtype=numpy.float64)
                            MM0 = numpy.zeros([(xorder0 + 1) * (yorder0 + 1), (xorder0 + 1) * (yorder0 + 1)], dtype=numpy.float64)
                            MM1 = numpy.zeros([(xorder0 + 1) * (yorder0 + 1), (xorder0 + 1) * (yorder0 + 1)], dtype=numpy.float64)
                            for (Width, Center, x, y, flag) in FitData:
                                if flag == 1:
                                    for k in range(yorder0 * 2 + 1):
                                        for j in range(xorder0 * 2 + 1):
                                            M0[j + k * (xorder0 * 2 + 1)] += math.pow(x, j) * math.pow(y, k)
                                    for k in range(yorder0 + 1):
                                        for j in range(xorder0 + 1):
                                            B0[j + k * (xorder0 + 1)] += math.pow(x, j) * math.pow(y, k) * Center
                                    for k in range(yorder0 * 2 + 1):
                                        for j in range(xorder0 * 2 + 1):
                                            M1[j + k * (xorder0 * 2 + 1)] += math.pow(x, j) * math.pow(y, k)
                                    for k in range(yorder0 + 1):
                                        for j in range(xorder0 + 1):
                                            B1[j + k * (xorder0 + 1)] += math.pow(x, j) * math.pow(y, k) * Width
                            # make Matrix MM0,MM1 and calculate A0,A1
                            for K in range((xorder0 + 1) * (yorder0 + 1)):
                                k0 = K % (xorder0 + 1)
                                k1 = int(K // (xorder0 + 1))
                                for J in range((xorder0 + 1) * (yorder0 + 1)):
                                    j0 = J % (xorder0 + 1)
                                    j1 = int(J // (xorder0 + 1))
                                    MM0[J, K] = M0[j0 + k0 + (j1 + k1) * (xorder0 * 2 + 1)]
                                    MM1[J, K] = M1[j0 + k0 + (j1 + k1) * (xorder0 * 2 + 1)]
                            LOG.trace('OLD:MM0 = %s', MM0.tolist())
                            LOG.trace('OLD:B0 = %s', B0.tolist())

                        try:
                            if(SVDsolver):
                                solver.set_data_points(xdata, ydata)
                                A0 = solver.find_good_solution(lmaxdata)
                                A1 = solver.find_good_solution(lmindata)
                                LOG.trace('SVD: A0=%s', A0.tolist())
                                LOG.trace('SVD: A1=%s', A1.tolist())
                                break
                            else:
                                A0 = LA.solve(MM0, B0)
                                A1 = LA.solve(MM1, B1)
                                LOG.trace('OLD: A0=%s', A0.tolist())
                                LOG.trace('OLD: A1=%s', A1.tolist())
                                del MM0, MM1, B0, B1, M0, M1
                                break

                        except Exception as e:
                            LOG.trace('------04------ in exception loop SingularMatrix=%s', SingularMatrix)
                            import traceback
                            LOG.trace(traceback.format_exc())
                            if xorder0 != 0 or yorder0 != 0:
                                xorder0 = max(xorder0 - 1, 0)
                                yorder0 = max(yorder0 - 1, 0)
                                LOG.info('Fit failed. Trying lower order (%s, %s)', xorder0, yorder0)
                            else:
                                SingularMatrix = True
                                break
                    if SingularMatrix: break

                    # Calculate Sigma
                    # Sigma should be calculated in the upper stage
                    # Fit0: Center or Lmax, Fit1: Width or Lmin
                    Diff = []
                    # TN refactoring
                    # Calculation of Diff is duplicated here and
                    # following clipping stage. So, evalueate Diff
                    # only once here and reuse it in clipping.
                    for (Width, Center, x, y, flag) in FitData:
                        LOG.trace('%s %s %s %s %s %s', xorder0, yorder0, x, y, A0, A1)
                        (Fit0, Fit1) = _eval_poly(xorder0+1, yorder0+1, x, y, A0, A1)
                        Fit0 -= Center
                        Fit1 -= Width
                        Diff.append(sqrt(Fit0*Fit0 + Fit1*Fit1))
                    if len(effective) > 1:
                        npdiff = numpy.array(Diff)[effective]
                        Threshold = npdiff.mean()
                        Threshold += sqrt(numpy.square(npdiff - Threshold).mean()) * self.nsigma
                    else:
                        Threshold = Diff[effective[0]] * 2.0
                    LOG.trace('Diff = %s', [Diff[i] for i in effective])
                    LOG.trace('2D Fit Threshold = %s', Threshold)

                    # Sigma Clip
                    NFlagged = 0
                    Number = len(FitData)
                    ### 2011/05/15
                    for i in range(Number):
                        # Reuse Diff
                        if Diff[i] <= Threshold:
                            FitData[i][4] = 1
                        else:
                            FitData[i][4] = 0
                            NFlagged += 1

                    LOG.trace('2D Fit Flagged/All = (%s, %s)', NFlagged, Number)
                    #2009/10/15 compare the number of the remainder and fitting order
                    if (Number - NFlagged) <= max(xorder0, yorder0) or Number == NFlagged:
                        SingularMatrix = True
                        break
                    #2017/9/27 exit if there is update (no flag, or flag number is the same to the previous iteration)
                    if NFlagged == 0: break
                    if iteration == 0: NFlagOrg = NFlagged
                    elif NFlagOrg == NFlagged: break
                # Iteration End

                ### 2011/05/15 Fitting is no longer (Width, Center) but (minchan, maxChan)
                LOG.trace('------06------ End of Iteration: SingularMatrix=%s', SingularMatrix)
                if SingularMatrix: # skip for next Ns (SubCluster)
                    LOG.trace('------06b----- Skip for the next SubCluster')
                    continue

                LOG.trace('------07------ SingularMatrix=False')
                # Clear FitData and restore all relevant data
                # FitData: [(Chan0, Chan1, RA, DEC, Flag)]
                FitData = []
                for i in range(len(category)):
                    if category[i] == Nc and Region[i][5] == 1 and SubPlane[int((Region[i][3] - x0)/grid_ra)][int((Region[i][4] - y0)/grid_dec)] > self.Valid:
                        FitData.append(tuple(Region2[i][:5]))
                if len(FitData) == 0: continue

                # for Channel Map velocity range determination 2014/1/12
                (MaskMin, MaskMax) = (10000.0, 0.0)
                # Calculate Fit for each position
                LOG.trace('------08------ Calc Fit for each pos')
                for x in range(nra):
                    for y in range(ndec):
                        if ValidPlane[x][y] == 1:
                            LOG.trace('------09------ in ValidPlane x=%s y=%s', x, y)
                            for PID in Grid2SpectrumID[x][y]:
                                ID = index_list[PID]
                                ### 2011/05/15 (Width, Center) -> (minchan, maxChan)
                                (Chan1, Chan0) = _eval_poly(xorder0+1, yorder0+1, PosList[0][PID], PosList[1][PID], A0, A1)
                                Fit0 = 0.5 * (Chan0 + Chan1)
                                Fit1 = (Chan1 - Chan0) + 1.0
                                LOG.trace('Fit0, Fit1 = %s, %s', Fit0, Fit1)
                                # 2015/04/23 remove MaxFWHM check
                                if (Fit1 >= MinFWHM): # and (Fit1 <= MaxFWHM):
                                    ProtectMask = self.calc_ProtectMask(Fit0, Fit1, self.nchan, MinFWHM, MaxFWHM)
                                    # for Channel map velocity range determination 2014/1/12
                                    MaskCen = (ProtectMask[0] + ProtectMask[1]) / 2.0
                                    if MaskMin > MaskCen:
                                        MaskMin = max(0, MaskCen)
                                    if MaskMax < MaskCen:
                                        MaskMax = min(self.nchan - 1, MaskCen)

                                    if ID in RealSignal:
                                        RealSignal[ID][2].append(ProtectMask)
                                    else:
                                        RealSignal[ID] = [PosList[0][PID], PosList[1][PID], [ProtectMask]]
                                else:
                                    LOG.trace('------10------ out of range Fit0=%s Fit1=%s', Fit0, Fit1)
                        elif BlurPlane[x][y] == 1:
                            LOG.trace('------11------ in BlurPlane x=%s y=%s', x, y)
                            # in Blur Plane, Fit is not extrapolated,
                            # but use the nearest value in Valid Plane
                            # Search the nearest Valid Grid
                            Nearest = []
                            square_aspect = grid_ra / grid_dec
                            square_aspect *= square_aspect
                            Dist2 = numpy.inf
                            for xx in range(nra):
                                for yy in range(ndec):
                                    if ValidPlane[xx][yy] == 1:
                                        Dist3 = (xx-x)*(xx-x)*square_aspect + (yy-y)*(yy-y)
                                        if Dist2 > Dist3:
                                            Nearest = [xx, yy]
                                            Dist2 = Dist3
                            (RA0, DEC0) = (x0 + grid_ra * (x + 0.5), y0 + grid_dec * (y + 0.5))
                            (RA1, DEC1) = (x0 + grid_ra * (Nearest[0] + 0.5), y0 + grid_dec * (Nearest[1] + 0.5))

                            # Setup the position near the border
                            RA2 = RA1 - (RA1 - RA0) * HalfGrid / sqrt(Dist2)
                            DEC2 = DEC1 - (DEC1 - DEC0) * HalfGrid / sqrt(Dist2)
                            LOG.trace('[X,Y],[XX,YY] = [%s,%s],%s', x, y, Nearest)
                            LOG.trace('(RA0,DEC0),(RA1,DEC1),(RA2,DEC2) = (%.5f,%.5f),(%.5f,%.5f),(%.5f,%.5f)',
                                      RA0, DEC0, RA1, DEC1, RA2, DEC2)
                            # Calculate Fit and apply same value to all the spectra in the Blur Grid
                            ### 2011/05/15 (Width, Center) -> (minchan, maxChan)
                            # Border case
                            #(Chan0, Chan1) = _eval_poly(xorder0+1, yorder0+1, RA2, DEC2, A0, A1)
                            # Center case
                            (Chan1, Chan0) = _eval_poly(xorder0+1, yorder0+1, RA1, DEC1, A0, A1)
                            Fit0 = 0.5 * (Chan0 + Chan1)
                            Fit1 = (Chan1 - Chan0)
                            LOG.trace('Fit0, Fit1 = %s, %s', Fit0, Fit1)
                            # 2015/04/23 remove MaxFWHM check
                            if (Fit1 >= MinFWHM): # and (Fit1 <= MaxFWHM):
                                ProtectMask = self.calc_ProtectMask(Fit0, Fit1, self.nchan, MinFWHM, MaxFWHM)

                                for PID in Grid2SpectrumID[x][y]:
                                    ID = index_list[PID]
                                    if ID in RealSignal:
                                        RealSignal[ID][2].append(ProtectMask)
                                    else:
                                        RealSignal[ID] = [PosList[0][PID], PosList[1][PID], [ProtectMask]]
                            else:
                                LOG.trace('------12------ out of range Fit0=%s Fit1=%s', Fit0, Fit1)
                                continue

                # Add every SubClusters to GridCluster just for Plot
                GridCluster[Nc] += BlurPlane
                #if not SingularMatrix: GridCluster[Nc] += BlurPlane

            if ((GridCluster[Nc] > 0.5)*1).sum() < self.Questionable or MaskMax == 0.0:
                lines[Nc][2] = False
                channelmap_range[Nc][2] = False
            else:
                # for Channel map velocity range determination 2014/1/12 arbitrary factor 0.8
                #channelmap_range[Nc][1] = (MaskMax - MaskMin - 10) * 0.8
                #channelmap_range[Nc][1] = MaskMax - MaskMin + lines[Nc][1] / 2.0
                # MaskMax-MaskMin is an maximum offset of line center
                channelmap_range[Nc][1] = MaskMax - MaskMin + lines[Nc][1]
                LOG.info('Nc, MaskMax, Min: %s, %s, %s', Nc, MaskMax, MaskMin)
                LOG.info('channelmap_range[Nc]: %s', channelmap_range[Nc])
                LOG.info('lines[Nc]: %s', lines[Nc])

            for x in range(nra):
                for y in range(ndec):
                    if Original[x][y] > self.Valid: GridCluster[Nc][x][y] = 2.0
                    elif GridCluster[Nc][x][y] > 0.5: GridCluster[Nc][x][y] = 1.0

        threshold = [1.5, 0.5, 0.5, 0.5]
        flag_digit = self.flag_digits['final']
        cluster_flag = self.__update_cluster_flag(cluster_flag, 'final', GridCluster, threshold, flag_digit)

        return (RealSignal, lines, channelmap_range, cluster_flag)

    def CleanIsolation(self, nra: int, ndec: int, Original: numpy.ndarray, Plane: numpy.ndarray, GridMember):
        """Clean spatially isolated cluster.

        Pick up only Valid detections, and check if there are enough
        surrounding pixels with Valid detection. Create subplane of the
        cluster by collecting contiguous cluster members.

        Args:
            nra: Number of horizontal grids
            ndec: Number of vertical grids
            Original: Spatial distribution of Marginal+Valid detections.
                      Value ranges from 0 to 1.
            Plane: Integer binary flag, 0 (False) or 1 (True), indicating whether
                   or not any cluster member exists in the grid.
            GridMember: Number of spectra at each grid position. Value ranges
                        from 0 to 3.

        Returns:
            2-tuple of cleaned clusters, MemberList and RealMember. If no cluster is
            left after the cleaning, empty lists are returned.
            MemberList contains positions of cluster member with Marginal+Valid detection
            MemberList[n]: [[(x00,y00),(x01,y01),........,(x0k-1,y0k-1)],
                            [(x10,y10),(x11,y11),..,(x1i-1,y1i-1)],
                                    ......
                            [(xn-10,yn-10),(xn-11,yn-11),..,(xn-1i-1,yn-1i-1)]]
            Realmember contains number of cluster members with only Valid detection
            RealMember: [Nvalid_00, Nvalid_01, ..., Nvalid_n-1i-1]
        """
        Nmember = []  # number of positions where value > self.Marginal in each SubCluster
        Realmember = []  # number of positions where value > self.Valid in each SubCluster
        MemberList = []
        NsubCluster = 0
        # Separate cluster members into several SubClusters by spacial connection
        for x in range(nra):
            for y in range(ndec):
                if Plane[x][y] == 1:
                    Plane[x][y] = 2
                    SearchList = [(x, y)]
                    M = 1
                    if Original[x][y] > self.Valid:
                        MM = 1
                    else:
                        MM = 0
                    MemberList.append([(x, y)])
                    while(len(SearchList) != 0):
                        cx, cy = SearchList[0]
                        #for dx in [-1, 0, 1]:
                        for dx in range(-min(1, cx), min(2, nra-cx)):
                            #for dy in [-1, 0, 1]:
                            for dy in range(-min(1, cy), min(2, ndec-cy)):
                                (nx, ny) = (cx + dx, cy + dy)
                                #if 0 <= nx < nra and 0 <= ny < ndec and Plane[nx][ny] == 1:
                                if Plane[nx][ny] == 1:
                                    Plane[nx][ny] = 2
                                    SearchList.append((nx, ny))
                                    M += 1
                                    if Original[nx][ny] > self.Valid:
                                        MM += 1
                                    MemberList[NsubCluster].append((nx, ny))
                        del SearchList[0]
                    Nmember.append(M)
                    Realmember.append(MM)
                    NsubCluster += 1

        if len(Nmember) > 0:
            Threshold = min(0.5 * max(Realmember), 3)
            for n in range(NsubCluster - 1, -1, -1):
                # isolated cluster made from single spectrum should be omitted
                if Nmember[n] == 1:
                    (x, y) = MemberList[n][0]
                    if GridMember[x][y] <= 1:
                        Nmember[n] = 0
                # Sub-Cluster whose member below the threshold is cleaned
                if Nmember[n] < Threshold:
                    for (x, y) in MemberList[n]:
                        Plane[x][y] == 0
                    del Nmember[n], Realmember[n], MemberList[n]
        return MemberList, Realmember

    def DoBlur(self,
               Realmember: int,
               nra: int,
               ndec: int,
               SubPlane: numpy.ndarray,
               ratio: Integral
        ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Blur cluster subplane.

        Convolve subplane data with two-dimensional boxcar kernal whose
        radius depends on Realmember and ratio.

            R_blur = sqrt(Realmember / pi) * ratio + 1.5

        Args:
            Realmember: Number of valid cluster members in the subplane
            nra: Number of horizontal grids
            ndec: Number of vertical grids
            SubPlane: Spatial distribution of Valid detections.
            ratio: Factor for blur radius

        Returns:
            2-tuple of numpy arrays. First one is an integer binary array
            of whether original (pre-blur) pixels are Valid detection
            while the second array is also an integer binary array
            of whether blurred pixels are Valid or Marginal detection
        """
        # Calculate Blur radius
        BlurF = sqrt(Realmember / 3.141592653) * ratio + 1.5
        Blur = int(BlurF)
        # Set-up kernel for convolution
        # caution: if nra < (Blur*2+1) and ndec < (Blur*2+1)
        #  => dimension of SPC.convolve2d(Sub,kernel) gets not (nra,ndec) but (Blur*2+1,Blur*2+1)
        if nra < (Blur * 2 + 1) and ndec < (Blur * 2 + 1): Blur = int((max(nra, ndec) - 1) // 2)
        kernel = numpy.zeros((Blur * 2 + 1, Blur * 2 + 1), dtype=int)
        for x in range(Blur * 2 + 1):
            dx = Blur - x
            for y in range(Blur * 2 + 1):
                dy = Blur - y
                if sqrt(dx*dx + dy*dy) <= BlurF:
                    kernel[x][y] = 1
        # ValidPlane is used for fitting parameters
        # BlurPlane is not used for fitting but just extend the parameter determined in ValidPlane
        return (SubPlane > self.Valid) * 1, (convolve2d(SubPlane, kernel) > self.Marginal) * 1

    def calc_ProtectMask(self, Center: Integral, Width: Integral, nchan: int, MinFWHM: Integral, MaxFWHM: Integral) -> List[int]:
        """Return ProtectMask according to Center and Width.

        Return ProtectMask: [MaskL, MaskR]

        This method translates a range of protected range given as (center, width) value
        into (start, end) style. Effective width of the translated range is calculated
        from the original with and the input parameters.

        Args:
            Center: Center of the range
            Width: Width of the range
            nchan: Number of channels of the spectrum
            MinFWHM: Minimum FWHM
            MaxFTHM: Maximum FWHM

        Returns:
            Range of protected range as (start, end) value.
        """
        # To keep broad line region, make allowance larger
        ### 2015/04/23 Allowance=MaxFWHM at x=MaxFWHM, Allowance=2xMinFWHM+10 at x=MinFWHM
        Allowance = ((MaxFWHM-Width)*(2.0*MinFWHM+10.0) + (Width-MinFWHM)*MaxFWHM) / (MaxFWHM-MinFWHM) / 2.0
        ### 2011/10/21 left side mask exceeded nchan
        ProtectMask = [min(max(int(Center - Allowance), 0), nchan - 1), min(int(Center + Allowance), nchan - 1)]
        LOG.trace('Allowance = %s ProtectMask = %s' % (Allowance, ProtectMask))
        return ProtectMask

    def __merge_lines(self, lines: List[List[Integral]], nchan: int) -> List[List[Integral]]:
        """Merge overlapping lines.

        Args:
            lines: List of lines as (start, end) list
            nchan: Number of channels

        Returns:
            Merged list of lines
        """
        nlines = len(lines)
        if nlines < 1:
            return []
        elif nlines < 2:
            return lines
        else:
            # TN refactoring
            # New line merge algorithm that doesn't require 1-d array with
            # length of nchan+2. It will be faster if nchan is large while
            # it would be slow when number of lines is (extremely) large.
            nlines *= 2
            flat_lines = numpy.array(lines).reshape((nlines))
            sorted_index = flat_lines.argsort()
            flag = -1
            left_edge = flat_lines[sorted_index[0]]
            nedges = 0
            for i in range(1, nlines-2):
                if sorted_index[i] % 2 == 0:
                    flag -= 1
                else:
                    #flag = min(0, flag + 1)
                    flag += 1
                if flag == 0 and flat_lines[sorted_index[i]] != flat_lines[sorted_index[i+1]]:
                    sorted_index[nedges] = left_edge
                    sorted_index[nedges+1] = flat_lines[sorted_index[i]]
                    nedges += 2
                    left_edge = flat_lines[sorted_index[i+1]]
            sorted_index[nedges] = left_edge
            sorted_index[nedges+1] = flat_lines[sorted_index[-1]]
            nedges += 2
            return sorted_index[:nedges].reshape((nedges//2, 2)).tolist()

    def __update_cluster_flag(
        self,
        cluster_flag: numpy.ndarray,
        stage: str,
        GridCluster: numpy.ndarray,
        threshold: List[Integral],
        factor: int
    ) -> numpy.ndarray:
        """Update cluster flag array.

        Set integer flag to cluster flag array (cluster_flag) according
        to the detection rate and its threshold. Flag values are set to
        the digit specified by the factor.

        cluster_flag is data for plotting clustering analysis results.
        It stores GridCluster quantized by given thresholds.
        it is defined as integer array and one digit is assigned to
        one clustering stage in each integer value:

            1st digit: detection
            2nd digit: validation
            3rd digit: smoothing
            4th digit: final

        If GridCluster value exceeds any threshold, corresponding
        digit is incremented. For example, flag 3210 stands for,

            value didn't exceed any thresholds in detection, and
            exceeded one (out of three) threshold in validation, and
            exceeded two (out of three) thresholds in smoothing, and
            exceeded three (out of four) thresholds in final.

        Args:
            cluster_flag: Two-dimensional array holding flag information
            stage: Name of the cluster validation stage
            GridCluster: Number of clusters detected in each spatial position.
                         Each array element ranges from 0 (no cluster in the
                         position) to 3 (detected cluster in all spectra in
                         the position).
            threshold: Threshold for cluster flag. This specifies the threshold
                       for the detection rate, which ranges from 0 to 1. Important
                       values for threshold is 0.333 (1/3), and 0.666 (2/3).
            factor: Digit control factor

        Returns:
            Updated cluster flag array
        """
        #cluster_flag = self.cluster_info['cluster_flag']
        for t in threshold:
            cluster_flag = cluster_flag + factor * (GridCluster > t)
        #self.cluster_info['cluster_flag'] = cluster_flag
        self.cluster_info['%s_threshold'%(stage)] = threshold
        #LOG.trace('cluster_flag = {}', cluster_flag)
        return cluster_flag


def convolve2d(data: numpy.ndarray, kernel: numpy.ndarray, mode: str = 'nearest', cval: float = 0.0) -> numpy.ndarray:
    """Perform two-dimensional convolution.

    Perform two-dimensional convolution. This implements direct convolution
    rather than FFT based one. Returne array has the same shape as the input.

    Args:
        data: Two-dimensional array to be convolved
        kernel: Convolution kernel as two-dimensional array
        mode: Mode to handle edge values. Two options are available.

                - 'nearest'  use nearest pixel value for pixels beyond the edge
                - 'constant' use cval for pixels beyond the edge

              Defaults to 'nearest'.
        cval: Constant value used when mode is 'constant'

    Returns:
        Colvolved data array with the same shape as input data array
    """
    (ndx, ndy) = data.shape
    (nkx, nky) = kernel.shape
    edgex = int(0.5 * (nkx - 1))
    edgey = int(0.5 * (nky - 1))
    dummy = numpy.ones((ndx+2*edgex, ndy+2*edgey), dtype=numpy.float64) * cval
    dummy[edgex:ndx+edgex, edgey:ndy+edgey] = data
    if mode == 'nearest':
        dummy[0:edgex, 0:edgey] = data[0][0]
        dummy[0:edgex, edgey+ndy:] = data[0][ndy-1]
        dummy[edgex+ndx:, 0:edgey] = data[ndx-1][0]
        dummy[edgex+ndx:, edgey+ndy:] = data[ndx-1][ndy-1]
        for i in range(ndx):
            dummy[i+edgex, 0:edgey] = data[i][0]
            dummy[i+edgex, edgey+ndy:] = data[i][ndy-1]
        for i in range(ndy):
            dummy[0:edgex, i+edgey] = data[0][i]
            dummy[edgex+ndx:, i+edgey] = data[ndx-1][i]
    cdata = numpy.zeros((ndx, ndy), dtype=numpy.float64)
    for ix in range(ndx):
        for iy in range(ndy):
            for jx in range(nkx):
                for jy in range(nky):
                    idx = ix + jx
                    idy = iy + jy
                    val = dummy[idx][idy]
                    cdata[ix][iy] += kernel[jx][jy] * val
    return cdata


def _eval_poly(
    xorder: int, yorder: int,
    x: Integral, y: Integral,
    xcoeff: List[Integral], ycoeff: List[Integral]
) -> Tuple[Integral, Integral]:
    """Evaluate sum of the polynomial terms.

    It computes sum of two-dimensional polynomial terms
    provided by xorder, yorder, xcoeff, and ycoeff.
    While xorder and yorder indicates maximum order of
    the polynomial, xcoeff and ycoeff provides coefficients
    of each polynomial term.

    Args:
        xorder: Maximum polynomial order for x
        yorder: Maximum polynomial order for y
        x: x value
        y: y value
        xcoeff: Coefficients for polynomial
        ycoeff: Coefficients for polynomial

    Returns:
        Sum of the polynomials
    """
    xpoly = 0.0
    ypoly = 0.0
    yk = 1.0
    idx = 0
    for k in range(yorder):
        xjyk = yk
        for j in range(xorder):
            xpoly += xjyk * xcoeff[idx]
            ypoly += xjyk * ycoeff[idx]
            xjyk *= x
            idx += 1
        yk *= y
    return xpoly, ypoly


def _to_validated_lines(detect_lines: dict) -> List[List[Union[float, bool]]]:
    """Convert list of detected lines into list with flags.

    In addition to the conversion from dict to list, it also converts
    [chmin, chmax] style line information into [center, width] style
    and adds flag information (initially all True).

    Args:
        detect_lines: List of detected lines

    Returns:
        Converted list of detected lines with flag
    """
    # conversion from [chmin, chmax] to [center, width, T/F]
    lines = []
    for line_prop in detect_lines.values():
        for line in line_prop[2]:
            if line not in lines:
                lines.append(line)
    lines_withflag = [[0.5*sum(x), x[1]-x[0], True] for x in lines]
    return lines_withflag


class SVDSolver2D(object):
    """Least-square solver based on singular value decomposition (SVD).

    The singular value decomposition (SVD) is a factorization of a
    matrix, which has many practical mathematical application.
    This class determines the best coefficients of two-dimensional
    polynomials in a least-square sense using SVD.
    """

    CONDITION_NUMBER_LIMIT = 1.0e-12

    def __init__(self, xorder: int, yorder: int) -> None:
        """Construct SVDSolver2D instance.

        Args:
            xorder: Maximum order of x-polynomial. Must be 0 or positive.
            yorder: Maximum order of y-polynomial. Must be 0 or positive.
        """
        self.xorder = xorder
        self.yorder = yorder

        assert 0 <= self.xorder
        assert 0 <= self.yorder

        # internal storage for solver
        self.N = 0
        self.L = (self.xorder + 1) * (self.yorder + 1)

        # design matrix
        self.storage = numpy.empty(self.N * self.L, dtype=numpy.float64)
        self.G = None

        # for SVD
        self.Vs = numpy.empty((self.L, self.L), dtype=numpy.float64)
        self.B = numpy.empty(self.L, dtype=numpy.float64)
        self.U = None

    def set_data_points(self, x: Union[List[Integral], numpy.ndarray], y: Union[List[Integral], numpy.ndarray]) -> None:
        """Set data array.

        Configure design matrix from the input data arrays.

        Args:
            x: One-dimensional data array
            y: One-dimensional data array
        """
        nx = len(x)
        ny = len(y)
        LOG.trace('nx, ny = %s, %s', nx, ny)
        assert nx == ny
        if self.N < nx:
            self.storage = numpy.resize(self.storage, nx * self.L)
        self.N = nx
        assert self.L <= self.N

        self.G = self.storage[:self.N * self.L].reshape((self.N, self.L))

        # matrix operation
        self._set_design_matrix(x, y)
        #self._svd()

    def _set_design_matrix(self, x: Union[List[Integral], numpy.ndarray], y: Union[List[Integral], numpy.ndarray]) -> None:
        """Configure design matrix.

        The design matrix G is a basis array that stores gj(xi)
        where

            g0  = 1,   g1  = x,     g2  = x^2      g3  = x^3,
            g4  = y,   g5  = x y,   g6  = x^2 y,   g7  = x^3 y
            g8  = y^2, g9  = x y^2, g10 = x^2 y^2, g11 = x^3 y^2
            g12 = y^3, g13 = x y^3, g14 = x^2 y^3, g15 = x^3 y^3

        when xorder = 3 and yorder = 3

        Args:
            x: One-dimensional data array
            y: One-dimensional data array
        """
        for k in range(self.N):
            yp = 1.0
            for i in range(self.yorder + 1):
                xp = 1.0
                for j in range(self.xorder + 1):
                    l = j + (self.xorder + 1) * i
                    self.G[k, l] = xp * yp
                    xp *= x[k]
                yp *= y[k]

    def _do_svd(self) -> None:
        """Perform singular value decomposition (SVD)."""
        LOG.trace('G.shape=%s', self.G.shape)
        self.U, self.s, self.Vh = LA.svd(self.G, full_matrices=False)
        LOG.trace('U.shape=%s (N,L)=(%s,%s)', self.U.shape, self.N, self.L)
        LOG.trace('s.shape=%s', self.s.shape)
        LOG.trace('Vh.shape=%s', self.Vh.shape)
        LOG.trace('s = %s', self.s)
        assert self.U.shape == (self.N, self.L)
        assert len(self.s) == self.L
        assert self.Vh.shape == (self.L, self.L)

    def _svd_with_mask(self, nmask: int = 0) -> None:
        """Compute intermediate matrix for SVD least-square problem.

        Args:
            nmask: Number of singular values to be masked. Defaults to 0.
        """
        if not hasattr(self, 's'):
            # do SVD
            self._do_svd()

        assert nmask < self.L

        for icol in range(self.L):
            if self.L - 1 - icol < nmask:
                sinv = 0.0
            else:
                sinv = 1.0 / self.s[icol]
            for irow in range(self.L):
                self.Vs[irow, icol] = self.Vh[icol, irow] * sinv

    def _svd_with_eps(self, eps: float = 1.0e-7) -> None:
        """Compute intermediate matrix for SVD least-square problem.

        Args:
            eps: Threshold for masking singular values. Defaults to 1.0e-7.
        """
        if not hasattr(self, 's'):
            # do SVD
            self._do_svd()

        assert 0.0 < eps

        # max value of s is always s[0] since it is sorted
        # in descendent order
        #threshold = self.s.max() * eps
        threshold = self.s[0] * eps
        for icol in range(self.L):
            if self.s[icol] < threshold:
                sinv = 0.0
            else:
                sinv = 1.0 / self.s[icol]
            for irow in range(self.L):
                self.Vs[irow, icol] = self.Vh[icol, irow] * sinv

    def _svd(self, eps: float) -> None:
        """Perform singular value decomposition (SVD).

        After SVD, singular values are compared with the threshold
        determined by the max singular value with threshold factor,
        eps, and values less than threshold are masked.

        Args:
            eps: Threshold factor for masking singular value
        """
        LOG.trace('G.shape=%s', self.G.shape)
        self.U, s, Vh = LA.svd(self.G, full_matrices=False)
        LOG.trace('U.shape=%s (N,L)=(%s,%s)', self.U.shape, self.N, self.L)
        LOG.trace('s.shape=%s', s.shape)
        LOG.trace('Vh.shape=%s', Vh.shape)
        assert self.U.shape == (self.N, self.L)
        assert len(s) == self.L
        assert Vh.shape == (self.L, self.L)
        assert 0.0 < eps

        threshold = s.max() * eps
        for i in range(self.L):
            if s[i] < threshold:
                s[i] = 0.0
            else:
                s[i] = 1.0 / s[i]
        for icol in range(self.L):
            for irow in range(self.L):
                self.Vs[irow, icol] = Vh[icol, irow] * s[icol]

    def _eval_poly_from_G(self, row: int, coeff: numpy.ndarray) -> float:
        """Evaluate polynomial with given coefficients.

        Args:
            row: Row id for the matrix
            coeff: Polynomial coefficient. Least-square solution.

        Returns:
            Resulting value
        """
        idx = 0
        poly = 0.0
        for k in range(self.yorder + 1):
            for j in range(self.xorder + 1):
                poly += self.G[row, idx] * coeff[idx]
                idx += 1
        return poly

    def solve_with_mask(
        self,
        z: Union[List[Integral], numpy.ndarray],
        out: Optional[numpy.ndarray] = None,
        nmask: int = 0
    ) -> numpy.ndarray:
        """Solve least-square problem with SVD.

        Find x which minimizes ||A x - b||^2 where A is design matrix and
        b is a vector given as arg (denoted to z).

        With this method, one can specify number of singular values to
        be masked to obtain stable solution.

        Args:
            z: right hand side vector
            out: Storage for output solution. This is used when memory for the
                 solution is allocated externally. Defaults to None.
            nmask: Number of singular values to be masked. Defaults to 0.

        Returns:
            Least-square solution
        """
        nz = len(z)
        assert nz == self.N

        self._svd_with_mask(nmask)

        if out is None:
            A = numpy.zeros(self.L, dtype=numpy.float64)
        else:
            A = out
            A[:] = 0
            assert len(A) == self.L
        self.B[:] = 0
        for i in range(self.L):
            for k in range(self.N):
                self.B[i] += self.U[k, i] * z[k]
        for i in range(self.L):
            for k in range(self.L):
                A[i] += self.Vs[i, k] * self.B[k]

        return A

    def solve_with_eps(
        self,
        z: Union[List[Integral], numpy.ndarray],
        out: Optional[numpy.ndarray] = None,
        eps: float = 1.0e-7
    ) -> numpy.ndarray:
        """Solve least-square problem with SVD.

        Find x which minimizes ||A x - b||^2 where A is design matrix and
        b is a vector given as arg (denoted to z).

        With this method, one can specify the threshold for singular values
        to be masked to obtain stable solution.

        Args:
            z: right hand side vector
            out: Storage for output solution. Defaults to None.
            eps: Threshold factor for masking singular values. Defaults to 1.0e-7.

        Returns:
            Least-square solution
        """
        assert 0.0 <= eps

        nz = len(z)
        assert nz == self.N

        self._svd_with_eps(eps)

        if out is None:
            A = numpy.zeros(self.L, dtype=numpy.float64)
        else:
            A = out
            A[:] = 0
            assert len(A) == self.L
        self.B[:] = 0
        for i in range(self.L):
            for k in range(self.N):
                self.B[i] += self.U[k, i] * z[k]
        for i in range(self.L):
            for k in range(self.L):
                A[i] += self.Vs[i, k] * self.B[k]

        return A

    def solve_for(
        self,
        z: Union[List[Integral], numpy.ndarray],
        out: Optional[numpy.ndarray] = None,
        eps: float = 1.0e-7
    ) -> numpy.ndarray:
        """Solve least-square problem with SVD.

        Find x which minimizes ||A x - b||^2 where A is design matrix and
        b is a vector given as arg (denoted to z).

        With this method, one can specify the threshold for singular values
        to be masked to obtain stable solution.

        Args:
            z: right hand side vector
            out: Storage for output solution. Defaults to None.
            eps: Threshold factor for masking singular values. Defaults to 1.0e-7.

        Returns:
            Least-square solution
        """
        assert 0.0 <= eps

        nz = len(z)
        assert nz == self.N

        self._svd(eps)

        if out is None:
            A = numpy.zeros(self.L, dtype=numpy.float64)
        else:
            A = out
            A[:] = 0
            assert len(A) == self.L
        self.B[:] = 0
        for i in range(self.L):
            for k in range(self.N):
                self.B[i] += self.U[k, i] * z[k]
        for i in range(self.L):
            for k in range(self.L):
                A[i] += self.Vs[i, k] * self.B[k]

        return A

    def find_good_solution(
        self,
        z: Union[List[Integral], numpy.ndarray],
        threshold: float = 0.05
    ) -> numpy.ndarray:
        """Find the best least-square solution from candidate SVD solutions.

        Find x which minimizes ||A x - b||^2 where A is design matrix and
        b is a vector given as arg (denoted to z).

        This method examines the solution with various masking threshold
        for singular value, and find the best solution among them.
        Range of masking threshold value is chosen empirically. Currently,
        the values ranging from 10^-11 to 10^-3 are examined.

        Solutions are scored based on the mean fractional deviation from
        actual data. If fractional deviaion exceeds threshold given as
        an argument, that will be noticed via the log message.
        If fractional deviation exceeds 1.0, exception will be thrown.

        Args:
            z: right hand side vector
            threshold: Threshold for score. Should be 0 or positive value.
                       Defaults to 0.05.

        Raises:
            RuntimeError: No good least-square solution is found

        Returns:
            The best least-square solution
        """
        assert 0.0 <= threshold
        eps_list = [10**x for x in range(-11, -3)]

        best_score = 1e30
        best_eps = eps_list[0]
        intlog = lambda x: int(numpy.log10(x))
        ans = numpy.empty(self.L, dtype=numpy.float64)
        best_ans = numpy.empty(self.L, dtype=numpy.float64)
        diff = numpy.empty(self.N, dtype=numpy.float64)

        # do SVD
        self._do_svd()

        for eps in eps_list:
            ans = self.solve_with_eps(z, out=ans, eps=eps)
            for i in range(self.N):
                fit = self._eval_poly_from_G(i, ans)
                if z[i] != 0:
                    diff[i] = abs((fit - z[i]) / z[i])
                else:
                    diff[i] = fit
            score = diff.mean()
            LOG.trace('eps=%s, score=%s', intlog(eps), score)
            if best_ans is None or score < best_score:
                best_ans[:] = ans
                best_score = score
                best_eps = eps
        if 1.0 <= best_score:
            raise RuntimeError('No good solution is found.')
        elif threshold < best_score:
            LOG.trace('Score is higher than given threshold (threshold %s, score %s)', threshold, best_score)

        LOG.trace('best eps: %s (score %s)', intlog(best_eps), best_score)
        return best_ans


def ValidationFactory(pattern: str) -> Union[Type[ValidateLineRaster],Type[ValidateLineSinglePointing]]:
    """Return appropriate task class according to observing pattern.

    The pattern string must be in uppercase letters.

    Args:
        pattern: Observing pattern

    Raises:
        ValueError: Invalid observing pattern

    Returns:
        Task class. Either ValidateLineRaster or ValidateLineSinglePointing.
    """
    if pattern == 'RASTER':
        return ValidateLineRaster
    elif pattern == 'SINGLE-POINT' or pattern == 'MULTI-POINT':
        return ValidateLineSinglePointing
    else:
        raise ValueError('Invalid observing pattern')
