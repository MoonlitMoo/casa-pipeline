"""Spectral line detection task."""
import collections
import math
import numbers
import numpy
import os
import time
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union

import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
import pipeline.h.heuristics as heuristics
# import pipeline.domain.measures as measures
# from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tools
from .. import common
from ..common import utils
from . import rules

from .typing import LineWindow

if TYPE_CHECKING:
    from pipeline.domain import MeasurementSet
    from pipeline.infrastructure.launcher import Context

NoData = common.NoData

LOG = infrastructure.get_logger(__name__)


class DetectLineInputs(vdp.StandardInputs):
    """Inputs for spectral line detection task."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    edge = vdp.VisDependentProperty(default=(0, 0))
    broadline = vdp.VisDependentProperty(default=True)

    @property
    def windowmode(self) -> str:
        """Return windowmode value.

        Returns:
            Windowmode string. Default is 'replace'.
        """
        return getattr(self, '_windowmode', 'replace')

    @windowmode.setter
    def windowmode(self, value: str) -> None:
        """Set windowmode.

        Args:
            value: windowmode value. Should be either 'replace' or 'merge'.

        Raises:
            ValueError: Invalid windowmode value.
        """
        if value not in ['replace', 'merge']:
            raise ValueError("linewindowmode must be either 'replace' or 'merge'.")
        self._windowmode = value

    def __init__(self,
                 context: 'Context',
                 group_id: int,
                 window: Optional[LineWindow] = None,
                 windowmode: Optional[str] = None,
                 edge: Optional[Tuple[int, int]] = None,
                 broadline: Optional[bool] = None) -> None:
        """Construct DetectLineInputs instance.

        Args:
            context: Pipeline context
            group_id: Reduction group id.
            window: Manual line window. Defaults to None, which means that no user-defined
                        line window is given.
            windowmode: Line window handling mode. 'replace' exclusively uses manual line window
                        while 'merge' merges manual line window into automatic line detection
                        and validation result. Defaults to 'replace' if None is given.
            edge: Edge channels to exclude. Defaults to None, which means that all channels
                  are processed.
            broadline: Detect broadline component or not. Defaults to True if None is given.
        """
        super(DetectLineInputs, self).__init__()

        self.context = context
        self.group_id = group_id
        self.window = window
        self.windowmode = windowmode
        self.edge = edge
        self.broadline = broadline


class DetectLineResults(common.SingleDishResults):
    """Results class to hold the result of spectral line detection task."""

    def __init__(self,
                 task: Optional[Type[basetask.StandardTaskTemplate]] = None,
                 success: Optional[bool] = None,
                 outcome: Any = None) -> None:
        """Construct DetectLineResults instance.

        Args:
            task: Task class that produced the result.
            success: Whether task execution is successful or not.
            outcome: Outcome of the task execution.
        """
        super(DetectLineResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        No specific merge operation is done.

        Args:
            context: Pipeline context.
        """
        LOG.debug('DetectLineResults.merge_with_context')
        super(DetectLineResults, self).merge_with_context(context)

    @property
    def signals(self) -> collections.OrderedDict:
        """Return detected spectral lines and associated spatial coordinates.

        Returns:
            Detected lines associated with spatial coordinates
        """
        return self._get_outcome('signals')

    def _outcome_name(self) -> str:
        """Return string representing the outcome.

        Returns:
            Empty string
        """
        return ''


class DetectLine(basetask.StandardTaskTemplate):
    """Spectral line detection task."""

    Inputs = DetectLineInputs
    LineFinder = heuristics.HeuristicsLineFinder
    ThresholdFactor = 3.0

    def __init__(self, inputs: DetectLineInputs) -> None:
        """Construct DetectLine instance.

        In addition to the construction process of superclass,
        it initializes LineFinder heuristics instance.

        Args:
            inputs: DetectLineInputs instance.
        """
        super(DetectLine, self).__init__(inputs)
        self.line_finder = self.LineFinder()

    def prepare(self,
                datatable_dict: dict,
                grid_table: List[List[Union[int, float, numpy.ndarray]]],
                spectral_data: Optional[numpy.ndarray] = None) -> DetectLineResults:
        """Find spectral line feature.

        The process finds emission lines and determines protection regions for baselinefit

        Args:
            datatable_dict: Dictionary holding datatable instance per MS.
            grid_table: Metadata for gridding. See simplegrid.py for detail.
            spectral_data: Gridded spectral data.

        Returns:
            DetectLineResults instance

        Raises:
            RuntimeError: Too large edge masks
        """
        spectra = spectral_data
        masks = (spectra != NoData)
        window = self.inputs.window
        windowmode = self.inputs.windowmode
        edge = self.inputs.edge
#         broadline = self.inputs.broadline #Not used anymore

        detect_signal = collections.OrderedDict()

        # Pre-Defined Spectrum Window
        LOG.debug('{}: window={}, windowmode={}'.format(self.__class__.__name__, window, windowmode))
        if windowmode == 'replace' and (window is None or len(window) > 0):
            LOG.info(f'Skip line detection: windowmode="{windowmode}", window="{window}"')
            nrow = len(grid_table)
            assert nrow > 0
            # predefined_window should be derived at the upper task (MaskLine)
            # and should be passed to inputs.window
            if window is None:
                window = []
            group_id = self.inputs.group_id
            group_desc = self.inputs.context.observing_run.ms_reduction_group[group_id]
            LOG.trace('predefined_window={0}'.format(window))
            for row in range(nrow):
                grid_info = grid_table[row]
                ra = grid_info[4]
                dec = grid_info[5]
                detect_signal[row] = [ra, dec, window]
            for m in group_desc:
                ms = m.ms
                spw_id = m.spw_id
                field_id = m.field_id
                antenna_id = m.antenna_id
                origin_basename = os.path.basename(ms.origin_ms)
                if origin_basename not in datatable_dict:
                    continue

                datatable = datatable_dict[origin_basename]
                for dt_row in utils.get_index_list_for_ms(datatable, [origin_basename], [antenna_id],
                                                     [field_id], [spw_id]):
                    datatable.putcell('MASKLIST', dt_row, window)

            result = DetectLineResults(task=self.__class__,
                                       success=True,
                                       outcome={'signals': detect_signal})

            return result

        # move assertion for spectral_data here since spectral_data is
        # not necessary when line window is specified
        assert spectral_data is not None

        (nrow, nchan) = spectra.shape

        LOG.info('Search regions for protection against the background subtraction...')
        LOG.info('DetectLine: Processing %s spectra...', nrow)

        # Set edge mask region
        (EdgeL, EdgeR) = common.parseEdge(edge)
        Nedge = EdgeR + EdgeL
        LOG.info('edge=%s', edge)
        LOG.info('EdgeL, EdgeR=%s, %s', EdgeL, EdgeR)
        LOG.info('Nedge=%s', Nedge)
        if Nedge >= nchan:
            message = 'Error: Edge masked region too large...'
            LOG.error(message)
            raise RuntimeError(message)

        #2019/08/16 MaxFWHM < nchan/2.0 Need wider line detection (NGC1097)
        MaxFWHM = int((nchan - Nedge)/2.0)
        Threshold = rules.LineFinderRule['Threshold']

        # 2011/05/17 TN
        # Switch to use either ASAP linefinder or John's linefinder
        ### Only 'Thre' is effective for Heuristics Linefinder. BoxSize, AvgLimit, and MinFWHM are ignored
        Thre = Threshold * self.ThresholdFactor

        # Create progress timer
        Timer = common.ProgressTimer(80, nrow, LOG.level)
        # 100.0: minimum number of channels for binned spectrum to detect lines
        MinChanBinSp = 50.0
        TmpRange = [4**i for i in range(int(math.ceil(math.log(len(spectra[0])/MinChanBinSp)/math.log(4))))]
        BinningRange = []
        for i in TmpRange:
            BinningRange.append([i, 0])
            if i > 1:
                BinningRange.append([i, i//2])
        for row in range(nrow):
            # Countup progress timer
            Timer.count()

            ProcStartTime = time.time()
            Protected = []
            if len(grid_table[row][6]) == 0:
                LOG.debug('Row %s: No spectrum', row)
                # No spectrum
                Protected = [[-1, -1, 1]]
            else:
                LOG.debug('Start Row %s', row)
                for [BINN, offset] in BinningRange:
                    SP = self.SpBinning(spectra[row], BINN, offset)
                    MSK = self.MaskBinning(masks[row], BINN, offset)

                    protected = self._detect(spectrum=SP,
                                             mask=MSK,
                                             threshold=Thre+math.sqrt(BINN)-1.0,
                                             tweak=True,
                                             edge=(EdgeL, EdgeR))
                    # 2019/8/16 Threshold gets too high when Binning gets large
                    #protected = self._detect(spectrum=SP,
                    #                         mask=MSK,
                    #                         threshold=Thre+math.log(BINN)/math.log(4),
                    #                         tweak=True,
                    #                         edge=(EdgeL, EdgeR))

                    MaxLineWidth = MaxFWHM
                    MinLineWidth = rules.LineFinderRule['MinFWHM']
                    for i in range(len(protected)):
                        if protected[i][0] != -1:
                            Chan0 = protected[i][0]*BINN+offset
                            Chan1 = protected[i][1]*BINN-1+offset
                            ChanW = Chan1 - Chan0
                            if (MinLineWidth <= ChanW) and (ChanW <= MaxLineWidth):
                                Protected.append([Chan0, Chan1, BINN])
                        else:
                            Protected.append([-1, -1, BINN])

                # plot to check detected lines
                #self.plot_detectrange(spectra[row], Protected, 'SpPlot0%04d.png' % row)

            detect_signal[row] = [grid_table[row][4],  # RA
                                  grid_table[row][5],  # DEC
                                  Protected]           # Protected Region
            ProcEndTime = time.time()
            LOG.info('Channel ranges of detected lines for Row %s: %s', row, detect_signal[row][2])

            LOG.debug('End Row %s: Elapsed Time=%.1f sec', row, (ProcEndTime - ProcStartTime))
        del Timer

        #LOG.debug('DetectSignal = %s'%(detect_signal))
        result = DetectLineResults(task=self.__class__,
                                   success=True,
                                   outcome={'signals': detect_signal})

        return result

    def plot_detectrange(self,
                         sp: numpy.ndarray,
                         protected: List[int],
                         fname: str) -> None:
        """Plot detected line range for testing.

        Args:
            sp: Spectrald data
            protected: Detected line ranges in channels. One-dimensional
                       list containing start and end channels for detected
                       lines alternatively.
            fname: File name for output PNG file.
        """
        print(protected, fname)
        plt.clf()
        plt.plot(sp)
        ymin, ymax = plt.ylim()
        for i in range(len(protected)):
            y = ymin + (ymax - ymin)/30.0 * (i + 1)
            plt.plot(protected[i], (y, y), 'r')
        plt.savefig(fname, format='png')

    def MaskBinning(self,
                    data: numpy.ndarray,
                    Bin: int,
                    offset: int = 0) -> numpy.ndarray:
        """Perform Binning for mask array.

        Args:
            data: boolean mask array
            Bin: Binning width
            offset: Offset channel to start binning. Defaults to 0.

        Returns:
            Mask array after binning
        """
        if Bin == 1:
            return data
        else:
            return numpy.array([data[i:i+Bin].min() for i in range(offset, len(data)-Bin+1, Bin)], dtype=bool)

    def SpBinning(self,
                  data: numpy.ndarray,
                  Bin: int,
                  offset: int = 0) -> numpy.ndarray:
        """Perform Binning for spectral data array.

        Args:
            data: float mask array
            Bin: Binning width
            offset: Offset channel to start binning. Defaults to 0.

        Returns:
            Spectral data array after binning
        """
        if Bin == 1:
            return data
        else:
            return numpy.array([data[i:i+Bin].mean() for i in range(offset, len(data)-Bin+1, Bin)], dtype=float)

    def analyse(self, result: DetectLineResults) -> DetectLineResults:
        """Analyse result.

        Do nothing.

        Returns:
            DetectLineResults instance
        """
        return result

    def _detect(self,
                spectrum: numpy.ndarray,
                mask: numpy.ndarray,
                threshold: float,
                tweak: bool,
                edge: List[int]) -> List[List[int]]:
        """Perform spectral line detection on given spectral data with mask.

        Args:
            spectrum: Spectral data
            mask: Channel mask
            threshold: Threshold for linedetection
            tweak: if True, spectral line ranges are extended to cover line edges.
            edge: Edge channels to exclude

        Returns:
            A list of [start, end] index lists of spectral lines, more explicitly,
            [[start0, end0], [start1, end1]..., [startN, endN]].
        """
        nchan = len(spectrum)
        (EdgeL, EdgeR) = edge
        Nedge = EdgeR + EdgeL
        #2015/04/23 0.5 -> 1/3.0
        #MaxFWHM = int(min(rules.LineFinderRule['MaxFWHM'], (nchan - Nedge)/3.0))
        #2019/08/16 MaxFWHM < nchan/2.0 Need wider line detection (NGC1097)
        MaxFWHM = int((nchan - Nedge)/2.0)
        MinFWHM = int(rules.LineFinderRule['MinFWHM'])

        LOG.trace('line detection parameters: ')
        LOG.trace('threshold (S/N per channel)=%s, channels, edges to be dropped=[%s, %s]',
                  threshold, EdgeL, EdgeR)
        line_ranges = self.line_finder(spectrum=spectrum,
                                       threshold=threshold,
                                       tweak=True,
                                       mask=mask,
                                       edge=(int(EdgeL), int(EdgeR)))
        # line_ranges = [line0L, line0R, line1L, line1R, ...]
        nlines = len(line_ranges) // 2

        ### Debug TT
        #LOG.info('NLINES=%s, EdgeL=%s, EdgeR=%s' % (nlines, EdgeL, EdgeR))
        #LOG.debug('ranges=%s'%(line_ranges))

        protected = []
        for y in range(nlines):
            Width = line_ranges[y*2+1] - line_ranges[y*2] + 1
            ### 2011/05/16 allowance was moved to clustering analysis
            #allowance = int(Width/5)
            LOG.debug('Ranges=%s, Width=%s', line_ranges[y*2:y*2+2], Width)
            if (Width >= MinFWHM and Width <= MaxFWHM and line_ranges[y*2] > EdgeL and
                    line_ranges[y*2+1] < (nchan - 1 - EdgeR)):
                protected.append([line_ranges[y*2], line_ranges[y*2+1]])
        if len(protected) == 0:
            protected = [[-1, -1]]
        elif len(protected) > 1:
            # 2007/09/01 add merged lines to the list if two lines are close enough
            flag = True
            for y in range(len(protected) - 1):
                curr0, curr1 = protected[y][0], protected[y][1]
                next0, next1 = protected[y+1][0], protected[y+1][1]
                if (next0 - curr1) < (0.25*min((curr1-curr0), (next1-next0))):
                    if flag:
                        if curr1 < next1 and curr0 < next0 and (next1 - curr0) <= MaxFWHM:
                            protected.append([curr0, next1])
                            Line0 = curr0
                        else:
                            continue
                    else:
                        if (next1 - Line0) <= MaxFWHM:
                            protected.pop()
                            protected.append([Line0, next1])
                        else:
                            flag = True
                            continue
                    flag = False
                else:
                    flag = True
        return protected


class LineWindowParser(object):
    """LineWindowParser is a parser for line window parameter.

    Supported format is as follows:

    [Single window list] -- apply to all spectral windows
      - integer list [chmin, chmax]
      - float list [fmin, fmax]
      - string list ['XGHz', 'YGHz']

    [Multiple window list] -- apply to all spectral windows
      - nested integer list [[chmin, chmax], [chmin, chmax], ...]
      - nested float list [[fmin, fmax], [fmin, fmax], ...]
      - nested string list [['XGHz', 'YGHz'], ['aMHz', 'bMHz'], ...]

    [Dictionary] -- apply to selected spectral windows
      - {<spwid>: <window list>} style dictionary
      - spwid should be an integer specifying spectral window id
      - window list should be in one of the above list-type window formats

    [MS channel selection syntax] -- apply to selected spectral windows
      - channel selection string 'A:chmin~chmax;chmin~chmax,B:fmin~fmax,...'

    Note that frequencies are interpreted as the value in LSRK frame except
    for the MS selection syntax format.
    Note also that frequencies given as a floating point number is interpreted
    as the value in Hz.

    Usage:
        parser = LineWindowParser(ms, window)
        parser.parse(field_id)
        for spwid in spwids:
            parsed = parser.get_result(spwid)

    """

    def __init__(self,
                 ms: 'MeasurementSet',
                 window: LineWindow) -> None:
        """
        Construct LineWindowParser instance.

        Args:
            ms: ms domain object
            window: line window parameter
        """
        self.ms = ms
        self.window = window
        self.parsed = None

        # science spectral windows
        self.science_spw = [x.id for x in self.ms.get_spectral_windows(science_windows_only=True)]

        # measure tool
        self.me = casa_tools.measures

    def parse(self, field_id: int) -> None:
        """Parse given parameter into dictionary.

        Result is cached as parsed attribute.

        Args:
            field_id: Field id to use
        """
        # convert self.window into dictionary
        if isinstance(self.window, str):
            if self.window.strip().startswith('{'):
                # should be a dictionary as a string (PPR execution)
                # convert string into dictionary
                processed = self._exclude_non_science_spws(
                    self._dict2dict(eval(self.window))
                )
            elif self.window.strip().startswith('['):
                # should be a list as a string (PPR execution)
                # convert string into list
                processed = self._list2dict(eval(self.window))
            else:
                # should be MS channel selection syntax
                # convert string into dictionary
                # then, filter out non-science spectral windows
                processed = self._exclude_non_science_spws(
                    self._string2dict(self.window)
                )
        elif isinstance(self.window, (list, numpy.ndarray)):
            # convert string into dictionary
            # keys are all science spectral window ids
            processed = self._list2dict(self.window)
        elif isinstance(self.window, dict):
            # filter out non-science spectral windows
            processed = self._exclude_non_science_spws(
                self._dict2dict(self.window)
            )
        else:
            # unsupported format or None
            processed = dict((spw, []) for spw in self.science_spw)

        # convert frequency selection into channel selection
        self.parsed = {}
        self._measure_init(field_id)
        try:
            for spwid, _window in processed.items():
                LOG.trace('_window=%s type %s', _window, type(_window))
                new_window = self._freq2chan(spwid, _window)
                if new_window is not None \
                    and len(new_window) > 0 \
                        and not isinstance(new_window[0], list):
                    new_window = [new_window]
#                 if len(new_window) > 0:
#                     tmp = []
#                     for w in new_window:
#                         if len(w) == 2:
#                             tmp.append(w)
#                     new_window = tmp
                self.parsed[spwid] = new_window
        finally:
            self._measure_done()

        # consistency check
        for spwid in self.science_spw:
            assert spwid in self.parsed

    def get_result(self, spw_id: int) -> Optional[List[int]]:
        """Return parsed line windows for given spw id.

        Args:
            spw_id: spw id

        Returns:
            Line windows as one-dimensional list that provides start/end channels
            of line windows alternatively.
        """
        if spw_id not in self.science_spw:
            LOG.info('Non-science spectral window was specified. Returning default window [].')
            return []

        if self.parsed is None:
            LOG.info('You need to run parse method first. Returning default window [].')
            return []

        if spw_id not in self.parsed:
            LOG.info('Unexpected behavior. Returning default window [].')
            return []

        return self.parsed[spw_id]

    def _string2dict(self, window: str) -> dict:
        """Convert line window string into dict.

        Args:
            window: Line window in the form of channel selection string

        Raises:
            RuntimeError: String is not compatible with channel selection syntax

        Returns:
            Dictionary containing line window list per spw
        """
        # utilize ms tool to convert selection string into lists
        with casa_tools.MSReader(self.ms.name) as ms:
            try:
                ms.msselect({'spw': window})
                idx = ms.msselectedindices()
            except RuntimeError as e:
                msg = str(e)
                LOG.warning(msg)
                if msg.startswith('No valid SPW'):
                    idx = {'channel': []}
                else:
                    raise e

        new_window = {}
        channel_selection = idx['channel']
        for sel in channel_selection:
            assert len(sel) == 4

            spwid = sel[0]
            chansel = list(sel[1:3])
            if spwid not in new_window:
                new_window[spwid] = []

            new_window[spwid].append(chansel)

        for spwid in self.science_spw:
            if spwid not in new_window:
                new_window[spwid] = []

        return new_window

    def _list2dict(self, window: List[int]) -> dict:
        """Convert line window list into dict.

        Simply applies the given line window list to all spws.

        Args:
            window: Line window in the form of channel list

        Returns:
            Dictionary containing line window list per spw
        """
        # apply given window to all science windows

        return dict((spwid, window) for spwid in self.science_spw)

    def _dict2dict(self, window: dict) -> dict:
        """Convert line window dict into another dict.

        Simply converts dict key to integer.

        Args:
            window: Line window list in the form of dict

        Returns:
            Dictionary containing line window list per spw
        """
        # key should be an integer
        return dict((int(spw), value) for spw, value in window.items())

    def _exclude_non_science_spws(self, window: dict) -> dict:
        """Filter line windows only for science spws.

        Args:
            window: Line window list per spw

        Returns:
            Filtered dict of line window list per spw
        """
        # filter non-science windows
        # set default window to science windows if not specified
        new_window = {}
        for spwid in self.science_spw:
            if spwid in window:
                w = window[spwid]
                if w is None:
                    new_window[spwid] = None
                else:
                    new_window[spwid] = list(w)
            else:
                new_window[spwid] = []

        return new_window

    def _freq2chan(self,
                   spwid: int,
                   window: Optional[Union[List[str], List[float], List[int]]]) -> Optional[List[int]]:
        """Convert frequency selection into channel selection.

        If float values are given, they are interpreted as the value in Hz.
        Input frequency values should be in LSRK frame. LSRK frequencies are
        converted to the frame in which spw is defined.

        If int values are given, input window list is simply sorted and
        returned as it is.

        Args:
            spwid: spw id to process
            window: Line window list in frequency domain

        Returns:
            Line window list in channel domain
        """
        if window is None:
            return window

        # window must be a list
        assert isinstance(window, list), "Unexpected value for 'window', must be a list."

        # return without conversion if empty list
        if len(window) == 0:
            return window

        item_type = type(window[0])

        # process recursively if item is a list
        if item_type in (list, numpy.ndarray):
            converted = []
            for w in window:
                LOG.trace('_freq2chan: w=%s type %s', w, type(w))
                _w = self._freq2chan(spwid, w)
                LOG.trace('_freq2chan: _w=%s type %s', _w, type(_w))
                if len(_w) == 2:
                    converted.append(_w)

            return converted

        # return without conversion if item is an integer
        if issubclass(item_type, numbers.Integral):
            window.sort()
            return window

        # convert floating-point value to quantity string
        if issubclass(item_type, numbers.Real):
            return self._freq2chan(spwid, ['{0}Hz'.format(x) for x in window])

        # now list item should be a quantity string
        assert item_type == str, 'unexpected item type {0}'.format(item_type)

        # also, length of the window should be 2
        assert len(window) == 2

        # frequency conversion from LSRK to TOPO
        new_window = self._lsrk2topo(spwid, window)

        # construct ms channel selection syntax
        spwsel = self._construct_msselection(spwid, new_window)

        # channel mapping using ms tool
        processed = self._string2dict(spwsel)

        # target spwid should exist
        assert spwid in processed

        new_window = sorted(processed[spwid])
        LOG.trace('_freq2chan: new_window=%s type %s', new_window, type(new_window))
        if len(new_window) == 0:
            return []
        assert len(new_window) == 1
        return new_window[0]

    def _lsrk2topo(self, spwid: int, window: List[str]) -> List[str]:
        """Apply frame conversion to line window frequencies in LSRK as needed.

        Args:
            spwid: spw id
            window: Line window list in LSRK frequency

        Returns:
            Line window list in the frame that spw is defined. In the case
            of ALMA data, spw is defined in TOPO frame so the returned
            frequency values are the ones in TOPO.
        """
        # if frequency frame for target spw is LSRK, just return input window
        spw = self.ms.get_spectral_window(spwid)
        frame = spw.frame
        if frame == 'LSRK':
            return window

        # assuming that measure tool is properly initialized
        qa = casa_tools.quanta
        qfreq = [qa.quantity(x) for x in window]
        if qa.gt(qfreq[0], qfreq[1]):
            qfreq = [qfreq[1], qfreq[0]]
        mfreq = [self.me.frequency(rf='LSRK', v0=x) for x in qfreq]
        new_mfreq = [self.me.measure(v=x, rf=frame) for x in mfreq]
        new_window = ['{value}{unit}'.format(**x['m0']) for x in new_mfreq]
        return new_window

    def _construct_msselection(self, spwid: int, window: List[str]) -> str:
        """Construct channel selection string for given spw.

        Args:
            spwid: spw id to apply selection
            window: line window list in the form of string quantity list

        Returns:
            channel selection string for the spw
        """
        return '{0}:{1}~{2}'.format(spwid, window[0], window[1])

    def _measure_init(self, field_id: int) -> None:
        """Initialize measure tool.

        Initialize measure tool from scratch. Set required measures
        for frequency conversion extracted from the MS domain object.

          - time measure from observation start time
          - position measure from antenna array position
          - direction measure from the field specified by field_id

        Args:
            field_id: Reference field id for direction measure
        """
        self._measure_done()
        # position is an observatory position
        position = self.ms.antenna_array.position

        # direction is a field reference direction
        fields = self.ms.get_fields(field_id=field_id)
        direction = fields[0].mdirection

        # epoch is an observing start time
        epoch = self.ms.start_time

        # initialize the measure
        self.me.doframe(position)
        self.me.doframe(direction)
        self.me.doframe(epoch)

    def _measure_done(self) -> None:
        """Close meaure tool."""
        self.me.done()


# TODO: move this to detection_test.py when appropriate MS data is available
#       or testable ms domain object can be created
def test_parser(ms: 'MeasurementSet') -> None:
    """Test LineWindowParser.

    Args:
        ms: MeasurementSet domain object
    """
    target_fields = ms.get_fields(intent='TARGET')
    field_id = target_fields[0].id
    science_spws = ms.get_spectral_windows(science_windows_only=True)
    science_spw_ids = [x.id for x in science_spws]
    # alias for science_spw_ids
    spwids = science_spw_ids
    chan_freq0 = science_spws[0].channels.chan_freqs.start
    increment0 = science_spws[0].channels.chan_freqs.delta
    get_chan_freq0 = lambda x: chan_freq0 + increment0 * x
    get_chan_qfreq0 = lambda x: '{0}Hz'.format(get_chan_freq0(x))
    chan_freq1 = science_spws[-1].channels.chan_freqs.start
    increment1 = science_spws[-1].channels.chan_freqs.delta
    get_chan_freq1 = lambda x: chan_freq1 + increment1 * x
    get_chan_qfreq1 = lambda x: '{0}Hz'.format(get_chan_freq1(x))
    if increment0 > 0:
        f0 = get_chan_qfreq0(100)
        f1 = get_chan_qfreq0(200)
    else:
        f0 = get_chan_qfreq0(200)
        f1 = get_chan_qfreq0(100)
    if increment1 > 0:
        f2 = get_chan_qfreq1(100)
        f3 = get_chan_qfreq1(200)
        f4 = get_chan_qfreq1(500)
        f5 = get_chan_qfreq1(700)
    else:
        f2 = get_chan_qfreq1(200)
        f3 = get_chan_qfreq1(100)
        f4 = get_chan_qfreq1(700)
        f5 = get_chan_qfreq1(500)

    test_cases = [
        # single global window (channel)
        [100, 200],
        # multiple global window (channel)
        [[100, 200], [500, 700]],
        # per spw windows (channel)
        {spwids[0]: [100, 200], spwids[-1]: [[100, 200], [500, 700]]},
        # single global window (frequency value)
        [get_chan_freq0(100), get_chan_freq0(200)],
        # multiple global window (frequency value)
        [[get_chan_freq0(100), get_chan_freq0(200)], [get_chan_freq0(500), get_chan_freq0(700)]],
        # per spw windows (frequency vaule)
        {spwids[0]: [get_chan_freq0(100), get_chan_freq0(200)],
         spwids[-1]: [[get_chan_freq1(100), get_chan_freq1(200)], [get_chan_freq1(500), get_chan_freq1(700)]]},
        # single global window (frequency quantity)
        [get_chan_qfreq0(100), get_chan_qfreq0(200)],
        # multiple global window (frequency quantity)
        [[get_chan_qfreq0(100), get_chan_qfreq0(200)], [get_chan_qfreq0(500), get_chan_qfreq0(700)]],
        # per spw windows (frequency quantity)
        {spwids[0]: [get_chan_qfreq0(100), get_chan_qfreq0(200)],
         spwids[-1]: [[get_chan_qfreq1(100), get_chan_qfreq1(200)], [get_chan_qfreq1(500), get_chan_qfreq1(700)]]},
        # per spw windows (string key)
        {str(spwids[0]): [get_chan_qfreq0(100), get_chan_qfreq0(200)],
         str(spwids[-1]): [[get_chan_qfreq1(100), get_chan_qfreq1(200)], [get_chan_qfreq1(500), get_chan_qfreq1(700)]]},
        # per spw windows (mixed)
        {spwids[0]: [100, 200],
         spwids[-1]: [[get_chan_qfreq1(100), get_chan_qfreq1(200)], [get_chan_qfreq1(500), get_chan_qfreq1(700)]]},
        # MS channel selection string (channel)
        '{0}:{1}~{2},{3}:{4}~{5};{6}~{7}'.format(spwids[0], 100, 200, spwids[-1], 100, 200, 500, 700),
        # MS channel selection string (frequency)
        '{0}:{1}~{2},{3}:{4}~{5};{6}~{7}'.format(spwids[0], f0, f1,
                                                 spwids[-1], f2, f3, f4, f5)
        ]

    results = []
    for window in test_cases:
        s = 'INPUT WINDOW: {0} (type {1})\n'.format(window, type(window))
        print(s)
        parser = LineWindowParser(ms, window)
        parser.parse(field_id)
        for spwid in spwids:
            parsed = parser.get_result(spwid)
            s += '\tSPW {0}: PARSED WINDOW = {1}\n'.format(spwid, parsed)
        results.append(s)

    print('=== TEST RESULTS ===')
    for s in results:
        print(s)
