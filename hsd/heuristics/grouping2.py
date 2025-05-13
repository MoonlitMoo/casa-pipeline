"""Set of heuristics for data grouping."""
from numbers import Real
from typing import Dict, List, NewType, Sequence, Tuple, Union

import numpy as np

import pipeline.infrastructure.api as api
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

Angle = NewType('Angle', Union[float, int, Dict])


class GroupByPosition2(api.Heuristic):
    """Grouping by RA/DEC position."""

    def calculate(self, ra: np.ndarray, dec: np.ndarray, r_combine: Angle, r_allowance: Angle) -> Tuple[Dict, List]:
        """Group data by RA/DEC position.

        Divides data into groups by their positions in two
        dimensional space which are given by ra and dec.
        Groups data within the circle with the radius of
        r_combine. Only position difference larger than
        r_allowance are regarded as significant.
        For r_combine and r_allowance, specified values are
        interpreted to be in degree unless their units are
        explicitly given.

        Args:
            ra:
              List of R.A.
            dec:
              List of DEC.
            r_combine:
              Data inside r_combine will be grouped together.
            r_allowance:
              Data inside r_allowance are assumed to be the same position.

        Returns:
            Two-tuple containing information on group membership
            (PosDict) and boundaries between groups (PosGap).

            PosDict is a dictionary whose keys are indices for
            ra and dec. Values of PosDict are the list which
            contains different value depending on whether the
            position specified by the index is reference data
            for the group or not. If k is reference index,
            PosDict[k] lists indices for group member
            ([ID1, ID2,..., IDN]). Otherwise, PosDict[k] is
            [-1, m] where m is the index to reference data.

            PosGap is a list of gaps in terms of array indices
            for ra and dec ([IDX1, IDX2,...,IDXN]). Length of
            PosGap is (number of groups) - 1.
        """
        qa = casa_tools.quanta
        if isinstance(r_combine, dict):
            # r_combine should be quantity
            CombineRadius = qa.convert(r_combine, 'deg')['value']
        else:
            CombineRadius = r_combine
        if isinstance(r_allowance, dict):
            # r_allowance should be quantity
            AllowanceRadius = qa.convert(r_allowance, 'deg')['value']
        else:
            AllowanceRadius = r_allowance

        ThresholdAR = AllowanceRadius * AllowanceRadius
        CombineDiameter = 2.0 * CombineRadius

        # 2009/2/10 Quicker Method
        Nrows = len(ra)
        PosDict = {}
        SelectDict = {}
        MinRA = ra.min()
        MinDEC = dec.min()
        # Calculate the lattice position (sRA, sDEC) for each pointings
        # Create list of pointings (dictionary) for each lattice position
        for x in range(Nrows):
            sRA = int((ra[x] - MinRA) / CombineDiameter)
            sDEC = int((dec[x] - MinDEC) / CombineDiameter)
            if sRA not in SelectDict:
                SelectDict[sRA] = {}
            if sDEC not in SelectDict[sRA]:
                SelectDict[sRA][sDEC] = [x]
            else:
                SelectDict[sRA][sDEC].append(x)

        # Create PosDict
        # make a list of spectra inside each lattice grid
        # store the list in PosDict[i] where 'i' is the smallest row number in the list
        # Other spectra have a reference to 'i'
        LOG.debug('SelectDict.keys() : %s' % list(SelectDict.keys()))
        for sRA in SelectDict:
            LOG.debug('len(SelectDict[%s].keys()) : %s' % (sRA, len(list(SelectDict[sRA].keys()))))
            for sDEC in SelectDict[sRA]:
                PosDict[SelectDict[sRA][sDEC][0]] = SelectDict[sRA][sDEC]
                if len(SelectDict[sRA][sDEC]) != 1:
                    for x in SelectDict[sRA][sDEC][1:]:
                        PosDict[x] = [-1, SelectDict[sRA][sDEC][0]]
        del SelectDict

        # Calculate thresholds to determine gaps
        DeltaP = np.sqrt((ra[1:] - ra[:-1])**2.0 + (dec[1:] - dec[:-1])**2.0)
        DeltaQ = np.take(DeltaP, np.nonzero(DeltaP > ThresholdAR)[0])
        if len(DeltaQ) != 0:
            ThresholdP = np.median(DeltaQ) * 10.0
        else:
            ThresholdP = 0.0
        LOG.info('threshold:%s deg' % ThresholdP)

        # List rows which distance from the previous position is larger than the threshold
        PosGap = []
        for x in range(1, Nrows):
            if DeltaP[x - 1] > ThresholdP:
                PosGap.append(x)
                LOG.info('Position Gap %s deg at row=%d' % (DeltaP[x-1], x))

        # Print information
        if len(PosGap) == 0:
            PosGapMsg = 'Found no position gap'
        else:
            PosGapMsg = 'Found %d position gap(s)' % len(PosGap)
        LOG.info(PosGapMsg)

        #print '\nPosGap', PosGap
        #print '\nPosDict', PosDict
        return PosDict, PosGap


class GroupByTime2(api.Heuristic):
    """Grouping by time sequence."""

    def calculate(self, timebase: Sequence[Real], time_diff: Sequence[Real]) -> Tuple[List, List]:
        """Group data by time sequence.

        Divides data into groups by their difference (time_diff).
        Two groups are defined based on "small" and "large" gaps,
        which are internally computed by ThresholdForGroupByTime
        heuristic. The time_diff is generated from timebase in
        most of the cases. The timebase contains all time stamps
        and time_diff is created from selected time stamps in
        other case.

        Args:
            timebase:
              base list of time stamps for threshold estimation
            time_diff:
              difference from the previous time stamp

        Returns:
            Two-tuple containing information on group membership
            (TimeTable) and boundaries between groups (TimeGap).

            TimeTable is the "list-of-list" whose items are the set
            of indices for each group. TimeTable[0] is the groups
            separaged by "small" gap while TimeTable[1] is for
            groups separated by "large" gap. They are used for
            baseline subtraction (hsd_baseline) and subsequent
            flagging (hsd_blflag).

            TimeTable:
                [[[ismall00,...,ismall0M],[...],...,[ismallX0,...,ismallXN]],
                 [[ilarge00,...,ilarge0P],[...],...,[ilargeY0,...,ilargeYQ]]]
            TimeTable[0]: separated by small gaps
            TimeTable[1]: separated by large gaps

            TimeGap is the list of indices which indicate boundaries
            for "small" and "large" gaps. These are used for plotting.

            TimeGap: [[rowX1, rowX2,...,rowXN], [rowY1, rowY2,...,rowYN]]
            TimeGap[0]: small gap
            TimeGap[1]: large gap
        """
        LOG.info('Grouping by Time...')

        # Threshold for grouping
        h = ThresholdForGroupByTime()
        (Threshold1, Threshold2) = h(timebase)

        TimeTable = [[], []]
        SubTable1 = [0]
        SubTable2 = [0]
        TimeGap = [[], []]

        # Detect small and large time gaps
        for index in range(len(time_diff)):
            indexp1 = index + 1
            if time_diff[index] <= Threshold1:
                SubTable1.append(indexp1)
            else:
                TimeTable[0].append(SubTable1)
                SubTable1 = [indexp1]
                TimeGap[0].append(indexp1)
                LOG.info('Small Time Gap: %s sec at row=%d' % (time_diff[index], indexp1))
            if time_diff[index] <= Threshold2:
                SubTable2.append(indexp1)
            else:
                TimeTable[1].append(SubTable2)
                SubTable2 = [indexp1]
                TimeGap[1].append(indexp1)
                LOG.info('Large Time Gap: %s sec at row=%d' % (time_diff[index], indexp1))

        if len(SubTable1) > 0: TimeTable[0].append(SubTable1)
        if len(SubTable2) > 0: TimeTable[1].append(SubTable2)
        del SubTable1, SubTable2

        # print information
        if len(TimeGap[0]) == 0:
            TimeGapMsg = 'Found no time gap'
            LOG.info(TimeGapMsg)
        else:
            TimeGapMsg1 = 'Found %d small time gap(s)' % len(TimeGap[0])
            TimeGapMsg2 = 'Found %d large time gap(s)' % len(TimeGap[1])
            LOG.info(TimeGapMsg1)
            LOG.info(TimeGapMsg2)

        #print '\nTimeGap', TimeGap
        #print '\nTimeTable', TimeTable
        return TimeTable, TimeGap
        # TimeGap is index
        # TimeTable[][0] is row, TimeTable[][1] is index


class ThresholdForGroupByTime(api.Heuristic):
    """Estimate thresholds for large and small time gaps."""

    def calculate(self, timebase: Sequence[Real]) -> Tuple[List, List]:
        """Estimate thresholds for large and small time gaps.

        Estimate thresholds for large and small time gaps using
        base list of time stamps. Threshold for small time gap,
        denoted as Threshold1, is computed from a median value
        of nonzero time differences multiplied by five, i.e.,

            dt = timebase[1:] - timebase[:-1]
            Threhold1 = 5 * np.median(dt[dt != 0])

        where timebase is assumed to be np.ndarray. Threshold
        for large time gap, denoted as Threshold2, is computed
        from a median value of time differences larger than
        Threshold1 mutiplied by five, i.e.,

            Threshold2 = 5 * np.median(
                dt[np.logical_and(dt != 0, dt > Threshold1)]
            )

        Args:
            timebase:
              base list of time stamps for threshold estimation

        Returns:
            Two-tuple of threshold values for small and large
            time gaps, respectively.
        """
        ArrayTime = np.array(timebase)

        # 2009/2/5 adapted for multi beam which assumes to have identical time stamps
        # identical time stamps are rejected before determining thresholds
        # DeltaT: difference from the previous time stamp
        DeltaT = ArrayTime[1:] - ArrayTime[:-1]
        DeltaT1 = np.take(DeltaT, np.nonzero(DeltaT)[0])
        Threshold1 = np.median(DeltaT1) * 5.0
        DeltaT2 = np.take(DeltaT1, np.nonzero(DeltaT1 > Threshold1)[0])
        if len(DeltaT2) > 0:
            Threshold2 = np.median(DeltaT2) * 5.0
        else:
            Threshold2 = Threshold1

        # print information
        LOG.info('Threshold1 = %s sec' % Threshold1)
        LOG.info('Threshold2 = %s sec' % Threshold2)
        LOG.info('MaxDeltaT = %s sec' % DeltaT1.max())
        LOG.info('MinDeltaT = %s sec' % DeltaT1.min())

        return Threshold1, Threshold2


class MergeGapTables2(api.Heuristic):
    """Merge time gap and position gaps."""

    def calculate(self, TimeGap: List, TimeTable: List, PosGap: List, tBEAM: Sequence[int]) -> Tuple[List, List]:
        """Merge time gap and position gaps.

        Merge time gap list (TimeGap) and position gap list (PosGap).
        TimeTable and TimeGap should be the first and the second
        elements of the return value of GroupByTime2 heuristic.
        Also, PosGap should be the second element of the return value
        of GroupByPosition2 heuristic. PosGap is merged into small
        TimeGap (TimeGap[0]).

        tBEAM is used to separate the data by beam for multi-beam
        data.

        Args:
            TimeTable:
              the first element of output from GroupByTime2 heuristic
            TimeGap:
              the second element of output from GroupByTime2 heuristic
            PosGap:
              the second element of output from GroupByPosition2()
            tBEAM:
              list of beam identifier.
        Returns:
            Two-tuple containing information on group membership
            (TimeTable) and boundaries between groups (TimeGap).

            TimeTable is the "list-of-list" whose items are the set
            of indices for each group. TimeTable[0] is the groups
            separaged by "small" gap while TimeTable[1] is for
            groups separated by "large" gap. They are used for
            baseline subtraction (hsd_baseline) and subsequent
            flagging (hsd_blflag).

            TimeTable:
                [[[ismall00,...,ismall0M],[...],...,[ismallX0,...,ismallXN]],
                 [[ilarge00,...,ilarge0P],[...],...,[ilargeY0,...,ilargeYQ]]]
            TimeTable[0]: separated by small gaps
            TimeTable[1]: separated by large gaps

            TimeGap is the list of indices which indicate boundaries
            for "small" and "large" gaps. The "small" gap is a merged
            list of gaps for groups separated by small time gaps and
            the ones grouped by positions. These are used for plotting.

            TimeGap: [[rowX1, rowX2,...,rowXN], [rowY1, rowY2,...,rowYN]]
            TimeGap[0]: small gap
            TimeGap[1]: large gap
        """
        LOG.info('Merging Position and Time Gap tables...')

        idxs = []
        for i in range(len(TimeTable[0])):
            idxs += TimeTable[0][i]
        IDX = list(np.sort(np.array(idxs)))
        tmpGap = list(np.sort(np.array(TimeGap[0] + PosGap)))
        NewGap = []
        if len(tmpGap) != 0:
            t = n = tmpGap[0]
            for n in tmpGap[1:]:
                if t != n and t in IDX:
                    NewGap.append(t)
                    t = n
            if n in IDX:
                NewGap.append(n)
        TimeGap[0] = NewGap

        SubTable1 = []
        TimeTable[0] = []
        for index in range(len(IDX)):
            n = IDX[index]
            if n in TimeGap[0]:
                TimeTable[0].append(SubTable1)
                SubTable1 = [n]
                LOG.info('Small Time Gap at row=%d' % n)
            else:
                SubTable1.append(n)
        if len(SubTable1) > 0:
            TimeTable[0].append(SubTable1)

        # 2009/2/6 Divide TimeTable in accordance with the Beam
        TimeTable2 = TimeTable[:]
        TimeTable = [[], []]
        for i in range(len(TimeTable2)):
            for index in range(len(TimeTable2[i])):
                #rows = TimeTable2[i][index][0]
                idxs = TimeTable2[i][index]
                BeamDict = {}
                for index2 in range(len(idxs)):
                    #row = rows[index2]
                    idx = idxs[index2]
                    if tBEAM[idx] in BeamDict:
                        #BeamDict[tBEAM[row]][0].append(row)
                        BeamDict[tBEAM[idx]].append(idx)
                    else:
                        BeamDict[tBEAM[idx]] = [idx]
                BeamList = list(BeamDict.values())
                for beam in BeamList:
                    TimeTable[i].append(beam)

        #print TimeTable[0]
        del BeamDict, BeamList, TimeTable2

        LOG.debug('TimeTable = %s' % (TimeTable))

        #print '\nTimeGap', TimeGap
        #print '\nTimeTable', TimeTable
        return TimeTable, TimeGap
