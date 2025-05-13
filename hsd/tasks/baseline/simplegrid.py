"""Task to perform simple two-dimensional gridding with "BOX" kernel."""
import collections
import functools
import os
from math import cos
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.domain.datatable import DataTableIndexer
from pipeline.infrastructure import casa_tools
from .. import common
from ..common import utils

from .typing import LineWindow

if TYPE_CHECKING:
    from pipeline.domain.measurementset import MeasurementSet
    from pipeline.domain.singledish import MSReductionGroupDesc, MSReductionGroupMember
    from pipeline.infrastructure.launcher import Context


LOG = infrastructure.get_logger(__name__)

NoData = common.NoData
DO_TEST = False


class SDSimpleGriddingInputs(vdp.StandardInputs):
    """Inputs class for simple gridding task."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    nplane = vdp.VisDependentProperty(default=3)

    @property
    def group_desc(self) -> 'MSReductionGroupDesc':
        """Return reduction group instance of the current group."""
        return self.context.observing_run.ms_reduction_group[self.group_id]

    @property
    def member_ms(self) -> List['MeasurementSet']:
        """Return a list of unitque MS domain objects of group members."""
        duplicated_ms_names = [ self.group_desc[i].ms.name for i in self.member_list ]
        return [ms for ms in self.context.observing_run.measurement_sets \
                if ms.name in duplicated_ms_names]

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
                 window: LineWindow,
                 windowmode: str,
                 nplane: Optional[int] = None) -> None:
        """Construct SDSimpleGriddingInputs instance.

        Args:
            context: Pipeline context
            group_id: Reduction group ID
            member_list: List of reduction group member IDs
            window: Manual line window
            windowmode: Line window mode. Either 'replace' or 'merge'
            nplane: Number of gridding planes. Defaults to 3 if None is given.
        """
        super(SDSimpleGriddingInputs, self).__init__()

        self.context = context
        self.group_id = group_id
        self.member_list = member_list
        self.window = window
        self.windowmode = windowmode
        self.nplane = nplane


class SDSimpleGriddingResults(common.SingleDishResults):
    """Results class to hold the result of simple gridding task."""

    def __init__(self,
                 task: Optional[Type[basetask.StandardTaskTemplate]] = None,
                 success: Optional[bool] = None,
                 outcome: Any = None) -> None:
        """Construct SDSimpleGriddingResults instance.

        Args:
            task: Task class that produced the result.
            success: Whether task execution is successful or not.
            outcome: Outcome of the task execution.
        """
        super(SDSimpleGriddingResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        No specific merge operation is done.

        Args:
            context: Pipeline context.
        """
        super(SDSimpleGriddingResults, self).merge_with_context(context)

    def _outcome_name(self) -> str:
        """Return string representing the outcome.

        Returns:
            Empty string
        """
        return ''


class SDSimpleGridding(basetask.StandardTaskTemplate):
    """Task to perform simple gridding.

    SDSimpleGridding task performs convolutional gridding on two-dimensional
    celestial coordinate with "BOX" gridding kernel. Approximate declination
    correction is applied.
    """

    Inputs = SDSimpleGriddingInputs

    def prepare(self,
                datatable_dict: dict,
                index_list: List[int]) -> SDSimpleGriddingResults:
        """Perform simple gridding.

        Args:
            datatable_dict: Dictionary holding datatable instance per MS.
            index_list: List of consecutive datatable row numbers.

        Returns:
            SDSimpleGriddingResults instance
        """
        window = self.inputs.window
        windowmode = self.inputs.windowmode
        LOG.debug('{}: window={}, windowmode={}'.format(self.__class__.__name__, window, windowmode))
        grid_table = self.make_grid_table(datatable_dict, index_list)
        # LOG.debug('work_dir=%s'%(work_dir))
        if windowmode == 'replace' and (window is None or len(window) > 0):
            # gridding should not be necessary
            LOG.info(f'Skip SimpleGridding: windowmode="{windowmode}", window="{window}"')
            retval = [None, None]
        else:
            import time
            start = time.time()
            retval = self.grid(grid_table=grid_table, datatable_dict=datatable_dict)
            end = time.time()
            LOG.debug('Elapsed time: %s sec', (end - start))

        outcome = {'spectral_data': retval[0],
                   'meta_data': retval[1],
                   'grid_table': grid_table}
        result = SDSimpleGriddingResults(task=self.__class__, success=True, outcome=outcome)

        return result

    def analyse(self, result: SDSimpleGriddingResults) -> SDSimpleGriddingResults:
        """Analyse results instance generated by prepare.

        Do nothing.

        Returns:
            SDSimpleGriddingResults instance
        """
        return result

    def make_grid_table(
        self, datatable_dict: dict, index_list: List[int]
    ) -> List[List[Union[int, float, numpy.ndarray]]]:
        """Create grid table.

        The method configures two-dimensional grid onto the celestial
        coordinate. Grid table holds spatial coordinate of each grid position
        as well as the meta data for gridding including list of data to be
        accumulated onto each grid position.

        Args:
            datatable_dict: Dictionary holding datatable instance per MS.
            index_list: List of consecutive datatable row numbers.

        Returns:
            Grid table. Format of the grid table is as follows.

            [
             [IF,POL,0,0,RAcent,DECcent,
              [[row0,r0,RMS0,index0,ant0],
               [row1,r1,RMS1,index1,ant1],
               ...,
               [rowN,rN,RMSN,indexN,antn]]],
             [IF,POL,0,1,RAcent,DECcent,
              [[row0,r0,RMS0,index0,ant0],
               [row1,r1,RMS1,index1,ant1],
               ...,
               [rowN,rN,RMSN,indexN,antn]]],
                        ......
             [IF,POL,M,N,RAcent,DECcent,
              [[row0,r0,RMS0,index0,ant0],
               [row1,r1,RMS1,index1,ant1],
               ...,
               [rowN,rN,RMSN,indexN,antn]]]
            ]

            where row0,row1,...,rowN should be combined to one for better S/N spectra
            while 'r' is a distance from grid position.

        """
        # Calculate Parameters for grid by RA/DEC positions
        reference_data = self.inputs.reference_member.ms
        reference_antenna = self.inputs.reference_member.antenna_id
        real_spw = self.inputs.reference_member.spw_id
        reference_spw = self.inputs.context.observing_run.real2virtual_spw_id(real_spw, reference_data)
        beam_size = reference_data.beam_sizes[reference_antenna][real_spw]
        grid_size = casa_tools.quanta.convert(beam_size, 'deg')['value']

        indexer = DataTableIndexer(self.inputs.context)

        def _g(colname):
            for i in index_list:
                origin_vis, j = indexer.serial2perms(i)
                datatable = datatable_dict[origin_vis]
                yield datatable.getcell(colname, j)

        ras = numpy.fromiter(_g('OFS_RA'), dtype=numpy.float64)
        decs = numpy.fromiter(_g('OFS_DEC'), dtype=numpy.float64)

        # Curvature has not been taken account
        dec_corr = 1.0 / cos(decs[0] / 180.0 * 3.141592653)
        grid_ra_corr = grid_size * dec_corr
        grid_dec = grid_size

        min_ra = ras.min()
        max_ra = ras.max()
        min_dec = decs.min()
        max_dec = decs.max()
        # Check if the distribution crosses over the RA=0
        if min_ra < 10 and max_ra > 350:
            ras = ras + numpy.less_equal(ras, 180) * 360.0
            min_ra = ras.min()
            max_ra = ras.max()
        LOG.info(' RA range: [%s, %s]', min_ra, max_ra)
        LOG.info('DEC range: [%s, %s]', min_dec, max_dec)
        ngrid_ra = int(int((max_ra - min_ra + grid_ra_corr) / (2.0 * grid_ra_corr)) * 2 + 1)
        ngrid_dec = int(int((max_dec - min_dec + grid_dec) / (2.0 * grid_dec)) * 2 + 1)
        min_ra = (min_ra + max_ra - ngrid_ra * grid_ra_corr) / 2.0
        min_dec = (min_dec + max_dec - ngrid_dec * grid_dec) / 2.0

        # Calculate Grid index for each position
        igrid_ra_corr = 1.0 / grid_ra_corr
        igrid_dec = 1.0 / grid_dec
        index_ra = numpy.array((ras - min_ra) * igrid_ra_corr, dtype=int)
        index_dec = numpy.array((decs - min_dec) * igrid_dec, dtype=int)

        # Counter for distributing spectrum into several planes (nplane)
        counter = numpy.zeros((ngrid_ra, ngrid_dec), dtype=int)

        # Make lists to store indexes for combine spectrum
        nplane = self.inputs.nplane
        combine_list = []
        for p in range(nplane):
            combine_list.append([])
            for x in range(ngrid_ra):
                combine_list[p].append([])
                for y in range(ngrid_dec):
                    combine_list[p][x].append([])

        # Store indexes
        index = 0
        for (ira, idec) in zip(index_ra, index_dec):
            combine_list[counter[ira][idec] % nplane][ira][idec].append(index)
            counter[ira][idec] += 1
            index += 1
        del index, index_ra, index_dec, counter

        # Create grid_table for output
        grid_table = []
        # vIF, vPOL: dummy (not necessary)
        vIF = reference_spw
        vPOL = 0

        msid_list = {}
        for i, ms in enumerate(self.inputs.context.observing_run.measurement_sets):
            msid_list[ms.basename] = i
        for y in range(ngrid_dec):
            DEC = min_dec + grid_dec * (y + 0.5)
            for x in range(ngrid_ra):
                RA = min_ra + grid_ra_corr * (x + 0.5)
                for p in range(nplane):
                    line = [vIF, vPOL, x, y, RA, DEC, []]
                    for index in combine_list[p][x][y]:
                        # math.sqrt is twice as fast as ** 0.5 according to
                        # the measurement on alma-cluster-proto03 in NAOJ
                        # (3.5GHz CPU 8 Cores, 16GB RAM).
                        # Furthermore, direct import of sqrt from math is
                        # slightly (~10%) faster than calling sqrt using
                        # 'math.sqrt'.
                        # Also, x * x is ~30% faster than x ** 2.0.
                        # Delta = (((ras[index] - RA) * dec_corr) ** 2.0 + \
                        #         (decs[index] - DEC) ** 2.0) ** 0.5
                        #Delta = sqrt((ras[index] - RA) * (ras[index] - RA)
                        #             * dec_corr * dec_corr
                        #             + (decs[index] - DEC) * (decs[index] - DEC))
                        _index = index_list[index]
                        origin_vis, datatable_index = indexer.serial2perms(_index)
                        datatable = datatable_dict[origin_vis]
                        row = datatable.getcell('ROW', datatable_index)
                        #stat = datatable.getcell('STATISTICS', datatable_index)[0]
                        ant = datatable.getcell('ANTENNA', datatable_index)
                        msid = msid_list[origin_vis]
                        line[6].append([row, None, None, datatable_index, ant, msid])
                    line[6] = numpy.array(line[6])
                    grid_table.append(line)
        del ras, decs, combine_list

        LOG.info('ngrid_ra = %s  ngrid_dec = %s', ngrid_ra, ngrid_dec)
        return grid_table

    def grid(self,
             grid_table: List[List[Union[int, float, numpy.ndarray]]],
             datatable_dict: dict) -> Tuple[numpy.ndarray, List[List[Union[int, float]]]]:
        """Perform gridding operation according to grid_table.

        The process does re-map and combine spectrum for each position.
        Number of spectra output is len(grid_table)

        Args:
            grid_table: Grid table generated by make_grid_table
            datatable_dict: Dictionary holding datatable instance per MS.

        Returns:
            Tuple of spectral data after gridding and the table that stores
            number of accumulated/flagged spectra as well as resulting RMS
            per grid position. The table structure is as follows.

            [
             [IF, POL, X, Y, RA, DEC, # of Combined Sp., # of flagged Sp., RMS]
             [IF, POL, X, Y, RA, DEC, # of Combined Sp., # of flagged Sp., RMS]
                     ......
             [IF, POL, X, Y, RA, DEC, # of Combined Sp., # of flagged Sp., RMS]
            ]

            Note that IF above is the virtual spw id that should be translated
            into real spw id when applying to measurementset domain object.
        """
        nrow = len(grid_table)
        LOG.info('SimpleGrid: Processing %s spectra...', nrow)

        reference_data = self.inputs.reference_member.ms
        real_spw = self.inputs.reference_member.spw_id
        nchan = reference_data.spectral_windows[real_spw].num_channels
        npol = reference_data.get_data_description(spw=real_spw).num_polarizations
        LOG.debug('nrow=%s nchan=%s npol=%s', nrow, nchan, npol)

        # loop for all ROWs in grid_table to make dictionary that
        # associates spectra in data_in and weights with grids.
        bind_to_grid = collections.defaultdict(list)
        vislist = [x.basename for x in self.inputs.context.observing_run.measurement_sets]
        for grid_table_row in range(nrow):
            [IF, POL, X, Y, RAcent, DECcent, RowDelta] = grid_table[grid_table_row]
            for [_data_row, _, _, _index, _ant, _msid] in RowDelta:
                index = int(_index)
                msid = int(_msid)
                vis = vislist[msid]
                datatable = datatable_dict[vis]
                ms = self.inputs.context.observing_run.measurement_sets[msid]
                data_row = int(_data_row)
                tSFLAG = datatable.getcell('FLAG_SUMMARY', index)
                tTSYS = datatable.getcell('TSYS', index)
                tEXPT = datatable.getcell('EXPOSURE', index)
                pols = []
                flags = []
                weights = []
                for (pol, flag_summary) in enumerate(tSFLAG):
                    if flag_summary == 1:
                        if tTSYS[pol] > 0.5 and tEXPT > 0.0:
                            Weight = tEXPT / (tTSYS[pol] ** 2.0)
                        else:
                            Weight = 1.0
                        pols.append(pol)
                        flags.append(flag_summary)
                        weights.append(Weight)
                bind_to_grid[ms.basename].append([data_row, grid_table_row, pols, weights, flags])
        LOG.debug('bind_to_grid.keys() = %s', [x for x in bind_to_grid])
        LOG.debug('bind_to_grid=%s', bind_to_grid)

        def cmp(x, y):
            if x[0] < y[0]:
                return -1
            elif x[0] > y[0]:
                return 1
            elif x[1] < y[1]:
                return -1
            elif x[1] > y[1]:
                return 1
            return 0
        keyfunc = functools.cmp_to_key(cmp)
        for k, v in bind_to_grid.items():
            v.sort(key=keyfunc)
        LOG.debug('sorted bind_to_grid=%s', bind_to_grid)

        # create storage for output
        StorageOut = numpy.zeros((nrow, nchan), dtype=complex)
        StorageWeight = numpy.zeros((nrow, nchan), dtype=numpy.float32)
        StorageNumSp = numpy.zeros((nrow), dtype=int)
        StorageNumFlag = numpy.zeros((nrow), dtype=int)
        OutputTable = []

        # Return empty result if all the spectra are flagged out
        number_of_spectra = sum(map(len, bind_to_grid.values()))
        if number_of_spectra == 0:
            LOG.warning('Empty grid table, maybe all the data are flagged out in the previous step.')
            return ([], [])

        # Create progress timer
        Timer = common.ProgressTimer(80, sum(map(len, bind_to_grid.values())), LOG.level)

        # Obtain spectrum and FLAG from Baselined MS (if exists) or member MS (calibrated)
        bl_mses = self.inputs.context.observing_run.get_measurement_sets_of_type([DataType.BASELINED])
        for in_ms in self.inputs.member_ms:
            origin_ms = self.inputs.context.observing_run.get_ms(in_ms.origin_ms)
            entries = bind_to_grid[origin_ms.basename]
            bl_ms = utils.match_origin_ms(bl_mses, in_ms.origin_ms)
            grid_ms = bl_ms if bl_ms is not None else in_ms
            vis = grid_ms.name
            ms_colname = utils.get_datacolumn_name(vis)
            rowmap = utils.make_row_map_between_ms(origin_ms, vis)
            LOG.debug('Start reading data from "%s"', os.path.basename(vis))
            LOG.debug('There are %s entries', len(entries))
            with casa_tools.TableReader(vis) as tb:
                for entry in entries:
                    [tROW, ROW, pols, weights, flags] = entry
                    Sp = None
                    Mask = None
                    # map row ID in origin MS and grid_ms
                    mapped_row = rowmap[tROW]
                    LOG.debug('tROW %s: mapped_row %s', tROW, mapped_row)
                    for (Weight, Pol, SFLAG) in zip(weights, pols, flags):
                        if SFLAG == 1:
                            if Sp is None:
                                Sp = tb.getcell(ms_colname, mapped_row)
                            if not numpy.all(numpy.isfinite(Sp[Pol])):
                                LOG.debug('vis "%s" row %s pol %s contains NaN or Inf', os.path.basename(vis), tROW, Pol)
                                Sp[Pol, :] = numpy.where(numpy.isfinite(Sp[Pol]), Sp[Pol], 0)
                            if Mask is None:
                                Mask = numpy.asarray(numpy.logical_not(tb.getcell('FLAG', mapped_row)),
                                                     dtype=int)#vquery(tb.getcell('FLAG', mapped_row) == False)
                            StorageOut[ROW] += (Sp[Pol] * Mask[Pol] * Weight)
                            StorageWeight[ROW] += (Mask[Pol] * Weight)
                            StorageNumSp[ROW] += 1 if numpy.any(Mask[Pol] == 1) else 0#query(any(Mask[Pol] == 1))
                        else:
                            StorageNumFlag[ROW] += 1
                    Timer.count()
            LOG.debug('DONE')

        # Calculate Tsys-ExpTime weighted average
        # RMS = n * Tsys/sqrt(Exptime)
        # Weight = 1/(RMS**2) = (Exptime/(Tsys**2))
        for ROW in range(nrow):
            LOG.debug('Calculate weighed average for row %s', ROW)
            [IF, POL, X, Y, RAcent, DECcent, RowDelta] = grid_table[ROW]
            if StorageNumSp[ROW] == 0 or numpy.all(StorageWeight[ROW] == 0.0):
                StorageOut[ROW,:] = NoData
                RMS = 0.0
            else:
                for ichan in range(nchan):
                    if StorageWeight[ROW, ichan] == 0.0:
                        StorageOut[ROW, ichan] = NoData
                    else:
                        StorageOut[ROW, ichan] /= StorageWeight[ROW, ichan]
                RMS = 1.0
            OutputTable.append([IF, POL, X, Y, RAcent, DECcent, StorageNumSp[ROW], StorageNumFlag[ROW], RMS])

        del StorageWeight, StorageNumSp, StorageNumFlag, Timer
        return (numpy.real(StorageOut), OutputTable)
