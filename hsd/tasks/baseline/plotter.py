"""Plotter for baseline subtraction result."""
import collections
import itertools
import os
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, Union

import matplotlib.figure as figure
import matplotlib.pyplot as plt
import numpy
from numpy.ma.core import MaskedArray

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common import atmutil
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from ..common import utils
from ..common import compress
from ..common import display
from ..common.display import DPIDetail, ch_to_freq, sd_polmap
from ..common import direction_utils as dirutil

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.domain.datatable import DataTableImpl as DataTable
    from pipeline.domain.measurementset import MeasurementSet

LOG = infrastructure.get_logger(__name__)

# A named tuple to store statistics of baseline quality
BinnedStat = collections.namedtuple(
    'BinnedStat',
    'bin_min_ratio bin_max_ratio bin_diff_ratio nbin nbin_factor valid'
)
QualityStat = collections.namedtuple('QualityStat', 'vis field spw ant pol stat')


class PlotterPool(object):
    """Pool class to hold resources for plotting.

    TODO: this class is not useful. Should be removed in future.
          create_plotter must be separated from the class.
    """

    def __init__(self) -> None:
        """Construct PlotterPool instance."""
        self.pool = {}

    def create_plotter(self,
                       num_ra: int,
                       num_dec: int,
                       ralist: List[float],
                       declist: List[float],
                       direction_reference: Optional[str] = None,
                       brightnessunit: str = 'Jy/beam') -> display.SDSparseMapPlotter:
        """Create plotter instance.

        Args:
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            ralist: List of RA values for labeling
            declist: List of Dec values for labeling
            direction_reference: Directon reference string. Defaults to None.
            brightnessunit: Brightness unit string. Defaults to 'Jy/beam'.

        Returns:
            Plotter instance
        """
        fig = figure.Figure()
        plotter = display.SDSparseMapPlotter(fig, nh=num_ra, nv=num_dec,
                                             step=1, brightnessunit=brightnessunit)
        plotter.direction_reference = direction_reference
        plotter.setup_labels_absolute(ralist, declist)
        return plotter

    def done(self) -> None:
        """Close plotters registered to the pool."""
        for plotter in self.pool.values():
            plotter.done()


class PlotDataStorage(object):
    """Storage class to hold array data for plotting."""

    def __init__(self) -> None:
        """Construct PlotDataStorage instance.

        This class holds three sets of data storage, one dimensional
        numpy array with a certain length, and the data array that refers
        whole or part of data storage. They are assigned to map data (float),
        associated mask (bool), and integrated data (float), respectively.

        Each data array is multi-dimensional array with (npol, nchan) for
        integrated data while (nh, nv, npol, nchan) for other arrays.
        Each data storage is one-dimensional array whose length should be
        larger than the total number of elements for associated data array.

        Intension is to minimize the number of memory allocation/de-allocation.
        To do that, data storage (memory for array) and data array (actual array
        accessed by the user) are separated so that allocated memory is reused
        if possible. Data storage is resized only when size of data storage is
        smaller than the size of data array which can vary upon request from
        the user.

        The constructor initializes data storage as zero-length array, which
        effectively means no memory allocation for the data array, and data
        array nominally refers to empty data storage. Actual memory allocation
        will be performed when the user first provides the array shape.
        """
        self.map_data_storage = numpy.zeros((0), dtype=float)
        self.integrated_data_storage = numpy.zeros((0), dtype=float)
        self.map_mask_storage = numpy.zeros((0), dtype=bool)
        self.integrated_mask_storage = numpy.zeros((0), dtype=bool)
        self.map_data = self.map_data_storage
        self.integrated_data = self.integrated_data_storage
        self.map_mask = self.map_mask_storage
        self.integrated_mask = self.integrated_mask_storage

    def resize_storage(self, num_ra: int, num_dec: int, num_pol: int, num_chan: int) -> None:
        """Resize storage.

        Resize storage array if necessary, i.e., only when current size is less than
        requested size calculated from input args. Data array refers storage but is
        reshaped to match the number of panels of sparse profile map.

        Args:
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            num_pol: Number of polarizations
            num_chan: Number of spectral channels
        """
        num_integrated = num_pol * num_chan
        num_map = num_ra * num_dec * num_integrated
        if len(self.map_data_storage) < num_map:
            self.map_data_storage = numpy.resize(self.map_data_storage, num_map)
        self.map_data = numpy.reshape(self.map_data_storage[:num_map], (num_ra, num_dec, num_pol, num_chan))

        if len(self.map_mask_storage) < num_map:
            self.map_mask_storage = numpy.resize(self.map_mask_storage, num_map)
        self.map_mask = numpy.reshape(self.map_mask_storage[:num_map], (num_ra, num_dec, num_pol, num_chan))

        if len(self.integrated_data_storage) < num_integrated:
            self.integrated_data_storage = numpy.resize(self.integrated_data_storage, num_integrated)
        self.integrated_data = numpy.reshape(self.integrated_data_storage[:num_integrated], (num_pol, num_chan))


class BaselineSubtractionDataManager(object):
    """Class to produce data of baseline subtraction plot and quality statistics."""

    def __init__(self, ms: 'MeasurementSet', blvis: str, context: 'Context', datatable: 'DataTable') -> None:
        """Construct BaselineSubtractionDataManager instance.

        Args:
            ms: Domain object for the MS before baseline subtraction
            blvis: Name of the MS after baseline subtraction
            context: Pipeline context
            datatable: DataTable instance
        """

        self.ms = ms
        self.context = context
        self.prefit_data = ms.name
        self.postfit_data = blvis
        self.datatable = datatable
        self.prefit_storage = PlotDataStorage()
        self.postfit_storage = PlotDataStorage()
        self.prefit_integrated_data = None
        self.prefit_map_data = None
        self.postfit_integrated_data = None
        self.postfit_map_data = None
        self.prefit_averaged_data = None

    def resize_storage(self, num_ra: int, num_dec: int, num_pol: int, num_chan: int) -> None:
        """Resize storage as necessary.

        Args:
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            num_pol: Number of polarizations
            num_chan: Number of spectral channels
        """
        self.prefit_storage.resize_storage(num_ra, num_dec, num_pol, num_chan)
        self.postfit_storage.resize_storage(num_ra, num_dec, num_pol, num_chan)

    def store_result_get_data(self,
                              num_ra: int,
                              num_dec: int,
                              rowlist: List[Dict[str, Union[int, float, List[int]]]],
                              npol: int,
                              nchan: int,
                              out_rowmap: Optional[dict] = None,
                              in_rowmap: Optional[dict] = None
                              ) -> Tuple[numpy.ma.masked_array,
                                         numpy.ma.masked_array,
                                         Optional[numpy.ma.masked_array],
                                         Optional[numpy.ma.masked_array],
                                         Optional[numpy.ma.masked_array]]:
        """
        Args:
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            rowlist: List of datatable row ids per sparse map panel with metadata
            npol: Number of polarizations
            nchan: Number of spectral channels
            out_rowmap: Row mapping between original (calibrated) MS and the MS
                        after baseline subtraction.
            in_rowmap: Row mapping between original (calibrated) MS and the MS
                       before baseline subtraction.

        Returns:
            Tuple containing five masked arrays; postfit integrated spectrum data,
            postfit sparse map data, prefit integrated spectrum data, prefit sparse map
            data, and prefit averaged sparse map data.
            Each element of postfit or prefit integrated spectrum data: (npol, nchan).
            Each element of postfit, prefit sparse map data or prefit averaged sparse map
            data: (nx, ny, npol, nchan).
        """

        dtrows = self.datatable.getcol('ROW')

        # get prefit data
        if self.prefit_integrated_data is None and not basetask.DISABLE_WEBLOG:
            self.prefit_integrated_data, self.prefit_map_data, self.prefit_averaged_data \
                = self.get_data(self.prefit_data, dtrows, num_ra, num_dec, nchan, npol,
                                rowlist, rowmap=in_rowmap,
                                integrated_data_storage=self.prefit_storage.integrated_data,
                                map_data_storage=self.prefit_storage.map_data,
                                map_mask_storage=self.prefit_storage.map_mask,
                                produce_averaged_data=True )

        # get postfit data
        if self.postfit_integrated_data is None:
            self.postfit_integrated_data, self.postfit_map_data \
                = self.get_data(self.postfit_data, dtrows, num_ra, num_dec, nchan, npol,
                                rowlist, rowmap=out_rowmap,
                                integrated_data_storage=self.postfit_storage.integrated_data,
                                map_data_storage=self.postfit_storage.map_data,
                                map_mask_storage=self.postfit_storage.map_mask,
                                produce_averaged_data=False )

        return self.postfit_integrated_data, self.postfit_map_data, self.prefit_integrated_data, self.prefit_map_data, self.prefit_averaged_data

    def analyze_plot_table(
        self,
        origin_ms_id: int,
        antid: int,
        virtual_spwid: int,
        polids: List[int],
        grid_table: List[List[Union[int, float, numpy.ndarray]]],
        org_direction: dirutil.Direction
    ) -> Tuple[int, int, int, List[Dict[str, Union[int, float, List[int]]]]]:
        """Create and analyze plot table.

        Extract datatable row ids that match the selection given by
        MS id, antenna id, (virtual) spw id, and polarization ids, and
        associate them with the sparse map panels.

        Args:
            origin_ms_id: MS id for selection
            antid: Antenna id for selection
            virtual_spwid: Virtual spw id for selection
            polids: List of polarization ids for selection
            grid_table: grid_table created by simplegrid module
            org_direction: direction of the origin

        Returns:
            Position information and associated datatable row ids
            for each sparse map positions
        """
        # plot table is separated into two parts: meta data part and row list part
        # plot_table layout: [RA_ID, DEC_ID, RA_DIR, DEC_DIR]
        # [[0, 0, RA0, DEC0], <- plane 0
        #  [0, 0, RA0, DEC0], <- plane 1
        #  [0, 0, RA0, DEC0], <- plane 2
        #  [1, 0, RA1, DEC0], <- plane 0
        #   ...
        #  [M, 0, RAM, DEC0], <- plane 2
        #  [0, 1, RA0, DEC1], <- plane 0
        #  ...
        #  [M, N, RAM, DECN]] <- plane 2

        plot_table = BaselineSubtractionPlotManager.generate_plot_meta_table(virtual_spwid,
                                                                             polids,
                                                                             grid_table)
        num_grid_rows = len(plot_table)  # num_plane * num_grid_ra * num_grid_dec
        assert num_grid_rows > 0
        num_grid_dec = plot_table[-1][1] + 1
        num_grid_ra = plot_table[-1][0] + 1
        num_plane = num_grid_rows // (num_grid_dec * num_grid_ra)
        LOG.debug('num_grid_ra=%a, num_grid_dec=%s, num_plane=%s, num_grid_rows=%s',
                  num_grid_ra, num_grid_dec, num_plane, num_grid_rows)
        xpanel, ypanel = configure_1d_panel(num_grid_ra, num_grid_dec)
        num_ra = len(xpanel)
        num_dec = len(ypanel)
        each_grid = configure_2d_panel(xpanel, ypanel, num_grid_ra, num_plane)
        rowlist = [{} for i in range(num_dec * num_ra)]

        for row_index, each_plane in enumerate(each_grid):
            dataids = BaselineSubtractionPlotManager.generate_plot_rowlist(origin_ms_id,
                                                                           antid,
                                                                           virtual_spwid,
                                                                           polids,
                                                                           grid_table,
                                                                           plot_table,
                                                                           each_plane)

            raid = row_index % num_ra
            decid = row_index // num_ra
            ralist = [plot_table[i][2] for i in each_plane]
            declist = [plot_table[i][3] for i in each_plane]
            ra = numpy.mean(ralist)
            dec = numpy.mean(declist)
            if org_direction is not None:
                ra, dec = dirutil.direction_recover(ra, dec, org_direction)

            rowlist[row_index].update(
                    {"RAID": raid, "DECID": decid, "RA": ra, "DEC": dec,
                     "IDS": dataids})
            LOG.trace('RA %s DEC %s: dataids=%s',
                      raid, decid, dataids)

        return num_ra, num_dec, num_plane, rowlist

    def get_data(
        self,
        infile: str,
        dtrows: numpy.ndarray,
        num_ra: int,
        num_dec: int,
        num_chan: int,
        num_pol: int,
        rowlist: List[Dict[str, Union[int, float, List[int]]]],
        rowmap: Optional[dict] = None,
        integrated_data_storage: Optional[numpy.ndarray] = None,
        map_data_storage: Optional[numpy.ndarray] = None,
        map_mask_storage: Optional[numpy.ndarray] = None,
        produce_averaged_data: bool = False
    ) -> Tuple[numpy.ma.masked_array, ...]:
        """Create array data for sparse map.

        Computes the following masked array data for sparse map:
        1. The integrated spectrum averaged over entire spectral data
           regardless of their spatial position, which is displayed in
           the top panel of the figure.
        2. The 'representative' spectrum for each panel of the sparse map.
           rowlist holds the Spatial grouping information of the panels.
        3. Averaged spectra for each panel of the sparse map
           rowlist holds the Spatial grouping information of the panels.
           Returned only when produce_averaged_data is specified as True.

        Args:
            infile: Name of the MS
            dtrows: List of datatable rows
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            num_chan: Number of spectral channels
            num_pol: Number of polarizaionts
            rowlist: List of datatable row ids per sparse map panel with metadata
            rowmap: Row mapping between original (calibrated) MS and the MS
                    specified by infile. It will be EchoDictionary if None is given.
            integrated_data_storage: Storage for integrated spectrum. If None is given,
                                     new array is created.
            map_data_storage: Storage for sparse map. If None is given, new array is created.
            map_mask_storage: Storage for sparse map mask. If None is given, new array is created.
            produce_averaged_data: Whether to produce averaged data for each panel. Defaut is False.

        Returns:
            Integrated spectrum averaged over entire spectral data
            Representative spectrum for each panel of the sparse map
            Averaged spectra for each panel of the sparse map (only if produce_averaged_data is True)
        """
        # default rowmap is EchoDictionary
        if rowmap is None:
            rowmap = utils.EchoDictionary()

        integrated_shape = (num_pol, num_chan)
        map_shape = (num_ra, num_dec, num_pol, num_chan)
        if integrated_data_storage is not None:
            assert integrated_data_storage.shape == integrated_shape
            assert integrated_data_storage.dtype == float
            integrated_data = integrated_data_storage
            integrated_data[:] = 0.0
        else:
            integrated_data = numpy.zeros((num_pol, num_chan), dtype=float)

        num_integrated = numpy.zeros((num_pol, num_chan), dtype=int)

        if map_data_storage is not None:
            assert map_data_storage.shape == map_shape
            assert map_data_storage.dtype == float
            map_data = map_data_storage
            map_data[:] = display.NoDataThreshold
        else:
            map_data = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=float) + display.NoDataThreshold
        if map_mask_storage is not None:
            assert map_mask_storage.shape == map_shape
            assert map_mask_storage.dtype == bool
            map_mask = map_mask_storage
            map_mask[:] = False
        else:
            map_mask = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=bool)

        if produce_averaged_data:
            num_to_average = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=int)
            map_data_to_average = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=float)
            map_mask_to_average = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=bool)

        # go through infile and pack the data
        with casa_tools.TableReader(infile) as tb:
            # column name for spectral data
            colnames = ['CORRECTED_DATA', 'DATA', 'FLOAT_DATA']
            colname = None
            for name in colnames:
                if name in tb.colnames():
                    colname = name
                    break
            assert colname is not None

            # loop for 'panels' of profile map
            for d in rowlist:
                midxperpol = []
                ix = num_ra - 1 - d['RAID']
                iy = d['DECID']
                if len( d['IDS'] ) > 0:
                    # create index table
                    index_list = []
                    for ids_idx, dt_id in enumerate( d['IDS'] ):
                        index_list.append(
                            {
                                'ids_idx'     : ids_idx,                 # index within the 'IDS'
                                'datatable_id': dt_id,                   # datatable id
                                'orig_row'    : dtrows[dt_id],           # row of original MS
                                'mapped_row'  : rowmap[ dtrows[dt_id] ], # row of the MS for baseline subtraction
                                'valid'       : [None] * num_pol         # valid flag for each pol
                            }                                            #  --True if not fully flagged
                        )

                    # fill the 'valid' column of the index table, and counts for averaging/integrating.
                    # sort the index_list with orig_row, to avoid jumping back and forth to access the infile.
                    for this_index in sort_with_key( index_list, 'orig_row' ):

                        # get the data and mask
                        mapped_row = this_index['mapped_row']
                        this_data = tb.getcell(colname, mapped_row)
                        this_mask = tb.getcell('FLAG', mapped_row)

                        # mark each rows whether it is fully flagged for each pol
                        for ipol in range(num_pol):
                            allflagged = numpy.all( this_mask[ipol] == True )
                            this_index['valid'][ipol] = not allflagged

                        # used later to calculate integrated/averaged data
                        binary_mask = numpy.asarray(numpy.logical_not(this_mask), dtype=int)
                        integrated_data += this_data.real * binary_mask
                        num_integrated += binary_mask
                        if produce_averaged_data:
                            map_data_to_average[ix, iy] += this_data.real * binary_mask
                            num_to_average[ix, iy] += binary_mask

                    # pick one 'representative' row for each ipol, and prepare storage data
                    # the 'representative' row is given by the 'median index' (i.e. ids_idx to the median of selected ids)
                    for ipol in range(num_pol):
                        # pick index of 'valid' (not fully flagged) rows for each pol
                        valid_index_list = [ r for r in index_list if r['valid'][ipol] ]

                        if len( valid_index_list ) > 0:
                            # get the row number (mapped_row) corresponding to the 'median index'
                            sorted_valid_index_list = sort_with_key( valid_index_list, 'datatable_id' )

                            # patch to get similar behaviors with those before PIPE-2064
                            choice = 0 if len(sorted_valid_index_list) < 3 else len(sorted_valid_index_list) // 2

                            median_index = sorted_valid_index_list[ choice ]
                            mapped_row = median_index['mapped_row']

                            # get data and mask for mapped_row, and prepare for storage data
                            this_data = tb.getcell(colname, mapped_row)
                            this_mask = tb.getcell('FLAG', mapped_row)
                            map_data[ix, iy, ipol] = this_data[ipol].real
                            map_mask[ix, iy, ipol] = this_mask[ipol]

                            # save the 'median index' with its ids_idx : to be used in get_lines() later
                            midxperpol.append( median_index['ids_idx'] )
                        else:
                            midxperpol.append(None)
                else:
                    LOG.debug('no data is available for (%s,%s)', ix, iy)
                    midxperpol = [None for ipol in range(num_pol)]
                # push median_index into the specific component of rowlist
                d['MEDIAN_INDEX'] = midxperpol
                LOG.debug('MEDIAN_INDEX for %s, %s is %s', ix, iy, midxperpol)

        # calculate integrated data
        integrated_data_masked = numpy.ma.masked_array(integrated_data, num_integrated == 0)
        integrated_data_masked /= num_integrated
        map_data_masked = numpy.ma.masked_array(map_data, map_mask)

        if produce_averaged_data:
            # calculate averaged data for each panel
            map_mask_to_average[:] = num_to_average == 0
            map_data_to_average[map_mask_to_average] = display.NoDataThreshold
            map_data_averaged_masked = numpy.ma.masked_array(map_data_to_average, map_mask_to_average)
            map_data_averaged_masked /= num_to_average
            return integrated_data_masked, map_data_masked, map_data_averaged_masked

        return integrated_data_masked, map_data_masked


class BaselineSubtractionPlotManager(BaselineSubtractionDataManager):
    """Manages any operation necessary to produce baseline subtraction plot."""

    def __init__(self, ms: 'MeasurementSet', blvis: str, context: 'Context', datatable: 'DataTable') -> None:
        """Construct BaselineSubtractionPlotManager instance.

        Args:
            ms: Domain object for the MS before baseline subtraction
            blvis: Name of the MS after baseline subtraction
            context: Pipeline context
            datatable: DataTable instance
        """
        super().__init__(ms, blvis, context, datatable)

        stage_number = self.context.task_counter
        self.stage_dir = os.path.join(self.context.report_dir, "stage%d" % stage_number)

        if basetask.DISABLE_WEBLOG:
            self.pool = None
        else:
            if not os.path.exists(self.stage_dir):
                os.makedirs(self.stage_dir, exist_ok=True)   # handle race condition in Tier-0 operation gracefully
            self.pool = PlotterPool()

    def finalize(self) -> None:
        """Finalize plot manager.

        Release resources allocated for plotting.
        """
        if self.pool is not None:
            self.pool.done()

    @staticmethod
    def _generate_plot_meta_table(
        spw_id: int,
        polarization_ids: List[int],
        grid_table: List[List[Union[int, float, numpy.ndarray]]]
    ) -> Generator[List[Union[int, float]], None, None]:
        """Extract necessary data from grid_table.

        Rows of grid_table are filtered by spw and polarization ids,
        and only spatial location of the grid are extracted.

        Args:
            spw_id: spw id for filtering
            polarization_ids: polarization ids for filtering
            grid_table: grid_table generated by simplegrid module

        Yields:
            List of spatial position information (pixel and world)
        """
        for row in grid_table:
            if row[0] == spw_id and row[1] in polarization_ids:
                new_row_entry = row[2:6]
                yield new_row_entry

    @staticmethod
    def generate_plot_meta_table(
        spw_id: int,
        polarization_ids: List[int],
        grid_table: List[List[Union[int, float, numpy.ndarray]]]
    ) -> List[List[Union[int, float]]]:
        """Return metadata table for plotting.

        Metadata table for given spw and polarization ids contains
        spatial position information in both pixel and world coordinates.

        Args:
            spw_id: spw id for filtering
            polarization_ids: polarization ids for filtering
            grid_table: grid_table generated by simplegrid module

        Returns:
            Metadata table (plot_table). The table is intended to be used jointly
            with the return value of generate_plot_rowlist.
            plot_table layout: [RA_ID, DEC_ID, RA_DIR, DEC_DIR]

                [[0, 0, RA0, DEC0], <- plane 0
                 [0, 0, RA0, DEC0], <- plane 1
                 [0, 0, RA0, DEC0], <- plane 2
                 [1, 0, RA1, DEC0], <- plane 0
                  ...
                 [M, 0, RAM, DEC0], <- plane 2
                 [0, 1, RA0, DEC1], <- plane 0
                 ...
                 [M, N, RAM, DECN]] <- plane 2
        """
        new_table = list(BaselineSubtractionPlotManager._generate_plot_meta_table(spw_id,
                                                                                  polarization_ids,
                                                                                  grid_table))
        return new_table

    @staticmethod
    def _generate_plot_rowlist(
        origin_ms_id: int,
        antenna_id: int,
        spw_id: int,
        polarization_ids: List[int],
        grid_table: List[List[Union[int, float, numpy.ndarray]]],
        grid_list: List[Tuple[int, int]]
    ) -> Generator[numpy.ndarray, None, None]:
        """Yield list of datatable row ids that match selection.

        Extract list of datatable row ids that match the selection for
        MS, spw, polarization, and spatial grid location. Yield row
        ids as numpy array.

        Args:
            origin_ms_id: MS id for selection
            antenna_id: Antenna id for selection
            spw_id: Spw id for selection
            polarization_ids: List of polarization ids
            grid_table: grid_table generated by simplegrid module
            grid_list: Spatial grid indices (ix, iy) for selection

        Yields:
            List of datatable row ids that matches selection
        """
        for row in grid_table:
            if row[0] == spw_id and row[1] in polarization_ids and (row[2], row[3]) in grid_list:
                new_row_entry = numpy.fromiter((r[3] for r in row[6] if r[-1] == origin_ms_id and r[-2] == antenna_id),
                                               dtype=int)
                yield new_row_entry

    @staticmethod
    def generate_plot_rowlist(
        origin_ms_id: int,
        antenna_id: int,
        spw_id: int,
        polarization_ids: List[int],
        grid_table: List[List[Union[int, float, numpy.ndarray]]],
        plot_table: List[List[Union[int, float]]],
        each_plane: List[int]
    ) -> List[int]:
        """Generate list of datatable row ids for plotting.

        Extract list of datatable row ids that matches selection for
        MS, spw, polarization, and spatial grid location. Return
        one-dimensional list of row ids to process.

        Args:
            origin_ms_id: MS id for selection
            antenna_id: Antenna id for selection
            spw_id: Spw id for selection
            polarization_ids: List of polarization ids for selection
            grid_table: grid_table generated by simplegrid module
            plot_table: Metadata table generated by generate_plot_meta_table
            each_plane: List of indices for grid_table/plot_table

        Returns:
            List of datatable row ids for plotting. Return value is intended
            to be used jointly with the table returned by generate_plot_meta_table.
        """
        xlist = [plot_table[idx][0] for idx in each_plane]
        ylist = [plot_table[idx][1] for idx in each_plane]
        grid_list = list(zip(xlist, ylist))
        new_table = list(BaselineSubtractionPlotManager._generate_plot_rowlist(origin_ms_id,
                                                                               antenna_id,
                                                                               spw_id,
                                                                               polarization_ids,
                                                                               grid_table,
                                                                               grid_list))
        return list(itertools.chain.from_iterable(new_table))

    @casa5style_plot
    def plot_spectra_with_fit(
        self,
        field_id: int,
        antenna_id: int,
        spw_id: int,
        postfit_integrated_data: numpy.ma.masked_array,
        postfit_map_data: numpy.ma.masked_array,
        prefit_integrated_data: numpy.ma.masked_array,
        prefit_map_data: numpy.ma.masked_array,
        prefit_averaged_data: numpy.ma.masked_array,
        num_ra: int,
        num_dec: int,
        rowlist: List[Dict[str, Union[int, float, List[int]]]],
        npol: int,
        frequency: List[float],
        grid_table: Optional[List[List[Union[int, float, numpy.ndarray]]]] = None,
        deviation_mask: Optional[List[Tuple[int, int]]] = None,
        channelmap_range: Optional[List[Tuple[int, int, bool]]] = None,
        edge: Optional[List[int]] = None,
        showatm: bool = True,
        in_rowmap: Optional[dict] = None
    ) -> List[compress.CompressedObj]:
        """Create various types of plots.

        For given set of field, antenna, and spw ids, this method generates
        plots of the following types:

            - sparse profile map for raw spectra before baseline subtraction
            - sparse profile map for spatially averaged spectra before baseline subtraction
            - sparse profile map for raw spectra after baseline subtraction
            - sparse profile map for spatially averaged spectra after baseline subtraction
            - visual illustration of baseline flatness after baseline subtraction

        Args:
            field_id: Field id to process
            antenna_id: Antenna id to process
            spw_id: Spw id to process. Should be the real spw id.
            postfit_integrated_data: integrated spectral data after baseline subtraction.
            postfit_map_data: sparse map spectral data after baseline subtraction.
            prefit_integrated_data: integrated spectral data before baseline subtraction.
            prefit_map_data: sparse map spectral data after baseline subtraction.
            prefit_averaged_data: averaged spectral data with each grid before baseline subtraction.
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            rowlist: List of datatable row ids per sparse map panel with metadata
            npol: Number of polarizations
            frequency: Frequency values of each element in spectrum.
            grid_table: grid_table generated by simplegrid module
            deviation_mask: List of channel ranges identified as "deviation mask". Each range
                            is represented as a tuple of start and end channels. Those ranges
                            are shown as red rectangle to indicate they are excluded from the fit.
                            None means no deviation mask is provided.
            channelmap_range: List of channel ranges identified as astronomical lines by
                              line detection and validation process. Channel range is represented
                              as a tuple of line center channel, line width in channel, and
                              validation result (boolean). Those ranges are shown as cyan rectangle
                              to indicate they are excluded from the fit. None means no line ranges
                              are provided.
            edge: Edge channels to exclude. Plot whole channels if None is given.
            showatm: Whether or not overlay ATM transmission curve. Defaults to True.
            in_rowmap: Row mapping between original (calibrated) MS and the MS
                       before baseline subtraction.

        Raises:
            Exception: Invalid plot type (maybe internal error)

        Returns:
            List of compressed Plot objects
        """
        if basetask.DISABLE_WEBLOG:
            return []

        if grid_table is None:
            return []

        # convert channelmap_range to plotter-aware format
        if channelmap_range is None:
            line_range = None
        else:
            line_range = [[r[0] - 0.5 * r[1], r[0] + 0.5 * r[1]] for r in channelmap_range if r[2] is True]
            if len(line_range) == 0:
                line_range = None

        self.field_id = field_id
        self.antenna_id = antenna_id
        self.spw_id = spw_id
        self.virtual_spw_id = self.context.observing_run.real2virtual_spw_id(self.spw_id, self.ms)
        data_desc = self.ms.get_data_description(spw=spw_id)
        num_pol = data_desc.num_polarizations
        self.pol_list = numpy.arange(num_pol, dtype=int)
        source_name = self.ms.fields[self.field_id].source.name.replace(' ', '_').replace('/', '_')
        LOG.debug('Generating plots for source %s ant %s spw %s',
                  source_name, self.antenna_id, self.spw_id)

        outprefix_template = lambda x: 'spectral_plot_%s_subtraction_%s_%s_ant%s_spw%s' % (x,
                                                                                           '.'.join(self.ms.basename.split('.')[:-1]),
                                                                                           source_name,
                                                                                           self.antenna_id,
                                                                                           self.virtual_spw_id)
        prefit_prefix = os.path.join(self.stage_dir, outprefix_template('before'))
        postfit_prefix = os.path.join(self.stage_dir, outprefix_template('after'))
        LOG.debug('prefit_prefix=\'%s\'', os.path.basename(prefit_prefix))
        LOG.debug('postfit_prefix=\'%s\'', os.path.basename(postfit_prefix))

        if showatm is True:
            atm_freq, atm_transmission = atmutil.get_transmission(vis=self.ms.name, antenna_id=self.antenna_id,
                                                                  spw_id=self.spw_id, doplot=False)
        else:
            atm_transmission = None
            atm_freq = None
        plot_list = self.plot_profile_map_with_fit(prefit_prefix, postfit_prefix,
                                                   postfit_integrated_data, postfit_map_data,
                                                   prefit_integrated_data, prefit_map_data,
                                                   prefit_averaged_data,
                                                   num_ra, num_dec, rowlist,
                                                   npol, frequency,
                                                   deviation_mask, line_range,
                                                   atm_transmission, atm_freq,
                                                   edge, in_rowmap=in_rowmap)
        plot_flatness = self.plot_flatness_profile(postfit_prefix,
                                                   postfit_integrated_data,
                                                   npol)
        plot_list.update(plot_flatness)

        ret = []
        for plot_type, plots in plot_list.items():
            if plot_type == 'pre_fit':
                ptype = 'sd_sparse_map_before_subtraction_raw'
                data = self.prefit_data
            elif plot_type == 'post_fit':
                ptype = 'sd_sparse_map_after_subtraction_raw'
                data = self.postfit_data
            elif plot_type == 'pre_fit_avg':
                ptype = 'sd_sparse_map_before_subtraction_avg'
                data = self.prefit_data
            elif plot_type == 'post_fit_flatness':
                ptype = 'sd_spectrum_after_subtraction_flatness'
                data = self.postfit_data
            else:
                raise Exception('Unrecognized plot type.')
            for pol, figfile in plots.items():
                if os.path.exists(figfile):
                    parameters = {'intent': 'TARGET',
                                  'spw': self.virtual_spw_id,  # parameter for plots are virtual spw id
                                  'pol': sd_polmap[pol],
                                  'ant': self.ms.antennas[self.antenna_id].name,
                                  'vis': os.path.basename(self.ms.origin_ms),
                                  'type': ptype,
                                  'file': data}
                    plot = logger.Plot(figfile,
                                       x_axis='Frequency',
                                       y_axis='Intensity',
                                       field=source_name,
                                       parameters=parameters)
                    ret.append(compress.CompressedObj(plot))
                    del plot
        return ret

    def plot_profile_map_with_fit(
        self,
        prefit_figfile_prefix: str,
        postfit_figfile_prefix: str,
        postfit_integrated_data: numpy.ma.masked_array,
        postfit_map_data: numpy.ma.masked_array,
        prefit_integrated_data: numpy.ma.masked_array,
        prefit_map_data: numpy.ma.masked_array,
        prefit_averaged_data: numpy.ma.masked_array,
        num_ra: int,
        num_dec: int,
        rowlist: List[Dict[str, Union[int, float, List[int]]]],
        npol: int,
        frequency: List[float],
        deviation_mask: Optional[List[Tuple[int, int]]],
        line_range: Optional[List[Tuple[int, int]]],
        atm_transmission: Optional[numpy.ndarray],
        atm_frequency: Optional[numpy.ndarray],
        edge: Optional[List[int]],
        in_rowmap: Optional[dict] = None
    ) -> Dict[str, Dict[int, str]]:
        """Create various type of plots.

        This method produces the following plots.

            - sparse profile map for raw spectra before baseline subtraction
            - sparse profile map for spatially averaged spectra before baseline subtraction
            - sparse profile map for raw spectra after baseline subtraction
            - sparse profile map for spatially averaged spectra after baseline subtraction

        Args:
            prefit_figfile_prefix: Prefix string for the filename (before baseline subtraction)
            postfit_figfile_prefix: Prefix for the filename (after baseline subtraction)
            postfit_integrated_data: integrated spectral data after baseline subtraction.
            postfit_map_data: sparse map spectral data after baseline subtraction.
            prefit_integrated_data: integrated spectral data before baseline subtraction.
            prefit_map_data: sparse map spectral data after baseline subtraction.
            prefit_averaged_data: averaged spectral data with each grid before baseline subtraction.
            num_ra: Number of panels along horizontal axis
            num_dec: Number of panels along vertical axis
            rowlist: List of datatable row indices per sparse map panel with metadata
            npol: Number of polarizations
            frequency: Frequency values of each element in spectrum
            deviation_mask: List of channel ranges identified as the "deviation mask"
            line_range: List of channel ranges identified as astronomical line
            atm_transmission: ATM transmission data
            atm_frequency: frequency label associated with ATM transmission
            edge: Edge channels excluded from the baseline fitting
            in_rowmap: Row mapping between original (calibrated) MS and the MS
                       before baseline subtraction.

        Returns:
            Dictionary containing names of the figure with plot type and
            polarization id as keys

                plot_list[plot_type][polarization_id] = figfile
        """

        polids = self.pol_list
        prefit_data = self.prefit_data

        # get brightnessunit from MS
        # default is Jy/beam
        bunit = utils.get_brightness_unit(self.ms.name, defaultunit='Jy/beam')

        # ralist/declist holds coordinates for axis labels (center of each panel)
        ralist = [r.get('RA') for r in rowlist if r['DECID'] == 0]
        declist = [r.get('DEC') for r in rowlist if r['RAID'] == 0]

        plotter = self.pool.create_plotter(num_ra, num_dec, ralist, declist,
                                           direction_reference=self.datatable.direction_ref,
                                           brightnessunit=bunit)

        if line_range is not None:
            lines_map = get_lines(self.datatable, num_ra, npol, rowlist)
        else:
            lines_map = None

        plot_list = {}

        # plot post-fit spectra
        plot_list['post_fit'] = {}
        plotter.setup_reference_level(0.0)
        plotter.set_deviation_mask(deviation_mask)
        plotter.set_edge(edge)
        plotter.set_atm_transmission(atm_transmission, atm_frequency)
        plotter.set_global_scaling()
        if utils.is_nro(self.context):
            plotter.set_channel_axis()
        for ipol in range(npol):
            postfit_figfile = postfit_figfile_prefix + '_pol%s.png' % ipol
            # LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
            if lines_map is not None:
                plotter.setup_lines(line_range, lines_map[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(postfit_map_data[:, :, ipol, :],
                         postfit_integrated_data[ipol],
                         frequency, figfile=postfit_figfile)
            # LOG.info('#TIMING# End SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
            if os.path.exists(postfit_figfile):
                plot_list['post_fit'][ipol] = postfit_figfile

        # fit_result shares its storage with postfit_map_data to reduce memory usage
        fit_result = postfit_map_data
        for x in range(num_ra):
            for y in range(num_dec):
                prefit = prefit_map_data[x][y]
                if not numpy.all(prefit == display.NoDataThreshold):
                    postfit = postfit_map_data[x][y]
                    fit_result[x, y] = prefit - postfit
                else:
                    fit_result[x, y, ::] = display.NoDataThreshold

        # plot pre-fit spectra
        plot_list['pre_fit'] = {}
        plotter.setup_reference_level(None)
        plotter.unset_global_scaling()
        for ipol in range(npol):
            prefit_figfile = prefit_figfile_prefix + '_pol%s.png' % (ipol)
            # LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
            if lines_map is not None:
                plotter.setup_lines(line_range, lines_map[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(prefit_map_data[:, :, ipol, :],
                         prefit_integrated_data[ipol],
                         frequency, fit_result=fit_result[:, :, ipol, :], figfile=prefit_figfile)
            # LOG.info('#TIMING# End SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
            if os.path.exists(prefit_figfile):
                plot_list['pre_fit'][ipol] = prefit_figfile

        del prefit_map_data, postfit_map_data, fit_result

        if line_range is not None:
            lines_map_avg = get_lines2(prefit_data, self.datatable, num_ra,
                                       rowlist, polids, rowmap=in_rowmap)
        else:
            lines_map_avg = None
        # plot pre-fit averaged spectra
        plot_list['pre_fit_avg'] = {}
        plotter.setup_reference_level(None)
        plotter.unset_global_scaling()
        for ipol in range(npol):
            prefit_avg_figfile = prefit_figfile_prefix + '_avg_pol{}.png'.format(ipol)
            if lines_map_avg is not None:
                plotter.setup_lines(line_range, lines_map_avg[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(prefit_averaged_data[:, :, ipol, :],
                         prefit_integrated_data[ipol],
                         frequency, fit_result=None, figfile=prefit_avg_figfile)

            if os.path.exists(prefit_avg_figfile):
                plot_list['pre_fit_avg'][ipol] = prefit_avg_figfile

        del prefit_integrated_data, prefit_averaged_data

        plotter.done()

        return plot_list

    def plot_flatness_profile(
        self,
        postfit_figfile_prefix: str,
        postfit_integrated_data: numpy.ma.masked_array,
        npol: int
    ) -> Dict[str, Dict[int, str]]:
        """Create plots of baseline flatness profile of a spectrum (after baseline subtraction).

        Args:
            postfit_figfile_prefix: Prefix for the filename (after baseline subtraction)
            postfit_integrated_data: integrated spectral data after baseline subtraction.
            npol: Number of polarizations

        Returns:
            Dictionary containing names of the figure with plot type and
            polarization id as keys

                plot_list[plot_type][polarization_id] = figfile
        """
        plot_list = {}
        # baseline flatness plots
        plot_list['post_fit_flatness'] = {}
        for ipol in range(npol):
            postfit_qa_figfile = postfit_figfile_prefix + '_flatness_pol%s.png' % ipol
            if os.path.exists(postfit_qa_figfile):
                plot_list['post_fit_flatness'][ipol] = postfit_qa_figfile

        del postfit_integrated_data

        return plot_list


class BaselineSubtractionQualityManager(BaselineSubtractionDataManager):
    """Class to calculate quality statistics."""

    def __init__(self, ms: 'MeasurementSet', blvis: str, context: 'Context', datatable: 'DataTable') -> None:
        """Construct BaselineSubtractionQualityManager instance.

        Args:
            ms: Domain object for the MS before baseline subtraction
            blvis: Name of the MS after baseline subtraction
            context: Pipeline context
            datatable: DataTable instance
        """
        super().__init__(ms, blvis, context, datatable)
        self.masked_data = None
        self.binned_freq = None
        self.binned_data = None

    def analyze_flatness(self, spectrum: List[float], frequency: List[float],
                         line_range: Optional[List[Tuple[float, float]]],
                         deviation_mask: Optional[List[Tuple[int, int]]],
                         edge: Tuple[int, int]) -> BinnedStat:
        """
        Create a data of baseline flatness of a spectrum.

        Also, this method computes statistics of binned spectrum.

        Args:
            spectrum: A spectrum to analyze baseline flatness and plot.
            frequency: Frequency values of each element in spectrum.
            line_range: ID ranges in spectrum array that should be considered
                as spectral lines and eliminated from inspection of baseline
                flatness.
            deviation_mask: ID ranges of deviation mask. These ranges are also
                eliminated from inspection of baseline flatness.
            edge: Number of elements in left and right edges that should be
                eliminates from inspection of baseline flatness.

        Return:
            Statistics of binned spectrum
        """
        masked_data = numpy.ma.masked_array(spectrum, mask=False)
        if edge is not None:
            (ch1, ch2) = edge
            masked_data.mask[0:ch1] = True
            masked_data.mask[len(masked_data)-ch2:] = True
        num_masked_1 = sum(numpy.where(masked_data.mask, 1, 0))
        LOG.debug(f'Number of newly masked channels with edge parameter: {num_masked_1}')
        if line_range is not None:
            for chmin, chmax in line_range:
                masked_data.mask[int(chmin):int(numpy.ceil(chmax))+1] = True
        num_masked_2 = sum(numpy.where(masked_data.mask, 1, 0))
        LOG.debug(f'Number of newly masked channels with line mask: {num_masked_2 - num_masked_1}')
        if deviation_mask is not None:
            for chmin, chmax in deviation_mask:
                masked_data.mask[chmin:chmax+1] = True
        num_masked_3 = sum(numpy.where(masked_data.mask, 1, 0))
        LOG.debug(f'Number of newly masked channels with deviation mask: {num_masked_3 - num_masked_2}')

        self.masked_data = masked_data

        stddev = self.masked_data.std()

        stat_valid = True
        nbin_factor = 1
        nbin = 20 if len(frequency) >= 512 else 10

        binned_freq, binned_data = binned_mean_ma(frequency, self.masked_data, nbin)
        LOG.debug(
            f'nbin {nbin}: len(binned_data) = {len(binned_data)}'
            f' count = {binned_data.count()}'
        )

        if stddev is numpy.ma.masked or not numpy.isfinite(stddev):
            # stddev is invalid, i.e., masked_data doesn't contain valid data
            stat_valid = False

        elif binned_data.count() < 2:
            # not enough valid data, increase nbin and try again
            nbin_org = nbin
            nbin_factor = 2
            nbin *= nbin_factor
            LOG.info(
                f'Calculation of baseline flatness QA metrics with nbin={nbin_org} has failed. '
                f'Increase nbin to {nbin}.'
            )
            binned_freq, binned_data = binned_mean_ma(frequency, self.masked_data, nbin)
            LOG.debug(
                f'nbin {nbin}: len(binned_data) = {len(binned_data)}'
                f' count = {binned_data.count()}'
            )

            if binned_data.count() < 2:
                # not enough valid data even for larger number of bins
                stat_valid = False

        self.binned_freq = binned_freq
        self.binned_data = binned_data

        if stat_valid:
            bin_min = numpy.nanmin(binned_data)
            bin_max = numpy.nanmax(binned_data)
            stat = BinnedStat(bin_min_ratio=bin_min/stddev,
                              bin_max_ratio=bin_max/stddev,
                              bin_diff_ratio=(bin_max-bin_min)/stddev,
                              nbin=nbin,
                              nbin_factor=nbin_factor,
                              valid=True)
        else:
            stat = BinnedStat(bin_min_ratio=None,
                              bin_max_ratio=None,
                              bin_diff_ratio=None,
                              nbin=nbin,
                              nbin_factor=nbin_factor,
                              valid=False)

        return stat

    def plot_flatness(self, spectrum: List[float], frequency: List[float],
                      line_range: Optional[List[Tuple[float, float]]],
                      deviation_mask: Optional[List[Tuple[int, int]]],
                      edge: Tuple[int, int], brightnessunit: str,
                      stat: BinnedStat,
                      figfile: str) -> None:
        """
        Create a plot of baseline flatness of a spectrum.

        Args:
            spectrum: A spectrum to analyze baseline flatness and plot.
            frequency: Frequency values of each element in spectrum.
            line_range: ID ranges in spectrum array that should be considered
                as spectral lines and eliminated from inspection of baseline
                flatness.
            deviation_mask: ID ranges of deviation mask. These ranges are also
                eliminated from inspection of baseline flatness.
            edge: Number of elements in left and right edges that should be
                eliminated from inspection of baseline flatness.
            brightnessunit: Brightness unit of spectrum.
            stat: Binned statistics data
            figfile: A file name to save figure.
        """
        stddev = self.masked_data.std()

        if stddev is numpy.ma.masked or not numpy.isfinite(stddev):
            # not enough valid data or stddev is invalid
            return

        # create a plot
        xmin = min(frequency[0], frequency[-1])
        xmax = max(frequency[0], frequency[-1])
        ymin = -3*stddev
        ymax = 3*stddev
        plt.clf()
        plt.plot(frequency, spectrum, color='b', linestyle='-', linewidth=0.4)
        plt.axis((xmin, xmax, ymin, ymax))
        plt.gca().get_xaxis().get_major_formatter().set_useOffset(False)
        plt.gca().get_yaxis().get_major_formatter().set_useOffset(False)
        plt.title('Spatially Averaged Spectrum')
        plt.ylabel(f'Intensity ({brightnessunit})')
        plt.xlabel('Frequency (GHz)')
        if edge is not None:
            (ch1, ch2) = edge
            fedge0 = ch_to_freq(0, frequency)
            fedge1 = ch_to_freq(ch1-1, frequency)
            fedge2 = ch_to_freq(len(frequency)-ch2, frequency)
            fedge3 = ch_to_freq(len(frequency)-1, frequency)
            plt.axvspan(fedge0, fedge1, color='lightgray')
            plt.axvspan(fedge2, fedge3, color='lightgray')
        if line_range is not None:
            for chmin, chmax in line_range:
                fmin = ch_to_freq(chmin, frequency)
                fmax = ch_to_freq(chmax, frequency)
                plt.axvspan(fmin, fmax, color='cyan')
        if deviation_mask is not None:
            for chmin, chmax in deviation_mask:
                fmin = ch_to_freq(chmin, frequency)
                fmax = ch_to_freq(chmax, frequency)
                plt.axvspan(fmin, fmax, ymin=0.97, ymax=1.0, color='red')
        plt.hlines([-stddev, 0.0, stddev], xmin, xmax, colors='k', linestyles='dashed')
        if stat.valid:
            plt.plot(self.binned_freq, self.binned_data, 'ro')

            if stat.nbin_factor == 1:
                note_nbin = f'applied default number of bins ({stat.nbin})'
            else:
                note_nbin = f'applied {stat.nbin_factor} times default number of bins ({stat.nbin})'
            facecolor = 'white'
        else:
            note_nbin = 'NO BINNED SPECTRUM AVAILABLE'
            facecolor = 'yellow'

        plt.text(
            0.98, 0.02, note_nbin,
            transform=plt.gca().transAxes,
            verticalalignment='bottom',
            horizontalalignment='right',
            bbox={'boxstyle': 'round', 'facecolor': facecolor, 'edgecolor': 'black'}
        )
        plt.savefig(figfile, dpi=DPIDetail)

    def calculate_baseline_quality_stat(self, field_id: int, ant_id: int, spw_id: int,
                                        postfit_integrated_data: numpy.ma.masked_array,
                                        npol: int, frequency: List[float],
                                        deviation_mask: Optional[List[Tuple[int, int]]],
                                        channelmap_range: Optional[List[Tuple[int, int, bool]]],
                                        edge: Optional[List[int]]) -> List[QualityStat]:
        """Calculate quality statistics after baseline subtraction.

        Args:
            field_id: Field id to process
            ant_id: Antnena id to process
            spw_id: Spw id to process. Should be the real spw id.
            postfit_integrated_data: integrated spectral data after baseline subtraction.
            npol: Number of polarizations
            frequency: Frequency values of each element in spectrum
            deviation_mask: List of channel ranges identified as the "deviation mask"
            channelmap_range: List of channel ranges identified as astronomical lines by
                              line detection and validation process. Channel range is represented
                              as a tuple of line center channel, line width in channel, and
                              validation result (boolean). Those ranges are shown as cyan rectangle
                              to indicate they are excluded from the fit. None means no line ranges
                              are provided.
            edge: Edge channels excluded from the baseline fitting

        Returns:
            List of namedtuple 'QualityStat' containing 'vis field spw ant pol stat'.
            The 'stat' is also namedtuple 'BinnedStat'. (see method analyze_flatness.)
        """

        virtual_spw_id = self.context.observing_run.real2virtual_spw_id(spw_id, self.ms)
        stage_number = self.context.task_counter
        self.stage_dir = os.path.join(self.context.report_dir, "stage%d" % stage_number)
        source_name = self.ms.fields[field_id].source.name.replace(' ', '_').replace('/', '_')
        outprefix_template = lambda x: 'spectral_plot_%s_subtraction_%s_%s_ant%s_spw%s' % (x,
                                                                                           '.'.join(self.ms.basename.split('.')[:-1]),
                                                                                           source_name,
                                                                                           ant_id,
                                                                                           virtual_spw_id)
        self.postfit_prefix = os.path.join(self.stage_dir, outprefix_template('after'))
        baseline_quality_stat = []

        # convert channelmap_range to plotter-aware format
        if channelmap_range is None:
            line_range = None
        else:
            line_range = [[r[0] - 0.5 * r[1], r[0] + 0.5 * r[1]] for r in channelmap_range if r[2] is True]
            if len(line_range) == 0:
                line_range = None

        # get brightnessunit from MS
        # default is Jy/beam
        bunit = utils.get_brightness_unit(self.ms.name, defaultunit='Jy/beam')

        for ipol in range(npol):
            stat = self.analyze_flatness(postfit_integrated_data[ipol], frequency, line_range, deviation_mask, edge)
            if stat.valid:
                if stat.nbin_factor > 1:
                    LOG.warning(
                        'Default number of bins for baseline flatness QA '
                        'metrics did not work for vis %s ant %s spw %s pol %s. '
                        'Applied %s times larger number of bins instead.',
                        self.ms.basename, ant_id, spw_id, ipol, stat.nbin_factor
                    )
                quality_stat = QualityStat(vis=os.path.basename(self.ms.origin_ms), field=source_name, spw=virtual_spw_id, ant=self.ms.antennas[ant_id].name, pol=sd_polmap[ipol], stat=[stat])
                baseline_quality_stat.append(quality_stat)
            else:
                LOG.warning(
                    'Failed to evaluate baseline flatness QA metrics for '
                    'vis %s ant %s spw %s pol %s. '
                    'Baseline flatness plot will be incomplete.',
                    self.ms.basename, ant_id, spw_id, ipol
                )

            if not basetask.DISABLE_WEBLOG:
                postfit_qa_figfile = self.postfit_prefix + '_flatness_pol%s.png' % ipol
                self.plot_flatness(postfit_integrated_data[ipol], frequency, line_range,
                                   deviation_mask, edge, bunit, stat, postfit_qa_figfile)

                if not os.path.exists(postfit_qa_figfile):
                    LOG.warning(
                        'No valid spectra in vis %s ant %s spw %s pol %s. '
                        'Neither baseline flatness QA metrics nor baseline '
                        'flatness plot are available.',
                        self.ms.basename, ant_id, spw_id, ipol
                    )

        del postfit_integrated_data

        return baseline_quality_stat

def generate_grid_panel_map(ngrid: int, npanel: int, num_plane: int = 1) -> Generator[List[int], None, None]:
    """Yield list of grid table indices that belong to the sparse map panel.

    Args:
        ngrid: Number of grid positions
        npanel: Number of panels
        num_plane: Number of planes per grid position. Defaults to 1.

    Yields:
        List of indices for the grid table
    """
    ng = ngrid // npanel
    mg = ngrid % npanel
    ng_per_panel = [ng * num_plane for i in range(npanel)]
    for i in range(mg):
        ng_per_panel[i] += num_plane

    s = 0
    e = 0
    for i in range(npanel):
        s = e
        e = s + ng_per_panel[i]
        yield list(range(s, e))


def configure_1d_panel(
    nx: int,
    ny: int,
    num_plane: int = 1
) -> Tuple[List[List[int]], List[List[int]]]:
    """Configure linear mapping between grid table index and sparse map panel.

    When number of grid positions, nx * ny, exceeds 50, neighboring positions
    are combined so that number of sparse map panels does not exceed 50.

    Args:
        nx: Number of grid positions along horizontal axis
        ny: Number of grid positions along vertical axis
        num_plane: Number of planes per grid position. Defaults to 1.

    Returns:
        Linear mapping between grid table index and sparse map panel
    """
    max_panels = 50
    num_panels = nx * ny

    nnx = nx
    nny = ny
    if num_panels > max_panels:
        div = lambda x: x // 2 + x % 2
        LOG.debug('original: {} x {} = {}'.format(nx, ny, num_panels))
        ndiv = 0
        nnp = num_panels
        while nnp > max_panels:
            nnx = div(nnx)
            nny = div(nny)
            nnp = nnx * nny
            ndiv += 1
            LOG.trace('div {}: {} x {} = {}'.format(ndiv, nnx, nny, nnp))
        LOG.debug('processed: {} x {} = {}'.format(nnx, nny, nnp))

    xpanel = list(generate_grid_panel_map(nx, nnx, num_plane))
    ypanel = list(generate_grid_panel_map(ny, nny, num_plane))

    return xpanel, ypanel


def configure_2d_panel(
    xpanel: List[List[int]],
    ypanel: List[List[int]],
    ngridx: int,
    num_plane: int = 3
) -> List[List[int]]:
    """Confure two-dimensional mapping between grid table and sparse map panel.

    Args:
        xpanel: Linear mapping result returned by configure_1d_panel
        ypanel: Linear mapping result returned by configure_1d_panel
        ngridx: Number of grid positions along horizontal axis
        num_plane: Number of planes per grid position. Defaults to 3.

    Returns:
        Two-dimensional mapping between grid table and sparse map panel
    """
    xypanel = []
    for ygrid in ypanel:
        for xgrid in xpanel:
            p = []
            for y in ygrid:
                for x in xgrid:
                    a = ngridx * num_plane * y + num_plane * x
                    p.extend(list(range(a, a + num_plane)))
            xypanel.append(p)
    return xypanel


def get_lines(
    datatable: 'DataTable',
    num_ra: int,
    num_pol: int,
    rowlist: List[Dict[str, Union[int, float, List[int]]]]
) -> List[collections.defaultdict]:
    """Get detected line ranges per sparse map panel.

    Line information is taken from the datatable row specified
    by 'MEDIAN_INDEX' value of each item in rowlist.

    Args:
        datatable  Datatable instance
        num_ra: Number of panels along horizontal axis
        num_pol: Number of polarizations
        rowlist: List of datatable row indices per sparse map panel with metadata

    Returns:
        List of detected line ranges per panel
    """
    lines_map = [collections.defaultdict(dict)] * num_pol
    # with casa_tools.TableReader(rwtablename) as tb:
    for d in rowlist:
        ix = num_ra - 1 - d['RAID']
        iy = d['DECID']
        ids = d['IDS']
        midx = d['MEDIAN_INDEX']
        for ipol in range(len(midx)):
            if midx is not None:
                if midx[ipol] is not None:
                    masklist = datatable.getcell('MASKLIST', ids[midx[ipol]])
                    lines_map[ipol][ix][iy] = None if (len(masklist) == 0 or numpy.all(masklist == -1)) else masklist
                else:
                    lines_map[ipol][ix][iy] = None
            else:
                lines_map[ipol][ix][iy] = None
    return lines_map


def get_lines2(
    infile: str,
    datatable: 'DataTable',
    num_ra: int,
    rowlist: List[Dict[str, Union[int, float, List[int]]]],
    polids: List[int],
    rowmap: Optional[dict] = None
) -> List[collections.defaultdict]:
    """Get detected line ranges per sparse map panel.

    get_lines2 performs the same operation with get_lines from
    the different input arguments.

    Args:
        infile: Name of MS before baseline subtraction
        datatable: Datatable instance
        num_ra: Number of panels along horizontal axis
        rowlist: List of datatable row ids per sparse map panel with metadata
        polids: List of polarization ids
        rowmap: Row mapping between original (calibrated) MS and the MS
                before baseline subtraction. It will be EchoDictionary if
                None is given.

    Returns:
        List of detected line ranges per panel
    """
    if rowmap is None:
        rowmap = utils.EchoDictionary()

    num_pol = len(polids)
    lines_map = [collections.defaultdict(dict)] * num_pol
    with casa_tools.TableReader(infile) as tb:
        for d in rowlist:
            ix = num_ra - 1 - d['RAID']
            iy = d['DECID']
            ids = d['IDS']
            ref_ra = d['RA']
            ref_dec = d['DEC']
            rep_ids = [-1 for i in range(num_pol)]
            min_distance = [1e30 for i in range(num_pol)]
            for dt_id in ids:
                row = rowmap[datatable.getcell('ROW', dt_id)]
                flag = tb.getcell('FLAG', row)
                ra = datatable.getcell('OFS_RA', dt_id)
                dec = datatable.getcell('OFS_DEC', dt_id)
                sqdist = (ra - ref_ra) * (ra - ref_ra) + (dec - ref_dec) * (dec - ref_dec)
                for ipol in range(num_pol):
                    if numpy.all(flag[ipol] == True):
                        # LOG.info('TN: ({}, {}) row {} pol {} is all flagged'.format(ix, iy, row, ipol))
                        continue

                    if sqdist <= min_distance[ipol]:
                        rep_ids[ipol] = dt_id

            # LOG.info('TN: rep_ids for ({}, {}) is {}'.format(ix, iy, rep_ids))
            for ipol in range(num_pol):
                if rep_ids[ipol] >= 0:
                    masklist = datatable.getcell('MASKLIST', rep_ids[ipol])
                    lines_map[ipol][ix][iy] = None if (len(masklist) == 0 or numpy.all(masklist == -1)) else masklist
                else:
                    lines_map[ipol][ix][iy] = None

    return lines_map


def binned_mean_ma(x: List[float], masked_data: MaskedArray,
                   nbin: int) -> Tuple[numpy.ndarray, MaskedArray]:
    """
    Bin an array.

    Return an array of averaged values of masked_data in each bin.
    An element of binned_data is masked if any of elements in masked_data that
    contribute to the bin is masked.

    Args:
        x: Abcissa value of each element in masked_data.
        masked_data: Data to be binned. The length of array must be equal to that of x.
        nbin: Number of bins.
    Returns:
        Arrays of binned abcissa and binned data
    """
    ndata = len(masked_data)
    assert nbin < ndata
    assert len(x) == ndata
    bin_width = ndata/nbin  # float
    # Prepare return values
    binned_data = numpy.ma.masked_array(numpy.zeros(nbin), mask=False)
    binned_x = numpy.zeros(nbin)
    min_i = 0
    for i in range(nbin):
        max_i = min(int(numpy.floor((i+1)*bin_width)), ndata-1)
        num_masked = sum(numpy.where(masked_data.mask[min_i:max_i+1], 1, 0))
        if num_masked == 0:
            msg = 'valid for flatness QA'
        else:
            msg = 'will be excluded from the flatness QA'
        LOG.trace(f'bin {i}: masked channels {num_masked}, {msg}')
        binned_x[i] = numpy.mean(x[min_i:max_i+1])
        if any(masked_data.mask[min_i:max_i+1]):
            binned_data.mask[i] = True
        else:
            binned_data[i] = numpy.nanmean(masked_data[min_i:max_i+1])
        min_i = max_i+1

    # mask NaN or Inf value
    binned_data.mask |= numpy.logical_not(numpy.isfinite(binned_data.data))

    return binned_x, binned_data


def sort_with_key( arr: List[Dict[str, Any]], key: str, reverse: Optional[bool]=False ) -> List[Dict[str, Any]]:
    """
    Sort the list of dictionaries with the value of the specified key in each list component

    Args:
        arr     : input list of dictionaries
        key     : key to the value to sort with
        reverse : True to sort in reversed order, defaults to False
    Returns:
        sorted list
    """
    return sorted( arr, reverse=reverse, key=lambda x: x[key] )
