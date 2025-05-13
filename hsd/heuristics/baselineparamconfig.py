import abc
import collections
import enum
import os
from typing import Dict, List, Sequence, Tuple, Union

import numpy

import pipeline.infrastructure.api as api
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.logging as logging
from pipeline.domain import DataTable, MeasurementSet
from pipeline.infrastructure import casa_tools
from pipeline.hsd.heuristics import fitorder

LOG = infrastructure.get_logger(__name__)


def TRACE():
    return LOG.isEnabledFor(logging.LOGGING_LEVELS['trace'])


def DEBUG():
    return LOG.isEnabledFor(logging.LOGGING_LEVELS['debug'])


class BaselineParamKeys(enum.Enum):
    ROW = 'row'
    POL = 'pol'
    MASK = 'mask'
    CLIPNITER = 'clipniter'
    CLIPTHRESH = 'clipthresh'
    USELF = 'use_linefinder'
    LFTHRESH = 'thresh'
    LEDGE = 'Ledge'
    REDGE = 'Redge'
    AVG_LIMIT = 'avg_limit'
    FUNC = 'blfunc'
    ORDER = 'order'
    NPIECE = 'npiece'
    NWAVE = 'nwave'


BLP = BaselineParamKeys


def write_blparam(fileobj, param):
    param_values = collections.defaultdict(str)
    for key in BLP:
        if key in param:
            param_values[key] = param[key]
    line = ','.join(map(str, [param_values[k] for k in BLP]))
    fileobj.write(line+'\n')


def as_maskstring(masklist):
    return ';'.join(['%s~%s' % (x[0], x[1]) for x in masklist])


def no_switching(engine, nchan, edge, num_pieces, masklist):
    return fitorder.FittingFunction.CSPLINE.blfunc, 0


def do_switching(engine, nchan, edge, num_pieces, masklist):
    return engine(nchan, edge, num_pieces, masklist)


class BaselineFitParamConfig(api.Heuristic, metaclass=abc.ABCMeta):
    """
    Generate/update BLParam file according to the input parameters.
    """

    ApplicableDuration = 'raster'  # 'raster' | 'subscan'
    MaxPolynomialOrder = 'none'  # 'none', 0, 1, 2,...
    PolynomialOrder = 'automatic'  # 'automatic', 0, 1, 2, ...

    def __init__(self, fitfunc: str = 'cspline', switchpoly: bool = True):
        """Construct BaselineFitParamConfig instance.

        Args:
            fitfunc: Fit function to use. Cubic spline ('spline' or 'cspline')
                     and polynomial ('poly' or 'polynomial') are available.
                     Default is 'cspline'.
            switchpoly: Whether or not fall back to low order polynomial fit
                        when large mask exist at the edge of spw if fitfunc
                        is either 'cspline' or 'spline'. Defaults to True.

        Raises:
            RuntimeError: Invalid fitting function was specified.
        """
        super(BaselineFitParamConfig, self).__init__()
        self.fitfunc = fitorder.get_fitting_function(fitfunc)
        LOG.info(f'Baseline parameter is optimized for {self.fitfunc.description} fitting')

        self.paramdict = {}
        self.heuristics_engine = fitorder.SwitchPolynomialWhenLargeMaskAtEdgeHeuristic()
        if switchpoly is True:
            self.switching_heuristic = do_switching
        else:
            self.switching_heuristic = no_switching

        self.paramdict[BLP.CLIPNITER] = self.ClipCycle
        self.paramdict[BLP.CLIPTHRESH] = 5.0

    # readonly attributes
    @property
    def ClipCycle(self):
        return 1

    is_multi_vis_task = False

    def __datacolumn(self, vis):
        colname = ''
        if isinstance(vis, str):
            with casa_tools.TableReader(vis) as tb:
                candidate_names = ['CORRECTED_DATA',
                                   'DATA',
                                   'FLOAT_DATA']
                for name in candidate_names:
                    if name in tb.colnames():
                        colname = name
                        break
        return colname

    def calculate(self, datatable: DataTable, ms: MeasurementSet,
                  rowmap: dict, antenna_id: int, field_id: int,
                  spw_id: int, fit_order: Union[str, int],
                  edge: Tuple[int, int], deviation_mask: List[dict],
                  blparam: str) -> str:
        """
        Generate/update BLParam file according to the input parameters.

        BLParam file will be an input to sdbaseline,.

        Args:
            datatable: DataTable instance
            ms: MS domain object to calculate fitting parameters
            rowmap: Row map dictionary between origin_ms and ms
            antenna_id: Antenna ID to process
            field_id: Field ID to process
            spw_id: Spw ID to process
            fit_order: Fiting order ('automatic' or number)
            edge: Number of edge channels to be excluded from the heuristics
                (format: [L, R])
            deviation_mask: Deviation mask
            blparam: Name of the BLParam file
                File contents will be updated by this heuristics

        Note:
            In this method, the boundaries of channel ranges are indicated by a list [start, end].
            Basically, the treatment of pythonic range is [start, end+1], but it is not intuitive for
            dealing with channels and masks of MeasurementSet. Therefore these are written as [start, end]
            in this source. Pythonic range-lists as boundaries for MS are only used in local processing
            scopes like a local function or loop block. Other than these scopes, we should write
            the channel/mask range as [start, end].

        Returns:
            Name of the BLParam file
        """
        LOG.debug('Starting BaselineFitParamConfig')

        # fitting order
        if fit_order == 'automatic':
            # fit order heuristics
            LOG.info('Baseline-Fitting order was automatically determined')
            self.fitorder_heuristic = fitorder.FitOrderHeuristics()
        else:
            LOG.info('Baseline-Fitting order was fixed to {}'.format(fit_order))
            self.fitorder_heuristic = lambda *args, **kwargs: fit_order #self.inputs.fit_order

        vis = ms.name
        if DEBUG() or TRACE():
            LOG.debug('MS "{}" ant {} field {} spw {}'.format(os.path.basename(vis), antenna_id, field_id, spw_id))

        nchan = ms.spectral_windows[spw_id].num_channels
        data_desc = ms.get_data_description(spw=spw_id)
        npol = data_desc.num_polarizations
        # edge must be formatted to [L, R]
        assert isinstance(edge, (list, tuple)) and len(edge) == 2, 'edge must be a list [L, R]. "{0}" was given.'.format(edge)

        if DEBUG() or TRACE():
            LOG.debug('nchan={nchan} edge={edge}'.format(nchan=nchan, edge=edge))

        if self.ApplicableDuration == 'subscan':
            timetable_index = 1
        else:
            timetable_index = 0

        index_list_total = []

        edge = fitorder.EdgeChannels(*edge)

        # prepare mask arrays
        mask_array = numpy.ones(nchan, dtype=int)
        mask_array[:edge.left] = 0
        mask_array[nchan-edge.right:] = 0

        # deviation mask
        if DEBUG() or TRACE():
            LOG.debug('Deviation mask for field {} antenna {} spw {}: {}'.format(
                field_id, antenna_id, spw_id, deviation_mask))
        if deviation_mask is not None:
            for mask_range in deviation_mask:
                mask_array[max(0, mask_range[0]):min(nchan, mask_range[1] + 1)] = 0

        base_mask_array = mask_array.copy()

        #LOG.info('base_mask_array = {}'.format(''.join(map(str, base_mask_array))))

        time_table = datatable.get_timetable(antenna_id, spw_id, None, os.path.basename(ms.origin_ms), field_id)
        member_list = time_table[timetable_index]

        # working with spectral data in scantable
        nrow_total = sum((len(x[0]) for x in member_list))

        LOG.info('Calculating Baseline Fitting Parameter...')
        LOG.info('Processing {} spectra...'.format(nrow_total))

        #colname = self.inputs.colname
        datacolumn = self.__datacolumn(vis)

        if DEBUG() or TRACE():
            LOG.debug('data column name is "{}"'.format(datacolumn))

        # open blparam file (append mode)
        with open(blparam, 'a') as blparamfileobj:

            with casa_tools.TableReader(vis) as tb:
                for y, member in enumerate( member_list ):
                    origin_rows = member[0] # origin_ms row ID
                    idxs = member[1] # datatable row ID
                    rows = [rowmap[i] for i in origin_rows] # vis row ID

                    spectra = numpy.zeros((len(rows), npol, nchan,), dtype=numpy.float32)
                    for (i, row) in enumerate(rows):
                        spectra[i] = tb.getcell(datacolumn, row).real
                    #get_mask_from_flagtra: 1 valid 0 invalid
                    #arg for mask_to_masklist: 0 valid 1 invalid
                    flaglist = [self.__convert_flags_to_masklist(tb.getcell('FLAG', row).astype(int))
                                for row in rows]

                    #LOG.trace("Flag Mask = %s" % str(flaglist))

                    spectra[:, :edge.left, :] = 0.0
                    spectra[:, nchan-edge.right:, :] = 0.0

                    # here we assume that masklist is polarization-independent
                    # (this is because that line detection/validation process accumulates
                    # polarization components together
                    masklist = [datatable.getcell('MASKLIST', idx) for idx in idxs]
                    #masklist = [datatable.getcell('MASKLIST',idxs[i]) + flaglist[i]
                    #            for i in range(len(idxs))]
                    #LOG.debug('DONE {}'.format(y))

                    npol = spectra.shape[1]
                    for pol in range(npol):
                        # MS rows contain npol spectra
                        if pol == 0:
                            index_list_total.extend(idxs)

                        # fit order determination
                        averaged_polyorder = self.fitorder_heuristic(
                            spectra[:, pol, :], [list(masklist[i]) + flaglist[i][pol] for i in range(len(idxs))], edge)
                        #del spectra

                        # write dummy baseline parameters and skip the subsequent calculations for fully flagged rows
                        if averaged_polyorder is None:
                            for irow in rows:
                                write_blparam( blparamfileobj, self._dummy_baseline_param( irow, pol ) )
                            continue

                        # fit order determination (cnt'd)
                        if fit_order == 'automatic' and self.MaxPolynomialOrder != 'none':
                            averaged_polyorder = min(averaged_polyorder, self.MaxPolynomialOrder)
                        #LOG.debug('time group {} pol {}: fitting order={}'.format(
                        #            y, pol, averaged_polyorder))

                        nrow = len(rows)
                        if DEBUG() or TRACE():
                            LOG.debug('nrow = {}'.format(nrow))
                            LOG.debug('len(idxs) = {}'.format(len(idxs)))

                        for i in range(nrow):
                            row = rows[i]
                            idx = idxs[i]
                            if TRACE():
                                LOG.trace('===== Processing at row = {} ====='.format(row))
                            #nochange = datatable.getcell('NOCHANGE',idx)
                            #LOG.trace('row = %s, Flag = %s'%(row, nochange))

                            # mask lines
                            maxwidth = 1
    #                       _masklist = masklist[i]
                            _masklist = list(masklist[i]) + flaglist[i][pol]
                            #LOG.info('_masklist = {}'.format(_masklist))
                            #LOG.info('masklist[{}] = {}'.format(i, masklist[i]))
                            #LOG.info('flaglist[{}][{}] = {}'.format(i, pol, flaglist[i][pol]))
                            #LOG.info('FLAG[{}][{}] = {}'.format(i, pol, ''.join(map(str, numpy.array(tb.getcell('FLAG', row)[pol], dtype=numpy.uint8)))))
                            for [chan0, chan1] in _masklist:
                                if chan1 - chan0 >= maxwidth:
                                    maxwidth = int((chan1 - chan0 + 1) / 1.4)
                                    # allowance in Process3 is 1/5:
                                    #    (1 + 1/5 + 1/5)^(-1) = (5/7)^(-1)
                                    #                         = 7/5 = 1.4
                            max_polyorder = int((nchan - sum(edge)) // maxwidth + 1)
                            if TRACE():
                                LOG.trace('Masked Region from previous processes = {}'.format(
                                    _masklist))
                                LOG.trace(f'edge parameters= ({edge.left}, {edge.right})')
                                LOG.trace('Polynomial order = {}  Max Polynomial order = {}'.format(averaged_polyorder, max_polyorder))

                            # fitting
                            polyorder = min(averaged_polyorder, max_polyorder)
                            mask_array[:] = base_mask_array
                            #LOG.info('mask_array = {}'.format(''.join(map(str, mask_array))))
                            #irow = len(row_list_total)+len(row_list)
                            #irow = len(index_list_total) + i
                            irow = row
                            param = self._configure_baseline_param(irow, pol, polyorder, nchan, edge, mask_array, _masklist)
                            if TRACE():
                                LOG.trace('Row {}: param={}'.format(row, param))
                            write_blparam(blparamfileobj, param)

        return blparam

    def _configure_baseline_param(self, row_idx: int, pol: int, polyorder: int, nchan: int, edge: fitorder.EdgeChannels, mask: Sequence[bool], mask_list: [List[List[int]]]) -> dict:
        """Configure baseline parameter values for given row and polarization incides.

        Args:
            row_idx: Row index
            pol: Polarization index (0: XX/RR, 1: YY/LL)
            polyorder: Polynomial fitting order
            nchan: Number of channels
            edge: Number of edge channels to be excluded from the fit
            mask: Boolean mask array
            mask_list: List of detected lines. Lines are excluded from the fit to protect them.
                       Lines are expressed as [start_channel, end_channel].

        Returns:
            Baseline fitting parameters as a dictionary
        """
        # Create mask for line protection
        effective_nchan = nchan - sum(edge)
        #LOG.info('__ mask (before) = {}'.format(''.join(map(str, mask))))

        # a stuff of masklist is a list of index [start, end]
        if isinstance(mask_list, (list, numpy.ndarray)):
            for [m0, m1] in mask_list:
                mask[max(0, m0):min(nchan, m1 + 1)] = 0
        else:
            LOG.critical('Invalid masklist')

        #LOG.info('__ mask (after)  = {}'.format(''.join(map(str, mask))))
        num_mask = int(effective_nchan - numpy.sum(mask[edge.left:nchan - edge.right] * 1.0))

        # here meaning of "masklist" is changed
        #         masklist: list of channel ranges to be *excluded* from the fit
        # fit_channel_list: list of channel ranges to be *included* in the fit
        fit_channel_list = self.__convert_mask_to_masklist(mask)
        #LOG.info('__ masklist (before)= {}'.format(masklist))
        #LOG.info('__ masklist (after) = {}'.format(fit_channel_list))

        if TRACE():
            LOG.trace('effective_nchan, num_mask, diff={}, {}'.format(
                effective_nchan, num_mask))

        outdata = self._get_fit_param(polyorder, nchan, edge, effective_nchan, num_mask, fit_channel_list)

        self.paramdict[BLP.ROW] = row_idx
        self.paramdict[BLP.POL] = pol

        # MASK, in short, fit_channel_list contains lists of indices [start, end+1]
        fit_channel_list = [[start, end - 1] for [start, end] in fit_channel_list]
        outdata[BLP.MASK] = as_maskstring(fit_channel_list)

        if TRACE():
            LOG.trace('outdata={}'.format(outdata))

        return outdata

    def _dummy_baseline_param( self, row: int, pol: int ) -> Dict[BLP, Union[int, float, str]]:
        """
        Create a dummy parameter dict for baseline parameters

        This replaces _calc_baseline_param() for fully flagged rows

        Args:
           row : row number
           pol : polarization index
        Returns:
           dummy parameter dict for baseline parameters
        """
        return {BLP.CLIPNITER: 1, BLP.CLIPTHRESH: 5.0,
                BLP.ROW: row, BLP.POL: pol, BLP.MASK: '', BLP.NPIECE: 1,
                BLP.FUNC: fitorder.FittingFunction.POLY.blfunc, BLP.ORDER: 1}

    def __convert_flags_to_masklist(self, flags: 'numpy.ndarray[numpy.ndarray[numpy.int64]]') -> List[List[List[int]]]:
        """
        Converts flag list to masklist.

        Args:
            flags : list of binary flag are loaded from FLAG column of MeasurementSet.

        Returns:
            list of masklist
        """
        return [self.__convert_mask_to_masklist(flag, 1) for flag in flags]

    def __convert_mask_to_masklist(self, mask: 'numpy.ndarray[numpy.int64]', end_offset: int=0) -> List[List[int]]:
        """
        Converts binary mask array to masklist / channellist for fitting.

        Resulting masklist is a list of channel ranges whose values are 1

        Argument
            mask : an array of channel mask in values 0 or 1
            end_offset : the offset value of the 'end' of [start, end]
        Returns
            A list of channel range [start, end]. It means below:
            - list of masking channel ranges to be *excluded* from the fit. __conver_flags_to_masklist() calls it.
              It consists of a pair of start and end index, [start, end].
            - list of fitting channel ranges to be *included* in the fit. ___calc_baseline_param() calls it.
              It consists of a pair of start and end+1 index, [start, end+1], for convenience of calculation.
        """
        # get indices of clump boundaries
        idx = (mask[1:] ^ mask[:-1]).nonzero()
        idx = (idx[0] + 1)
        # idx now contains pairs of start-end indices, edges need handling
        # depending on first and last mask value
        if mask[0]:
            if len(idx) == 0:
                return [[0, len(mask) - end_offset]]
            r = [[0, idx[0]]]
            if len(idx) % 2 == 1:
                r.extend(idx[1:].reshape(-1, 2).tolist())
            else:
                r.extend(idx[1:-1].reshape(-1, 2).tolist())
        else:
            if len(idx) == 0:
                return []
            if len(idx) % 2 == 1:
                r = (idx[:-1].reshape(-1, 2).tolist())
            else:
                r = (idx.reshape(-1, 2).tolist())
        if mask[-1]:
            r.append([idx[-1], len(mask)])
        return [[start, end - end_offset] for start, end in r]

    def _get_fit_param(self, polyorder: int, nchan: int, edge: fitorder.EdgeChannels, nchan_without_edge: int, nchan_masked: int, masklist: List[List[int]]):
        """Configure fitting parameter values except mask.

        This method constructs dictionary that holds baseline parameters
        specific to fitting. For polynomial fitting, fitting function and
        fitting order are included in the dictionary. For cubic spline
        (cspline) fitting, number of segments in addition to fitting function
        and fitting order are included. Fitting function can fallback to 'poly'
        even if input fitting function is 'cspline' when too many edge channels
        are flagged and user instructs to do so. Please see the implementation
        of SwitchPolynomialWhenLargeMaskAtEdgeHeuristic for detail about
        fallback.

        Args:
            polyorder: Polynomial fitting order
            nchan: Number of channels
            edge: Number of edge channels to be excluded from the fit
            nchan_without_edge: Effective number of channels
            nchan_masked: Number of masked channels
            masklist: List of detected lines. Lines are excluded from the fit to protect them.
                       Lines are expressed as [start_channel, end_channel].

        Returns:
            Baseline fitting parameter dictionary
        """
        if fitorder.is_polynomial_fit(self.fitfunc):
            self.paramdict[BLP.FUNC] = self.fitfunc.blfunc
            self.paramdict[BLP.ORDER] = polyorder
        elif fitorder.is_cubic_spline_fit(self.fitfunc):
            num_nomask = nchan_without_edge - nchan_masked
            num_pieces = max(int(min(polyorder * num_nomask / float(nchan_without_edge) + 0.5, 0.1 * num_nomask)), 1)
            if TRACE():
                LOG.trace('Cubic Spline Fit: Number of Sections = {}'.format(num_pieces))
            self.paramdict[BLP.NPIECE] = num_pieces

            fitfunc, order = self.switching_heuristic(
                self.heuristics_engine,
                nchan,
                edge,
                num_pieces,
                masklist
            )
            self.paramdict[BLP.FUNC] = fitfunc.blfunc
            self.paramdict[BLP.ORDER] = order
        else:
            RuntimeError('Should not happen!')

        return self.paramdict
