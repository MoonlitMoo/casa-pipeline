import numpy as np

import pipeline.infrastructure.logging as logging
from pipeline.infrastructure import casa_tools

LOG = logging.get_logger(__name__)


def create_V_array(n_pol: int, n_chan: int):
    """Create an empty array to hold the time averaged visibilities.

    Args:
        n_pol: int
            Number of polarizations.
        n_chan: int
            Number of channels.

    Returns:
        numpy.ma
            Empty numpy masked array
    """
    # Create variables and storage space for V_k
    result_dtype = [
        ('antenna', np.dtype('int32')),
        ('corrected_data', np.dtype('complex128'), (n_pol, n_chan)),
        ('time', np.dtype('float64')),
        ('sigma', np.dtype('complex128'), (n_pol, n_chan)),
        ('chan_freq', np.dtype('float64'), (n_chan,)),
        ('resolution', np.dtype('float64'), (n_chan,))
    ]
    # new numpy array to hold visibilities V
    return np.ma.empty((0,), dtype=result_dtype)


class MSWrapper(object):
    """
    MSWrapper is a wrapper around a NumPy array populated with measurement set
    data for a specified scan and spectral window. The MSWrapper can be
    filtered on various criteria, e.g, spw, scan, antenna, etc., to narrow the
    data to a particular data selection.

    The static method MSWrapper.create_from_ms should be used to instantiate
    MSWrapper objects.

    The static methods MSWrapper.create_averages_from_ms and
    MSWrapper.create_averages_from_combination can also be used to create a new
    instance of a MSWrapper object.
    """

    @staticmethod
    def create_from_ms(filename, scan, spw):
        """
        Create a new MSWrapper for the specified scan and spw.

        Reading in raw measurement set data can be a very memory-intensive
        process, so data selection is deliberately limited to one scan and one
        spw at a time.

        :param filename: measurement set filename
        :param scan: integer scan ID
        :param spw: integer spw ID
        :return:
        """
        LOG.trace('MSWrapper.create_from_ms(%r, %r, %r)', filename, scan, spw)

        data_selection = {"scan": str(scan), "spw": str(spw)}
        colnames = ['antenna1', 'antenna2', 'flag', 'time', 'corrected_amplitude', 'corrected_data', 'corrected_phase']

        with casa_tools.MSReader(filename) as openms:
            openms.msselect(data_selection)
            raw_data = openms.getdata(colnames)
            axis_info = openms.getdata(['axis_info'])
            num_rows = openms.nrow(selected=True)

        corr_axis = axis_info['axis_info']['corr_axis']
        freq_axis = axis_info['axis_info']['freq_axis']

        scalar_cols = [c for c in colnames if len(raw_data[c].shape) == 1]
        var_cols = [c for c in colnames if c not in scalar_cols]

        # data has axis order pol->channel->time. Swap order to a more natural time->pol->channel
        for c in var_cols:
            raw_data[c] = raw_data[c].swapaxes(0, 2).swapaxes(1, 2)

        dtypes = {c: get_dtype(raw_data, c) for c in colnames}

        col_dtypes = [dtypes[c] for c in dtypes if dtypes[c] is not None]
        data = np.ma.empty(num_rows, dtype=col_dtypes)

        for c in scalar_cols:
            data[c] = raw_data[c]

        # convert to NumPy MaskedArray if FLAG column is present
        mask = raw_data['flag']
        var_cols_to_mask = [c for c in var_cols if c != 'flag']
        for c in var_cols_to_mask:
            data[c] = np.ma.MaskedArray(data=raw_data[c], dtype=raw_data[c].dtype, mask=mask)

        return MSWrapper(filename, scan, spw, data, corr_axis, freq_axis)

    def filter(self, antenna1=None, antenna2=None, **kwargs):
        """
        Return a new MSWrapper containing rows matching the column selection
        criteria.

        Data for rows meeting all the column criteria will be funnelled into
        the new MSWrapper return object. A boolean AND is effectively
        performed: e.g., antenna1=3 AND antenna2=5 will only return rows for
        one baseline.

        Data can be filtered on any column listed in the wrapper.data.dtype.
        """
        mask_args = dict(kwargs)

        # create a mask that lets all data through for columns that are not
        # specified as arguments, or just the specified values through for
        # columns that are specified as arguments
        def passthrough(k, column_name):
            if k is None:
                if column_name not in kwargs:
                    mask_args[column_name] = np.ma.unique(self[column_name])
            else:
                mask_args[column_name] = k

        for arg, column_name in [(antenna1, 'antenna1'), (antenna2, 'antenna2')]:
            passthrough(arg, column_name)

        # combine masks to create final data selection mask
        mask = np.ones(len(self))
        for k, v in mask_args.items():
            mask = (mask == 1) & (self._get_mask(v, k) == 1)

        # find data for the selection mask
        data = self[mask]

        # create new object for the filtered data
        return MSWrapper(self.filename, self.scan, self.spw, data, self.corr_axis, self.freq_axis)

    def xor_filter(self, antenna1=None, antenna2=None, **kwargs):
        """
        Return a new MSWrapper containing rows matching the column selection
        criteria.

        Data for rows meeting any column criteria will be funnelled into
        the new MSWrapper return object. A boolean AND is effectively
        performed: e.g., antenna1=3 AND antenna2=5 will only return rows for
        one baseline.

        Data can be filtered on any column listed in the wrapper.data.dtype.

        DANGER! DANGER! DANGER!

        NOTE! This class has only been tested for baseline selection! Using
        this method for other use cases could be dangerous. Use at your own
        risk!
        """

        # TODO this method could probably be refactored to use numpy xor, but
        # I don't have time right now...

        mask_args = dict(kwargs)

        # create a mask that lets all data through for columns that are not
        # specified as arguments, or just the specified values through for
        # columns that are specified as arguments
        def passthrough(k, column_name):
            if k is None:
                if column_name not in kwargs:
                    mask_args[column_name] = np.unique(self[column_name])
            else:
                mask_args[column_name] = k

        for arg, column_name in [(antenna1, 'antenna1'), (antenna2, 'antenna2')]:
            passthrough(arg, column_name)

        # combine masks to create final data selection mask
        mask = np.zeros(len(self))
        for k, v in mask_args.items():
            mask = (mask == 1) | (self._get_mask(v, k) == 1)

        # remove autocorrelations
        mask = (mask == 1) & (self['antenna1'] != self['antenna2'])

        # find data for the selection mask
        data = self[mask]

        # create new object for the filtered data
        return MSWrapper(self.filename, self.scan, self.spw, data, self.corr_axis, self.freq_axis)

    @staticmethod
    def create_averages_from_ms(filename, scan, spw, memlim, perantave=True):
        """
        Create a new MSWrapper for the specified scan and spw.

        Reading in raw measurement set data can be a very memory-intensive
        process, so data selection is deliberately limited to one scan and one
        spw at a time.

        :param filename: measurement set filename
        :param scan: integer scan ID
        :param spw: integer spw ID
        :param memlim: Limit for memory for data buffer use, given in Gigabytes
        :param perantave: Compute per antenna average (default True)
        :type perantave: bool
        :return: MSWrapper instance
        """
        # Method added as part of PIPE-687
        LOG.trace('MSWrapper.create_averages_from_ms(%r, %r, %r, %r, %r)',
                  filename, scan, spw, memlim, perantave)

        # Epsilon used to avoid division by zero in sigma calculations
        epsilon = 1.e-6

        # put memory limit in bytes
        memlim = int(memlim * (1024.0 ** 3))

        # Initialize necessary column name lists
        col_names = ['antenna1', 'antenna2', 'flag', 'time', 'corrected_data']
        # Data type for these columns
        col_dtypes = [
            np.dtype('int32'),
            np.dtype('int32'),
            np.dtype('bool'),
            np.dtype('float64'),
            np.dtype('complex128')
        ]
        # Data size per element for each column, in bytes
        col_dsizes = np.array([c.itemsize for c in col_dtypes], dtype='int32')
        # Is polarization a dimension of the column? 1=True, 0=False
        poldim = np.array([0, 0, 1, 0, 1], dtype='int32')
        # Is frequency a dimension of the column? 1=True, 0=False
        freqdim = np.array([0, 0, 1, 0, 1], dtype='int32')
        # Total size of the whole piece of data should be:
        # Size = nrows*Sum( dsizes*(npols*poldim+1)*(nchan*freqdim+1) )(over number of colnames)

        # commented out from copied PL code
        with casa_tools.MSReader(filename) as openms:

            # select data for this scan
            data_selection = {"scan": str(scan), "spw": str(spw)}
            openms.msselect(data_selection)
            md = openms.metadata()
            antennaids = md.antennaids()
            nant = len(antennaids)
            nbl = nant * (nant - 1) / 2 + nant
            nrows = openms.nrow(selected=True)
            ntstamps = nrows / nbl
            axis_info = openms.getdata(['axis_info'])
            # Create frequency axes to be introduced in output V
            corr_axis = axis_info['axis_info']['corr_axis']
            freq_axis = axis_info['axis_info']['freq_axis']
            # get 1D array of channel frequencies and include its definition in the dtype
            chan_freq = freq_axis['chan_freq']
            chan_freq = chan_freq.swapaxes(0, 1)[0]
            # get 1D array of channel widths and include the column in the dtype
            resolution = freq_axis['resolution']
            resolution = resolution.swapaxes(0, 1)[0]

            npol = len(corr_axis)
            nchan = len(freq_axis['chan_freq'])
            # Data size per row and for the whole scan
            rowdatasize = np.sum(col_dsizes * (npol * poldim + 1) * (nchan * freqdim + 1))
            scandatasize = nrows * rowdatasize
            # Calculate the number of iteration that need to be done to go over
            # the entire piece of data for this scan,spw
            niter = int(np.ceil(1.0 * scandatasize / memlim))
            nrowsbuffer = int(np.floor(1.0 * memlim / rowdatasize))

            LOG.debug('Scan {0:d} has {1:d} rows ({2:.3f} Gb), memory limit is set to {3:.3f} Gb'.format(
                scan, nrows, 1.0 * scandatasize / (1024.0 ** 3), 1.0 * memlim / (1024.0 ** 3)))
            LOG.debug('reading data in {0:d} chunks of {1:d} rows'.format(niter, nrowsbuffer))

            # new numpy array to hold visibilities V
            V = create_V_array(npol, nchan)
            # Partial sums storage variables
            realdatasum = {ant: np.ma.zeros((npol, nchan), dtype=np.dtype('float64')) for ant in antennaids}
            imagdatasum = {ant: np.ma.zeros((npol, nchan), dtype=np.dtype('float64')) for ant in antennaids}
            realdatasqsum = {ant: np.ma.zeros((npol, nchan), dtype=np.dtype('float64')) for ant in antennaids}
            imagdatasqsum = {ant: np.ma.zeros((npol, nchan), dtype=np.dtype('float64')) for ant in antennaids}
            # ndata is a counter for the amount of data being averaged
            ndata = {ant: np.ma.zeros((npol, nchan), dtype=np.dtype('float64')) for ant in antennaids}

            # Sigma is a function of sqrt(num_antennas - 1). Calculate and cache this value now.
            norm_sigma_tmaverage = np.sqrt(ntstamps * (nant - 1.0))

            # return ([nrows, npols, nchan], [memlim, rowdatasize, scandatasize], [niter, nrowsbuffer])
            # Load data in chunks of 'nrowsbuffer' number of rows
            openms.iterinit(maxrows=nrowsbuffer)
            do_next = openms.iterorigin()

            while do_next:
                raw_data = openms.getdata(col_names)
                # Create maked array of corrected and masked data
                # data has axis order pol->channel->time. Swap order to a more natural time->pol->channel
                data = np.ma.MaskedArray(
                    data=raw_data['corrected_data'].swapaxes(0, 2).swapaxes(1, 2),
                    dtype=np.dtype('complex128'),
                    mask=raw_data['flag'].swapaxes(0, 2).swapaxes(1, 2)
                )
                # Iterate over antennas, selecting rows of data for averaging
                for antenna in antennaids:
                    sel = np.logical_xor(raw_data['antenna1'] == antenna, raw_data['antenna2'] == antenna)
                    seldatareal = data[sel].real
                    seldataimag = data[sel].imag
                    # If the per antenna adjustment is selected, invert the sign of the imaginary part when the
                    # antenna is in position 2
                    if perantave:
                        ant2sel = raw_data['antenna2'][sel]
                        sel2 = np.where(ant2sel == antenna)[0]
                        for j in sel2:
                            seldataimag[j, :, :] *= -1.0
                    # Accumulate the sum of data averaged over time and baselines (MS rows)
                    realseldata = seldatareal.sum(axis=0)
                    imagseldata = seldataimag.sum(axis=0)
                    # Perform the the stacking to existing sums with np.ma.sum() to avoid
                    # propagating masking over the cumulative sum
                    realdatasum[antenna] = np.ma.MaskedArray([realdatasum[antenna], realseldata]).sum(axis=0)
                    imagdatasum[antenna] = np.ma.MaskedArray([imagdatasum[antenna], imagseldata]).sum(axis=0)
                    # and count the number of unmasked datapoints accumulated
                    ndata[antenna] = np.ma.MaskedArray([ndata[antenna], data[sel].count(axis=0)]).sum(axis=0)
                    # Calculate sum of squares over time and baselines, separated by real and imaginary
                    realdatasq = np.square(seldatareal).sum(axis=0)
                    imagdatasq = np.square(seldataimag).sum(axis=0)
                    # Perform the the stacking to existing sums of squares
                    realdatasqsum[antenna] = np.ma.MaskedArray([realdatasqsum[antenna], realdatasq]).sum(axis=0)
                    imagdatasqsum[antenna] = np.ma.MaskedArray([imagdatasqsum[antenna], imagdatasq]).sum(axis=0)
                # Jump to next chunk of data
                do_next = openms.iternext()

            # For pixels with no unmasked data accumulated, make sure those
            # pixels are filled with some epsilon dummy value
            for antenna in antennaids:
                zerosel = ndata[antenna].data < 1.0
                ndata[antenna].data[zerosel] = epsilon
                ndata[antenna].mask += zerosel

            # Iterate over antennas, now calculating the mean and sigma of the data
            # creating the variable V to be returned as output
            for antenna in antennaids:
                # Compute time averaged visibilities (Vk arrays)
                V_k = np.ma.empty((1,), dtype=V.data.dtype)

                # add antenna and channel frequencies to the row for this antenna
                V_k['antenna'] = antenna
                V_k['chan_freq'] = chan_freq
                V_k['resolution'] = resolution

                # Average data over time by taking sum/N
                V_k['corrected_data'] = (realdatasum[antenna] + 1j * imagdatasum[antenna]) / ndata[antenna]

                # Equation 2: sigma_{k}(nu_{i}) = std(V_{jk}(nu_{i}))_{j} / sqrt(n_{ant})
                # calculate this from the sum of data and data squared:
                # sigma = sqrt( ( sum_squared_values + (1/N)*sum_values^2 ) / (N-1) )
                # where this N is the sum of accumulated values
                sigma_k_real = np.ma.sqrt(
                    (realdatasqsum[antenna] - np.square(realdatasum[antenna]) / ndata[antenna]) / (ndata[antenna] - 1.0)
                )
                sigma_k_imag = np.ma.sqrt(
                    (imagdatasqsum[antenna] - np.square(imagdatasum[antenna]) / ndata[antenna]) / (ndata[antenna] - 1.0)
                )
                # apply final formula from sigma values
                V_k['sigma'] = (sigma_k_real + 1j * sigma_k_imag) / norm_sigma_tmaverage

                # Add antenna averaged data to output array V
                V = np.ma.concatenate((V, V_k), axis=0)

        return MSWrapper(filename, scan, spw, None, corr_axis, freq_axis, V=V)

    @staticmethod
    def create_averages_from_combination(mswlist):
        """
        Calculate and return the average MSWrapper of the list of MSWrapper objects given as input.
        The 'sigma' column get filled with the standard error of the mean, by adding inverse squared variances
        of each of the objects in the list.

        :mswlist: Python List of MSWrapper objects to combine. All of them must be from the same
                  dataset and SPW, otherwise a dummy object is returned.
        :return: MSWrapper object
        """
        # Method added as part of PIPE-687
        LOG.trace('MSWrapper.create_averages_from_combination(%r)', mswlist)

        # Get data from first MSWrapper element of the list, scan will be the list of scans
        # discard raw data if present
        nscans = len(mswlist)
        scan = [mswlist[idxscan].scan for idxscan in range(nscans)]
        filename = mswlist[0].filename
        spw = mswlist[0].spw
        corr_axis = mswlist[0].corr_axis
        freq_axis = mswlist[0].freq_axis
        (nant, npol, nchan) = np.shape(mswlist[0].V['corrected_data'])
        # Check all elements of the list are compatible, if not, return dummy MSWrapper object
        if not (np.all([mswlist[idxscan].spw == mswlist[0].spw for idxscan in range(nscans)]) and
                np.all([mswlist[idxscan].filename == mswlist[0].filename for idxscan in range(nscans)]) and
                np.all([mswlist[idxscan].corr_axis == mswlist[0].corr_axis for idxscan in range(nscans)]) and
                np.all(
                    [np.all([mswlist[idxscan].freq_axis['chan_freq'], mswlist[0].freq_axis['chan_freq']]) for idxscan in
                     range(nscans)]) and
                np.all(
                    [np.all([mswlist[idxscan].freq_axis['resolution'], mswlist[0].freq_axis['resolution']]) for idxscan
                     in range(nscans)])):
            # TODO: Should we raise an error here?
            LOG.warning('List of MSWrapper objects is not compatible. This task can only combine from same MS and SPW!')
            return MSWrapper(None, None, None, None, None, None, V=None)

        # new numpy array to hold visibilities V
        V = create_V_array(npol, nchan)

        # Average data
        for ant in range(nant):
            # Compute time averaged visibilities (Vk arrays)
            V_k = np.ma.empty((1,), dtype=V.data.dtype)
            # add antenna and channel frequencies to the row for this antenna from first scan
            V_k['antenna'] = ant
            V_k['chan_freq'] = mswlist[0].V['chan_freq'][ant]
            V_k['resolution'] = mswlist[0].V['resolution'][ant]

            # Calculate mean and sigma for this antenna

            data = [mswlist[idxscan].V['corrected_data'][ant, :, :] for idxscan in range(nscans)]
            datamean = np.ma.mean(data, axis=0)
            invsigmasqreal = [1.0 / (mswlist[idxscan].V['sigma'][ant, :, :].real ** 2) for idxscan in range(nscans)]
            invsigmasqimag = [1.0 / (mswlist[idxscan].V['sigma'][ant, :, :].imag ** 2) for idxscan in range(nscans)]
            sigmameanreal = 1.0 / np.ma.sqrt(np.ma.sum(invsigmasqreal, axis=0))
            sigmameanimag = 1.0 / np.ma.sqrt(np.ma.sum(invsigmasqimag, axis=0))
            V_k['corrected_data'] = datamean
            V_k['sigma'] = sigmameanreal + 1.j * sigmameanimag

            # Add antenna averaged data to output array V
            V = np.ma.concatenate((V, V_k), axis=0)

        return MSWrapper(filename, scan, spw, None, corr_axis, freq_axis, V=V)

    def __init__(self, filename, scan, spw, data, corr_axis, freq_axis, V=None):
        self.filename = filename
        self.scan = scan
        self.spw = spw
        self.data = data
        self.corr_axis = corr_axis
        self.freq_axis = freq_axis
        self.V = V

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data.dtype.names

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return (i for i in self.data)

    def _get_mask(self, allowed, column):
        try:
            iter(allowed)
        except TypeError:
            allowed = [allowed]
        mask = np.zeros(len(self))
        for a in allowed:
            if a not in self.data[column]:
                raise KeyError('{} column {} value not found: {}'.format(self.filename, column, a))
            mask = (mask == 1) | (self[column] == a)
        return mask


def get_dtype(data, column_name):
    """
    Get the numpy data type for a CASA caltable column.

    :param data: CASA table tool with caltable open.
    :param column_name: name of column to process
    :return: 3-tuple of column name, NumPy dtype, column shape
    """
    column_data = data[column_name]
    column_dtype = column_data.dtype
    column_shape = column_data.shape

    if len(column_shape) == 1:
        return column_name, column_dtype

    return column_name, column_dtype, column_shape[1:]


def calc_vk(wrapper):
    """
    Return a NumPy array containing time-averaged visibilities for each
    baseline in the input MSWrapper.

    :param wrapper: MSWrapper to process
    :return:
    """
    # PIPE-687: This function was moved from ampphase_vs_freq_qa.py. It is here to test the consistency
    #  of the old and new code. This may be removed in the future once the accuracy of the output has
    #  been validated.

    # find indices of all antennas
    antenna1 = set(wrapper['antenna1'])
    antenna2 = set(wrapper['antenna2'])
    all_antennas = antenna1.union(antenna2)

    # Sigma is a function of sqrt(num_antennas - 1). Calculate and cache this value now.
    root_num_antennas = np.sqrt(len(all_antennas) - 1)

    # columns in this list are omitted from V_k
    excluded_columns = ['antenna1', 'antenna2', 'corrected_phase', 'flag']

    # create a new dtype that adds 'antenna' and 'sigma' columns, filtering out columns we want to omit
    column_names = [c for c in wrapper.data.dtype.names if c not in excluded_columns]
    result_dtype = [get_dtype(wrapper.data, c) for c in column_names]
    result_dtype.insert(0, ('antenna', np.int32))
    result_dtype.append(('sigma', wrapper['corrected_data'].dtype, wrapper['corrected_data'].shape[1:]))

    # get 1D array of channel frequencies and include its definition in the dtype
    chan_freq = wrapper.freq_axis['chan_freq']
    chan_freq = chan_freq.swapaxes(0, 1)[0]
    result_dtype.append(('chan_freq', chan_freq.dtype, chan_freq.shape))

    # get 1D array of channel widths and include the column in the dtype
    resolution = wrapper.freq_axis['resolution']
    resolution = resolution.swapaxes(0, 1)[0]
    result_dtype.append(('resolution', resolution.dtype, resolution.shape))

    # new numpy array to hold visibilities V
    V = np.ma.empty((0,), dtype=result_dtype)

    for k in all_antennas:
        # create new row to hold all data for this antenna
        V_k = np.ma.empty((1,), dtype=V.data.dtype)

        # add antenna and channel frequencies to the row for this antenna
        V_k['antenna'] = k
        V_k['chan_freq'] = chan_freq
        V_k['resolution'] = resolution

        # Equation 2: sigma_{k}(nu_{i}) = std(V_{jk}(nu_{i}))_{j} / sqrt(n_{ant})
        # select all visibilities created using this antenna.
        V_jk = wrapper.xor_filter(antenna1=k, antenna2=k)
        sigma_k_real = V_jk['corrected_data'].real.std(axis=0) / root_num_antennas
        sigma_k_imag = V_jk['corrected_data'].imag.std(axis=0) / root_num_antennas
        V_k['sigma'] = sigma_k_real + 1j * sigma_k_imag

        # add the remaining columns
        for col in column_names:
            V_k[col] = V_jk[col].mean(axis=0)

        V = np.ma.concatenate((V, V_k), axis=0)

    return V
