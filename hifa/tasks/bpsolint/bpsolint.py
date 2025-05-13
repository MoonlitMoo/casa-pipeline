import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common import calibrationtableaccess as caltableaccess
from pipeline.hifa.heuristics import snr as snr_heuristics
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class BpSolintInputs(vdp.StandardInputs):

    @vdp.VisDependentProperty
    def field(self):
        # Return field names in the current ms that have been
        # observed with the desired intent
        fields = self.ms.get_fields(intent=self.intent)
        fieldids = set(sorted([f.id for f in fields]))
        fieldnames = []
        for fieldid in fieldids:
            field = self.ms.get_fields(field_id=fieldid)
            fieldnames.append(field[0].name)
        field_names = set(fieldnames)
        return ','.join(field_names)

    intent = vdp.VisDependentProperty(default='BANDPASS')

    @vdp.VisDependentProperty
    def spw(self):

        # Get the science spw ids
        sci_spws = {spw.id for spw in self.ms.get_spectral_windows(science_windows_only=True)}

        # Get the bandpass spw ids
        bandpass_spws = []
        for scan in self.ms.get_scans(scan_intent=self.intent):
            bandpass_spws.extend(spw.id for spw in scan.spws)
        bandpass_spws = set(bandpass_spws).intersection(sci_spws)

        # Get science target spw ids
        target_spws = []
        for scan in self.ms.get_scans(scan_intent='TARGET'):
            target_spws.extend([spw.id for spw in scan.spws])
        target_spws = set(target_spws).intersection(sci_spws)

        # Compute the intersection of the bandpass and science target spw
        # ids
        spws = list(bandpass_spws.intersection(target_spws))
        spws = [str(spw) for spw in sorted(spws)]
        return ','.join(spws)

    phaseupsnr = vdp.VisDependentProperty(default=20.0)
    minphaseupints = vdp.VisDependentProperty(default=2)
    evenbpints = vdp.VisDependentProperty(default=False)
    bpsnr = vdp.VisDependentProperty(default=50.0)
    minbpsnr = vdp.VisDependentProperty(default=20.0)
    minbpnchan = vdp.VisDependentProperty(default=8)
    hm_nantennas = vdp.VisDependentProperty(default='unflagged')
    maxfracflagged = vdp.VisDependentProperty(default=0.90)

    def __init__(self, context, output_dir=None, vis=None, field=None,
                 intent=None, spw=None, phaseupsnr=None, minphaseupints=None,
                 evenbpints=None, bpsnr=None, minbpsnr=None, minbpnchan=None, hm_nantennas=None, maxfracflagged=None):

        super(BpSolintInputs, self).__init__()

        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.field = field
        self.intent = intent
        self.spw = spw

        self.phaseupsnr = phaseupsnr
        self.minphaseupints = minphaseupints
        self.evenbpints = evenbpints
        self.bpsnr = bpsnr
        self.minbpsnr = minbpsnr
        self.minbpnchan = minbpnchan
        self.hm_natennas = hm_nantennas
        self.maxfracflagged = maxfracflagged


@task_registry.set_equivalent_casa_task('hifa_bpsolint')
@task_registry.set_casa_commands_comment('Compute the best per spw bandpass solution intervals.')
class BpSolint(basetask.StandardTaskTemplate):
    Inputs = BpSolintInputs

    def prepare(self, **parameters):

        # Simplify the inputs
        inputs = self.inputs

        # Turn the CASA field name and spw id lists into Python lists
        fieldlist = inputs.field.split(',')
        spwlist = [int(spw) for spw in inputs.spw.split(',')]

        # Setup BP SNR
        bpsnr = inputs.bpsnr if 'bpsnr' not in parameters else parameters['bpsnr']

        # Log the data selection choices
        LOG.info('Estimating bandpass solution intervals for MS %s' % inputs.ms.basename)
        LOG.info('    Setting bandpass intent to %s ' % inputs.intent)
        LOG.info('    Selecting bandpass fields %s ' % fieldlist)
        LOG.info('    Selecting bandpass spws %s ' % spwlist)
        LOG.info('    Setting requested phaseup snr to %0.1f ' % inputs.phaseupsnr)
        LOG.info('    Setting requested bandpass snr to %0.1f ' % bpsnr)
        if len(fieldlist) <= 0 or len(spwlist) <= 0:
            LOG.info('    No bandpass data')
            return BpSolintResults(vis=inputs.vis)

        # Compute the bandpass solint parameters and return a solution
        # dictionary
        solint_dict = snr_heuristics.estimate_bpsolint(inputs.ms, fieldlist,
                                                       inputs.intent, spwlist, inputs.hm_nantennas,
                                                       inputs.maxfracflagged,
                                                       inputs.phaseupsnr, inputs.minphaseupints, bpsnr,
                                                       inputs.minbpnchan, evenbpsolints=inputs.evenbpints)

        if not solint_dict:
            LOG.info('No solution interval dictionary')
            return BpSolintResults(vis=inputs.vis)

        # Check the existence of strong atmospheric line and recalculate
        # solution interval with lower bpsnr if necessary
        if bpsnr > inputs.minbpsnr and \
            self._get_max_solint_channels(solint_dict) >= 2:
            # check if Tsys caltable exists in context
            caltable = self._get_tsys_caltable(inputs.ms.name)
            if caltable is None:
                LOG.warning("No Tsys calibration table found for MS %s. Skipping strong atmospheric line check." %
                            inputs.ms.basename)
            elif check_strong_atm_lines(inputs.ms, fieldlist, inputs.intent, spwlist, solint_dict, caltable):
                LOG.info("Strong atmospheric line(s) detected in Tsys spectra. Recalculating solution interval with bpsnr = %f" % inputs.minbpsnr)
                return self.prepare(bpsnr=inputs.minbpsnr)

        # Construct the results object
        result = self._get_results(inputs.vis, spwlist, solint_dict)

        # Return the results
        return result

    def analyse(self, result):
        return result

    # Get final results from the spw dictionary
    @staticmethod
    def _get_results(vis, spwidlist, solint_dict):

        # Initialize result structure.
        result = BpSolintResults(vis=vis, spwids=spwidlist)

        # Initialize the lists
        phsolints = []
        phintsolints = []
        nphsolutions = []
        phsensitivities = []
        phintsnrs = []
        exptimes = []

        bpsolints = []
        bpchansolints = []
        nbpsolutions = []
        bpsensitivities = []
        bpchansnrs = []
        bandwidths = []

        # Loop over the spws. Values for spws with
        # not dictionary entries are set to None
        for spwid in spwidlist:

            if spwid not in solint_dict:

                phsolints.append(None)
                phintsolints.append(None)
                nphsolutions.append(None)
                phsensitivities.append(None)
                phintsnrs.append(None)
                exptimes.append(None)

                bpsolints.append(None)
                bpchansolints.append(None)
                nbpsolutions.append(None)
                bpsensitivities.append(None)
                bpchansnrs.append(None)
                bandwidths.append(None)

            else:

                phsolints.append(solint_dict[spwid]['phaseup_solint'])
                phintsolints.append(solint_dict[spwid]['nint_phaseup_solint'])
                nphsolutions.append(solint_dict[spwid]['nphaseup_solutions'])
                phsensitivities.append('%fmJy' % solint_dict[spwid]['sensitivity_per_integration_mJy'])
                phintsnrs.append(solint_dict[spwid]['snr_per_integration'])
                exptimes.append('%fs' % (60*solint_dict[spwid]['exptime_minutes']))

                bpsolints.append(solint_dict[spwid]['bpsolint'])
                bpchansolints.append(solint_dict[spwid]['nchan_bpsolint'])
                nbpsolutions.append(solint_dict[spwid]['nbandpass_solutions'])
                bpsensitivities.append('%fmJy' % solint_dict[spwid]['sensitivity_per_channel_mJy'])
                bpchansnrs.append(solint_dict[spwid]['snr_per_channel'])
                bandwidths.append('%fHz' % solint_dict[spwid]['bandwidth'])

        # Populate the result.
        result.phsolints = phsolints
        result.phintsolints = phintsolints
        result.nphsolutions = nphsolutions
        result.phsensitivities = phsensitivities
        result.phintsnrs = phintsnrs
        result.exptimes = exptimes

        result.bpsolints = bpsolints
        result.bpchansolints = bpchansolints
        result.nbpsolutions = nbpsolutions
        result.bpchansensitivities = bpsensitivities
        result.bpchansnrs = bpchansnrs
        result.bandwidths = bandwidths

        return result

    @staticmethod
    def _get_max_solint_channels(solint_dict):
        """
        The method returns maximum BP solution interval of all SPWs
        in solution interval dictionary (solint_dict)

        Inputs: solution interval dictionary returned by snr.estimate_bpsolint
        """
        max_solint = 0
        for spw_solint in solint_dict.values():
            max_solint = max(spw_solint['nchan_bpsolint'], max_solint)
        return max_solint

    def _get_tsys_caltable(self, vis):
        caltables = self.inputs.context.callibrary.active.get_caltable(
            caltypes='tsys')

        # return just the tsys table that matches the vis being handled
        result = None
        for name in caltables:
            # Get the tsys table name
            tsystable_vis = caltableaccess.CalibrationTableDataFiller._readvis(name)
            if tsystable_vis in vis:
                result = name
                break

        return result        


class BpSolintResults(basetask.Results):
    def __init__(self, vis=None, spwids=[],
                 phsolints=[], phintsolints=[], nphsolutions=[],
                 phsensitivities=[], phintsnrs=[], exptimes = [],
                 bpsolints=[], bpchansolints=[], nbpsolutions=[],
                 bpsensitivities=[], bpchansnrs=[], bandwidths = []):
        """
        Initialise the results object.
        """
        super(BpSolintResults, self).__init__()

        self.vis = vis

        # Spw list
        self.spwids = spwids

        # Phaseup solutions
        self.phsolints = phsolints
        self.phintsolints = phintsolints
        self.nphsolutions = nphsolutions
        self.phsensitivities = phsensitivities
        self.phintsnrs = phintsnrs
        self.exptimes = exptimes

        # Bandpass solutions
        self.bpsolints = bpsolints
        self.bpchansolints = bpchansolints
        self.nbpsolutions = nbpsolutions
        self.bpchansensitivities = bpsensitivities
        self.bpchansnrs = bpchansnrs
        self.bandwidths = bandwidths

    def __repr__(self):
        if self.vis is None or not self.spwids:
            return ('BpSolintResults:\n'
                    '\tNo bandpass solution intervals computed')
        else:
            line = 'BpSolintResults:\nvis %s\n' % (self.vis)
            line = line + 'Phaseup solution time intervals\n'
            for i in range(len(self.spwids)):
                line = line + \
                       "    spwid %2d solint '%s' intsolint %2d sensitivity %s intsnr %0.1f\n" % \
                       (self.spwids[i], self.phsolints[i], self.phintsolints[i],
                        self.phsensitivities[i], self.phintsnrs[i])
            line = line + 'Bandpass frequency solution intervals\n'
            for i in range(len(self.spwids)):
                line = line + \
                       "    spwid %2d solint '%s' channels %2d sensitivity %s chansnr %0.1f\n" % \
                       (self.spwids[i], self.bpsolints[i], self.bpchansolints[i],
                        self.bpchansensitivities[i], self.bpchansnrs[i])
            return line


def check_strong_atm_lines(ms, fieldlist, intent, spwidlist, solint_dict, tsysname, lineStrengthThreshold=0.1,
                           minAdjacantChannels=3, nSigma=8):
    """
    This function tests if existence of strong atmospheric lines in Tsys spectra
    (see CAS-11951).

    Inputs:
        ms: Measurementset object
        fieldlist: a list of field names
        intents: an intent string
        spwidlist: a list of spw IDs
        solint_dict: solution interval dictionary returned by snr.estimate_bpsolint
        tsysname: the name of Tsys caltable

    Conditions of strong atmospheric line:
        (a) the peak of atmospheric line components is larger than a threshold, AND
        (b) there is at least an atmospheric line with it's width larger than a threshold

    Returns: True if any of SpW meets condition of strong atmospheric lines
    """
    LOG.info('Check for strong atmospheric line in Tsys spectra of fields, %s, in %s' % \
             (str(fieldlist), ms.basename))
    # make sure MS and Tsys caltable corresponds
    tsystable_vis = caltableaccess.CalibrationTableDataFiller._readvis(tsysname)
    if tsystable_vis != ms.name:
        raise RuntimeError("Input MS ({}) and Tsys caltable ({}) does not correspond."
                           "".format(os.path.basename(ms.name), os.path.basename(tsysname)))

    tsys_info = snr_heuristics.get_tsysinfo(ms, fieldlist, intent, spwidlist)

    strong_atm_lines = False
    for spw in spwidlist:
        if spw not in tsys_info or spw not in solint_dict:
            continue
        tsys_spw = solint_dict[spw]['tsys_spw']
        LOG.info('Investigating Tsys spectra for spw %d (Tsys spw %d)' % (spw, tsys_spw))

        # Obtain median Tsys spectrum
        scanobj = ms.get_scans(scan_id = tsys_info[spw]['tsys_scan'])[0]
        atmfields = ms.get_fields(intent='ATMOSPHERE')
        fieldids = [fobj.id for fobj in scanobj.fields.intersection(frozenset(atmfields))]
        median_tsys = get_median_tsys_spectrum_from_caltable(tsysname, tsys_spw, fieldids[0])
        if median_tsys is None:
            LOG.warning('Unable to define median Tsys spectrum for Tsys spw = %d, scan = %d' %
                        (tsys_spw, tsys_info[spw]['tsys_scan']))
            continue

        # Smooth median Tsys spectrum with kernel size, # of Tsys chans /16
        kernel_width = len(median_tsys) // 16
        kernel = numpy.ones(kernel_width, dtype=float)/float(kernel_width)
        LOG.debug("Subtracting smoothed Tsys spectrum (kernel = %d channels)" % kernel_width)
        smoothed_tsys = numpy.convolve(median_tsys, kernel, mode='same')

        # Define an index range to avoid edge effect of smoothing
        idx_offset = kernel_width // 2
        idx_range = slice(idx_offset, idx_offset + numpy.abs(len(median_tsys)-kernel_width) + 1, 1)

        # Take absolute difference of median Tsys spectum w/ and w/o smoothing
        diff_tsys = numpy.abs(median_tsys - smoothed_tsys)[idx_range]

        # If peak is smaller than a threshold, no strong line -> continue to the next spw
        peak = numpy.max(diff_tsys)
        threshold = lineStrengthThreshold * numpy.median(median_tsys[idx_range])
        if peak < threshold:
            LOG.info('No strong line is found. peak = %f, threshold = %f' % (peak, threshold))
            continue
        LOG.info('Strong line(s) are found. peak = %f, threshold = %f' % (peak, threshold))

        # Scaled MAD of diff_tsys
        scaled_mad = 1.4826*numpy.median(numpy.abs(diff_tsys - numpy.median(diff_tsys)))
        LOG.debug('*** Scaled MAD of diff_tsys = %f' % scaled_mad)

        # Check amplitude and width of diff_tsys (presumably atm lines).
        # If both exceeds thresholds -> strong line 
        LOG.info('Examining line intensities and widths ' + \
                 '(thresholds: intensity = %f, width = %d channels)' % \
                 (nSigma * scaled_mad, minAdjacantChannels))
        diff_tsys.mask = (diff_tsys.data < nSigma * scaled_mad)
        line_slices = numpy.ma.notmasked_contiguous(diff_tsys)
        if line_slices is None:
            # notmasked_contiguous returns None when all array is masked.
            # this happens when diff_tsys does not exceed nSigma threshold.
            LOG.info('No line exceeds intensity threshold.')
            continue
        for line in line_slices:
            LOG.debug('*** line channels: start = %d, width=%d' % \
                      (line.start+idx_offset, line.stop-line.start))
            # check line width
            if line.stop-line.start < minAdjacantChannels:
                continue
            else:
                # strong atmospheric line
                strong_atm_lines = True
                LOG.info('*** Found a spectral line with >= %d channels' % minAdjacantChannels)
                return strong_atm_lines
        LOG.info('*** All lines are < %d channels' % minAdjacantChannels)

    return strong_atm_lines


def get_median_tsys_spectrum_from_caltable(tsysname, spwid, fieldid, interpolate_flagged=True):
    """
    Returns masked median Tsys spectrum of an SPW and scan combination in Tsys caltable.

    Inputs:
        tsysname: the path to Tsys caltable
        spwid: SPW ID of Tsys to select
        fieldid: field ID of Tsys to select
        interpolate_flag: if True, operate piecewise linear interpolation of flagged channels

    Returns: masked array of median Tsys spectrum
    """
    if not os.path.exists(tsysname):
        raise ValueError('Could not find Tsys caltable, %s' % tsysname)
    with casa_tools.TableReader(tsysname) as tb:
        seltb = tb.query('SPECTRAL_WINDOW_ID == %s && FIELD_ID == %s' % (spwid, fieldid))
        if seltb.nrows() == 0:
            seltb.close()
            LOG.warning('No matching Tsys measurement for SPW=%s and field=%s in Tsys caltable %s' %
                        (spwid, fieldid, tsysname))
            return None
        try: # axis order: [POL, FREQ, ROW]
            tsys = seltb.getcol('FPARAM')
            flag = seltb.getcol('FLAG')
        finally:
            seltb.close()
    ma_median = numpy.median(numpy.ma.masked_array(tsys, flag), axis=[0, 2])
    if ma_median.count() == 0:  # No valid channel
        return None
    if interpolate_flagged:
        ma_median.data[ma_median.mask] = numpy.interp(numpy.where(ma_median.mask==True)[0],
                                                      numpy.where(ma_median.mask==False)[0],
                                                      ma_median[~ma_median.mask])
        # clear-up mask
        ma_median.mask = False
        return ma_median
