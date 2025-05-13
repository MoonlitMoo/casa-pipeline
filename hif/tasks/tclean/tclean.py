import os
import re
import inspect

from typing import Optional

import numpy as np
from scipy.ndimage import label

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.imageheader as imageheader
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.pipelineqa as pipelineqa
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry

from . import cleanbase
from .automaskthresholdsequence import AutoMaskThresholdSequence
from .autoscalthresholdsequence import AutoScalThresholdSequence
from .imagecentrethresholdsequence import ImageCentreThresholdSequence
from .manualmaskthresholdsequence import ManualMaskThresholdSequence
from .nomaskthresholdsequence import NoMaskThresholdSequence
from .resultobjects import TcleanResult
from .vlaautomaskthresholdsequence import VlaAutoMaskThresholdSequence
from .vlassmaskthresholdsequence import VlassMaskThresholdSequence

LOG = infrastructure.get_logger(__name__)


class TcleanInputs(cleanbase.CleanBaseInputs):
    # Search order of input vis
    # This is just an initial default to get any vis. The real selection is
    # usually made in hif_makeimlist and passed on as explicit parameter
    # via hif_makeimages.
    processing_data_type = [DataType.SELFCAL_LINE_SCIENCE, DataType.REGCAL_LINE_SCIENCE,  DataType.SELFCAL_CONT_SCIENCE, DataType.REGCAL_CONT_SCIENCE,
                           DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    # simple properties ------------------------------------------------------------------------------------------------

    calcsb = vdp.VisDependentProperty(default=False)
    cleancontranges = vdp.VisDependentProperty(default=False)
    datacolumn = vdp.VisDependentProperty(default=None)
    datatype = vdp.VisDependentProperty(default=None)
    datatype_info = vdp.VisDependentProperty(default=None)
    hm_cleaning = vdp.VisDependentProperty(default='rms')
    masklimit = vdp.VisDependentProperty(default=4.0)
    mosweight = vdp.VisDependentProperty(default=None)
    parallel = vdp.VisDependentProperty(default='automatic')
    reffreq = vdp.VisDependentProperty(default=None)
    restfreq = vdp.VisDependentProperty(default=None)
    tlimit = vdp.VisDependentProperty(default=None)
    usepointing = vdp.VisDependentProperty(default=None)
    weighting = vdp.VisDependentProperty(default=None)
    pblimit = vdp.VisDependentProperty(default=None)
    cfcache = vdp.VisDependentProperty(default=None)
    cfcache_nowb = vdp.VisDependentProperty(default=None)
    pbmask = vdp.VisDependentProperty(default=None)

    # override CleanBaseInputs default value of 'auto'
    hm_masking = vdp.VisDependentProperty(default='centralregion')

    # properties requiring some logic ----------------------------------------------------------------------------------

    @vdp.VisDependentProperty
    def image_heuristics(self):
        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()
        return image_heuristics_factory.getHeuristics(
            vislist=self.vis,
            spw=self.spw,
            observing_run=self.context.observing_run,
            imagename_prefix=self.context.project_structure.ousstatus_entity_id,
            proj_params=self.context.project_performance_parameters,
            contfile=self.context.contfile,
            linesfile=self.context.linesfile,
            imaging_params=self.context.imaging_parameters,
            # TODO: imaging_mode should not be fixed
            imaging_mode='ALMA'
        )

    imagename = vdp.VisDependentProperty(default='')
    @imagename.convert
    def imagename(self, value):
        return value.replace('STAGENUMBER', str(self.context.stage))

    specmode = vdp.VisDependentProperty(default=None)
    @specmode.convert
    def specmode(self, value):
        if value == 'repBW':
            self.orig_specmode = 'repBW'
            return 'cube'
        self.orig_specmode = value
        return value

    @vdp.VisDependentProperty
    def spwsel_lsrk(self):
        # mutable object, so should not use VisDependentProperty(default={})
        return {}

    @vdp.VisDependentProperty
    def spwsel_topo(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @vdp.VisDependentProperty(null_input=[None, -999, -999.0])
    def robust(self):
        # Fallback value if undefined in the imaging target and in the
        # imaging parameters. TODO: Use heuristic
        return 0.5

    @vdp.VisDependentProperty
    def uvtaper(self):
        # Fallback value if undefined in the imaging target and in the
        # imaging parameters. TODO: Use heuristic
        return []

    # class methods ----------------------------------------------------------------------------------------------------

    def __init__(self, context, output_dir=None, vis=None, imagename=None, intent=None, field=None, spw=None,
                 spwsel_lsrk=None, spwsel_topo=None, uvrange=None, specmode=None, gridder=None, deconvolver=None,
                 nterms=None, outframe=None, imsize=None, cell=None, phasecenter=None, psf_phasecenter=None, stokes=None, nchan=None,
                 start=None, width=None, nbin=None, datacolumn=None, datatype=None, datatype_info=None, pblimit=None,
                 cfcache=None, restoringbeam=None, hm_masking=None, hm_sidelobethreshold=None, hm_noisethreshold=None,
                 hm_lownoisethreshold=None, hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None,
                 hm_dogrowprune=None, hm_minpercentchange=None, hm_fastnoise=None, hm_nsigma=None,
                 hm_perchanweightdensity=None, hm_npixels=None, hm_cleaning=None,
                 iter=None, mask=None, niter=None, threshold=None, tlimit=None, drcorrect=None, masklimit=None,
                 calcsb=None, cleancontranges=None, parallel=None,
                 # Extra parameters not in the CLI task interface
                 weighting=None, robust=None, uvtaper=None, scales=None, cycleniter=None, cyclefactor=None, nmajor=None,
                 hm_minpsffraction=None, hm_maxpsffraction=None,
                 sensitivity=None, reffreq=None, restfreq=None, conjbeams=None, is_per_eb=None, antenna=None,
                 usepointing=None, mosweight=None, spwsel_all_cont=None, spwsel_low_bandwidth=None,
                 spwsel_low_spread=None, num_all_spws=None, num_good_spws=None, bl_ratio=None, cfcache_nowb=None,
                 # End of extra parameters
                 heuristics=None, pbmask=None):
        super(TcleanInputs, self).__init__(context, output_dir=output_dir, vis=vis, imagename=imagename, antenna=antenna,
                                           datacolumn=datacolumn, datatype=datatype, datatype_info=datatype_info,
                                           intent=intent, field=field, spw=spw, uvrange=uvrange, specmode=specmode,
                                           gridder=gridder, deconvolver=deconvolver, uvtaper=uvtaper, nterms=nterms,
                                           cycleniter=cycleniter, cyclefactor=cyclefactor, nmajor=nmajor,
                                           hm_minpsffraction=hm_minpsffraction, hm_maxpsffraction=hm_maxpsffraction,
                                           scales=scales, outframe=outframe, imsize=imsize, cell=cell, phasecenter=phasecenter,
                                           psf_phasecenter=psf_phasecenter, nchan=nchan, start=start, width=width, stokes=stokes,
                                           weighting=weighting, robust=robust, restoringbeam=restoringbeam, pblimit=pblimit,
                                           iter=iter, mask=mask, hm_masking=hm_masking, cfcache=cfcache,
                                           hm_sidelobethreshold=hm_sidelobethreshold,
                                           hm_noisethreshold=hm_noisethreshold,
                                           hm_lownoisethreshold=hm_lownoisethreshold,
                                           hm_negativethreshold=hm_negativethreshold, hm_minbeamfrac=hm_minbeamfrac,
                                           hm_growiterations=hm_growiterations, hm_dogrowprune=hm_dogrowprune,
                                           hm_minpercentchange=hm_minpercentchange, hm_fastnoise=hm_fastnoise,
                                           hm_nsigma=hm_nsigma, hm_perchanweightdensity=hm_perchanweightdensity,
                                           hm_npixels=hm_npixels, niter=niter, threshold=threshold,
                                           sensitivity=sensitivity, conjbeams=conjbeams, is_per_eb=is_per_eb,
                                           usepointing=usepointing, mosweight=mosweight,
                                           parallel=parallel, heuristics=heuristics)

        self.calcsb = calcsb
        self.cleancontranges = cleancontranges
        self.hm_cleaning = hm_cleaning
        self.image_heuristics = heuristics
        self.masklimit = masklimit
        self.nbin = nbin
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.spwsel_lsrk = spwsel_lsrk
        self.spwsel_topo = spwsel_topo
        self.spwsel_all_cont = spwsel_all_cont
        self.spwsel_low_bandwidth = spwsel_low_bandwidth
        self.spwsel_low_spread = spwsel_low_spread
        self.num_all_spws = num_all_spws
        self.num_good_spws = num_good_spws
        self.bl_ratio = bl_ratio
        self.tlimit = tlimit
        self.drcorrect = drcorrect

        # For MOM0/8_FC and cube RMS we need the LSRK frequency ranges in
        # various places
        self.cont_freq_ranges = ''

        self.is_per_eb = is_per_eb
        self.antenna = antenna
        self.usepointing = usepointing
        self.mosweight = mosweight
        self.datacolumn = datacolumn
        self.datatype = datatype
        self.datatype_info = datatype_info
        self.pblimit = pblimit
        self.cfcache = cfcache
        self.cfcache_nowb = cfcache_nowb
        self.pbmask = pbmask


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(TcleanInputs)


@task_registry.set_equivalent_casa_task('hif_tclean')
@task_registry.set_casa_commands_comment('A single target source is cleaned.')
class Tclean(cleanbase.CleanBase):
    Inputs = TcleanInputs

    is_multi_vis_task = True

    def rm_stage_files(self, imagename, stokes=''):
        filenames = utils.glob_ordered('%s*.%s.iter*' % (imagename, stokes))
        for filename in filenames:
            try:
                if os.path.isfile(filename):
                    os.remove(filename)
                elif os.path.isdir(filename):
                    rmtree_job = casa_tasks.rmtree(filename, ignore_errors=False)
                    self._executor.execute(rmtree_job)
                else:
                    raise Exception('Cannot remove %s' % filename)
            except Exception as e:
                LOG.warning('Exception while deleting %s: %s' % (filename, e))

    def rm_iter_files(self, rootname, iteration):
        # Delete any old files with this naming root
        filenames = utils.glob_ordered('%s.iter%s*' % (rootname, iteration))
        for filename in filenames:
            try:
                rmtree_job = casa_tasks.rmtree(filename)
                self._executor.execute(rmtree_job)
            except Exception as e:
                LOG.warning('Exception while deleting %s: %s' % (filename, e))

    def copy_products(self, old_pname, new_pname, ignore=None):
        imlist = utils.glob_ordered('%s.*' % (old_pname))
        imlist = [xx for xx in imlist if ignore is None or ignore not in xx]
        for image_name in imlist:
            newname = image_name.replace(old_pname, new_pname)
            LOG.info('Copying {} to {}'.format(image_name, newname))
            job = casa_tasks.copytree(image_name, newname)
            self._executor.execute(job)

    def move_products(self, old_pname, new_pname, ignore_list=None, remove_list=None, copy_list=None):
        """Move imaging products of one iteration to another.

        Certain image types can be excluded from the default "move" operation using the following keywords (in the precedence order):
            ignore_list:    do nothing (no 'remove', 'copy', or 'move'), if any string from the list is in the image name.
            remove_list:    remove without 'move' or 'copy', if any string from the list in the image name.
            copy_list:      copy instead move, if any string from the list is in the image name.
        """
        imlist = utils.glob_ordered('%s.*' % (old_pname))
        for image_name in imlist:
            newname = image_name.replace(old_pname, new_pname)
            if isinstance(ignore_list, (list, tuple)):
                if any([ignore_pattern in image_name for ignore_pattern in ignore_list]):
                    continue
            if isinstance(remove_list, (list, tuple)):
                if any([remove_pattern in image_name for remove_pattern in remove_list]):
                    LOG.info('Remove {}'.format(image_name))
                    job = casa_tasks.rmtree(image_name)
                    self._executor.execute(job)
                    continue
            if isinstance(copy_list, (list, tuple)):
                if any([copy_pattern in image_name for copy_pattern in copy_list]):
                    LOG.info('Copying {} to {}'.format(image_name, newname))
                    job = casa_tasks.copytree(image_name, newname)
                    self._executor.execute(job)
                    continue
            LOG.info('Moving {} to {}'.format(image_name, newname))
            job = casa_tasks.move(image_name, newname)
            self._executor.execute(job)

        return

    def prepare(self):
        inputs = self.inputs
        context = self.inputs.context
        self.known_synthesized_beams = self.inputs.context.synthesized_beams

        LOG.info('Start imaging for intent %s, field %s, spw %s', inputs.intent, inputs.field, inputs.spw)

        per_spw_cont_sensitivities_all_chan = context.per_spw_cont_sensitivities_all_chan
        qaTool = casa_tools.quanta

        # if 'start' or 'width' are defined in velocity units, track these
        #  for conversion to frequency and back before and after tclean call. Added
        #  to support SRDP ALMA optimized imaging.
        self.start_as_velocity = None
        self.width_as_velocity = None
        self.start_as_frequency = None
        self.width_as_frequency = None
        self.aggregate_lsrk_bw = None

        # delete any old files with this naming root. One of more
        # of these (don't know which) will interfere with this run.
        if inputs.stokes:
            LOG.info('deleting {}*.{}.iter*'.format(inputs.imagename, inputs.stokes))
            self.rm_stage_files(inputs.imagename, inputs.stokes)
        else:
            LOG.info('deleting {}*.iter*'.format(inputs.imagename))
            self.rm_stage_files(inputs.imagename)

        # Get the image parameter heuristics
        self.image_heuristics = inputs.image_heuristics

        # Set initial masking limits
        self.pblimit_image, self.pblimit_cleanmask = self.image_heuristics.pblimits(None, inputs.specmode)
        if not inputs.pblimit:
            inputs.pblimit = self.pblimit_image

        # Remove MSs that do not contain data for the given field(s)
        _, visindexlist = self.image_heuristics.get_scanidlist(inputs.vis, inputs.field, inputs.intent)
        filtered_vislist = [inputs.vis[i] for i in visindexlist]
        if filtered_vislist != inputs.vis:
            inputs.vis = filtered_vislist
            # Also need to reset any antenna list to trigger recalculation below.
            inputs.antenna = None

        # Generate the image name if one is not supplied.
        if inputs.imagename in (None, ''):
            inputs.imagename = self.image_heuristics.imagename(intent=inputs.intent,
                                                               field=inputs.field,
                                                               spwspec=inputs.spw,
                                                               specmode=inputs.specmode)

        # Determine the default gridder
        if inputs.gridder in (None, ''):
            inputs.gridder = self.image_heuristics.gridder(inputs.intent, inputs.field)

        # Determine deconvolver
        if inputs.deconvolver in (None, ''):
            inputs.deconvolver = self.image_heuristics.deconvolver(inputs.specmode, inputs.spw, inputs.intent, inputs.stokes)

        # Determine weighting
        if inputs.weighting in (None, ''):
            inputs.weighting = self.image_heuristics.weighting(inputs.specmode)

        # Determine perchanweightdensity
        if inputs.hm_perchanweightdensity in (None, ''):
            inputs.hm_perchanweightdensity = self.image_heuristics.perchanweightdensity(inputs.specmode)

        # Determine nterms
        if (inputs.nterms in ('', None)) and (inputs.deconvolver == 'mtmfs'):
            inputs.nterms = self.image_heuristics.nterms(inputs.spw)

        # Determine antennas to be used
        if inputs.antenna in (None, [], ''):
            antenna_ids = self.image_heuristics.antenna_ids(inputs.intent)
            # PIPE-964: The '&' at the end of the antenna input was added to not to consider the cross baselines by
            #  default. The cross baselines with antennas not listed (for TARGET images the antennas with the minority
            #  antenna sizes are not listed) could be added in some future configurations by removing this character.
            inputs.antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), '')))+'&' for v in inputs.vis]


        # If imsize not set then use heuristic code to calculate the
        # centers for each field  / spw
        imsize = inputs.imsize
        cell = inputs.cell
        if imsize in (None, [], '') or cell in (None, [], ''):

            # The heuristics cell size is always the same for x and y as
            # the value derives from a single value returned by imager.advise
            synthesized_beam, self.known_synthesized_beams = \
                self.image_heuristics.synthesized_beam(field_intent_list=[(inputs.field, inputs.intent)],
                                                       spwspec=inputs.spw,
                                                       robust=inputs.robust,
                                                       uvtaper=inputs.uvtaper,
                                                       parallel=inputs.parallel,
                                                       known_beams=self.known_synthesized_beams,
                                                       force_calc=inputs.calcsb,
                                                       shift=True)
            cell = self.image_heuristics.cell(beam=synthesized_beam)

            if inputs.cell in (None, [], ''):
                inputs.cell = cell
                LOG.info('Heuristic cell: %s' % cell)

            field_ids = self.image_heuristics.field(inputs.intent, inputs.field)
            largest_primary_beam = self.image_heuristics.largest_primary_beam_size(spwspec=inputs.spw,
                                                                                   intent=inputs.intent)
            # spw dependent imsize (FOV) in continuum spectral mode
            imsize_spwlist = inputs.spw if inputs.specmode == 'cont' else None
            imsize = self.image_heuristics.imsize(fields=field_ids,
                                                  cell=inputs.cell,
                                                  primary_beam=largest_primary_beam,
                                                  spwspec=imsize_spwlist,
                                                  intent=inputs.intent,specmode=inputs.specmode)

            if inputs.imsize in (None, [], ''):
                inputs.imsize = imsize
                LOG.info('Heuristic imsize: %s', imsize)

        if inputs.specmode == 'cube':
            # To avoid noisy edge channels, use only the frequency
            # intersection and skip one channel on either end.
            if self.image_heuristics.is_eph_obj(inputs.field):
                frame = 'REST'
            else:
                frame = 'LSRK'
            if0, if1, channel_width = self.image_heuristics.freq_intersection(inputs.vis, inputs.field, inputs.intent,
                                                                              inputs.spw, frame)

            if (if0 == -1) or (if1 == -1):
                LOG.error('No frequency intersect among selected MSs for Field %s SPW %s' % (inputs.field, inputs.spw))
                error_result = TcleanResult(vis=inputs.vis,
                                            sourcename=inputs.field,
                                            intent=inputs.intent,
                                            spw=inputs.spw,
                                            specmode=inputs.specmode,
                                            imaging_mode=self.image_heuristics.imaging_mode)
                error_result.error = '%s/%s/spw%s clean error: No frequency intersect among selected MSs' % (inputs.field, inputs.intent, inputs.spw)
                return error_result

            # Check for manually supplied values
            if0_auto = if0
            if1_auto = if1
            channel_width_auto = channel_width

            if inputs.start != '':

                # if specified in velocity units, convert to frequency
                #    then back again before the tclean call
                if 'm/' in inputs.start:
                    self.start_as_velocity = qaTool.quantity(inputs.start)
                    inputs.start = utils.velocity_to_frequency(inputs.start, inputs.restfreq)
                    self.start_as_frequency = inputs.start

                if0 = qaTool.convert(inputs.start, 'Hz')['value']
                if if0 < if0_auto:
                    LOG.error('Supplied start frequency (%s GHz) < f_low_native (%s GHz) for Field %s '
                              'SPW %s' % (if0/1e9, if0_auto/1e9, inputs.field, inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode,
                                                imaging_mode=self.image_heuristics.imaging_mode)
                    error_result.error = '%s/%s/spw%s clean error: f_start < f_low_native' % (inputs.field,
                                                                                              inputs.intent, inputs.spw)
                    return error_result
                LOG.info('Using supplied start frequency %s' % inputs.start)

            if (inputs.width != '') and (inputs.nbin not in (None, -1)):
                LOG.error('Field %s SPW %s: width and nbin are mutually exclusive' % (inputs.field, inputs.spw))
                error_result = TcleanResult(vis=inputs.vis,
                                            sourcename=inputs.field,
                                            intent=inputs.intent,
                                            spw=inputs.spw,
                                            specmode=inputs.specmode,
                                            imaging_mode=self.image_heuristics.imaging_mode)
                error_result.error = '%s/%s/spw%s clean error: width and nbin are mutually exclusive' % (inputs.field,
                                                                                                         inputs.intent,
                                                                                                         inputs.spw)
                return error_result

            if inputs.width != '':
                # if specified in velocity units, convert to frequency
                #    then back again before the tclean call
                if 'm/' in inputs.width:
                    self.width_as_velocity = qaTool.quantity(inputs.width)
                    start_plus_width = qaTool.add(self.start_as_velocity, inputs.width)
                    start_plus_width_freq = utils.velocity_to_frequency(start_plus_width, inputs.restfreq)
                    inputs.width = qaTool.sub(start_plus_width_freq, inputs.start)
                    self.width_as_frequency = inputs.width

                channel_width_manual = qaTool.convert(inputs.width, 'Hz')['value']
                # PIPE-1984: add tolerance acceptance when comparing user-specified chanwidths with
                # the intrinsic vis chanwidths.
                channel_width_tolerance = 0.05
                if abs(channel_width_manual) < channel_width_auto*(1-channel_width_tolerance):
                    LOG.error('User supplied channel width (%s GHz) smaller than native '
                              'value (%s GHz) for Field %s SPW %s' % (channel_width_manual/1e9, channel_width_auto/1e9, inputs.field, inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode,
                                                imaging_mode=self.image_heuristics.imaging_mode)
                    error_result.error = '%s/%s/spw%s clean error: user channel width too small' % (inputs.field,
                                                                                                    inputs.intent,
                                                                                                    inputs.spw)
                    return error_result
                else:
                    if abs(channel_width_manual) < channel_width_auto:
                        LOG.warning('User supplied channel width (%s GHz) smaller than native '
                                    'value (%s GHz) for Field %s SPW %s but within the tolerance of %f; '
                                    'use the native value instead.', channel_width_manual/1e9, channel_width_auto/1e9, inputs.field, inputs.spw, channel_width_tolerance)
                        channel_width = channel_width_auto
                    else:
                        LOG.info('Using supplied width %s' % inputs.width)
                        channel_width = channel_width_manual
                if abs(channel_width) > channel_width_auto:
                    inputs.nbin = int(utils.round_half_up(abs(channel_width) / channel_width_auto) + 0.5)
            elif inputs.nbin not in (None, -1):
                LOG.info('Applying binning factor %d' % inputs.nbin)
                channel_width *= inputs.nbin

            if self.image_heuristics.is_eph_obj(inputs.field):
                # Determine extra channels to skip for ephemeris objects to
                # account for fast moving objects.
                ref_ms = context.observing_run.get_ms(inputs.vis[0])
                real_spw = context.observing_run.virtual2real_spw_id(inputs.spw, ref_ms)
                real_spw_obj = ref_ms.get_spectral_window(real_spw)
                centre_frequency_TOPO = float(real_spw_obj.centre_frequency.to_units(measures.FrequencyUnits.HERTZ))
                channel_width_freq_TOPO = float(real_spw_obj.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ))
                freq0 = qaTool.quantity(centre_frequency_TOPO, 'Hz')
                freq1 = qaTool.quantity(centre_frequency_TOPO + channel_width_freq_TOPO, 'Hz')
                channel_width_velo_TOPO = float(qaTool.getvalue(qaTool.convert(utils.frequency_to_velocity(freq1, freq0), 'km/s')))
                # Skip 1 km/s
                extra_skip_channels = int(np.ceil(1.0 / abs(channel_width_velo_TOPO)))
            else:
                extra_skip_channels = 0

            if inputs.nchan not in (None, -1):
                if1 = if0 + channel_width * inputs.nchan
                if if1 > if1_auto:
                    LOG.error('Calculated stop frequency (%s GHz) > f_high_native (%s GHz) for Field %s '
                              'SPW % s' % (if1/1e9, if1_auto/1e9, inputs.field, inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode,
                                                imaging_mode=self.image_heuristics.imaging_mode)
                    error_result.error = '%s/%s/spw%s clean error: f_stop > f_high' % (inputs.field,
                                                                                       inputs.intent, inputs.spw)
                    return error_result
                LOG.info('Using supplied nchan %d' % inputs.nchan)
            else:
                # Skip edge channels and extra channels if no nchan is supplied.
                # Adjust to binning since the normal nchan heuristics already includes it.
                if inputs.nbin not in (None, -1):
                    inputs.nchan = int(utils.round_half_up((if1 - if0) / channel_width - 2)) - 2 * int(extra_skip_channels // inputs.nbin)
                else:
                    inputs.nchan = int(utils.round_half_up((if1 - if0) / channel_width - 2)) - 2 * extra_skip_channels

            if inputs.start == '':
                # tclean interprets the start frequency as the center of the
                # first channel. We have, however, an edge to edge range.
                # Thus shift by 0.5 channels if no start is supplied.
                # Additionally skipping the edge channel (cf. "- 2" above)
                # means a correction of 1.5 channels.
                if inputs.nbin not in (None, -1):
                    inputs.start = '%.10fGHz' % ((if0 + (1.5 + extra_skip_channels) * channel_width / inputs.nbin) / 1e9)
                else:
                    inputs.start = '%.10fGHz' % ((if0 + (1.5 + extra_skip_channels) * channel_width) / 1e9)

            # Always adjust width to apply possible binning
            inputs.width = '%.7fMHz' % (channel_width / 1e6)

        # Make sure there are LSRK selections if cont.dat/lines.dat exist.
        # For ALMA this is already done at the hif_makeimlist step. For VLASS
        # this does not (yet) happen in hif_editimlist.
        if inputs.spwsel_lsrk == {}:
            all_continuum = True
            low_bandwidth = True
            low_spread = True
            for spwid in inputs.spw.split(','):


                cont_ranges_spwsel, all_continuum_spwsel, low_bandwidth_spwsel, low_spread_spwsel = self.image_heuristics.cont_ranges_spwsel()
                spwsel_spwid = cont_ranges_spwsel.get(utils.dequote(inputs.field), {}).get(spwid, 'NONE')
                all_continuum = all_continuum and all_continuum_spwsel.get(utils.dequote(inputs.field), {}).get(spwid, False)
                low_bandwidth = low_bandwidth and low_bandwidth_spwsel.get(utils.dequote(inputs.field), {}).get(spwid, False)
                low_spread = low_spread and low_spread_spwsel.get(utils.dequote(inputs.field), {}).get(spwid, False)

                if inputs.intent == 'TARGET':
                    if (spwsel_spwid == 'NONE') and self.image_heuristics.warn_missing_cont_ranges():
                        LOG.warning('No continuum frequency range information detected for %s, spw %s.' % (inputs.field,
                                                                                                           spwid))

                if spwsel_spwid in ('ALL', 'ALLCONT', '', 'NONE'):
                    if self.image_heuristics.is_eph_obj(inputs.field):
                        spwsel_spwid_refer = 'SOURCE'
                    else:
                        spwsel_spwid_refer = 'LSRK'
                else:
                    _, spwsel_spwid_refer = spwsel_spwid.split()

                if spwsel_spwid_refer not in ('LSRK', 'SOURCE'):
                    LOG.warning('Frequency selection is specified in %s but must be in LSRK or SOURCE' %
                                spwsel_spwid_refer)

                inputs.spwsel_lsrk['spw%s' % spwid] = spwsel_spwid
            inputs.spwsel_all_cont = all_continuum
            inputs.spwsel_low_bandwidth = low_bandwidth
            inputs.spwsel_low_spread = low_spread

        # Get TOPO frequency ranges for all MSs
        (spw_topo_freq_param, _, _, spw_topo_chan_param_dict, _, _, self.aggregate_lsrk_bw) = self.image_heuristics.calc_topo_ranges(inputs)

        # Save continuum frequency ranges for later.
        if (inputs.specmode == 'cube') and (inputs.spwsel_lsrk.get('spw%s' % inputs.spw, None) not in (None,
                                                                                                       'NONE', '')):
            self.cont_freq_ranges = inputs.spwsel_lsrk['spw%s' % inputs.spw].split()[0]
        else:
            self.cont_freq_ranges = ''

        # Get sensitivity
        sens_reffreq = None
        if inputs.sensitivity is not None:
            # Override with manually set value
            sensitivity = qaTool.convert(inputs.sensitivity, 'Jy')['value']
            eff_ch_bw = 1.0
        else:
            # Get a noise estimate from the CASA sensitivity calculator
            (sensitivity, eff_ch_bw, _, sens_reffreq, per_spw_cont_sensitivities_all_chan) = \
                self.image_heuristics.calc_sensitivities(inputs.vis, inputs.field, inputs.intent, inputs.spw,
                                                         inputs.nbin, spw_topo_chan_param_dict, inputs.specmode,
                                                         inputs.gridder, inputs.cell, inputs.imsize, inputs.weighting,
                                                         inputs.robust, inputs.uvtaper,
                                                         known_sensitivities=per_spw_cont_sensitivities_all_chan,
                                                         force_calc=inputs.calcsb, calc_reffreq=True)

        if sensitivity is None:
            LOG.error('Could not calculate the sensitivity for Field %s Intent %s SPW %s' % (inputs.field,
                                                                                             inputs.intent, inputs.spw))
            error_result = TcleanResult(vis=inputs.vis,
                                        sourcename=inputs.field,
                                        intent=inputs.intent,
                                        spw=inputs.spw,
                                        specmode=inputs.specmode,
                                        imaging_mode=self.image_heuristics.imaging_mode)
            error_result.error = '%s/%s/spw%s clean error: no sensitivity' % (inputs.field, inputs.intent, inputs.spw)
            return error_result

        # PIPE-2130: for the VLA-PI workflow only, determine the optimal reffreq value from spw center frequencies
        # weighted by predicted per-spw sensitivity.
        # This is only triggered if all below conditions meet:
        #   * reffreq is not specified in the Tclean/input (from MakeImList or Editimlist)
        #   * deconvolver is mtmfs
        #   * nterms>=2 or CASA default nterms=None
        if (
            self.image_heuristics.imaging_mode in {"VLA", "VLA-SCAL"}
            and inputs.specmode == "cont"
            and inputs.deconvolver == "mtmfs"
            and (inputs.nterms is None or inputs.nterms >= 2)
            and inputs.reffreq is None
            and sens_reffreq is not None
        ):
            # Set reffreq in GHz
            inputs.reffreq = f"{sens_reffreq/1e9}GHz"

        # Choose TOPO frequency selections
        if inputs.specmode != 'cube':
            inputs.spwsel_topo = spw_topo_freq_param
        else:
            inputs.spwsel_topo = ['%s' % inputs.spw] * len(inputs.vis)

        if inputs.tlimit:
            tlimit = inputs.tlimit
        else:
            # Initial tlimit for iter0
            tlimit = self.image_heuristics.tlimit(0, inputs.field, inputs.intent, inputs.specmode, 0.0)

        # Determine threshold
        if inputs.hm_cleaning == 'manual':
            threshold = inputs.threshold
        elif inputs.hm_cleaning == 'sensitivity':
            raise Exception('sensitivity threshold not yet implemented')
        elif inputs.hm_cleaning == 'rms':
            if inputs.threshold not in (None, '', 0.0):
                threshold = inputs.threshold
            else:
                threshold = '%.3gJy' % (tlimit * sensitivity)
        else:
            raise Exception('hm_cleaning mode {} not recognized. '
                            'Threshold not set.'.format(inputs.hm_cleaning))

        multiterm = inputs.nterms if inputs.deconvolver == 'mtmfs' else None

        # Choose sequence manager
        # Central mask based on PB
        if inputs.hm_masking == 'centralregion':
            sequence_manager = ImageCentreThresholdSequence(multiterm=multiterm,
                                                            gridder=inputs.gridder, threshold=threshold,
                                                            sensitivity=sensitivity, niter=inputs.niter)
        # Auto-boxing
        elif inputs.hm_masking == 'auto' and self.image_heuristics.imaging_mode == 'VLA':
            sequence_manager = VlaAutoMaskThresholdSequence(multiterm=multiterm,
                                                            gridder=inputs.gridder, threshold=threshold,
                                                            sensitivity=sensitivity, niter=inputs.niter)
        # Auto-boxing-selfcal
        elif inputs.hm_masking == 'auto' and '-SCAL' in self.image_heuristics.imaging_mode:
            sequence_manager = AutoScalThresholdSequence(multiterm=multiterm,
                                                         gridder=inputs.gridder, threshold=threshold,
                                                         sensitivity=sensitivity, niter=inputs.niter)
        # Auto-boxing
        elif inputs.hm_masking == 'auto':
            sequence_manager = AutoMaskThresholdSequence(multiterm=multiterm,
                                                         gridder=inputs.gridder, threshold=threshold,
                                                         sensitivity=sensitivity, niter=inputs.niter)

        # VLASS-SE masking
        # Intended to cover VLASS-SE-CONT, VLASS-SE-CONT-AWP-P001, VLASS-SE-CONT-AWP-P032 as of 01.03.2021
        elif inputs.hm_masking == 'manual' and \
                (self.image_heuristics.imaging_mode.startswith('VLASS-SE-CONT') or self.image_heuristics.imaging_mode.startswith('VLASS-SE-CUBE')):
            sequence_manager = VlassMaskThresholdSequence(multiterm=multiterm, mask=inputs.mask,
                                                          gridder=inputs.gridder, threshold=threshold,
                                                          sensitivity=sensitivity, niter=inputs.niter, executor=self._executor)

        # Manually supplied mask
        elif inputs.hm_masking == 'manual':
            sequence_manager = ManualMaskThresholdSequence(multiterm=multiterm, mask=inputs.mask,
                                                           gridder=inputs.gridder, threshold=threshold,
                                                           sensitivity=sensitivity, niter=inputs.niter)
        # No mask
        elif inputs.hm_masking == 'none':
            sequence_manager = NoMaskThresholdSequence(multiterm=multiterm,
                                                       gridder=inputs.gridder, threshold=threshold,
                                                       sensitivity=sensitivity, niter=inputs.niter)
        else:
            raise Exception('hm_masking mode {} not recognized. '
                            'Sequence manager not set.'.format(inputs.hm_masking))

        # VLASS-SE-CONT mode currently needs a non-standard cleaning workflow
        # due to limitations in CASA: PSFs for an awproject mosaic image is
        # not optimal. Thus, PSFs need to be created with the tclean parameter
        # wbawp set to False. The awproject mosaic cleaning then continued
        # with this PSF. CASA is expected to handle this with version 6.2.

        if self.image_heuristics.imaging_mode in ['VLASS-SE-CONT', 'VLASS-SE-CONT-AWP-P001', 'VLASS-SE-CONT-AWP-P032',
                                                  'VLASS-SE-CONT-MOSAIC', 'VLASS-SE-CUBE']:
            result = self._do_iterative_vlass_se_imaging(sequence_manager=sequence_manager)
        elif '-SCAL' in self.image_heuristics.imaging_mode:
            result = self._do_scal_imaging(sequence_manager=sequence_manager)
        else:
            result = self._do_iterative_imaging(sequence_manager=sequence_manager)

        # The updated sensitivity dictionary needs to be transported via the
        # result object so that one can update the context later on in the
        # merge_with_context method (direct updates of the context do not
        # work since we are working on copies and since the HPC case will
        # have multiple instances which would overwrite each others results).
        # This result object is, however, only created in cleanbase.py while
        # we have the dictionary already here in tclean.py. Thus one has to
        # set this property only after getting the final result object.
        result.per_spw_cont_sensitivities_all_chan = per_spw_cont_sensitivities_all_chan

        # Record aggregate LSRK bandwidth and mosaic field sensitivities for weblog
        # TODO: Record total bandwidth as opposed to range
        #       Save channel selection in result for weblog.
        result.set_aggregate_bw(self.aggregate_lsrk_bw)
        result.set_eff_ch_bw(eff_ch_bw)

        result.synthesized_beams = self.known_synthesized_beams

        result.bl_ratio = inputs.bl_ratio
        return result

    def analyse(self, result):
        # Perform QA here if this is a sub-task
        context = self.inputs.context
        pipelineqa.qa_registry.do_qa(context, result)

        return result

    def _do_iterative_vlass_se_imaging(self, sequence_manager):
        """VLASS-SE-CONT imaging mode specific cleaning and imaging cycle

        This method implements the following workflow:
         1a) Compute PSF with cfcache_nowb (wbapw=False)
         1b) Create a copy of the PSF, delete products from 1a
         2) Initialise TClean with cfcache (wbapw=True)
         3) Replace 2) PSF with 1) PSF
         4) Continue imaging, clean according heuristics

         The method is base on _do_iterative_imaging(), but cube imaging support
         is removed.
        """
        inputs = self.inputs
        qaTool = casa_tools.quanta

        # Items for image header for manifest
        obspatt = None
        arrays = self.image_heuristics.arrays()
        if inputs.nbin not in (None, -1):
            modifier = f'binned{inputs.nbin}'
        else:
            modifier = ''
        session = ','.join(inputs.context.observing_run.get_ms(_vis).session for _vis in inputs.vis)

        # Local list from input masks
        if type(inputs.mask) is list:
            vlass_masks = inputs.mask
        else:
            vlass_masks = [inputs.mask]

        cfcache = inputs.cfcache

        # awproject gridder specific case: replace iter0 PSF with a PSF computed with wbawp=False
        if inputs.gridder == 'awproject':
            # Store CFCache references in local variables
            if inputs.cfcache_nowb:
                cfcache_nowb = inputs.cfcache_nowb
            else:
                raise Exception("wbapw=True CFCache is necessary in this imaging mode "
                                "but it was not defined.")

            # Compute PSF only
            LOG.info('Computing PSF with wbawp=False')
            iteration = 0
            # Replace main CFCache reference with the wbawp=False cache
            inputs.cfcache = cfcache_nowb
            result_psf = self._do_clean(iternum=iteration, cleanmask='', niter=0, threshold='0.0mJy',
                                        sensitivity=sequence_manager.sensitivity, result=None, calcres=False,
                                        wbawp=False)

            # Rename PSF before they are overwritten in the text TClean call
            self._replace_psf(result_psf.psf, result_psf.psf.replace('iter%s' % iteration, 'tmp'))

            # Delete any old files with this naming root
            rootname, _ = os.path.splitext(result_psf.psf)
            rootname, _ = os.path.splitext(rootname)
            self.rm_iter_files(rootname, iteration)

        # Compute the dirty image
        LOG.info('Initialise tclean iter 0')
        iteration = 0

        if hasattr(self.image_heuristics, 'restore_startmodel') and isinstance(self.image_heuristics.restore_startmodel, (str, list)):
            restore_startmodel = self.image_heuristics.restore_startmodel
            restore_imagename = self.image_heuristics.restore_imagename
            calcres_iter0 = False
            LOG.info(f'set calcres=False for the tclean initilization call because we are going to restore the model column.')
            LOG.info(f'Will use startmodel: {restore_startmodel}, for the final modelcolumn prediction.')
        else:
            restore_imagename = restore_startmodel = calcres_iter0 = None

        # Restore main CFCache reference / initialise tclean
        inputs.cfcache = cfcache
        result = self._do_clean(iternum=iteration, cleanmask='', niter=0, threshold='0.0mJy',
                                sensitivity=sequence_manager.sensitivity, result=None, calcres=calcres_iter0)

        if inputs.gridder == 'awproject':
            # only run for AWPproject, see VLASS memo 15 Sec2.2.
            LOG.info('Replacing PSF with wbawp=False PSF')
            # Remove *psf.tmp.* files with clear_origin=True argument
            self._replace_psf(result_psf.psf.replace('iter%s' % iteration, 'tmp'), result.psf, clear_origin=False)
            del result_psf  # Not needed in further steps

        if restore_imagename is not None:
            # PIPE-1354: this block will only be executed for the VLASS selfcal modelcolumn restoration (from hifv_restorepims)
            #
            # As of CASA 6.4.1, if selfcal model images are made with mpicasa and later fed to a predict-only serial tclean call
            # using "startmodel", the csys latpoles mismatch from CAS-13338 will cause the input model images to be regridded
            # in-flight: a side effect not desired in the VLASS-SE-CUBE case.
            # for now we make a copy of models under the tclean-predict imagename, and
            # you should see a resetting warning instead:
            #           WARN    SIImageStore::Open existing Images (file src/code/synthesis/ImagerObjects/SIImageStore.cc, line 568)
            #           Mismatch in Csys latpoles between existing image on disk ([350.792, 50.4044, 180, 50.4044])
            #           The DirectionCoordinates have differing latpoles -- Resetting to match image on disk
            # note: we preassume that restore_imagename and new_pname are different.
            rootname, _ = os.path.splitext(result.psf)
            rootname, _ = os.path.splitext(rootname)
            LOG.info('Copying model images for the modelcolumn prediction tclean call.')
            new_pname = f'{rootname}.iter{iteration}'
            self.copy_products(restore_imagename, os.path.basename(new_pname))
            restore_startmodel = None

        if calcres_iter0 is None or calcres_iter0:
            # tclean results assessment only happen when calcres=True or implicit default (None) for iter0

            # Determine masking limits depending on PB
            extension = '.tt0' if result.multiterm else ''
            self.pblimit_image, self.pblimit_cleanmask = self.image_heuristics.pblimits(result.flux + extension)

            # Keep pblimits for mom8_fc QA statistics and score (PIPE-704)
            result.set_pblimit_image(self.pblimit_image)
            result.set_pblimit_cleanmask(self.pblimit_cleanmask)

            # Give the result to the sequence_manager for analysis
            (residual_cleanmask_rms,  # printed
             residual_non_cleanmask_rms,  # printed
             residual_min,  # printed
             residual_max,  # USED
             nonpbcor_image_non_cleanmask_rms_min,  # added to result, later used in Weblog under name 'image_rms_min'
             nonpbcor_image_non_cleanmask_rms_max,  # added to result, later used in Weblog under name 'image_rms_max'
             nonpbcor_image_non_cleanmask_rms,  # printed added to result, later used in Weblog under name 'image_rms'
             pbcor_image_min,  # added to result, later used in Weblog under name 'image_min'
             pbcor_image_max,  # added to result, later used in Weblog under name 'image_max'
             # USED
             residual_robust_rms,
             nonpbcor_image_robust_rms_and_spectra,
             pbcor_image_min_iquv,
             pbcor_image_max_iquv,
             nonpbcor_image_non_cleanmask_rms_iquv) = \
                sequence_manager.iteration_result(model=result.model,
                                                  restored=result.image, residual=result.residual,
                                                  flux=result.flux, cleanmask=None,
                                                  pblimit_image=self.pblimit_image,
                                                  pblimit_cleanmask=self.pblimit_cleanmask,
                                                  cont_freq_ranges=self.cont_freq_ranges)

            LOG.info('Dirty image stats')
            LOG.info('    Residual rms: %s', residual_non_cleanmask_rms)
            LOG.info('    Residual max: %s', residual_max)
            LOG.info('    Residual min: %s', residual_min)
            LOG.info('    Residual scaled MAD: %s', residual_robust_rms)

            LOG.info('Continue cleaning')

        # Continue iterating (calcpsf and calcres are set automatically false)
        iteration = 1

        # Do not reuse mask from earlier iteration stage. <imagename>.iter<n>.mask (copy of mask from iteration n-1)
        # would lead to collision with new_cleanmask of nth iteration. VLASS-SE-CONT uses two different masks.
        do_not_copy_mask = True
        for mask in vlass_masks:
            # Create the name of the next clean mask from the root of the
            # previous residual image.
            rootname, ext = os.path.splitext(result.residual)
            rootname, ext = os.path.splitext(rootname)

            # Delete any old files with this naming root
            self.rm_iter_files(rootname, iteration)

            if self.image_heuristics.imaging_mode == 'VLASS-SE-CUBE':
                # PIPE-1401: copy input user masks (vlass-se-cont tier1/tier2) for imaging of each spw group.
                # Because various metadata (e.g., spw list) are written into headers of mask images and later used by the
                # weblog display/renderer class, we have to differentiate in-use mask files of individual spw groups, even
                # they are identical. Here, we make mask copy per clean_target (i.e. a spw group of VLASS-SE-CUBE.)
                # under its distinct name.
                if mask in ['', None, 'pb']:
                    new_cleanmask = mask
                else:
                    # note: this "new_cleanmask" name must be different from imagename+'.mask'.
                    #   1) tclean() doesn't support specifying usemask='user' and mask=imagename+'.mask' (pre-exists) at the same time.
                    #   2) the vlass-se-cont mask is Stokes-I only and needs to be "regridded" (i.e. broadcasted) in the Pol. axis.
                    #      Therefore a "restart" by copying the user mask under imagename+'.mask' is also not supported in tclean() as of ver 6.4.1.
                    new_cleanmask = f'{rootname}.iter{iteration}.cleanmask'
            else:
                # Determine stage mask name and replace stage substring place holder with actual stage number.
                # Special cases when mask is an empty string, None, or when it is set to 'pb'.
                new_cleanmask = mask if mask in ['', None, 'pb'] else 's{:d}_0.{}'.format(
                    self.inputs.context.task_counter, re.sub('s[0123456789]+_[0123456789]+.', '', mask, 1))

            threshold = self.image_heuristics.threshold(iteration, sequence_manager.threshold, inputs.hm_masking)
            nsigma = self.image_heuristics.nsigma(iteration, inputs.hm_nsigma, inputs.hm_masking)

            seq_result = sequence_manager.iteration(new_cleanmask, self.pblimit_image,
                                                    self.pblimit_cleanmask, iteration=iteration)

            # Use previous iterations's products as starting point
            old_pname = '%s.iter%s' % (rootname, iteration - 1)
            new_pname = '%s.iter%s' % (rootname, iteration)
            if self.image_heuristics.imaging_mode == 'VLASS-SE-CUBE':
                # PIPE-1401: We move (instead of copy) most imaging products from one iteration to the next,
                # to reduce the disk I/O operations and storage use.
                self.move_products(os.path.basename(old_pname), os.path.basename(new_pname),
                                   ignore_list=['.cleanmask'] if do_not_copy_mask else [],
                                   copy_list=['.residual', '.image'],
                                   remove_list=['.mask'] if do_not_copy_mask else [])
            else:
                self.copy_products(os.path.basename(old_pname), os.path.basename(new_pname),
                                   ignore='mask' if do_not_copy_mask else None)

            LOG.info('Iteration %s: Clean control parameters' % iteration)
            LOG.info('    Mask %s', new_cleanmask)
            LOG.info('    Threshold %s', threshold)
            LOG.info('    Niter %s', seq_result.niter)

            # The user_cycleniter_final_image_nomask ImageParamsHeuristicsVlassSeCont class variable should be
            # prioritised over cycleniter parameter set by the user when cleaning without a user mask (see PIPE-978
            # and PIPE-977). The heuristic method cycleniter() takes care of this, but in order to be triggered, the
            # cycleniter parameter should be reset to None for the specific cases.
            if mask == 'pb':
                cycleniter = inputs.cycleniter
                inputs.cycleniter = None

            result = self._do_clean(iternum=iteration, cleanmask=new_cleanmask, niter=seq_result.niter, nsigma=nsigma,
                                    threshold=threshold, sensitivity=sequence_manager.sensitivity, result=result)
            # Restore cycleniter if needed
            if mask == 'pb':
                inputs.cycleniter = cycleniter

            # Give the result to the clean 'sequencer'
            (residual_cleanmask_rms,
             residual_non_cleanmask_rms,
             residual_min,
             residual_max,
             nonpbcor_image_non_cleanmask_rms_min,
             nonpbcor_image_non_cleanmask_rms_max,
             nonpbcor_image_non_cleanmask_rms,
             pbcor_image_min,
             pbcor_image_max,
             residual_robust_rms,
             nonpbcor_image_robust_rms_and_spectra,
             pbcor_image_min_iquv,
             pbcor_image_max_iquv,
             nonpbcor_image_non_cleanmask_rms_iquv) = \
                sequence_manager.iteration_result(model=result.model,
                                                  restored=result.image, residual=result.residual,
                                                  flux=result.flux, cleanmask=new_cleanmask,
                                                  pblimit_image=self.pblimit_image,
                                                  pblimit_cleanmask=self.pblimit_cleanmask,
                                                  cont_freq_ranges=self.cont_freq_ranges)

            # Center frequency and effective bandwidth in Hz for the image header (and thus the manifest).
            ctrfrq = 0.5 * (float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_ch1'], 'Hz')))
                         +  float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_chN'], 'Hz'))))
            if inputs.specmode in ('mfs', 'cont'):
                effbw = float(qaTool.getvalue(qaTool.convert(self.aggregate_lsrk_bw, 'Hz')))
            else:
                msobj = self.inputs.context.observing_run.get_ms(name=inputs.vis[0])
                nbin = inputs.nbin if inputs.nbin is not None and inputs.nbin > 0 else 1
                _, _, _, effbw = self.image_heuristics.get_bw_corr_factor(msobj, inputs.spw, nbin)

            self._update_miscinfo(imagename=result.image.replace('.image', '.image' + extension),
                                  nfield=max([len(field_ids.split(',')) for field_ids in self.image_heuristics.field(inputs.intent, inputs.field)]),
                                  datamin=pbcor_image_min, datamax=pbcor_image_max, datarms=nonpbcor_image_non_cleanmask_rms, stokes=inputs.stokes,
                                  effbw=effbw, ctrfrq=ctrfrq)

            # Keep image cleanmask area min and max and non-cleanmask area RMS for weblog and QA
            result.set_image_min(pbcor_image_min)
            result.set_image_min_iquv(pbcor_image_min_iquv)
            result.set_image_max(pbcor_image_max)
            result.set_image_max_iquv(pbcor_image_max_iquv)
            result.set_image_rms(nonpbcor_image_non_cleanmask_rms)
            result.set_image_rms_iquv(nonpbcor_image_non_cleanmask_rms_iquv)
            result.set_image_rms_min(nonpbcor_image_non_cleanmask_rms_min)
            result.set_image_rms_max(nonpbcor_image_non_cleanmask_rms_max)
            result.set_image_robust_rms_and_spectra(nonpbcor_image_robust_rms_and_spectra)

            # Determine fractional flux outside of mask for final image (only VLASS-SE-CONT imaging stage 1)
            outmaskratio = self.image_heuristics.get_outmaskratio(iteration, result.image + extension,
                                                                  re.sub(r'\.image$', '.pb', result.image) + extension,
                                                                  new_cleanmask)
            result.set_outmaskratio(iteration, outmaskratio)

            LOG.info('Clean image iter %s stats' % iteration)
            LOG.info('    Clean image annulus area rms: %s', nonpbcor_image_non_cleanmask_rms)
            LOG.info('    Clean image min: %s', pbcor_image_min)
            LOG.info('    Clean image max: %s', pbcor_image_max)
            LOG.info('    Residual annulus area rms: %s', residual_non_cleanmask_rms)
            LOG.info('    Residual cleanmask area rms: %s', residual_cleanmask_rms)
            LOG.info('    Residual max: %s', residual_max)
            LOG.info('    Residual min: %s', residual_min)

            # Up the iteration counter
            iteration += 1

        # Save predicted model visibility columns to MS at the end of the first imaging stage (see heuristic method).
        savemodel = self.image_heuristics.savemodel(iteration-1)
        if savemodel:
            LOG.info("Saving predicted model visibilities to MeasurementSet after last iteration (iter %s)" %
                     (iteration-1))
            _ = self._do_clean(iternum=iteration-1, cleanmask='', niter=0, threshold='0.0mJy',
                               sensitivity=sequence_manager.sensitivity, savemodel=savemodel, startmodel=restore_startmodel,
                               result=None, calcpsf=False, calcres=False, parallel=False)

        return result

    def _replace_psf(self, origin: str, target: str, clear_origin: bool = False):
        """Replace multi-term CASA image.

        The <origin>.tt0, <origin>.tt1 and <origin>.tt2 images are copied or moved
        to <target>.tt0, <target>.tt1 and <target>.tt2 images.

        If clear_origin argument is True then after copying the <origin>.tt* files
        they will be deleted.
        """
        for tt in ['tt0', 'tt1', 'tt2']:
            target_psf = f'{target}.{tt}'
            origin_psf = f'{origin}.{tt}'
            if os.path.isdir(target_psf):
                self._executor.execute(casa_tasks.rmtree(target_psf, ignore_errors=False))
            if clear_origin:
                LOG.info(f'Moving {origin_psf} to {target_psf}')
                self._executor.execute(casa_tasks.move(origin_psf, target_psf))
            else:
                LOG.info(f'Copying {origin_psf} to {target_psf}')
                self._executor.execute(casa_tasks.copytree(origin_psf, target_psf))

    def _do_scal_imaging(self, sequence_manager):
        """Do self-calibration imaging sequence.

        This method produces the optimal selfcal solution via the iterative imaging-selfcal loop.
        It also generate before-scal/after-scal. The input MS is assumed to be calibrated via
        standard calibration procedures but they have been splitted from the original *_targets.ms
        and rebinned in frequency.
        """

        raise NotImplementedError("The self-calibration imaging/gaincal loop is not implemented yet!")

    def _do_iterative_imaging(self, sequence_manager):

        inputs = self.inputs
        qaTool = casa_tools.quanta

        # Items for image header for manifest
        obspatt = 'mos' if self.image_heuristics.is_mosaic(inputs.field, inputs.intent) else 'sf'
        arrays = self.image_heuristics.arrays()
        if inputs.nbin not in (None, -1):
            modifier = f'binned{inputs.nbin}'
        else:
            modifier = ''
        session = ','.join(inputs.context.observing_run.get_ms(_vis).session for _vis in inputs.vis)

        # Compute the dirty image
        LOG.info('Compute the dirty image')
        iteration = 0
        result = self._do_clean(iternum=iteration, cleanmask='', niter=0, threshold='0.0mJy',
                                sensitivity=sequence_manager.sensitivity, result=None)

        # PIPE-1790: skip any further processing in case of errors (i.e. tclean failed to produce an image)
        if result.error:
            return result

        # Check for bad PSF fit
        bad_psf_channels = None
        if inputs.specmode == 'cube':
            bad_psf_fit = self.image_heuristics.check_psf(result.psf, inputs.field, inputs.spw)
            if bad_psf_fit:
                newcommonbeam, bad_psf_channels = self.image_heuristics.find_good_commonbeam(result.psf)
                if newcommonbeam is None:
                    result.error = '%s/%s/spw%s clean error: no valid beams' % (inputs.field, inputs.intent, inputs.spw)
                    return result
                elif bad_psf_channels.shape != (0,):
                    LOG.warning('Found bad PSF fits for SPW %s in channels %s' %
                                (inputs.spw, ','.join(map(str, bad_psf_channels))))
                    # For Cycle 7 the new common beam shall not yet be used (PIPE-375).
                    # In the future, we might use the PIPE-375 method to calculate unskewed
                    # common beam in case of PSF fit problems.  For implementation details see
                    # https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/browse/pipeline/hif/tasks/tclean/tclean.py?at=9b8902e66bf44e644e612b1980e5aee5361e8ddd#607

        # Determine masking limits depending on PB
        extension = '.tt0' if result.multiterm else ''
        self.pblimit_image, self.pblimit_cleanmask = self.image_heuristics.pblimits(result.flux+extension, specmode=inputs.specmode)

        # Keep pblimits for mom8_fc QA statistics and score (PIPE-704)
        result.set_pblimit_image(self.pblimit_image)
        result.set_pblimit_cleanmask(self.pblimit_cleanmask)

        # Give the result to the sequence_manager for analysis
        (residual_cleanmask_rms,  # printed
         residual_non_cleanmask_rms,  # printed
         residual_min,  # printed
         residual_max,  # USED
         nonpbcor_image_non_cleanmask_rms_min,  # added to result, later used in Weblog under name 'image_rms_min'
         nonpbcor_image_non_cleanmask_rms_max,  # added to result, later used in Weblog under name 'image_rms_max'
         nonpbcor_image_non_cleanmask_rms,   # printed added to result, later used in Weblog under name 'image_rms'
         pbcor_image_min,  # added to result, later used in Weblog under name 'image_min'
         pbcor_image_max,  # added to result, later used in Weblog under name 'image_max'
         # USED
         residual_robust_rms,
         nonpbcor_image_robust_rms_and_spectra,
         pbcor_image_min_iquv,
         pbcor_image_max_iquv,
         nonpbcor_image_non_cleanmask_rms_iquv) = \
            sequence_manager.iteration_result(model=result.model,
                                              restored=result.image, residual=result.residual,
                                              flux=result.flux, cleanmask=None,
                                              pblimit_image=self.pblimit_image,
                                              pblimit_cleanmask=self.pblimit_cleanmask,
                                              cont_freq_ranges=self.cont_freq_ranges)

        LOG.info('Dirty image stats')
        LOG.info('    Residual rms: %s', residual_non_cleanmask_rms)
        LOG.info('    Residual max: %s', residual_max)
        LOG.info('    Residual min: %s', residual_min)
        LOG.info('    Residual scaled MAD: %s', residual_robust_rms)

        # All continuum
        if inputs.specmode == 'cube' and inputs.spwsel_all_cont:
            # Center frequency and effective bandwidth in Hz for the image header (and thus the manifest).
            ctrfrq = 0.5 * (float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_ch1'], 'Hz')))
                         +  float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_chN'], 'Hz'))))
            if inputs.specmode in ('mfs', 'cont'):
                effbw = float(qaTool.getvalue(qaTool.convert(self.aggregate_lsrk_bw, 'Hz')))
            else:
                msobj = self.inputs.context.observing_run.get_ms(name=inputs.vis[0])
                nbin = inputs.nbin if inputs.nbin is not None and inputs.nbin > 0 else 1
                _, _, _, effbw = self.image_heuristics.get_bw_corr_factor(msobj, inputs.spw, nbin)

            self._update_miscinfo(imagename=result.image.replace('.image', '.image'+extension),
                                  nfield=max([len(field_ids.split(',')) for field_ids in self.image_heuristics.field(inputs.intent, inputs.field)]),
                                  datamin=pbcor_image_min, datamax=pbcor_image_max, datarms=nonpbcor_image_non_cleanmask_rms, stokes=inputs.stokes,
                                  effbw=effbw, level='member', ctrfrq=ctrfrq, obspatt=obspatt, arrays=arrays, modifier=modifier, session=session)

            # PIPE-2211: Update some keywords for manifest usage on all other imaging products
            for im_name in result.im_names.values():
                if os.path.exists(im_name):
                    self._update_miscinfo(imagename=im_name, stokes=inputs.stokes, level='member', obspatt=obspatt, arrays=arrays, modifier=modifier, session=session)

            result.set_image_min(pbcor_image_min)
            result.set_image_min_iquv(pbcor_image_min_iquv)
            result.set_image_max(pbcor_image_max)
            result.set_image_max_iquv(pbcor_image_max_iquv)
            result.set_image_rms(nonpbcor_image_non_cleanmask_rms)
            result.set_image_rms_iquv(nonpbcor_image_non_cleanmask_rms_iquv)
            result.set_image_rms_min(nonpbcor_image_non_cleanmask_rms_min)
            result.set_image_rms_max(nonpbcor_image_non_cleanmask_rms_max)
            result.set_image_robust_rms_and_spectra(nonpbcor_image_robust_rms_and_spectra)
            result.cube_all_cont = True
            keep_iterating = False
        else:
            keep_iterating = True

        # Adjust threshold based on the dirty image statistics
        dirty_dynamic_range = None if sequence_manager.sensitivity == 0.0 else residual_max / sequence_manager.sensitivity
        tlimit = self.image_heuristics.tlimit(1, inputs.field, inputs.intent, inputs.specmode, dirty_dynamic_range)
        new_threshold, DR_correction_factor, maxEDR_used = \
            self.image_heuristics.dr_correction(sequence_manager.threshold, dirty_dynamic_range, residual_max,
                                                inputs.intent, tlimit, inputs.drcorrect)
        sequence_manager.threshold = new_threshold
        sequence_manager.dr_corrected_sensitivity = sequence_manager.sensitivity * DR_correction_factor

        # Adjust niter based on the dirty image statistics
        new_niter = self.image_heuristics.niter_correction(sequence_manager.niter, inputs.cell, inputs.imsize,
                                                           residual_max, new_threshold, residual_robust_rms, intent=inputs.intent)
        sequence_manager.niter = new_niter

        # Save corrected sensitivity in iter0 result object for 'cube' and
        # 'all continuum' since there is no further iteration.
        if inputs.specmode == 'cube' and inputs.spwsel_all_cont:
            result.set_dirty_dynamic_range(dirty_dynamic_range)
            result.set_DR_correction_factor(DR_correction_factor)
            result.set_maxEDR_used(maxEDR_used)
            result.set_dr_corrected_sensitivity(sequence_manager.dr_corrected_sensitivity)

        iteration = 1
        do_not_copy_mask = False
        while keep_iterating:
            # Create the name of the next clean mask from the root of the
            # previous residual image.
            rootname, _ = os.path.splitext(result.residual)
            rootname, _ = os.path.splitext(rootname)

            # Delete any old files with this naming root
            self.rm_iter_files(rootname, iteration)

            if inputs.hm_masking == 'auto':
                new_cleanmask = '%s.iter%s.mask' % (rootname, iteration)
            elif inputs.hm_masking == 'manual':
                new_cleanmask = inputs.mask
            elif inputs.hm_masking == 'none':
                new_cleanmask = ''
            else:
                new_cleanmask = '%s.iter%s.cleanmask' % (rootname, iteration)

            # perform an iteration.
            if (inputs.specmode == 'cube') and (not inputs.cleancontranges):
                seq_result = sequence_manager.iteration(new_cleanmask, self.pblimit_image,
                                                        self.pblimit_cleanmask, inputs.spw, inputs.spwsel_lsrk,
                                                        iteration=iteration)
            else:
                seq_result = sequence_manager.iteration(new_cleanmask, self.pblimit_image,
                                                        self.pblimit_cleanmask, iteration=iteration)

            # Use previous iterations's products as starting point
            old_pname = '%s.iter%s' % (rootname, iteration-1)
            new_pname = '%s.iter%s' % (rootname, iteration)
            self.copy_products(os.path.basename(old_pname), os.path.basename(new_pname),
                               ignore='mask' if do_not_copy_mask else None)

            threshold = self.image_heuristics.threshold(iteration, sequence_manager.threshold, inputs.hm_masking)
            
            savemodel = self.image_heuristics.savemodel(iteration)
            niter = self.image_heuristics.niter_by_iteration(iteration, inputs.hm_masking, seq_result.niter)
            if inputs.cyclefactor not in (None, -999):
                cyclefactor = inputs.cyclefactor
            else:
                cyclefactor = self.image_heuristics.cyclefactor(iteration, inputs.field, inputs.intent, inputs.specmode, dirty_dynamic_range)

            LOG.info('Iteration %s: Clean control parameters' % iteration)
            LOG.info('    Mask %s', new_cleanmask)
            LOG.info('    Threshold %s', threshold)
            LOG.info('    Niter %s', niter)

            result = self._do_clean(iternum=iteration, cleanmask=new_cleanmask, niter=niter, nsigma=inputs.hm_nsigma,
                                    threshold=threshold, sensitivity=sequence_manager.sensitivity, savemodel=savemodel,
                                    result=result, cyclefactor=cyclefactor)
            if result.image is None:
                if not result.error:
                    result.error = '%s/%s/spw%s clean error: failed to produce an image' % (inputs.field, inputs.intent, inputs.spw)
                return result

            # Give the result to the clean 'sequencer'
            (residual_cleanmask_rms,
             residual_non_cleanmask_rms,
             residual_min,
             residual_max,
             nonpbcor_image_non_cleanmask_rms_min,
             nonpbcor_image_non_cleanmask_rms_max,
             nonpbcor_image_non_cleanmask_rms,
             pbcor_image_min,
             pbcor_image_max,
             residual_robust_rms,
             nonpbcor_image_robust_rms_and_spectra,
             pbcor_image_min_iquv,
             pbcor_image_max_iquv,
             nonpbcor_image_non_cleanmask_rms_iquv) = \
                sequence_manager.iteration_result(model=result.model,
                                                  restored=result.image, residual=result.residual,
                                                  flux=result.flux, cleanmask=new_cleanmask,
                                                  pblimit_image=self.pblimit_image,
                                                  pblimit_cleanmask=self.pblimit_cleanmask,
                                                  cont_freq_ranges=self.cont_freq_ranges)

            # Center frequency and effective bandwidth in Hz for the image header (and thus the manifest).
            ctrfrq = 0.5 * (float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_ch1'], 'Hz')))
                         +  float(qaTool.getvalue(qaTool.convert(nonpbcor_image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_chN'], 'Hz'))))
            if inputs.specmode in ('mfs', 'cont'):
                effbw = float(qaTool.getvalue(qaTool.convert(self.aggregate_lsrk_bw, 'Hz')))
            else:
                msobj = self.inputs.context.observing_run.get_ms(name=inputs.vis[0])
                nbin = inputs.nbin if inputs.nbin is not None and inputs.nbin > 0 else 1
                _, _, _, effbw = self.image_heuristics.get_bw_corr_factor(msobj, inputs.spw, nbin)

            self._update_miscinfo(imagename=result.image.replace('.image', '.image'+extension),
                                  nfield=max([len(field_ids.split(',')) for field_ids in self.image_heuristics.field(inputs.intent, inputs.field)]),
                                  datamin=pbcor_image_min, datamax=pbcor_image_max, datarms=nonpbcor_image_non_cleanmask_rms, stokes=inputs.stokes,
                                  effbw=effbw, level='member', ctrfrq=ctrfrq, obspatt=obspatt, arrays=arrays, modifier=modifier, session=session)

            # PIPE-2211: Update some keywords for manifest usage on all other imaging products
            for im_name in result.im_names.values():
                if os.path.exists(im_name):
                    self._update_miscinfo(imagename=im_name, stokes=inputs.stokes, level='member', obspatt=obspatt, arrays=arrays, modifier=modifier, session=session)

            keep_iterating, hm_masking = self.image_heuristics.keep_iterating(iteration, inputs.hm_masking,
                                                                              result.tclean_stopcode,
                                                                              dirty_dynamic_range, residual_max,
                                                                              residual_robust_rms,
                                                                              inputs.field, inputs.intent, inputs.spw,
                                                                              inputs.specmode)
            do_not_copy_mask = hm_masking != inputs.hm_masking
            inputs.hm_masking = hm_masking

            # Keep image cleanmask area min and max and non-cleanmask area RMS for weblog and QA
            result.set_image_min(pbcor_image_min)
            result.set_image_min_iquv(pbcor_image_min_iquv)
            result.set_image_max(pbcor_image_max)
            result.set_image_max_iquv(pbcor_image_max_iquv)
            result.set_image_rms(nonpbcor_image_non_cleanmask_rms)
            result.set_image_rms_iquv(nonpbcor_image_non_cleanmask_rms_iquv)
            result.set_image_rms_min(nonpbcor_image_non_cleanmask_rms_min)
            result.set_image_rms_max(nonpbcor_image_non_cleanmask_rms_max)
            result.set_image_robust_rms_and_spectra(nonpbcor_image_robust_rms_and_spectra)

            # Keep dirty DR, correction factor and information about maxEDR heuristic for weblog
            result.set_dirty_dynamic_range(dirty_dynamic_range)
            result.set_DR_correction_factor(DR_correction_factor)
            result.set_maxEDR_used(maxEDR_used)
            result.set_dr_corrected_sensitivity(sequence_manager.dr_corrected_sensitivity)

            LOG.info('Clean image iter %s stats' % iteration)
            LOG.info('    Clean image annulus area rms: %s', nonpbcor_image_non_cleanmask_rms)
            LOG.info('    Clean image min: %s', pbcor_image_min)
            LOG.info('    Clean image max: %s', pbcor_image_max)
            LOG.info('    Residual annulus area rms: %s', residual_non_cleanmask_rms)
            LOG.info('    Residual cleanmask area rms: %s', residual_cleanmask_rms)
            LOG.info('    Residual max: %s', residual_max)
            LOG.info('    Residual min: %s', residual_min)

            # Up the iteration counter
            iteration += 1

        # If specmode is "cube", create from the non-pbcorrected cube
        # after continuum subtraction an image of the moment 0 / 8 integrated
        # intensity for the line-free channels.
        if inputs.specmode == 'cube':
            # Moment maps of line-free channels
            self._calc_mom0_8_10_fc(result)
            # Moment maps of all channels
            self._calc_mom0_8(result)

        # Record any failed PSF fit channels
        result.bad_psf_channels = bad_psf_channels

        return result

    def _do_clean(self, iternum, cleanmask, niter, threshold, sensitivity, result, nsigma=None, savemodel=None, startmodel=None,
                  calcres=None, calcpsf=None, wbawp=None, parallel=None, clean_imagename=None, cyclefactor=None):
        """Do basic cleaning."""
        inputs = self.inputs

        if parallel is None:
            parallel = mpihelpers.parse_mpi_input_parameter(inputs.parallel)

        # if 'start' or 'width' are defined in velocity units, convert from frequency back
        # to velocity before tclean call. Added to support SRDP ALMA optimized imaging.
        if self.start_as_velocity:
            inputs.start = casa_tools.quanta.tos(self.start_as_velocity)
        if self.width_as_velocity:
            inputs.width = casa_tools.quanta.tos(self.width_as_velocity)

        # optionally override the output image rootname
        if isinstance(clean_imagename, str):
            imagename = clean_imagename
        else:
            imagename = inputs.imagename

        # Fallback for cases that do not set cyclefactor explicitly like
        # for PIPE-1782. Alternatively, one would have to pass cyclefactor
        # for all _do_clean calls.
        if cyclefactor is None:
            cyclefactor = inputs.cyclefactor

        clean_inputs = cleanbase.CleanBase.Inputs(inputs.context,
                                                  output_dir=inputs.output_dir,
                                                  vis=inputs.vis,
                                                  is_per_eb=inputs.is_per_eb,
                                                  imagename=imagename,
                                                  antenna=inputs.antenna,
                                                  intent=inputs.intent,
                                                  field=inputs.field,
                                                  spw=inputs.spw,
                                                  spwsel=inputs.spwsel_topo,
                                                  spwsel_all_cont=inputs.spwsel_all_cont,
                                                  spwsel_low_bandwidth=inputs.spwsel_low_bandwidth,
                                                  spwsel_low_spread=inputs.spwsel_low_spread,
                                                  reffreq=inputs.reffreq,
                                                  restfreq=inputs.restfreq,
                                                  conjbeams=inputs.conjbeams,
                                                  uvrange=inputs.uvrange,
                                                  orig_specmode=inputs.orig_specmode,
                                                  specmode=inputs.specmode,
                                                  gridder=inputs.gridder,
                                                  datacolumn=inputs.datacolumn,
                                                  datatype=inputs.datatype,
                                                  datatype_info=inputs.datatype_info,
                                                  deconvolver=inputs.deconvolver,
                                                  nterms=inputs.nterms,
                                                  cycleniter=inputs.cycleniter,
                                                  cyclefactor=cyclefactor,
                                                  nmajor=inputs.nmajor,
                                                  hm_minpsffraction=inputs.hm_minpsffraction,
                                                  hm_maxpsffraction=inputs.hm_maxpsffraction,
                                                  scales=inputs.scales,
                                                  outframe=inputs.outframe,
                                                  imsize=inputs.imsize,
                                                  cell=inputs.cell,
                                                  cfcache=inputs.cfcache,
                                                  phasecenter=inputs.phasecenter,
                                                  psf_phasecenter=inputs.psf_phasecenter,
                                                  stokes=inputs.stokes,
                                                  nchan=inputs.nchan,
                                                  nbin=inputs.nbin,
                                                  start=inputs.start,
                                                  width=inputs.width,
                                                  weighting=inputs.weighting,
                                                  robust=inputs.robust,
                                                  mosweight=inputs.mosweight,
                                                  uvtaper=inputs.uvtaper,
                                                  restoringbeam=inputs.restoringbeam,
                                                  iter=iternum,
                                                  mask=cleanmask,
                                                  savemodel=savemodel,
                                                  startmodel=startmodel,
                                                  hm_masking=inputs.hm_masking,
                                                  hm_sidelobethreshold=inputs.hm_sidelobethreshold,
                                                  hm_noisethreshold=inputs.hm_noisethreshold,
                                                  hm_lownoisethreshold=inputs.hm_lownoisethreshold,
                                                  hm_negativethreshold=inputs.hm_negativethreshold,
                                                  hm_minbeamfrac=inputs.hm_minbeamfrac,
                                                  hm_growiterations=inputs.hm_growiterations,
                                                  hm_dogrowprune=inputs.hm_dogrowprune,
                                                  hm_minpercentchange=inputs.hm_minpercentchange,
                                                  hm_fastnoise=inputs.hm_fastnoise,
                                                  niter=niter,
                                                  hm_nsigma=nsigma,
                                                  hm_perchanweightdensity=inputs.hm_perchanweightdensity,
                                                  hm_npixels=inputs.hm_npixels,
                                                  threshold=threshold,
                                                  sensitivity=sensitivity,
                                                  pblimit=inputs.pblimit,
                                                  pbmask=inputs.pbmask,
                                                  result=result,
                                                  parallel=parallel,
                                                  heuristics=inputs.image_heuristics,
                                                  calcpsf=calcpsf,
                                                  calcres=calcres,
                                                  wbawp=wbawp)
        clean_task = cleanbase.CleanBase(clean_inputs)

        clean_result = self._executor.execute(clean_task)

        # if 'start' or 'width' were defined in velocity units, convert from velocity back
        # to frequency after tclean call. Added to support SRDP ALMA optimized imaging.
        if self.start_as_velocity:
            inputs.start = self.start_as_frequency
        if self.width_as_velocity:
            inputs.width = self.width_as_frequency

        return clean_result

    # Remove pointing table.
    def _empty_pointing_table(self):
        # Concerned that simply renaming things directly
        # will corrupt the table cache, so do things using only the
        # table tool.
        for vis in self.inputs.vis:
            with casa_tools.TableReader('%s/POINTING' % vis, nomodify=False) as table:
                # make a copy of the table
                LOG.debug('Making copy of POINTING table')
                copy = table.copy('%s/POINTING_COPY' % vis, valuecopy=True)
                LOG.debug('Removing all POINTING table rows')
                table.removerows(list(range(table.nrows())))
                copy.done()

    # Restore pointing table
    def _restore_pointing_table(self):
        for vis in self.inputs.vis:
            # restore the copy of the POINTING table
            with casa_tools.TableReader('%s/POINTING_COPY' % vis, nomodify=False) as table:
                LOG.debug('Copying back into POINTING table')
                original = table.copy('%s/POINTING' % vis, valuecopy=True)
                original.done()

    def _calc_moment_image(self, imagename=None, moments=None, outfile=None, chans=None, iter=None):
        '''
        Computes moment image, writes it to disk and updates moment image metadata.

        This method is used in _calc_mom0_8() and _calc_mom0_8_10_fc().
        '''
        context = self.inputs.context

        # Determine moment image type.
        mom_type = ""
        if ".mom0" in outfile:
            mom_type += "mom0"
        elif ".mom8" in outfile:
            mom_type += "mom8"
        elif ".mom10" in outfile:
            mom_type += "mom10"
        if "_fc" in outfile:
            mom_type += "_fc"

        # Execute job to create the MOM8_FC image.
        job = casa_tasks.immoments(imagename=imagename, moments=moments, outfile=outfile, chans=chans)
        self._executor.execute(job)
        assert os.path.exists(outfile)

        # Using virtual spw setups for all interferometry pipelines
        virtspw = True

        # Update the metadata in the MOM8_FC image.
        imageheader.set_miscinfo(name=outfile, spw=self.inputs.spw, virtspw=virtspw,
                                 field=self.inputs.field, iter=iter,
                                 datatype=self.inputs.datatype, type=mom_type,
                                 intent=self.inputs.intent, specmode=self.inputs.orig_specmode,
                                 context=context)

    # Calculate a "mom0_fc", "mom8_fc" and "mom10_fc: images: this is a moment
    # 0 (integrated value of the spectrum), 8 (maximum value of the spectrum)
    # and 10 (minimum value of the spectrum) integration over the line-free
    # channels of the non-primary-beam corrected image-cube, after continuum
    # subtraction; where the "line-free" channels are taken from those identified
    # as continuum channels.
    # This is a diagnostic plot representing the residual emission
    # in the line-free (aka continuum) channels. If the continuum subtraction
    # worked well, then this image should just contain noise.
    def _calc_mom0_8_10_fc(self, result):

        # Find max iteration that was performed.
        maxiter = max(result.iterations.keys())

        # Get filename of image from result, and modify to select the
        # non-PB-corrected image.
        extension = '.tt0' if result.multiterm else ''
        imagename = result.iterations[maxiter]['image'].replace('.image', '.image%s' % (extension)).replace('.pbcor', '')
        # Set output filename for MOM0_FC image.
        mom0fc_name = '%s.mom0_fc' % imagename

        # Set output filename for MOM8_FC image.
        mom8fc_name = '%s.mom8_fc' % imagename

        # Set output filename for MOM10_FC image.
        mom10fc_name = '%s.mom10_fc' % imagename

        # Convert frequency ranges to channel ranges.
        cont_chan_ranges = utils.freq_selection_to_channels(imagename, self.cont_freq_ranges)

        # Only continue if there were continuum channel ranges for this spw.
        if cont_chan_ranges[0] != 'NONE':

            # Create a channel ranges string.
            if cont_chan_ranges[0] in ('ALL', 'ALLCONT'):
                cont_chan_ranges_str = ''
                cont_chan_indices = slice(None)
            else:
                cont_chan_ranges_str = ";".join(["%s~%s" % (ch0, ch1) for ch0, ch1 in cont_chan_ranges])
                cont_chan_indices = np.hstack([np.arange(start, stop + 1) for start, stop in cont_chan_ranges])

            # Calculate MOM0_FC image
            self._calc_moment_image(imagename=imagename, moments=[0], outfile=mom0fc_name, chans=cont_chan_ranges_str,
                                    iter=maxiter)
            # Update the result.
            result.set_mom0_fc(maxiter, mom0fc_name)

            # Calculate MOM8_FC image
            self._calc_moment_image(imagename=imagename, moments=[8], outfile=mom8fc_name, chans=cont_chan_ranges_str,
                                    iter=maxiter)

            # Calculate MOM10_FC image
            self._calc_moment_image(imagename=imagename, moments=[10], outfile=mom10fc_name, chans=cont_chan_ranges_str,
                                    iter=maxiter)

            # Calculate the MOM8_FC peak SNR for the QA

            # Create flattened PB over continuum channels
            flattened_pb_name = result.flux + extension + '.flattened_mom_0_8_10_fc'
            with casa_tools.ImageReader(result.flux + extension) as image:
                flattened_pb = image.collapse(function='mean', axes=[2, 3], chans=cont_chan_ranges_str, outfile=flattened_pb_name)
                flattened_pb.done()

            # If possible, create flattened mask over continuum channels
            cleanmask = result.iterations[maxiter].get('cleanmask', '')
            if os.path.exists(cleanmask):
                if '.mask' in cleanmask:
                    flattened_mask_name = cleanmask.replace('.mask', '.mask.flattened_mom_0_8_10_fc')
                elif '.cleanmask' in cleanmask:
                    flattened_mask_name = cleanmask.replace('.cleanmask', '.cleanmask.flattened_mom_0_8_10_fc')
                else:
                    raise 'Cannot handle clean mask name %s' % (os.path.basename(cleanmask))

                with casa_tools.ImageReader(cleanmask) as image:
                    flattened_mask_image = image.collapse(function='max', axes=[2, 3], chans=cont_chan_ranges_str, outfile=flattened_mask_name)
                    flattened_mask_image.done()
            else:
                flattened_mask_name = None

            # Calculate MOM8_FC statistics
            with casa_tools.ImageReader(mom8fc_name) as image:
                # Get the min, max, median, MAD and number of pixels of the MOM8 FC image from the area excluding the cleaned area edges (PIPE-704)
                mom8_statsmask = '"{:s}" > {:f}'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05)
                mom8_stats = image.statistics(mask=mom8_statsmask, robust=True, stretch=True)
                mom8_image_median_all = mom8_stats.get('median')[0]
                mom8_image_mad = mom8_stats.get('medabsdevmed')[0]
                mom8_image_min = mom8_stats.get('min')[0]
                mom8_image_max = mom8_stats.get('max')[0]
                mom8_n_pixels = int(mom8_stats.get('npts')[0])

                # Additionally get the median in the MOM8 FC annulus region for the peak SNR calculation
                mom8_statsmask2 = '"{:s}" > {:f} && "{:s}" < {:f}'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05, os.path.basename(flattened_pb_name), result.pblimit_cleanmask)
                mom8_stats2 = image.statistics(mask=mom8_statsmask2, robust=True, stretch=True)

                mom8_image_median_annulus = mom8_stats2.get('median')[0]

            # Calculate MOM10 FC statistics
            with casa_tools.ImageReader(mom10fc_name) as image:
                # Get the min, max, median, MAD and number of pixels of the MOM10 FC image from the area excluding the cleaned area edges
                mom10_statsmask = '"{:s}" > {:f}'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05)
                mom10_stats = image.statistics(mask=mom10_statsmask, robust=True)
                mom10_image_median_all = mom10_stats.get('median')[0]
                mom10_image_mad = mom10_stats.get('medabsdevmed')[0]
                mom10_image_min = mom10_stats.get('min')[0]
                mom10_image_max = mom10_stats.get('max')[0]
                mom10_n_pixels = int(mom10_stats.get('npts')[0])

                # Additionally get the median in the MOM8 FC annulus region for the peak SNR calculation
                mom10_statsmask2 = '"{:s}" > {:f} && "{:s}" < {:f}'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05, os.path.basename(flattened_pb_name), result.pblimit_cleanmask)
                mom10_stats2 = image.statistics(mask=mom10_statsmask2, robust=True)

                mom10_image_median_annulus = mom10_stats2.get('median')[0]

            # Get sigma and channel scaled MAD from the cube
            if flattened_mask_name is not None:
                # Mask cleanmask and 1.05 * pblimit_image < pb < pblimit_cleanmask
                cube_statsmask = '"{:s}" > {:f} && "{:s}" < {:f} && "{:s}" < 0.1'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05, os.path.basename(flattened_pb_name), result.pblimit_cleanmask, flattened_mask_name)
            else:
                # Mask 1.05 * pblimit_image < pb < pblimit_cleanmask
                cube_statsmask = '"{:s}" > {:f} && "{:s}" < {:f}'.format(os.path.basename(flattened_pb_name), result.pblimit_image * 1.05, os.path.basename(flattened_pb_name), result.pblimit_cleanmask)
                LOG.info('No cleanmask available to exclude for MOM8_FC RMS and peak SNR calculation. Calculating sigma, channel scaled MAD and peak SNR from annulus area of 1.05 x pblimit_image < pb < pblimit_cleanmask.')

            with casa_tools.ImageReader(imagename) as image:
                cube_stats_masked = image.statistics(mask=cube_statsmask, stretch=True, robust=True, axes=[0, 1, 2], algorithm='chauvenet', maxiter=5)

            cube_sigma_fc_chans = np.median(cube_stats_masked.get('sigma')[cont_chan_indices])
            cube_scaledMAD_fc_chans = np.median(cube_stats_masked.get('medabsdevmed')[cont_chan_indices]) / 0.6745

            mom8_fc_peak_snr = (mom8_image_max - mom8_image_median_annulus) / cube_scaledMAD_fc_chans

            LOG.info('MOM8_FC image {:s} has a maximum of {:#.5g}, median of {:#.5g} resulting in a Peak SNR of {:#.5g} times the channel scaled MAD of {:#.5g}.'.format(os.path.basename(mom8fc_name), mom8_image_max, mom8_image_median_annulus, mom8_fc_peak_snr, cube_scaledMAD_fc_chans))

            # New score based on mom8/mom10 histogram asymmetry and largest mom8 segment needs
            # more metrics (PIPE-1232)
            histogram_threshold = mom8_image_median_all + 2.0 * mom8_image_mad / 0.6745

            # Get the PB image as numpy array
            with casa_tools.ImageReader(flattened_pb_name) as image:
                flattened_pb_image = image.getchunk()[:,:,0,0]

            # Get the MOM8 FC image as numpy array
            with casa_tools.ImageReader(mom8fc_name) as image:
                mom8fc_image = image.getchunk()[:,:,0,0]
                mom8fc_image_summary = image.summary()

            mom8fc_masked_image = np.ma.array(mom8fc_image, mask=np.where(flattened_pb_image > result.pblimit_image * 1.05, False, True))

            # Get number of pixels per beam
            major_radius = casa_tools.quanta.getvalue(casa_tools.quanta.convert(mom8fc_image_summary['restoringbeam']['major'], 'rad')) / 2
            minor_radius = casa_tools.quanta.getvalue(casa_tools.quanta.convert(mom8fc_image_summary['restoringbeam']['minor'], 'rad')) / 2
            cellx = abs(casa_tools.quanta.getvalue(casa_tools.quanta.convert(casa_tools.quanta.quantity(mom8fc_image_summary['incr'][0], mom8fc_image_summary['axisunits'][0]), 'rad')))
            celly = abs(casa_tools.quanta.getvalue(casa_tools.quanta.convert(casa_tools.quanta.quantity(mom8fc_image_summary['incr'][1], mom8fc_image_summary['axisunits'][1]), 'rad')))
            num_pixels_in_beam = float(major_radius * minor_radius * np.pi / np.log(2) / cellx / celly)
            # Get threshold for maximum segment calculation
            cut1 = mom8_image_median_annulus + 3.0 * cube_scaledMAD_fc_chans
            cut2 = mom8_image_median_annulus + 0.5 * np.ma.max(mom8fc_masked_image - mom8_image_median_annulus)
            cut3 = mom8_image_median_annulus + 2.0 * cube_scaledMAD_fc_chans
            segments_threshold = max(min(cut1, cut2), cut3)

            # Get largest segment
            mom8_segments_image = np.ma.where(mom8fc_masked_image > segments_threshold, 1, 0)
            mom8_frac_max_segment = 0.0
            mom8_max_segment_beams = 0.0
            # Label segments
            mom8_label_image, num_labels = label(mom8_segments_image)
            if num_labels > 0:
                # Calculate largest segment size
                mom8_num_pixels_in_largest_segment = np.max([np.ma.sum(np.ma.where(mom8_label_image == i, 1, 0)) for i in range(1, num_labels + 1)])
                # Prune small segments
                if mom8_num_pixels_in_largest_segment >= 0.1 * num_pixels_in_beam:
                    mom8_frac_max_segment = mom8_num_pixels_in_largest_segment / mom8_n_pixels
                    mom8_max_segment_beams = mom8_num_pixels_in_largest_segment / num_pixels_in_beam

            # Get the MOM10 FC image as numpy array
            with casa_tools.ImageReader(mom10fc_name) as image:
                mom10fc_image = image.getchunk()[:,:,0,0]

            mom10fc_masked_image = np.ma.array(np.abs(mom10fc_image), mask=np.where(flattened_pb_image > result.pblimit_image * 1.05, False, True))

            # Get histogram asymmetry

            # Global minimum/maximum of both images
            mom8_10fc_min = np.ma.min(np.ma.append(mom8fc_masked_image, mom10fc_masked_image))
            mom8_10fc_max = np.ma.max(np.ma.append(mom8fc_masked_image, mom10fc_masked_image))

            # Calculate histograms
            # np.histogram does not know masked arrays; need to convert to normal array using inverted mask as index
            mom8fc_histogram, bins = np.histogram(mom8fc_masked_image[np.invert(mom8fc_masked_image.mask)], range=[mom8_10fc_min, mom8_10fc_max], bins='auto')
            mom8fc_flux_bins = 0.5*(bins[:-1]+bins[1:])

            # np.histogram does not know masked arrays; need to convert to normal array using inverted mask as index
            mom10fc_histogram, bins = np.histogram(mom10fc_masked_image[np.invert(mom10fc_masked_image.mask)], range=[mom8_10fc_min, mom8_10fc_max], bins='auto')
            mom10fc_flux_bins = 0.5*(bins[:-1]+bins[1:])

            # Interpolating numpix of moment10 at the position of moment8 flux
            mom8fc_histogram_interp = np.interp(mom10fc_flux_bins, mom8fc_flux_bins, mom8fc_histogram, left=0.0, right=0.0)

            # Interpolating numpix of moment8 at the position of moment10 flux
            mom10fc_histogram_interp = np.interp(mom8fc_flux_bins, mom10fc_flux_bins, mom10fc_histogram, left=0.0, right=0.0)

            # Compute the deviation between mom8 and mom10 pixel histogram for every flux bin
            deviation1 = mom8fc_histogram_interp[mom10fc_flux_bins > histogram_threshold] - mom10fc_histogram[mom10fc_flux_bins > histogram_threshold]
            deviation2 = mom10fc_histogram_interp[mom8fc_flux_bins > histogram_threshold] - mom8fc_histogram[mom8fc_flux_bins > histogram_threshold]

            # Compute the area of the histogram (whatever miminum)
            smallerarea = np.min(np.append(np.sum(mom10fc_histogram[mom10fc_flux_bins > histogram_threshold]), np.sum(mom8fc_histogram[mom8fc_flux_bins > histogram_threshold])))

            # histasymm is now comparing the summation of the absolute deviation vs histogram area
            mom_8_10_histogram_asymmetry = 0.5 * (np.sum(np.abs(deviation1)) + np.sum(np.abs(deviation2))) / smallerarea

            # Update the result.
            result.set_cube_sigma_fc_chans(maxiter, cube_sigma_fc_chans)
            result.set_cube_scaledMAD_fc_chans(maxiter, cube_scaledMAD_fc_chans)

            result.set_mom8_fc(maxiter, mom8fc_name)
            result.set_mom8_fc_image_min(maxiter, mom8_image_min)
            result.set_mom8_fc_image_max(maxiter, mom8_image_max)
            result.set_mom8_fc_image_median_all(maxiter, mom8_image_median_all)
            result.set_mom8_fc_image_median_annulus(maxiter, mom8_image_median_annulus)
            result.set_mom8_fc_image_mad(maxiter, mom8_image_mad)
            result.set_mom8_fc_peak_snr(maxiter, mom8_fc_peak_snr)
            result.set_mom8_fc_n_pixels(maxiter, mom8_n_pixels)
            result.set_mom8_fc_frac_max_segment(maxiter, mom8_frac_max_segment)
            result.set_mom8_fc_max_segment_beams(maxiter, mom8_max_segment_beams)

            result.set_mom10_fc(maxiter, mom10fc_name)
            result.set_mom10_fc_image_min(maxiter, mom10_image_min)
            result.set_mom10_fc_image_max(maxiter, mom10_image_max)
            result.set_mom10_fc_image_median_all(maxiter, mom10_image_median_all)
            result.set_mom10_fc_image_median_annulus(maxiter, mom10_image_median_annulus)
            result.set_mom10_fc_image_mad(maxiter, mom10_image_mad)
            result.set_mom10_fc_n_pixels(maxiter, mom10_n_pixels)

            result.set_mom8_10_fc_histogram_asymmetry(maxiter, mom_8_10_histogram_asymmetry)

        else:
            LOG.warning('Cannot create MOM0_FC / MOM8_FC / MOM10_FC images for intent "%s", '
                        'field %s, spw %s, no continuum ranges found.' %
                        (self.inputs.intent, self.inputs.field, self.inputs.spw))

    def _calc_mom0_8(self, result):
        '''
        Creates moment 0 (integrated flux) and 8 (peak flux) maps for non--primary-beam corrected
        images after continuum subtraction, using *all channels* (may include channels with lines).

        See PIPE-558 (https://open-jira.nrao.edu/browse/PIPE-558).
        '''

        # Find max iteration that was performed.
        maxiter = max(result.iterations.keys())

        # Get filename of image from result, and modify to select the
        # non-PB-corrected image.
        imagename = result.iterations[maxiter]['image'].replace('.pbcor', '')

        # Set output filename for MOM0 all channel image.
        mom0_name = '%s.mom0' % imagename
        # Calculate moment image
        self._calc_moment_image(imagename=imagename, moments=[0], outfile=mom0_name, chans='', iter=maxiter)
        # Update the result.
        result.set_mom0(maxiter, mom0_name)

        # Set output filename for MOM8 all channel image.
        mom8_name = '%s.mom8' % imagename
        # Calculate moment image
        self._calc_moment_image(imagename=imagename, moments=[8], outfile=mom8_name, chans='', iter=maxiter)
        # Update the result.
        result.set_mom8(maxiter, mom8_name)

    def _update_miscinfo(self, imagename: str, nfield: Optional[int] = None, datamin: Optional[float] = None,
                         datamax: Optional[float] = None, datarms: Optional[float] = None,
                         stokes: Optional[str] = None, effbw: Optional[float] = None,
                         level: Optional[str] = None, ctrfrq: Optional[float] = None,
                         obspatt: Optional[str] = None, arrays: Optional[str] = None,
                         modifier: Optional[str] = None, session: Optional[str] = None):
        """
        Update image header keywords.

        Args:
            imagename (str): image name
            nfield (int): number of mosaic fields
            datamin (float): minimum image value
            datamax (float): maximum image value
            datarms (float): rms image value
            stokes (str): Stokes parameter
            effbw (float): effective bandwidth in Hz
            level (str): OUS kind (member, group)
            obspatt (str): observing pattern (mos, sf, sd)
            ctrfrq: Image center frequency in Hz
            modifier (str): image name modifier (binnedN?)
            session (str): session name
        Returns:
            None
        """

        with casa_tools.ImageReader(imagename) as image:
            # Get current header
            info = image.miscinfo()

            # Update keywords that are not None
            frame = inspect.currentframe()
            keywords, _, _, values = inspect.getargvalues(frame)
            for keyword in keywords:
                if keyword not in ('self', 'imagename') and values[keyword] is not None:
                    if keyword == 'session':
                        # PIPE-2148, limiting 'sessionX' keyword length to 68 characters
                        # due to FITS header keyword string length limit.
                        info = imageheader.wrap_key(info, 'sessio', session)
                    else:
                        info[keyword] = values[keyword]

            # Save header back to image
            image.setmiscinfo(info)
