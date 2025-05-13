import os
import traceback

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.imageheader as imageheader
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import logging

from .resultobjects import TcleanResult

LOG = infrastructure.get_logger(__name__)

# The basic clean tasks classes. Clean performs a single clean run.


class CleanBaseInputs(vdp.StandardInputs):

    # simple properties ------------------------------------------------------------------------------------------------

    antenna = vdp.VisDependentProperty(default='')
    datacolumn = vdp.VisDependentProperty(default='')
    datatype = vdp.VisDependentProperty(default='')
    datatype_info = vdp.VisDependentProperty(default='')
    deconvolver = vdp.VisDependentProperty(default='')
    cycleniter = vdp.VisDependentProperty(default=-999)
    cyclefactor = vdp.VisDependentProperty(default=-999.0)
    nmajor = vdp.VisDependentProperty(default=None)
    cfcache = vdp.VisDependentProperty(default='')
    field = vdp.VisDependentProperty(default='')
    gridder = vdp.VisDependentProperty(default='')
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='')
    iter = vdp.VisDependentProperty(default=0)
    mask = vdp.VisDependentProperty(default='')
    hm_dogrowprune = vdp.VisDependentProperty(default=None)
    hm_growiterations = vdp.VisDependentProperty(default=-999)
    hm_lownoisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_masking = vdp.VisDependentProperty(default='auto')
    hm_minbeamfrac = vdp.VisDependentProperty(default=-999.0)
    hm_minpercentchange = vdp.VisDependentProperty(default=-999.0)
    hm_minpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_maxpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_fastnoise = vdp.VisDependentProperty(default=None)
    hm_negativethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_noisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_sidelobethreshold = vdp.VisDependentProperty(default=-999.0)
    mosweight = vdp.VisDependentProperty(default=None)
    nchan = vdp.VisDependentProperty(default=-1)
    nbin = vdp.VisDependentProperty(default=-1)
    niter = vdp.VisDependentProperty(default=5000)
    hm_nsigma = vdp.VisDependentProperty(default=0.0)
    hm_perchanweightdensity = vdp.VisDependentProperty(default=None)
    hm_npixels = vdp.VisDependentProperty(default=0)
    nterms = vdp.VisDependentProperty(default=None)
    orig_specmode = vdp.VisDependentProperty(default='')
    outframe = vdp.VisDependentProperty(default='LSRK')
    parallel = vdp.VisDependentProperty(default='automatic')
    pblimit = vdp.VisDependentProperty(default=0.2)
    is_per_eb = vdp.VisDependentProperty(default=False)
    phasecenter = vdp.VisDependentProperty(default='')
    psf_phasecenter = vdp.VisDependentProperty(default='')
    restoringbeam = vdp.VisDependentProperty(default='common')
    robust = vdp.VisDependentProperty(default=-999.0)
    savemodel = vdp.VisDependentProperty(default='none')
    startmodel = vdp.VisDependentProperty(default='')
    scales = vdp.VisDependentProperty(default=None)
    sensitivity = vdp.VisDependentProperty(default=None)
    spwsel_all_cont = vdp.VisDependentProperty(default=None)
    spwsel_low_bandwidth = vdp.VisDependentProperty(default=None)
    spwsel_low_spread = vdp.VisDependentProperty(default=None)
    start = vdp.VisDependentProperty(default='')
    stokes = vdp.VisDependentProperty(default='I')
    threshold = vdp.VisDependentProperty(default=None)
    usepointing = vdp.VisDependentProperty(default=None)
    uvrange = vdp.VisDependentProperty(default='')
    uvtaper = vdp.VisDependentProperty(default=None)
    weighting = vdp.VisDependentProperty(default='briggs')
    width = vdp.VisDependentProperty(default='')
    restfreq = vdp.VisDependentProperty(default=None)
    wprojplanes = vdp.VisDependentProperty(default=None)
    wbawp = vdp.VisDependentProperty(default=None)
    rotatepastep = vdp.VisDependentProperty(default=None)
    calcpsf = vdp.VisDependentProperty(default=None)
    calcres = vdp.VisDependentProperty(default=None)
    pbmask = vdp.VisDependentProperty(default=None)

    # properties requiring some logic ----------------------------------------------------------------------------------

    @vdp.VisDependentProperty
    def cell(self):
        return []

    @vdp.VisDependentProperty
    def imsize(self):
        return []

    @imsize.convert
    def imsize(self, value):
        if isinstance(value, str) and value.startswith('['):
            # Remove the characters [, ], and ' from the value.
            temp = value.translate(str.maketrans("[]'"))
            temp = temp.split(',')
            return list(map(int, temp))
        return value

    @vdp.VisDependentProperty
    def specmode(self, value):
        if 'TARGET' in self.intent:
            return 'cube'
        return 'mfs'

    @vdp.VisDependentProperty
    def spw(self):
        first_ms = self.context.observing_run.measurement_sets[0]
        return ','.join([spw.id for spw in first_ms.get_spectral_windows()])

    @vdp.VisDependentProperty
    def spwsel(self):
        return []

    def __init__(self, context, output_dir=None, vis=None, imagename=None, datacolumn=None, datatype=None, datatype_info=None, intent=None, field=None,
                 spw=None, spwsel=None, spwsel_all_cont=None, spwsel_low_bandwidth=None, spwsel_low_spread=None, uvrange=None, orig_specmode=None,
                 specmode=None, gridder=None, deconvolver=None, uvtaper=None, nterms=None, cycleniter=None, cyclefactor=None, nmajor=None, hm_minpsffraction=None,
                 hm_maxpsffraction=None, scales=None, outframe=None, imsize=None,
                 cell=None, phasecenter=None, psf_phasecenter=None, nchan=None, nbin=None, start=None, width=None, stokes=None, weighting=None,
                 robust=None, restoringbeam=None, iter=None, mask=None, savemodel=None, startmodel=None, hm_masking=None,
                 hm_sidelobethreshold=None, hm_noisethreshold=None, hm_lownoisethreshold=None, wprojplanes=None,
                 hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None, hm_dogrowprune=None,
                 hm_minpercentchange=None, hm_fastnoise=None, pblimit=None, niter=None, hm_nsigma=None,
                 hm_perchanweightdensity=None, hm_npixels=None, threshold=None, sensitivity=None, reffreq=None,
                 restfreq=None, conjbeams=None, is_per_eb=None, antenna=None, usepointing=None, mosweight=None,
                 result=None, parallel=None, heuristics=None, rotatepastep=None, cfcache=None, calcpsf=None,
                 calcres=None, wbawp=None, pbmask=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.imagename = imagename
        self.datacolumn = datacolumn
        self.datatype = datatype
        self.datatype_info = datatype_info
        self.intent = intent
        self.field = field
        self.spw = spw
        self.spwsel = spwsel
        self.spwsel_all_cont = spwsel_all_cont
        self.spwsel_low_bandwidth = spwsel_low_bandwidth
        self.spwsel_low_spread = spwsel_low_spread
        self.uvrange = uvrange
        self.savemodel = savemodel
        self.startmodel = startmodel
        self.orig_specmode = orig_specmode
        self.specmode = specmode
        self.gridder = gridder
        self.deconvolver = deconvolver
        self.uvtaper = uvtaper
        self.nterms = nterms
        self.cycleniter = cycleniter
        self.cyclefactor = cyclefactor
        self.nmajor = nmajor
        self.hm_minpsffraction = hm_minpsffraction
        self.hm_maxpsffraction = hm_maxpsffraction
        self.scales = scales
        self.outframe = outframe
        self.imsize = imsize
        self.cell = cell
        self.phasecenter = phasecenter
        self.psf_phasecenter = psf_phasecenter
        self.nchan = nchan
        self.nbin = nbin
        self.start = start
        self.width = width
        self.stokes = stokes
        self.weighting = weighting
        self.robust = robust
        self.restoringbeam = restoringbeam
        self.iter = iter
        self.mask = mask
        self.pbmask = pbmask

        self.hm_masking = hm_masking
        self.hm_sidelobethreshold = hm_sidelobethreshold
        self.hm_noisethreshold = hm_noisethreshold
        self.hm_lownoisethreshold = hm_lownoisethreshold
        self.hm_negativethreshold = hm_negativethreshold
        self.hm_minbeamfrac = hm_minbeamfrac
        self.hm_growiterations = hm_growiterations
        self.hm_dogrowprune = hm_dogrowprune
        self.hm_minpercentchange = hm_minpercentchange
        self.hm_fastnoise = hm_fastnoise

        self.cfcache = cfcache
        self.pblimit = pblimit
        self.niter = niter
        self.threshold = threshold
        self.hm_nsigma = hm_nsigma
        self.hm_perchanweightdensity = hm_perchanweightdensity
        self.hm_npixels = hm_npixels
        self.sensitivity = sensitivity
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.conjbeams = conjbeams
        self.result = result
        self.parallel = parallel
        self.is_per_eb = is_per_eb
        self.antenna = antenna
        self.usepointing = usepointing
        self.mosweight = mosweight
        self.wprojplanes = wprojplanes
        self.wbawp = wbawp
        self.rotatepastep = rotatepastep
        self.heuristics = heuristics
        self.calcpsf = calcpsf
        self.calcres = calcres


class CleanBase(basetask.StandardTaskTemplate):
    Inputs = CleanBaseInputs

    is_multi_vis_task = True

    def prepare(self):
        context = self.inputs.context
        inputs = self.inputs

        # Make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        # Remove MSs that do not contain data for the given field(s)
        scanidlist, visindexlist = inputs.heuristics.get_scanidlist(inputs.vis, inputs.field, inputs.intent)
        inputs.vis = [inputs.vis[i] for i in visindexlist]
        inputs.spwsel = [inputs.spwsel[i] for i in visindexlist]

        # Initialize imaging results structure
        if not inputs.result:
            plotdir = os.path.join(inputs.context.report_dir,
                                   'stage%s' % inputs.context.stage.split('_')[0])
            field_ids = inputs.heuristics.field(inputs.intent, inputs.field)
            result = TcleanResult(vis=inputs.vis,
                                  datacolumn=inputs.datacolumn,
                                  datatype=inputs.datatype,
                                  datatype_info=inputs.datatype_info,
                                  sourcename=inputs.field,
                                  field_ids=field_ids,
                                  intent=inputs.intent,
                                  spw=inputs.spw,
                                  orig_specmode=inputs.orig_specmode,
                                  specmode=inputs.specmode,
                                  stokes=inputs.stokes,
                                  multiterm=inputs.nterms if inputs.deconvolver == 'mtmfs' else None,
                                  plotdir=plotdir, imaging_mode=inputs.heuristics.imaging_mode,
                                  is_per_eb=inputs.is_per_eb,
                                  is_eph_obj=inputs.heuristics.is_eph_obj(inputs.field))
        else:
            result = inputs.result

        try:
            result = self._do_clean_cycle(scanidlist, result, iter=inputs.iter)
        except Exception as e:
            error_msg = ('Cleaning failure for field {!s}, intent {!s}, specmode {!s}, spw {!s}, iteration {!s}: {!s}'.
                         format(inputs.field, inputs.intent, inputs.specmode, inputs.spw, inputs.iter, e))
            result.error = CleanBaseError(error_msg, 'Cleaning failure')
            LOG.error(error_msg)
            LOG.info(traceback.format_exc())
        return result

    def analyse(self, result):
        return result

    def _do_clean_cycle(self, scanidlist=None, result=None, iter=1):
        """
        Compute a clean image.
        """
        if scanidlist is None:
            scanidlist = []

        context = self.inputs.context
        inputs = self.inputs

        # Derive names of clean products for this iteration
        old_model_name = result.model
        model_name = '%s.%s.iter%s.model' % (inputs.imagename, inputs.stokes, iter)
        if old_model_name is not None:
            if os.path.exists(old_model_name):
                if result.multiterm:
                    rename_image(old_name=old_model_name, new_name=model_name,
                                 extensions=['.tt%d' % nterm for nterm in range(result.multiterm)])
                else:
                    rename_image(old_name=old_model_name, new_name=model_name)

        if inputs.niter == 0 and not (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            image_name = ''
        else:
            image_name = '%s.%s.iter%s.image' % (
                inputs.imagename, inputs.stokes, iter)

        residual_name = '%s.%s.iter%s.residual' % (
            inputs.imagename, inputs.stokes, iter)
        psf_name = '%s.%s.iter%s.psf' % (
            inputs.imagename, inputs.stokes, iter)
        flux_name = '%s.%s.iter%s.pb' % (
            inputs.imagename, inputs.stokes, iter)
        mask_name = '%s.%s.iter%s.mask' % (
            inputs.imagename, inputs.stokes, iter)
        alpha_name = '%s.%s.iter%s.alpha' % (
            inputs.imagename, inputs.stokes, iter)
        pbcor_image_name = '%s.%s.iter%s.image.pbcor' % (
            inputs.imagename, inputs.stokes, iter)

        parallel = all([mpihelpers.parse_mpi_input_parameter(inputs.parallel), 'TARGET' in inputs.intent])

        # PIPE-1878: calculate iteration/specmode-dependent threshold scaling factor (only used for VLA-PI) to correct the
        # potential tclean noise estimation bias.
        rms_multiplier = inputs.heuristics.get_nfrms_multiplier(iter, inputs.intent, inputs.specmode, image_name)

        # Need to translate the virtual spw IDs to real ones
        real_spwsel = context.observing_run.get_real_spwsel(inputs.spwsel, inputs.vis)

        # Common tclean parameters
        tclean_job_parameters = {
            'vis':           inputs.vis,
            'imagename':     '%s.%s.iter%s' % (os.path.basename(inputs.imagename), inputs.stokes, iter),
            'datacolumn':    inputs.datacolumn,
            'antenna':       inputs.antenna,
            'field':         inputs.field,
            'spw':           real_spwsel,
            'intent':        utils.to_CASA_intent(inputs.ms[0], inputs.intent),
            'specmode':      inputs.specmode if inputs.specmode != 'cont' else 'mfs',
            'gridder':       inputs.gridder,
            'pblimit':       inputs.pblimit,
            'niter':         inputs.niter,
            'threshold':     inputs.threshold,
            'deconvolver':   inputs.deconvolver,
            'interactive':   False,
            'nchan':         inputs.nchan,
            'start':         inputs.start,
            'width':         inputs.width,
            'imsize':        inputs.imsize,
            'cell':          inputs.cell,
            'cfcache':       inputs.cfcache,
            'stokes':        inputs.stokes,
            'weighting':     inputs.weighting,
            'robust':        inputs.robust,
            'restoringbeam': inputs.restoringbeam,
            'uvrange':       inputs.uvrange,
            'savemodel':     inputs.savemodel,
            'startmodel':    inputs.startmodel,
            'perchanweightdensity':  inputs.hm_perchanweightdensity,
            'npixels':    inputs.hm_npixels,
            'parallel':     parallel,
            'wbawp':        inputs.wbawp,
            'fullsummary':   True
        }

        # Set special phasecenter and outframe for ephemeris objects.
        # Needs to be done here since the explicit coordinates are
        # used in heuristics methods upstream.
        if inputs.heuristics.is_eph_obj(inputs.field):
            tclean_job_parameters['phasecenter'] = 'TRACKFIELD'
            tclean_job_parameters['psfphasecenter'] = None
            # 2018-08-13: Spectral tracking has been implemented via a new
            # specmode option (CAS-11766).
            if inputs.specmode == 'cube':
                tclean_job_parameters['specmode'] = 'cubesource'
            # 2018-04-19: 'REST' does not yet work (see CAS-8965, CAS-9997)
            #tclean_job_parameters['outframe'] = 'REST'
            tclean_job_parameters['outframe'] = ''
            # 2018-07-10: Parallel imaging of ephemeris objects does not
            # yet work (see CAS-11631)
            # 2021-02-16: PIPE-981 asks for allowing parallelized tclean
            # runs for ephemeris sources.
            #tclean_job_parameters['parallel'] = False
        else:
            tclean_job_parameters['phasecenter'] = inputs.phasecenter
            if inputs.gridder in ('mosaic', 'awproject') and inputs.psf_phasecenter != inputs.phasecenter:
                tclean_job_parameters['psfphasecenter'] = inputs.psf_phasecenter
                result.used_psfphasecenter = True
            else:
                tclean_job_parameters['psfphasecenter'] = None
            tclean_job_parameters['outframe'] = inputs.outframe

        if scanidlist not in [[], None]:
            tclean_job_parameters['scan'] = scanidlist

        # special frequency interpolation setting for cube mode and nbin=2 (PIPE-2115)
        if inputs.specmode == 'cube' and inputs.nbin == 2:
            tclean_job_parameters['interpolation'] = 'nearest'

        # Set up masking parameters
        if inputs.hm_masking == 'auto':
            tclean_job_parameters['usemask'] = 'auto-multithresh'

            # get heuristics parameters
            (
                sidelobethreshold,
                noisethreshold,
                lownoisethreshold,
                negativethreshold,
                minbeamfrac,
                growiterations,
                dogrowprune,
                minpercentchange,
                fastnoise,
            ) = inputs.heuristics.get_autobox_params(
                iter, inputs.intent, inputs.specmode, inputs.robust, rms_multiplier=rms_multiplier
            )

            # Override individually with manual settings
            if inputs.hm_sidelobethreshold != -999.0:
                tclean_job_parameters['sidelobethreshold'] = inputs.hm_sidelobethreshold
            elif sidelobethreshold is not None:
                tclean_job_parameters['sidelobethreshold'] = sidelobethreshold

            if inputs.hm_noisethreshold != -999.0:
                tclean_job_parameters['noisethreshold'] = inputs.hm_noisethreshold
            elif noisethreshold is not None:
                tclean_job_parameters['noisethreshold'] = noisethreshold

            if inputs.hm_lownoisethreshold != -999.0:
                tclean_job_parameters['lownoisethreshold'] = inputs.hm_lownoisethreshold
            elif lownoisethreshold is not None:
                tclean_job_parameters['lownoisethreshold'] = lownoisethreshold

            if inputs.hm_negativethreshold != -999.0:
                tclean_job_parameters['negativethreshold'] = inputs.hm_negativethreshold
            elif negativethreshold is not None:
                tclean_job_parameters['negativethreshold'] = negativethreshold

            if inputs.hm_minbeamfrac != -999.0:
                tclean_job_parameters['minbeamfrac'] = inputs.hm_minbeamfrac
            elif minbeamfrac is not None:
                tclean_job_parameters['minbeamfrac'] = minbeamfrac

            if inputs.hm_growiterations != -999:
                tclean_job_parameters['growiterations'] = inputs.hm_growiterations
            elif growiterations is not None:
                tclean_job_parameters['growiterations'] = growiterations

            if inputs.hm_dogrowprune not in (None, ''):
                tclean_job_parameters['dogrowprune'] = inputs.hm_dogrowprune
            elif dogrowprune is not None:
                tclean_job_parameters['dogrowprune'] = dogrowprune

            if inputs.hm_minpercentchange != -999:
                tclean_job_parameters['minpercentchange'] = inputs.hm_minpercentchange
            elif minpercentchange is not None:
                tclean_job_parameters['minpercentchange'] = minpercentchange

            if inputs.hm_fastnoise not in (None, ''):
                tclean_job_parameters['fastnoise'] = inputs.hm_fastnoise
            elif fastnoise is not None:
                tclean_job_parameters['fastnoise'] = fastnoise

        else:
            if inputs.hm_fastnoise not in (None, ''):
                tclean_job_parameters['fastnoise'] = inputs.hm_fastnoise
            else:
                tclean_job_parameters['fastnoise'] = True
            if inputs.hm_masking != 'none' and inputs.mask == 'pb':
                # In manual cleaning mode decide for cleaning with pbmask according
                # to heuristic class method (see PIPE-977)
                tclean_job_parameters['usemask'] = 'pb'
                tclean_job_parameters['pbmask'] = inputs.pbmask if inputs.pbmask else inputs.heuristics.pbmask()
            elif (inputs.hm_masking != 'none') and (inputs.mask != ''):
                tclean_job_parameters['usemask'] = 'user'
                tclean_job_parameters['mask'] = inputs.mask

        # Show nterms parameter only if it is used.
        if result.multiterm:
            tclean_job_parameters['nterms'] = result.multiterm

        # Select whether to restore image
        if inputs.niter == 0 and not (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            tclean_job_parameters['restoration'] = False
            tclean_job_parameters['pbcor'] = False
        else:
            tclean_job_parameters['restoration'] = True
            tclean_job_parameters['pbcor'] = inputs.heuristics.pb_correction()

        # Re-use products from previous iteration.
        if iter > 0:
            tclean_job_parameters['restart'] = True
            tclean_job_parameters['calcpsf'] = False
            tclean_job_parameters['calcres'] = False

        # Allow setting calcpsf and calcres explicitly
        if type(inputs.calcpsf) is bool:
            tclean_job_parameters['calcpsf'] = inputs.calcpsf
        if type(inputs.calcres) is bool:
            tclean_job_parameters['calcres'] = inputs.calcres

        # Additional heuristics or task parameters
        if inputs.cyclefactor not in (None, -999):
            tclean_job_parameters['cyclefactor'] = inputs.cyclefactor
        else:
            # Call first and assign to variable to avoid calling slow methods twice
            cyclefactor = inputs.heuristics.cyclefactor(iter)
            if cyclefactor:
                tclean_job_parameters['cyclefactor'] = cyclefactor

        if inputs.hm_minpsffraction not in (None, -999):
            tclean_job_parameters['minpsffraction'] = inputs.hm_minpsffraction

        if inputs.hm_maxpsffraction not in (None, -999):
            tclean_job_parameters['maxpsffraction'] = inputs.hm_maxpsffraction

        if inputs.cycleniter not in (None, -999):
            tclean_job_parameters['cycleniter'] = inputs.cycleniter
        else:
            cycleniter = inputs.heuristics.cycleniter(iter)
            if cycleniter is not None:
                tclean_job_parameters['cycleniter'] = cycleniter

        if inputs.nmajor not in (None, -999):
            tclean_job_parameters['nmajor'] = inputs.nmajor
        else:
            nmajor = inputs.heuristics.nmajor(iter)
            if nmajor is not None:
                tclean_job_parameters['nmajor'] = nmajor

        if inputs.scales:
            tclean_job_parameters['scales'] = inputs.scales
        else:
            scales = inputs.heuristics.scales(iter)
            if scales:
                tclean_job_parameters['scales'] = scales

        if inputs.uvrange:
            tclean_job_parameters['uvrange'] = inputs.uvrange
        else:
            uvrange, _ = inputs.heuristics.uvrange(field=inputs.field, spwspec=inputs.spw, specmode=inputs.specmode)
            if uvrange:
                tclean_job_parameters['uvrange'] = uvrange

        if inputs.uvtaper:
            tclean_job_parameters['uvtaper'] = inputs.uvtaper
        else:
            uvtaper = inputs.heuristics.uvtaper(None)
            if uvtaper:
                tclean_job_parameters['uvtaper'] = uvtaper

        if inputs.reffreq:
            tclean_job_parameters['reffreq'] = inputs.reffreq
        else:
            reffreq = inputs.heuristics.reffreq()
            if reffreq:
                tclean_job_parameters['reffreq'] = reffreq

        if inputs.restfreq:
            tclean_job_parameters['restfreq'] = inputs.restfreq
        else:
            restfreq = inputs.heuristics.restfreq(specmode=inputs.specmode, nchan=inputs.nchan, start=inputs.start, width=inputs.width)
            if restfreq:
                tclean_job_parameters['restfreq'] = restfreq

        if inputs.conjbeams is not None:
            tclean_job_parameters['conjbeams'] = inputs.conjbeams
        else:
            conjbeams = inputs.heuristics.conjbeams()
            if conjbeams is not None:
                tclean_job_parameters['conjbeams'] = conjbeams

        if inputs.usepointing is not None:
            tclean_job_parameters['usepointing'] = inputs.usepointing
        else:
            usepointing = inputs.heuristics.usepointing()
            if usepointing is not None:
                tclean_job_parameters['usepointing'] = usepointing

        if inputs.mosweight not in (None, ''):
            tclean_job_parameters['mosweight'] = inputs.mosweight
        else:
            mosweight = inputs.heuristics.mosweight(inputs.intent, inputs.field)
            if mosweight is not None:
                tclean_job_parameters['mosweight'] = mosweight

        tclean_job_parameters['nsigma'] = inputs.heuristics.nsigma(
            iter, inputs.hm_nsigma, inputs.hm_masking, rms_multiplier=rms_multiplier
        )
        tclean_job_parameters['wprojplanes'] = inputs.heuristics.wprojplanes(gridder=inputs.gridder, spwspec=inputs.spw)
        tclean_job_parameters['rotatepastep'] = inputs.heuristics.rotatepastep()
        tclean_job_parameters['smallscalebias'] = inputs.heuristics.smallscalebias()
        tclean_job_parameters['usepointing'] = inputs.heuristics.usepointing()
        tclean_job_parameters['pointingoffsetsigdev'] = inputs.heuristics.pointingoffsetsigdev()
        tclean_job_parameters['psfcutoff'] = inputs.heuristics.psfcutoff()

        # Up until CASA 6.1 (including) it is was necessary to run tclean calls with
        # restoringbeam == 'common' in two steps in HPC mode (CAS-10849).
        # With CASA 6.2.0-57 the cube refactor is in place and the two step
        # process is no longer needed (PIPE-980). See removed code at:
        # https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/browse/pipeline/hif/tasks/tclean/cleanbase.py?at=15e495a29d0bfc93892c65eceb660d61a1805790#521

        # PIPE-1672: use fullsummary=False for cube imaging to avoid potential MPIbuffer-related issues.
        if 'cube' in tclean_job_parameters['specmode']:
            tclean_job_parameters['fullsummary'] = False

        job = casa_tasks.tclean(**tclean_job_parameters)
        tclean_result = self._executor.execute(job)

        # Record last tclean command for weblog
        result.set_tclean_command(str(job))

        tclean_stopcode_ignore = inputs.heuristics.tclean_stopcode_ignore(iter, inputs.hm_masking)
        if inputs.niter > 0:

            tclean_stopcode = tclean_result['stopcode']
            tclean_iterdone = tclean_result['iterdone']
            tclean_niter = tclean_result['niter']
            tclean_nmajordone = tclean_result['nmajordone']
            result.set_tclean_stopcode(tclean_stopcode)
            result.set_tclean_stopreason(tclean_stopcode)
            result.set_tclean_iterdone(tclean_iterdone)
            result.set_nmajordone(iter, tclean_nmajordone)

            # The return structure of tclean_result['summaryminor'] has changed after CAS-6692 (CASA ver>=6.5).
            # Before CAS-6692, tclean_result['summaryminor'] is a 2D array with 6 rows,
            #   idx=0 : iteration number
            #   idx=1 : peak residual
            #   idx=2 : model flux
            #   idx=3 : cyclethreshold
            #   idx=4 : deconvolver id (for multi-field)
            #   idx=5 : subimage id (related to channel id, stokes id..)
            # https://open-jira.nrao.edu/browse/CAS-6692?focusedCommentId=60810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-60810
            # note that the subimage id (also called planeid or chunckid here) is formulated as:
            #   chunkid = chanid+polid*nSubChans
            # which can be obscure and depending on the MPI setup:
            # https://open-jira.nrao.edu/browse/CAS-6692?focusedCommentId=190256&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-190256
            #
            # After CAS-6692, tclean_result['summaryminor'] is described in details on the latest CASAdocs:
            # https://casadocs.readthedocs.io/en/latest/notebooks/synthesis_imaging.html#Returned-Dictionary
            #
            # Also see the comparison from one practical example:
            # https://open-jira.nrao.edu/browse/CAS-6692?focusedCommentId=190220&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-190220

            if isinstance(tclean_result['summaryminor'], dict):
                # after CAS-6692
                tclean_summaryminor = tclean_result['summaryminor']
                result.set_summaryminor(iter, tclean_summaryminor)
            else:
                # before CAS-6692
                tclean_nminordone = tclean_result['summaryminor'][0, :]
                tclean_peakresidual = tclean_result['summaryminor'][1, :]
                tclean_totalflux = tclean_result['summaryminor'][2, :]
                tclean_planeid = tclean_result['summaryminor'][5, :]
                result.set_nminordone_array(iter, tclean_nminordone)
                result.set_peakresidual_array(iter, tclean_peakresidual)
                result.set_totalflux_array(iter, tclean_totalflux)
                result.set_planeid_array(iter, tclean_planeid)

            LOG.info('tclean used %d iterations' % tclean_iterdone)

            if tclean_stopcode == 0 and tclean_iterdone > 0:
                LOG.warning('tclean exit status 0 for Field: %s SPW: %s: the image may not be cleaned as expected.' %
                            (inputs.field, inputs.spw))

            if tclean_stopcode == 1:
                result.error = CleanBaseError('tclean reached niter limit. Field: %s SPW: %s' %
                                              (inputs.field, inputs.spw), 'Reached niter limit')
                LOG.log(
                    logging.INFO if tclean_stopcode in tclean_stopcode_ignore else logging.WARNING,
                    'tclean reached niter limit of {} for {} / spw{} / iter{} !'.format(
                        tclean_niter, utils.dequote(inputs.field),
                        inputs.spw, iter))

            if tclean_stopcode in [5, 6]:
                result.error = CleanBaseError('tclean stopped to prevent divergence (stop code %d). Field: %s SPW: %s' %
                                              (tclean_stopcode, inputs.field, inputs.spw),
                                              'tclean stopped to prevent divergence.')
                LOG.log(logging.INFO if tclean_stopcode in tclean_stopcode_ignore else logging.WARNING,
                        'tclean stopped to prevent divergence (stop code {}). Field: {} SPW: {} iter{} !'.format(
                            tclean_stopcode, inputs.field, inputs.spw, iter))

        # Collect images to be examined and stored in TcleanResult
        im_names = {}

        # Using virtual spw setups for all interferometry pipelines
        virtspw = True

        if iter > 0 or (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            im_names['model'] = model_name
            im_names['image'] = image_name
            im_names['pbcorimage'] = pbcor_image_name
            im_names['alpha'] = alpha_name

        im_names['residual'] = residual_name
        im_names['psf'] = psf_name
        im_names['flux'] = flux_name
        if os.path.exists(inputs.mask):
            im_names['cleanmask'] = inputs.mask
        elif os.path.exists(mask_name):
            im_names['cleanmask'] = mask_name

        result.im_names.update(im_names)

        for im_type, im_name in im_names.items():
            # Set misc info on imaging products
            # - Usually we only need to do this for a single image per image type;
            # - For multiterm calls, we create an image list depending on the image type;
            #   - current, only include .tt0 and .tt1
            name_list = [im_name]
            if result.multiterm:
                if im_type == 'pbcorimage' and im_name.find('.image.pbcor') != -1:
                    name_list = [im_name.replace('.image.pbcor', '.image.'+mterm+'.pbcor')
                                 for mterm in ['tt0', 'tt1']]
                if im_type == 'alpha':
                    name_list = [im_name, im_name+'.error']
                if im_type in ['model', 'image', 'residual', 'psf', 'flux']:
                    name_list = ['{}.{}'.format(im_name, mterm) for mterm in ['tt0', 'tt1']]
            for name in name_list:
                if os.path.exists(name):
                    imageheader.set_miscinfo(name=name, spw=inputs.spw, virtspw=virtspw, field=inputs.field,
                                             datatype=inputs.datatype, type=im_type, iter=iter,
                                             intent=inputs.intent, specmode=inputs.orig_specmode,
                                             robust=inputs.robust, weighting=inputs.weighting,
                                             is_per_eb=inputs.is_per_eb,
                                             context=context)
            # Store in TcleanResult
            if im_type == 'model':
                result.set_model(iter=iter, image=im_name)
            if im_type == 'pbcorimage':
                if os.path.exists(name_list[0]):
                    result.set_image(iter=iter, image=im_names['pbcorimage'])
                else:
                    result.set_image(iter=iter, image=im_names['image'])
            if im_type == 'residual' and os.path.exists(name_list[0]):
                result.set_residual(iter=iter, image=im_name)
            if im_type == 'psf':
                result.set_psf(image=im_name)
            if im_type == 'flux':
                result.set_flux(image=im_name)
            if im_type == 'cleanmask':
                result.set_cleanmask(iter=iter, image=im_name)

        # Keep threshold and sensitivity for QA and weblog
        result.set_threshold(inputs.threshold)
        result.set_sensitivity(inputs.sensitivity)
        result.set_imaging_params(iter, tclean_job_parameters)

        # This operation is used as a workaround for CAS-13401 and can be removed after the CAS ticket is resolved.
        if tclean_job_parameters['stokes'] != 'I' and inputs.heuristics.imaging_mode != 'ALMA':
            self._copy_restoringbeam_from_psf(tclean_job_parameters['imagename'])

        return result

    def _copy_restoringbeam_from_psf(self, imagename):
        """Copy the per-plane beam set from .psf image to .image/.residual.

        Note: this is a workaround for CAS-13401, in which CASA/tclean(stokes='IQUV') doesn't save
              the per-plane restoring beam information into the residual and restored images.
              For CASA ver>=6.6.0 (after the CAS-13401 implementation), this block should act as a no-op.
        """
        bm_src = '.psf'
        bm_src_ext_try = ['', '.tt0']
        bm_dst = ['.image', '.residual', '.image.pbcor']
        bm_dst_ext = ['', '.tt0', '.tt1', '.tt2']

        is_src_present = False
        for bm_src_ext in bm_src_ext_try:
            if os.path.exists(imagename+bm_src+bm_src_ext):
                is_src_present = True
                break

        if not is_src_present:
            LOG.error('The restoring beam information source image is not found.')
        else:
            LOG.info(
                f'Try to copy the restoring beam set from {imagename+bm_src+bm_src_ext} to the corresponding image products')
        with casa_tools.ImageReader(imagename+bm_src+bm_src_ext) as bm_src_im:
            src_shape = bm_src_im.shape()
            LOG.info('The restoring beam information source image ')
            for bm_dst0 in bm_dst:
                for bm_ext0 in bm_dst_ext:
                    if os.path.exists(imagename+bm_dst0+bm_ext0):
                        with casa_tools.ImageReader(imagename+bm_dst0+bm_ext0) as bm_dst_im:
                            if (bm_dst_im.shape() == src_shape).all() and not bm_dst_im.restoringbeam():
                                # PIPE-2061: we only copy the beam info if the target and source images have the same shape
                                # and the target image doesn't have a beam.
                                LOG.info(f'Copy the per-plane beam set to {imagename+bm_dst0+bm_ext0}')
                                for idx_c in range(src_shape[3]):
                                    for idx_p in range(src_shape[2]):
                                        LOG.debug(f'working on idx_chan={idx_c}, idx_pol={idx_p}')
                                        bm = bm_src_im.restoringbeam(channel=idx_c, polarization=idx_p)
                                        bm_dst_im.setrestoringbeam(beam=bm, channel=idx_c, polarization=idx_p)
                            else:
                                LOG.info(
                                    'The restoring beam copying source and target images have different shapes or the target '
                                    'image already has a beam. We will skip copying the restoring beam')


def rename_image(old_name, new_name, extensions=['']):
    """
    Rename an image
    """
    if old_name is not None:
        for extension in extensions:
            with casa_tools.ImageReader('%s%s' % (old_name, extension)) as image:
                image.rename(name=new_name, overwrite=True)


class CleanBaseError(object):
    """Clean Base Error Class to transfer detailed messages for weblog
    reporting.
    """

    def __init__(self, longmsg='', shortmsg=''):
        self.longmsg = longmsg
        self.shortmsg = shortmsg
