"""A pipeline task to add to a list of images to be made by hif_makeimages()

The hif_editimlist() task typically uses a parameter file as input.  Depending
on the use case, there will usually be a minimal set of input parameters
defined in this file.  Each set of image parameters gets stored in the global
context in the clean_list_pending attribute.

Example:
    A common case is providing a list of VLASS image parameters via a file::

        CASA <1>: hif_editimlist(parameter_file='vlass_QLIP_parameters.list')

    The ``vlass_QLIP_parameters.list`` file might contain something like the
    following::

        phasecenter='J2000 12:16:04.600 +059.24.50.300'
        imagename='QLIP_image'

    An equivalent way to invoke the above example would be::

        CASA <2>: hif_editimlist(phasecenter='J2000 12:16:04.600 +059.24.50.300',
                                 imagename='QLIP_image')

Any imaging parameters that are not specified when hif_editimlist() is called,
either as a task parameter or via a parameter file, will have a default value
or heuristic applied.

Todo:
    * In the future this task will be modified to allow editing the parameters
    of an existing context.clean_list_pending entry.

"""
import ast
import copy
import os

import numpy as np

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import imageparams_factory
from pipeline.hif.tasks.makeimlist.cleantarget import CleanTarget
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure.utils import utils

from .resultobjects import EditimlistResult

LOG = infrastructure.get_logger(__name__)


class EditimlistInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_LINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    search_radius_arcsec = vdp.VisDependentProperty(default=1000.0)
    conjbeams = vdp.VisDependentProperty(default=False)
    cfcache = vdp.VisDependentProperty(default='')
    cfcache_nowb = vdp.VisDependentProperty(default='')
    cyclefactor = vdp.VisDependentProperty(default=-999.)
    cycleniter = vdp.VisDependentProperty(default=-999)
    nmajor = vdp.VisDependentProperty(default=None)
    datatype = vdp.VisDependentProperty(default='')
    datacolumn = vdp.VisDependentProperty(default='')
    deconvolver = vdp.VisDependentProperty(default='')
    editmode = vdp.VisDependentProperty(default='')
    imaging_mode = vdp.VisDependentProperty(default='')
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='')
    gridder = vdp.VisDependentProperty(default='')
    mask = vdp.VisDependentProperty(default=None)
    pbmask = vdp.VisDependentProperty(default=None)
    nchan = vdp.VisDependentProperty(default=-1)
    niter = vdp.VisDependentProperty(default=0)
    nterms = vdp.VisDependentProperty(default=0)
    parameter_file = vdp.VisDependentProperty(default='')
    pblimit = vdp.VisDependentProperty(default=-999.)
    phasecenter = vdp.VisDependentProperty(default='')
    reffreq = vdp.VisDependentProperty(default='')
    restfreq = vdp.VisDependentProperty(default='')
    robust = vdp.VisDependentProperty(default=-999.)
    scales = vdp.VisDependentProperty(default='')
    specmode = vdp.VisDependentProperty(default='')
    start = vdp.VisDependentProperty(default='')
    stokes = vdp.VisDependentProperty(default='')
    threshold = vdp.VisDependentProperty(default='')
    nsigma = vdp.VisDependentProperty(default=-999.)
    uvtaper = vdp.VisDependentProperty(default='')
    uvrange = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default='')
    sensitivity = vdp.VisDependentProperty(default=0.0)
    # VLASS-SE-CONT specific option: if True then perform final clean iteration without mask for selfcal image
    clean_no_mask_selfcal_image = vdp.VisDependentProperty(default=False)
    # VLASS-SE-CONT specific option: user settable cycleniter in cleaning without mask in final imaging stage
    cycleniter_final_image_nomask = vdp.VisDependentProperty(default=None)
    # VLASS-SE-CUBE plane rejection parameters
    vlass_plane_reject_ms = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def cell(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        if 'hm_cell' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['hm_cell']
        return []

    @cell.convert
    def cell(self, val):
        if isinstance(val, str):
            val = [val]
        for item in val:
            if isinstance(item, str):
                if 'ppb' in item:
                    return item
        return val

    @vdp.VisDependentProperty
    def imsize(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        if 'hm_imsize' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['hm_imsize']
        return []

    @imsize.convert
    def imsize(self, val):
        if not isinstance(val, list):
            val = [val]

        for item in val:
            if isinstance(item, str):
                if 'pb' in item:
                    return item
        return val

    @vdp.VisDependentProperty
    def field(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @field.convert
    def field(self, val):
        if not isinstance(val, (str, list, type(None))):
            # PIPE-1881: allow field names that mistakenly get casted into non-string datatype by
            # recipereducer (utils.string_to_val) and executeppr (XmlObjectifier.castType)
            LOG.warning('The field selection input %r is not a string and will be converted.', val)
            val = str(val)
        if not isinstance(val, list):
            val = [val]
        return val

    @vlass_plane_reject_ms.postprocess
    def vlass_plane_reject_ms(self, unprocessed):
        """Convert the allowed argument input datatype to the dictionary form used by the task."""
        vlass_plane_reject_dict = {
            'apply': True, 'exclude_spw': '', 'flagpct_thresh': 0.9, 'nfield_thresh': 12}
        if isinstance(unprocessed, dict):
            vlass_plane_reject_dict.update(unprocessed)
        if isinstance(unprocessed, bool):
            vlass_plane_reject_dict['apply'] = unprocessed
        LOG.debug("convert the task input of vlass_plane_reject_ms from %r to %r.",
                  unprocessed, vlass_plane_reject_dict)
        return vlass_plane_reject_dict

    @vdp.VisDependentProperty
    def nbin(self):
        if 'nbins' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['nbins']
        return -1

    @vdp.VisDependentProperty
    def spw(self):
        return ''

    @spw.convert
    def spw(self, val):
        # Use str() method to catch single spwid case via PPR which maps to int.
        return str(val)

    def __init__(self, context, output_dir=None, vis=None,
                 search_radius_arcsec=None, cell=None, cfcache=None, conjbeams=None,
                 cyclefactor=None, cycleniter=None, nmajor=None, datatype=None, datacolumn=None, deconvolver=None,
                 editmode=None, field=None, imaging_mode=None,
                 imagename=None, imsize=None, intent=None, gridder=None,
                 mask=None, pbmask=None, nbin=None, nchan=None, niter=None, nterms=None,
                 parameter_file=None, pblimit=None, phasecenter=None, reffreq=None, restfreq=None,
                 robust=None, scales=None, specmode=None, spw=None,
                 start=None, stokes=None, threshold=None, nsigma=None,
                 uvtaper=None, uvrange=None, width=None, sensitivity=None, clean_no_mask_selfcal_image=None,
                 vlass_plane_reject_ms=None,
                 cycleniter_final_image_nomask=None):

        super(EditimlistInputs, self).__init__()
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.search_radius_arcsec = search_radius_arcsec
        self.cell = cell
        self.cfcache = cfcache
        self.conjbeams = conjbeams
        self.cyclefactor = cyclefactor
        self.cycleniter = cycleniter
        self.nmajor = nmajor
        self.datatype = datatype
        self.datacolumn = datacolumn
        self.deconvolver = deconvolver
        self.editmode = editmode
        self.field = field
        self.imaging_mode = imaging_mode
        self.imagename = imagename
        self.imsize = imsize
        self.intent = intent
        self.gridder = gridder
        self.mask = mask
        self.pbmask = pbmask
        self.nbin = nbin
        self.nchan = nchan
        self.niter = niter
        self.nterms = nterms
        self.parameter_file = parameter_file
        self.pblimit = pblimit
        self.phasecenter = phasecenter
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.robust = robust
        self.scales = scales
        self.specmode = specmode
        self.spw = spw
        self.start = start
        self.stokes = stokes
        self.threshold = threshold
        self.nsigma = nsigma
        self.uvtaper = uvtaper
        self.uvrange = uvrange
        self.width = width
        self.sensitivity = sensitivity
        self.clean_no_mask_selfcal_image = clean_no_mask_selfcal_image
        self.cycleniter_final_image_nomask = cycleniter_final_image_nomask
        self.vlass_plane_reject_ms = vlass_plane_reject_ms

# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(EditimlistInputs)


@task_registry.set_equivalent_casa_task('hif_editimlist')
class Editimlist(basetask.StandardTaskTemplate):
    # 'Inputs' will be used later in execute_task().
    #   See h/cli/utils.py and infrastructure/argmagger.py
    Inputs = EditimlistInputs

    # TODO:  check to see if I should set this to False
    is_multi_vis_task = True

    def prepare(self):

        inp = self.inputs

        # get the class inputs as a dictionary
        inpdict = inp.as_dict()
        LOG.debug(inp.as_dict())

        # if a file is given, read whatever parameters are defined in the file.
        # note: inputs from the parameter file take precedence over individual task arguements.
        if inp.parameter_file:
            if os.access(inp.parameter_file, os.R_OK):
                with open(inp.parameter_file) as parfile:
                    for line in parfile:
                        # ignore comment lines or lines that don't contain '='
                        if line.startswith('#') or '=' not in line:
                            continue
                        # split key=value into a key, value components
                        parameter, value = line.partition('=')[::2]
                        # strip whitespace
                        parameter = parameter.strip()
                        value = value.strip()
                        # all params come in as strings.  evaluate it to set it to the proper type
                        value = ast.literal_eval(value)

                        # use this information to change the values in inputs
                        LOG.debug("Setting inputdict['{k}'] to {v} {t}".format(k=parameter, v=value, t=type(value)))
                        inpdict[parameter] = value
            else:
                LOG.error('Input parameter file is not readable: {fname}'.format(fname=inp.parameter_file))

        # now construct the list of imaging command parameter lists that must
        # be run to obtain the required images
        result = EditimlistResult()

        # will default to adding a new image list entry
        inpdict.setdefault('editmode', 'add')

        # Use the ms object from the context to change field ids to fieldnames, if needed
        # TODO think about how to handle multiple MSs
        ms = inp.context.observing_run.get_ms(inp.vis[-1])
        fieldnames = []

        if inpdict['field']:
            # assume field entries are either all integers or all strings, but not a mix
            if isinstance(inpdict['field'][0], int):
                fieldobj = ms.get_fields(field_id=inpdict['field'][0])
                for fieldname in fieldobj.name:
                    fieldnames.append(fieldname)
            else:
                for fieldname in inpdict['field']:
                    fieldnames.append(fieldname)

            if len(fieldnames) > 1:
                fieldnames = [','.join(fieldnames)]
        # fieldnames is now a list of fieldnames: ['fieldA', 'fieldB', ...]
        # add quotes to any fieldnames with disallowed characters
        fieldnames = [utils.fieldname_for_casa(fn) for fn in fieldnames]

        imlist_entry = CleanTarget()  # initialize a target structure for clean_list_pending

        img_mode = 'VLASS-QL' if not inpdict['imaging_mode'] else inpdict['imaging_mode']
        result.img_mode = img_mode
        result.editmode = inpdict['editmode'].lower()

        # The default spw range for VLASS is 2~17. hif_makeimages() needs a csv list.
        # We set the imlist_entry spw before the heuristics object because the heursitics class
        # uses it in initialization.
        if img_mode in ('VLASS-QL', 'VLASS-SE-CONT', 'VLASS-SE-CONT-AWP-P001', 'VLASS-SE-CONT-AWP-P032',
                        'VLASS-SE-CONT-MOSAIC', 'VLASS-SE-CUBE', 'VLASS-SE-TAPER'):
            if not inpdict['spw']:
                imlist_entry['spw'] = ','.join([str(x) for x in range(2, 18)])
                if img_mode.startswith('VLASS-SE-CUBE'):
                    imlist_entry['spw'] = [str(x) for x in range(2, 18)]
            else:
                if 'MHz' in inpdict['spw']:
                    # map the center frequencies (MHz) to spw ids
                    cfreq_spw = {}
                    spws = ms.get_spectral_windows(science_windows_only=True)
                    for spw_ii in spws:
                        centre_freq = int(spw_ii.centre_frequency.to_units(measures.FrequencyUnits.MEGAHERTZ))
                        spwid = spw_ii.id
                        cfreq_spw[centre_freq] = spwid

                    user_freqs = inpdict['spw'].split(',')
                    spws = []
                    for uf in user_freqs:
                        uf_int = int(uf.replace('MHz', ''))
                        spws.append(cfreq_spw[uf_int])
                    imlist_entry['spw'] = ','.join([str(x) for x in spws])
                else:
                    imlist_entry['spw'] = inpdict['spw']
        else:
            if inpdict['spw'].replace(',', '').replace(' ', '').isdigit():  # with spaces and commas removed
                imlist_entry['spw'] = inpdict['spw']
            else:
                # if these are spw names, translate them to spw ids
                spws = ms.get_spectral_windows(science_windows_only=True)
                tmpspw_str = inpdict['spw']
                for spw_ii in spws:
                    if spw_ii.name in inpdict['spw']:
                        LOG.info('Using spwd id {id} for spw name {name}'.format(id=spw_ii.id, name=spw_ii.name))
                        tmpspw_str = tmpspw_str.replace(spw_ii.name, str(spw_ii.id))
                for spw_jj in inpdict['spw'].replace(' ', '').split(','):
                    if spw_jj in tmpspw_str:  # if spwname hasn't been replaced with an id, then warn
                        LOG.warning('spw name \'{name}\' was not found in {ms}'.format(name=spw_jj, ms=inp.vis[-1]))
                imlist_entry['spw'] = tmpspw_str

        # phasecenter is required user input (not determined by heuristics)
        imlist_entry['phasecenter'] = inpdict['phasecenter']

        iph = imageparams_factory.ImageParamsHeuristicsFactory()

        # note: heuristics.imageparams_base expects 'spw' to be a selection string.
        # For VLASS-SE-CUBE, 'spw' is the representational string of spw group list, e.g. spw="['1,2','3,4,5']"
        th = imlist_entry['heuristics'] = iph.getHeuristics(vislist=inp.vis, spw=str(imlist_entry['spw']),
                                                            observing_run=inp.context.observing_run,
                                                            imagename_prefix=inp.context.project_structure.ousstatus_entity_id,
                                                            proj_params=inp.context.project_performance_parameters,
                                                            imaging_params=inp.context.imaging_parameters,
                                                            imaging_mode=img_mode)

        # Determine current VLASS-SE-CONT imaging stage (used in heuristics to make decisions)
        # Intended to cover VLASS-SE-CONT, VLASS-SE-CONT-AWP-P001, VLASS-SE-CONT-AWP-P032 modes as of 01.03.2021
        if img_mode.startswith('VLASS-SE-CONT'):
            # If 0 hif_makeimlist results are found, then we are in stage 1
            th.vlass_stage = utils.get_task_result_count(inp.context, 'hif_makeimages') + 1
            # Below method only exists for ImageParamsHeuristicsVlassSeCont and ImageParamsHeuristicsVlassSeContAWPP001
            th.set_user_cycleniter_final_image_nomask(inpdict['cycleniter_final_image_nomask'])

        # For VLASS-SE-CUBE, we only run hif_makeimages once and reuse most imaging heuristics
        # from SE-CONT-MOSAIC/vlass_stage=3. Therefore, ImageParamsHeuristicsVlassSeCube is constructed
        # as a subclass of ImageParamsHeuristicsVlassSeContMosaic with vlass_stage=3 at its initialization.
        # vlass_stage=3 stays once the workflow starts to create the imaging target list.
        if img_mode.startswith('VLASS-SE-CUBE'):
            th.set_user_cycleniter_final_image_nomask(inpdict['cycleniter_final_image_nomask'])
            # the below statement is redundant and only serves as a reminder that vlass_stage=3 for all VLASS-SE-CUBE heuristics.
            th.vlass_stage = 3

        imlist_entry['threshold'] = inpdict['threshold']
        imlist_entry['hm_nsigma'] = None if inpdict['nsigma'] in (None, -999.0) else float(inpdict['nsigma'])

        if imlist_entry['threshold'] and imlist_entry['hm_nsigma']:
            LOG.warning("Both 'threshold' and 'nsigma' were specified.")

        imlist_entry['pblimit'] = None if inpdict['pblimit'] in (None, -999.0) else inpdict['pblimit']
        imlist_entry['stokes'] = th.stokes() if not inpdict['stokes'] else inpdict['stokes']
        imlist_entry['conjbeams'] = th.conjbeams() if not inpdict['conjbeams'] else inpdict['conjbeams']
        imlist_entry['reffreq'] = th.reffreq() if not inpdict['reffreq'] else inpdict['reffreq']
        
        # niter_correction is run again in tclean.py
        imlist_entry['niter'] = th.niter() if not inpdict['niter'] else inpdict['niter']
        imlist_entry['cyclefactor'] = inpdict['cyclefactor']
        imlist_entry['cycleniter'] = inpdict['cycleniter']
        imlist_entry['nmajor'] = inpdict['nmajor']
        imlist_entry['cfcache'], imlist_entry['cfcache_nowb'] = th.get_cfcaches(inpdict['cfcache'])
        imlist_entry['scales'] = th.scales() if not inpdict['scales'] else inpdict['scales']
        imlist_entry['uvtaper'] = (th.uvtaper() if not 'uvtaper' in inp.context.imaging_parameters
                                   else inp.context.imaging_parameters['uvtaper']) if not inpdict['uvtaper'] else inpdict['uvtaper']

        imlist_entry['deconvolver'] = th.deconvolver(None, None) if not inpdict['deconvolver'] else inpdict['deconvolver']
        imlist_entry['mask'] = th.mask() if not inpdict['mask'] else inpdict['mask']
        imlist_entry['pbmask'] = None if not inpdict['pbmask'] else inpdict['pbmask']
        imlist_entry['specmode'] = th.specmode() if not inpdict['specmode'] else inpdict['specmode']
        imlist_entry['robust'] = th.robust(specmode=imlist_entry['specmode']) if inpdict['robust'] in (None, -999.0) else inpdict['robust']

        imlist_entry['uvrange'], _ = th.uvrange(field=fieldnames[0] if fieldnames else None,
                                                spwspec=imlist_entry['spw'],
                                                specmode=imlist_entry['specmode']) if not inpdict['uvrange'] else inpdict['uvrange']

        LOG.info('RADIUS')
        LOG.info(repr(inpdict['search_radius_arcsec']))
        LOG.info('default={d}'.format(d=not inpdict['search_radius_arcsec']
                                      and not isinstance(inpdict['search_radius_arcsec'], float)
                                      and not isinstance(inpdict['search_radius_arcsec'], int)))
        buffer_arcsec = th.buffer_radius() \
            if (not inpdict['search_radius_arcsec']
                and not isinstance(inpdict['search_radius_arcsec'], float)
                and not isinstance(inpdict['search_radius_arcsec'], int)) else inpdict['search_radius_arcsec']
        LOG.info("{k} = {v}".format(k='search_radius', v=buffer_arcsec))
        result.capture_buffer_size(buffer_arcsec)
        imlist_entry['intent'] = th.intent() if not inpdict['intent'] else inpdict['intent']

        # imlist_entry['datacolumn'] is either None or an non-empty string here based on the current heuristics implementation.
        imlist_entry['datacolumn'] = th.datacolumn() if not inpdict['datacolumn'] else inpdict['datacolumn']
        imlist_entry['nterms'] = th.nterms(imlist_entry['spw']) if not inpdict['nterms'] else inpdict['nterms']

        # Specify the sensitivity value to be used in TcleanInputs
        if 'VLASS' in img_mode:
            # Force sensitivity = 0.0 to disable sensitivity calculation inside hif_tclean()
            imlist_entry['sensitivity'] = 0.0
        else:
            # Assign user input if not None or 0.0; otherwise, set to None for in-flight calculation by hif_tclean()
            imlist_entry['sensitivity'] = inpdict['sensitivity'] if inpdict['sensitivity'] else None

        # ---------------------------------------------------------------------------------- set cell (SRDP ALMA)
        ppb = 5.0  # pixels per beam
        if fieldnames:
            synthesized_beam, _ = th.synthesized_beam(field_intent_list=[[fieldnames[0], 'TARGET']],
                                                        spwspec=imlist_entry['spw'],
                                                        robust=imlist_entry['robust'],
                                                        uvtaper=imlist_entry['uvtaper'],
                                                        pixperbeam=ppb,
                                                        known_beams=inp.context.synthesized_beams,
                                                        force_calc=False, shift=True)
        else:
            synthesized_beam = None

        # inpdict['cell'] can have input of the form ['0.5arcsec', '0.5arcsec'] or '3ppb'
        # It is only a string if the pixel-per-beam value is provided.
        # With pixel-per-beam input, the cell size needs to be calculated using
        # th.cell(), so set inpdict['cell'] to the empty list here to trigger this calculation.
        if inpdict['cell'] and isinstance(inpdict['cell'], str):
            ppb = float(inpdict['cell'].split('ppb')[0])
            inpdict['cell'] = []

        imlist_entry['cell'] = th.cell(beam=synthesized_beam,
                                       pixperbeam=ppb) if not inpdict['cell'] else inpdict['cell']
        # ----------------------------------------------------------------------------------  set imsize (SRDP ALMA)
        largest_primary_beam = th.largest_primary_beam_size(spwspec=imlist_entry['spw'], intent='TARGET')
        fieldids = th.field('TARGET', fieldnames)

        # Fail if there is no field to image. This could occur if a field is requested that is not in the MS.
        # th.field will return [''] if no fields were found in the MS that match any input fields and intents
        # PIPE-2189: fieldnames is an empty list in the case of VLASS imaging.
        if fieldnames and len(fieldids) == 1 and fieldids[0] == '':
            msg = "Field(s): {} not present in MS: {}".format(','.join(fieldnames), ms.name)
            LOG.error(msg)

        if isinstance(inpdict['imsize'], str):
            sfpblimit = float(inpdict['imsize'].split('pb')[0])
            inpdict['imsize'] = []
        else:
            sfpblimit = 0.2

        if inpdict['imsize']:
            # PIPE-2189: take the manually specfied imsize; commonly used by VLASS.
            imlist_entry['imsize'] = inpdict['imsize']
        else:
            if img_mode == 'VLA' and imlist_entry['specmode'] == 'cont':
                # PIPE-675: VLA imsize heuristic update; band dependent FOV in 'cont' specmode.
                imlist_entry['imsize'] = th.imsize(fields=fieldids, cell=imlist_entry['cell'],
                                                   primary_beam=largest_primary_beam, spwspec=imlist_entry['spw'],
                                                   intent=imlist_entry['intent'], specmode=imlist_entry['specmode'])
            else:
                imlist_entry['imsize'] = th.imsize(fields=fieldids, cell=imlist_entry['cell'],
                                                   primary_beam=largest_primary_beam,
                                                   sfpblimit=sfpblimit, intent=imlist_entry['intent'],
                                                   specmode=imlist_entry['specmode'])

        imlist_entry['nchan'] = inpdict['nchan']
        imlist_entry['nbin'] = inpdict['nbin']
        imlist_entry['start'] = inpdict['start']
        imlist_entry['width'] = inpdict['width']

        imlist_entry['restfreq'] = th.restfreq(
            specmode=imlist_entry['specmode'],
            nchan=imlist_entry['nchan'],
            start=imlist_entry['start'],
            width=imlist_entry['width']) if not inpdict['restfreq'] else inpdict['restfreq']

        # for VLASS phasecenter is required user input (not determined by heuristics)
        if inpdict['phasecenter']:
            imlist_entry['phasecenter'] = inpdict['phasecenter']
            imlist_entry['psf_phasecenter'] = inpdict['phasecenter']
        else:
            phasecenter, psf_phasecenter = th.phasecenter(fieldids)
            imlist_entry['phasecenter'] = phasecenter
            imlist_entry['psf_phasecenter'] = psf_phasecenter

        # set the field name list in the image list target
        if fieldnames:
            imlist_entry['field'] = fieldnames[0]
        else:
            if imlist_entry['phasecenter'] not in ['', None]:
                # TODO: remove the dependency on cell size being in arcsec

                # remove brackets and begin/end string characters
                # if cell is a list, get the first string element
                if isinstance(imlist_entry['cell'], type([])):
                    imlist_entry['cell'] = imlist_entry['cell'][0]
                imlist_entry['cell'] = imlist_entry['cell'].strip('[').strip(']')
                imlist_entry['cell'] = imlist_entry['cell'].replace("'", '')
                imlist_entry['cell'] = imlist_entry['cell'].replace('"', '')
                # We always search for fields in 1sq degree with a surrounding buffer
                mosaic_side_arcsec = 3600  # 1 degree
                dist = (mosaic_side_arcsec / 2.) + float(buffer_arcsec)
                dist_arcsec = str(dist) + 'arcsec'
                LOG.info("{k} = {v}".format(k='dist_arcsec', v=dist_arcsec))

                # PIPE-1948/PIPE-2004: we updated the field select algorithm for VLASS-PL2023.
                # * restrict field intents to 'TARGET'
                # * restrict field names to beginning with '0', '1', '2', or 'T' (e.g. the NCP field 'T32t02.NCP')
                #   note: we also consider rge double quote possibility because field names might be protected in
                #   the name strings of Field domain objects.
                # * use spherical sky offsets to select fields based on positions.
                found_fields = th.select_fields(offsets=dist_arcsec,
                                                intent='TARGET',
                                                phasecenter=imlist_entry['phasecenter'],
                                                name='0*,"0*,1*,"1*,2*,"2*,T*,"T*')
                # for the existing VLASS workflow, only one MS is used, though this might change in the future.
                found_fields = found_fields[0]

                if found_fields:
                    imlist_entry['field'] = ','.join(str(x) for x in found_fields)  # field ids, not names

        if not imlist_entry['spw']:   # could be None or an empty string
            LOG.warning('spw is not specified')   # probably should raise an error rather than warning? - will likely fail later anyway
            imlist_entry['spw'] = None

        if not imlist_entry['field']:
            LOG.warning('field is not specified')   # again, should probably raise an error, as it will eventually fail anyway
            imlist_entry['field'] = None

        # validate specmode
        if imlist_entry['specmode'] not in ('mfs', 'cont', 'cube', 'repBW'):
            msg = 'specmode must be one of "mfs", "cont", "cube", or "repBW"'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        if not img_mode.startswith('VLASS'):
            # this block is only executed for non-VLASS data
            if imlist_entry['datacolumn'] in (None, ''):
                # if datacolumn is not specified by the user input or heuristics, we need to determine the datacolumn
                # from the list of datatypes to consider in the order of preference.
                specmode_datatypes = DataType.get_specmode_datatypes(imlist_entry['intent'], imlist_entry['specmode'])
                # PIPE-1798: filter the list to only include the one(s) starting with the user supplied restriction str, i.e., inputs.datatype.
                if isinstance(inpdict['datatype'], str):
                    specmode_datatypes = [dt for dt in specmode_datatypes if dt.name.startswith(inpdict['datatype'].upper())]
                # loop over the datatype candidate list find the first one that appears in the given (source,spw) combinations.
                for dtype in specmode_datatypes:
                    datacolumn_name = ms.get_data_column(dtype, imlist_entry['field'], imlist_entry['spw'])
                    if datacolumn_name in ('DATA', 'CORRECTED_DATA'):
                        imlist_entry['datatype'] = imlist_entry['datatype_info'] = dtype.name
                        if datacolumn_name == 'DATA':
                            imlist_entry['datacolumn'] = 'data'
                        if datacolumn_name == 'CORRECTED_DATA':
                            imlist_entry['datacolumn'] = 'corrected'
                        break
                    else:
                        LOG.debug(f'No valid datacolumn is associated with the  data selection: '
                                  f"datatype={dtype!r}, field={imlist_entry['field']!r}, spw={imlist_entry['spw']!r}")
                specmode_datatypes_str = ', '.join([dt.name for dt in specmode_datatypes])
                if imlist_entry['datatype'] is None:
                    LOG.warning(
                        f"No data from field={imlist_entry['field']!r} / spw={imlist_entry['spw']!r} is "
                        f'in the allowed datatype(s): {specmode_datatypes_str}.'
                        ' No clean target will be added.')
                    return result
            else:
                # if datacolumn is specified, pick it and label cleantarget with the corresponding datatype (when available).
                ms_datacolumn = imlist_entry['datacolumn'].upper()
                if ms_datacolumn == 'CORRECTED':
                    ms_datacolumn = 'CORRECTED_DATA'
                dtype = ms.get_data_type(ms_datacolumn, imlist_entry['field'], imlist_entry['spw'])
                LOG.warning(
                    f'datacolumn={imlist_entry["datacolumn"]!r} is selected based on user or heuristic input, which overrides the datatype-based selection.')
                if dtype is None:
                    LOG.warning(f'No valid datatype is associated with the data selection: '
                                f"datacolumn={imlist_entry['datacolumn']!r}, field={imlist_entry['field']!r}, spw={imlist_entry['spw']!r}")
                else:
                    imlist_entry['datatype'] = imlist_entry['datatype_info'] = dtype.name

        # PIPE-1710/PIPE-1474: append a corresponding suffix to the image file name according to the datatype of selected visibilities.
        datatype_suffix = None
        if isinstance(imlist_entry['datatype'], str):
            if imlist_entry['datatype'].lower().startswith('selfcal'):
                datatype_suffix = 'selfcal'
            if imlist_entry['datatype'].lower().startswith('regcal'):
                datatype_suffix = 'regcal'

        imlist_entry['gridder'] = th.gridder(imlist_entry['intent'], imlist_entry['field']
                                             ) if not inpdict['gridder'] else inpdict['gridder']
        imlist_entry['imagename'] = th.imagename(intent=imlist_entry['intent'], field=imlist_entry['field'],
                                                 spwspec=imlist_entry['spw'], specmode=imlist_entry['specmode'],
                                                 band=None, datatype=datatype_suffix) if not inpdict['imagename'] else inpdict['imagename']

        # In this case field and spwspec is not needed in the filename, furthermore, imaging is done in multiple stages
        # prepend the STAGENUMBER string in order to differentiate them. In TcleanInputs class this is replaced by the
        # actual stage number string.
        # Intended to cover VLASS-SE-CONT, VLASS-SE-CONT-AWP-P001, VLASS-SE-CONT-AWP-P032,
        # VLASS-SE-CONT-MOSAIC, and VLASS-SE-CUBE as of 05/03/2022
        if img_mode.startswith('VLASS-SE-CONT') or img_mode.startswith('VLASS-SE-CUBE'):
            imagename = th.imagename(intent=imlist_entry['intent'], field=None, spwspec=None,
                                     specmode=imlist_entry['specmode'],
                                     band=None) if not inpdict['imagename'] else inpdict['imagename']
            imlist_entry['imagename'] = 's{}.{}'.format('STAGENUMBER', imagename)
            # Try to obtain previously computed mask name
            imlist_entry['mask'] = th.mask(results_list=inp.context.results,
                                           clean_no_mask=inpdict['clean_no_mask_selfcal_image']) if not inpdict['mask'] \
                else inpdict['mask']

        for key, value in imlist_entry.items():
            LOG.info("%s = %r", key, value)

        try:
            if imlist_entry['field']:
                if result.img_mode == 'VLASS-SE-CUBE':
                    result = self._add_vlasscube_targets(result, imlist_entry)
                else:
                    result.add_target(imlist_entry)
            else:
                raise TypeError
        except TypeError:
            LOG.error('No fields to image.')

        # check for required user inputs
        if not imlist_entry['imagename']:
            LOG.error('No imagename provided.')

        if not imlist_entry['phasecenter']:
            LOG.error('No phasecenter provided.')

        return result

    def analyse(self, result):
        return result

    def _add_vlasscube_targets(self, result, imlist_entry):
        """Add multiple clean targets for the VLASS-SE-CUBE mode.
        
        For the "coarse cube" mode, we perform the following operations:
            - loop over individual spw groups
            - generate corresponding clean target using a modified copy of the base CleanTarget object template
            - aggregate clean targets list after the VLASS-SE-CUBE plane rejection criteria is applied.
        note: the initial 'spw' from the base CleanTarget object template, i.e., imlist_entry['spw'], is expected to be a list here.
        For VLASS-SE-CUBE, we add additional attributes so the template can render the target-specific parameters properly.        
        """

        vlass_plane_reject_keys_allowed = [
            'apply', 'exclude_spw', 'flagpct_thresh', 'nfield_thresh']

        for k in self.inputs.vlass_plane_reject_ms:
            if k not in vlass_plane_reject_keys_allowed:
                LOG.warning(
                    "The key %r in the 'vlass_plane_reject_ms' task input dictionary is not expected and will be ignored.", k)

        result.targets_reffreq = []
        result.targets_spw = []
        result.targets_imagename = []
        th = imlist_entry['heuristics']

        vlass_flag_stats = self._vlass_plane_rejection(imlist_entry)
        result.vlass_flag_stats = vlass_flag_stats

        spwgroup_reject_list = []

        for idx, spw in enumerate(imlist_entry['spw']):

            imlist_entry_per_spwgroup = copy.deepcopy(imlist_entry)
            imlist_entry_per_spwgroup['spw'] = spw
            imlist_entry_per_spwgroup['imagename'] = imlist_entry['imagename'] + \
                '.spw' + spw.replace('~', '-').replace(',', '_')
            imlist_entry_per_spwgroup['reffreq'] = th.meanfreq_spwgroup(spw)
            # flagpct within the 1de^2 box
            imlist_entry_per_spwgroup['flagpct'] = vlass_flag_stats['flagpct_spwgroup'][idx]
            # flagpct over the entire mosaic
            flagpct = th.flagpct_spwgroup(results_list=self.inputs.context.results, spw_selection=spw)
            # PIPE-1800: flagpct_threshold here is the flag percent rejection threshold across the entire mosaic.
            # We hardcode the value to 1.0 which means we reject any spw that is completely flagged.
            # Note that this is different from vlass_plane_reject_ms['flagpct_thresh'] which is a per-field flagging threshold to 
            # define "bad" fields.
            flagpct_threshold = 1.0

            if flagpct is None:
                LOG.warning(
                    'Can not find previous flagging summary for spw=%r, but we will still add it as an imaging target.',
                    spw)
            else:
                if flagpct >= flagpct_threshold:
                    LOG.warning(
                        'VLASS Data for spw=%r is %.2f%% flagged, and we will skip it as an imaging target.', spw,
                        flagpct * 100)
                    continue

            if vlass_flag_stats['spwgroup_reject'][idx]:
                spwgroup_reject_list.append(spw)
                continue

            result.targets_reffreq.append(imlist_entry_per_spwgroup['reffreq'])
            result.targets_spw.append(imlist_entry_per_spwgroup['spw'])
            result.targets_imagename.append(os.path.basename(imlist_entry_per_spwgroup['imagename']))
            result.add_target(imlist_entry_per_spwgroup)

        if spwgroup_reject_list:
            LOG.warning(
                'VLASS Data for spw=%r meets the plane rejection criteria: nfield>=%i with flagpct>=%.2f%%.', ','.join(
                    spwgroup_reject_list),
                self.inputs.vlass_plane_reject_ms['nfield_thresh'],
                self.inputs.vlass_plane_reject_ms['flagpct_thresh'] * 100)

        return result

    def _vlass_plane_rejection(self, imlist_entry):
        """Decide whether to reject a spw group based on the number of high-flagging-percentage fields."""

        vis_name = self.inputs.vis[-1]

        msobj = self.inputs.context.observing_run.get_ms(vis_name)
        job = casa_tasks.flagdata(vis=vis_name, mode='summary', fieldcnt=True)
        flag_stats = self._executor.execute(job)

        # PIPE-1800: for plane-rejection, we restrict the flagging stats evaluation within the
        # 1deg^2 box based on the cutout layout.
        mosaic_side_arcsec = 3600  # 1 degree
        dist = (mosaic_side_arcsec / 2.)
        dist_arcsec = str(dist) + 'arcsec'

        fid_list = imlist_entry['heuristics'].select_fields(offsets=dist_arcsec,
                                                            intent='TARGET',
                                                            phasecenter=imlist_entry['phasecenter'],
                                                            name='0*,"0*,1*,"1*,2*,"2*,T*,"T*')
        # for the existing VLASS workflow, only one MS is used, though this might change in the future.
        fid_list = fid_list[0]

        field_objs = msobj.get_fields(field_id=fid_list)
        n_spwgroup = len(imlist_entry['spw'])
        n_flagged_field_spwgroup = np.zeros((len(field_objs), n_spwgroup))
        n_total_field_spwgroup = np.zeros((len(field_objs), n_spwgroup))
        scan_list = np.zeros(len(field_objs))
        fname_list = []

        for field_idx, field_obj in enumerate(field_objs):
            fname_list.append(field_obj.name)
            for spwgroup_idx, spwgroup_sel in enumerate(imlist_entry['spw']):
                n_flagged = n_total = 0.0
                spw_list = spwgroup_sel.split(',')
                for spw_str in spw_list:
                    n_flagged += flag_stats[field_obj.name]['spw'][spw_str]['flagged']
                    n_total += flag_stats[field_obj.name]['spw'][spw_str]['total']
                n_flagged_field_spwgroup[field_idx, spwgroup_idx] = n_flagged
                n_total_field_spwgroup[field_idx, spwgroup_idx] = n_total
                scan_list[field_idx] = list(flag_stats[field_obj.name]['scan'].keys())[0]

        nfield_above_flagpct = np.sum(n_flagged_field_spwgroup/n_total_field_spwgroup >
                                      self.inputs.vlass_plane_reject_ms['flagpct_thresh'], axis=0)
        spwgroup_reject = [False]*n_spwgroup
        for idx, nfield in enumerate(nfield_above_flagpct):
            is_spwgroup_excluded = set(imlist_entry['spw'][idx].split(',')) & set(
                self.inputs.vlass_plane_reject_ms['exclude_spw'].split(','))
            if self.inputs.vlass_plane_reject_ms['apply'] and nfield >= self.inputs.vlass_plane_reject_ms['nfield_thresh'] and not is_spwgroup_excluded:
                spwgroup_reject[idx] = True

        vlass_flag_stats = {}
        vlass_flag_stats['spwgroup_list'] = imlist_entry['spw']
        vlass_flag_stats['scan_list'] = scan_list
        vlass_flag_stats['fname_list'] = fname_list
        vlass_flag_stats['flagpct_field_spwgroup'] = n_flagged_field_spwgroup / \
            n_total_field_spwgroup
        vlass_flag_stats['flagpct_spwgroup'] = np.sum(
            n_flagged_field_spwgroup, axis=0)/np.sum(n_total_field_spwgroup, axis=0)
        vlass_flag_stats['nfield_above_flagpct'] = nfield_above_flagpct
        vlass_flag_stats['flagpct_thresh'] = self.inputs.vlass_plane_reject_ms['flagpct_thresh']
        vlass_flag_stats['nfield_thresh'] = self.inputs.vlass_plane_reject_ms['nfield_thresh']
        vlass_flag_stats['spwgroup_reject'] = spwgroup_reject

        return vlass_flag_stats
