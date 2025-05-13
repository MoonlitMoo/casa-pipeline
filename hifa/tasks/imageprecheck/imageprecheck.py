import copy

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.hifa.heuristics import imageprecheck
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class ImagePreCheckResults(basetask.Results):
    def __init__(self, real_repr_target=False, repr_target='', repr_source='', repr_spw=None,
                 reprBW_mode=None, reprBW_nbin=None,
                 minAcceptableAngResolution='0.0arcsec', maxAcceptableAngResolution='0.0arcsec',
                 maxAllowedBeamAxialRatio=0.0, user_minAcceptableAngResolution='0.0arcsec',
                 user_maxAcceptableAngResolution='0.0arcsec', user_maxAllowedBeamAxialRatio=0.0,
                 sensitivityGoal='0mJy', hm_robust=0.5, hm_uvtaper=[],
                 sensitivities=None, sensitivity_bandwidth=None, score=None, single_continuum=False,
                 per_spw_cont_sensitivities_all_chan=None, synthesized_beams=None, beamRatios=None,
                 error=False, error_msg=None):
        super(ImagePreCheckResults, self).__init__()

        if sensitivities is None:
            sensitivities = []

        self.real_repr_target = real_repr_target
        self.repr_target = repr_target
        self.repr_source = repr_source
        self.repr_spw = repr_spw
        self.reprBW_mode = reprBW_mode
        self.reprBW_nbin = reprBW_nbin
        self.minAcceptableAngResolution = minAcceptableAngResolution
        self.maxAcceptableAngResolution = maxAcceptableAngResolution
        self.maxAllowedBeamAxialRatio = maxAllowedBeamAxialRatio
        self.sensitivityGoal = sensitivityGoal
        self.hm_robust = hm_robust
        self.hm_uvtaper = hm_uvtaper
        self.sensitivities = sensitivities
        self.sensitivities_for_aqua = []
        self.sensitivity_bandwidth = sensitivity_bandwidth
        self.score = score
        self.single_continuum = single_continuum
        self.per_spw_cont_sensitivities_all_chan = per_spw_cont_sensitivities_all_chan
        self.synthesized_beams = synthesized_beams
        self.beamRatios = beamRatios
        self.error = error
        self.error_msg = error_msg

        # Update these values for the weblog
        self.user_minAcceptableAngResolution = user_minAcceptableAngResolution
        self.user_maxAcceptableAngResolution = user_maxAcceptableAngResolution
        self.user_maxAllowedBeamAxialRatio = user_maxAllowedBeamAxialRatio

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        # Calculated sensitivities for later stages
        if self.per_spw_cont_sensitivities_all_chan is not None:
            if 'recalc' in self.per_spw_cont_sensitivities_all_chan:
                context.per_spw_cont_sensitivities_all_chan = copy.deepcopy(self.per_spw_cont_sensitivities_all_chan)
                del context.per_spw_cont_sensitivities_all_chan['recalc']
            else:
                utils.update_sens_dict(context.per_spw_cont_sensitivities_all_chan, self.per_spw_cont_sensitivities_all_chan)

        # Calculated beams for later stages
        if self.synthesized_beams is not None:
            if 'recalc' in self.synthesized_beams:
                context.synthesized_beams = copy.deepcopy(self.synthesized_beams)
                del context.synthesized_beams['recalc']
            else:
                utils.update_beams_dict(context.synthesized_beams, self.synthesized_beams)

        # Store calculated robust and uvtaper values in the context for later stages
        #
        # Note: For Cycle 6 the robust heuristic is used in subsequent stages.
        #       The uvtaper heuristic is not yet to be used for the ALMA PI pipeline,
        #       but is used for SRDP. 
        context.imaging_parameters['robust'] = self.hm_robust
        if 'uvtaper' in context.imaging_parameters.keys():
            del context.imaging_parameters['uvtaper']
        if self.hm_uvtaper != []:
            context.imaging_parameters['uvtaper'] = self.hm_uvtaper

        # It was decided not use a file based transport for the time being (03/2018)
        # Write imageparams.dat file
        #imageparams_filehandler = imageparamsfilehandler.ImageParamsFileHandler()
        #imageparams_filehandler.write(self.hm_robust, self.hm_uvtaper)

        # Add sensitivities to be reported to AQUA
        self.sensitivities_for_aqua.extend([s for s in self.sensitivities if s['robust']==self.hm_robust and s['uvtaper']==self.hm_uvtaper])

    def __repr__(self):
        return 'ImagePreCheckResults:\n\t{0}'.format(
            '\n\t'.join(['robust=%.2f' % (self.hm_robust), 'uvtaper=%s' % (self.hm_uvtaper)]))


class ImagePreCheckInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    calcsb = vdp.VisDependentProperty(default=False)
    parallel = vdp.VisDependentProperty(default='automatic')
    desired_angular_resolution = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, desired_angular_resolution=None, calcsb=None, parallel=None):
        self.context = context
        self.vis = vis
        self.desired_angular_resolution = desired_angular_resolution
        self.calcsb = calcsb
        self.parallel = parallel


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(ImagePreCheckInputs)


@task_registry.set_equivalent_casa_task('hifa_imageprecheck')
class ImagePreCheck(basetask.StandardTaskTemplate):
    Inputs = ImagePreCheckInputs

    is_multi_vis_task = True

    def prepare(self):

        inputs = self.inputs
        context = self.inputs.context

        cqa = casa_tools.quanta

        calcsb = inputs.calcsb
        parallel = inputs.parallel

        known_per_spw_cont_sensitivities_all_chan = context.per_spw_cont_sensitivities_all_chan
        known_synthesized_beams = context.synthesized_beams

        imageprecheck_heuristics = imageprecheck.ImagePreCheckHeuristics(inputs)

        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()

        if inputs.desired_angular_resolution in [None, '']:
            imaging_mode = 'ALMA'
        else:
            # This 'ALMA-SRDP' imaging mode is used when a value was
            # provided for desired_angular_resolution. This is only done
            # as part of the SRDP recipe (See: PIPE-1712)
            #
            # It's possible for the SRDP recipe to set
            # desired_angular_resolution to '', but in this case,
            # the 'ALMA' imaging mode can be used, because without
            # a desired_angular_resolution, SRDP falls back to the
            # default 'ALMA' behavior (no uvtaper).
            #
            # In the future, this could be expanded to be a more
            # complete imaging mode.
            imaging_mode = 'ALMA-SRDP'

        image_heuristics = image_heuristics_factory.getHeuristics(
            vislist=inputs.vis,
            spw='',
            observing_run=context.observing_run,
            imagename_prefix=context.project_structure.ousstatus_entity_id,
            proj_params=context.project_performance_parameters,
            contfile=context.contfile,
            linesfile=context.linesfile,
            imaging_params=context.imaging_parameters,
            imaging_mode=imaging_mode
        )

        repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, \
            minAcceptableAngResolution, maxAcceptableAngResolution, maxAllowedBeamAxialRatio, \
            sensitivityGoal = image_heuristics.representative_target()

        # Store PI selected angular resolution
        pi_minAcceptableAngResolution = minAcceptableAngResolution
        pi_maxAcceptableAngResolution = maxAcceptableAngResolution
        pi_maxAllowedBeamAxialRatio = maxAllowedBeamAxialRatio

        # Fetch angular resolution goals if they were entered via the task interface
        # See: PIPE-708, used only for SRDP, and PIPE-1712
        if inputs.desired_angular_resolution not in [None, '']:
            # Parse user angular resolution goal
            if type(inputs.desired_angular_resolution) is str:
                userAngResolution = cqa.quantity(inputs.desired_angular_resolution)
            else:
                userAngResolution = cqa.quantity('%.3garcsec' % inputs.desired_angular_resolution)
            # Assume symmetric beam for now
            user_desired_beam = {'minor': cqa.convert(userAngResolution, 'arcsec'),
                                 'major': cqa.convert(userAngResolution, 'arcsec'),
                                 'positionangle': '0.0deg'}
            # Determine
            userAngResolution = cqa.sqrt(cqa.div(cqa.add(cqa.pow(user_desired_beam['minor'],2),
                                                         cqa.pow(user_desired_beam['major'],2)),
                                                 2.0))
            LOG.info('Setting user specified desired angular resolution to %s' % cqa.tos(userAngResolution))
        else:
            userAngResolution = cqa.quantity('%.3garcsec' % 0.0)

        # Store user selected angular resolution (if task interface entered)
        user_minAcceptableAngResolution = cqa.mul(userAngResolution, 0.8)
        user_maxAcceptableAngResolution = cqa.mul(userAngResolution, 1.2)
        user_maxAllowedBeamAxialRatio = maxAllowedBeamAxialRatio

        # Use user selected angular resolution for calculation, if specified
        if cqa.getvalue(userAngResolution)[0] != 0.0:
            minAcceptableAngResolution = user_minAcceptableAngResolution
            maxAcceptableAngResolution = user_maxAcceptableAngResolution
            maxAllowedBeamAxialRatio = user_maxAllowedBeamAxialRatio

        repr_field = list(image_heuristics.field_intent_list('TARGET', repr_source))[0][0]

        repr_ms = self.inputs.ms[0]
        real_repr_spw = context.observing_run.virtual2real_spw_id(int(repr_spw), repr_ms)
        real_repr_spw_obj = repr_ms.get_spectral_window(real_repr_spw)
        single_continuum = any(['Single_Continuum' in t for t in real_repr_spw_obj.transitions])

        # Get the array
        diameter = min([a.diameter for a in repr_ms.antennas])
        if diameter == 7.0:
            array = '7m'
            robust_values_to_check = [0.5]
        else:
            array = '12m'
            robust_values_to_check = [0.0, 0.5, 1.0, 2.0]

        # Approximate reprBW with nbin
        if reprBW_mode in ['nbin', 'repr_spw']:
            physicalBW_of_1chan = float(real_repr_spw_obj.channels[0].getWidth().convert_to(measures.FrequencyUnits.HERTZ).value)
            nbin = int(cqa.getvalue(cqa.convert(repr_target[2], 'Hz'))/physicalBW_of_1chan + 0.5)
            cont_sens_bw_modes = ['aggBW']
            scale_aggBW_to_repBW = False
        elif reprBW_mode == 'multi_spw':
            nbin = -1
            cont_sens_bw_modes = ['repBW', 'aggBW']
            scale_aggBW_to_repBW = True
        else:
            nbin = -1
            cont_sens_bw_modes = ['repBW', 'aggBW']
            scale_aggBW_to_repBW = False

        primary_beam_size = image_heuristics.largest_primary_beam_size(spwspec=str(repr_spw), intent='TARGET')
        gridder = image_heuristics.gridder('TARGET', repr_field)
        field_ids = image_heuristics.field('TARGET', repr_field)
        cont_spwids = sorted(context.observing_run.virtual_science_spw_ids)
        repr_field_obj = repr_ms.get_fields(repr_field, intent='TARGET')[0]
        filtered_cont_spwids = sorted(
            [context.observing_run.real2virtual_spw_id(s.id, repr_ms) for s in repr_field_obj.valid_spws
             if context.observing_run.real2virtual_spw_id(s.id, repr_ms) in list(map(int, cont_spwids))])
        cont_spw = ','.join(map(str, filtered_cont_spwids))
        num_cont_spw = len(filtered_cont_spwids)

        # Get default heuristics uvtaper value
        default_uvtaper = image_heuristics.uvtaper()
        beams = {(0.0, str(default_uvtaper), 'repBW'): None, \
                 (0.5, str(default_uvtaper), 'repBW'): None, \
                 (1.0, str(default_uvtaper), 'repBW'): None, \
                 (2.0, str(default_uvtaper), 'repBW'): None, \
                 (0.0, str(default_uvtaper), 'aggBW'): None, \
                 (0.5, str(default_uvtaper), 'aggBW'): None, \
                 (1.0, str(default_uvtaper), 'aggBW'): None, \
                 (2.0, str(default_uvtaper), 'aggBW'): None}
        cells = {}
        imsizes = {}
        sensitivities = []
        sensitivity_bandwidth = None
        for robust in robust_values_to_check:
            # Calculate nbin / reprBW sensitivity if necessary
            if reprBW_mode in ['nbin', 'repr_spw']:

                beams[(robust, str(default_uvtaper), 'repBW')], known_synthesized_beams = image_heuristics.synthesized_beam(
                    [(repr_field, 'TARGET')], str(repr_spw), robust=robust, uvtaper=default_uvtaper,
                    known_beams=known_synthesized_beams, force_calc=calcsb, parallel=parallel, shift=True)

                # If the beam is invalid, error and return.
                if beams[(robust, str(default_uvtaper), 'repBW')] == 'invalid':
                    LOG.error('Beam for repBW and robust value of %.1f is invalid. Cannot continue.' % robust)
                    return ImagePreCheckResults(error=True, error_msg='Invalid beam')

                cells[(robust, str(default_uvtaper), 'repBW')] = image_heuristics.cell(beams[(robust, str(default_uvtaper), 'repBW')])
                imsizes[(robust, str(default_uvtaper), 'repBW')] = image_heuristics.imsize(field_ids, cells[(robust, str(default_uvtaper), 'repBW')], primary_beam_size, centreonly=False, intent='TARGET')

                try:
                    sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                        image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', str(repr_spw), nbin, {}, 'cube', gridder, cells[(robust, str(default_uvtaper), 'repBW')], imsizes[(robust, str(default_uvtaper), 'repBW')], 'briggs', robust, default_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                    # Set calcsb flag to False since the first calculations of beam
                    # and sensitivity will have already reset the dictionaries.
                    calcsb = False
                    sensitivities.append(Sensitivity(
                        array=array,
                        intent='TARGET',
                        field=repr_field,
                        spw=str(repr_spw),
                        is_representative=True,
                        bandwidth=cqa.quantity(sens_bw, 'Hz'),
                        bwmode='repBW',
                        beam=beams[(robust, str(default_uvtaper), 'repBW')],
                        cell=[cqa.convert(cells[(robust, str(default_uvtaper), 'repBW')][0], 'arcsec'),
                              cqa.convert(cells[(robust, str(default_uvtaper), 'repBW')][0], 'arcsec')],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(sensitivity, 'Jy/beam')))
                except:
                    sensitivities.append(Sensitivity(
                        array=array,
                        intent='TARGET',
                        field=repr_field,
                        spw=str(repr_spw),
                        is_representative=True,
                        bandwidth=cqa.quantity(0.0, 'Hz'),
                        bwmode='repBW',
                        beam=beams[(robust, str(default_uvtaper), 'repBW')],
                        cell=['0.0 arcsec', '0.0 arcsec'],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(0.0, 'Jy/beam')))
                    sens_bw = 0.0

                sensitivity_bandwidth = cqa.quantity(sens_bw, 'Hz')

            beams[(robust, str(default_uvtaper), 'aggBW')], known_synthesized_beams = image_heuristics.synthesized_beam(
                [(repr_field, 'TARGET')], cont_spw, robust=robust, uvtaper=default_uvtaper,
                known_beams=known_synthesized_beams, force_calc=calcsb, parallel=parallel, shift=True)

            # If the beam is invalid, error and return.
            if beams[(robust, str(default_uvtaper), 'aggBW')] == 'invalid':
                LOG.error('Beam for aggBW and robust value of %.1f is invalid. Cannot continue.' % robust)
                return ImagePreCheckResults(error=True, error_msg='Invalid beam')

            cells[(robust, str(default_uvtaper), 'aggBW')] = image_heuristics.cell(beams[(robust, str(default_uvtaper), 'aggBW')])
            imsizes[(robust, str(default_uvtaper), 'aggBW')] = image_heuristics.imsize(field_ids, cells[(robust, str(default_uvtaper), 'aggBW')], primary_beam_size, centreonly=False, intent='TARGET')

            # Calculate full cont sensitivity (no frequency ranges excluded)
            try:
                sensitivity, _, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                    image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', cont_spw, -1, {}, 'cont', gridder, cells[(robust, str(default_uvtaper), 'aggBW')], imsizes[(robust, str(default_uvtaper), 'aggBW')], 'briggs', robust, default_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                # Set calcsb flag to False since the first calculations of beam
                # and sensitivity will have already reset the dictionaries.
                calcsb = False
                for cont_sens_bw_mode in cont_sens_bw_modes:
                    if scale_aggBW_to_repBW and cont_sens_bw_mode == 'repBW':
                        # Handle scaling to repSPW_BW < repBW <= 0.9 * aggBW case
                        _bandwidth = repr_target[2]
                        _sensitivity = cqa.mul(cqa.quantity(sensitivity, 'Jy/beam'), cqa.sqrt(cqa.div(cqa.quantity(sens_bw, 'Hz'), repr_target[2])))
                    else:
                        _bandwidth = cqa.quantity(min(sens_bw, num_cont_spw * 1.875e9), 'Hz')
                        _sensitivity = cqa.quantity(sensitivity, 'Jy/beam')

                    sensitivities.append(Sensitivity(
                        array=array,
                        intent='TARGET',
                        field=repr_field,
                        spw=cont_spw,
                        is_representative=True,
                        bandwidth=_bandwidth,
                        bwmode=cont_sens_bw_mode,
                        beam=beams[(robust, str(default_uvtaper), 'aggBW')],
                        cell=[cqa.convert(cells[(robust, str(default_uvtaper), 'aggBW')][0], 'arcsec'),
                              cqa.convert(cells[(robust, str(default_uvtaper), 'aggBW')][0], 'arcsec')],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=_sensitivity))
            except Exception as e:
                for _ in cont_sens_bw_modes:
                    sensitivities.append(Sensitivity(
                        array=array,
                        intent='TARGET',
                        field=repr_field,
                        spw=cont_spw,
                        is_representative=True,
                        bandwidth=cqa.quantity(0.0, 'Hz'),
                        bwmode='aggBW',
                        beam=beams[(robust, str(default_uvtaper), 'aggBW')],
                        cell=['0.0 arcsec', '0.0 arcsec'],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(0.0, 'Jy/beam')))
                sens_bw = 0.0

            if sensitivity_bandwidth is None:
                sensitivity_bandwidth = cqa.quantity(_bandwidth, 'Hz')

        # Apply robust heuristic based on beam sizes for the used robust values.
        if reprBW_mode in ['nbin', 'repr_spw']:
            hm_robust, hm_robust_score, beamRatio_0p0, beamRatio_0p5, beamRatio_1p0, beamRatio_2p0 = \
                imageprecheck_heuristics.compare_beams( \
                    beams[(0.0, str(default_uvtaper), 'repBW')], \
                    beams[(0.5, str(default_uvtaper), 'repBW')], \
                    beams[(1.0, str(default_uvtaper), 'repBW')], \
                    beams[(2.0, str(default_uvtaper), 'repBW')], \
                    minAcceptableAngResolution, \
                    maxAcceptableAngResolution, \
                    maxAllowedBeamAxialRatio)
        else:
            hm_robust, hm_robust_score, beamRatio_0p0, beamRatio_0p5, beamRatio_1p0, beamRatio_2p0 = \
                imageprecheck_heuristics.compare_beams( \
                    beams[(0.0, str(default_uvtaper), 'aggBW')], \
                    beams[(0.5, str(default_uvtaper), 'aggBW')], \
                    beams[(1.0, str(default_uvtaper), 'aggBW')], \
                    beams[(2.0, str(default_uvtaper), 'aggBW')], \
                    minAcceptableAngResolution, \
                    maxAcceptableAngResolution, \
                    maxAllowedBeamAxialRatio)

        # Save beam ratios for weblog
        beamRatios = {
            (0.0, str(default_uvtaper)): beamRatio_0p0,
            (0.5, str(default_uvtaper)): beamRatio_0p5,
            (1.0, str(default_uvtaper)): beamRatio_1p0,
            (2.0, str(default_uvtaper)): beamRatio_2p0
        }

        if imaging_mode != 'ALMA-SRDP':
            # The below is for the ALMA-PI pipeline. userAngResolution)[0] will be 0.0 because desired_angular_resolution is not provided.
            # It's equal to this code block:
            #   https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/browse/pipeline/hifa/tasks/imageprecheck/imageprecheck.py?until=24152b4b508ec7e19a7dd360ff30c5c71755802e&untilPath=pipeline%2Fhifa%2Ftasks%2Fimageprecheck%2Fimageprecheck.py#357-462
            # with the disabled code block removed to avoid duplication from the SRDP-specific code from PIPE-1712.
            if real_repr_target:
                hm_uvtaper = default_uvtaper
            else:
                hm_robust = 0.5
                hm_uvtaper = default_uvtaper
                minAcceptableAngResolution = cqa.quantity(0.0, 'arcsec')
                maxAcceptableAngResolution = cqa.quantity(0.0, 'arcsec')
            # The default ALMA IF recipe (non-SRDP) always uses the default_uvtaper
            hm_uvtaper = default_uvtaper
        else:
            # The below block is only run for SRDP where userAngResolution)[0] is not 0.0
            #
            # Determine non-default UV taper value if the best robust is 2.0 and the user requested resolution parameter
            # (desired_angular_resolution) is set for SRDP. (PIPE-708)
            #
            # Compute uvtaper for representative targets and also if representative target cannot be determined
            # (real_repr_target = False) for representative targets and if user set angular resolution goal.
            #
            # Note that uvtaper is not computed for the ACA (7m) array, because robust of 2.0 is not in the checked range
            # (see robust_values_to_check).
            if hm_robust == 2.0:
                # Calculate the length of the 190th baseline, used to set the upper limit on uvtaper. See PIPE-1104.
                length_of_190th_baseline = image_heuristics.calc_length_of_nth_baseline(190)
                reprBW_mode_string = ['repBW' if reprBW_mode in ['nbin', 'repr_spw'] else 'aggBW']
                try:
                    hm_uvtaper = image_heuristics.uvtaper(beam_natural=beams[(2.0, str(default_uvtaper), reprBW_mode_string[0])],
                                                          beam_user=user_desired_beam,
                                                          tapering_limit=length_of_190th_baseline, repr_freq=repr_freq)
                except:
                    hm_uvtaper = []
                if hm_uvtaper != []:
                    # Add sensitivity entries with actual tapering
                    beams[(hm_robust, str(hm_uvtaper), 'repBW')], known_synthesized_beams = image_heuristics.synthesized_beam(
                        [(repr_field, 'TARGET')], str(repr_spw), robust=hm_robust, uvtaper=hm_uvtaper,
                        known_beams=known_synthesized_beams, force_calc=calcsb, parallel=parallel, shift=True)

                    # If the beam is invalid, error and return.
                    if beams[(hm_robust, str(hm_uvtaper), 'repBW')] == 'invalid':
                        LOG.error('Beam for uvtaper repBW is invalid. Cannot continue.')
                        return ImagePreCheckResults(error=True, error_msg='Invalid beam')

                    cells[(hm_robust, str(hm_uvtaper), 'repBW')] = image_heuristics.cell(beams[(hm_robust, str(hm_uvtaper), 'repBW')])
                    imsizes[(hm_robust, str(hm_uvtaper), 'repBW')] = image_heuristics.imsize(
                        field_ids, cells[(hm_robust, str(hm_uvtaper), 'repBW')], primary_beam_size, centreonly=False, intent='TARGET')
                    if reprBW_mode in ['nbin', 'repr_spw']:
                        try:
                            sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', str(repr_spw), nbin, {}, 'cube', gridder, cells[(
                                hm_robust, str(hm_uvtaper), 'repBW')], imsizes[(hm_robust, str(hm_uvtaper), 'repBW')], 'briggs', hm_robust, hm_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                            sensitivities.append(Sensitivity(
                                array=array,
                                intent='TARGET',
                                field=repr_field,
                                spw=str(repr_spw),
                                is_representative=True,
                                bandwidth=cqa.quantity(sens_bw, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'repBW')],
                                cell=[cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'repBW')][0], 'arcsec'),
                                      cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'repBW')][0], 'arcsec')],
                                robust=hm_robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(sensitivity, 'Jy/beam')))
                        except Exception as e:
                            sensitivities.append(Sensitivity(
                                array=array,
                                intent='TARGET',
                                field=repr_field,
                                spw=str(repr_spw),
                                is_representative=True,
                                bandwidth=cqa.quantity(0.0, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'repBW')],
                                cell=['0.0 arcsec', '0.0 arcsec'],
                                robust=robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(0.0, 'Jy/beam')))

                    beams[(hm_robust, str(hm_uvtaper), 'aggBW')], known_synthesized_beams = image_heuristics.synthesized_beam(
                        [(repr_field, 'TARGET')], cont_spw, robust=hm_robust, uvtaper=hm_uvtaper,
                        known_beams=known_synthesized_beams, force_calc=calcsb, parallel=parallel, shift=True)

                    # If the beam is invalid, error and return.
                    if beams[(hm_robust, str(hm_uvtaper), 'aggBW')] == 'invalid':
                        LOG.error('Beam for uvtaper aggBW is invalid. Cannot continue.')
                        return ImagePreCheckResults(error=True, error_msg='Invalid beam')

                    cells[(hm_robust, str(hm_uvtaper), 'aggBW')] = image_heuristics.cell(beams[(hm_robust, str(hm_uvtaper), 'aggBW')])
                    imsizes[(hm_robust, str(hm_uvtaper), 'aggBW')] = image_heuristics.imsize(
                        field_ids, cells[(hm_robust, str(hm_uvtaper), 'aggBW')], primary_beam_size, centreonly=False, intent='TARGET')
                    try:
                        sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = image_heuristics.calc_sensitivities(
                            inputs.vis, repr_field, 'TARGET', cont_spw, -1, {},
                            'cont', gridder, cells[(hm_robust, str(hm_uvtaper),
                                                    'aggBW')],
                            imsizes[(hm_robust, str(hm_uvtaper),
                                     'aggBW')],
                            'briggs', hm_robust, hm_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                        if scale_aggBW_to_repBW and cont_sens_bw_mode == 'repBW':
                            # Handle scaling to repSPW_BW < repBW <= 0.9 * aggBW case
                            _bandwidth = repr_target[2]
                            _sensitivity = cqa.mul(cqa.quantity(sensitivity, 'Jy/beam'),
                                                   cqa.sqrt(cqa.div(cqa.quantity(sens_bw, 'Hz'), repr_target[2])))
                        else:
                            _bandwidth = cqa.quantity(min(sens_bw, num_cont_spw * 1.875e9), 'Hz')
                            _sensitivity = cqa.quantity(sensitivity, 'Jy/beam')

                        for cont_sens_bw_mode in cont_sens_bw_modes:
                            sensitivities.append(Sensitivity(
                                array=array,
                                intent='TARGET',
                                field=repr_field,
                                spw=str(repr_spw),
                                is_representative=True,
                                bandwidth=_bandwidth,
                                bwmode=cont_sens_bw_mode,
                                beam=beams[(hm_robust, str(hm_uvtaper), 'aggBW')],
                                cell=[cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'aggBW')][0], 'arcsec'),
                                      cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'aggBW')][0], 'arcsec')],
                                robust=hm_robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=_sensitivity))
                    except:
                        for _ in cont_sens_bw_modes:
                            sensitivities.append(Sensitivity(
                                array=array,
                                intent='TARGET',
                                field=repr_field,
                                spw=str(repr_spw),
                                is_representative=True,
                                bandwidth=cqa.quantity(0.0, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'aggBW')],
                                cell=['0.0 arcsec', '0.0 arcsec'],
                                robust=robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(0.0, 'Jy/beam')))
                    # Update beamRatios
                    if reprBW_mode in ['nbin', 'repr_spw']:
                        beamRatios[(hm_robust, str(hm_uvtaper))] = cqa.tos(cqa.div(beams[(hm_robust, str(hm_uvtaper),
                                                                                          'repBW')]['major'],
                                                                                   beams[(hm_robust, str(hm_uvtaper),
                                                                                          'repBW')]['minor']),
                                                                           2)
                    else:
                        beamRatios[(hm_robust, str(hm_uvtaper))] = cqa.tos(cqa.div(beams[(hm_robust, str(hm_uvtaper),
                                                                                          'aggBW')]['major'],
                                                                                   beams[(hm_robust, str(hm_uvtaper),
                                                                                          'aggBW')]['minor']),
                                                                           2)
            else:
                hm_uvtaper = default_uvtaper
        return ImagePreCheckResults(
            real_repr_target,
            repr_target,
            repr_source,
            repr_spw,
            reprBW_mode,
            nbin,
            minAcceptableAngResolution=pi_minAcceptableAngResolution,
            maxAcceptableAngResolution=pi_maxAcceptableAngResolution,
            maxAllowedBeamAxialRatio=pi_maxAllowedBeamAxialRatio,
            user_minAcceptableAngResolution=user_minAcceptableAngResolution,
            user_maxAcceptableAngResolution=user_maxAcceptableAngResolution,
            user_maxAllowedBeamAxialRatio=user_maxAllowedBeamAxialRatio,
            sensitivityGoal=sensitivityGoal,
            hm_robust=hm_robust,
            hm_uvtaper=hm_uvtaper,
            sensitivities=sensitivities,
            sensitivity_bandwidth=sensitivity_bandwidth,
            score=hm_robust_score,
            single_continuum=single_continuum,
            per_spw_cont_sensitivities_all_chan=known_per_spw_cont_sensitivities_all_chan,
            synthesized_beams=known_synthesized_beams,
            beamRatios=beamRatios
        )

    def analyse(self, results):
        return results
