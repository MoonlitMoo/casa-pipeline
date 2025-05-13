import copy
import operator
import os

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import imageparams_factory
from pipeline.hif.tasks.makeimages.resultobjects import MakeImagesResult
from pipeline.infrastructure import casa_tools, task_registry

from .cleantarget import CleanTarget
from .resultobjects import MakeImListResult

LOG = infrastructure.get_logger(__name__)


class MakeImListInputs(vdp.StandardInputs):
    # Must use empty data type list to allow for user override and
    # automatic determination depending on specmode, field and spw.
    processing_data_type = []

    # simple properties with no logic ----------------------------------------------------------------------------------
    calmaxpix = vdp.VisDependentProperty(default=300)
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='TARGET')
    nchan = vdp.VisDependentProperty(default=-1)
    outframe = vdp.VisDependentProperty(default='LSRK')
    phasecenter = vdp.VisDependentProperty(default='')
    psf_phasecenter = vdp.VisDependentProperty(default='')
    start = vdp.VisDependentProperty(default='')
    uvrange = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default='')
    clearlist = vdp.VisDependentProperty(default=True)
    per_eb = vdp.VisDependentProperty(default=False)
    per_session = vdp.VisDependentProperty(default=False)
    calcsb = vdp.VisDependentProperty(default=False)
    datatype = vdp.VisDependentProperty(default='')
    datacolumn = vdp.VisDependentProperty(default='')
    parallel = vdp.VisDependentProperty(default='automatic')
    robust = vdp.VisDependentProperty(default=None)
    uvtaper = vdp.VisDependentProperty(default=None)
    allow_wproject = vdp.VisDependentProperty(default=False)

    # properties requiring some processing or MS-dependent logic -------------------------------------------------------

    contfile = vdp.VisDependentProperty(default='cont.dat')

    @contfile.postprocess
    def contfile(self, unprocessed):
        return os.path.join(self.context.output_dir, unprocessed)

    @vdp.VisDependentProperty
    def field(self):
        if 'TARGET' in self.intent and 'field' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['field']
        return ''

    @field.convert
    def field(self, val):
        if not isinstance(val, (str, type(None))):
            # PIPE-1881: allow field names that mistakenly get casted into non-string datatype by
            # recipereducer (utils.string_to_val) and executeppr (XmlObjectifier.castType)
            LOG.warning('The field selection input %r is not a string and will be converted.', val)
            val = str(val)
        return val

    @vdp.VisDependentProperty
    def hm_cell(self):
        return []

    @hm_cell.convert
    def hm_cell(self, val):
        if not isinstance(val, str) and not isinstance(val, list):
            raise ValueError('Malformatted value for hm_cell: {!r}'.format(val))

        if isinstance(val, str):
            val = [val]

        for item in val:
            if isinstance(item, str):
                if 'ppb' in item:
                    return item

        return val

    @vdp.VisDependentProperty
    def hm_imsize(self):
        return []

    @hm_imsize.convert
    def hm_imsize(self, val):
        if not isinstance(val, int) and not isinstance(val, str) and not isinstance(val, list):
            raise ValueError('Malformatted value for hm_imsize: {!r}'.format(val))

        if isinstance(val, int):
            return [val, val]

        if isinstance(val, str):
            val = [val]

        for item in val:
            if isinstance(item, str):
                if 'pb' in item:
                    return item

        return val

    linesfile = vdp.VisDependentProperty(default='lines.dat')

    @linesfile.postprocess
    def linesfile(self, unprocessed):
        return os.path.join(self.context.output_dir, unprocessed)

    @vdp.VisDependentProperty
    def nbins(self):
        if 'TARGET' in self.intent and 'nbins' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['nbins']
        return ''

    @vdp.VisDependentProperty
    def spw(self):
        if 'TARGET' in self.intent and 'spw' in self.context.size_mitigation_parameters and self.specmode=='cube':
            return self.context.size_mitigation_parameters['spw']
        return ''

    @spw.convert
    def spw(self, val):
        # Use str() method to catch single spwid case via PPR which maps to int.
        return str(val)

    @vdp.VisDependentProperty
    def specmode(self):
        if 'TARGET' in self.intent:
            return 'cube'
        return 'mfs'

    def get_spw_hm_cell(self, spwlist):
        """If possible obtain spwlist specific hm_cell, otherwise return generic value.

        hif_checkproductsize() task determines the mitigation parameters. It does not know, however, about the
        set spwlist in the hif_makeimlist call and determines mitigation parameters per band (complete spw set).
        The band containing the set spwlist is determined by checking whether spwlist is a subset of the band
        spw list. The mitigation parameters found for the matching band are applied to the set spwlist.

        If no singluar band (spw set) is found that would contain spwlist, then the default hm_cell heuristics is
        returned.

        TODO: refactor and make hif_checkproductsize() (or a new task) spwlist aware."""

        mitigated_hm_cell = None
        if 'TARGET' in self.intent and 'hm_cell' in self.context.size_mitigation_parameters:
            mitigated_hm_cell = self.context.size_mitigation_parameters['hm_cell']

        multi_target_size_mitigation = self.context.size_mitigation_parameters.get('multi_target_size_mitigation', {})
        if multi_target_size_mitigation:
            multi_target_spwlist = [spws for spws in multi_target_size_mitigation.keys() if set(
                spwlist.split(',')).issubset(set(spws.split(',')))]
            if len(multi_target_spwlist) == 1:
                mitigated_hm_cell = multi_target_size_mitigation.get(multi_target_spwlist[0], {}).get('hm_cell')

        if mitigated_hm_cell in [None, {}] or self.hm_cell:
            return self.hm_cell
        else:
            return mitigated_hm_cell

    def get_spw_hm_imsize(self, spwlist):
        """If possible obtain spwlist specific hm_imsize, otherwise return generic value.

        TODO: refactor and make hif_checkproductsize() (or a new task) spwlist aware."""
        mitigated_hm_imsize = None
        if 'TARGET' in self.intent and 'hm_imsize' in self.context.size_mitigation_parameters:
            mitigated_hm_imsize = self.context.size_mitigation_parameters['hm_imsize']
        multi_target_size_mitigation = self.context.size_mitigation_parameters.get('multi_target_size_mitigation', {})
        if multi_target_size_mitigation:
            multi_target_spwlist = [spws for spws in multi_target_size_mitigation.keys() if set(
                spwlist.split(',')).issubset(set(spws.split(',')))]
            if len(multi_target_spwlist) == 1:
                mitigated_hm_imsize = multi_target_size_mitigation.get(multi_target_spwlist[0], {}).get('hm_imsize')
        if mitigated_hm_imsize in [None, {}] or self.hm_imsize:
            return self.hm_imsize
        else:
            return mitigated_hm_imsize

    def __init__(self, context, output_dir=None, vis=None, imagename=None, intent=None, field=None, spw=None,
                 contfile=None, linesfile=None, uvrange=None, specmode=None, outframe=None, hm_imsize=None,
                 hm_cell=None, calmaxpix=None, phasecenter=None, psf_phasecenter=None, nchan=None, start=None, width=None, nbins=None,
                 robust=None, uvtaper=None, clearlist=None, per_eb=None, per_session=None, calcsb=None, datatype=None,
                 datacolumn=None, parallel=None, known_synthesized_beams=None, allow_wproject=False, scal=False):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.imagename = imagename
        self.intent = intent
        self.field = field
        self.spw = spw
        self.contfile = contfile
        self.linesfile = linesfile
        self.uvrange = uvrange
        self.specmode = specmode
        self.outframe = outframe
        self.hm_imsize = hm_imsize
        self.hm_cell = hm_cell
        self.calmaxpix = calmaxpix
        self.phasecenter = phasecenter
        self.psf_phasecenter = psf_phasecenter
        self.nchan = nchan
        self.start = start
        self.width = width
        self.nbins = nbins
        self.robust = robust
        self.uvtaper = uvtaper
        self.clearlist = clearlist
        self.per_eb = per_eb
        self.per_session = per_session
        self.calcsb = calcsb
        self.datatype = datatype
        self.datacolumn = datacolumn
        self.parallel = parallel
        self.known_synthesized_beams = known_synthesized_beams
        self.allow_wproject = allow_wproject
        self.scal = scal


@task_registry.set_equivalent_casa_task('hif_makeimlist')
@task_registry.set_casa_commands_comment('A list of target sources to be imaged is constructed.')
class MakeImList(basetask.StandardTaskTemplate):
    Inputs = MakeImListInputs

    is_multi_vis_task = True

    def prepare(self):
        # this python class will produce a list of images to be calculated.
        inputs = self.inputs

        calcsb = inputs.calcsb
        parallel = inputs.parallel
        if inputs.known_synthesized_beams is not None:
            known_synthesized_beams = inputs.known_synthesized_beams
        else:
            known_synthesized_beams = inputs.context.synthesized_beams
        qaTool = casa_tools.quanta

        imaging_mode = inputs.context.project_summary.telescope

        if inputs.scal:
            if imaging_mode in ['ALMA', 'VLA', 'JVLA', 'EVLA']:
                imaging_mode += '-SCAL'
            else:
                raise Exception('The self-cal imaging modes (*-SCAL) are only allowed for ALMA and VLA')

        result = MakeImListResult()
        result.clearlist = inputs.clearlist

        # describe the function of this task by interpreting the inputs
        # parameters to give an execution context
        long_descriptions = [_DESCRIPTIONS.get((intent.strip(), inputs.specmode), inputs.specmode) for intent in inputs.intent.split(',')]
        result.metadata['long description'] = 'Set-up parameters for %s imaging' % ' & '.join(utils.deduplicate(long_descriptions))

        sidebar_suffixes = {_SIDEBAR_SUFFIX.get((intent.strip(), inputs.specmode), inputs.specmode) for intent in inputs.intent.split(',')}
        result.metadata['sidebar suffix'] = '/'.join(sidebar_suffixes)

        # Check if this stage has been disabled for vla (never set for ALMA)
        if inputs.context.vla_skip_mfs_and_cube_imaging and inputs.specmode in ('mfs', 'cube'):
            result.set_info({'msg': 'Line imaging stages have been disabled for VLA due to no MS being produced for line imaging.',
                                 'intent': inputs.intent,
                                 'specmode': inputs.specmode})
            result.contfile = None
            result.linesfile = None
            return result

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result.mitigation_error = True
                result.set_info({'msg': 'Size mitigation had failed. No imaging targets were created.',
                                 'intent': inputs.intent,
                                 'specmode': inputs.specmode})
                result.contfile = None
                result.linesfile = None
                return result

        # validate vis
        if inputs.vis not in (None, '', ['']) and not isinstance(inputs.vis, list):
            msg = '"vis" must be a list of strings'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        # validate specmode
        if inputs.specmode not in ('mfs', 'cont', 'cube', 'repBW'):
            msg = '"specmode" must be one of "mfs", "cont", "cube", or "repBW"'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        # datatype and datacolumn are mutually exclusive
        if inputs.datatype not in (None, '') and inputs.datacolumn not in (None, ''):
            msg = '"datatype" and "datacolumn" are mutually exclusive'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        # validate datacolumn
        if inputs.datacolumn.upper() not in (None, '', 'DATA', 'CORRECTED'):
            msg = '"datacolumn" must be "data" or "corrected"'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        # obtain the list of datatypes to consider in case that no datatype was explicitly requested
        specmode_datatypes = DataType.get_specmode_datatypes(inputs.intent, inputs.specmode)

        # Check against any user input for datatype to make sure that the
        # correct initial vis list is chosen (e.g. for REGCAL_CONTLINE_ALL and RAW).
        known_datatypes_str = [v.name for v in DataType]
        explicit_user_datatypes = False
        if inputs.datatype not in (None, ''):
            # Consider every comma separated user value just once
            user_datatypes_str = list(set([datatype_str.strip().upper() for datatype_str in inputs.datatype.split(',')]))
            if all(datatype_str not in ('BEST', 'ALL', 'SELFCAL', 'REGCAL') for datatype_str in user_datatypes_str):
                datatype_checklist = [datatype_str not in known_datatypes_str for datatype_str in user_datatypes_str]
                if any(datatype_checklist):
                    msg = 'Undefined data type(s): {}'.format(','.join(d for d, c in zip(user_datatypes_str, datatype_checklist) if c))
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result
                explicit_user_datatypes = True
                # Use only intersection of specmode and user data types
                specmode_datatypes = list(set(specmode_datatypes).intersection(set(DataType[datatype_str] for datatype_str in user_datatypes_str)))
        else:
            user_datatypes_str = []

        specmode_datatypes_str = [specmode_datatype.name for specmode_datatype in specmode_datatypes]

        datacolumn = inputs.datacolumn

        # Data type handling considers automatic and manual use cases. The automatic
        # use case tries to choose the best available data type by walking through the
        # predefined lists of data types per specmode. The first available data type
        # is used and if a fallback data type must be used, a warning is issued.

        # The manual use case is controlled by the "datatype" task parameter. It can
        # is a string that can contain comma separated explicit data type strings
        # (not DataType enums) or the special strings "best", "all", "selfcal",
        # "regcal" or "selfcal,regcal".

        # The following code block checks for any such task parameter input to override
        # the automatic scheme. Several variables are defined to pass the information
        # into the actual loop over imaging targets. The naming convention is such that
        # simple variable names like "global_datatype" or "selected_datatype" refer to
        # actual DataType enums. To compare these against the user input, one needs
        # stringified versions. The corresponding variables have "_str" appended. In
        # addition, there are variables aimed at rendering the data type information in
        # the weblog. These are not just simple data type strings, but can also be
        # something like "<actual data type> instead of <desired data type>" or "N/A".
        # Those variables have the basename as above and then "_info" appended. For
        # the loops over several user data types, lists are needed. The list names
        # are using the plurals of the basenames and the corresponding "_str" or "_info"
        # appendices.

        # Select the correct vis list
        if inputs.vis in (None, '', ['']):
            (ms_objects_and_columns, selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=specmode_datatypes, msonly=False)
        else:
            (ms_objects_and_columns, selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=specmode_datatypes, msonly=False, vis=inputs.vis)

        if not ms_objects_and_columns:
            result.set_info({'msg': 'No data found. No imaging targets were created.',
                             'intent': inputs.intent,
                             'specmode': inputs.specmode})
            result.contfile = None
            result.linesfile = None
            return result

        # Check for changing vis lists.
        if explicit_user_datatypes:
            for user_datatype_str in user_datatypes_str:
                if inputs.vis in (None, '', ['']):
                    (sub_ms_objects_and_columns, sub_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=[DataType[user_datatype_str]], msonly=False)
                else:
                    (sub_ms_objects_and_columns, sub_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=[DataType[user_datatype_str]], msonly=False, vis=inputs.vis)
                if set(ms_objects_and_columns) != set(sub_ms_objects_and_columns):
                    msg = 'Requested data types and specmode lead to multiple vis lists. Please run hif_makeimlist with data type selections per kind of MS (targets, targets_line, etc.).'
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result

        global_datatype = selected_datatype
        global_datatype_str = global_datatype.name
        global_datatype_info = global_datatype_str
        selected_datatypes_str = [global_datatype_str]
        selected_datatypes_info = [global_datatype_info]
        automatic_datatype_choice = True

        global_columns = list(ms_objects_and_columns.values())

        if inputs.datatype in (None, '') and inputs.datacolumn in (None, ''):
            # Log these messages only if there is no user data type
            LOG.info(f'Using data type {selected_datatype.name} for imaging.')

            if selected_datatype == DataType.RAW:
                LOG.warn('Falling back to raw data for imaging.')

            if not all(global_column == global_columns[0] for global_column in global_columns):
                LOG.warn(f'Data type based column selection changes among MSes: {",".join(f"{k.basename}: {v}" for k,v in ms_objects_and_columns.items())}.')

        if inputs.datacolumn not in (None, ''):
            ms_datacolumn = inputs.datacolumn.upper()
            # Handle difference in MS and tclean column naming schemes
            if ms_datacolumn == 'CORRECTED':
                ms_datacolumn = 'CORRECTED_DATA'
            ms_object = list(ms_objects_and_columns)[0]
            if ms_datacolumn not in ms_object.data_colnames():
                msg = f'Data column {inputs.datacolumn} not available.'
                LOG.error(msg)
                result.error = True
                result.error_msg = msg
                return result
            global_datacolumn = inputs.datacolumn.upper()
            global_datatype = ms_object.get_data_type(ms_datacolumn)
            global_datatype_str = global_datatype.name
            global_datatype_info = f'{global_datatype_str} instead of {selected_datatype.name} due to user datacolumn'
            selected_datatypes_str = [global_datatype_str]
            selected_datatypes_info = [global_datatype_info]
            automatic_datatype_choice = False
            LOG.info(f'Manual override of datacolumn to {global_datacolumn}. Automatic data type ({selected_datatype.name}) based datacolumn would have been "{"DATA" if global_columns[0] == "DATA" else "CORRECTED"}". Data type of {global_datacolumn} column is {global_datatype_str}.')
        else:
            if global_columns[0] == 'DATA':
                global_datacolumn = 'data'
            elif global_columns[0] == 'CORRECTED_DATA':
                global_datacolumn = 'corrected'
            else:
                LOG.warn(f'Unknown column name {global_columns[0]}')
                global_datacolumn = ''

        datacolumn = global_datacolumn
        inputs.vis = [k.basename for k in ms_objects_and_columns.keys()]

        # Handle user supplied data type requests
        if inputs.datatype not in (None, ''):
            # Extract all available data types for the vis list
            vislist_datatypes_str = []
            for vis in inputs.vis:
                ms_object = inputs.context.observing_run.get_ms(vis)
                # Collect the data types across the vis list
                vislist_datatypes_str = vislist_datatypes_str + [ms_datatype.name for ms_datatype in ms_object.data_column]
            # Use just a set
            vislist_datatypes_str = list(set(vislist_datatypes_str))
            # Intersection of specmode based and vis based datatypes gives
            # list of actually available data types for this call.
            available_datatypes_str = list(set(specmode_datatypes_str).intersection(vislist_datatypes_str))

            if 'BEST' in user_datatypes_str:
                if user_datatypes_str != ['BEST']:
                    msg = '"BEST" and all other options are mutually exclusive'
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result

                # Automatic choice with fallback per source/spw selection
                user_datatypes_str = [global_datatype_str]
                user_datatypes_info = [global_datatype_info]
                automatic_datatype_choice = global_datatype is not None
                LOG.info(f'Using data type {global_datatype} for imaging.')
            elif 'ALL' in user_datatypes_str:
                if user_datatypes_str != ['ALL']:
                    msg = '"ALL" and all other options are mutually exclusive'
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result

                # All SELFCAL and REGCAL choices available for this vis list
                # List selfcal first, then regcal
                user_datatypes_str = [datatype_str for datatype_str in available_datatypes_str if 'SELFCAL' in datatype_str]
                user_datatypes_str = user_datatypes_str + [datatype_str for datatype_str in available_datatypes_str if 'REGCAL' in datatype_str]
                user_datatypes_info = [datatype_str for datatype_str in user_datatypes_str]
                automatic_datatype_choice = False
            else:
                user_datatypes_str = [datatype_str.strip().upper() for datatype_str in inputs.datatype.split(',')]
                if 'REGCAL' in user_datatypes_str or 'SELFCAL' in user_datatypes_str:
                    # Check if any explicit data types are given
                    if any(datatype_str in specmode_datatypes for datatype_str in user_datatypes_str):
                        msg = '"REGCAL"/"SELFCAL" and explicit data types are mutually exclusive'
                        LOG.error(msg)
                        result.error = True
                        result.error_msg = msg
                        return result

                    # Expand SELFCAL and REGCAL to explicit data types for this vis list
                    expanded_user_datatypes_str = []
                    # List selfcal first, then regcal
                    if 'SELFCAL' in user_datatypes_str:
                        expanded_user_datatypes_str = expanded_user_datatypes_str + [datatype_str for datatype_str in available_datatypes_str if 'SELFCAL' in datatype_str]
                    if 'REGCAL' in user_datatypes_str:
                        expanded_user_datatypes_str = expanded_user_datatypes_str + [datatype_str for datatype_str in available_datatypes_str if 'REGCAL' in datatype_str]
                    user_datatypes_str = expanded_user_datatypes_str
                    automatic_datatype_choice = False
                else:
                    # Explicit individual data types
                    datatype_checklist = [datatype_str not in known_datatypes_str for datatype_str in user_datatypes_str]
                    if any(datatype_checklist):
                        msg = 'Undefined data type(s): {}'.format(','.join(d for d, c in zip(user_datatypes_str, datatype_checklist) if c))
                        LOG.error(msg)
                        result.error = True
                        result.error_msg = msg
                        return result
                    automatic_datatype_choice = False
                user_datatypes_info = [datatype_str for datatype_str in user_datatypes_str]

            selected_datatypes_str = user_datatypes_str
            selected_datatypes_info = user_datatypes_info

        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()

        # Initial heuristics instance without spw information.
        self.heuristics = image_heuristics_factory.getHeuristics(
            vislist=inputs.vis,
            spw='',
            observing_run=inputs.context.observing_run,
            imagename_prefix=inputs.context.project_structure.ousstatus_entity_id,
            proj_params=inputs.context.project_performance_parameters,
            contfile=inputs.contfile,
            linesfile=inputs.linesfile,
            imaging_params=inputs.context.imaging_parameters,
            imaging_mode=imaging_mode
        )

        # Get representative target information
        repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, maxAllowedBeamAxialRatio, sensitivityGoal = self.heuristics.representative_target()

        # representative target case
        if inputs.specmode == 'repBW':
            repr_target_mode = True
            image_repr_target = False

            # The PI cube shall only be created for real representative targets
            if not real_repr_target:
                LOG.info('No representative target found. No PI cube will be made.')
                result.set_info({'msg': 'No representative target found. No PI cube will be made.',
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            # The PI cube shall only be created for cube mode
            elif reprBW_mode in ['multi_spw', 'all_spw']:
                LOG.info("Representative target bandwidth specifies aggregate continuum. No PI cube will be made since"
                         " specmode='cont' already covers this case.")
                result.set_info({'msg': "Representative target bandwidth specifies aggregate continuum. No PI cube will"
                                        " be made since specmode='cont' already covers this case.",
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            elif reprBW_mode == 'repr_spw':
                LOG.info("Representative target bandwidth specifies per spw continuum. No PI cube will be made since"
                         " specmode='mfs' already covers this case.")
                result.set_info({'msg': "Representative target bandwidth specifies per spw continuum. No PI cube will"
                                        " be made since specmode='mfs' already covers this case.",
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            else:
                repr_spw_nbin = 1
                if inputs.context.size_mitigation_parameters.get('nbins', '') != '':
                    nbin_items = inputs.nbins.split(',')
                    for nbin_item in nbin_items:
                        key, value = nbin_item.split(':')
                        if key == str(repr_spw):
                            repr_spw_nbin = int(value)

                # The PI cube shall only be created if the PI bandwidth is greater
                # than 4 times the nbin averaged bandwidth used in the default cube
                ref_ms = inputs.context.observing_run.get_ms(inputs.vis[0])
                real_repr_spw = inputs.context.observing_run.virtual2real_spw_id(repr_spw, ref_ms)
                physicalBW_of_1chan_Hz = float(ref_ms.get_spectral_window(real_repr_spw).channels[0].getWidth().convert_to(measures.FrequencyUnits.HERTZ).value)
                repr_spw_nbin_bw_Hz = repr_spw_nbin * physicalBW_of_1chan_Hz
                reprBW_Hz = qaTool.getvalue(qaTool.convert(repr_target[2], 'Hz'))

                if reprBW_Hz > 4.0 * repr_spw_nbin_bw_Hz:
                    repr_spw_nbin = int(reprBW_Hz / physicalBW_of_1chan_Hz + 0.5)
                    inputs.nbins = '%d:%d' % (repr_spw, repr_spw_nbin)
                    LOG.info('Making PI cube at %.3g MHz channel width.' % (physicalBW_of_1chan_Hz * repr_spw_nbin / 1e6))
                    image_repr_target = True
                    inputs.field = repr_source
                    inputs.spw = str(repr_spw)
                else:
                    LOG.info('Representative target bandwidth is less or equal than 4 times the nbin averaged default'
                             ' cube channel width. No PI cube will be made since the default cube already covers this'
                             ' case.')
                    result.set_info({'msg': 'Representative target bandwidth is less or equal than 4 times the nbin'
                                            ' averaged default cube channel width. No PI cube will be made since the'
                                            ' default cube already covers this case.',
                                     'intent': 'TARGET',
                                     'specmode': 'repBW'})
                    result.contfile = None
                    result.linesfile = None
                    return result
        else:
            repr_target_mode = False
            image_repr_target = False

        if (not repr_target_mode) or (repr_target_mode and image_repr_target):
            # read the spw, if none then set default
            spw = inputs.spw

            if spw == '':
                spwids = sorted(inputs.context.observing_run.virtual_science_spw_ids, key=int)
            else:
                spwids = spw.split(',')
            spw = ','.join("'%s'" % (spwid) for spwid in spwids)
            spw = '[%s]' % spw

            spwlist = spw.replace('[', '').replace(']', '')
            spwlist = spwlist[1:-1].split("','")
        else:
            spw = '[]'
            spwlist = []

        if inputs.per_eb and inputs.per_session:
            msg = '"per_eb" and "per_session" are mutually exclusive'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        if inputs.per_eb:
            vislists = [[vis] for vis in inputs.vis]
        elif inputs.per_session:
            vislists = list(sessionutils.group_vislist_into_sessions(inputs.context, inputs.vis).values())
        else:
            vislists = [inputs.vis]

        if 'VLA' in imaging_mode:
            ref_ms = inputs.context.observing_run.get_ms(inputs.vis[0])
            vla_band = ref_ms.get_vla_spw2band()
            band_spws = {}
            for k, v in vla_band.items():
                if str(k) in spwlist:
                    band_spws.setdefault(v, []).append(k)
        elif 'ALMA' in imaging_mode:
            ref_ms = inputs.context.observing_run.get_ms(inputs.vis[0])
            band_spws = {}
            for spwid in spwlist:
                real_spw_id = inputs.context.observing_run.virtual2real_spw_id(int(spwid), ref_ms)
                real_spw_id_obj = ref_ms.get_spectral_window(real_spw_id)
                if real_spw_id_obj:
                    band_name = real_spw_id_obj.band
                    band_spws.setdefault(band_name, []).append(spwid)
        else:
            band_spws = {None: 0}

        # Need to record if there are targets for a vislist
        have_targets = {}

        expected_num_targets = 0
        for selected_datatype_str, selected_datatype_info in zip(selected_datatypes_str, selected_datatypes_info):
            for band in band_spws:
                if band is not None:
                    # The format of "spw" is "['<id1>','<id2>',...,'<idN>']", i.e. without any
                    # blanks (see also above where the default spw value is computed for the non
                    # band case).
                    spw = "[" + ",".join(f"'{item}'" for item in band_spws[band]) + "]"
                    spwlist = band_spws[band]
                for vislist in vislists:
                    if inputs.per_eb:
                        imagename_prefix = utils.remove_trailing_string(os.path.basename(vislist[0]), '.ms')
                    elif inputs.per_session:
                        imagename_prefix = inputs.context.observing_run.get_ms(vislist[0]).session
                    else:
                        imagename_prefix = inputs.context.project_structure.ousstatus_entity_id

                    self.heuristics = image_heuristics_factory.getHeuristics(
                        vislist=vislist,
                        spw=spw,
                        observing_run=inputs.context.observing_run,
                        imagename_prefix=imagename_prefix,
                        proj_params=inputs.context.project_performance_parameters,
                        contfile=inputs.contfile,
                        linesfile=inputs.linesfile,
                        imaging_params=inputs.context.imaging_parameters,
                        imaging_mode=imaging_mode
                    )
                    if inputs.specmode == 'cont':
                        # Make sure the spw list is sorted numerically
                        spwlist_local = [','.join(map(str, sorted(map(int, spwlist))))]
                    else:
                        spwlist_local = spwlist

                    # get list of field_ids/intents to be cleaned
                    if (not repr_target_mode) or (repr_target_mode and image_repr_target):
                        field_intent_list = self.heuristics.field_intent_list(
                          intent=inputs.intent, field=inputs.field)
                        if not field_intent_list:
                            continue
                    else:
                        continue

                    # Expand cont spws
                    if inputs.specmode == 'cont':
                        spwids = spwlist_local[0].split(',')
                    else:
                        spwids = spwlist_local

                    # Generate list of observed vis/field/spw combinations
                    vislist_field_intent_spw_combinations = {}
                    for field_intent in field_intent_list:
                        vislist_for_field = []
                        spwids_for_field = set()
                        for vis in vislist:
                            ms_domain_obj = inputs.context.observing_run.get_ms(vis)
                            # Get the real spw IDs for this MS
                            # TODO: This is missing spws that got removed in hif_uvcontsub.
                            #       Need to involve the full spwlist from above.
                            ms_science_spwids = [s.id for s in ms_domain_obj.get_spectral_windows()]
                            if field_intent[0] in [f.name for f in ms_domain_obj.fields]:
                                try:
                                    # Get a field domain object. Make sure that it has the necessary intent. Otherwise the list of spw IDs
                                    # will not match with the available science spw IDs.
                                    # Using all intents (inputs.intent) here. Further filtering is performed in the next block.
                                    field_domain_objs = ms_domain_obj.get_fields(field_intent[0], intent=inputs.intent)
                                    if field_domain_objs != []:
                                        field_domain_obj = field_domain_objs[0]
                                        # Get all science spw IDs for this field and record the ones that are present in this MS
                                        field_intent_science_spwids = [spw_domain_obj.id for spw_domain_obj in field_domain_obj.valid_spws if spw_domain_obj.id in ms_science_spwids and field_intent[1] in spw_domain_obj.intents]
                                        # Record the virtual spwids
                                        spwids_per_vis_and_field = [
                                            inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj)
                                            for spwid in field_intent_science_spwids
                                            if inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj) in list(map(int, spwids))]
                                    else:
                                        spwids_per_vis_and_field = []
                                except Exception as e:
                                    LOG.error(e)
                                    spwids_per_vis_and_field = []
                            else:
                                spwids_per_vis_and_field = []
                            if spwids_per_vis_and_field != []:
                                vislist_for_field.append(vis)
                                spwids_for_field.update(spwids_per_vis_and_field)
                        vislist_field_intent_spw_combinations[field_intent] = {'vislist': None, 'spwids': None}
                        if vislist_for_field != []:
                            vislist_field_intent_spw_combinations[field_intent]['vislist'] = vislist_for_field
                            vislist_field_intent_spw_combinations[field_intent]['spwids'] = sorted(list(spwids_for_field), key=int)
                            # Count the observed field/spw combinations. If the selected data type
                            # is not available, the counter will be decremented later since that
                            # check happens further below.
                            if inputs.specmode == 'cont':
                                expected_num_targets += 1
                            else:
                                expected_num_targets += len(spwids_for_field)

                    # Save original vislist_field_intent_spw_combinations dictionary to be able to generate
                    # proper messages if the vis list changes when falling back to a different data
                    # type for a given source/spw combination later on. The vislist_field_intent_spw_combinations
                    # dictionary is possibly being modified on-the-fly below.
                    original_vislist_field_intent_spw_combinations = copy.deepcopy(vislist_field_intent_spw_combinations)

                    # Remove bad spws and record actual vis/field/spw combinations containing data.
                    # Record all spws with actual data in a global list.
                    # Need all spw keys (individual and cont) to distribute the
                    # cell and imsize heuristic results which work on the
                    # highest/lowest frequency spw only.
                    all_spw_keys = []
                    valid_data = {}
                    filtered_spwlist = []
                    valid_data[str(vislist)] = {}
                    for vis in vislist:
                        ms_domain_obj = inputs.context.observing_run.get_ms(vis)
                        valid_data[vis] = {}
                        for field_intent in field_intent_list:
                            valid_data[vis][field_intent] = {}
                            if field_intent not in valid_data[str(vislist)]:
                                valid_data[str(vislist)][field_intent] = {}
                            # Check only possible field/spw combinations to speed up
                            if vislist_field_intent_spw_combinations.get(field_intent, None) is not None:
                                # Check if this field is present in the current MS and has the necessary intent.
                                # Using get_fields(name=...) since it does not throw an exception if the field is not found.
                                if ms_domain_obj.get_fields(name=field_intent[0], intent=field_intent[1]) != []:
                                    observed_vis_list = vislist_field_intent_spw_combinations.get(field_intent, None).get('vislist', None)
                                    observed_spwids_list = vislist_field_intent_spw_combinations.get(field_intent, None).get('spwids', None)
                                    if observed_vis_list is not None and observed_spwids_list is not None:
                                        # Save spws in main list
                                        all_spw_keys.extend(map(str, observed_spwids_list))
                                        # Also save cont selection
                                        all_spw_keys.append(','.join(map(str, observed_spwids_list)))
                                        for observed_spwid in map(str, observed_spwids_list):
                                            valid_data[vis][field_intent][str(observed_spwid)] = self.heuristics.has_data(field_intent_list=[field_intent], spwspec=observed_spwid, vislist=[vis])[field_intent]
                                            if not valid_data[vis][field_intent][str(observed_spwid)] and vis in observed_vis_list:
                                                LOG.warning('Data for EB {}, field {}, spw {} is completely flagged.'.format(
                                                    os.path.basename(vis), field_intent[0], observed_spwid))
                                            # Aggregated value per vislist (replace with lookup pattern later)
                                            if str(observed_spwid) not in valid_data[str(vislist)][field_intent]:
                                                valid_data[str(vislist)][field_intent][str(observed_spwid)] = valid_data[vis][field_intent][str(observed_spwid)]
                                            else:
                                                valid_data[str(vislist)][field_intent][str(observed_spwid)] = valid_data[str(vislist)][field_intent][str(observed_spwid)] or valid_data[vis][field_intent][str(observed_spwid)]
                                            if valid_data[vis][field_intent][str(observed_spwid)]:
                                                filtered_spwlist.append(observed_spwid)
                    filtered_spwlist = sorted(list(set(filtered_spwlist)), key=int)

                    # Collapse cont spws
                    if inputs.specmode == 'cont':
                        filtered_spwlist_local = [','.join(filtered_spwlist)]
                    else:
                        filtered_spwlist_local = filtered_spwlist

                    if filtered_spwlist_local == [] or filtered_spwlist_local == ['']:
                        # We can get here for example if a multiband observations does not feature
                        # LF or HF spws for a given field (PIPE-832). We should not log anything in
                        # that case. Before PIPE-832 there used to be a provisional error message for
                        # potential other cases, but this probably never happened. For now just
                        # continuing in the loop.
                        #LOG.error('No spws left for vis list {}'.format(','.join(os.path.basename(vis) for vis in vislist)))
                        continue

                    # Parse hm_cell to get optional pixperbeam setting
                    cell = inputs.get_spw_hm_cell(filtered_spwlist_local[0])
                    if isinstance(cell, str):
                        pixperbeam = float(cell.split('ppb')[0])
                        cell = []
                    else:
                        pixperbeam = 5.0

                    # Add actual, possibly reduced cont spw combination to be able to properly populate the lookup tables later on
                    if inputs.specmode == 'cont':
                        all_spw_keys.append(','.join(filtered_spwlist))
                    # Keep only unique entries
                    all_spw_keys = list(set(all_spw_keys))

                    # Select only the lowest / highest frequency spw to get the smallest (for cell size)
                    # and largest beam (for imsize)
                    min_freq = 1e15
                    max_freq = 0.0
                    min_freq_spwid = -1
                    max_freq_spwid = -1
                    for spwid in filtered_spwlist:
                        try:
                            ref_msname = self.heuristics.get_ref_msname(spwid)
                            ref_ms = inputs.context.observing_run.get_ms(ref_msname)
                            real_spwid = inputs.context.observing_run.virtual2real_spw_id(spwid, ref_ms)
                            spwid_centre_freq = ref_ms.get_spectral_window(real_spwid).centre_frequency.to_units(measures.FrequencyUnits.HERTZ)
                            if spwid_centre_freq < min_freq:
                                min_freq = spwid_centre_freq
                                min_freq_spwid = spwid
                            if spwid_centre_freq > max_freq:
                                max_freq = spwid_centre_freq
                                max_freq_spwid = spwid
                        except Exception as e:
                            LOG.warn(f'Could not determine min/max frequency for spw {spwid}. Exception: {str(e)}')

                    if min_freq_spwid == -1 or max_freq_spwid == -1:
                        LOG.error('Could not determine min/max frequency spw IDs for %s.' % (str(filtered_spwlist_local)))
                        continue

                    min_freq_spwlist = [str(min_freq_spwid)]
                    max_freq_spwlist = [str(max_freq_spwid)]

                    # Get robust and uvtaper values
                    if inputs.robust not in (None, -999.0):
                        robust = inputs.robust
                    elif 'robust' in inputs.context.imaging_parameters:
                        robust = inputs.context.imaging_parameters['robust']
                    else:
                        robust = self.heuristics.robust(specmode=inputs.specmode)

                    if inputs.uvtaper not in (None, []):
                        uvtaper = inputs.uvtaper
                    elif 'uvtaper' in inputs.context.imaging_parameters:
                        uvtaper = inputs.context.imaging_parameters['uvtaper']
                    else:
                        uvtaper = self.heuristics.uvtaper()

                    # cell is a list of form [cellx, celly]. If the list has form [cell]
                    # then that means the cell is the same size in x and y. If cell is
                    # empty then fill it with a heuristic result
                    cells = {}
                    if cell == []:
                        synthesized_beams = {}
                        min_cell = ['3600arcsec']
                        for spwspec in max_freq_spwlist:
                            # Use only fields that were observed in spwspec
                            actual_field_intent_list = []
                            for field_intent in field_intent_list:
                                if (vislist_field_intent_spw_combinations.get(field_intent, None) is not None and
                                        vislist_field_intent_spw_combinations[field_intent].get('spwids', None) is not None and
                                        spwspec in list(map(str, vislist_field_intent_spw_combinations[field_intent]['spwids']))):
                                    actual_field_intent_list.append(field_intent)

                            synthesized_beams[spwspec], known_synthesized_beams = self.heuristics.synthesized_beam(
                                field_intent_list=actual_field_intent_list, spwspec=spwspec, robust=robust, uvtaper=uvtaper,
                                pixperbeam=pixperbeam, known_beams=known_synthesized_beams, force_calc=calcsb,
                                parallel=parallel, shift=True)

                            if synthesized_beams[spwspec] == 'invalid':
                                LOG.error('Beam for virtual spw %s and robust value of %.1f is invalid. Cannot continue.'
                                          '' % (spwspec, robust))
                                result.error = True
                                result.error_msg = 'Invalid beam'
                                return result

                            # Avoid recalculating every time since the dictionary will be cleared with the first recalculation request.
                            calcsb = False
                            # the heuristic cell is always the same for x and y as
                            # the value derives from the single value returned by
                            # imager.advise
                            cells[spwspec] = self.heuristics.cell(beam=synthesized_beams[spwspec], pixperbeam=pixperbeam)
                            if ('invalid' not in cells[spwspec]):
                                min_cell = cells[spwspec] if (qaTool.convert(cells[spwspec][0], 'arcsec')['value'] < qaTool.convert(min_cell[0], 'arcsec')['value']) else min_cell
                        # Rounding to two significant figures
                        min_cell = ['%.2g%s' % (qaTool.getvalue(min_cell[0]), qaTool.getunit(min_cell[0]))]
                        # Need to populate all spw keys because the imsize heuristic picks
                        # up the lowest frequency spw.
                        for spwspec in all_spw_keys:
                            cells[spwspec] = min_cell
                    else:
                        for spwspec in all_spw_keys:
                            cells[spwspec] = cell

                    # get primary beams
                    largest_primary_beams = {}
                    for spwspec in min_freq_spwlist:
                        if list(field_intent_list) != []:
                            largest_primary_beams[spwspec] = self.heuristics.largest_primary_beam_size(spwspec=spwspec, intent=list(field_intent_list)[0][1])
                        else:
                            largest_primary_beams[spwspec] = self.heuristics.largest_primary_beam_size(spwspec=spwspec, intent='TARGET')

                    # if phase center not set then use heuristic code to calculate the
                    # centers for each field
                    phasecenter = inputs.phasecenter
                    psf_phasecenter = inputs.psf_phasecenter
                    phasecenters = {}
                    psf_phasecenters = {}
                    if phasecenter == '':
                        for field_intent in field_intent_list:
                            try:
                                field_ids = self.heuristics.field(field_intent[1], field_intent[0], vislist=vislist_field_intent_spw_combinations[field_intent]['vislist'])
                                phasecenters[field_intent[0]], psf_phasecenters[field_intent[0]] = self.heuristics.phasecenter(field_ids, vislist=vislist_field_intent_spw_combinations[field_intent]['vislist'], intent=field_intent[1], primary_beam=largest_primary_beams[min_freq_spwlist[0]], shift_to_nearest_field=True)
                            except Exception as e:
                                # problem defining center
                                LOG.warning(e)
                    else:
                        for field_intent in field_intent_list:
                            phasecenters[field_intent[0]] = phasecenter
                            psf_phasecenters[field_intent[0]] = psf_phasecenter

                    # if imsize not set then use heuristic code to calculate the
                    # centers for each field/spwspec
                    imsize = inputs.get_spw_hm_imsize(filtered_spwlist_local[0])
                    if isinstance(imsize, str):
                        sfpblimit = float(imsize.split('pb')[0])
                        imsize = []
                    else:
                        sfpblimit = 0.2
                    imsizes = {}
                    if imsize == []:
                        for field_intent in field_intent_list:
                            max_x_size = 1
                            max_y_size = 1
                            for spwspec in min_freq_spwlist:

                                try:
                                    field_ids = self.heuristics.field(field_intent[1], field_intent[0], vislist=vislist_field_intent_spw_combinations[field_intent]['vislist'])
                                    # Image size (FOV) may be determined depending on the fractional bandwidth of the
                                    # selected spectral windows. In continuum spectral mode pass the spw list string
                                    # to imsize heuristics (used only for VLA), otherwise pass None to disable the feature.
                                    imsize_spwlist = filtered_spwlist_local if inputs.specmode == 'cont' else None
                                    h_imsize = self.heuristics.imsize(
                                        fields=field_ids, cell=cells[spwspec],
                                        primary_beam=largest_primary_beams[spwspec],
                                        sfpblimit=sfpblimit, centreonly=False,
                                        vislist=vislist_field_intent_spw_combinations[field_intent]['vislist'],
                                        spwspec=imsize_spwlist, intent=field_intent[1],
                                        joint_intents=inputs.intent, specmode=inputs.specmode)
                                    if field_intent[1] in [
                                            'PHASE',
                                            'BANDPASS',
                                            'AMPLITUDE',
                                            'FLUX',
                                            'CHECK',
                                            'POLARIZATION',
                                            'POLANGLE',
                                            'POLLEAKAGE',
                                            'DIFFGAINREF',
                                            'DIFFGAINSRC',
                                            ]:
                                        h_imsize = [min(npix, inputs.calmaxpix) for npix in h_imsize]
                                    imsizes[(field_intent[0], spwspec)] = h_imsize
                                    if imsizes[(field_intent[0], spwspec)][0] > max_x_size:
                                        max_x_size = imsizes[(field_intent[0], spwspec)][0]
                                    if imsizes[(field_intent[0], spwspec)][1] > max_y_size:
                                        max_y_size = imsizes[(field_intent[0], spwspec)][1]
                                except Exception as e:
                                    # problem defining imsize
                                    LOG.warning(e)
                                    pass

                            if max_x_size == 1 or max_y_size == 1:
                                LOG.error('imsize of [{:d}, {:d}] for field {!s} intent {!s} spw {!s} is degenerate.'.format(max_x_size, max_y_size, field_intent[0], field_intent[1], min_freq_spwlist))
                            else:
                                # Need to populate all spw keys because the imsize for the cont
                                # target is taken from this dictionary.
                                for spwspec in all_spw_keys:
                                    imsizes[(field_intent[0], spwspec)] = [max_x_size, max_y_size]

                    else:
                        for field_intent in field_intent_list:
                            for spwspec in all_spw_keys:
                                imsizes[(field_intent[0], spwspec)] = imsize

                    # if nchan is not set then use heuristic code to calculate it
                    # for each field/spwspec. The channel width needs to be calculated
                    # at the same time.
                    specmode = inputs.specmode
                    nchan = inputs.nchan
                    nchans = {}
                    width = inputs.width
                    widths = {}
                    if specmode not in ('mfs', 'cont') and width == 'pilotimage':
                        for field_intent in field_intent_list:
                            for spwspec in filtered_spwlist_local:
                                try:
                                    nchans[(field_intent[0], spwspec)], widths[(field_intent[0], spwspec)] = \
                                      self.heuristics.nchan_and_width(field_intent=field_intent[1], spwspec=spwspec)
                                except Exception as e:
                                    # problem defining nchan and width
                                    LOG.warning(e)
                                    pass

                    else:
                        for field_intent in field_intent_list:
                            for spwspec in all_spw_keys:
                                nchans[(field_intent[0], spwspec)] = nchan
                                widths[(field_intent[0], spwspec)] = width

                    usepointing = self.heuristics.usepointing()

                    # now construct the list of imaging command parameter lists that must
                    # be run to obtain the required images

                    # Remember if there are targets for this vislist
                    have_targets[','.join(vislist)] = len(field_intent_list) > 0

                    # Sort field/intent list alphabetically considering the intent as the first
                    # and the source name as the second key.
                    sorted_field_intent_list = sorted(field_intent_list, key=operator.itemgetter(1,0))

                    # In case of TARGET intent place representative source first in the list.
                    if 'TARGET' in inputs.intent:
                        sorted_field_intent_list = utils.place_repr_source_first(sorted_field_intent_list, repr_source)

                    for field_intent in sorted_field_intent_list:
                        mosweight = self.heuristics.mosweight(field_intent[1], field_intent[0])
                        for spwspec in filtered_spwlist_local:
                            # Start with original vis list
                            vislist_field_intent_spw_combinations[field_intent]['vislist'] = original_vislist_field_intent_spw_combinations[field_intent]['vislist']

                            # The field/intent and spwspec loops still cover the full parameter
                            # space. Here we filter the actual combinations.
                            valid_field_spwspec_combination = False
                            actual_spwids = []
                            if vislist_field_intent_spw_combinations[field_intent].get('spwids', None) is not None:
                                for spwid in spwspec.split(','):
                                    if valid_data[str(vislist)].get(field_intent, None):
                                        if valid_data[str(vislist)][field_intent].get(str(spwid), None):
                                            if int(spwid) in vislist_field_intent_spw_combinations[field_intent]['spwids']:
                                                valid_field_spwspec_combination = True
                                                actual_spwids.append(spwid)
                            if not valid_field_spwspec_combination:
                                continue

                            # For 'cont' mode we still need to restrict the virtual spw ID list to just
                            # the ones that were actually observed for this field.
                            adjusted_spwspec = ','.join(map(str, actual_spwids))

                            spwspec_ok = True
                            actual_spwspec_list = []
                            spwsel = {}
                            all_continuum = True
                            low_bandwidth = True
                            low_spread = True
                            cont_ranges_spwsel_dict = {}
                            all_continuum_spwsel_dict = {}
                            low_bandwidth_spwsel_dict = {}
                            low_spread_spwsel_dict = {}
                            spwsel_spwid_dict = {}

                            # Check if the globally selected data type is available for this field/spw combination.
                            if selected_datatype_str not in (None, '', 'N/A') and inputs.datacolumn in (None, ''):
                                if automatic_datatype_choice:
                                    # In automatic mode check for source/spw specific fall back to next available data type.
                                    (local_ms_objects_and_columns, local_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=specmode_datatypes, msonly=False, source=field_intent[0], spw=adjusted_spwspec, vis=vislist)
                                else:
                                    # In manual mode check determine the data column for the current data type.
                                    (local_ms_objects_and_columns, local_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=[DataType[selected_datatype_str]], msonly=False, source=field_intent[0], spw=adjusted_spwspec, vis=vislist)

                                if local_selected_datatype is None:
                                    expected_num_targets -= 1
                                    LOG.warn(f'Data type {selected_datatype_str} is not available for field {field_intent[0]} SPW {adjusted_spwspec} in the chosen vis list. Skipping imaging target.')
                                    continue

                                local_selected_datatype_str = local_selected_datatype.name
                                local_selected_datatype_info = local_selected_datatype_str
                                local_columns = list(local_ms_objects_and_columns.values())

                                if local_selected_datatype_str != selected_datatype_str:
                                    if automatic_datatype_choice:
                                        LOG.warn(f'Data type {selected_datatype_str} is not available for field {field_intent[0]} SPW {adjusted_spwspec}. Falling back to data type {local_selected_datatype_str}.')
                                        local_selected_datatype_info = f'{local_selected_datatype_str} instead of {selected_datatype_str} due to source/spw selection'
                                    else:
                                        # Manually selected data type unavailable -> skip making an imaging target
                                        expected_num_targets -= 1
                                        LOG.warn(f'Data type {selected_datatype_str} is not available for field {field_intent[0]} SPW {adjusted_spwspec} in the chosen vis list. Skipping imaging target.')
                                        continue

                                if local_columns != []:
                                    if not all(local_column == local_columns[0] for local_column in local_columns):
                                        LOG.warn(f'Data type based column selection changes among MSes: {",".join(f"{k.basename}: {v}" for k,v in local_ms_objects_and_columns.items())}.')

                                if local_columns != []:
                                    if local_columns[0] == 'DATA':
                                        local_datacolumn = 'data'
                                    elif local_columns[0] == 'CORRECTED_DATA':
                                        local_datacolumn = 'corrected'
                                    else:
                                        LOG.warn(f'Unknown column name {local_columns[0]}')
                                        local_datacolumn = ''
                                else:
                                    LOG.warn(f'Empty list of columns')
                                    local_datacolumn = ''

                                datacolumn = local_datacolumn

                                if not inputs.per_eb and original_vislist_field_intent_spw_combinations[field_intent]['vislist'] != [k.basename for k in local_ms_objects_and_columns.keys()]:
                                    vislist_field_intent_spw_combinations[field_intent]['vislist'] = [k.basename for k in local_ms_objects_and_columns.keys()]
                                    if automatic_datatype_choice and local_selected_datatype_str != selected_datatype_str:
                                        LOG.warn(f'''Modifying vis list from {original_vislist_field_intent_spw_combinations[field_intent]['vislist']} to {vislist_field_intent_spw_combinations[field_intent]['vislist']} for fallback data type {local_selected_datatype_str}.''')
                                    else:
                                        LOG.warn(f'''Modifying vis list from {original_vislist_field_intent_spw_combinations[field_intent]['vislist']} to {vislist_field_intent_spw_combinations[field_intent]['vislist']} for data type {local_selected_datatype_str}.''')

                                    if vislist_field_intent_spw_combinations[field_intent]['vislist'] == []:
                                        LOG.warn(f'Empty vis list for field {field_intent[0]} specmode {specmode} data type {local_selected_datatype_str}. Skipping imaging target.')
                                        continue
                            else:
                                datacolumn = global_datacolumn
                                local_selected_datatype = global_datatype
                                local_selected_datatype_str = global_datatype_str
                                local_selected_datatype_info = global_datatype_info

                            # PIPE-1710: add a suffix to image file name depending on datatype
                            if local_selected_datatype_str.lower().startswith('regcal'):
                                datatype_suffix = 'regcal'
                            elif local_selected_datatype_str.lower().startswith('selfcal'):
                                datatype_suffix = 'selfcal'
                            else:
                                datatype_suffix = None

                            # Save the specific vislist in a copy of the heuristics object tailored to the
                            # current imaging target
                            target_heuristics = copy.deepcopy(self.heuristics)
                            target_heuristics.vislist = vislist_field_intent_spw_combinations[field_intent]['vislist']

                            for spwid in adjusted_spwspec.split(','):
                                cont_ranges_spwsel_dict[spwid], all_continuum_spwsel_dict[spwid], low_bandwidth_spwsel_dict[spwid], low_spread_spwsel_dict[spwid] = target_heuristics.cont_ranges_spwsel()
                                spwsel_spwid_dict[spwid] = cont_ranges_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, 'NONE')

                            no_cont_ranges = False
                            if (field_intent[1] == 'TARGET' and specmode == 'cont' and
                                    all([v == '' for v in spwsel_spwid_dict.values()])):
                                LOG.warning('No valid continuum ranges were found for any spw. Creating an aggregate continuum'
                                            ' image from the full bandwidth from all spws, but this should be used with'
                                            ' caution.')
                                no_cont_ranges = True

                            for spwid in adjusted_spwspec.split(','):
                                spwsel_spwid = spwsel_spwid_dict[spwid]
                                if 'ALMA' in imaging_mode and field_intent[1] == 'TARGET' and specmode in ('mfs', 'cont') and not no_cont_ranges:
                                    if spwsel_spwid == '':
                                        if specmode == 'cont':
                                            LOG.warning('Spw {!s} will not be used in creating the aggregate continuum image'
                                                        ' of {!s} because no continuum range was found.'
                                                        ''.format(spwid, field_intent[0]))
                                        else:
                                            LOG.warning('Spw {!s} will not be used for {!s} because no continuum range was'
                                                        ' found.'.format(spwid, field_intent[0]))
                                            spwspec_ok = False
                                        continue
                                    #elif (spwsel_spwid == ''):
                                    #    LOG.warning('Empty continuum frequency range for %s, spw %s. Run hif_findcont ?' % (field_intent[0], spwid))

                                all_continuum = all_continuum and all_continuum_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, False)
                                low_bandwidth = low_bandwidth and low_bandwidth_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, False)
                                low_spread = low_spread and low_spread_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, False)

                                if spwsel_spwid in ('ALL', 'ALLCONT', '', 'NONE'):
                                    spwsel_spwid_freqs = ''
                                    if target_heuristics.is_eph_obj(field_intent[0]):
                                        spwsel_spwid_refer = 'SOURCE'
                                    else:
                                        spwsel_spwid_refer = 'LSRK'
                                else:
                                    spwsel_spwid_freqs, spwsel_spwid_refer = spwsel_spwid.split()

                                if spwsel_spwid_refer not in ('LSRK', 'SOURCE'):
                                    LOG.warning('Frequency selection is specified in %s but must be in LSRK or SOURCE'
                                                '' % spwsel_spwid_refer)
                                    # TODO: skip this field and/or spw ?

                                actual_spwspec_list.append(spwid)
                                spwsel['spw%s' % (spwid)] = spwsel_spwid

                            actual_spwspec = ','.join(actual_spwspec_list)

                            num_all_spws = len(adjusted_spwspec.split(','))
                            num_good_spws = 0 if no_cont_ranges else len(actual_spwspec_list)

                            # construct imagename
                            if inputs.imagename == '':
                                imagename = target_heuristics.imagename(output_dir=inputs.output_dir, intent=field_intent[1],
                                                                        field=field_intent[0], spwspec=actual_spwspec,
                                                                        specmode=specmode, band=band, datatype=datatype_suffix)
                            else:
                                imagename = inputs.imagename

                            if inputs.nbins != '' and inputs.specmode != 'cont':
                                nbin_items = inputs.nbins.split(',')
                                nbins_dict = {}
                                for nbin_item in nbin_items:
                                    key, value = nbin_item.split(':')
                                    nbins_dict[key] = int(value)
                                try:
                                    if '*' in nbins_dict:
                                        nbin = nbins_dict['*']
                                    else:
                                        nbin = nbins_dict[spwspec]
                                except:
                                    LOG.warning('Could not determine binning factor for spw %s. Using default channel width.'
                                                '' % adjusted_spwspec)
                                    nbin = -1
                            else:
                                nbin = -1

                            # Get stokes value. Note that the full list of intents
                            # (inputs.intent) is used to decide whether to do IQUV
                            # for ALMA as PIPE-1829 asked for Stokes I only if other
                            # calibration intents are done together with POLARIZATION.
                            stokes = self.heuristics.stokes(field_intent[1], inputs.intent)

                            if spwspec_ok and (field_intent[0], spwspec) in imsizes and ('invalid' not in cells[spwspec]):
                                LOG.debug(
                                  'field:%s intent:%s spw:%s cell:%s imsize:%s phasecenter:%s' %
                                  (field_intent[0], field_intent[1], adjusted_spwspec,
                                   cells[spwspec], imsizes[(field_intent[0], spwspec)],
                                   phasecenters[field_intent[0]]))

                                # Remove MSs that do not contain data for the given field/intent combination
                                # FIXME: This should already have been filtered above. This filter is just from the domain objects.
                                scanidlist, visindexlist = target_heuristics.get_scanidlist(vislist_field_intent_spw_combinations[field_intent]['vislist'],
                                                                                            field_intent[0], field_intent[1])
                                domain_filtered_vislist = [vislist_field_intent_spw_combinations[field_intent]['vislist'][i] for i in visindexlist]
                                if inputs.specmode == 'cont':
                                    filtered_vislist = domain_filtered_vislist
                                else:
                                    # Filter MSs with fully flagged field/spw selections
                                    filtered_vislist = [v for v in domain_filtered_vislist if valid_data[v][field_intent][str(adjusted_spwspec)]]

                                # Save the filtered vislist
                                if target_heuristics.vislist != filtered_vislist:
                                    LOG.warn(f'''Modifying vis list from {target_heuristics.vislist} to {filtered_vislist}''')
                                    target_heuristics.vislist = filtered_vislist

                                # Get list of antenna IDs
                                antenna_ids = target_heuristics.antenna_ids(inputs.intent)
                                # PIPE-964: The '&' at the end of the antenna input was added to not to consider the cross
                                #  baselines by default. The cross baselines with antennas not listed (for TARGET images
                                #  the antennas with the minority antenna sizes are not listed) could be added in some
                                #  future configurations by removing this character.
                                antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), '')))+'&'
                                           for v in filtered_vislist]

                                drcorrect, maxthreshold = self._get_drcorrect_maxthreshold(
                                    field_intent[0], actual_spwspec, local_selected_datatype_str)
                                target_heuristics.imaging_params['maxthreshold'] = maxthreshold
                                nfrms_multiplier = self._get_nfrms_multiplier(
                                    field_intent[0], actual_spwspec, local_selected_datatype_str)
                                target_heuristics.imaging_params['nfrms_multiplier'] = nfrms_multiplier                                

                                deconvolver, nterms = self._get_deconvolver_nterms(field_intent[0], field_intent[1],
                                                                                   actual_spwspec, stokes, inputs.specmode,
                                                                                   local_selected_datatype_str, target_heuristics)

                                reffreq = target_heuristics.reffreq(deconvolver, inputs.specmode, spwsel)
                                target_heuristics.imaging_params['allow_wproject'] = inputs.allow_wproject
                                gridder = target_heuristics.gridder(field_intent[1], field_intent[0], spwspec=actual_spwspec)

                                # Get field-specific uvrange value
                                uvrange = inputs.uvrange if inputs.uvrange not in (None, [], '') else None
                                bl_ratio = None
                                if uvrange is None:
                                    try:
                                        uvrange, bl_ratio = self.heuristics.uvrange(field=field_intent[0], spwspec=actual_spwspec)
                                    except Exception as ex:
                                        # After PIPE-2266, an exception is unlikely to occur because a null-selection condition has been excluded.
                                        # Nevertheless, we retain this exception handler as an extra precaution.
                                        LOG.warning("Error calculating the heuristics uvrange value for field %s spw %s : %s",
                                                    field_intent[0], actual_spwspec, ex)
                                              
                                target = CleanTarget(
                                    antenna=antenna,
                                    field=field_intent[0],
                                    intent=field_intent[1],
                                    spw=actual_spwspec,
                                    spwsel_lsrk=spwsel,
                                    spwsel_all_cont=all_continuum,
                                    spwsel_low_bandwidth=low_bandwidth,
                                    spwsel_low_spread=low_spread,
                                    num_all_spws=num_all_spws,
                                    num_good_spws=num_good_spws,
                                    cell=cells[spwspec],
                                    imsize=imsizes[(field_intent[0], spwspec)],
                                    phasecenter=phasecenters[field_intent[0]],
                                    psf_phasecenter=psf_phasecenters[field_intent[0]],
                                    specmode=inputs.specmode,
                                    gridder=gridder,
                                    wprojplanes=target_heuristics.wprojplanes(gridder=gridder, spwspec=actual_spwspec),
                                    imagename=imagename,
                                    start=inputs.start,
                                    width=widths[(field_intent[0], spwspec)],
                                    nbin=nbin,
                                    nchan=nchans[(field_intent[0], spwspec)],
                                    robust=robust,
                                    uvrange=uvrange,
                                    bl_ratio=bl_ratio,
                                    uvtaper=uvtaper,
                                    stokes=stokes,
                                    heuristics=target_heuristics,
                                    vis=filtered_vislist,
                                    datacolumn=datacolumn,
                                    datatype=local_selected_datatype_str,
                                    datatype_info=local_selected_datatype_info,
                                    is_per_eb=inputs.per_eb if inputs.per_eb else None,
                                    usepointing=usepointing,
                                    mosweight=mosweight,
                                    drcorrect=drcorrect,
                                    deconvolver=deconvolver,
                                    nterms=nterms,
                                    reffreq=reffreq)

                                result.add_target(target)

        if inputs.intent == 'TARGET' and result.num_targets == 0 and not result.clean_list_info:
            result.set_info({'msg': 'No data found. No imaging targets were created.',
                             'intent': inputs.intent,
                             'specmode': inputs.specmode})

        if inputs.intent == 'CHECK':
            if not any(have_targets.values()):
                info_msg = 'No check source found.'
                LOG.info(info_msg)
                result.set_info({'msg': info_msg, 'intent': 'CHECK', 'specmode': inputs.specmode})
            elif inputs.per_eb and (not all(have_targets.values())):
                info_msg = 'No check source data found in EBs %s.' % (','.join([os.path.basename(k)
                                                                                for k, v in have_targets.items()
                                                                                if not v]))
                LOG.info(info_msg)
                result.set_info({'msg': info_msg, 'intent': 'CHECK', 'specmode': inputs.specmode})

        # Record total number of expected clean targets
        result.set_expected_num_targets(expected_num_targets)

        # Pass contfile and linefile names to context (via resultobjects)
        # for hif_findcont and hif_makeimages
        result.contfile = inputs.contfile
        result.linesfile = inputs.linesfile

        result.synthesized_beams = known_synthesized_beams

        return result

    def analyse(self, result):
        return result

    def _get_nfrms_multiplier(self, field, spw, datatype_str):
        """Get the nfrms multiplier for the selfcal-succesfull imaging target for VLA."""
        nfrms_multiplier = None
        context = self.inputs.context

        if (
            context.project_summary.telescope in ("VLA", "JVLA", "EVLA")
            and hasattr(context, "selfcal_targets")
            and datatype_str.startswith("SELFCAL_")
            and self.inputs.specmode == "cont"
        ):
            for sc_target in context.selfcal_targets:
                sc_spw = set(sc_target["spw"].split(","))
                im_spw = set(spw.split(","))
                # PIPE-1878: use previous succesfully selfcal results to derive the optimal nfrms multiplier value
                if sc_target["field"] == field and im_spw.intersection(sc_spw) and sc_target["sc_success"]:
                    try:
                        nfrms = sc_target["sc_lib"]["RMS_NF_final"]
                        rms = sc_target["sc_lib"]["RMS_final"]
                        LOG.info(
                            "The ratio of nf_rms and rms from the selfcal-final imaging for field %s and spw %s: %s",
                            field,
                            spw,
                            nfrms / rms,
                        )
                        nfrms_multiplier = max(nfrms / rms, 1.0)
                        LOG.info(
                            "Using nfrms_multiplier=%s for field %s and spw %s based "
                            "on previous selfcal aggregate continuum imaging results.",
                            nfrms_multiplier,
                            field,
                            spw,
                        )
                    except Exception as ex:
                        LOG.warning(
                            f"Error calculating the nfrms multiplier value for field {field} spw {spw} from previous succesful self-calibration outcome: {ex}"
                        )

                    break

        return nfrms_multiplier

    def _get_deconvolver_nterms(self, field, intent, spw, stokes, specmode, datatype_str, image_heuristics):
        """
        Get deconvolver and nterms. First check for any modified parameters based
        on existing selfcal results. Otherwise use the heuristics methods.
        """

        deconvolver, nterms = None, None
        context = self.inputs.context

        if hasattr(context, 'selfcal_targets') and datatype_str.startswith('SELFCAL_') and specmode == 'cont':
            for sc_target in context.selfcal_targets:
                sc_spw = set(sc_target['spw'].split(','))
                im_spw = set(spw.split(','))
                if sc_target['field'] == field and im_spw.intersection(sc_spw) and sc_target['sc_success']:
                    nterms_sc = sc_target['sc_lib']['nterms']
                    if nterms_sc is not None and nterms_sc > 1:
                        nterms = nterms_sc
                        deconvolver = 'mtmfs'
                        LOG.info(f"Using deconvolver='mtmfs', nterms={nterms} for field {field} and spw {spw} based "
                                 "on previous selfcal aggregate continuum imaging results.")
                    break

        if deconvolver is None and nterms is None:
            deconvolver = image_heuristics.deconvolver(specmode, spw, intent, stokes)
            nterms = image_heuristics.nterms(spw)

        return deconvolver, nterms

    def _get_drcorrect_maxthreshold(self, field, spw, datatype_str):
        """Get the modified drcorrect parameter based on existing selfcal results."""

        drcorrect = maxthreshold = None
        context = self.inputs.context

        if context.project_summary.telescope in ('VLA', 'JVLA', 'EVLA'):
            return drcorrect, maxthreshold

        if hasattr(context, 'selfcal_targets') and datatype_str.startswith('SELFCAL_') and self.inputs.specmode == 'cont':

            for sc_target in context.selfcal_targets:
                sc_spw = set(sc_target['spw'].split(','))
                im_spw = set(spw.split(','))
                if sc_target['field'] == field and im_spw.intersection(sc_spw) and sc_target['sc_success']:

                    # PIPE-1452: use previous succesfully selfcal results to modify DR correction values
                    drcorrect = sc_target['sc_rms_scale']
                    LOG.info(f"Using drcorrect={drcorrect} for field {field} and spw {spw} based "
                             "on previous selfcal aggregate continuum imaging results.")

                    # PIPE-2117: use previous regcal reuslts to set the cleaning threshold upper limit
                    for r in context.results:
                        result = r.read()
                        if isinstance(result, MakeImagesResult):
                            for tclean_result in result.results:
                                if tclean_result.datatype_info.startswith('REGCAL_CONTLINE') and \
                                        tclean_result.specmode == 'cont' and \
                                        tclean_result.sourcename == field and \
                                        im_spw.intersection(set(tclean_result.spw.split(','))):
                                    maxthreshold = tclean_result.threshold
                    if maxthreshold is not None:
                        LOG.info(
                            f"Define an imaging threshold upper limit of {maxthreshold} for self-calibrated field {field} and spw {spw} "
                            "based on previous regular aggregate continuum imaging results.")
                    break

        return drcorrect, maxthreshold


# maps intent and specmode Inputs parameters to textual description of execution context.
_DESCRIPTIONS = {
    ('PHASE', 'mfs'): 'phase calibrator',
    ('PHASE', 'cont'): 'phase calibrator',
    ('BANDPASS', 'mfs'): 'bandpass calibrator',
    ('BANDPASS', 'cont'): 'bandpass calibrator',
    ('AMPLITUDE', 'mfs'): 'flux calibrator',
    ('AMPLITUDE', 'cont'): 'flux calibrator',
    ('POLARIZATION', 'mfs'): 'polarization calibrator',
    ('POLARIZATION', 'cont'): 'polarization calibrator',
    ('POLANGLE', 'mfs'): 'polarization calibrator',
    ('POLANGLE', 'cont'): 'polarization calibrator',
    ('POLLEAKAGE', 'mfs'): 'polarization calibrator',
    ('POLLEAKAGE', 'cont'): 'polarization calibrator',
    ('DIFFGAINREF', 'mfs'): 'diffgain calibrator',
    ('DIFFGAINREF', 'cont'): 'diffgain calibrator',
    ('DIFFGAINSRC', 'mfs'): 'diffgain calibrator',
    ('DIFFGAINSRC', 'cont'): 'diffgain calibrator',
    ('CHECK', 'mfs'): 'check source',
    ('CHECK', 'cont'): 'check source',
    ('TARGET', 'mfs'): 'target per-spw continuum',
    ('TARGET', 'cont'): 'target aggregate continuum',
    ('TARGET', 'cube'): 'target cube',
    ('TARGET', 'repBW'): 'representative bandwidth target cube'
}

_SIDEBAR_SUFFIX = {
    ('PHASE', 'mfs'): 'cals',
    ('PHASE', 'cont'): 'cals',
    ('BANDPASS', 'mfs'): 'cals',
    ('BANDPASS', 'cont'): 'cals',
    ('AMPLITUDE', 'mfs'): 'cals',
    ('AMPLITUDE', 'cont'): 'cals',
    ('DIFFGAINREF', 'mfs'): 'cals',
    ('DIFFGAINREF', 'cont'): 'cals',
    ('DIFFGAINSRC', 'mfs'): 'cals',
    ('DIFFGAINSRC', 'cont'): 'cals',
    ('POLARIZATION', 'mfs'): 'pol',
    ('POLARIZATION', 'cont'): 'pol',
    ('POLANGLE', 'mfs'): 'pol',
    ('POLANGLE', 'cont'): 'pol',
    ('POLLEAKAGE', 'mfs'): 'pol',
    ('POLLEAKAGE', 'cont'): 'pol',
    ('CHECK', 'mfs'): 'checksrc',
    ('CHECK', 'cont'): 'checksrc',
    ('TARGET', 'mfs'): 'mfs',
    ('TARGET', 'cont'): 'cont',
    ('TARGET', 'cube'): 'cube',
    ('TARGET', 'repBW'): 'cube_repBW'
}
