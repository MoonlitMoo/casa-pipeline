"""Imaging stage."""

import collections
import functools
import math
import os
from numbers import Number
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import numpy
from scipy import interpolate

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.imageheader as imageheader
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataTable, DataType, MeasurementSet
from pipeline.h.heuristics import fieldnames
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.hsd.heuristics import rasterscan
from pipeline.hsd.heuristics.rasterscan import RasterScanHeuristicsResult, RasterScanHeuristicsFailure
from pipeline.hsd.tasks import common
from pipeline.hsd.tasks.baseline import baseline
from pipeline.hsd.tasks.common import compress, direction_utils, observatory_policy, rasterutil, sdtyping
from pipeline.hsd.tasks.common import utils as sdutils
from pipeline.hsd.tasks.imaging import (detectcontamination, gridding,
                                        imaging_params, resultobjects,
                                        sdcombine, weighting, worker)
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry

if TYPE_CHECKING:
    from casatools import coordsys
    from pipeline.infrastructure import Context
    from resultobjects import SDImagingResults

LOG = infrastructure.get_logger(__name__)

# SensitivityInfo:
#     sensitivity: Sensitivity of an image
#     frequency_range: frequency ranges from which the sensitivity is calculated
#     to_export: True if the sensitivity shall be exported to aqua report. (to avoid exporting NRO sensitivity in K)
SensitivityInfo = collections.namedtuple('SensitivityInfo', 'sensitivity frequency_range to_export')
# RasterInfo: center_ra, center_dec = R.A. and Declination of map center
#             width=map extent along scan, height=map extent perpendicular to scan
#             angle=scan direction w.r.t. horizontal coordinate, row_separation=separation between raster rows.
RasterInfo = collections.namedtuple('RasterInfo', 'center_ra center_dec width height '
                                                  'scan_angle row_separation row_duration')
# Reference MS in combined list
REF_MS_ID = 0


class SDImagingInputs(vdp.StandardInputs):
    """Inputs for imaging task class."""

    # Search order of input vis
    processing_data_type = [DataType.BASELINED, DataType.ATMCORR,
                            DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    infiles = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    spw = vdp.VisDependentProperty(default='')
    pol = vdp.VisDependentProperty(default='')
    field = vdp.VisDependentProperty(default='')
    mode = vdp.VisDependentProperty(default='line')

    @field.postprocess
    def field(self, unprocessed: Optional[str]) -> Optional[str]:
        """Get fields as a string.

        Args:
            unprocessed : Unprocessed fields

        Returns:
            fields as a string
        """
        # LOG.info('field.postprocess: unprocessed = "{0}"'.format(unprocessed))
        if unprocessed is not None and unprocessed != '':
            return unprocessed

        # filters field with intents in self.intent
        p = fieldnames.IntentFieldnames()
        fields = set()
        vislist = [self.vis] if isinstance(self.vis, str) else self.vis
        for vis in vislist:
            # This assumes the same fields in all MSes
            msobj = self.context.observing_run.get_ms(vis)
            # this will give something like '0542+3243,0343+242'
            intent_fields = p(msobj, self.intent)
            fields.update(utils.safe_split(intent_fields))

        # LOG.info('field.postprocess: fields = "{0}"'.format(fields))

        return ','.join(fields)

    @property
    def antenna(self) -> str:
        return ''

    @property
    def intent(self) -> str:
        return 'TARGET'

    # Synchronization between infiles and vis is still necessary
    @vdp.VisDependentProperty
    def vis(self) -> List[str]:
        return self.infiles

    @property
    def is_ampcal(self) -> bool:
        return self.mode.upper() == 'AMPCAL'

    # TODO: Replace the decorator with @functools.cached_property
    #       when we completely get rid of python 3.6 support
    @property
    @functools.lru_cache(1)
    def datatype(self) -> DataType:
        """Return datatype enum corresponding to dataset returned by vis/infiles attribute."""
        # TODO: It would be ideal to integrate datatype stuff into vdp.
        #       Since this is urgent fix for PIPE-1480, it is better for now
        #       to confine the change into single file to avoid unexpected
        #       side effect.
        _, _datatype = self.context.observing_run.get_measurement_sets_of_type(
            self.processing_data_type, msonly=False
        )
        return _datatype

    def __init__(self, context: 'Context', mode: Optional[str]=None, restfreq: Optional[str]=None,
                 infiles: Optional[List[str]]=None, field: Optional[str]=None, spw: Optional[str]=None,
                 org_direction: Optional['sdtyping.Direction']=None):
        """Initialize an object.

        Args:
            context : Pipeline context
            mode : Spectrum mode. Defaults to None, but in effect to 'line'.
            restfreq : Rest frequency. Defaults to None, it executes without rest frequency.
            infiles : String joined infiles list. Defaults to None.
            field : Field ID. Defaults to None, all fields are used.
            spw : Spectral window. Defaults to None, all spws are used.
            org_direction : Directions of the origin for moving targets.
                            Defaults to None, it doesn't have some moving targets.
        """
        super(SDImagingInputs, self).__init__()

        self.context = context
        self.restfreq = restfreq
        if self.restfreq is None:
            self.restfreq = ''
        self.mode = mode
        self.infiles = infiles
        self.field = field
        self.mode = mode
        self.spw = spw
        self.org_direction = org_direction


@task_registry.set_equivalent_casa_task('hsd_imaging')
@task_registry.set_casa_commands_comment('Perform single dish imaging.')
class SDImaging(basetask.StandardTaskTemplate):
    """SDImaging processing class."""

    Inputs = SDImagingInputs
    # stokes to image and requred POLs for it
    stokes = 'I'
    # for linear feed in ALMA. this affects pols passed to gridding module
    required_pols = ['XX', 'YY']

    is_multi_vis_task = True

    def prepare(self) -> resultobjects.SDImagingResults:
        """Execute imaging process. This is the main method of imaging.

        Returns:
            result object of imaging
        """
        _cp = self.__initialize_common_parameters()

        # loop over reduction group (spw and source combination)
        for __group_id, __group_desc in _cp.reduction_group.items():
            _rgp = imaging_params.ReductionGroupParameters(__group_id, __group_desc)

            if not self.__initialize_reduction_group_parameters(_cp, _rgp):
                continue

            for _rgp.name, _rgp.members in _rgp.image_group.items():
                self.__set_image_group_item_into_reduction_group_patameters(_cp, _rgp)

                # Step 1: initialize weight column
                self.__initialize_weight_column_based_on_baseline_rms(_cp, _rgp)

                # Step 2: imaging
                if not self.__execute_imaging(_cp, _rgp):
                    continue

                if self.__has_imager_result_outcome(_rgp):
                    # Imaging was successful, proceed following steps

                    self.__add_image_list_to_combine(_rgp)

                    # Additional Step.
                    # Make grid_table and put rms and valid spectral number array
                    # to the outcome.
                    # The rms and number of valid spectra is used to create RMS maps.
                    self.__make_grid_table(_cp, _rgp)
                    self.__define_rms_range_in_image(_cp, _rgp)
                    self.__set_asdm_to_outcome_vis_if_imagemode_is_ampcal(_cp, _rgp)

                    # NRO doesn't need per-antenna Stokes I images
                    if _cp.is_not_nro():
                        self.__append_result(_cp, _rgp)

                if self.__has_nro_imager_result_outcome(_rgp):
                    self.__additional_imaging_process_for_nro(_cp, _rgp)

            if self.__skip_this_loop(_rgp):
                continue

            self.__prepare_for_combine_images(_rgp)

            # Step 3: imaging of all antennas
            self.__execute_combine_images(_rgp)

            _pp = imaging_params.PostProcessParameters()
            if self.__has_imager_result_outcome(_rgp):

                # Imaging was successful, proceed following steps

                # Additional Step.
                # Make grid_table and put rms and valid spectral number array
                # to the outcome
                # The rms and number of valid spectra is used to create RMS maps
                self.__make_post_grid_table(_cp, _rgp, _pp)

                # calculate RMS of line free frequencies in a combined image
                try:
                    self.__generate_parameters_for_calculate_sensitivity(_cp, _rgp, _pp)

                    self.__set_representative_flag(_rgp, _pp)

                    self.__warn_if_early_cycle(_rgp)

                    self.__calculate_sensitivity(_cp, _rgp, _pp)
                finally:
                    _pp.done()

                self.__detect_contamination(_rgp)

                self.__append_result(_cp, _rgp)

            # NRO specific: generate combined image for each correlation
            if _cp.is_nro and not self.__execute_combine_images_for_nro(_cp, _rgp, _pp):
                continue

        return _cp.results

    @classmethod
    def _finalize_worker_result(cls,
                                context: 'Context',
                                result: 'SDImagingResults',
                                session: str,
                                sourcename: str,
                                spwlist: List[int],
                                antenna: str,
                                specmode: str,
                                imagemode: str,
                                stokes: str,
                                datatype: DataType,
                                datamin: Optional[float],
                                datamax: Optional[float],
                                datarms: Optional[float],
                                validsp: List[List[int]],
                                rms: List[List[float]],
                                edge: List[int],
                                reduction_group_id: int,
                                file_index: List[int],
                                assoc_antennas: List[int],
                                assoc_fields: List[int],
                                assoc_spws: List[int],
                                sensitivity_info: Optional[SensitivityInfo]=None,
                                theoretical_rms: Optional[Dict]=None,
                                effbw: Optional[float]=None):
        """
        Fanalize the worker result.

        Args:
            context            : Pipeline context
            result             : SDImagingResults instance
            session            : Session name
            sourcename         : Name of the source
            spwlist            : List of SpWs
            antenna            : Antenna name
            specmode           : Specmode for tsdimaging
            imagemode          : Image mode
            stokes             : Stokes parameter
            datatype           : Datatype enum
            datamin            : Minimum value of the image
            datamax            : Maximum value of the image
            datarms            : Rms of the image
            validsp            : # of combined spectra
            rms                : Rms values
            edge               : Edge channels
            reduction_group_id : Reduction group ID
            file_index         : MS file index
            assoc_antennas     : List of associated antennas
            assoc_fields       : List of associated fields
            assoc_spws         : List of associated SpWs
            sensitivity_info   : Sensitivity information
            theoretical_rms    : Theoretical RMS
            effbw              : Effective channel bandwidth in Hz
        Returns:
            (none)
        """
        # override attributes for image item
        # the following attribute is currently hard-coded
        sourcetype = 'TARGET'

        _locals = locals()
        image_keys = ('sourcename', 'spwlist', 'antenna', 'ant_name', 'specmode', 'sourcetype')
        for x in image_keys:
            if x in _locals:
                setattr(result.outcome['image'], x, _locals[x])

        # fill outcomes
        outcome_keys = ('imagemode', 'stokes', 'validsp', 'rms', 'edge', 'reduction_group_id',
                        'file_index', 'assoc_antennas', 'assoc_fields', 'assoc_spws', 'assoc_spws')
        for x in outcome_keys:
            if x in _locals:
                result.outcome[x] = _locals[x]

        # attach sensitivity_info if available
        if sensitivity_info is not None:
            result.sensitivity_info = sensitivity_info
        # attach theoretical RMS if available
        if theoretical_rms is not None:
            result.theoretical_rms = theoretical_rms

        # set some information to image header
        image_item = result.outcome['image']
        imagename = image_item.imagename

        # Virtual spws are not applicable for NRO data.
        # So if is_nro() returns True, the 'virtspw' flag is set to False.
        virtspw = not sdutils.is_nro(context)

        for name in (imagename, imagename + '.weight'):
            imageheader.set_miscinfo(name=name,
                                     spw=','.join(map(str, spwlist)),
                                     virtspw=virtspw,
                                     field=image_item.sourcename,
                                     nfield=1,
                                     datatype=datatype.name,
                                     type='singledish',
                                     iter=1,  # nominal
                                     intent=sourcetype,
                                     specmode=specmode,
                                     is_per_eb=False,
                                     context=context)

            # update miscinfo
            # TODO: Eventually, the following code together with
            #       Tclean.update_miscinfo should be merged into
            #       imageheader.set_miscinfo.
            with casa_tools.ImageReader(name) as image:
                info = image.miscinfo()

                if '.weight' not in name:
                    if datamin:
                        info['datamin'] = datamin

                    if datamax:
                        info['datamax'] = datamax

                    if datarms:
                        info['datarms'] = datarms

                info['stokes'] = stokes

                if effbw:
                    info['effbw'] = effbw

                info['level'] = 'member'
                info['obspatt'] = 'sd'
                info['arrays'] = 'TP'
                info['modifier'] = ''

                # PIPE-2148, limiting 'sessionX' keyword length to 68 characters
                # due to FITS header keyword string length limit.
                info = imageheader.wrap_key(info, 'sessio', session)

                image.setmiscinfo(info)

        # finally replace task attribute with the top-level one
        result.task = cls

    def __get_edge(self) -> List[int]:
        """
        Search results and retrieve edge parameter from the most recent SDBaselineResults if it exists.

        Returns:
            A list of edge
        """
        __getresult = lambda r: r.read() if hasattr(r, 'read') else r
        __registered_results = [__getresult(r) for r in self.inputs.context.results]
        __baseline_stage = -1
        for __stage in range(len(__registered_results) - 1, -1, -1):
            if isinstance(__registered_results[__stage], baseline.SDBaselineResults):
                __baseline_stage = __stage
        if __baseline_stage > 0:
            ret = list(__registered_results[__baseline_stage].outcome['edge'])
            LOG.info('Retrieved edge information from SDBaselineResults: {}'.format(ret))
        else:
            LOG.info('No SDBaselineResults available. Set edge as [0,0]')
            ret = [0, 0]
        return ret

    def __initialize_common_parameters(self) -> imaging_params.CommonParameters:
        """Initialize common parameters of prepare().

        Returns:
            common parameters object of prepare()
        """
        return imaging_params.initialize_common_parameters(
            reduction_group=self.inputs.context.observing_run.ms_reduction_group,
            infiles=self.inputs.infiles,
            restfreq_list=self.inputs.restfreq,
            ms_list=self.inputs.ms,
            ms_names=[msobj.name for msobj in self.inputs.ms],
            session_names=[msobj.session for msobj in self.inputs.ms],
            args_spw=sdutils.convert_spw_virtual2real(self.inputs.context, self.inputs.spw),
            in_field=self.inputs.field,
            imagemode=self.inputs.mode.upper(),
            is_nro=sdutils.is_nro(self.inputs.context),
            results=resultobjects.SDImagingResults(),
            edge=self.__get_edge(),
            dt_dict=dict((__ms.basename, DataTable(sdutils.get_data_table_path(self.inputs.context, __ms)))
                         for __ms in self.inputs.ms)
        )

    def __get_correlations_if_nro(self, _cp: imaging_params.CommonParameters,
                                  _rgp: imaging_params.ReductionGroupParameters) -> Optional[str]:
        """If data is from NRO, then get correlations.

        Args:
            _cp : Common parameters object of prepare()
            _rgp : Reduction group parameter object of prepare()

        Returns:
            joined list of correlations
        """
        if _cp.is_nro:
            __correlations = []
            for c in _rgp.pols_list:
                if c not in __correlations:
                    __correlations.append(c)

            assert len(__correlations) == 1
            return ''.join(__correlations[0])
        else:
            return None

    def __get_rgp_image_group(self, _cp: imaging_params.CommonParameters,
                              _rgp: imaging_params.ReductionGroupParameters) -> Dict[str, List[List[str]]]:
        """Get image group of reduction group.

        Args:
            _cp : Common parameters object of prepare()
            _rgp : Reduction group parameter object of prepare()

        Returns:
            image group dictionary, value is list of [ms, antenna, spwid,
            fieldid, pollist, channelmap]
        """
        __image_group = {}
        for __msobj, __ant, __spwid, __fieldid, __pollist, __chanmap in \
                zip(_cp.ms_list, _rgp.antenna_list, _rgp.spwid_list, _rgp.fieldid_list, _rgp.pols_list,
                    _rgp.channelmap_range_list):
            __identifier = __msobj.fields[__fieldid].name
            __antenna = __msobj.antennas[__ant].name
            __identifier += '.' + __antenna
            # create image per asdm and antenna for ampcal
            if self.inputs.is_ampcal:
                __asdm_name = common.asdm_name_from_ms(__msobj)
                __identifier += '.' + __asdm_name
            if __identifier in __image_group:
                __image_group[__identifier].append([__msobj, __ant, __spwid, __fieldid, __pollist, __chanmap])
            else:
                __image_group[__identifier] = [[__msobj, __ant, __spwid, __fieldid, __pollist, __chanmap]]

        return __image_group

    def __initialize_reduction_group_parameters(self, _cp: imaging_params.CommonParameters,
                                                _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Set default values into the instance of imaging_params.ReductionGroupParameters.

        Note: _cp.ms_list is set in this function.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        LOG.debug('Processing Reduction Group {}'.format(_rgp.group_id))
        LOG.debug('Group Summary:')
        for __group in _rgp.group_desc:
            LOG.debug('\t{}: Antenna {:d} ({}) Spw {:d} Field {:d} ({})'.format(__group.ms.basename, __group.antenna_id,
                                                                                __group.antenna_name, __group.spw_id,
                                                                                __group.field_id, __group.field_name))

        # Which group in group_desc list should be processed
        # fix for CAS-9747
        # There may be the case that observation didn't complete so that some of
        # target fields are missing in MS. In this case, directly pass in_field
        # to get_valid_ms_members causes trouble. As a workaround, ad hoc pre-selection
        # of field name is applied here.
        # 2017/02/23 TN
        __field_sel = ''
        if len(_cp.in_field) == 0:
            # fine, just go ahead
            __field_sel = _cp.in_field
        elif _rgp.group_desc.field_name in [x.strip('"') for x in _cp.in_field.split(',')]:
            # pre-selection of the field name
            __field_sel = _rgp.group_desc.field_name
        else:
            LOG.info('Skip reduction group {:d}'.format(_rgp.group_id))
            return False  # no field name is included in in_field, skip

        _rgp.member_list = list(common.get_valid_ms_members(_rgp.group_desc, _cp.ms_names, self.inputs.antenna,
                                __field_sel, _cp.args_spw))
        LOG.trace('group {}: member_list={}'.format(_rgp.group_id, _rgp.member_list))

        # skip this group if valid member list is empty
        if len(_rgp.member_list) == 0:
            LOG.info('Skip reduction group {:d}'.format(_rgp.group_id))
            return False
        _rgp.member_list.sort()  # list of group_desc IDs to image
        _rgp.antenna_list = [_rgp.group_desc[i].antenna_id for i in _rgp.member_list]
        _rgp.spwid_list = [_rgp.group_desc[i].spw_id for i in _rgp.member_list]
        _cp.ms_list = [_rgp.group_desc[i].ms for i in _rgp.member_list]
        _rgp.fieldid_list = [_rgp.group_desc[i].field_id for i in _rgp.member_list]
        __temp_dd_list = [_cp.ms_list[i].get_data_description(spw=_rgp.spwid_list[i])
                          for i in range(len(_rgp.member_list))]
        _rgp.channelmap_range_list = [_rgp.group_desc[i].channelmap_range for i in _rgp.member_list]
        # this becomes list of list [[poltypes for ms0], [poltypes for ms1], ...]
        #             polids_list = [[ddobj.get_polarization_id(corr) for corr in ddobj.corr_axis \
        #                             if corr in self.required_pols ] for ddobj in temp_dd_list]
        _rgp.pols_list = [[__corr for __corr in __ddobj.corr_axis if
                           __corr in self.required_pols] for __ddobj in __temp_dd_list]

        # NRO specific
        _rgp.correlations = self.__get_correlations_if_nro(_cp, _rgp)

        LOG.debug('Members to be processed:')
        for i in range(len(_rgp.member_list)):
            LOG.debug('\t{}: Antenna {} Spw {} Field {}'.format(_cp.ms_list[i].basename, _rgp.antenna_list[i],
                                                                _rgp.spwid_list[i], _rgp.fieldid_list[i]))

        # image is created per antenna (science) or per asdm and antenna (ampcal)
        _rgp.image_group = self.__get_rgp_image_group(_cp, _rgp)

        LOG.debug('image_group={}'.format(_rgp.image_group))

        _rgp.combined = imaging_params.CombinedImageParameters()
        _rgp.tocombine = imaging_params.ToCombineImageParameters()

        return True

    def __pick_restfreq_from_restfreq_list(self, _cp: imaging_params.CommonParameters,
                                           _rgp: imaging_params.ReductionGroupParameters):
        """Pick restfreq from restfreq_list.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        if isinstance(_cp.restfreq_list, list):
            __v_spwid = self.inputs.context.observing_run.real2virtual_spw_id(_rgp.spwids[0], _rgp.msobjs[0])
            __v_spwid_list = [
                self.inputs.context.observing_run.real2virtual_spw_id(int(i), _rgp.msobjs[0]) for
                i in _cp.args_spw[_rgp.msobjs[0].name].split(',')]
            __v_idx = __v_spwid_list.index(__v_spwid)
            if len(_cp.restfreq_list) > __v_idx:
                _rgp.restfreq = _cp.restfreq_list[__v_idx]
                if _rgp.restfreq is None:
                    _rgp.restfreq = ''
                LOG.info("Picked restfreq = '{}' from {}".format(_rgp.restfreq, _cp.restfreq_list))
            else:
                _rgp.restfreq = ''
                LOG.warning("No restfreq for spw {} in {}. Applying default value.".format(__v_spwid,
                                                                                           _cp.restfreq_list))
        else:
            _rgp.restfreq = _cp.restfreq_list
            LOG.info("Processing with restfreq = {}".format(_rgp.restfreq))

    def __set_image_name_based_on_virtual_spwid(self, _cp: imaging_params.CommonParameters,
                                                _rgp: imaging_params.ReductionGroupParameters):
        """Generate image name based on virtual spw id and set it to RGP.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        __v_spwids_unique = numpy.unique(_rgp.v_spwids)
        assert len(__v_spwids_unique) == 1
        _rgp.imagename = self.get_imagename(_rgp.source_name, __v_spwids_unique, _rgp.ant_name,
                                            _rgp.asdm, specmode=_rgp.specmode)
        LOG.info("Output image name: {}".format(_rgp.imagename))
        _rgp.imagename_nro = None
        if _cp.is_nro:
            _rgp.imagename_nro = self.get_imagename(_rgp.source_name, __v_spwids_unique, _rgp.ant_name, _rgp.asdm,
                                                    stokes=_rgp.correlations, specmode=_rgp.specmode)
            LOG.info("Output image name for NRO: {}".format(_rgp.imagename_nro))

    def __set_image_group_item_into_reduction_group_patameters(self, _cp: imaging_params.CommonParameters,
                                                               _rgp: imaging_params.ReductionGroupParameters):
        """Set values for imaging into RGP.

        This method does (1)get parameters from image group in RGP to do gridding(imaging) and set them into RGP,
        and (2)generate an image name and pick the rest frequency.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        _rgp.msobjs = [x[0] for x in _rgp.members]
        _rgp.antids = [x[1] for x in _rgp.members]
        _rgp.spwids = [x[2] for x in _rgp.members]
        _rgp.fieldids = [x[3] for x in _rgp.members]
        _rgp.polslist = [x[4] for x in _rgp.members]
        _rgp.chanmap_range_list = [x[5] for x in _rgp.members]
        LOG.info("Processing image group: {}".format(_rgp.name))
        for idx in range(len(_rgp.msobjs)):
            LOG.info(
                "\t{}: Antenna {:d} ({}) Spw {} Field {:d} ({})"
                "".
                format(_rgp.msobjs[idx].basename, _rgp.antids[idx], _rgp.msobjs[idx].antennas[_rgp.antids[idx]].name,
                       _rgp.spwids[idx], _rgp.fieldids[idx], _rgp.msobjs[idx].fields[_rgp.fieldids[idx]].name))

        # reference data is first MS
        _rgp.ref_ms = _rgp.msobjs[0]
        _rgp.ant_name = _rgp.ref_ms.antennas[_rgp.antids[0]].name

        # for ampcal
        _rgp.asdm = None
        if self.inputs.is_ampcal:
            _rgp.asdm = common.asdm_name_from_ms(_rgp.ref_ms)

        # source name
        _rgp.source_name = _rgp.group_desc.field_name.replace(' ', '_')

        # specmode
        __ref_field = _rgp.fieldids[0]
        __is_eph_obj = _rgp.ref_ms.get_fields(field_id=__ref_field)[0].source.is_eph_obj
        _rgp.specmode = 'cubesource' if __is_eph_obj else 'cube'

        # filenames for gridding
        _cp.infiles = [__ms.name for __ms in _rgp.msobjs]
        LOG.debug('infiles={}'.format(_cp.infiles))

        # virtual spw ids
        _rgp.v_spwids = [self.inputs.context.observing_run.real2virtual_spw_id(s, m)
                         for s, m in zip(_rgp.spwids, _rgp.msobjs)]

        # image name
        self.__set_image_name_based_on_virtual_spwid(_cp, _rgp)

        # restfreq
        self.__pick_restfreq_from_restfreq_list(_cp, _rgp)

    def __initialize_weight_column_based_on_baseline_rms(self, _cp: imaging_params.CommonParameters,
                                                         _rgp: imaging_params.ReductionGroupParameters):
        """Initialize weight column of MS.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        __origin_ms = [msobj.origin_ms for msobj in _rgp.msobjs]
        __work_ms = [msobj.name for msobj in _rgp.msobjs]
        __weighting_inputs = vdp.InputsContainer(weighting.WeightMS, self.inputs.context,
                                                 infiles=__origin_ms, outfiles=__work_ms,
                                                 antenna=_rgp.antids, spwid=_rgp.spwids, fieldid=_rgp.fieldids)
        __weighting_task = weighting.WeightMS(__weighting_inputs)
        self._executor.execute(__weighting_task, merge=False, datatable_dict=_cp.dt_dict)

    def __initialize_coord_set(self, _cp: imaging_params.CommonParameters,
                               _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Initialize coordinate set of MS.

        if initialize is fault, current loop of reduction group goes to next loop immediately.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        Returns:
            A flag initialize succeeded or not
        """
        # PIPE-313: evaluate map extent using pointing data from all the antenna in the data
        __dummyids = [None for _ in _rgp.antids]
        __image_coord = worker.ImageCoordinateUtil(self.inputs.context, _cp.infiles,
                                                   __dummyids, _rgp.spwids, _rgp.fieldids)
        if not __image_coord:  # No valid data is found
            return False
        _rgp.coord_set = True
        _rgp.phasecenter, _rgp.cellx, _rgp.celly, _rgp.nx, _rgp.ny, _rgp.org_direction = __image_coord
        return True

    def __execute_imaging_worker(self, _cp: imaging_params.CommonParameters,
                                 _rgp: imaging_params.ReductionGroupParameters):
        """Execute imaging worker.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        # register data for combining
        _rgp.combined.extend(_cp, _rgp)
        _rgp.stokes_list = [self.stokes]
        __imagename_list = [_rgp.imagename]
        if _cp.is_nro:
            _rgp.stokes_list.append(_rgp.correlations)
            __imagename_list.append(_rgp.imagename_nro)

        __imager_results = []
        for __stokes, __imagename in zip(_rgp.stokes_list, __imagename_list):
            __imager_inputs = worker.SDImagingWorker.Inputs(self.inputs.context, _cp.infiles,
                                                            outfile=__imagename,
                                                            mode=_cp.imagemode,
                                                            antids=_rgp.antids,
                                                            spwids=_rgp.spwids,
                                                            fieldids=_rgp.fieldids,
                                                            restfreq=_rgp.restfreq,
                                                            stokes=__stokes,
                                                            edge=_cp.edge,
                                                            phasecenter=_rgp.phasecenter,
                                                            cellx=_rgp.cellx,
                                                            celly=_rgp.celly,
                                                            nx=_rgp.nx, ny=_rgp.ny,
                                                            org_direction=_rgp.org_direction)
            __imager_task = worker.SDImagingWorker(__imager_inputs)
            __imager_result = self._executor.execute(__imager_task)
            __imager_results.append(__imager_result)

        # per-antenna image (usually Stokes I)
        _rgp.imager_result = __imager_results[0]
        # per-antenna correlation image (XXYY/RRLL)
        _rgp.imager_result_nro = __imager_results[1] if _cp.is_nro else None

    def __make_grid_table(self, _cp: imaging_params.CommonParameters, _rgp: imaging_params.ReductionGroupParameters):
        """Make grid table for gridding.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        LOG.info('Additional Step. Make grid_table')
        _rgp.imagename = _rgp.imager_result.outcome['image'].imagename
        with casa_tools.ImageReader(_rgp.imagename) as ia:
            __cs = ia.coordsys()
            __dircoords = [i for i in range(__cs.naxes()) if __cs.axiscoordinatetypes()[i] == 'Direction']
            __cs.done()
            _rgp.nx = ia.shape()[__dircoords[0]]
            _rgp.ny = ia.shape()[__dircoords[1]]
        __observing_pattern = _rgp.msobjs[0].observing_pattern[_rgp.antids[0]][_rgp.spwids[0]][_rgp.fieldids[0]]
        __grid_task_class = gridding.gridding_factory(__observing_pattern)
        _rgp.validsps = []
        _rgp.rmss = []
        __grid_input_dict = {}
        for __msobj, __antid, __spwid, __fieldid, __poltypes, _dummy in _rgp.members:
            __msname = __msobj.name  # Use parent ms
            for p in __poltypes:
                if p not in __grid_input_dict:
                    __grid_input_dict[p] = [[__msname], [__antid], [__fieldid], [__spwid]]
                else:
                    __grid_input_dict[p][0].append(__msname)
                    __grid_input_dict[p][1].append(__antid)
                    __grid_input_dict[p][2].append(__fieldid)
                    __grid_input_dict[p][3].append(__spwid)

        # Generate grid table for each POL in image (per ANT,
        # FIELD, and SPW, over all MSes)
        for __pol, __member in __grid_input_dict.items():
            __mses = __member[0]
            __antids = __member[1]
            __fieldids = __member[2]
            __spwids = __member[3]
            __pols = [__pol for i in range(len(__mses))]
            __gridding_inputs = __grid_task_class.Inputs(self.inputs.context, infiles=__mses,
                                                         antennaids=__antids,
                                                         fieldids=__fieldids,
                                                         spwids=__spwids,
                                                         poltypes=__pols,
                                                         nx=_rgp.nx, ny=_rgp.ny)
            __gridding_task = __grid_task_class(__gridding_inputs)
            __gridding_result = self._executor.execute(__gridding_task, merge=False,
                                                       datatable_dict=_cp.dt_dict)
            # Extract RMS and number of spectra from grid_tables
            if isinstance(__gridding_result.outcome, compress.CompressedObj):
                __grid_table = __gridding_result.outcome.decompress()
            else:
                __grid_table = __gridding_result.outcome
            _rgp.validsps.append([r[6] for r in __grid_table])
            _rgp.rmss.append([r[8] for r in __grid_table])

    def __add_image_list_to_combine(self, _rgp: imaging_params.ReductionGroupParameters):
        """Add image list to combine.

        Args:
            _rgp : Reduction group parameter object of prepare()
        """
        if os.path.exists(_rgp.imagename) and os.path.exists(_rgp.imagename + '.weight'):
            _rgp.tocombine.images.append(_rgp.imagename)
            _rgp.tocombine.org_directions.append(_rgp.org_direction)
            _rgp.tocombine.specmodes.append(_rgp.specmode)

    def __define_rms_range_in_image(self, _cp: imaging_params.CommonParameters,
                                    _rgp: imaging_params.ReductionGroupParameters):
        """Define RMS range and finalize worker result.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        __cqa = casa_tools.quanta

        LOG.info("Calculate spectral line and deviation mask frequency ranges in image.")
        with casa_tools.ImageReader(_rgp.imagename) as ia:
            __cs = ia.coordsys()
            __frequency_frame = __cs.getconversiontype('spectral')
            __cs.done()
            __rms_exclude_freq = self._get_rms_exclude_freq_range_image(__frequency_frame, _cp, _rgp)
            LOG.info("The spectral line and deviation mask frequency ranges = {}".format(str(__rms_exclude_freq)))
        _rgp.combined.rms_exclude.extend(__rms_exclude_freq)
        __file_index = [common.get_ms_idx(self.inputs.context, name) for name in _cp.infiles]
        __spwid = str(_rgp.combined.v_spws[REF_MS_ID])
        __spwobj = _rgp.ref_ms.get_spectral_window(__spwid)
        __effective_bw = __cqa.quantity(__spwobj.channels.chan_effbws[0], 'Hz')
        effbw = float(__cqa.getvalue(__effective_bw))
        self._finalize_worker_result(self.inputs.context, _rgp.imager_result, session=','.join(_cp.session_names), sourcename=_rgp.source_name,
                                     spwlist=_rgp.v_spwids, antenna=_rgp.ant_name, specmode=_rgp.specmode,
                                     imagemode=_cp.imagemode, stokes=self.stokes,
                                     datatype=self.inputs.datatype, datamin=None, datamax=None, datarms=None,
                                     validsp=_rgp.validsps,
                                     rms=_rgp.rmss, edge=_cp.edge, reduction_group_id=_rgp.group_id,
                                     file_index=__file_index, assoc_antennas=_rgp.antids, assoc_fields=_rgp.fieldids,
                                     assoc_spws=_rgp.v_spwids, effbw=effbw)

    def __additional_imaging_process_for_nro(self, _cp: imaging_params.CommonParameters,
                                             _rgp: imaging_params.ReductionGroupParameters):
        """Add image list to combine and finalize worker result.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        __cqa = casa_tools.quanta

        # Imaging was successful, proceed following steps
        # add image list to combine
        if os.path.exists(_rgp.imagename_nro) and os.path.exists(_rgp.imagename_nro + '.weight'):
            _rgp.tocombine.images_nro.append(_rgp.imagename_nro)
            _rgp.tocombine.org_directions_nro.append(_rgp.org_direction)
            _rgp.tocombine.specmodes.append(_rgp.specmode)
        __file_index = [common.get_ms_idx(self.inputs.context, name) for name in _cp.infiles]
        __spwid = str(_rgp.combined.v_spws[REF_MS_ID])
        __spwobj = _rgp.ref_ms.get_spectral_window(__spwid)
        __effective_bw = __cqa.quantity(__spwobj.channels.chan_effbws[0], 'Hz')
        effbw = float(__cqa.getvalue(__effective_bw))
        self._finalize_worker_result(self.inputs.context, _rgp.imager_result_nro, session=','.join(_cp.session_names), sourcename=_rgp.source_name,
                                     spwlist=_rgp.v_spwids, antenna=_rgp.ant_name, specmode=_rgp.specmode,
                                     imagemode=_cp.imagemode, stokes=_rgp.stokes_list[1],
                                     datatype=self.inputs.datatype, datamin=None, datamax=None, datarms=None,
                                     validsp=_rgp.validsps,
                                     rms=_rgp.rmss, edge=_cp.edge, reduction_group_id=_rgp.group_id,
                                     file_index=__file_index, assoc_antennas=_rgp.antids, assoc_fields=_rgp.fieldids,
                                     assoc_spws=_rgp.v_spwids, effbw=effbw)
        _cp.results.append(_rgp.imager_result_nro)

    def __make_post_grid_table(self, _cp: imaging_params.CommonParameters,
                               _rgp: imaging_params.ReductionGroupParameters,
                               _pp: imaging_params.PostProcessParameters):
        """Make grid table on post process.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()
        """
        LOG.info('Additional Step. Make grid_table')
        _pp.imagename = _rgp.imager_result.outcome['image'].imagename
        _pp.org_direction = _rgp.imager_result.outcome['image'].org_direction
        with casa_tools.ImageReader(_pp.imagename) as ia:
            __cs = ia.coordsys()
            __dircoords = [i for i in range(__cs.naxes()) if __cs.axiscoordinatetypes()[i] == 'Direction']
            __cs.done()
            _pp.nx = ia.shape()[__dircoords[0]]
            _pp.ny = ia.shape()[__dircoords[1]]
        __antid = _rgp.combined.antids[REF_MS_ID]
        __spwid = _rgp.combined.spws[REF_MS_ID]
        __fieldid = _rgp.combined.fieldids[REF_MS_ID]
        __observing_pattern = _rgp.ref_ms.observing_pattern[__antid][__spwid][__fieldid]
        __grid_task_class = gridding.gridding_factory(__observing_pattern)
        _pp.validsps = []
        _pp.rmss = []
        __grid_input_dict = {}
        for __msname, __antid, __spwid, __fieldid, __poltypes in zip(_rgp.combined.infiles,
                                                                     _rgp.combined.antids,
                                                                     _rgp.combined.spws,
                                                                     _rgp.combined.fieldids,
                                                                     _rgp.combined.pols):
            for p in __poltypes:
                if p not in __grid_input_dict:
                    __grid_input_dict[p] = [[__msname], [__antid], [__fieldid], [__spwid]]
                else:
                    __grid_input_dict[p][0].append(__msname)
                    __grid_input_dict[p][1].append(__antid)
                    __grid_input_dict[p][2].append(__fieldid)
                    __grid_input_dict[p][3].append(__spwid)

        for __pol, __member in __grid_input_dict.items():
            __mses = __member[0]
            __antids = __member[1]
            __fieldids = __member[2]
            __spwids = __member[3]
            __pols = [__pol for i in range(len(__mses))]
            __gridding_inputs = __grid_task_class.Inputs(self.inputs.context, infiles=__mses, antennaids=__antids,
                                                         fieldids=__fieldids, spwids=__spwids, poltypes=__pols,
                                                         nx=_pp.nx, ny=_pp.ny)
            __gridding_task = __grid_task_class(__gridding_inputs)
            __gridding_result = self._executor.execute(__gridding_task, merge=False, datatable_dict=_cp.dt_dict)
            # Extract RMS and number of spectra from grid_tables
            if isinstance(__gridding_result.outcome, compress.CompressedObj):
                __grid_table = __gridding_result.outcome.decompress()
            else:
                __grid_table = __gridding_result.outcome
            _pp.validsps.append([r[6] for r in __grid_table])
            _pp.rmss.append([r[8] for r in __grid_table])

    def __generate_parameters_for_calculate_sensitivity(self, _cp: imaging_params.CommonParameters,
                                                        _rgp: imaging_params.ReductionGroupParameters,
                                                        _pp: imaging_params.PostProcessParameters):
        """Generate parameters to calculate sensitivity.

        Note: If it fails to calculate image statistics for some reason, it sets the RMS value to -1.0.
              -1.0 is the special value in image statistics calculation of all tasks.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()
        """
        LOG.info('Calculate sensitivity of combined image')
        with casa_tools.ImageReader(_pp.imagename) as ia:
            _pp.cs = ia.coordsys()
            _pp.faxis = _pp.cs.findaxisbyname('spectral')
            _pp.chan_width = _pp.cs.increment()['numeric'][_pp.faxis]
            _pp.brightnessunit = ia.brightnessunit()
            _pp.beam = ia.restoringbeam()
        _pp.qcell = list(_pp.cs.increment(format='q', type='direction')['quantity'].values())
        # cs.increment(format='s', type='direction')['string']

        # Define image channels to calculate statistics
        _pp.include_channel_range = self._get_stat_chans(_pp.imagename, _rgp.combined.rms_exclude, _cp.edge)
        _pp.stat_chans = convert_range_list_to_string(_pp.include_channel_range)
        # Define region to calculate statistics
        _pp.raster_infos = self.get_raster_info_list(_cp, _rgp)
        _pp.region = self._get_stat_region(_pp)

        # Image statistics
        if _pp.region is None:
            LOG.warning('Could not get valid region of interest to calculate image statistics.')
            _pp.image_rms = -1.0
        else:
            __statval = calc_image_statistics(_pp.imagename, _pp.stat_chans, _pp.region)
            if len(__statval['rms']):
                _pp.image_rms = __statval['rms'][0]
                LOG.info("Statistics of line free channels ({}): RMS = {:f} {}, Stddev = {:f} {}, "
                         "Mean = {:f} {}".format(_pp.stat_chans, __statval['rms'][0], _pp.brightnessunit,
                                                 __statval['sigma'][0], _pp.brightnessunit,
                                                 __statval['mean'][0], _pp.brightnessunit))
            else:
                LOG.warning('Could not get image statistics. Potentially no valid pixel in region of interest.')
                _pp.image_rms = -1.0

            for __stat_name in ['max', 'min']:
                __val = __statval.get(__stat_name, [])
                setattr(_pp, f'image_{__stat_name}', __val[0] if __val else -1.0)

        # Theoretical RMS
        LOG.info('Calculating theoretical RMS of image, {}'.format(_pp.imagename))
        _pp.theoretical_rms = self.calculate_theoretical_image_rms(_cp, _rgp, _pp)

    def __execute_combine_images(self, _rgp: imaging_params.ReductionGroupParameters):
        """Combine images.

        Args:
            _rgp : Reduction group parameter object of prepare()
        """
        LOG.info('Combine images of Source {} Spw {:d}'.format(_rgp.source_name, _rgp.combined.v_spws[REF_MS_ID]))
        __combine_inputs = sdcombine.SDImageCombineInputs(self.inputs.context, inimages=_rgp.tocombine.images,
                                                          outfile=_rgp.imagename,
                                                          org_directions=_rgp.tocombine.org_directions,
                                                          specmodes=_rgp.tocombine.specmodes)
        __combine_task = sdcombine.SDImageCombine(__combine_inputs)
        __freq_chan_reversed = False
        if isinstance(_rgp.imager_result, resultobjects.SDImagingResultItem):
            __freq_chan_reversed = _rgp.imager_result.frequency_channel_reversed
        _rgp.imager_result = self._executor.execute(__combine_task)
        _rgp.imager_result.frequency_channel_reversed = __freq_chan_reversed

    def __set_representative_flag(self,
                                  _rgp: imaging_params.ReductionGroupParameters,
                                  _pp: imaging_params.PostProcessParameters):
        """Set is_representative_source_and_spw flag.

        Args:
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()
        """
        __rep_source_name, __rep_spw_id = _rgp.ref_ms.get_representative_source_spw()
        _pp.is_representative_source_and_spw = \
            __rep_spw_id == _rgp.combined.spws[REF_MS_ID] and \
            __rep_source_name == utils.dequote(_rgp.source_name)

    def __warn_if_early_cycle(self, _rgp: imaging_params.ReductionGroupParameters):
        """Warn when it processes MeasurementSet of ALMA Cycle 2 and earlier.

        Args:
            _rgp (imaging_params.ReductionGroupParameters): Reduction group parameter object of prepare()
        """
        __cqa = casa_tools.quanta
        if _rgp.ref_ms.antenna_array.name == 'ALMA' and \
           __cqa.time(_rgp.ref_ms.start_time['m0'], 0, ['ymd', 'no_time'])[0] < '2015/10/01':
            LOG.warning("ALMA Cycle 2 and earlier project does not have a valid effective bandwidth. "
                        "Therefore, a nominal value of channel separation loaded from the MS "
                        "is used as an effective bandwidth for RMS estimation.")

    def __calculate_sensitivity(self, _cp: imaging_params.CommonParameters,
                                _rgp: imaging_params.ReductionGroupParameters,
                                _pp: imaging_params.PostProcessParameters):
        """Calculate channel and frequency ranges of line free channels.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()
        """
        __ref_pixel = _pp.cs.referencepixel()['numeric']
        __freqs = []
        __cqa = casa_tools.quanta

        for __ichan in _pp.include_channel_range:
            __ref_pixel[_pp.faxis] = __ichan
            __freqs.append(_pp.cs.toworld(__ref_pixel)['numeric'][_pp.faxis])

        if len(__freqs) > 1 and __freqs[0] > __freqs[1]:  # LSB
            __freqs.reverse()
        _pp.stat_freqs = str(', ').join(['{:f}~{:f}GHz'.format(__freqs[__iseg] * 1.e-9, __freqs[__iseg + 1] * 1.e-9)
                                        for __iseg in range(0, len(__freqs), 2)])
        __file_index = [common.get_ms_idx(self.inputs.context, name) for name in _rgp.combined.infiles]
        __bw = __cqa.quantity(_pp.chan_width, 'Hz')
        __spwid = str(_rgp.combined.v_spws[REF_MS_ID])
        __spwobj = _rgp.ref_ms.get_spectral_window(__spwid)
        __effective_bw = __cqa.quantity(__spwobj.channels.chan_effbws[0], 'Hz')
        __sensitivity = Sensitivity(array='TP', intent='TARGET', field=_rgp.source_name,
                                    spw=__spwid, is_representative=_pp.is_representative_source_and_spw,
                                    bandwidth=__bw, bwmode='cube', beam=_pp.beam, cell=_pp.qcell,
                                    sensitivity=__cqa.quantity(_pp.image_rms, _pp.brightnessunit),
                                    effective_bw=__effective_bw, imagename=_rgp.imagename,
                                    datatype=self.inputs.datatype.name)
        __theoretical_noise = Sensitivity(array='TP', intent='TARGET', field=_rgp.source_name,
                                          spw=__spwid, is_representative=_pp.is_representative_source_and_spw,
                                          bandwidth=__bw, bwmode='cube', beam=_pp.beam, cell=_pp.qcell,
                                          sensitivity=_pp.theoretical_rms)
        __sensitivity_info = SensitivityInfo(__sensitivity, _pp.stat_freqs, (_cp.is_not_nro()))
        effbw = float(__cqa.getvalue(__effective_bw))
        self._finalize_worker_result(self.inputs.context, _rgp.imager_result, session=','.join(_cp.session_names), sourcename=_rgp.source_name,
                                     spwlist=_rgp.combined.v_spws, antenna='COMBINED', specmode=_rgp.specmode,
                                     imagemode=_cp.imagemode, stokes=self.stokes,
                                     datatype=self.inputs.datatype, datamin=_pp.image_min, datamax=_pp.image_max,
                                     datarms=_pp.image_rms, validsp=_pp.validsps, rms=_pp.rmss,
                                     edge=_cp.edge, reduction_group_id=_rgp.group_id, file_index=__file_index,
                                     assoc_antennas=_rgp.combined.antids, assoc_fields=_rgp.combined.fieldids,
                                     assoc_spws=_rgp.combined.v_spws, sensitivity_info=__sensitivity_info,
                                     theoretical_rms=__theoretical_noise, effbw=effbw)

    def __execute_combine_images_for_nro(self, _cp: imaging_params.CommonParameters,
                                         _rgp: imaging_params.ReductionGroupParameters,
                                         _pp: imaging_params.PostProcessParameters) -> bool:
        """Combine images for NRO data.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()
        Returns:
            False if a valid image to combine does not exist for a specified source or spw.
        """
        __cqa = casa_tools.quanta

        if len(_rgp.tocombine.images_nro) == 0:
            LOG.warning("No valid image to combine for Source {}, Spw {:d}".format(_rgp.source_name, _rgp.spwids[0]))
            return False
        # image name
        # image name should be based on virtual spw id
        _pp.imagename = self.get_imagename(_rgp.source_name, _rgp.combined.v_spws_unique,
                                           stokes=_rgp.correlations, specmode=_rgp.specmode)

        # Imaging of all antennas
        LOG.info('Combine images of Source {} Spw {:d}'.format(_rgp.source_name, _rgp.combined.v_spws[REF_MS_ID]))
        __combine_inputs = sdcombine.SDImageCombineInputs(self.inputs.context, inimages=_rgp.tocombine.images_nro,
                                                          outfile=_pp.imagename,
                                                          org_directions=_rgp.tocombine.org_directions_nro,
                                                          specmodes=_rgp.tocombine.specmodes)
        __combine_task = sdcombine.SDImageCombine(__combine_inputs)
        _rgp.imager_result = self._executor.execute(__combine_task)
        if _rgp.imager_result.outcome is not None:
            # Imaging was successful, proceed following steps
            __file_index = [common.get_ms_idx(self.inputs.context, name) for name in _rgp.combined.infiles]
            __spwid = str(_rgp.combined.v_spws[REF_MS_ID])
            __spwobj = _rgp.ref_ms.get_spectral_window(__spwid)
            __effective_bw = __cqa.quantity(__spwobj.channels.chan_effbws[0], 'Hz')
            effbw = float(__cqa.getvalue(__effective_bw))
            self._finalize_worker_result(self.inputs.context, _rgp.imager_result, session=','.join(_cp.session_names), sourcename=_rgp.source_name,
                                         spwlist=_rgp.combined.v_spws, antenna='COMBINED', specmode=_rgp.specmode,
                                         imagemode=_cp.imagemode, stokes=_rgp.stokes_list[1],
                                         datatype=self.inputs.datatype, datamin=_pp.image_min, datamax=_pp.image_max,
                                         datarms=_pp.image_rms, validsp=_pp.validsps,
                                         rms=_pp.rmss, edge=_cp.edge, reduction_group_id=_rgp.group_id,
                                         file_index=__file_index, assoc_antennas=_rgp.combined.antids,
                                         assoc_fields=_rgp.combined.fieldids, assoc_spws=_rgp.combined.v_spws, effbw=effbw)
            _cp.results.append(_rgp.imager_result)
        return True

    def __prepare_for_combine_images(self, _rgp: imaging_params.ReductionGroupParameters):
        """Prepare before combining images.

        Args:
            _rgp : Reduction group parameter object of prepare()
        """
        # reference MS
        _rgp.ref_ms = self.inputs.context.observing_run.get_ms(name=_rgp.combined.infiles[REF_MS_ID])
        # image name
        # image name should be based on virtual spw id
        _rgp.combined.v_spws_unique = numpy.unique(_rgp.combined.v_spws)
        assert len(_rgp.combined.v_spws_unique) == 1
        _rgp.imagename = self.get_imagename(_rgp.source_name, _rgp.combined.v_spws_unique, specmode=_rgp.specmode)

    def __skip_this_loop(self, _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Determine whether combine should be skipped.

        Args:
            _rgp : Reduction group parameter object of prepare()

        Returns:
            A boolean flag of determination whether the loop should be skipped or not.
        """
        if self.inputs.is_ampcal:
            LOG.info("Skipping combined image for the amplitude calibrator.")
            return True
        # Make combined image
        if len(_rgp.tocombine.images) == 0:
            LOG.warning("No valid image to combine for Source {}, Spw {:d}".format(_rgp.source_name, _rgp.spwids[0]))
            return True
        return False

    def __execute_imaging(self, _cp: imaging_params.CommonParameters,
                          _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Execute imaging per antenna, source.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        Returns:
            False if coordinate setting fails before imaging is executed.
        """
        LOG.info('Imaging Source {}, Ant {} Spw {:d}'.format(_rgp.source_name, _rgp.ant_name, _rgp.spwids[0]))
        # map coordinate (use identical map coordinate per spw)
        if not _rgp.coord_set and not self.__initialize_coord_set(_cp, _rgp):
            return False
        self.__execute_imaging_worker(_cp, _rgp)
        return True

    def __set_asdm_to_outcome_vis_if_imagemode_is_ampcal(self, _cp: imaging_params.CommonParameters,
                                                         _rgp: imaging_params.ReductionGroupParameters):
        """Set ASDM to vis.outcome if imagemode of the vis is AMPCAL.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
        """
        if self.inputs.is_ampcal:
            if len(_cp.infiles) == 1 and (_rgp.asdm not in ['', None]):
                _rgp.imager_result.outcome['vis'] = _rgp.asdm

    def __has_imager_result_outcome(self, _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Check whether imager_result.outcome has some value or not.

        Args:
            _rgp : Reduction group parameter object of prepare()
        Returns:
            True if imager_result.outcome has some value.
        """
        return _rgp.imager_result.outcome is not None

    def __has_nro_imager_result_outcome(self, _rgp: imaging_params.ReductionGroupParameters) -> bool:
        """Check whether imager_result_nro.outcome has some value or not.

        Args:
            _rgp : Reduction group parameter object of prepare()
        Returns:
            True if imager_result_nro.outcome has some value.
        """
        return _rgp.imager_result_nro is not None and _rgp.imager_result_nro.outcome is not None

    def __detect_contamination(self, _rgp: imaging_params.ReductionGroupParameters):
        """Detect contamination of image.

        Args:
            _rgp : Reduction group parameter object of prepare()
        """
        # PIPE-251: detect contamination
        do_plot = not basetask.DISABLE_WEBLOG
        contaminated = detectcontamination.detect_contamination(
            self.inputs.context, _rgp.imager_result.outcome['image'],
            _rgp.imager_result.frequency_channel_reversed,
            do_plot
        )
        _rgp.imager_result.outcome['contaminated'] = contaminated

    def __append_result(self, _cp: imaging_params.CommonParameters, _rgp: imaging_params.ReductionGroupParameters):
        """Append result to RGP.

        Args:
            _rgp : Reduction group parameter object of prepare()
        """
        _cp.results.append(_rgp.imager_result)

    def analyse(self, result: 'SDImagingResults') -> 'SDImagingResults':
        """Override method of basetask.

        Args:
            result : Result object

        Returns:
            Result object
        """
        return result

    def _get_rms_exclude_freq_range_image(self, to_frame: str, _cp: imaging_params.CommonParameters,
                                          _rgp: imaging_params.ReductionGroupParameters) -> List[Tuple[Number, Number]]:
        """
        Return a combined list of frequency ranges.

        This method combines deviation mask, channel map ranges, and edges.

        Args
            to_frame : The frequency frame of output
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()

        Returns:
            a list of combined frequency ranges in output frequency frame (to_frame),
            e.g., [ [minfreq0,maxfreq0], [minfreq1,maxfreq1], ...]
        """
        image_rms_freq_range = []
        channelmap_range = []
        # LOG.info("#####Raw chanmap_range={}".format(str(_rgp.chanmap_range_list)))
        for chanmap_range in _rgp.chanmap_range_list:
            for map_range in chanmap_range:
                if map_range[2]:
                    min_chan = int(map_range[0] - map_range[1] * 0.5)
                    max_chan = int(numpy.ceil(map_range[0] + map_range[1] * 0.5))
                    channelmap_range.append([min_chan, max_chan])
        LOG.debug("#####CHANNEL MAP RANGE = {}".format(str(channelmap_range)))
        for i in range(len(_rgp.msobjs)):
            # define channel ranges of lines and deviation mask for each MS
            msobj = _rgp.msobjs[i]
            fieldid = _rgp.fieldids[i]
            antid = _rgp.antids[i]
            spwid = _rgp.spwids[i]
            spwobj = msobj.get_spectral_window(spwid)
            deviation_mask = getattr(msobj, 'deviation_mask', {})
            exclude_range = deviation_mask.get((fieldid, antid, spwid), [])
            LOG.debug("#####{} : DEVIATION MASK = {}".format(msobj.basename, str(exclude_range)))
            if len(exclude_range) == 1 and exclude_range[0] == [0, spwobj.num_channels - 1]:
                # deviation mask is full channel range when all data are flagged
                LOG.warning("Ignoring DEVIATION MASK of {} (SPW {:d}, FIELD {:d}, ANT {:d}). "
                            "Possibly all data flagged".format(msobj.basename, spwid, fieldid, antid))
                exclude_range = []
            if _cp.edge[0] > 0:
                exclude_range.append([0, _cp.edge[0] - 1])
            if _cp.edge[1] > 0:
                exclude_range.append([spwobj.num_channels - _cp.edge[1], spwobj.num_channels - 1])
            if len(channelmap_range) > 0:
                exclude_range.extend(channelmap_range)
            # check the validity of channel number and fix it when out of range
            min_chan = 0
            max_chan = spwobj.num_channels - 1
            exclude_channel_range = [[max(min_chan, x[0]), min(max_chan, x[1])]
                                     for x in merge_ranges(exclude_range)]
            LOG.info("{} : channel map and deviation mask channel ranges "
                     "in MS frame = {}".format(msobj.basename, str(exclude_channel_range)))
            # define frequency ranges of RMS
            exclude_freq_range = numpy.zeros(2 * len(exclude_channel_range))
            for jseg in range(len(exclude_channel_range)):
                (lfreq, rfreq) = (spwobj.channels.chan_freqs[jchan] for jchan in exclude_channel_range[jseg])
                # handling of LSB
                exclude_freq_range[2 * jseg: 2 * jseg + 2] = [min(lfreq, rfreq), max(lfreq, rfreq)]
            LOG.debug("#####CHANNEL MAP AND DEVIATION MASK FREQ RANGE = {}".format(str(exclude_freq_range)))
            if len(exclude_freq_range) == 0:
                continue  # no ranges to add
            # convert MS freqency ranges to image frame
            field = msobj.fields[fieldid]
            direction_ref = field.mdirection
            start_time = msobj.start_time
            end_time = msobj.end_time
            me = casa_tools.measures
            qa = casa_tools.quanta
            qmid_time = qa.quantity(start_time['m0'])
            qmid_time = qa.add(qmid_time, end_time['m0'])
            qmid_time = qa.div(qmid_time, 2.0)
            time_ref = me.epoch(rf=start_time['refer'],
                                v0=qmid_time)
            position_ref = msobj.antennas[antid].position

            if to_frame == 'REST':
                mse = casa_tools.ms
                mse.open(msobj.name)
                obstime = qa.time(qmid_time, form='ymd')[0]
                v_to = mse.cvelfreqs(spwids=[spwid], obstime=obstime, outframe='SOURCE')
                v_from = mse.cvelfreqs(spwids=[spwid], obstime=obstime, outframe=spwobj.frame)
                mse.close()
                _to_imageframe = interpolate.interp1d(v_from, v_to, kind='linear',
                                                      bounds_error=False, fill_value='extrapolate')
            else:
                # initialize
                me.done()
                me.doframe(time_ref)
                me.doframe(direction_ref)
                me.doframe(position_ref)

                def _to_imageframe(x):
                    m = me.frequency(rf=spwobj.frame, v0=qa.quantity(x, 'Hz'))
                    converted = me.measure(v=m, rf=to_frame)
                    qout = qa.convert(converted['m0'], outunit='Hz')
                    return qout['value']

            image_rms_freq_range.extend(map(_to_imageframe, exclude_freq_range))
            me.done()

        # LOG.info("#####Overall LINE CHANNELS IN IMAGE FRAME = {}".format(str(image_rms_freq_range)))
        if len(image_rms_freq_range) == 0:
            return image_rms_freq_range

        return merge_ranges(numpy.reshape(image_rms_freq_range, (len(image_rms_freq_range) // 2, 2), 'C'))

    def get_imagename(self, source: str, spwids: List[int],
                      antenna: str=None, asdm: str=None, stokes: str=None, specmode: str='cube') -> str:
        """Generate a filename of the image.

        Args:
            source : Source name
            spwids : SpW IDs
            antenna : Antenna name. Defaults to None.
            asdm : ASDM. Defaults to None.
            stokes : Stokes parameter. Defaults to None.
            specmode : specmode for tsdimaging. Defaults to 'cube'.

        Raises:
            ValueError: if asdm is not provided for ampcal

        Returns:
            A filename of the image
        """
        context = self.inputs.context
        is_nro = sdutils.is_nro(context)
        if is_nro:
            namer = filenamer.Image(virtspw=False)
        else:
            namer = filenamer.Image()
        if self.inputs.is_ampcal:
            nameroot = asdm
            if nameroot is None:
                raise ValueError('ASDM uid must be provided to construct ampcal image name')
        elif is_nro:
            nameroot = ''
        else:
            nameroot = context.project_structure.ousstatus_entity_id
            if nameroot == 'unknown':
                nameroot = 'oussid'
        nameroot = filenamer.sanitize(nameroot)
        namer._associations.asdm(nameroot)
        # output_dir = context.output_dir
        # if output_dir:
        #    namer.output_dir(output_dir)
        if not is_nro:
            namer.stage(context.stage)

        namer.source(source)
        if self.inputs.is_ampcal:
            namer.intent(self.inputs.mode.lower())
        elif is_nro:
            pass
        else:
            namer.science()
        namer.spectral_window(spwids[0])
        if stokes is None:
            stokes = self.stokes
        namer.polarization(stokes)
        namer.specmode(specmode)
        # so far we always create native resolution, full channel image
        # namer.spectral_image()
        namer._associations.format('image.sd')
        # namer.single_dish()
        namer.antenna(antenna)
        # iteration is necessary for exportdata
        namer.iteration(0)
        imagename = namer.get_filename()
        return imagename

    def _get_stat_chans(self, imagename: str,
                        combined_rms_exclude: List[Tuple[float, float]],
                        edge: Tuple[int, int]=(0, 0)) -> List[int]:
        """Return a list of channel ranges to calculate image statistics.

        Args:
            imagename : A filename of the image
            combined_rms_exclude : A list of frequency ranges to exclude
            edge : The left and right edge channels to exclude. Defaults to (0, 0).
        Retruns:
            A 1-d list of channel ranges to INCLUDE in calculation of image
            statistics, e.g., [imin0, imax0, imin0, imax0, ...]
        """
        with casa_tools.ImageReader(imagename) as ia:
            try:
                cs = ia.coordsys()
                faxis = cs.findaxisbyname('spectral')
                num_chan = ia.shape()[faxis]
                exclude_chan_ranges = convert_frequency_ranges_to_channels(combined_rms_exclude, cs, num_chan)
            finally:
                cs.done()
        LOG.info("Merged spectral line channel ranges of combined image = {}".format(str(exclude_chan_ranges)))
        include_chan_ranges = invert_ranges(exclude_chan_ranges, num_chan, edge)
        LOG.info("Line free channel ranges of image to calculate RMS = {}".format(str(include_chan_ranges)))
        return include_chan_ranges

    def _get_stat_region(self, _pp: imaging_params.PostProcessParameters) -> Optional[str]:
        """
        Retrun region to calculate statistics.

        Median width, height, and position angle is adopted as a reference
        map extent and then the width and height will be shrinked by 2 beam
        size in each direction.

        Arg:
            _pp : Imaging post process parameters of prepare()

        Retruns:
            Region expression string of a rotating box.
            Returns None if no valid region of interest is defined.
        """
        cqa = casa_tools.quanta
        beam_unit = cqa.getunit(_pp.beam['major'])
        assert cqa.getunit(_pp.beam['minor']) == beam_unit
        beam_size = numpy.sqrt(cqa.getvalue(_pp.beam['major']) * cqa.getvalue(_pp.beam['minor']))[0]
        center_unit = 'deg'
        angle_unit = None
        for r in _pp.raster_infos:
            if r is None:
                continue
            angle_unit = cqa.getunit(r.scan_angle)
            break
        if angle_unit is None:
            LOG.warning('No valid raster information available.')
            return None

        def __value_in_unit(quantity: dict, unit: str) -> float:
            # Get value(s) of quantity in a specified unit
            return cqa.getvalue(cqa.convert(quantity, unit))

        def __extract_values(value: str, unit: str) -> Number:
            # Extract valid values of specified attributes in list
            return [__value_in_unit(getattr(r, value), unit) for r in _pp.raster_infos if r is not None]

        rep_width = numpy.nanmedian(__extract_values('width', beam_unit))
        rep_height = numpy.nanmedian(__extract_values('height', beam_unit))
        rep_angle = numpy.nanmedian([cqa.getvalue(r.scan_angle) for r in _pp.raster_infos if r is not None])
        center_ra = numpy.nanmedian(__extract_values('center_ra', center_unit))
        center_dec = numpy.nanmedian(__extract_values('center_dec', center_unit))
        width = rep_width - beam_size
        height = rep_height - beam_size
        if width <= 0 or height <= 0:  # No valid region selected.
            return None
        if _pp.org_direction is not None:
            (center_ra, center_dec) = direction_utils.direction_recover(center_ra,
                                                                        center_dec,
                                                                        _pp.org_direction)
        center = [cqa.tos(cqa.quantity(center_ra, center_unit)),
                  cqa.tos(cqa.quantity(center_dec, center_unit))]

        region = "rotbox[{}, [{}{}, {}{}], {}{}]".format(center,
                                                         width, beam_unit,
                                                         height, beam_unit,
                                                         rep_angle, angle_unit)
        return region

    def get_raster_info_list(self, _cp: imaging_params.CommonParameters,
                             _rgp: imaging_params.ReductionGroupParameters) -> List[RasterInfo]:
        """
        Retrun a list of raster information.

        Each raster infromation is analyzed for element wise combination of
        infile, antenna, field, and SpW IDs in input parameter lists.

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()

        Returns:
            A list of RasterInfo. If raster information could not be obtained,
            the corresponding elements in the list will be None.
        """
        assert len(_rgp.combined.infiles) == len(_rgp.combined.antids)
        assert len(_rgp.combined.infiles) == len(_rgp.combined.fieldids)
        assert len(_rgp.combined.infiles) == len(_rgp.combined.spws)
        raster_info_list = []
        for (infile, antid, fieldid, spwid) in \
                zip(_rgp.combined.infiles, _rgp.combined.antids, _rgp.combined.fieldids, _rgp.combined.spws):
            msobj = self.inputs.context.observing_run.get_ms(name=infile)
            # Non raster data set.
            if msobj.observing_pattern[antid][spwid][fieldid] != 'RASTER':
                f = msobj.get_fields(field_id=fieldid)[0]
                LOG.warning('Not a raster map: field {} in {}'.format(f.name, msobj.basename))
                raster_info_list.append(None)
            dt = _cp.dt_dict[msobj.basename]
            try:
                raster_info_list.append(_analyze_raster_pattern(dt, msobj, fieldid, spwid, antid, _rgp))
            except Exception:
                f = msobj.get_fields(field_id=fieldid)[0]
                a = msobj.get_antenna(antid)[0]
                LOG.info('Could not get raster information of field {}, Spw {}, Ant {}, MS {}. '
                         'Potentially be because all data are flagged.'.format(f.name, spwid, a.name, msobj.basename))
                raster_info_list.append(None)
        assert len(_rgp.combined.infiles) == len(raster_info_list)
        return raster_info_list

    def calculate_theoretical_image_rms(self, _cp: imaging_params.CommonParameters,
                                        _rgp: imaging_params.ReductionGroupParameters,
                                        _pp: imaging_params.PostProcessParameters) -> Dict[str, float]:
        """Calculate theoretical RMS of an image (PIPE-657).

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _pp : Imaging post process parameters of prepare()

        Note: the number of elements in _rgp.combined.antids, fieldids, spws, and pols should be equal
              to that of infiles

        Returns:
            A quantum value of theoretical image RMS.
            The value of quantity will be negative when calculation is aborted, i.e., -1.0 Jy/beam
        """
        _tirp = imaging_params.TheoreticalImageRmsParameters(_pp, self.inputs.context)

        if len(_rgp.combined.infiles) == 0:
            LOG.error('No MS given to calculate a theoretical RMS. Aborting calculation of theoretical thermal noise.')
            return _tirp.failed_rms
        assert len(_rgp.combined.infiles) == len(_rgp.combined.antids)
        assert len(_rgp.combined.infiles) == len(_rgp.combined.fieldids)
        assert len(_rgp.combined.infiles) == len(_rgp.combined.spws)
        assert len(_rgp.combined.infiles) == len(_rgp.combined.pols)
        assert len(_rgp.combined.infiles) == len(_pp.raster_infos)

        for (_tirp.infile, _tirp.antid, _tirp.fieldid, _tirp.spwid, _tirp.pol_names, _tirp.raster_info) in \
            zip(_rgp.combined.infiles, _rgp.combined.antids, _rgp.combined.fieldids,
                _rgp.combined.spws, _rgp.combined.pols, _pp.raster_infos):
            halt, skip = self.__loop_initializer_of_theoretical_image_rms(_cp, _rgp, _tirp)
            if halt:
                return _tirp.failed_rms
            if skip:
                continue

            # effective BW
            self.__obtain_effective_BW(_tirp)
            # obtain average Tsys
            self.__obtain_average_tsys(_tirp)
            # obtain Wx, and Wy
            self.__obtain_wx_and_wy(_tirp)
            # obtain T_ON
            self.__obtain_t_on_actual(_tirp)
            # obtain calibration tables applied
            self.__obtain_calibration_tables_applied(_tirp)
            # obtain T_sub,on, T_sub,off
            if not self.__obtain_t_sub_on_off(_tirp):
                return _tirp.failed_rms
            # obtain factors by convolution function
            # (THIS ASSUMES SF kernel with either convsupport = 6 (ALMA) or 3 (NRO)
            # TODO: Ggeneralize factor for SF, and Gaussian convolution function
            if not self.__obtain_and_set_factors_by_convolution_function(_pp, _tirp):
                return _tirp.failed_rms

        if _tirp.N == 0:
            LOG.warning('No rms estimate is available.')
            return _tirp.failed_rms

        __theoretical_rms = numpy.sqrt(_tirp.sq_rms) / _tirp.N
        LOG.info('Theoretical RMS of image = {} {}'.format(__theoretical_rms, _pp.brightnessunit))
        return _tirp.cqa.quantity(__theoretical_rms, _pp.brightnessunit)

    def __obtain_t_sub_on_off(self, _tirp: imaging_params.TheoreticalImageRmsParameters) -> bool:
        """Obtain TsubON and TsubOFF. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()

        Returns:
            False if it cannot get Tsub On/Off values by some error.

        Raises:
            BaseException : raises when it cannot find a sky caltable applied.
        """
        _tirp.t_sub_on = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.raster_info.row_duration, _tirp.time_unit))[0]
        __sky_field = _tirp.calmsobj.calibration_strategy['field_strategy'][_tirp.fieldid]
        try:
            __skytab = ''
            __caltabs = _tirp.context.callibrary.applied.get_caltable('ps')
            # For some reasons, sky caltable is not registered to calstate
            for __cto, __cfrom in _tirp.context.callibrary.applied.merged().items():
                if __cto.vis == _tirp.calmsobj.name and (__cto.field == '' or
                                                         _tirp.fieldid in
                                                         [f.id for f in _tirp.calmsobj.get_fields(name=__cto.field)]):
                    for __cf in __cfrom:
                        if __cf.gaintable in __caltabs:
                            __skytab = __cf.gaintable
                            break
        except BaseException:
            LOG.error('Could not find a sky caltable applied. ' + _tirp.error_msg)
            raise
        if not os.path.exists(__skytab):
            LOG.warning('Could not find a sky caltable applied. ' + _tirp.error_msg)
            return False
        LOG.info('Searching OFF scans in {}'.format(os.path.basename(__skytab)))
        with casa_tools.TableReader(__skytab) as tb:
            __interval_unit = tb.getcolkeyword('INTERVAL', 'QuantumUnits')[0]
            __t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}&&FIELD_ID=={}'.format(_tirp.spwid, _tirp.antid,
                                                                                       __sky_field),
                           columns='INTERVAL')
            if __t.nrows == 0:
                LOG.warning('No sky caltable row found for spw {}, antenna {}, field {} in {}. {}'.format(
                    _tirp.spwid, _tirp.antid, __sky_field, os.path.basename(__skytab), _tirp.error_msg))
                __t.close()
                return False
            try:
                __interval = __t.getcol('INTERVAL')
            finally:
                __t.close()

        _tirp.t_sub_off = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.cqa.quantity(__interval.mean(),
                                                                                  __interval_unit), _tirp.time_unit))[0]
        LOG.info('Subscan Time ON = {} {}, OFF = {} {}'.format(_tirp.t_sub_on, _tirp.time_unit,
                 _tirp.t_sub_off, _tirp.time_unit))
        return True

    def __obtain_jy_per_k(self, _pp: imaging_params.PostProcessParameters,
                          _tirp: imaging_params.TheoreticalImageRmsParameters) -> Union[float, bool]:
        """Obtain Jy/K. A sub method of calculate_theoretical_image_rms().

        Args:
            _pp : Imaging post process parameters of prepare()
            _tirp : Parameter object of calculate_theoretical_image_rms()

        Returns:
            Jy/K value or failure flag

        Raises:
            BaseException : raises when it cannot find a Jy/K caltable applied.
        """
        if _pp.brightnessunit == 'K':
            __jy_per_k = 1.0
            LOG.info('No Jy/K conversion was performed to the image.')
        else:
            try:
                __k2jytab = ''
                __caltabs = _tirp.context.callibrary.applied.get_caltable(('amp', 'gaincal'))
                __found = __caltabs.intersection(_tirp.calst.get_caltable(('amp', 'gaincal')))
                if len(__found) == 0:
                    LOG.warning('Could not find a Jy/K caltable applied. ' + _tirp.error_msg)
                    return False
                if len(__found) > 1:
                    LOG.warning('More than one Jy/K caltables are found.')
                __k2jytab = __found.pop()
                LOG.info('Searching Jy/K factor in {}'.format(os.path.basename(__k2jytab)))
            except BaseException:
                LOG.error('Could not find a Jy/K caltable applied. ' + _tirp.error_msg)
                raise
            if not os.path.exists(__k2jytab):
                LOG.warning('Could not find a Jy/K caltable applied. ' + _tirp.error_msg)
                return False
            with casa_tools.TableReader(__k2jytab) as tb:
                __t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}'.format(_tirp.spwid, _tirp.antid),
                               columns='CPARAM')
                if __t.nrows == 0:
                    LOG.warning('No Jy/K caltable row found for spw {}, antenna {} in {}. {}'.format(_tirp.spwid,
                                _tirp.antid, os.path.basename(__k2jytab), _tirp.error_msg))
                    __t.close()
                    return False
                try:
                    tc = __t.getcol('CPARAM')
                finally:
                    __t.close()

                __jy_per_k = (1. / tc.mean(axis=-1).real ** 2).mean()
                LOG.info('Jy/K factor = {}'.format(__jy_per_k))  # obtain Jy/k factor
        return __jy_per_k

    def __obtain_and_set_factors_by_convolution_function(self, _pp: imaging_params.PostProcessParameters,
                                                         _tirp: imaging_params.TheoreticalImageRmsParameters) -> bool:
        """Obtain factors by convolution function, and set it into TIRP. A sub method of calculate_theoretical_image_rms().

        Args:
            _pp : Imaging post process parameters of prepare()
            _tirp : Parameter object of calculate_theoretical_image_rms()

        Returns:
            False if it cannot get Jy/K
        """
        policy = observatory_policy.get_imaging_policy(_tirp.context)
        jy_per_k = self.__obtain_jy_per_k(_pp, _tirp)
        if jy_per_k is False:
            return False
        ang = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.raster_info.scan_angle, 'rad'))[0] + 0.5 * numpy.pi
        c_proj = numpy.sqrt((_tirp.cy_val * numpy.sin(ang)) ** 2 + (_tirp.cx_val * numpy.cos(ang)) ** 2)
        inv_variant_on = _tirp.effBW * numpy.abs(_tirp.cx_val * _tirp.cy_val) * \
            _tirp.t_on_act / _tirp.width / _tirp.height
        inv_variant_off = _tirp.effBW * c_proj * _tirp.t_sub_off * _tirp.t_on_act / _tirp.t_sub_on / _tirp.height
        for ipol in _tirp.polids:
            _tirp.sq_rms += (jy_per_k * _tirp.mean_tsys_per_pol[ipol]) ** 2 * \
                (policy.get_conv2d() ** 2 / inv_variant_on + policy.get_conv1d() ** 2 / inv_variant_off)
            _tirp.N += 1.0
        return True

    def __obtain_t_on_actual(self, _tirp: imaging_params.TheoreticalImageRmsParameters):
        """Obtain T_on actual. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()
        """
        unit = _tirp.dt.getcolkeyword('EXPOSURE', 'UNIT')
        t_on_tot = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.cqa.quantity(
            _tirp.dt.getcol('EXPOSURE').take(_tirp.index_list, axis=-1).sum(), unit), _tirp.time_unit))[0]
        # flagged fraction
        full_intent = utils.to_CASA_intent(_tirp.msobj, 'TARGET')
        flagdata_summary_job = casa_tasks.flagdata(vis=_tirp.infile, mode='summary',
                                                   antenna='{}&&&'.format(_tirp.antid),
                                                   field=str(_tirp.fieldid),
                                                   spw=str(_tirp.spwid), intent=full_intent,
                                                   spwcorr=False, fieldcnt=False,
                                                   name='summary')
        flag_stats = self._executor.execute(flagdata_summary_job)
        frac_flagged = flag_stats['spw'][str(_tirp.spwid)]['flagged'] / flag_stats['spw'][str(_tirp.spwid)]['total']
        # the actual time on source
        _tirp.t_on_act = t_on_tot * (1.0 - frac_flagged)
        LOG.info('The actual on source time = {} {}'.format(_tirp.t_on_act, _tirp.time_unit))
        LOG.info('- total time on source = {} {}'.format(t_on_tot, _tirp.time_unit))
        LOG.info('- flagged Fraction = {} %'.format(100 * frac_flagged))

    def __obtain_calibration_tables_applied(self, _tirp: imaging_params.TheoreticalImageRmsParameters):
        """Obtain calibration tables applied. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()
        """
        __calto = callibrary.CalTo(vis=_tirp.calmsobj.name, field=str(_tirp.fieldid))
        _tirp.calst = _tirp.context.callibrary.applied.trimmed(_tirp.context, __calto)

    def __obtain_wx_and_wy(self, _tirp: imaging_params.TheoreticalImageRmsParameters):
        """Obtain Wx and Wy. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()
        """
        _tirp.width = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.raster_info.width, _tirp.ang_unit))[0]
        _tirp.height = _tirp.cqa.getvalue(_tirp.cqa.convert(_tirp.raster_info.height, _tirp.ang_unit))[0]

    def __obtain_average_tsys(self, _tirp: imaging_params.TheoreticalImageRmsParameters):
        """Obtain average Tsys. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()
        """
        _tirp.mean_tsys_per_pol = _tirp.dt.getcol('TSYS').take(_tirp.index_list, axis=-1).mean(axis=-1)
        LOG.info('Mean Tsys = {} K'.format(str(_tirp.mean_tsys_per_pol)))

    def __obtain_effective_BW(self, _tirp: imaging_params.TheoreticalImageRmsParameters):
        """Obtain effective BW. A sub method of calculate_theoretical_image_rms().

        Args:
            _tirp : Parameter object of calculate_theoretical_image_rms()
        """
        with casa_tools.MSMDReader(_tirp.infile) as __msmd:
            _tirp.effBW = __msmd.chaneffbws(_tirp.spwid).mean()
            LOG.info('Using an MS effective bandwidth, {} kHz'.format(_tirp.effBW * 0.001))

    def __loop_initializer_of_theoretical_image_rms(self, _cp: imaging_params.CommonParameters,
                                                    _rgp: imaging_params.ReductionGroupParameters,
                                                    _tirp: imaging_params.TheoreticalImageRmsParameters) -> Tuple[bool]:
        """Initialize imaging_params.TheoreticalImageRmsParameters for the loop of calculate_theoretical_image_rms().

        Args:
            _cp : Common parameter object of prepare()
            _rgp : Reduction group parameter object of prepare()
            _tirp : Parameter object of calculate_theoretical_image_rms()

        Returns:
            Tupled flag to describe the loop action [go|halt|skip].
                GO   : (False, False)
                HALT : (True,  True)
                SKIP : (False, True)
        """
        _tirp.msobj = _tirp.context.observing_run.get_ms(name=_tirp.infile)
        __callist = _tirp.context.observing_run.get_measurement_sets_of_type([DataType.REGCAL_CONTLINE_ALL])
        _tirp.calmsobj = sdutils.match_origin_ms(__callist, _tirp.msobj.origin_ms)
        __dd_corrs = _tirp.msobj.get_data_description(spw=_tirp.spwid).corr_axis
        _tirp.polids = [__dd_corrs.index(p) for p in _tirp.pol_names if p in __dd_corrs]
        __field_name = _tirp.msobj.get_fields(field_id=_tirp.fieldid)[0].name
        _tirp.error_msg = 'Aborting calculation of theoretical thermal noise of ' + \
                          'Field {} and SpW {}'.format(__field_name, _rgp.combined.spws)
        __HALT = (True, True)
        __SKIP = (False, True)
        __GO = (False, False)
        if _tirp.msobj.observing_pattern[_tirp.antid][_tirp.spwid][_tirp.fieldid] != 'RASTER':
            LOG.warning('Unable to calculate RMS of non-Raster map. ' + _tirp.error_msg)
            return __HALT
        LOG.info(
            'Processing MS {}, Field {}, SpW {}, '
            'Antenna {}, Pol {}'.
            format(_tirp.msobj.basename, __field_name, _tirp.spwid,
                   _tirp.msobj.get_antenna(_tirp.antid)[0].name, str(_tirp.pol_names)))
        if _tirp.raster_info is None:
            __rsres = RasterScanHeuristicsResult(_tirp.msobj)
            _rgp.imager_result.rasterscan_heuristics_results_incomp \
                              .setdefault(_tirp.msobj.origin_ms, []) \
                              .append(__rsres)
            __rsres.set_result_fail(_tirp.antid, _tirp.spwid, _tirp.fieldid)
            LOG.debug(f'Raster scan analysis incomplete. Skipping calculation of theoretical image RMS : EB:{_tirp.msobj.execblock_id}:{_tirp.msobj.antennas[_tirp.antid].name}')
            return __SKIP
        _tirp.dt = _cp.dt_dict[_tirp.msobj.basename]
        _tirp.index_list = common.get_index_list_for_ms(_tirp.dt, [_tirp.msobj.origin_ms],
                                                        [_tirp.antid], [_tirp.fieldid], [_tirp.spwid])
        if len(_tirp.index_list) == 0:  # this happens when permanent flag is set to all selection.
            LOG.info('No unflagged row in DataTable. Skipping further calculation.')
            return __SKIP
        return __GO


def _analyze_raster_pattern(datatable: DataTable, msobj: MeasurementSet,
                            fieldid: int, spwid: int, antid: int, rgp: 'imaging_params.ReductionGroupParameters') -> RasterInfo:
    """Analyze raster scan pattern from pointing in DataTable.

    Args:
        datatable : DataTable instance
        msobj : MS class instance to process
        fieldid : A field ID to process
        spwid : An SpW ID to process
        antid : An antenna ID to process
        rgp : Reduction group parameter object of prepare()

    Returns:
        A named Tuple of RasterInfo
    """
    origin_basename = os.path.basename(msobj.origin_ms)
    metadata = rasterutil.read_datatable(datatable)
    pflag = metadata.pflag
    ra = metadata.ra
    dec = metadata.dec
    exposure = datatable.getcol('EXPOSURE')
    timetable = datatable.get_timetable(ant=antid, spw=spwid, field_id=fieldid, ms=origin_basename, pol=None)
    # dtrow_list is a list of numpy array holding datatable rows separated by raster rows
    # [[r00, r01, r02, ...], [r10, r11, r12, ...], ...]
    dtrow_list_nomask = rasterutil.extract_dtrow_list(timetable)
    dtrow_list = [rows[pflag[rows]] for rows in dtrow_list_nomask if numpy.any(pflag[rows] == True)]
    radec_unit = datatable.getcolkeyword('OFS_RA', 'UNIT')
    assert radec_unit == datatable.getcolkeyword('OFS_DEC', 'UNIT')
    exp_unit = datatable.getcolkeyword('EXPOSURE', 'UNIT')
    _rsres = RasterScanHeuristicsResult(msobj)
    rgp.imager_result.rasterscan_heuristics_results_rgap \
                     .setdefault(msobj.origin_ms, []) \
                     .append(_rsres)
    try:
        gap_r = rasterscan.find_raster_gap(ra, dec, dtrow_list)
    except Exception as e:
        if isinstance(e, RasterScanHeuristicsFailure):
            _rsres.set_result_fail(antid, spwid, fieldid)
            LOG.debug('{} : EB:{}:{}'.format(e, msobj.execblock_id, msobj.antennas[antid].name))
        try:
            dtrow_list_large = rasterutil.extract_dtrow_list(timetable, for_small_gap=False)
            se_small = [(v[0], v[-1]) for v in dtrow_list]
            se_large = [(v[0], v[-1]) for v in dtrow_list_large]
            gap_r = []
            for sl, el in se_large:
                for i, (ss, es) in enumerate(se_small):
                    if ss == sl:
                        gap_r.append(i)
                        break
            gap_r.append(len(dtrow_list))
        except Exception:
            LOG.warning('Could not find gaps between raster scans. No result is produced.')
            return None

    cqa = casa_tools.quanta
    idx_all = numpy.concatenate(dtrow_list)
    mean_dec = numpy.mean(datatable.getcol('DEC')[idx_all])
    dec_unit = datatable.getcolkeyword('DEC', 'UNIT')
    map_center_dec = cqa.getvalue(
        cqa.convert(cqa.quantity(mean_dec, dec_unit), 'rad')
    )[0]
    dec_factor = numpy.abs(numpy.cos(map_center_dec))

    ndata = len(dtrow_list)
    duration = numpy.fromiter(
        map(lambda x: exposure[x].sum(), dtrow_list),
        dtype=float, count=ndata
    )
    num_integration = numpy.fromiter(
        map(len, dtrow_list), dtype=int, count=ndata
    )
    center_ra = numpy.fromiter(
        map(lambda x: ra[x].mean(), dtrow_list),
        dtype=float, count=ndata
    )
    center_dec = numpy.fromiter(
        map(lambda x: dec[x].mean(), dtrow_list),
        dtype=float, count=ndata
    )
    delta_ra = numpy.fromiter(
        map(lambda x: (ra[x[-1]] - ra[x[0]]) * dec_factor, dtrow_list),
        dtype=float, count=ndata
    )
    delta_dec = numpy.fromiter(
        map(lambda x: dec[x[-1]] - dec[x[0]], dtrow_list),
        dtype=float, count=ndata
    )
    height_list = []
    for s, e in zip(gap_r[:-1], gap_r[1:]):
        start_ra = center_ra[s]
        start_dec = center_dec[s]
        end_ra = center_ra[e - 1]
        end_dec = center_dec[e - 1]
        height_list.append(
            numpy.hypot((start_ra - end_ra) * dec_factor, start_dec - end_dec)
        )
    LOG.debug('REFACTOR: ant %s, spw %s, field %s', antid, spwid, fieldid)
    LOG.debug('REFACTOR: map_center_dec=%s, dec_factor=%s', map_center_dec, dec_factor)
    LOG.debug('REFACTOR: duration=%s', list(duration))
    LOG.debug('REFACTOR: num_integration=%s', list(num_integration))
    LOG.debug('REFACTOR: delta_ra=%s', list(delta_ra))
    LOG.debug('REFACTOR: delta_dec=%s', list(delta_dec))
    LOG.debug('REFACTOR: center_ra=%s', list(center_ra))
    LOG.debug('REFACTOR: center_dec=%s', list(center_dec))
    LOG.debug('REFACTOR: height_list=%s', list(height_list))
    LOG.debug('REFACTOR: dtrows')
    for rows in dtrow_list:
        LOG.debug('REFACTOR:   %s', list(rows))
    center_ra = numpy.array(center_ra)
    center_dec = numpy.array(center_dec)
    row_sep_ra = (center_ra[1:] - center_ra[:-1]) * dec_factor
    row_sep_dec = center_dec[1:] - center_dec[:-1]
    row_separation = numpy.median(numpy.hypot(row_sep_ra, row_sep_dec))
    # find complate raster
    num_row_int = rasterutil.find_most_frequent(num_integration)
    complete_idx = numpy.where(num_integration >= num_row_int)
    # raster scan parameters
    row_duration = numpy.array(duration)[complete_idx].mean()
    row_delta_ra = numpy.abs(delta_ra)[complete_idx].mean()
    row_delta_dec = numpy.abs(delta_dec)[complete_idx].mean()
    width = numpy.hypot(row_delta_ra, row_delta_dec)
    sign_ra = +1.0 if delta_ra[complete_idx[0][0]] >= 0 else -1.0
    sign_dec = +1.0 if delta_dec[complete_idx[0][0]] >= 0 else -1.0
    scan_angle = math.atan2(sign_dec * row_delta_dec, sign_ra * row_delta_ra)
    hight = numpy.max(height_list)
    center = (cqa.quantity(0.5 * (center_ra.min() + center_ra.max()), radec_unit),
              cqa.quantity(0.5 * (center_dec.min() + center_dec.max()), radec_unit))
    raster_info = RasterInfo(center[0], center[1],
                             cqa.quantity(width, radec_unit), cqa.quantity(hight, radec_unit),
                             cqa.quantity(scan_angle, 'rad'), cqa.quantity(row_separation, radec_unit),
                             cqa.quantity(row_duration, exp_unit))
    LOG.info('Raster Information')
    LOG.info('- Map Center: [{}, {}]'.format(cqa.angle(raster_info.center_ra, prec=8, form='time')[0],
                                             cqa.angle(raster_info.center_dec, prec=8)[0]))
    LOG.info('- Scan Extent: [{}, {}] (scan direction: {})'.format(cqa.tos(raster_info.width),
                                                                   cqa.tos(raster_info.height),
                                                                   cqa.tos(raster_info.scan_angle)))
    LOG.info('- Raster row separation = {}'.format(cqa.tos(raster_info.row_separation)))
    LOG.info('- Raster row scan duration = {}'.format(cqa.tos(cqa.convert(raster_info.row_duration, 's'))))
    return raster_info


def calc_image_statistics(imagename: str, chans: str, region: str) -> dict:
    """Return image statistics with channel and region selection.

    Args:
        imagename : Path to image to calculate statistics
        chans : Channel range selection string, e.g., '0~110;240~300'
        region : Region definition string.

    Returns:
        A dictionary of statistic values returned by ia.statistics.
    """
    LOG.info("Calculateing image statistics of chans='{}', region='{}' in {}".format(chans, region, imagename))
    rg = casa_tools.regionmanager
    with casa_tools.ImageReader(imagename) as ia:
        cs = ia.coordsys()
        try:
            chan_sel = rg.frombcs(csys=cs.torecord(), shape=ia.shape(), chans=chans)
        finally:
            cs.done()
            rg.done()
        subim = ia.subimage(region=chan_sel)
        try:
            stat = subim.statistics(region=region)
        finally:
            subim.close()
    return stat


# Utility methods to calcluate channel ranges
def convert_frequency_ranges_to_channels(range_list: List[Tuple[float, float]],
                                         cs: 'coordsys', num_chan: int) -> List[Tuple[int, int]]:
    """Convert frequency ranges to channel ones.

    Args:
        range_list : A list of min/max frequency ranges,
            e.g., [[fmin0,fmax0],[fmin1, fmax1],...]
        cs : A coordinate system to convert world values to pixel one
        num_chan : The number of channels in frequency axis

    Returns:
        A list of min/max channels, e.g., [[imin0, imax0],[imin1,imax1],...]
    """
    faxis = cs.findaxisbyname('spectral')
    ref_world = cs.referencevalue()['numeric']
    LOG.info("Aggregated spectral line frequency ranges of combined image = {}".format(str(range_list)))
    channel_ranges = []  # should be list for sort
    for segment in range_list:
        ref_world[faxis] = segment[0]
        start_chan = cs.topixel(ref_world)['numeric'][faxis]
        ref_world[faxis] = segment[1]
        end_chan = cs.topixel(ref_world)['numeric'][faxis]
        # handling of LSB
        min_chan = min(start_chan, end_chan)
        max_chan = max(start_chan, end_chan)
        # LOG.info("#####Freq to Chan: [{:f}, {:f}] -> [{:f}, {:f}]".format(segment[0], segment[1], min_chan, max_chan))
        if max_chan < -0.5 or min_chan > num_chan - 0.5:  # out of range
            # LOG.info("#####Omitting channel range [{:f}, {:f}]".format(min_chan, max_chan))
            continue
        channel_ranges.append([max(int(min_chan), 0),
                               min(int(max_chan), num_chan - 1)])
    channel_ranges.sort()
    return merge_ranges(channel_ranges)


def convert_range_list_to_string(range_list: List[int]) -> str:
    """Convert a list of index ranges to string.

    Args:
        range_list : A list of ranges, e.g., [imin0, imax0, imin1, imax1, ...]

    Returns:
        A string in form, e.g., 'imin0~imax0;imin1~imax1'

    Examples:
        >>> convert_range_list_to_string( [5, 10, 15, 20] )
        '5~10;15~20'
    """
    stat_chans = str(';').join(['{:d}~{:d}'.format(range_list[iseg], range_list[iseg + 1])
                               for iseg in range(0, len(range_list), 2)])
    return stat_chans


def merge_ranges(range_list: List[Tuple[Number, Number]]) -> List[Tuple[Number, Number]]:
    """Merge overlapping ranges in range_list.

    Args:
        range_list : A list of ranges to merge, e.g., [ [min0,max0], [min1,max1], .... ]
                    Each range in the list should be in ascending order (min0 <= max0)
                    There is no assumption in the order of ranges, e.g., min0 w.r.t min1

    Raises:
        ValueError: too few elements in the range description, such as [] or [min0]

    Returns:
        A list of merged ranges
        e.g., [[min_merged0,max_marged0], [min_merged1,max_merged1], ....]
    """
    # LOG.info("#####Merge ranges: {}".format(str(range_list)))
    num_range = len(range_list)
    if num_range == 0:
        return []
    merged = [range_list[0][0:2]]
    for i in range(1, num_range):
        segment = range_list[i]
        if len(segment) < 2:
            raise ValueError("segments in range list must have 2 elements")
        overlap = -1
        for j in range(len(merged)):
            if segment[1] < merged[j][0] or segment[0] > merged[j][1]:  # no overlap
                continue
            else:
                overlap = j
                break
        if overlap < 0:
            merged.append(segment[0:2])
        else:
            merged[j][0] = min(merged[j][0], segment[0])
            merged[j][1] = max(merged[j][1], segment[1])
    # Check if further merge is necessary
    while len(merged) < num_range:
        num_range = len(merged)
        merged = merge_ranges(merged)
    # LOG.info("#####Merged: {}".format(str(merged)))
    return merged


def invert_ranges(id_range_list: List[Tuple[int, int]],
                  num_ids: int, edge: Tuple[int, int]) -> List[int]:
    """Return inverted ID ranges.

    Args:
        id_range_list : A list of min/max ID ranges to invert. The list should
            be sorted in the ascending order of min IDs.
        num_ids : Number of IDs to consider
        edge : The left and right edges to exclude

    Returns:
        A 1-d list of inverted ranges

    Examples:
        >>> id_range_list = [[5,10],[15,20]]
        >>> num_ids = 30
        >>> edge = (2,3)
        >>> invert_ranges(id_range_list, num_ids, edge)
        [2, 4, 11, 14, 21, 26]
    """
    inverted_list = []
    if len(id_range_list) == 0:
        inverted_list = [edge[0], num_ids - 1 - edge[1]]
    else:
        if id_range_list[0][0] > edge[0]:
            inverted_list.extend([edge[0], id_range_list[0][0] - 1])
        for j in range(len(id_range_list) - 1):
            start_include = id_range_list[j][1] + 1
            end_include = id_range_list[j + 1][0] - 1
            if start_include <= end_include:
                inverted_list.extend([start_include, end_include])
        if id_range_list[-1][1] + 1 < num_ids - 1 - edge[1]:
            inverted_list.extend([id_range_list[-1][1] + 1, num_ids - 1 - edge[1]])
    return inverted_list
