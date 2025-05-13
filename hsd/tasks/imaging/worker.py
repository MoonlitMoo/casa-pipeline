"""Worker classes of SDImaging."""

from typing import TYPE_CHECKING, Dict, List, NewType, Optional, Tuple, Union

import math
import os
import shutil

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataTable, DataType
from pipeline.infrastructure import casa_tasks, casa_tools
from pipeline.infrastructure.launcher import Context

from .. import common
from ..common import direction_utils as dirutil
from ..common import observatory_policy, sdtyping, utils
from . import resultobjects

LOG = infrastructure.get_logger(__name__)


def ImageCoordinateUtil(
    context: Context,
    ms_names: List[str],
    ant_list: List[Optional[int]],
    spw_list: List[int],
    fieldid_list: List[int]
) -> Union[Tuple[str, 'sdtyping.Angle', 'sdtyping.Angle', int, int, 'sdtyping.Direction'], bool]:
    """
    Calculate spatial coordinate of image.

    Items in ant_list can be None, which indicates that the function will take into
    account pointing data from all the antennas in MS.

    Args:
        context: Pipeline context
        ms_names: List of MS names
        ant_list: List of antenna ids. List elements could be None.
        spw_list: List of spw ids.
        fieldid_list: List of field ids.

    Raises:
        RuntimeError: Raises if found unexpected unit of RA/DEC in DataTable.

    Returns:
        - Six tuple containing phasecenter, horizontal and vertical cell sizes,
          horizontal and vertical number of pixels, and direction of the origin
          (for moving targets).
        - False if no valid data exists.
    """
    # A flag to use field direction as image center (True) rather than center of the map extent
    USE_FIELD_DIR = False

    ref_msobj = context.observing_run.get_ms(ms_names[0])
    ref_fieldid = fieldid_list[0]
    ref_spw = spw_list[0]
    source_name = ref_msobj.fields[ref_fieldid].name
    # trim source name to workaround '"name"' type of sources
    # trimmed_name = source_name.strip('"')
    is_eph_obj = ref_msobj.get_fields(ref_fieldid)[0].source.is_eph_obj
    is_known_eph_obj = ref_msobj.get_fields(ref_fieldid)[0].source.is_known_eph_obj

    imaging_policy = observatory_policy.get_imaging_policy(context)

    # qa tool
    qa = casa_tools.quanta

    # msmd-less implementation
    spw = ref_msobj.get_spectral_window(ref_spw)
    freq_hz = numpy.float64(spw.mean_frequency.value)
#     fields = ref_msobj.get_fields(name=trimmed_name)
    fields = ref_msobj.get_fields(name=source_name)
    fnames = [f.name for f in fields]
    if USE_FIELD_DIR:
        me_center = fields[0].mdirection

    # cellx and celly
    theory_beam_arcsec = imaging_policy.get_beam_size_arcsec(ref_msobj, ref_spw)
    grid_factor = imaging_policy.get_beam_size_pixel()
    grid_size = qa.quantity(theory_beam_arcsec, 'arcsec')
    cellx = qa.div(grid_size, grid_factor)
    celly = cellx
    cell_in_deg = qa.convert(cellx, 'deg')['value']
    LOG.info('Calculating image coordinate of field \'%s\', reference frequency %fGHz' % (fnames[0], freq_hz * 1.e-9))
    LOG.info('cell=%s' % (qa.tos(cellx)))

    # nx, ny and center
    ra0 = []
    dec0 = []
    outref = None
    org_direction = None
    for vis, ant_id, field_id, spw_id in zip(ms_names, ant_list, fieldid_list, spw_list):

        msobj = context.observing_run.get_ms(vis)
        # get first org_direction if source if ephemeris source
        # if is_eph_obj and org_direction==None:
        if (is_eph_obj or is_known_eph_obj) and org_direction is None:
            # get org_direction
            org_direction = msobj.get_fields(field_id)[0].source.org_direction

        datatable_name = utils.get_data_table_path(context, msobj)
        datatable = DataTable(name=datatable_name, readonly=True)

        if (datatable.getcolkeyword('RA', 'UNIT') != 'deg') or \
           (datatable.getcolkeyword('DEC', 'UNIT') != 'deg') or \
           (datatable.getcolkeyword('OFS_RA', 'UNIT') != 'deg') or \
           (datatable.getcolkeyword('OFS_DEC', 'UNIT') != 'deg'):
            raise RuntimeError("Found unexpected unit of RA/DEC in DataTable. It should be in 'deg'")

        if ant_id is None:
            # take all the antennas into account
            num_antennas = len(msobj.antennas)
            _vislist = [msobj.origin_ms for _ in range(num_antennas)]
            _antlist = [i for i in range(num_antennas)]
            _fieldlist = [field_id for _ in range(num_antennas)]
            _spwlist = [spw_id for _ in range(num_antennas)]
            index_list = sorted(common.get_index_list_for_ms(datatable, _vislist, _antlist, _fieldlist, _spwlist))
        else:
            index_list = sorted(common.get_index_list_for_ms(datatable, [msobj.origin_ms], [ant_id], [field_id], [spw_id]))

        # if is_eph_obj:
        if is_eph_obj or is_known_eph_obj:
            _ra = datatable.getcol('OFS_RA').take(index_list)
            _dec = datatable.getcol('OFS_DEC').take(index_list)
        else:
            _ra = datatable.getcol('RA').take(index_list)
            _dec = datatable.getcol('DEC').take(index_list)

        ra0.extend(_ra)
        dec0.extend(_dec)

        outref = datatable.direction_ref
        del datatable

    if len(ra0) == 0:
        antenna_id = ant_list[0]
        if antenna_id is None:
            LOG.warning('No valid data for source %s spw %s in %s. Image will not be created.',
                        source_name, ref_spw, ref_msobj.basename)
        else:
            antenna_name = ref_msobj.antennas[antenna_id].name
            LOG.warning('No valid data for source %s antenna %s spw %s in %s. Image will not be created.',
                        source_name, antenna_name, ref_spw, ref_msobj.basename)
        return False

    if outref is None:
        LOG.warning('No direction reference is set. Assuming ICRS')
        outref = 'ICRS'

    # if is_eph_obj:
    #     ra_offset = qa.convert( org_direction['m0'], 'deg' )['value']
    #     dec_offset = qa.convert( org_direction['m1'], 'deg' )['value']
    # else:
    #     ra_offset = 0.0
    #     dec_offset = 0.0

    # convert offset coordinate into shifted coordinate for ephemeris sources
    # to determine the image size (size, center, npix)
    if is_eph_obj or is_known_eph_obj:
        ra = []
        dec = []
        for ra1, dec1 in zip(ra0, dec0):
            ra2, dec2 = dirutil.direction_recover(ra1, dec1, org_direction)
            ra.append(ra2)
            dec.append(dec2)
    else:
        ra = ra0
        dec = dec0
    del ra0, dec0

    ra_min = min(ra)
    ra_max = max(ra)
    dec_min = min(dec)
    dec_max = max(dec)

    if USE_FIELD_DIR:
        # phasecenter = field direction
        ra_center = qa.convert(me_center['m0'], 'deg')
        dec_center = qa.convert(me_center['m1'], 'deg')
    else:
        # map center
        ra_center = qa.quantity(0.5 * (ra_min + ra_max), 'deg')
        dec_center = qa.quantity(0.5 * (dec_min + dec_max), 'deg')
    ra_center_in_deg = qa.getvalue(ra_center)
    dec_center_in_deg = qa.getvalue(dec_center)
    phasecenter = '{0} {1} {2}'.format(outref,
                                       qa.formxxx(ra_center, 'hms'),
                                       qa.formxxx(dec_center, 'dms'))
    LOG.info('phasecenter=\'%s\'' % (phasecenter,))

    dec_correction = 1.0 / math.cos(dec_center_in_deg / 180.0 * 3.1415926535897931)
    width = 2 * max(abs(ra_center_in_deg - ra_min), abs(ra_max - ra_center_in_deg))
    height = 2 * max(abs(dec_center_in_deg - dec_min), abs(dec_max - dec_center_in_deg))
    LOG.debug('Map extent: [%f, %f] arcmin' % (width / 60., height / 60.))

    nx = int(width / (cell_in_deg * dec_correction)) + 1
    ny = int(height / cell_in_deg) + 1

    # Adjust nx and ny to be even number for performance (which is
    # recommended by imager).
    # Also increase nx and ny  by 2 if they are even number.
    # This is due to a behavior of the imager. The imager configures
    # direction axis as follows:
    #     reference value: phasecenter
    #           increment: cellx, celly
    #     reference pixel: ceil((nx-1)/2), ceil((ny-1)/2)
    # It means that reference pixel will not be map center if nx/ny
    # is even number. It results in asymmetric area coverage on both
    # sides of the reference pixel, which may miss certain range of
    # (order of 1 pixel) observed area near the edge.
    if nx % 2 == 0:
        nx += 2
    else:
        nx += 1
    if ny % 2 == 0:
        ny += 2
    else:
        ny += 1

    # PIPE-1416
    margin = imaging_policy.get_image_margin()
    nx, ny = (nx + margin, ny + margin)

    LOG.info('Image pixel size: [nx, ny] = [%s, %s]' % (nx, ny))
    return phasecenter, cellx, celly, nx, ny, org_direction


def get_brightness_unit(infiles: List[str]) -> str:
    """Return image brightness unit according to the unit of input MSes.

    If multiple units were detected, it will warn it and
    return one of the units detected. What is returned is
    uncertain.

    Args:
        infiles: List of MS names

    Returns:
        Brightness unit string
    """
    units = {utils.get_brightness_unit(infile) for infile in infiles}
    num_units = len(units)
    LOG.info(f'units {units}')
    brightnessunit = units.pop()

    if num_units > 1:
        LOG.warning(f'Brightness unit is not consistent among input MSes. Will use {brightnessunit}, but result may not correct.')

    return brightnessunit


class SDImagingWorkerInputs(vdp.StandardInputs):
    """Inputs class for imaging worker.

    NOTE: infile should be a complete list of MSes
    """

    # Search order of input vis
    processing_data_type = [DataType.BASELINED, DataType.ATMCORR,
                            DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    infiles = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    outfile = vdp.VisDependentProperty(default='')
    mode = vdp.VisDependentProperty(default='LINE')
    antids = vdp.VisDependentProperty(default=-1)
    spwids = vdp.VisDependentProperty(default=-1)
    fieldids = vdp.VisDependentProperty(default=-1)
    stokes = vdp.VisDependentProperty(default='I')
    edge = vdp.VisDependentProperty(default=(0, 0))
    phasecenter = vdp.VisDependentProperty(default='')
    cellx = vdp.VisDependentProperty(default='')
    celly = vdp.VisDependentProperty(default='')
    nx = vdp.VisDependentProperty(default=-1)
    ny = vdp.VisDependentProperty(default=-1)
    org_direction = vdp.VisDependentProperty(default=None)

    # Synchronization between infiles and vis is still necessary
    @vdp.VisDependentProperty
    def vis(self) -> List[str]:
        """Return the list of input file names

        Returns:
            list of input file names
        """
        return self.infiles

    def __init__(self, context: Context, infiles: List[str], outfile: str, mode: str,
                 antids: List[int], spwids: List[int], fieldids: List[int], restfreq: str,
                 stokes: str, edge: Optional[List[int]]=None, phasecenter: Optional[str]=None,
                 cellx: Optional['sdtyping.Angle']=None,
                 celly: Optional['sdtyping.Angle']=None,
                 nx: Optional[int]=None, ny: Optional[int]=None,
                 org_direction: Optional['sdtyping.Direction']=None):
        """Initialise an instance of SDImagingWorkerInputs.

        Args:
            context: pipeline context
            infiles: list of input file names
            outfile: output file name
            mode: imaging mode
            antids: list of antenna IDs
            spwids: list of spectrum windows IDs
            fieldids: list of field IDs
            restfreq: Rest frequency
            stokes: Stokes Planes
            edge: numbers of edge channels to be excluded in imaging. When edge=[10, 20], 10 and 20 channels
                  in the beginning and at the end of a spectral window, respectively, are excluded.
            phasecenter: Image center
            cellx: size(unit and value) per pixel of image axis x
            celly: size(unit and value) per pixel of image axis y
            nx: the number of pixels x
            ny: the number of pixels y
            org_direction:  a measure of direction of origin for ephemeris object
        """
        # NOTE: spwids and pols are list of numeric id list while scans
        #       is string (mssel) list
        super(SDImagingWorkerInputs, self).__init__()

        self.context = context
        self.infiles = infiles  # input MS names
        self.outfile = outfile  # output image name
        self.mode = mode
        self.antids = antids
        self.spwids = spwids
        self.fieldids = fieldids
        self.restfreq = restfreq
        self.stokes = stokes
        self.edge = edge
        self.phasecenter = phasecenter
        self.cellx = cellx
        self.celly = celly
        self.nx = nx
        self.ny = ny
        self.org_direction = org_direction

    @property
    def is_freq_axis_ascending(self) -> bool:
        _ref_spwobj = self.context.observing_run.get_ms(self.infiles[0]).spectral_windows[self.spwids[0]]
        return _ref_spwobj.channels.chan_freqs.delta > 0


class SDImagingWorker(basetask.StandardTaskTemplate):
    """Worker class of imaging task."""

    Inputs = SDImagingWorkerInputs

    is_multi_vis_task = True

    def prepare(self):
        """Execute imaging process of sdimaging task.

        Returns:
            SDImagingResultItem instance
        """
        inputs = self.inputs
        context = self.inputs.context
        infiles = inputs.infiles
        outfile = inputs.outfile
        edge = inputs.edge
        antid_list = inputs.antids
        spwid_list = inputs.spwids
        fieldid_list = inputs.fieldids
        imagemode = inputs.mode
        mses = [context.observing_run.get_ms(name) for name in infiles]
        v_spwids = [context.observing_run.real2virtual_spw_id(i, ms) for i, ms in zip(spwid_list, mses)]
        rep_ms = mses[0]
        rep_fieldid = fieldid_list[0]
        ant_name = rep_ms.antennas[antid_list[0]].name
        source_name = rep_ms.fields[rep_fieldid].clean_name
        phasecenter, cellx, celly, nx, ny, org_direction = \
            self._get_map_coord(inputs, context, infiles, antid_list, spwid_list, fieldid_list)
        is_eph_obj = rep_ms.get_fields(field_id=rep_fieldid)[0].source.is_eph_obj

        if self._do_imaging(infiles, antid_list, spwid_list, fieldid_list, outfile, imagemode,
                            edge, phasecenter, cellx, celly, nx, ny):
            specmode = 'cubesource' if is_eph_obj else 'cube'
            # missing attributes in result instance will be filled in by the
            # parent class
            image_item = imagelibrary.ImageItem(imagename=outfile,
                                                sourcename=source_name,
                                                spwlist=v_spwids,  # virtual
                                                specmode=specmode,
                                                sourcetype='TARGET',
                                                org_direction=org_direction)
            image_item.antenna = ant_name  # name #(group name)
            outcome = {}
            outcome['image'] = image_item
            is_frequency_channel_reversed = not self.inputs.is_freq_axis_ascending
            result = resultobjects.SDImagingResultItem(task=None,
                                                       success=True,
                                                       outcome=outcome,
                                                       frequency_channel_reversed=is_frequency_channel_reversed)
        else:
            # Imaging failed due to missing valid data
            result = resultobjects.SDImagingResultItem(task=None,
                                                       success=False,
                                                       outcome=None)

        return result

    def analyse(self, result: basetask.Results) -> basetask.Results:
        """Inherited method. NOT USE."""
        return result

    def _get_map_coord(self, inputs: SDImagingWorkerInputs, context: Context, infiles: List[str],
                       ant_list: List[int], spw_list: List[int], field_list: List[int]) \
            -> Tuple[str, 'sdtyping.Angle', 'sdtyping.Angle', int, int, 'sdtyping.Direction']:
        """Gather or generate the input image parameters.

        Args:
            inputs: SDImagingWorkerInputs object
            context: pipeline context
            infiles: list of input file names
            ant_list: list of anntena IDs
            spw_list: list of SPW IDs
            field_list: list of field IDs

        Raises:
            RuntimeError: an exception which is raised from ImageCoordinateUtil

        Returns:
            Image coordinate
        """
        params = (inputs.phasecenter, inputs.cellx, inputs.celly, inputs.nx, inputs.ny, inputs.org_direction)
        coord_set = (params.count(None) == 0) or ((params.count(None) == 1) and inputs.org_direction is None)
        if coord_set:
            return params
        else:
            params = ImageCoordinateUtil(context, infiles, ant_list, spw_list, field_list)
            if not params:
                raise RuntimeError("No valid data")
            return params

    def _do_imaging(self, infiles: List[str], antid_list: List[int], spwid_list: List[int],
                    fieldid_list: List[int], imagename: str, imagemode: str, edge: List[int],
                    phasecenter: str, cellx: 'sdtyping.Angle', celly: 'sdtyping.Angle', nx: int, ny: int) -> bool:
        """Process imaging.

        Args:
            infiles: list of input file names
            antid_list: list of anntena IDs
            spwid_list: list of SPW IDs
            fieldid_list: list of field IDs
            imagename: output image file name
            imagemode: imaging mode
            edge: numbers of edge channels to be excluded in imaging. When edge=[10, 20], 10 and 20 channels
                  in the beginning and at the end of a spectral window, respectively, are excluded.
            phasecenter: Image center
            cellx: size(unit and value) per pixel of image axis x
            celly: size(unit and value) per pixel of image axis y
            nx: the number of pixels x
            ny: the number of pixels y

        Returns:
            Whether an image file with valid pixels has been generated.
        """
        context = self.inputs.context
        reference_data = context.observing_run.get_ms(infiles[0])
        ref_spwid = spwid_list[0]

        LOG.debug('Members to be processed:')
        for (m, a, s, f) in zip(infiles, antid_list, spwid_list, fieldid_list):
            LOG.debug('\tMS %s: Antenna %s Spw %s Field %s' % (os.path.basename(m), a, s, f))

        # Check for ephemeris source
        # known_ephemeris_list = ['MERCURY', 'VENUS', 'MARS', 'JUPITER', 'SATURN', 'URANUS', 'NEPTUNE', 'PLUTO', 'SUN',
        #                         'MOON']

        ephemsrcname = ''
        reference_field = reference_data.fields[fieldid_list[0]]
        source_name = reference_field.name
        is_eph_obj = reference_data.get_fields(fieldid_list[0])[0].source.is_eph_obj
        is_known_eph_obj = reference_data.get_fields(fieldid_list[0])[0].source.is_known_eph_obj

        # for ephemeris sources without ephemeris data in MS (eg. not for ALMA)
        # if not is_eph_obj:
        if is_known_eph_obj:
            # me = casa_tools.measures
            # ephemeris_list = me.listcodes(me.direction())['extra']
            # known_ephemeris_list = numpy.delete( ephemeris_list, numpy.where(ephemeris_list=='COMET') )
            # if source_name.upper() in known_ephemeris_list:
            ephemsrcname = source_name.upper()

        # baseline
        # baseline = '0&&&'

        # mode
        mode = 'channel'

        # stokes
        stokes = self.inputs.stokes

        # PIPE-858
        # Set stokes to 'pseudoI' if 'I' is specified. That is to make output images generated by tsdimaging
        # compatible with those by sdimaging. Setting stokes='I' in sdimaging is equivalent to
        # stokes='pseudoI' in tsdimaging. Please see
        #
        #     https://casadocs.readthedocs.io/en/stable/notebooks/synthesis_imaging.html#Types-of-images
        #
        # for difference between 'I' and 'pseudoI'.
        #
        # Change of parameter is confined to this method not to affect metadata of output images.
        if stokes == 'I':
            stokes = 'pseudoI'

        # start, nchan, step
        ref_spwobj = reference_data.spectral_windows[ref_spwid]
        total_nchan = ref_spwobj.num_channels
        if total_nchan == 1:
            start = 0
            step = 1
            nchan = 1
        else:
            nchan = total_nchan - sum(edge)
            # set start and step values to make the frequency axis of all FITS in ascending order.
            if numpy.logical_not(self.inputs.is_freq_axis_ascending):
                step = -1
                start = nchan - edge[1] - 1
            else:
                step = 1
                start = edge[0]
        # ampcal
        if imagemode == 'AMPCAL':
            step = nchan
            nchan = 1
        # restfreq
        restfreq = self.inputs.restfreq
        if not isinstance(restfreq, str):
            raise RuntimeError("Invalid type for restfreq '{0}' (not a string)".format(restfreq))
        if restfreq.strip() == '':
            # if restfreq is NOT given by user
            # first try using SOURCE.REST_FREQUENCY
            # if it is not available, use SPECTRAL_WINDOW.REF_FREQUENCY instead
            source_id = reference_field.source_id
            rest_freq_value = utils.get_restfrequency(vis=infiles[0], spwid=ref_spwobj.id, source_id=source_id)
            rest_freq_unit = 'Hz'
            if rest_freq_value is None:
                # REST_FREQUENCY is not defined in the SOURCE tableq
                rest_freq = ref_spwobj.ref_frequency
                rest_freq_value = numpy.double(rest_freq.value)
                rest_freq_unit = rest_freq.units['symbol']
            if rest_freq_value is not None:
                qa = casa_tools.quanta
                restfreq = qa.tos(qa.quantity(rest_freq_value, rest_freq_unit))
            else:
                raise RuntimeError("Could not get reference frequency of Spw %d" % ref_spwid)
        else:
            # restfreq is given by user
            # check if user provided restfreq is valid
            qa = casa_tools.quanta
            x = qa.quantity(restfreq)
            if x['value'] <= 0:
                raise RuntimeError("Invalid restfreq '{0}' (must be positive)".format(restfreq))
            x = qa.convert(x, 'Hz')
            if qa.getunit(x) != 'Hz':
                raise RuntimeError("Invalid restfreq '{0}' (inappropriate unit)".format(restfreq))

        # outframe
        outframe = '' if is_eph_obj else 'LSRK'

        # specmode
        specmode = 'cubesource' if is_eph_obj else 'cube'

        # gridfunction
        gridfunction = 'SF'

        # truncate, gwidth, jwidth, and convsupport
        truncate = gwidth = jwidth = -1  # defaults (not used)

        # PIPE-689: convsupport should be 3 for NRO Pipeline
        imaging_policy = observatory_policy.get_imaging_policy(context)
        convsupport = imaging_policy.get_convsupport()

        cleanup_params = ['outfile', 'infiles', 'spw', 'scan']

        # override phasecenter for sources with ephemeris table
        if is_eph_obj:
            phasecenter = 'TRACKFIELD'
            LOG.info("phasecenter is overrided with \'TRACKFIELD\'")

        # brightness unit
        brightnessunit = get_brightness_unit(infiles)

        qa = casa_tools.quanta
        image_args = {'mode': mode,
                      'intent': "OBSERVE_TARGET#ON_SOURCE",
                      'nchan': nchan,
                      'start': start,
                      'width': step,
                      # the task only accepts lower letter
                      'outframe': outframe.lower(),
                      'gridfunction': gridfunction,
                      'convsupport': convsupport,
                      'truncate': truncate,
                      'gwidth': gwidth,
                      'jwidth': jwidth,
                      'imsize': [nx, ny],
                      'cell': [qa.tos(cellx), qa.tos(celly)],
                      'phasecenter': phasecenter,
                      'restfreq': restfreq,
                      'stokes': stokes,
                      'ephemsrcname': ephemsrcname,
                      'brightnessunit': brightnessunit}

        # remove existing image explicitly
        for rmname in [imagename, imagename.rstrip('/') + '.weight']:
            if os.path.exists(rmname):
                shutil.rmtree(rmname)

        # imaging
        infile_list = []
        spwsel_list = []
        fieldsel_list = []
        antsel_list = []
        for (msname, ant, spw, field) in zip(infiles, antid_list, spwid_list, fieldid_list):
            LOG.debug('Registering data to image: vis=\'%s\', ant=%s, spw=%s, field=%s%s'
                      % (msname, ant, spw, field, (' (ephemeris source)' if ephemsrcname != '' else '')))
            infile_list.append(msname)
            spwsel_list.append(str(spw))
            fieldsel_list.append(str(field))
            antsel_list.append(str(ant))
        # collapse selection if possible
        spwsel_list = spwsel_list[0] if len(set(spwsel_list)) == 1 else spwsel_list
        fieldsel_list = fieldsel_list[0] if len(set(fieldsel_list)) == 1 else fieldsel_list
        antsel_list = antsel_list[0] if len(set(antsel_list)) == 1 else antsel_list
        # set-up image dependent parameters
        for p in cleanup_params:
            image_args[p] = None
        image_args['outfile'] = imagename
        image_args['infiles'] = infile_list
        image_args['spw'] = spwsel_list
        image_args['field'] = fieldsel_list
        image_args['antenna'] = antsel_list
        LOG.debug('Executing sdimaging task: args=%s' % (image_args))

        # execute job
        image_args['specmode'] = specmode
        image_job = casa_tasks.tsdimaging(**image_args)
        self._executor.execute(image_job)
        # tsdimaging changes the image filename, workaround to revert it
        imagename_tmp = imagename + '.image'
        os.rename(imagename_tmp, imagename)

        # check imaging result
        imagename = image_args['outfile']
        weightname = imagename + '.weight'
        if not os.path.exists(imagename) or not os.path.exists(weightname):
            LOG.error("Generation of %s failed" % imagename)
            return False

        # check for valid pixels (non-zero weight)
        # Task sdimaging does not fail even if no data is gridded to image.
        # In that case, image is not masked, no restoring beam is set to
        # image, and all pixels in corresponding weight image is zero.
        with casa_tools.ImageReader(weightname) as ia:
            sumsq = ia.statistics()['sumsq'][0]
        if sumsq == 0.0:
            LOG.warning("No valid pixel found in image, %s. Discarding the image from futher processing." % imagename)
            return False

        virtual_spw_id = context.observing_run.real2virtual_spw_id(ref_spwid, reference_data)

        if numpy.logical_not(self.inputs.is_freq_axis_ascending):
            LOG.info(f"Channel frequencies in spw {virtual_spw_id} is in decending order in observation data. "
                     f"They will be reversed to have the frequency axis of output image cube {imagename} in ascending order.")

        return True
