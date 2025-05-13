"""Set of base classes and utility functions for display modules."""
import abc
import collections
import copy
import enum
import itertools
import math
import os
import scipy
from typing import Generator, List, NoReturn, Optional, Tuple, Union

from casatools import coordsys as casa_coordsys  # Used for annotation purpose.

import matplotlib
import matplotlib.figure as figure
from matplotlib.axes import Axes
from matplotlib.dates import date2num, DateFormatter, MinuteLocator
import matplotlib.gridspec as gridspec
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.displays.pointing as pointing
from pipeline.infrastructure import casa_tools
from pipeline.domain.singledish import MSReductionGroupDesc
from pipeline.infrastructure.renderer.logger import Plot
from pipeline.infrastructure.utils import absolute_path
from .utils import mjd_to_datetime

LOG = infrastructure.get_logger(__name__)

DPISummary = 90
# DPIDetail = 120
# DPIDetail = 130
DPIDetail = 260
LightSpeedQuantity = casa_tools.quanta.constants('c')
LightSpeed = casa_tools.quanta.convert(LightSpeedQuantity, 'km/s')['value']  # speed of light in km/s

sd_polmap = {0: 'XX', 1: 'YY', 2: 'XY', 3: 'YX'}

NoData = -32767.0
NoDataThreshold = NoData + 10000.0


def mjd_to_plotval(mjd_list: Union[List[float], np.ndarray]) -> np.ndarray:
    """Convert list of MJD values to Matplotlib dates.

    Args:
        mjd_list: Sequence of MJD values in day.

    Returns:
        np.ndarray: Sequence of Matplotlib dates.
    """
    datetime_list = [mjd_to_datetime(x) for x in mjd_list]
    return date2num(datetime_list)


def is_invalid_axis_range(xmin: float, xmax: float, ymin: float, ymax: float) -> bool:
    """Check if given range is valid.

    Args:
        xmin: lower limit of the x-axis. Can be NaN or Inf.
        xmax: upper limit of the x-axis. Can be NaN or Inf.
        ymin: lower limit of the y-axis. Can be NaN or Inf.
        ymax: upper limit of the y-axis. Can be NaN or Inf.

    Returns:
        Return False if any of xmin, xmax, ymin, ymax is NaN or Inf.
        Otherwise, True is returned.
    """
    axis_ranges = [xmin, xmax, ymin, ymax]

    def _is_invalid(v):
        # check if given value is Inf or NaN or masked
        return (np.isfinite(v) is False) or np.ma.is_masked(v)

    zero_range_x = xmax - xmin == 0
    zero_range_y = ymax - ymin == 0
    invalid_values = any(map(_is_invalid, axis_ranges))

    return zero_range_x or zero_range_y or invalid_values


class CustomDateFormatter(DateFormatter):
    """Customized date formatter.

    Default format of the label is same as DateFormtter.
    For the leftmost label as well as when date is changed,
    this formatter puts extra label '%Y/%m/%d' beneath the
    deafult one.
    """

    def __call__(self, x: float, pos: float = 0) -> str:
        """Return the label for tick value x at position pos.

        Args:
            x: tick value
            pos: position. Defaults to 0.

        Returns:
            str: tick label.
        """
        fmt_saved = self.fmt
        if pos == 0 or x % 1.0 == 0.0:
            self.fmt = '%H:%M\n%Y/%m/%d'
        tick = DateFormatter.__call__(self, x, pos)
        self.fmt = fmt_saved
        return tick


def utc_formatter(fmt: str = '%H:%M') -> CustomDateFormatter:
    """Generate CustomDateFormatter instance.

    Generate CustomDateFormatter instance with the format
    given by fmt.

    Args:
        fmt: Tick format. Defaults to '%H:%M'.

    Returns:
        CustomDateFormatter: formatter instance.
    """
    return CustomDateFormatter(fmt)


def utc_locator(start_time: Optional[float] = None,
                end_time: Optional[float] = None) -> MinuteLocator:
    """Generate MinuteLocator instance.

    Generate MinuteLocator instance. If either start_time or end_time is None,
    default MinuteLocator instance is returned. Otherwise, tick interval is
    adjusted according to start_time and end_time.

    Args:
        start_time: Leftmost value of time sequence.
                    Can be minimum or maximum. Defaults to None.
        end_time: Rightmost value of time sequence.
                  Can be minimum or maximum. Defaults to None.

    Returns:
        MinuteLocator: locator instance.
    """
    if start_time is None or end_time is None:
        return MinuteLocator()
    else:
        dt = abs(end_time - start_time) * 1440.0  # day -> minutes
        if dt < 2:
            tick_interval = 1
        else:
            tick_interval = max(int(dt / 10), 2)
            tick_candidates = np.asarray([i for i in range(1, 61) if 60 % i == 0])
            tick_interval = tick_candidates[np.argmin(abs(tick_candidates - tick_interval))]

        # print tick_interval
        return MinuteLocator(byminute=list(range(0, 60, tick_interval)))


class SingleDishDisplayInputs(object):
    """Represents inputs to Display classes."""

    def __init__(self,
                 context: infrastructure.launcher.Context,
                 result: infrastructure.api.Results) -> None:
        """Construct SingleDishDisplayInputs instance.

        Args:
            context: Pipeline context.
            result: Pipeline task execution result.
        """
        self.context = context
        self.result = result

    @property
    def isnro(self) -> bool:
        """Check if given datasets are taken by NRO45m telescope.

        Raises:
            RuntimeError: Data from two or more observatories are mixed.

        Returns:
            bool: True if data is from NRO45m, otherwise False
        """
        arrays = {ms.antenna_array.name for ms in self.context.observing_run.measurement_sets}
        if len(arrays) != 1:
            raise RuntimeError('array name is not unique: {}'.format(list(arrays)))

        return 'NRO' in arrays


class SpectralImage(object):
    """Representation of four-dimensional spectral image."""

    @property
    def data(self) -> np.ndarray:
        """Retrun image data."""
        with casa_tools.ImageReader(self.imagename) as ia:
            data = ia.getchunk()

        return data

    @property
    def mask(self) -> np.ndarray:
        """Return boolean image mask."""
        with casa_tools.ImageReader(self.imagename) as ia:
            mask = ia.getchunk(getmask=True)

        return mask

    def __init__(self, imagename: str) -> None:
        """Construct SpectralImage instance.

        Args:
            imagename: Name of the image.
        """
        qa = casa_tools.quanta
        if not isinstance(imagename, str):
            raise ValueError('imagename must be string')

        if not imagename:
            raise ValueError('imagename must be a name of the image (CASA image or FITS).')

        self.imagename = imagename
        # read data to storage
        with casa_tools.ImageReader(imagename) as ia:
            self.image_shape = ia.shape()
            coordsys = ia.coordsys()
            self._load_coordsys(coordsys)
            coordsys.done()
            bottom = ia.toworld(np.zeros(len(self.image_shape), dtype=int), 'q')['quantity']
            top = ia.toworld(self.image_shape - 1, 'q')['quantity']
            direction_keys = ['*{}'.format(x + 1) for x in self.id_direction]
            ra_min = bottom[direction_keys[0]]
            ra_max = top[direction_keys[0]]
            if qa.gt(ra_min, ra_max):
                ra_min, ra_max = ra_max, ra_min
            self.ra_min = ra_min
            self.ra_max = ra_max
            self.dec_min = bottom[direction_keys[1]]
            self.dec_max = top[direction_keys[1]]
            self._brightnessunit = ia.brightnessunit()
            beam = ia.restoringbeam()
        self._beamsize_in_deg = qa.convert(qa.sqrt(qa.mul(beam['major'], beam['minor'])), 'deg')['value']

    def _load_coordsys(self, coordsys: casa_coordsys) -> None:
        """Load axes information of coordinate system.

        Args:
            coordsys: coordsys instance of the image.
        """
        coord_types = coordsys.axiscoordinatetypes()
        self._load_id_coord_types(coord_types)
        self.units = coordsys.units()
        self.direction_reference = coordsys.referencecode('dir')[0]
        self.frequency_frame = coordsys.getconversiontype('spectral')
        self.stokes_string = ''.join(coordsys.stokes())
        self.stokes = coordsys.stokes()
        self.rest_frequency = coordsys.restfrequency()
        self.refpixs = coordsys.referencepixel()['numeric']
        self.refvals = coordsys.referencevalue()['numeric']
        self.increments = coordsys.increment()['numeric']

    def _load_id_coord_types(self, coord_types: casa_coordsys) -> None:
        """Load indices for coordinate axes.

        Args:
            coord_types: coordsys instance of the image.
        """
        id_direction = coord_types.index('Direction')
        self.id_direction = [id_direction, id_direction + 1]
        self.id_spectral = coord_types.index('Spectral')
        self.id_stokes = coord_types.index('Stokes')
        LOG.debug('id_direction=%s', self.id_direction)
        LOG.debug('id_spectral=%s', self.id_spectral)
        LOG.debug('id_stokes=%s', self.id_stokes)

    @property
    def nx(self) -> int:
        """Return number of pixels for horizontal (longitude) axis."""
        return self.image_shape[self.id_direction[0]]

    @property
    def ny(self) -> int:
        """Return number of pixels for vertical (latitude) axis."""
        return self.image_shape[self.id_direction[1]]

    @property
    def nchan(self) -> int:
        """Return number of pixels (channels) for spectral axis."""
        return self.image_shape[self.id_spectral]

    @property
    def npol(self) -> int:
        """Return number of pixels (polarizations or correlations) for Stokes axis."""
        return self.image_shape[self.id_stokes]

    @property
    def brightnessunit(self) -> str:
        """Return brightness unit of the image."""
        return self._brightnessunit

    @property
    def beam_size(self) -> float:
        """Return beam diameter in degree."""
        return self._beamsize_in_deg

    def to_velocity(self,
                    frequency: Union[float, np.ndarray],
                    freq_unit: str = 'GHz') -> Union[float, np.ndarray]:
        """Convert frequency or array of frequency to velocity.

        Args:
            frequency: Frequency value(s).
            freq_unit: Frequency Unit. Defaults to 'GHz'.

        Returns:
            Union[float, np.ndarray]: Velocity value(s).
        """
        qa = casa_tools.quanta
        if self.rest_frequency['unit'] != freq_unit:
            vrf = qa.convert(self.rest_frequency, freq_unit)['value']
        else:
            vrf = self.rest_frequency['value']
        return (1.0 - (frequency / vrf)) * LightSpeed

    def spectral_axis(self, unit: str = 'GHz') -> Tuple[float, float, float]:
        """Return conversion information for spectral axis.

        Three-tuple required for conversion between pixel and world spectral
        axis is returned. The tuple consists of reference pixel, reverence value,
        and increment for spectral axis.

        Args:
            unit: Frequency unit. Defaults to 'GHz'.

        Returns:
            Tuple[float, float, float]: (refpix, refval, increment) for spectral axis.
        """
        return self.__axis(self.id_spectral, unit=unit)

    def direction_axis(self, idx: int, unit: str = 'deg') -> Tuple[float, float, float]:
        """Return conversion information for direction axes.

        Three-tuple required for conversion between pixel and world direction
        axes is returned. The tuple consists of reference pixel, refrence value,
        and increment for direction axis. Direction index must be given to specify
        either longitude (0) or latitude (1) axis.

        Args:
            idx: Index for direction axes.
            unit: Direction unit. Defaults to 'deg'.

        Returns:
            Tuple[float, float, float]: (refpix, refval, increment) for direction axis
                                        specified by idx.
        """
        return self.__axis(self.id_direction[idx], unit=unit)

    def __axis(self, idx: int, unit: str) -> Tuple[float, float, float]:
        """Return conversion information for specified image axis.

        Three-tuple required for conversion between pixel and world direction
        axes is returned. The tuple consists of reference pixel, refrence value,
        and increment for the axis specified by idx.

        Args:
            idx: Axis index.
            unit: Unit string.

        Returns:
            Tuple[float, float, float]: (refpix, refval, increment) for
                                        the axis specified by idx.
        """
        qa = casa_tools.quanta
        refpix = self.refpixs[idx]
        refval = self.refvals[idx]
        increment = self.increments[idx]

        _unit = self.units[idx]
        if _unit != unit:
            refval = qa.convert(qa.quantity(refval, _unit), unit)['value']
            increment = qa.convert(qa.quantity(increment, _unit), unit)['value']
        return (refpix, refval, increment)


ChannelSelection = enum.Enum('ChannelSelection', ['ALL', 'LINE_ONLY', 'LINE_FREE'])


class Moment(enum.IntEnum):
    INTEGRATED = 0
    MAXIMUM = 8


MomentSpec = collections.namedtuple('MomentSpec', 'moments chans')


class SDImageDisplayInputs(SingleDishDisplayInputs):
    """Manages input data for plotter classes for single dish images."""

    MomentMapList = [
        MomentSpec(moments=[Moment.MAXIMUM], chans=ChannelSelection.ALL),
        MomentSpec(moments=[Moment.INTEGRATED, Moment.MAXIMUM], chans=ChannelSelection.LINE_FREE)
    ]

    def __init__(self,
                 context: infrastructure.launcher.Context,
                 result: infrastructure.api.Results) -> None:
        """Construct SDImageDisplayInputs instance.

        Args:
            context: Pipeline context.
            result: Pipeline task execution result.
        """
        super(SDImageDisplayInputs, self).__init__(context, result)
        self.image = SpectralImage(self.imagename)

    @property
    def imagename(self) -> str:
        """Return name of the single dish image."""
        return self.result.outcome['image'].imagename

    def moment_imagename(self, moments: Union[List[Moment], Moment], chans: ChannelSelection) -> str:
        """Return name of the moment image.

        If number of moments is 1, moment image name will include moment
        type. On the other hand, moment image name will not contain
        moment type if multiple moment types are specified. That is
        because immoments treats given image name as a prefix when
        the task computes multiple moments at once.

        Args:
            moments: Type of moment or list of them
            chans: Channel selection spec

        Returns:
            Name of moment image name
        """
        name = self.imagename.rstrip('/') + f'.{chans.name.lower()}'

        if isinstance(moments, Moment):
            moments = [moments]

        if len(moments) == 1:
            name += f'.{moments[0].name.lower()}'

        return name

    @property
    def spw(self) -> int:
        """Return spectral window (spw) id for the image."""
        spwlist = self.result.outcome['image'].spwlist
        if isinstance(spwlist, list):
            return spwlist[0]
        else:
            return spwlist

    @property
    def vis(self) -> Optional[str]:
        """Return name of the MeasurementSet if available.

        If no MeasurementSet is associated with the result,
        None is returned.
        """
        if 'vis' in self.result.outcome:
            return self.result.outcome['vis']
        else:
            return None

    @property
    def antenna(self) -> str:
        """Return name of the antenna registered to the image.

        In single dish pipeline, per-antenna images are created
        first, and then combined them into one image. For the
        former case, antenna name is returned while the special
        string "COMBINED" is returned.

        Returns:
            str: Name of the antenna.
        """
        return self.result.outcome['image'].antenna

    @property
    def reduction_group(self) -> MSReductionGroupDesc:
        """Return ReductionGroupDesc instance.

        Return ReductionGroupDesc instance corresponding to the reduction group
        associated to the image.
        """
        group_id = self.result.outcome['reduction_group_id']
        return self.context.observing_run.ms_reduction_group[group_id]

    @property
    def msid_list(self) -> List[int]:
        """Return list of indices for MeasurementSets.

        The list specifies the MeasurementSets that are used to
        generate the image.

        Returns:
            List[int]: index list for MeasurementSets.
        """
        return self.result.outcome['file_index']

    @property
    def antennaid_list(self) -> List[int]:
        """Return list of antenna ids.

        Return list of antenna ids corresponding to antenna
        name returned by self.antenna. Order of the index is
        consistent with self.msid_list, i.e. antnenaid_list[0]
        corresponds to the antenna id for the MeasurementSet
        specified by msid_list[0]. For 'COMBINED' antenna,
        indices for all the antennas are returned.

        Returns:
            List[int]: List of antenna ids.
        """
        return self.result.outcome['assoc_antennas']

    @property
    def fieldid_list(self) -> List[int]:
        """Return list of field ids.

        Return list of field ids. Order of the index is
        consistent with self.msid_list, i.e. fieldid_list[0]
        corresponds to the field id for the MeasurementSet
        specified by msid_list[0].

        Returns:
            List[int]: List of field ids.
        """
        return self.result.outcome['assoc_fields']

    @property
    def spwid_list(self) -> List[int]:
        """Return list of spectral windo (spw) ids.

        Return list of spw ids. Order of the index is
        consistent with self.msid_list, i.e. spwid_list[0]
        corresponds to the spw id for the MeasurementSet
        specified by msid_list[0].

        Returns:
            List[int]: List of spw ids.
        """
        return self.result.outcome['assoc_spws']

    @property
    def stage_number(self) -> int:
        """Return Processing stage id."""
        return self.result.stage_number

    @property
    def stage_dir(self) -> str:
        """Return weblog subdirectory name for the stage."""
        return os.path.join(self.context.report_dir,
                            'stage{}'.format(self.stage_number))

    @property
    def source(self) -> str:
        """Return name of the target source."""
        return self.result.outcome['image'].sourcename

    @property
    def contamination_plot(self) -> str:
        """Return file name of the contamination plot."""
        return self.imagename.rstrip('/') + '.contamination.png'

    def valid_lines(self, is_freq_chan_reversed_image: bool=False) -> List[List[int]]:
        """Return list of chnnel ranges of valid spectral lines."""
        group_desc = self.reduction_group
        ant_index = self.antennaid_list
        spwid_list = self.spwid_list
        msid_list = self.msid_list
        fieldid_list = self.fieldid_list

        line_list = []

        msobj_list = self.context.observing_run.measurement_sets
        msname_list = [absolute_path(msobj.name) for msobj in msobj_list]
        for g in group_desc:
            found = False
            for (msid, ant, fid, spw) in zip(msid_list, ant_index, fieldid_list, spwid_list):
                group_msid = msname_list.index(absolute_path(g.ms.name))
                if group_msid == msid and g.antenna_id == ant and \
                   g.field_id == fid and g.spw_id == spw:
                    found = True
                    break
            if found:
                for ll in copy.deepcopy(g.channelmap_range):
                    if ll not in line_list and ll[2] is True:
                        line_list.append(ll)
        if is_freq_chan_reversed_image:
            _right_edge = float(self.image.nchan - 1)
            for ll in line_list:
                ll[0] = _right_edge - ll[0]
        return line_list

    def create_channel_mask(self, channel_selection: ChannelSelection) -> str:
        """Generate channel mask for immoments according to channel selection enum.

        Args:
            channel_selection: Channel selection enum.

        Returns:
            Channel selection string.
        """
        if channel_selection == ChannelSelection.ALL:
            # use all channels
            return ''

        # convert line list into (start, end) list
        range_list = []
        for line in self.valid_lines():
            line_center, line_width = line[:2]
            line_start = int(round(line_center - line_width / 2))
            line_end = int(round(line_start + line_width))
            range_list.append((line_start, line_end))

        # invert range if line-free channels are requested
        if channel_selection == ChannelSelection.LINE_FREE:
            range_list = invert_range_list(range_list, self.image.nchan)

        # convert line list into channel selection string
        # range_list is inclusive at the start while exclusive
        # at the end, i.e., [start, end)
        # On the other hand, CASA's channel selection is inclusive
        # at both ends, i.e., [start, end]
        return ';'.join([f'{s}~{e - 1}' for s, e in range_list])

    def get_line_free_channels(self) -> List[int]:
        """Get list of line-free channels.

        Returns:
            Indices of line-free channels
        """
        # per-channel mask to diffentiate line/line-free regions
        # line regions: False
        # line-free regions: True
        is_line_free = np.ones(self.image.nchan, dtype=bool)

        # invalidate line regions
        for line in self.valid_lines():
            line_center, line_width = line[:2]
            line_start = int(round(line_center - line_width / 2))
            line_end = int(round(line_start + line_width))
            is_line_free[line_start:line_end] = False

        return np.where(is_line_free)[0]

    def compute_per_channel_stats(self) -> dict:
        """Compute per-channel statistics of cube image.

        Returns:
            Statistics dictionary
        """
        spectral_axis = self.image.id_spectral
        axes = list(range(len(self.image.image_shape)))
        axes.pop(spectral_axis)
        with casa_tools.ImageReader(self.imagename) as ia:
            # cf. hif/tasks/tclean/tclean.py cube_stats_masked
            stats = ia.statistics(
                robust=True, stretch=True,
                axes=axes, algorithm='chauvenet', maxiter=5
            )
        return stats


def invert_range_list(range_list: List[List[int]], nchan: int) -> List[List[int]]:
    """Invert channel range list.

    Overlap among ranges is handled properly.

    Args:
        range_list: List of (start, end) ranges.
        nchan: Length of target array.

    Returns:
        Inverted list of ranges.
    """
    # merge range
    arr = np.zeros(nchan, dtype=bool)
    for start, end in range_list:
        # range_list is inclusive at the beginning while exclusive
        # at the end, i.e., [start, end)
        arr[start:end] = True

    # detect change of value
    idx = np.where(arr[1:] != arr[:-1])[0] + 1
    if not arr[0]:
        idx = np.insert(idx, 0, 0)
    if not arr[-1]:
        idx = np.append(idx, nchan)

    inverted = [list(x) for x in idx.reshape(len(idx) // 2, 2)]

    return inverted


class SDCalibrationDisplay(object, metaclass=abc.ABCMeta):
    """Base plotter class for single-dish calibration tasks."""

    Inputs = SingleDishDisplayInputs

    def __init__(self, inputs: SingleDishDisplayInputs) -> None:
        """Construct SDCalibrationDisplay instance.

        Args:
            inputs: Inputs instance.
        """
        self.inputs = inputs

    def plot(self) -> List[Plot]:
        """Generate plots according to the provided results.

        Returns:
            List[Plot]: List of Plot instances.
        """
        results = self.inputs.result
        report_dir = self.inputs.context.report_dir
        stage_dir = os.path.join(report_dir, 'stage{}'.format(results.stage_number))
        plots = []
        for result in results:
            if result is None or result.outcome is None:
                plot = None
            else:
                plot = self.doplot(result, stage_dir)

            if plot is not None:
                plots.append(plot)
        return plots

    @abc.abstractmethod
    def doplot(self, result: infrastructure.api.Results, stage_dir: str) -> NoReturn:
        """Generate plot from the result instance.

        This method must be implemented in the subclasses.
        The result should be single Results instance rather than ResultsList.

        Args:
            result: Pipeline task execution result.
            stage_dir: Name of pipeline weblog subdirectory.

        Raises:
            NotImplementedError: This method is not implemented in the base class.
        """
        raise NotImplementedError()


class SDImageDisplay(object, metaclass=abc.ABCMeta):
    """Base plotter class for imaging tasks."""

    Inputs = SDImageDisplayInputs

    def __init__(self, inputs: SDImageDisplayInputs) -> None:
        """Construct SDImageDisplay instance.

        Args:
            inputs: Inputs instance.
        """
        self.inputs = inputs
        self.imagename = self.inputs.imagename

        # Figure instance for plotting
        self.figure = figure.Figure()

    def init(self) -> None:
        """Initialize plotter using specifiec image."""
        # self.image = SpectralImage(self.imagename)
        qa = casa_tools.quanta
        self.nchan = self.image.nchan
        self.nx = self.image.nx
        self.ny = self.image.ny
        self.npol = self.image.npol
        self.brightnessunit = self.image.brightnessunit
        self.direction_reference = self.image.direction_reference
        (refpix, refval, increment) = self.image.spectral_axis(unit='GHz')
        self.frequency = np.array([refval + increment * (i - refpix) for i in range(self.nchan)])
        self.velocity = self.image.to_velocity(self.frequency, freq_unit='GHz')
        self.frequency_frame = self.image.frequency_frame
        self.x_max = self.nx - 1
        self.x_min = 0
        self.y_max = self.ny - 1
        self.y_min = 0
        self.ra_min = qa.convert(self.image.ra_min, 'deg')['value']
        self.ra_max = qa.convert(self.image.ra_max, 'deg')['value']
        self.dec_min = qa.convert(self.image.dec_min, 'deg')['value']
        self.dec_max = qa.convert(self.image.dec_max, 'deg')['value']
        self.stokes_string = self.image.stokes_string

        LOG.debug('(ra_min,ra_max)=(%s,%s)', self.ra_min, self.ra_max)
        LOG.debug('(dec_min,dec_max)=(%s,%s)', self.dec_min, self.dec_max)

        self.beam_size = self.image.beam_size
        self.beam_radius = self.beam_size / 2.0
        self.grid_size = self.beam_size / 3.0
        LOG.debug('beam_radius=%s', self.beam_radius)
        LOG.debug('grid_size=%s', self.grid_size)

        # 2008/9/20 Dec Effect has been taken into account
        self.aspect = 1.0 / math.cos(0.5 * (self.dec_min + self.dec_max) / 180.0 * 3.141592653)

    @property
    def context(self) -> infrastructure.launcher.Context:
        """Return Pipeline context."""
        return self.inputs.context

    @property
    def stage_dir(self) -> str:
        """Return weblog subdirectory."""
        return self.inputs.stage_dir

    @property
    def image(self) -> SpectralImage:
        """Return SpectralImage instance."""
        return self.inputs.image

    @property
    def spw(self) -> int:
        """Return spw id."""
        return self.inputs.spw

    @property
    def antenna(self) -> str:
        """Return antenna name."""
        return self.inputs.antenna

    @property
    def vis(self) -> str:
        """Return MS name."""
        return self.inputs.vis

    @property
    def data(self) -> Optional[np.ndarray]:
        """Return image data as numpy float array."""
        return self.image.data if self.image is not None else None

    @property
    def mask(self) -> Optional[np.ndarray]:
        """Return image mask as numpy bool array.

        Mask is True for valid pixels while False for invalid pixels.

        Returns:
            Optional[np.ndarray]: Image mask.
        """
        return self.image.mask if self.image is not None else None

    @property
    def id_spectral(self) -> int:
        """Return axis index for spectral axis."""
        return self.image.id_spectral if self.image is not None else None

    @property
    def id_stokes(self) -> int:
        """Return axis index for Stokes or polarization axis."""
        return self.image.id_stokes if self.image is not None else None

    @property
    def num_valid_spectrum(self) -> np.ndarray:
        """Return Number of valid spectral data accumulated to each position."""
        return self.__reshape2d(self.inputs.result.outcome['validsp'])

    @property
    def rms(self) -> np.ndarray:
        """Return rms for each position."""
        return self.__reshape2d(self.inputs.result.outcome['rms'])

    @property
    def edge(self) -> Tuple[int, int]:
        """Return edge channels to exclude."""
        return self.inputs.result.outcome['edge']

    def __reshape2d(self, array2d: np.ndarray) -> np.ndarray:
        """Reshape input two-dimensional array into three-dimensional array.

        Returned array should have the shape (nx, ny, npol) where nx is
        number of pixels along horizontal direction (longitude) axis,
        ny is number of pixels along vertical direction (latitude) axis,
        and npol is number of polarizations or correlations.

        Args:
            array2d: Two-dimensional array.

        Returns:
            np.ndarray: Resheped array.
        """
        array3d = np.zeros((self.npol, self.ny, self.nx), dtype=array2d.dtype)
        if len(array2d) == self.npol:
            each_len = np.array(list(map(len, array2d)))
            if np.all(each_len == 0):
                # no valid data in the pixel
                array3d = np.zeros((self.npol, self.ny, self.nx), dtype=array2d.dtype)
            elif np.all(each_len == self.ny * self.nx):
                # all polarizations has valid data in each pixel
                array3d = np.array(array2d).reshape((self.npol, self.ny, self.nx))
            elif np.any(each_len == self.ny * self.nx):
                # probably one of the polarization components has no valid data
                invalid_pols = np.where(each_len == 0)[0]
                _array2d = []
                for i in range(self.npol):
                    if i in invalid_pols:
                        _array2d.append(np.zeros((self.ny * self.nx), dtype=array2d.dtype))
                    else:
                        _array2d.append(array2d[i])
                array3d = np.array(_array2d).reshape((self.npol, self.ny, self.nx))
        return np.flipud(array3d.transpose())


#
# sparse profile map
def form3(n: int) -> int:
    """Return a factor for calculation of panel position.

    For given integer, form4 provide a factor for the
    calculation of vertical panel position for sparse
    profile map.

    Args:
        n: Number of panels along vertical axis.

    Returns:
        float: Factor for panel position.
    """
    if n <= 4:
        return 4
    elif n == 5:
        return 5
    elif n < 8:
        return 7
    else:
        return 8


def form4(n: int) -> float:
    """Return a factor for calculation of panel position.

    For given integer, form4 provide a factor for the
    calculation of vertical panel position for sparse
    profile map.

    Args:
        n: Number of panels along vertical axis.

    Returns:
        float: Factor for panel position.
    """
    if n <= 4:
        return 4
    elif n < 8:
        return 5
    else:
        return 5.5


class SparseMapAxesManager(pointing.MapAxesManagerBase):
    """Creates and manages Axes instances for sparse profile map.

    Sparse profile map consists of the following Axes:

        - Integrated spectrum
        - Atmospheric transmission (overlays integrated spectrum)
        - Channel axis (optional, overlays integrated spectrum)
        - Sparse profile map
    """

    def __init__(self, fig: figure.Figure, nh: int, nv: int, brightnessunit: str,
                 ticksize: int, clearpanel: bool = True) -> None:
        """Construct SparseMapAxesManager instance.

        Args:
            fig: matplotlib.figure.Figure instance
            nh: Number of panels along vertical axis.
            nv: Number of panels along horizontal axis.
            brightnessunit: Brightness unit.
            ticksize: Size of tick label.
            clearpanel: Clear existing Axes. Defaults to True.
        """
        super(SparseMapAxesManager, self).__init__()
        self.figure = fig
        self.nh = nh
        self.nv = nv
        self.ticksize = ticksize
        self.brightnessunit = brightnessunit

        self._axes_integsp = None
        self._axes_spmap = None
        self._axes_atm = None
        self._axes_chan = None

        _f = form4(self.nv)
        self.gs_top = gridspec.GridSpec(1, 1,
                                        left=0.08,
                                        bottom=1.0 - 1.0 / _f, top=0.96)
        self.gs_bottom = gridspec.GridSpec(self.nv + 1, self.nh + 1,
                                           hspace=0, wspace=0,
                                           left=0, right=0.95,
                                           bottom=0.01, top=1.0 - 1.0 / _f - 0.07)
#         self.gs_top = gridspec.GridSpec(1, 1,
#                                         bottom=1.0 - 1.0/form3(self.nv), top=0.96)
#         self.gs_bottom = gridspec.GridSpec(self.nv+1, self.nh+1,
#                                            hspace=0, wspace=0,
#                                            left=0, right=0.95,
#                                            bottom=0.01, top=1.0 - 1.0/form3(self.nv)-0.07)

    @property
    def axes_integsp(self) -> Axes:
        """Create Axes instance for integrated spectrum.

        Creates and returns Axes instance for integrated or averaged
        spectrum, which is located at the top of the figure.

        Returns:
            Axes: Axes instance for integrated spectrum.
        """
        if self._axes_integsp is None:
            axes = self.figure.add_subplot(self.gs_top[:, :])
            axes.cla()
            axes.xaxis.get_major_formatter().set_useOffset(False)
            axes.yaxis.get_major_formatter().set_useOffset(False)
            axes.set_xlabel('Frequency(GHz)', size=(self.ticksize + 1))
            axes.set_ylabel('Intensity({})'.format(self.brightnessunit), size=(self.ticksize + 1))
            xlabels = axes.get_xticklabels()
            for label in xlabels:
                label.set_fontsize(self.ticksize)
            ylabels = axes.get_yticklabels()
            for label in ylabels:
                label.set_fontsize(self.ticksize)
            axes.set_title('Spatially Averaged Spectrum', size=(self.ticksize + 1))

            self._axes_integsp = axes
        return self._axes_integsp

    @property
    def axes_spmap(self) -> List[Axes]:
        """Create Axes instances for profile map.

        Creates and returns list of Axes instances that constitutes
        sparse profile map.

        Returns:
            List[Axes]: List of Axes instances for profile map.
        """
        if self._axes_spmap is None:
            self._axes_spmap = list(self.__axes_spmap())

        return self._axes_spmap

    @property
    def axes_atm(self) -> Axes:
        """Create Axes instance for Atmospheric transmission profile.

        Creates and returns Axes instance for Atmospheric transmission
        profile that is calculated by Atmopheric Transmission at
        Microwaves (ATM) model. The Axes overlays integrated spectrum.

        Returns:
            Axes: Axes instance for ATM transmission.
        """
        if self._axes_atm is None:
            self._axes_atm = self.axes_integsp.twinx()
            self._axes_atm.set_position(self.axes_integsp.get_position())
            ylabel = self._axes_atm.set_ylabel('ATM Transmission', size=self.ticksize)
            ylabel.set_color('m')
            self._axes_atm.yaxis.set_tick_params(colors='m', labelsize=self.ticksize - 1)
            self._axes_atm.yaxis.set_major_locator(
                matplotlib.ticker.MaxNLocator(nbins=4, integer=True, min_n_ticks=2)
            )
            self._axes_atm.yaxis.set_major_formatter(
                matplotlib.ticker.FuncFormatter(lambda x, pos: '{}%'.format(int(x)))
            )
        return self._axes_atm

    @property
    def axes_chan(self) -> Axes:
        """Create Axes instance for channel axis.

        Creates and returns Axes instance for channel axis on
        integrated spectrum.

        Returns:
            Axes: Axes for channel axis.
        """
        if self._axes_chan is None:
            self.__adjust_integsp_for_chan()
            self._axes_chan = self.axes_integsp.twiny()
            self._axes_chan.set_position(self.axes_integsp.get_position())
            if self._axes_atm is not None:
                self._axes_atm.set_position(self.axes_integsp.get_position())
            self._axes_chan.set_xlabel('Channel', size=self.ticksize - 1)
            self._axes_chan.xaxis.set_label_coords(0.5, 1.11)
            self._axes_chan.tick_params(axis='x', pad=0)
            xlabels = self._axes_chan.get_xticklabels()
            for label in xlabels:
                label.set_fontsize(self.ticksize - 1)
        return self._axes_chan

    def __adjust_integsp_for_chan(self) -> None:
        """Adjust size of Axes for integrated spectrum for channel axis.

        Adjust size of Axes for integrated spectrum to locate channel axis
        at the top of the panel.
        """
        a = self._axes_integsp
        bbox = a.get_position().get_points()
        blc = bbox[0]
        trc = bbox[1]
        # gives [left, bottom, width, height]
        left = blc[0]
        bottom = blc[1]
        width = trc[0] - blc[0]
        height = trc[1] - blc[1] - 0.03
        a.set_position((left, bottom, width, height))
        a.title.set_position((0.5, 1.2))

    def __axes_spmap(self) -> Generator[Axes, None, None]:
        """Create Axes instances for sparse profile map.

        Yields:
            Generator[Axes, None, None]:
                Axes instances corresponding to individual profile.
        """
        for x in range(self.nh):
            for y in range(self.nv):
                axes = self.figure.add_subplot(self.gs_bottom[self.nv - y - 1, self.nh - x])
                axes.cla()
                axes.yaxis.set_major_locator(matplotlib.ticker.NullLocator())
                axes.xaxis.set_major_locator(matplotlib.ticker.NullLocator())

                yield axes

    def setup_labels(self,
                     label_ra: Union[List[float], np.ndarray],
                     label_dec: Union[List[float], np.ndarray]) -> None:
        """Set up position labels for sparse profile map.

        Set up position (longitude and latitude) labels for sparse
        profile map according to label_ra and label_dec, which are
        the arrays with shape of (nh, 2) and (nv, 2) that hold
        minimum and maximum positions for each panel, where nh and nv
        are number of panels along horizontal and vertical axes.
        Label value is the mean of minimum and maximum values of
        positions and is converted to position string (HMS or DMS
        format).

        Args:
            label_ra: min/max horizontal positions for each panel.
            label_dec: min/max vertical positions for each panel.
        """
        if self.direction_reference.upper() == 'GALACTIC':
            xaxislabel = pointing.DDMMSSs
        else:
            xaxislabel = pointing.HHMMSSss
        for x in range(self.nh):
            a1 = self.figure.add_subplot(self.gs_bottom[-1, self.nh - x])
            a1.set_axis_off()
            if len(a1.texts) == 0:
                a1.text(0.5, 0.5, xaxislabel((label_ra[x][0] + label_ra[x][1]) / 2.0),
                        horizontalalignment='center', verticalalignment='center', size=self.ticksize)
            else:
                a1.texts[0].set_text(xaxislabel((label_ra[x][0] + label_ra[x][1]) / 2.0))
        for y in range(self.nv):
            a1 = self.figure.add_subplot(self.gs_bottom[self.nv - y - 1, 0])
            a1.set_axis_off()
            if len(a1.texts) == 0:
                a1.text(0.5, 0.5, pointing.DDMMSSs((label_dec[y][0] + label_dec[y][1]) / 2.0),
                        horizontalalignment='center', verticalalignment='center', size=self.ticksize)
            else:
                a1.texts[0].set_text(pointing.DDMMSSs((label_dec[y][0] + label_dec[y][1]) / 2.0))
        a1 = self.figure.add_subplot(self.gs_bottom[-1, 0])
        a1.set_axis_off()
        ralabel, declabel = self.get_axes_labels()
        a1.text(0.5, 1, declabel, horizontalalignment='center', verticalalignment='bottom', size=(self.ticksize + 1))
        a1.text(1, 0.5, ralabel, horizontalalignment='right', verticalalignment='center', size=(self.ticksize + 1))

    def clear_plot_objects(self) -> None:
        """Remove all plot objects from Axes.

        Remove all plot objects, which includes lines, patches, and texts,
        from the Axes objects.
        """
        all_axes = [self._axes_integsp, self._axes_atm, self._axes_chan]
        if self._axes_spmap is not None:
            all_axes.extend(self._axes_spmap)
        active_axes = [a for a in all_axes if a is not None]
        LOG.trace('There are %s active axes objects', len(active_axes))
        for a in active_axes:
            LOG.trace('Axes: %s', a)
            LOG.trace('Lines: %s', a.lines)
            LOG.trace('Patches: %s', a.patches)
            LOG.trace('Texts: %s', a.texts)
            for obj in itertools.chain(a.lines[:], a.patches[:], a.texts[:], a.images[:]):
                LOG.trace('Removing %s...', obj)
                obj.remove()


class SDSparseMapPlotter(object):
    """Plotter for sparse spectral map."""

    def __init__(self, fig: figure.Figure, nh: int, nv: int, step: int, brightnessunit: str,
                 clearpanel: bool = True) -> None:
        """Construct SDSparseMapPlotter instance.

        Args:
            fig: matplotlib.figure.Figure instance
            nh: Number of panels along vertical axis.
            nv: Number of panels along horizontal axis.
            brightnessunit: Brightness unit.
            clearpanel: Clear existing Axes. Defaults to True.
        """
        self.step = step
        if step > 1:
            ticksize = 10 - int(max(nh, nv) * step // (step - 1)) // 2
        elif step == 1:
            ticksize = 10 - int(max(nh, nv)) // 2
        ticksize = max(ticksize, 3)
        fig.set_dpi(DPIDetail)
        self.axes = SparseMapAxesManager(fig, nh, nv, brightnessunit, ticksize, clearpanel)
        self.lines_averaged = None
        self.lines_map = None
        self.reference_level = None
        self.global_scaling = True
        self.deviation_mask = None
        self.edge = None
        self.atm_transmission = None
        self.atm_frequency = None
        self.channel_axis = False

    @property
    def nh(self) -> int:
        """Return number of panels along horizontal axis."""
        return self.axes.nh

    @property
    def nv(self) -> int:
        """Return number of panels along vertical axis."""
        return self.axes.nv

    @property
    def ticksize(self) -> int:
        """Return tick label size."""
        return self.axes.ticksize

    @property
    def direction_reference(self) -> str:
        """Return direction reference string."""
        return self.axes.direction_reference

    @direction_reference.setter
    def direction_reference(self, value) -> None:
        """Set direction reference string."""
        self.axes.direction_reference = value

    def setup_labels_relative(self,
                              refpix_list: Tuple[float, float],
                              refval_list: Tuple[float, float],
                              increment_list: Tuple[float, float]) -> None:
        """Set up position labels.

        Set up position labels for both horizontal and vertical axes
        according to reference pixels (refpix_list), reference values
        (refval_list), and increments (increment_list), which should
        be given as two-tuples or lists with at least two elements.

        Args:
            refpix_list: reference pixels.
            refval_list: reference values.
            increment_list: increments.
        """
        LabelRA = np.zeros((self.nh, 2), np.float32) + NoData
        LabelDEC = np.zeros((self.nv, 2), np.float32) + NoData
        refpix = refpix_list[0]
        refval = refval_list[0]
        increment = increment_list[0]
        LOG.debug('axis 0: refpix,refval,increment=%s,%s,%s', refpix, refval, increment)
        for x in range(self.nh):
            x0 = (self.nh - x - 1) * self.step
            x1 = (self.nh - x - 2) * self.step + 1
            LabelRA[x][0] = refval + (x0 - refpix) * increment
            LabelRA[x][1] = refval + (x1 - refpix) * increment
        refpix = refpix_list[1]
        refval = refval_list[1]
        increment = increment_list[1]
        LOG.debug('axis 1: refpix,refval,increment=%s,%s,%s', refpix, refval, increment)
        for y in range(self.nv):
            y0 = y * self.step
            y1 = (y + 1) * self.step - 1
            LabelDEC[y][0] = refval + (y0 - refpix) * increment
            LabelDEC[y][1] = refval + (y1 - refpix) * increment
        self.axes.setup_labels(LabelRA, LabelDEC)

    def setup_labels_absolute(self, ralist: List[float], declist: List[float]) -> None:
        """Set up position labels.

        Set up position labels for both horizontal and vertical axes
        according to the list of positions along horizontal (ralist)
        and vertical (declist) axes.

        Args:
            ralist: List of horizontal position.
            declist: List of vertical positions.
        """
        assert self.step == 1  # this function is used only for step=1
        LabelRA = [[x, x] for x in ralist]
        LabelDEC = [[y, y] for y in declist]
        self.axes.setup_labels(LabelRA, LabelDEC)

    def setup_lines(self,
                    lines_averaged: List[float],
                    lines_map: Optional[List[float]] = None) -> None:
        """Set detected lines.

        Provided lines are displayed as shaded area. Lines given to
        lines_averaged are displayed in the Axes for integrated
        spectrum. Lines given to lines_map are interpreted as lines
        for each panel of sparse profile map.

        Args:
            lines_averaged: Lines for integrated spectrum.
            lines_map: Lines for sparse profile map. Defaults to None.
        """
        self.lines_averaged = lines_averaged
        self.lines_map = lines_map

    def setup_reference_level(self, level: Optional[float] = 0.0) -> None:
        """Set reference level of the sparse profile map.

        If float value is given, red horizontal line at the value is
        displayed to each panel. If None is given, no line is diaplayed.

        Args:
            level: Reference level. Defaults to 0.0.
        """
        self.reference_level = level

    def set_global_scaling(self) -> None:
        """Enable global scaling.

        Enable global scanling. Applies the same y-axis
        range to all panels in sparse profile map.
        """
        self.global_scaling = True

    def unset_global_scaling(self) -> None:
        """Disable global scaling.

        Disable global scaling. Y-axis ranges of panels
        in sparse profile map are adjusted individually.
        """
        self.global_scaling = False

    def set_deviation_mask(self, mask) -> None:
        """Set deviation mask.

        Deviation mask ranges are displayed as red bar at
        the top of integrated spectrum.
        """
        self.deviation_mask = mask

    def set_edge(self, edge: Tuple[int, int]) -> None:
        """Set edge parameter.

        Edge region specified by edge parameter is shaded with grey.

        Args:
            edge: Edge area to be shaded.
        """
        self.edge = edge

    def set_atm_transmission(self, transmission: List[float], frequency: List[float]) -> None:
        """Set atmospheric transmission data.

        If trasnmission and frequency are given properly, atmospheric
        transmission is overlaid to integrated spectrum as magenta line.
        Number of elements for transmission and frequency must be the same.

        Args:
            transmission: Atmospheric transmission.
            frequency: Frequency label.
        """
        if self.atm_transmission is None:
            self.atm_transmission = [transmission]
            self.atm_frequency = [frequency]
        else:
            self.atm_transmission.append(transmission)
            self.atm_frequency.append(frequency)

    def unset_atm_transmission(self) -> None:
        """Disable displaying atmospheric transmission."""
        self.atm_transmission = None
        self.atm_frequency = None

    def set_channel_axis(self) -> None:
        """Enable channel axis for integrated spectrum.

        Channel axis is displayed in the upper side of the Axes
        for integrated spectrum.
        """
        self.channel_axis = True

    def unset_channel_axis(self) -> None:
        """Disable channel axis for integrated spectrum."""
        self.channel_axis = False

    def add_channel_axis(self, frequency: List[float]) -> None:
        """Add channel axis to integrated spectrum.

        Args:
            frequency: Frequency label.
        """
        axes = self.axes.axes_chan
        f = np.asarray(frequency)
        axes.set_xlim((np.argmin(f), np.argmax(f)))

    def plot(self,
             map_data: Union[np.ndarray, np.ma.masked_array],
             averaged_data: Union[np.ndarray, np.ma.masked_array],
             frequency: np.ndarray, fit_result: Optional[np.ndarray] = None,
             figfile: Optional[str] = None) -> bool:
        """Generate sparse profile map.

        Generates sparse profile map. If fit_result is given, it is
        indicated as red lines in sparse profile map. If figfile is
        provided, the plot is exported to the file.

        Args:
            map_data: Data for sparse profile map.
            averaged_data: Data for integrated spectrum.
            frequency: Frequency label.
            fit_result: Data for fit result. Defaults to None.
            figfile: Name of the plot file. Defaults to None.

        Returns:
            bool: Whether or not if plot is successful.
        """
        if figfile is None:
            LOG.debug('Skip creating sparse profile map')
            return False

        overlay_atm_transmission = self.atm_transmission is not None
        LOG.debug(f'overlay_atm_transmission = {overlay_atm_transmission}')

        spmin = np.nanmin(averaged_data)
        spmax = np.nanmax(averaged_data)
        dsp = spmax - spmin
        spmin -= dsp * 0.1
        if overlay_atm_transmission:
            spmax += dsp * 0.4
        else:
            spmax += dsp * 0.1
        LOG.debug('spmin=%s, spmax=%s', spmin, spmax)

        global_xmin = min(frequency[0], frequency[-1])
        global_xmax = max(frequency[0], frequency[-1])
        # NOTE: we assume channels are equally binned
        channel_width = abs(frequency[1] - frequency[0])
        spw_width = channel_width * len(frequency)
        global_xmin -= spw_width * 0.01
        global_xmax += spw_width * 0.01
        LOG.debug('global_xmin=%s, global_xmax=%s', global_xmin, global_xmax)

        # Auto scaling
        # to eliminate max/min value due to bad pixel or bad fitting,
        #  1/10-th value from max and min are used instead
        valid_index = np.ma.where(map_data.min(axis=2) > NoDataThreshold)
        valid_data = map_data[valid_index[0], valid_index[1], :]
        LOG.debug('valid_data.shape={shape}'.format(shape=valid_data.shape))
        del valid_index
        if isinstance(map_data, np.ma.masked_array):
            def stat_per_spectra(spectra, oper):
                for v in spectra:
                    unmasked = v.data[v.mask == False]
                    if len(unmasked) > 0:
                        yield oper(unmasked)
            ListMax = np.fromiter(stat_per_spectra(valid_data, np.max), dtype=np.float64)
            ListMin = np.fromiter(stat_per_spectra(valid_data, np.min), dtype=np.float64)
#             ListMax = np.fromiter((np.max(v.data[v.mask == False]) for v in valid_data),
#                                      dtype=np.float64)
#             ListMin = np.fromiter((np.min(v.data[v.mask == False]) for v in valid_data),
#                                      dtype=np.float64)
            LOG.debug('ListMax from masked_array=%s', ListMax)
            LOG.debug('ListMin from masked_array=%s', ListMin)
        else:
            ListMax = valid_data.max(axis=1)
            ListMin = valid_data.min(axis=1)
        del valid_data
        if len(ListMax) == 0 or len(ListMin) == 0:
            return False
        # if isinstance(ListMin, np.ma.masked_array):
        #     ListMin = ListMin.data[ListMin.mask == False]
        # if isinstance(ListMax, np.ma.masked_array):
        #     ListMax = ListMax.data[ListMax.mask == False]
        LOG.debug('ListMax=%s', list(ListMax))
        LOG.debug('ListMin=%s', list(ListMin))
        global_ymax = np.sort(ListMax)[len(ListMax) - len(ListMax) // 10 - 1]
        global_ymin = np.sort(ListMin)[len(ListMin) // 10]
        global_ymax = global_ymax + (global_ymax - global_ymin) * 0.2
        global_ymin = global_ymin - (global_ymax - global_ymin) * 0.1
        del ListMax, ListMin

        axes = self.axes.axes_integsp
        axes.plot(frequency, averaged_data, color='b', linestyle='-', linewidth=0.4)
        if self.channel_axis is True:
            self.add_channel_axis(frequency)
        (_xmin, _xmax, _ymin, _ymax) = axes.axis()

        LOG.info('global_ymin=%s, global_ymax=%s', global_ymin, global_ymax)
        LOG.info('spmin=%s, spmax=%s', spmin, spmax)

        # do not create plots if any of specified axis ranges
        # are invalid
        if is_invalid_axis_range(global_xmin, global_xmax, spmin, spmax):
            LOG.warning(
                'Invalid axis range for averaged spectrum. Plot %s will not be created.',
                os.path.basename(figfile)
            )
            return False

        try:
            # PIPE-1140
            axes.axis([global_xmin, global_xmax, spmin, spmax])
        except Exception:
            LOG.warning(
                'Axis configuration for %s failed. Plot will not be created.',
                os.path.basename(figfile)
            )
            return False
        fedge_span = np.array([0, 0, 0, 0], dtype=float)
        if self.edge is not None:
            (ch1, ch2) = self.edge
            LOG.info('ch1, ch2: [%s, %s]' % (ch1, ch2))
            if ch1 > 0:
                fedge_span[0:2] = ch_to_freq([-0.5, ch1 - 1 + 0.5], frequency)
                axes.axvspan(fedge_span[0], fedge_span[1], color='lightgray')
            if ch2 > 0:
                fedge_span[2:] = ch_to_freq(
                    [len(frequency) - 1 - (ch2 - 1) - 0.5, len(frequency) - 1 + 0.5],
                    frequency
                )
                axes.axvspan(fedge_span[2], fedge_span[3], color='lightgray')
        if self.lines_averaged is not None:
            for chmin, chmax in self.lines_averaged:
                fmin, fmax = ch_to_freq([chmin - 0.5, chmax + 0.5], frequency)
                LOG.debug('plotting line range for mean spectrum: [%s, %s]', chmin, chmax)
                axes.axvspan(fmin, fmax, color='cyan')
        if self.deviation_mask is not None:
            LOG.debug('plotting deviation mask %s', self.deviation_mask)
            for chmin, chmax in self.deviation_mask:
                fmin, fmax = ch_to_freq([chmin - 0.5, chmax + 0.5], frequency)
                axes.axvspan(fmin, fmax, ymin=0.95, ymax=1, color='red')

        if overlay_atm_transmission:
            axes_atm = self.axes.axes_atm
            amin = 100
            amax = 0
            for (_t, f) in zip(self.atm_transmission, self.atm_frequency):
                # fraction -> percentage
                t = _t * 100
                axes_atm.plot(f, t, color='m', linestyle='-', linewidth=0.4)
                amin = min(amin, t.min())
                amax = max(amax, t.max())

            # trick to make transmission curve is shown in the upper part
            Y = 60
            ymin = max(0, (amin - Y) / (100 - Y) * 100)
            ymax = amax + (100 - amax) * 0.1

            # to make sure y-range is more than 2 (for MaxNLocator)
            if ymax - ymin < 2:
                if ymin > 2:
                    ymin -= 2
                elif ymax < 98:
                    ymax += 2

            axes_atm.axis([global_xmin, global_xmax, ymin, ymax])

        is_valid_fit_result = (fit_result is not None and fit_result.shape == map_data.shape)

        for x in range(self.nh):
            for y in range(self.nv):
                if self.global_scaling is True:
                    xmin = global_xmin
                    xmax = global_xmax
                    ymin = global_ymin
                    ymax = global_ymax
                else:
                    xmin = global_xmin
                    xmax = global_xmax
                    if map_data[x][y].min() > NoDataThreshold:
                        median = np.ma.median(map_data[x][y])
                        # mad = np.median(map_data[x][y] - median)
                        sigma = map_data[x][y].std()
                        ymin = median - 2.0 * sigma
                        ymax = median + 5.0 * sigma
                    else:
                        ymin = global_ymin
                        ymax = global_ymax
                    LOG.debug('Per panel scaling turned on: ymin=%s, ymax=%s (global ymin=%s, ymax=%s)',
                              ymin, ymax, global_ymin, global_ymax)
                axes = self.axes.axes_spmap[y + (self.nh - x - 1) * self.nv]
                if map_data[x][y].min() > NoDataThreshold:
                    axes.plot(frequency, map_data[x][y], color='b', linestyle='-', linewidth=0.2)
                    if self.lines_map is not None and self.lines_map[x][y] is not None:
                        for chmin, chmax in self.lines_map[x][y]:
                            fmin, fmax = ch_to_freq([chmin - 0.5, chmax + 0.5], frequency)
                            LOG.debug('plotting line range for %s, %s: [%s, %s]', x, y, chmin, chmax)
                            axes.axvspan(fmin, fmax, color='cyan')
                    if any(fedge_span > 0):
                        axes.axvspan(fedge_span[0], fedge_span[1], color='lightgray')
                        axes.axvspan(fedge_span[2], fedge_span[3], color='lightgray')

                    # elif self.lines_averaged is not None:
                    #     for chmin, chmax in self.lines_averaged:
                    #         fmin = ch_to_freq(chmin, frequency)
                    #         fmax = ch_to_freq(chmax, frequency)
                    #         LOG.debug('plotting line range for %s, %s (reuse lines_averaged): [%s, %s]',
                    #                   x, y, chmin, chmax)
                    #        plot_helper.axvspan(fmin, fmax, color='cyan')
                    if is_valid_fit_result:
                        axes.plot(frequency, fit_result[x][y], color='r', linewidth=0.4)
                    elif self.reference_level is not None and ymin < self.reference_level and self.reference_level < ymax:
                        axes.axhline(self.reference_level, color='r', linewidth=0.4)
                else:
                    axes.text((xmin + xmax) / 2.0, (ymin + ymax) / 2.0, 'NO DATA', ha='center', va='center',
                                     size=(self.ticksize + 1))
                axes.axis([xmin, xmax, ymin, ymax])

        self.axes.figure.savefig(figfile, dpi=DPIDetail)
        LOG.debug('figfile=\'%s\'', figfile)

        self.axes.clear_plot_objects()

        return True

    def done(self) -> None:
        """Clean up plot."""
        fig = self.axes.figure
        del self.axes
        fig.clf()
        del fig


def ch_to_freq(ch: Union[float, List[float]], frequency: List[float]) -> Union[float, List[float]]:
    """Convert channel into frequency.

    Args:
        ch: Channel value, or list of channel values.
        frequency: Frequency labels.

    Returns:
        float: Frequency value(s) corresponding to ch.
    """
    interpolator = scipy.interpolate.interp1d(
        np.arange(len(frequency)),
        frequency,
        kind='linear',
        fill_value='extrapolate')
    return interpolator(ch)
