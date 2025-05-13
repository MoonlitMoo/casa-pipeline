"""Parameter classes of Imaging."""

import re
from typing import TYPE_CHECKING, Any, Dict, List, NewType, Optional, Union

import numpy

from casatools import coordsys
from pipeline.hsd.tasks.common import utils as sdutils
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.debugwrapper import debugwrapper

if TYPE_CHECKING:
    from collections import namedtuple
    from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResults
    from pipeline.infrastructure import Context
    from pipeline.domain.datatable import DataTableImpl
    from pipeline.domain import MeasurementSet
    from pipeline.domain.singledish import MSReductionGroupDesc
    RasterInfo = NewType('RasterInfo', namedtuple('RasterInfo', 'center_ra center_dec width'
                                                                'height scan_angle row_separation row_duration'))
    ImageGroup = Dict[str, List[Union['MeasurementSet', int,
                                      List[Union[str, List[List[Union[float, bool]]]]]]]]


class ObservedList(list):
    """Class inherit list to observe its behavior.

    This class intends to observe/output list stuff for debugging more easier.
    If you want to search a needle in a haystack, please uncomment out @debugwrapper()."""

    # @debugwrapper(msg='list.setitem')
    def __setitem__(self, index: int, value: object):
        """Overrode list.__setitem__().

        Args:
            index : index
            value : object to set at index
        """
        super().__setitem__(index, value)

    # @debugwrapper(msg='list.insert')
    def insert(self, index: int, value: object):
        """Overrode list.insert().

        Args:
            index : index
            value : object to insert at index
        """
        super().insert(index, value)

    # @debugwrapper(msg='list.append')
    def append(self, value: object):
        """Overrode list.append().

        Args:
            value : object to append
        """
        super().append(value)

    # @debugwrapper(msg='list.extend')
    def extend(self, value: list):
        """Overrode list.extend().

        Args:
            value : object to merge
        """
        super().extend(value)


class Parameters:
    """Abstract class of Parameter object.

    Note: _immutable_parameters is the list which is set some parameter names of a class inherit Parameter.
    All parameters in _immutable_parameters are set immutable by freeze().
    """

    # List immutable-able parameter names in it at a child class
    _immutable_parameters = []
    _immutable_prefix = '__immutable_'

    def __init__(self) -> None:
        """Initialize an instance."""
        self.__dict__[self._immutable_prefix] = False
        # initialize immutable-able parameters.
        # the name of parameters must be started with an alphanumeric character.
        for _name in self._immutable_parameters:
            if re.match(r'^[a-zA-Z0-9].\w*$', _name):
                self.__dict__[f'{self._immutable_prefix}{_name}'] = True
            else:
                raise ValueError('invalid parameter name')

    def is_immutable(self, _name: str='') -> bool:
        """Check itself or an attribute whether immutable or not.

        Args:
            _name (str): Attribute name. If it is a default value, the method returns
                         a boolean whether the instance itself is immutable or not.

        Returns:
            True if it is immutable.
        """
        return self.__dict__.get(f'{self._immutable_prefix}{_name}', False)

    def freeze(self) -> None:
        """Set the instance immutable."""
        self.__dict__[self._immutable_prefix] = True

    @debugwrapper(msg='setattr')
    def __setattr__(self, _name: str, _value: Any) -> None:
        """Override object.__setattr__().

        Args:
            _name (str): Attribute name
            _value (Any): Attribute Object

        Raises:
            NotImplementedError: raises if _name is immutable.
        """
        if self.is_immutable() and self.is_immutable(_name):
            raise NotImplementedError(f'attribute {_name} is immutable.')
        super.__setattr__(self, _name, _value)


class CommonParameters(Parameters):
    """Common parameters class of prepare()."""

    _immutable_parameters = ['reduction_group', 'restfreq_list', 'ms_names', 'session_names', 'args_spw',
                             'imagemode', 'is_nro', 'results', 'edge', 'dt_dict']

    def is_not_nro(self) -> numpy.bool_:
        """Return True if is_nro is False.

        Returns:
            True if this data is not from NRO.
        """
        return not self.is_nro


def initialize_common_parameters(args_spw: Dict[str, str], dt_dict: Dict[str, 'DataTableImpl'],
                                 edge: List[int], imagemode: str, in_field: str,
                                 infiles: List[str], is_nro: numpy.bool_, ms_list: List['MeasurementSet'],
                                 ms_names: List[str], session_names: List[str], reduction_group: Dict[int, 'MSReductionGroupDesc'],
                                 restfreq_list: Union[str, List[str]], results: 'SDImagingResults'
                                 ) -> CommonParameters:
    """Initialize an instance of CommonParameters and return it.

    Args:
        args_spw (Dict[str, str]): Spw selection per MS
        dt_dict (Dict[str, DataTableImpl]): Dictionary of input MS and corresponding datatable
        edge (List[int]): Edge channel of most recent SDBaselineResults or [0, 0]
        imagemode (str): Image mode
        in_field (str): Comma-separated list of target field names that are extracted from all input MSs
        infiles (List[str]): List of input files
        is_nro (numpy.bool_): Flag of NRO data
        ms_list (List[MeasurementSet]): List of ms to process
        ms_names (List[str]): List of name of ms in ms_list
        session_names (List[str]): List of session name of ms in ms_list
        reduction_group (Dict[int, MSReductionGroupDesc]): Reduction group object
        restfreq_list (Union[str, List[str]]): List of rest frequency
        results (SDImagingResults): Instance of SDImagingResults

    Returns:
        An instance of CommonParameters
    """
    _tmp = CommonParameters()
    _tmp.args_spw = args_spw
    _tmp.dt_dict = dt_dict
    _tmp.edge = edge
    _tmp.imagemode = imagemode
    _tmp.in_field = in_field
    _tmp.infiles = infiles
    _tmp.is_nro = is_nro
    _tmp.ms_list = ms_list
    _tmp.ms_names = ms_names
    _tmp.session_names = session_names
    _tmp.reduction_group = reduction_group
    _tmp.restfreq_list = restfreq_list
    _tmp.results = results
    _tmp.freeze()
    return _tmp


class ReductionGroupParameters(Parameters):
    """Parameters of Reduction Group Processing."""

    def __init__(self, group_id: int, group_desc: 'MSReductionGroupDesc'):
        """Initialize an object with group value.

        Args:
            group_id : Reduction group ID
            group_desc : MeasurementSet Reduction Group Desciption object
        """
        super().__init__()
        self.group_id = group_id           # int: Reduction group ID
        self.group_desc = group_desc       # MSReductionGroupDesc(spw_name:str, frequency_range:List[float], nchan:int,
                                           #                      field:str, member:List[MSReductionGroupMember]):
                                           #                      MSReductionGroupDesc object
        self.antenna_list = None           # List[int]: List of antenna ID
        self.antids = None                 # List[int]: List of antenna ID
        self.ant_name = None               # str: Name of antenna
        self.asdm = None                   # str: ASDM name of reference MS
        self.cellx = None                  # Dict[str, float]: Cell size x, {'unit': 'arcsec', 'value': 6.4}
        self.celly = None                  # Dict[str, float]: Cell size y, {'unit': 'arcsec', 'value': 6.4}
        self.chanmap_range_list = None     # List[List[List[Union[float, bool]]]]: List of channel map range
        self.channelmap_range_list = None  # List[List[List[Union[float, bool]]]]: List of channel map range
        self.combined = None               # CombinedImageParameters: CombinedImageParameters object
        self.coord_set = False             # bool: Flag of Coord setting
        self.correlations = None           # str: A joined list of correlations
        self.fieldid_list = None           # List[int]: List of field ID
        self.fieldids = None               # List[int]: List of field ID
        self.image_group = None            # ImageGroup: Dictionary of image group of reduction group
        self.imagename = None              # str: Image name
        self.imagename_nro = None          # Optional[str]: Image name for NRO
        self.imager_result = None          # SDImagingResultItem: Result object of imager
        self.imager_result_nro = None      # SDImagingResultItem: Result object of imager for NRO
        self.member_list = None            # List[int]: List of reduction group ID
        self.members = None                # List[List[MeasurementSet, int, List[str]]]: Image group of reduction group
        self.msobjs = None                 # List[MeasurementSet]: List of MeasurementSet
        self.name = None                   # str: Name of MeasurementSet
        self.nx = None                     # Union[int, numpy.int64]: X of image shape
        self.ny = None                     # Union[int, numpy.int64]: Y of image shape
        self.org_direction = None          # Direction: a direction of origin for ephemeris object
        self.phasecenter = None            # str: Phase center of coord set
        self.polslist = None               # List[List[str]]: List of Polarization. NOT USED NOW virtually.
        self.pols_list = None              # List[List[str]]: List of Polarization
        self.ref_ms = None                 # MeasurementSet: Reference MS object
        self.restfreq = None               # str: Rest frequency
        self.rmss = None                   # List[float]: List of RMSs
        self.source_name = None            # str: Name of source like 'M100'
        self.specmode = None               # str: Spec mode like 'cube'
        self.spwid_list = None             # List[int]: List of Spectral window IDs of _group_desc
        self.spwids = None                 # List[int]: List of Spectral window IDs of _members
        self.stokes_list = None            # List[str]: List of stokes
        self.tocombine = None              # ToCombineImageParameters: ToCombineImageParameters object
        self.validsps = None               # List[List[int]]: List of valid spectrum
        self.v_spwids = None               # List[int]: List of Virtual Spectral window IDs


class CombinedImageParameters(Parameters):
    """Parameter class for combined image."""

    def __init__(self):
        """Initialize an object."""
        super().__init__()
        self.antids = ObservedList()           # ObservedList[int]: List of antenna ID
        self.fieldids = ObservedList()         # ObservedList[int]: List of field ID
        self.infiles = ObservedList()          # ObservedList[str]: List of input file names
        self.pols = ObservedList()             # List[List[str]]: List of Polarization
        self.rms_exclude = ObservedList()      # ObservedList[numpy.ndarray[float]]: RMS mask frequency range
        self.spws = ObservedList()             # ObservedList[int]: List of Spectral window IDs
        self.v_spws = ObservedList()           # ObservedList[int]: List of Virtual Spectral window IDs
        self.v_spws_unique = ObservedList()    # ObservedList[int]: List of unique values of _v_spws

    def extend(self, _cp: CommonParameters, _rgp: ReductionGroupParameters):
        """Extend list properties using CP and RGP.

        Args:
            _cp : CommonParameters object
            _rgp : ReductionGroupParameters object
        """
        self.infiles.extend(_cp.infiles)
        self.antids.extend(_rgp.antids)
        self.fieldids.extend(_rgp.fieldids)
        self.spws.extend(_rgp.spwids)
        self.v_spws.extend(_rgp.v_spwids)
        self.pols.extend(_rgp.polslist)


class ToCombineImageParameters(Parameters):
    """Parameter class to combine image."""

    def __init__(self):
        """Initialize an object."""
        super().__init__()
        self.images = ObservedList()               # ObservedList[str]: List of image name
        self.images_nro = ObservedList()           # ObservedList[str]: List of image name for NRO
        self.org_directions = ObservedList()       # ObservedList[Direction]: List of directions of origin for ephemeris object
        self.org_directions_nro = ObservedList()   # ObservedList[Direction]: List of Directions for NRO
        self.specmodes = ObservedList()            # ObservedList[str]: List of spec mode


class PostProcessParameters(Parameters):
    """Parameters for post processing of image generating."""

    def __init__(self):
        """Initialize an object."""
        super().__init__()
        self.beam = None                           # Dict[str, Dict[str, float]]: Beam data
        self.brightnessunit = None                 # str: Brightness unit like 'Jy/beam'
        self.chan_width = None                     # numpy.float64: Channel width of faxis
        self.cs = None                             # coordsys: Coordsys object
        self.faxis = None                          # int: Spectral axis which is found
        self.imagename = None                      # str: Image name
        self.image_rms = None                      # float: Image statistics (rms)
        self.image_max = None                      # float: Image statistics (maximum value)
        self.image_min = None                      # float: Image statistics (minimum value)
        self.include_channel_range = None          # List[int]: List of channel ranges to calculate image statistics
        self.is_representative_source_and_spw = None  # bool: Flag of representative source spw and representative spw
        self.nx = None                             # numpy.int64: X of image shape
        self.ny = None                             # numpy.int64: Y of image shape
        self.org_direction = None                  # Direction: a direction of origin for ephemeris object
        self.qcell = None                          # Dict[str, Dict[str, float]]: Cell data
        self.raster_infos = None                   # List[RasterInfo]: List of RasterInfo(center/width/height/angle/row)
        self.region = None                         # str: Region to calculate statistics
        self.rmss = None                           # List[float]: List of RMSs
        self.stat_chans = None                     # str: Converted string from include_channel_range
        self.stat_freqs = None                     # str: Statistics frequencies
        self.theoretical_rms = None                # Dict[str, float]: Theoretical RMSs
        self.validsps = None                       # List[int]: List of valid spectrum

    def done(self):
        if isinstance(self.cs, coordsys):
            self.cs.done()


class TheoreticalImageRmsParameters(Parameters):
    """ Parameter class of calculate_theoretical_image_rms()."""

    def __init__(self, _pp: PostProcessParameters, context: 'Context'):
        """Initialize the object.

        Args:
            _pp : imaging post process parameters of prepare()
            context : pipeline Context
        """
        super().__init__()
        self.cqa = casa_tools.quanta       # LoggingQuanta: LoggingQuanta object
        self.failed_rms = self.cqa.quantity(-1, _pp.brightnessunit)    # Dict[str, float]: Failed RMS value
        self.sq_rms = 0.0                  # float: Square of RMS
        self.N = 0.0                       # float: Number of data for statistics
        self.time_unit = 's'               # str: Time unit
        self.ang_unit = self.cqa.getunit(_pp.qcell[0])     # str: Ang unit
        self.cx_val = self.cqa.getvalue(_pp.qcell[0])[0]   # float: cx
        self.cy_val = self.cqa.getvalue(self.cqa.convert(_pp.qcell[1], self.ang_unit))[0]  # float: cy
        self.bandwidth = numpy.abs(_pp.chan_width)         # float: Band width
        self.context = context                             # Context: Pipeline context
        self.is_nro = sdutils.is_nro(context)              # bool: NRO flag
        self.infile = None                 # str: Input file
        self.antid = None                  # int Antenna ID
        self.fieldid = None                # int: Field ID
        self.spwid = None                  # int: Spectrum ID
        self.pol_names = None              # List[str]: Polarization names
        self.polids = None                 # List[int]: Polarization ID
        self.raster_info = None            # RasterInfo: RasterInfo object
        self.msobj = None                  # MeasurementSet: MeasuremetSet
        self.calmsobj = None               # MeasurementSet: Calibrated MeasurementSet
        self.error_msg = None              # str: Error message
        self.dt = None                     # DataTableImpl: Datatable object
        self.index_list = None             # numpy.ndarray[int64]: Index list
        self.effBW = None                  # float: Effective BW
        self.mean_tsys_per_pol = None      # numpy.ndarray[float]: Mean of Tsys per polarization
        self.width = None                  # float: Width
        self.height = None                 # float: Height
        self.calst = None                  # IntervalCalState: Interval calibration state object
        self.t_on_act = None               # float: T_on actual
        self.t_sub_on = None               # float: Tsub on
        self.t_sub_off = None              # float: Tsub off
