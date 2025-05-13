"""
Module to define data type.

Classes:
    DataType: An Enum class to define data type.
"""

from enum import Enum, auto, unique
from operator import attrgetter

@unique
class DataType(Enum):
    """
    A class to define DataType enum.

    Attributes:
        RAW: raw data
        REGCAL_CONTLINE_ALL: calibrated data
        BASELINED: data after spectral baseline subtraction
        ATMCORR: data corrected for residual ATM
        REGCAL_CONT_SCIENCE: calibrated data of target scans used for cont only
        SELFCAL_CONT_SCIENCE: self-calibrated data of target scans used for cont only
        REGCAL_CONTLINE_SCIENCE: calibrated data of target scans
        SELFCAL_CONTLINE_SCIENCE: self-calibrated data of target scans
        REGCAL_LINE_SCIENCE: calibrated spectral line data
        SELFCAL_LINE_SCIENCE self-calibrated spectral line data
    """

    RAW = auto()
    REGCAL_CONTLINE_ALL = auto()
    ATMCORR = auto()
    BASELINED = auto()
    REGCAL_CONTLINE_SCIENCE = auto()
    SELFCAL_CONTLINE_SCIENCE = auto()
    REGCAL_CONT_SCIENCE = auto()
    SELFCAL_CONT_SCIENCE = auto()
    REGCAL_LINE_SCIENCE = auto()
    SELFCAL_LINE_SCIENCE = auto()

    @staticmethod
    def get_specmode_datatypes(intent, specmode):
        """
        Return the list of valid datatypes depending on the intent and specmode,
        in order of preference for the automatic choice of datatype (if not manually overridden).
        """
        if intent == 'TARGET':
            if specmode == 'mfs':
                # The preferred data types are SELFCAL_CONTLINE_SCIENCE and REGCAL_CONTLINE_SCIENCE.
                # The remaining fallback values are just there to support experimental usage of
                # the first set of MSes.
                specmode_datatypes = [DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE,
                                      DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
            elif specmode == 'cont':
                # The preferred data types are SELFCAL_CONTLINE_SCIENCE and REGCAL_CONTLINE_SCIENCE.
                # For VLA, also SELFCAL_CONT_SCIENCE and REGCAL_CONT_SCIENCE.
                # The remaining fallback values are just there to support experimental usage of
                # the first set of MSes.
                specmode_datatypes = [DataType.SELFCAL_CONT_SCIENCE, DataType.REGCAL_CONT_SCIENCE,
                                      DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE,
                                      DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
            else:
                # The preferred data types for cube and repBW specmodes are SELFCAL_LINE_SCIENCE and
                # REGCAL_LINE_SCIENCE. The remaining fallback values are just there to support
                # experimental usage of the first and second sets of MSes.
                specmode_datatypes = [DataType.SELFCAL_LINE_SCIENCE, DataType.REGCAL_LINE_SCIENCE,
                                      DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE,
                                      DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
        else:
            # Calibrators are only present in the first set of MSes.
            # Thus listing only their possible data types.
            specmode_datatypes = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
        return specmode_datatypes

    @staticmethod
    def get_short_datatype_desc(datatype_str):
        """
        Return a short summary string for weblog purposes.
        """
        if datatype_str == 'RAW':
            return '<span style="background-color:lightgray;">RAW</span>'
        elif 'REGCAL' in datatype_str:
            return '<span style="background-color:lightblue;">REGCAL</span>'
        elif 'SELFCAL' in datatype_str:
            return '<span style="background-color:palegreen;">SELFCAL</span>'
        else:
            return 'UNKNOWN'

TYPE_PRIORITY_ORDER = sorted(DataType.__members__.values(), key=attrgetter('value'))
