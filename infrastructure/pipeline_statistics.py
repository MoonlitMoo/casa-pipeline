import datetime
import enum
from typing import Dict, List, Set, Union

import numpy as np

from pipeline import environment
from pipeline.domain import measures
from pipeline.domain.datatype import DataType
from pipeline.domain.measurementset import MeasurementSet

from . import logging

LOG = logging.get_logger(__name__)


class PipelineStatisticsLevel(enum.Enum):
    """
    An enum to specify which "level" of information an individual pipeline statistic applies to:
    SPW, EB, SOURCE, or MOUS.
    """
    MOUS = enum.auto()
    EB = enum.auto()
    SPW = enum.auto()
    SOURCE = enum.auto()


class PipelineStatistics(object):
    """A single unit of pipeline statistics information.

    Attributes:
        name: The name of this pipeline statistic
        value: The value for this pipeline statistic
        longdesc: A long description of the value
        origin: The stage that this value was calculated or populated in (Optional)
        units: The units associated with the value (Optional)
        level: A PipelineStatisticsLevel that specifies whether this value applies to a MOUS, EB, or SPW
        spw: The SPW this value applies to (if applicable) (Optional)
        eb: The EB or MS this value applies to (if applicable) (Optional)
        mous: The MOUS this value applies to
    """
    def __init__(self, name: str, value: Union[str, int, float, List, Dict, Set, np.int64, np.ndarray],
                 longdesc: str, origin: str='', units: str='',
                 level: PipelineStatisticsLevel=None, spw: str=None, mous: str=None, eb: str=None, 
                 source: str=None):

        self.name = name
        self.value = value
        self.longdesc = longdesc
        # The origin is the name of the pipeline stage associateted with this statistics value
        self.origin = origin
        self.units = units
        # The level indicates whether a given quantity applies to the whole MOUS, EB, or SPW
        self.level = level
        # The spw, mous, and/or eb are set, if applicable.
        self.mous = mous
        self.eb = eb
        self.spw = spw
        self.source = source

        # Convert initial value from the pipeline to a value that can be serialized by JSON
        # In the future, it may make sense to move this conversion to when the data is written out.
        if type(value) is set:
            self.value = list(self.value)
        elif type(value) is np.int64:
            self.value = int(self.value)
        elif type(value) is np.ndarray:
            self.value = list(self.value)

    def __str__(self) -> str:
        return 'PipelineStatistics({!s}, {!r}, {!r}, {!s})'.format(self.name, self.value, self.origin, self.units)

    def to_dict(self) -> Dict:
        """
        Convert an individual pipeline statistics item to a dictionary
        representation
        """
        stats_dict = {}

        if self.value not in ["", None]:
            stats_dict['value'] = self.value

        if self.units not in ["", None]:
            stats_dict['units'] = self.units

        if self.longdesc not in ["", None]: 
            stats_dict['longdescription'] = self.longdesc

        if self.origin not in ["", None]:
            stats_dict['origin'] = self.origin

        return stats_dict


def to_nested_dict(stats_collection) -> Dict:
    """
    Generates a nested output dict with EBs, SPWs, TARGETs, MOUSs
    Each level is represented in the structure of the output.
    """
    final_dict = {}

    eb_section_key = "EB"
    spw_section_key = "SPW"
    source_section_key = "TARGET"

    # Step through the collect statistics values and construct
    # a dictionary representation. The output format is as follows:
    # { mous_name: {
    #    mous_property: { ...
    #    }
    #    "EB": {
    #       eb_name: {
    #           eb_property: {...
    #           }
    #       }
    #    }
    #    "SPW": {
    #       spw_id: {
    #           spw_property: {...
    #           }
    #       }
    #    }
    #    "TARGET": {
    #       source_name: {
    #           source_property: {...
    #           }
    #       }
    #    }
    #  }
    #  header: {version: 0.1, creation_date: YYMMDD-HH:MM:SS Z}
    # }
    for stat in stats_collection:
        if stat.mous not in final_dict:
            final_dict[stat.mous] = {}
        if stat.level == PipelineStatisticsLevel.MOUS:
            final_dict[stat.mous][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.EB:
            if eb_section_key not in final_dict[stat.mous]:
                final_dict[stat.mous][eb_section_key] = {}
            if stat.eb not in final_dict[stat.mous][eb_section_key]:
                final_dict[stat.mous][eb_section_key][stat.eb] = {}
            final_dict[stat.mous][eb_section_key][stat.eb][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.SPW:
            if spw_section_key not in final_dict[stat.mous]:
                final_dict[stat.mous][spw_section_key] = {}
            if stat.spw not in final_dict[stat.mous][spw_section_key]:
                final_dict[stat.mous][spw_section_key][stat.spw] = {}
            final_dict[stat.mous][spw_section_key][stat.spw][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.SOURCE:
            if source_section_key not in final_dict[stat.mous]:
                final_dict[stat.mous][source_section_key] = {}
            if stat.source not in final_dict[stat.mous][source_section_key]:
                final_dict[stat.mous][source_section_key][stat.source] = {}
            final_dict[stat.mous][source_section_key][stat.source][stat.name] = stat.to_dict()
        else:
            LOG.debug("In pipleine statics file creation, invalid level: {} specified.".format(stat.level))

    # Generate and append a header with information about statistics file version and date created
    version_dict = _generate_header()
    final_dict['header'] = version_dict

    return final_dict


def _generate_header() -> Dict:
    """
    Creates a header with information about the pipeline stats file
    """
    version_dict = {}
    version_dict["version"] = 1.0
    now = datetime.datetime.now(datetime.timezone.utc)
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S %Z")
    version_dict["stats_file_creation_date"] = dt_string
    return version_dict


def _get_mous_values(context, mous: str, ms_list: List[MeasurementSet]) -> List[PipelineStatistics]:
    """
    Get the statistics values for a given MOUS
    """
    level = PipelineStatisticsLevel.MOUS
    stats_collection = []

    p1 = PipelineStatistics(
        name='project_id',
        value=context.observing_run.project_ids.pop(),
        longdesc='Proposal id number',
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p1)

    p2 = PipelineStatistics(
        name='pipeline_version',
        value=environment.pipeline_revision,
        longdesc="pipeline version string",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p2)

    p3 = PipelineStatistics(
        name='pipeline_recipe',
        value=context.project_structure.recipe_name,
        longdesc="recipe name",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p3)

    p4 = PipelineStatistics(
        name='casa_version',
        value=environment.casa_version_string,
        longdesc="casa version string",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p4)

    p5 = PipelineStatistics(
        name='mous_uid',
        value=context.get_oussid(),
        longdesc="Member Obs Unit Set ID",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p5)

    p6 = PipelineStatistics(
        name='n_EB',
        value=len(context.observing_run.execblock_ids),
        longdesc="number of execution blocks",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p6)

    all_bands = sorted({spw.band for spw in ms_list[0].get_all_spectral_windows()})
    p7 = PipelineStatistics(
        name='bands',
        value=all_bands,
        longdesc="Band(s) used in observations.",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p7)

    first_ms = ms_list[0]
    science_source_names = sorted({source.name for source in first_ms.sources if 'TARGET' in source.intents})
    p8 = PipelineStatistics(
        name='n_target',
        value=len(science_source_names),
        longdesc="total number of science targets in the MOUS",
        origin="hifa_importdata",
        mous=mous,
        level=level)
    stats_collection.append(p8)

    p9 = PipelineStatistics(
        name='target_list',
        value=science_source_names,
        longdesc="list of science target names",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(p9)

    p10 = PipelineStatistics(
        name='rep_target',
        value=first_ms.representative_target[0],
        longdesc="representative target name",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(p10)

    p11 = PipelineStatistics(
        name='n_spw',
        value=len(first_ms.get_all_spectral_windows()),
        longdesc="number of spectral windows",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(p11)

    return stats_collection


def _get_eb_values(context, mous: str, ms_list: List[MeasurementSet]) -> List[PipelineStatistics]:
    """
    Get the statistics values for a given EB
    """
    level = PipelineStatisticsLevel.EB
    stats_collection = []

    for ms in ms_list:
        eb = ms.name

        p1 = PipelineStatistics(
            name='n_ant',
            value=len(ms.antennas),
            longdesc="Number of antennas per execution block",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=level)
        stats_collection.append(p1)

        p2 = PipelineStatistics(
            name='n_scan',
            value=len(ms.get_scans()),
            longdesc="number of scans per EB",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=level)
        stats_collection.append(p2)

        l80 = np.percentile(ms.antenna_array.baselines_m, 80)
        p3 = PipelineStatistics(
            name='L80',
            value=l80,
            longdesc="80th percentile baseline",
            origin="hifa_importdata",
            units="m",
            mous=mous,
            eb=eb,
            level=level)
        stats_collection.append(p3)

    return stats_collection


def _get_spw_values(context, mous: str, ms_list: List[MeasurementSet]) -> List[PipelineStatistics]:
    """
    Get the statistics values for a given SPW
    """
    level = PipelineStatisticsLevel.SPW
    stats_collection = []
    ms = ms_list[0]
    spw_list = ms.get_all_spectral_windows()

    for spw in spw_list:
        p1 = PipelineStatistics(
            name='spw_width',
            value=float(spw.bandwidth.to_units(measures.FrequencyUnits.MEGAHERTZ)),
            longdesc="width of the spectral window",
            origin="hifa_importdata",
            units="MHz",
            spw=spw.id,
            mous=mous,
            level=level)
        stats_collection.append(p1)

        p2 = PipelineStatistics(
            name='spw_freq',
            value=float(spw.centre_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)),
            longdesc="central frequency of the spectral window in TOPO",
            origin="hifa_importdata",
            units="GHz",
            spw=spw.id,
            mous=mous,
            level=level)
        stats_collection.append(p2)

        p3 = PipelineStatistics(
            name='n_chan',
            value=spw.num_channels,
            longdesc="number of channels in the spectral window",
            origin="hifa_importdata",
            mous=mous,
            spw=spw.id,
            level=level)
        stats_collection.append(p3)

        p4 = PipelineStatistics(
            name='nbin_online',
            value=spw.sdm_num_bin,
            longdesc="online nbin factor",
            origin="hifa_importdata",
            spw=spw.id,
            mous=mous,
            level=level)
        stats_collection.append(p4)

        chan_width_MHz = [chan * 1e-6 for chan in spw.channels.chan_widths]
        p5 = PipelineStatistics(
            name='chan_width',
            value=chan_width_MHz[0],
            longdesc="frequency width of the channels in the spectral window",
            origin="hifa_importdata",
            units="MHz",
            spw=spw.id,
            mous=mous,
            level=level)
        stats_collection.append(p5)

        dd = ms.get_data_description(spw=int(spw.id))
        numpols = dd.num_polarizations

        p6 = PipelineStatistics(
            name='n_pol',
            value=numpols,
            longdesc="number of polarizations in the spectral window",
            origin="hifa_importdata",
            mous=mous,
            spw=spw.id,
            level=level)
        stats_collection.append(p6)

    return stats_collection


def _get_source_values(context, mous: str, ms_list: List[MeasurementSet]) -> List[PipelineStatistics]:
    """
    Get the statistics values for a given source
    """
    level = PipelineStatisticsLevel.SOURCE
    stats_collection = []
    first_ms = ms_list[0]

    science_sources = sorted({source for source in first_ms.sources
                              if 'TARGET' in source.intents}, key=lambda source: source.name)

    for source in science_sources:
        pointings = len([f for f in first_ms.fields if f.source_id == source.id])

        p1 = PipelineStatistics(
            name='n_pointings',
            value=pointings,
            longdesc="number of mosaic pointings for the science target",
            origin="hifa_importdata",
            mous=mous,
            source=source.name,
            level=level)
        stats_collection.append(p1)

    return stats_collection


def generate_product_pl_run_info(context) -> List[PipelineStatistics]:
    """
    Gather statistics results for the pipleline run information and pipeline product information
    These can be directly obtained from the context.
    """
    stats_collection = []
    mous = context.get_oussid()

    # List of datatypes to use (in order) for fetching EB-level information.
    # The following function call will fetch all the MSes for only the first
    # datatype it finds in the list. This is needed so that information is
    # not repeated for the ms and _targets.ms when both are present.
    datatypes = [DataType.REGCAL_CONTLINE_ALL, DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE,
        DataType.REGCAL_LINE_SCIENCE, DataType.SELFCAL_LINE_SCIENCE, DataType.RAW]
    ms_list = context.observing_run.get_measurement_sets_of_type(datatypes)

    # Add MOUS-level information
    mous_values = _get_mous_values(context, mous, ms_list)
    stats_collection.extend(mous_values)

    # Add per-EB information:
    eb_values = _get_eb_values(context, mous, ms_list)
    stats_collection.extend(eb_values)

    # Add per-SPW stats information
    # The spw ids from the first MS are used so the information will be included once per MOUS.
    spw_values = _get_spw_values(context, mous, ms_list)
    stats_collection.extend(spw_values)

    # Add per-SOURCE stats information
    source_values = _get_source_values(context, mous, ms_list)
    stats_collection.extend(source_values)

    return stats_collection
