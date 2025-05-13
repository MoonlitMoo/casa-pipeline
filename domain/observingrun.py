import collections
import itertools
import operator
import os
from typing import List, Tuple, Union, Optional, TYPE_CHECKING

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from .datatype import DataType, TYPE_PRIORITY_ORDER
from . import MeasurementSet

if TYPE_CHECKING:
    from datetime import datetime

LOG = infrastructure.get_logger(__name__)


def sort_measurement_set(ms: MeasurementSet) -> Tuple[int, 'datetime']:
    """Return sort key for measurement set object.

    Sort key consists of data priority and observation start time.
    Data priority is determined from the data types of given
    measurement set. Sort order of the data types is defined in
    DATA_TYPE_PRIORITY list. If no DataType is set to the MS,
    the lowest priority is assigned to the MS.

    Returns:
        Data priority and observation start time
    """
    data_types = set(itertools.chain(*ms.data_types_per_source_and_spw.values()))
    assert all([t in TYPE_PRIORITY_ORDER for t in data_types])
    if len(data_types) > 0:
        data_priority = max([TYPE_PRIORITY_ORDER.index(t) for t in data_types])
    else:
        # no data type assignments, assign lowest priority (largest value)
        data_priority = len(TYPE_PRIORITY_ORDER)
    observation_start_time = utils.get_epoch_as_datetime(ms.start_time)
    return data_priority, observation_start_time


class ObservingRun(object):
    def __init__(self):
        self.measurement_sets = []
        self.virtual_science_spw_ids = {}
        self.virtual_science_spw_names = {}
        self.virtual_science_spw_shortnames = {}

    def add_measurement_set(self, ms):
        if ms.basename in [m.basename for m in self.measurement_sets]:
            msg = '{0} is already in the pipeline context'.format(ms.name)
            LOG.error(msg)
            raise Exception(msg)

        # Initialise virtual science spw IDs from first MS
        if self.measurement_sets == []:
            self.virtual_science_spw_ids = \
                dict((int(s.id), s.name) for s in ms.get_spectral_windows(science_windows_only=True))
            self.virtual_science_spw_names = \
                dict((s.name, int(s.id)) for s in ms.get_spectral_windows(science_windows_only=True))
            self.virtual_science_spw_shortnames = {}
            for name in self.virtual_science_spw_names:
                if 'ALMA' in name:
                    i = name.rfind('#')
                    if i != -1:
                        self.virtual_science_spw_shortnames[name] = name[:i]
                    else:
                        self.virtual_science_spw_shortnames[name] = name
                else:
                    self.virtual_science_spw_shortnames[name] = name
        else:
            for s in ms.get_spectral_windows(science_windows_only=True):
                if s.name not in self.virtual_science_spw_names:
                    msg = 'Science spw name {0} (ID {1}) of EB {2} does not match spw names of first EB. Virtual spw' \
                          ' ID mapping will not work.'.format(s.name, s.id,
                                                              os.path.basename(ms.name).replace('.ms', ''))
                    LOG.error(msg)

        self.measurement_sets.append(ms)
        self.measurement_sets.sort(key=sort_measurement_set)

    def get_ms(self, name=None, intent=None):
        """Returns the first measurement set matching the given identifier.
        Identifier precedence is name then intent.
        """
        if name:
            for ms in self.measurement_sets:
                if name in (ms.name, ms.basename):
                    return ms

            for ms in self.measurement_sets:
                # single dish data are registered without the MS suffix
                with_suffix = '%s.ms' % name
                if with_suffix in (ms.name, ms.basename):
                    return ms

            raise KeyError('No measurement set found with name {0}'.format(name))

        if intent:
            # Remove any extraneous characters, as intent could be specified
            # as *BANDPASS* for example
            intent = intent.replace('*', '')
            for ms in self.measurement_sets:
                for field in ms.fields:
                    if intent in field.intents:
                        return ms
            raise KeyError('No measurement set found with intent {0}'.format(intent))

    def get_measurement_sets(self, names=None, intents=None, fields=None):
        """
        Returns measurement sets matching the given arguments.
        """
        candidates = self.measurement_sets

        # filter out MeasurementSets with no vis hits
        if names is not None:
            candidates = [ms for ms in candidates
                          if ms.name in names]

        # filter out MeasurementSets with no intent hits
        if intents is not None:
            if isinstance(intents, str):
                intents = utils.safe_split(intents)
            intents = set(intents)

            candidates = [ms for ms in candidates
                          if intents.issubset(ms.intents)]

        # filter out MeasurementSets with no field name hits
        if fields is not None:
            if isinstance(fields, str):
                fields = utils.safe_split(fields)
            fields_to_match = set(fields)

            candidates = [ms for ms in candidates
                          if fields_to_match.isdisjoint({field.name for field in ms.fields})]

        return candidates

    def get_measurement_sets_of_type(self, dtypes: List[DataType],
                                     msonly: bool=True,
                                     source: Optional[str]=None,
                                     spw: Optional[str]=None,
                                     vis: Optional[List[str]]=None) -> Union[List[MeasurementSet],
                                                                             Tuple[collections.OrderedDict, DataType]]:
        """
        Return a list of MeasurementSet domain object with matching DataType.

        The method returns a list of MSes of the first matching DataType in the
        list of dtypes.

        Args:
            dtypes: Search order of DataType. The search starts with the
                first DataType in the list and fallbacks to another DataType
                in the list only if no MS is found with the searched DataType.
                The search order of DataType is in the order of elements in
                list. Search stops at the first DataType with which at least
                one MS is found.
            msonly: If True, return a list of MS domain object only.
            source: Filter for particular source name selection (comma
                separated list of names).
            spw: Filter for particular virtual spw specification (comma
                separated list of virtual IDs).
            vis: Filter for list of MS names (list of strings)

        Returns:
            When msonly is True, a list of MeasurementSet domain objects of
            a matching DataType (and optionally sources and spws) is returned.
            Otherwise, a tuple of an ordered dictionary and a matched DataType
            (and optionally sources and spws) is returned. The ordered
            dictionary stores matching MS domain objects as keys and matching
            data column names as values. The order of elements is that appears
            in measurement_sets list attribute.
        """
        found = []
        column = []
        for dtype in dtypes:
            for ms in self.measurement_sets:
                # Filter for vis list
                if isinstance(vis, list):
                    if ms.name not in vis:
                        continue
                if spw is not None:
                    real_spw_ids = ','.join(str(self.virtual2real_spw_id(spw_id, ms)) for spw_id in spw.split(',') if self.virtual2real_spw_id(spw_id, ms) is not None)
                    if not real_spw_ids:
                        real_spw_ids = None
                else:
                    real_spw_ids = None
                dcol = ms.get_data_column(dtype, source, real_spw_ids)
                if dcol is not None:
                    found.append(ms)
                    column.append(dcol)
            if len(found) > 0:
                break
        if len(found) > 0:
            LOG.debug('Found {} MSes with type {}'.format(len(found), dtype))
        else:
            LOG.debug('No MSes are found with types {}'.format(dtypes))
        if msonly:
            return found
        else:
            if len(found) > 0:
                ms_dict = collections.OrderedDict( (m, c) for m, c in zip(found, column))
                return ms_dict, dtype
            else:
                return collections.OrderedDict(), None

    def get_fields(self, names=None):
        """
        Returns fields matching the given arguments from all measurement sets.
        """
        match = [ms.fields for ms in self.measurement_sets]
        # flatten the fields lists to one sequence
        match = itertools.chain(*match)

        if names is not None:
            if isinstance(names, str):
                names = utils.safe_split(names)
            names = set(names)
            match = [f for f in match if f.name in names]

        return match

    @staticmethod
    def get_real_spw_id_by_name(spw_name, target_ms):
        """
        Translate a (science) spw name to the real spw ID for a given MS.

        :param spw_name: the spw name to convert
        :type spw_name: string
        :param target_ms: the MS to map spw_name to
        :type target_ms: domain.MeasurementSet
        """
        spw_id = None
        for spw in target_ms.get_spectral_windows(science_windows_only=True):
            if spw.name == spw_name:
                spw_id = spw.id
        return spw_id

    def get_virtual_spw_id_by_name(self, spw_name):
        """
        Translate a (science) spw name to the virtual spw ID for this pipeline run.

        :param spw_name: the spw name to convert
        :type spw_name: string
        """
        return self.virtual_science_spw_names.get(spw_name, None)

    def virtual2real_spw_id(self, spw_id, target_ms):
        """
        Translate a virtual (science) spw ID to the real one for a given MS.

        :param spw_id: the spw id to convert
        :type spw_id: integer or str
        :param target_ms: the MS to map spw_id to
        :type target_ms: domain.MeasurementSet
        """
        return self.get_real_spw_id_by_name(self.virtual_science_spw_ids.get(int(spw_id), None), target_ms)

    def real2virtual_spw_id(self, spw_id, target_ms):
        """
        Translate a real (science) spw ID of a given MS to the virtual one for this pipeline run.

        :param spw_id: the spw id to convert
        :type spw_id: integer or str
        :param target_ms: the MS to map spw_id to
        :type target_ms: domain.MeasurementSet
        """
        return self.get_virtual_spw_id_by_name(target_ms.get_spectral_window(int(spw_id)).name)

    def get_real_spwsel(self, spwsel, vis):
        """
        Translate a virtual (science) spw selection to the real one for a given MS.

        :param spwsel: the list of spw selections to convert
        :type spwsel: list of strings
        :param vis: the list of MS names to map spwsel to
        :type vis: list of MS names
        """
        real_spwsel = []
        for spwsel_item, ms_name in zip(spwsel, vis):
            real_spwsel_items = []
            for spw_item in spwsel_item.split(','):
                if spw_item.find(':') == -1:
                    real_spw_id = self.virtual2real_spw_id(int(spw_item), self.get_ms(ms_name))
                    real_spwsel_items.append(str(real_spw_id))
                else:
                    virtual_spw_id, selection = spw_item.split(':')
                    real_spw_id = self.virtual2real_spw_id(int(virtual_spw_id), self.get_ms(ms_name))
                    real_spwsel_items.append('%s:%s' % (str(real_spw_id), selection))
            real_spwsel.append(','.join(real_spwsel_items))
        return real_spwsel

    @property
    def start_time(self):
        if not self.measurement_sets:
            return None
        earliest, _ = min([(ms, utils.get_epoch_as_datetime(ms.start_time)) for ms in self.measurement_sets],
                          key=operator.itemgetter(1))
        return earliest.start_time

    @property
    def start_datetime(self):
        if not self.start_time:
            return None
        return utils.get_epoch_as_datetime(self.start_time)

    @property
    def end_time(self):
        if not self.measurement_sets:
            return None
        latest, _ = max([(ms, utils.get_epoch_as_datetime(ms.end_time)) for ms in self.measurement_sets],
                        key=operator.itemgetter(1))
        return latest.end_time

    @property
    def end_datetime(self):
        if not self.end_time:
            return None
        return utils.get_epoch_as_datetime(self.end_time)

    @property
    def project_ids(self):
        return {ms.project_id for ms in self.measurement_sets}

    @property
    def schedblock_ids(self):
        return {ms.schedblock_id for ms in self.measurement_sets}

    @property
    def execblock_ids(self):
        return {ms.execblock_id for ms in self.measurement_sets}

    @property
    def observers(self):
        return {ms.observer for ms in self.measurement_sets}
