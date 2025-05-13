import collections
import itertools
import math
from typing import List, Union, Sequence

import numpy
from casatasks.private import simutil

from pipeline.infrastructure import casa_tools
from . import measures, Antenna
from .measures import Distance, DistanceUnits
from .. import infrastructure

LOG = infrastructure.get_logger(__name__)

Baseline = collections.namedtuple('Baseline', 'antenna1 antenna2 length')


class AntennaArray(object):
    def __init__(self, name: str, position, antennas: List[Antenna]):
        self.__name = name
        self.__position = position

        # antennas instance property must be set early for subsequent calls to
        # self.get_antenna(...) to succeed
        self.antennas = antennas

        # PIPE-1823
        # Prior to PIPE-1823, .antennas was considered mutable and .baselines was
        # calculated on demand. This led to gross inefficiencies and excessive memory
        # use, as described in PIPE-1823. To resolve this, antennas is now considered
        # immutable, allowing the array of baseline lengths to be precomputed and
        # stored as an instance property.
        #
        # Storing on the instance raises the context size by a small amount (ballpark
        # ~4k for ALMA, 0.5k for VLA). This is probably OK, but we could detach the
        # array from the context if this too proves a problem.
        self.baseline_lookup = self._calc_baseline_lookup(antennas)

        # Mask symmetric values and self-correlations (by specifying offset=1) from the
        # lookup table to give a 1-D array we can use for calculating statistics
        self.baselines_m = self.baseline_lookup[numpy.tril_indices_from(self.baseline_lookup, -1)]

        # create mask to omit diagonal (=self-correlations). This mask will be applied
        # when looking up indices of min/max baselines below.
        mask_self_corr = numpy.full(self.baseline_lookup.shape, True)
        numpy.fill_diagonal(mask_self_corr, False)

        # We want the IDs of the antennas that give the minimum and maximum baselines.
        # Using argmin/argmax returns the index within a flattened input array, and that
        # index can be reshaped into a 2D index using unravel_index. However, min/max
        # needs to be calculated on masked values to omit self-correlations, and this
        # masking warps the 1D coordinates, breaking the subsequent index unravelling.
        # To get around this we use numpy masked arrays; using argmin/argmax on the masked
        # array returns non-warped coordinates we can unravel and dereference on the original
        # unmasked array.
        ma = numpy.ma.array(self.baseline_lookup, mask=~mask_self_corr)

        if len(antennas) <= 1:
            # if there is only zero or one antenna, no need to calculate min/max baselines.
            self.baseline_min = None
            self.baseline_max = None
        else:
            min_x, min_y = numpy.unravel_index(
                indices=numpy.ma.argmin(ma, axis=None),
                shape=self.baseline_lookup.shape
            )
            self.baseline_min = Baseline(
                antenna1=self.get_antenna(id=min_x),
                antenna2=self.get_antenna(id=min_y),
                length=Distance(value=self.baseline_lookup[min_x][min_y], units=DistanceUnits.METRE)
            )

            max_x, max_y = numpy.unravel_index(
                indices=numpy.argmax(ma, axis=None),
                shape=self.baseline_lookup.shape
            )
            self.baseline_max = Baseline(
                antenna1=self.get_antenna(id=max_x),
                antenna2=self.get_antenna(id=max_y),
                length=Distance(value=self.baseline_lookup[max_x][max_y], units=DistanceUnits.METRE)
            )

        self._baselines = self.baselines_for_antennas([a.id for a in antennas])

    def __repr__(self):
        return 'AntennaArray({0!r}, {1}, {2!r})'.format(
            self.__name,
            self.__position,
            self.antennas
        )

    @property
    def name(self):
        return self.__name

    @property
    def position(self):
        return self.__position

    @property
    def elevation(self):
        """
        Get the array elevation as a CASA quantity.
        """
        return self.__position['m2']

    @property
    def latitude(self):
        """
        Get the array latitude as a CASA quantity.
        """
        return self.__position['m1']

    @property
    def longitude(self):
        """
        Get the array longitude as a CASA quantity.
        """
        return self.__position['m0']

    @property
    def centre(self):
        datum = self.__position['refer']
        if datum == 'ITRF':
            return self.__position

        qa = casa_tools.quanta
        longitude = qa.convert(self.longitude, 'rad')
        latitude = qa.convert(self.latitude, 'rad')
        elevation = qa.convert(self.elevation, 'm')

        s = simutil.simutil()
        return s.long2xyz(qa.getvalue(longitude)[0],
                          qa.getvalue(latitude)[0],
                          qa.getvalue(elevation)[0],
                          datum)

    def baselines_for_antennas(self, antenna_ids: Sequence[int]):
        unique_ids = set(antenna_ids)
        return [
            Baseline(
                antenna1=self.get_antenna(ant1),
                antenna2=self.get_antenna(ant2),
                length=Distance(self.baseline_lookup[ant1][ant2], DistanceUnits.METRE)
            ) for ant1, ant2 in itertools.combinations(unique_ids, 2)
        ]

    @property
    def baselines(self) -> List[Baseline]:
        LOG.warning('Deprecated: AntennaArray.baselines is deprecated. Use AntennaArray.baselines_m instead')
        return self._baselines

    @staticmethod
    def _calc_baseline_lookup(antennas: List[Antenna]) -> numpy.ndarray:
        """
        Calculate a 2D matrix of baseline lengths where:

         - x index = antenna 1 ID
         - y index = antenna 2 ID
         - value   = baseline in metres

        This matrix is rectangular and symmetric (that is, baseline between
        antenna 1 and antenna 2 is the same as between antenna 2 and antenna
        1). A regular non-sparse matrix is used over a sparse triangular
        matrix, valuing simplicity of implementation over efficiency.
        """
        # no baselines = zero baseline length
        if len(antennas) < 2:
            return numpy.zeros(shape=(len(antennas),)*2)

        # calculate the array size required for our baselines. Another assumption:
        # the antenna IDs zero indexed and continuous enough for a sparse matrix to
        # be unnecessary.
        max_id = max(a.id for a in antennas) + 1
        baselines = numpy.zeros((max_id, max_id))

        qa = casa_tools.quanta

        def diff(ant1: Antenna, ant2: Antenna, attr: str):
            """
            Function to return the position difference along the attr axis between two
            antennas. 
            """
            v1 = qa.getvalue(ant1.offset[attr])[0]
            v2 = qa.getvalue(ant2.offset[attr])[0]
            return v1-v2

        for (ant1, ant2) in itertools.combinations(antennas, 2):
            baseline_m = math.sqrt(diff(ant1, ant2, 'longitude offset')**2 +
                                   diff(ant1, ant2, 'latitude offset')**2 +
                                   diff(ant1, ant2, 'elevation offset')**2)
            baselines[ant1.id][ant2.id] = baseline_m
            baselines[ant2.id][ant1.id] = baseline_m

        return baselines

    def get_offset(self, antenna):
        """
        Get the offset of the given antenna from the centre of the array.
        """
        dx = antenna.offset['longitude offset']['value']
        dy = antenna.offset['latitude offset']['value']

        x_offset = measures.Distance(dx, measures.DistanceUnits.METRE)
        y_offset = measures.Distance(dy, measures.DistanceUnits.METRE)

        return x_offset, y_offset

    def get_baseline(
            self,
            antenna1: Union[int, str],
            antenna2: Union[int, str]
    ) -> Union[Baseline, None]:
        """
        Get the baseline distance between two antennas.

        Return the baseline length in metres between antennass identified by
        arguments antenna1 and antenna2. If an identifier does not match a
        known antenna, None will be returned.

        Antenna identifiers will be considered first as numeric antenna IDs,
        then as antenna names.

        # FIXME This function signature seems too wide. Do clients really call by
        # antenna name? Do they *really* handle None? Wouldn't it be better to let
        # the IndexError exception bubble up?
        """
        def get_antenna_by_id_then_name(predicate):
            try:
                return self.get_antenna(id=int(predicate))
            except (ValueError, IndexError):
                # ValueError = failed cast to int
                # IndexError = no antenna with that ID
                try:
                    return self.get_antenna(name=predicate)
                except IndexError:
                    return None

        ant1 = get_antenna_by_id_then_name(antenna1)
        ant2 = get_antenna_by_id_then_name(antenna2)
        if ant1 is None or ant2 is None:
            # no match by using arg as ID or antenna name
            return None

        return Baseline(
            ant1,
            ant2,
            Distance(self.baseline_lookup[ant1.id][ant2.id], DistanceUnits.METRE)
        )

    def get_antenna(self, id=None, name=None):
        if id is not None:
            l = [ant for ant in self.antennas if ant.id == id]
            if not l:
                raise IndexError('No antenna with ID {0}'.format(id))
            return l[0]

        if name is not None:
            l = [ant for ant in self.antennas if ant.name == name]
            if not l:
                raise IndexError('No antenna with name {0}'.format(name))  
            return l[0]

        raise Exception('No id or name given to get_antenna')

    @property
    def median_direction(self):
        """The median center direction for the array."""
        # construct lists of the longitude and latitude of each antenna.. 
        qt = casa_tools.quanta
        longs = [qt.getvalue(antenna.longitude) for antenna in self.antennas]
        lats = [qt.getvalue(antenna.latitude) for antenna in self.antennas]

        # .. and find the median of these lists 
        med_lon = numpy.median(numpy.array(longs))
        med_lat = numpy.median(numpy.array(lats))

        # Construct and return a CASA direction using these median values. As
        # antenna positions are given in radians, the units of the median
        # direction is set to radians too.  
        mt = casa_tools.measures
        return mt.direction(v0=qt.quantity(med_lon, 'rad'), 
                            v1=qt.quantity(med_lat, 'rad'))

    def __str__(self):
        names = ', '.join([antenna.name for antenna in self.antennas])
        return 'AntennaArray({0})'.format(names)
