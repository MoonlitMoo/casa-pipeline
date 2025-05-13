
# ------------------------------------------------------------------------------

# findrefant.py

# Description:
# ------------
# This file contains the reference antenna heuristics.

# The present heuristics are geometry and flagging.

# Classes:
# --------
# RefAntHeuristics - This class chooses the reference antenna heuristics.
# RefAntGeometry   - This class contains the geometry heuristics for the
#                    reference antenna.
# RefAntFlagging   - This class contains the flagging heuristics for the
#                    reference antenna.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

# Imports
# -------

import collections
import os
import operator
from typing import Dict

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


# ------------------------------------------------------------------------------
# class RefAntHeuristics
# ------------------------------------------------------------------------------

# RefAntHeuristics
# ----------------

# Description:
# ------------
# This class chooses the reference antenna heuristics.

# Inherited classes:
# ------------------
# api.Heuristics - The base class common to all types of heuristics.

# Public member variables:
# ------------------------
# vis      - This python string contains the MS name.
#
# field    - This python string or list of strings contains the field numbers
#            or IDs.  Presently it is used only for the flagging heuristic.
# spw      - This python string or list of strings contains the spectral
#            window numbers of IDs.  Presently it is used only for the
#            flagging heuristic.
# intent   - This python string or list of strings contains the intent(s).
#            Presently it is used only for the flagging heuristic.
#
# geometry - This python boolean determines whether the geometry heuristic will
#            be used.
# flagging - This python boolean determines whether the flagging heuristic will
#            be used.

# Public member functions:
# ------------------------
# __init__  - This public member function constructs an instance of the
#             RefAntHeuristics() class.
# calculate - This public member function forms the reference antenna list
#             calculated from the selected heuristics.

# Private member functions:
# -------------------------
# _get_names - This private member function gets the antenna names from the MS.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version created with public member variables vis, field,
#               spw, intent, geometry, and flagging; public member functions
#               __init__() and calculate(); and private member function
#               _get_names().

# ------------------------------------------------------------------------------

class RefAntHeuristics(object, metaclass=vdp.PipelineInputsMeta):
    refantignore = vdp.VisDependentProperty(default='')

# ------------------------------------------------------------------------------

# RefAntHeuristics::__init__

# Description:
# ------------
# This public member function constructs an instance of the RefAntHeuristics()
# class.

# The primary purpose of this class is to initialize the public member
# variables.  The defaults for all parameters (except context) are None.

# Inputs:
# -------
# vis        - This python string contains the MS name.
#
# field      - This python string or list of strings contains the field numbers
#              or IDs.  Presently it is used only for the flagging heuristic.
# spw        - This python string or list of strings contains the spectral
#              window numbers of IDs.  Presently it is used only for the
#              flagging heuristic.
# intent     - This python string or list of strings contains the intent(s).
#              Presently it is used only for the flagging heuristic.
#
# geometry   - This python boolean determines whether the geometry heuristic
#              will be used in automatic mode.
# flagging   - This python boolean determines whether the flagging heuristic
#              will be used in automatic mode.

# Outputs:
# --------
# None, returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def __init__( self, vis, field, spw, intent, geometry, flagging, refantignore=None):

        # Initialize the public member variables of this class

        self.vis = vis
        self.field = field
        self.spw = spw
        self.intent = intent
        self.geometry = geometry
        self.flagging = flagging
        self.refantignore = refantignore

        # Return None

# ------------------------------------------------------------------------------

# RefAntHeuristics::calculate

# Description:
# ------------
# This public member function forms the reference antenna list calculated from
# the selected heuristics.

# NB: A total score is calculated from all heuristics.  The best antennas have
# the highest scores, so a reverse sort is performed to obtain the final list.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The numpy array of strings containing the ranked reference antenna list,
# returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def calculate( self ):
        # If no heuristics are specified, return no reference antennas
        if not (self.geometry or self.flagging):
            return []

        # Get the antenna names and initialize the score dictionary
        names = self._get_names()
        LOG.debug('Got antenna name list {0}'.format(names))

        scores = {n: 0.0 for n in names}

        # For each selected heuristic, add the score for each antenna
        if self.geometry:
            geoClass = RefAntGeometry(self.vis)
            geoScore = geoClass.calc_score()
            for n in names:
                if n in geoScore:
                    scores[n] += geoScore[n]
                    LOG.debug(f'{self.vis}: Antenna {n} geometry score {geoScore[n]}  total score {scores[n]}')

        if self.flagging:
            flagClass = RefAntFlagging(self.vis, self.field, self.spw, self.intent)
            flagScore = flagClass.calc_score()
            for n in names:
                if n in flagScore:
                    scores[n] += flagScore[n]
                    LOG.info(f'{self.vis}: Antenna {n} flagging score {flagScore[n]} total score {scores[n]}')

            # PIPE-1805: identify antennas for which the flagging sub-score is
            # zero. Remove these from the list of refants to consider, unless
            # this would lead to an empty refant list.
            ants_to_remove = {ant for ant, score in flagScore.items() if score == 0}
            if ants_to_remove:
                # Ensure the ants to remove do not comprise all considered ants.
                if set(names).intersection(ants_to_remove) != set(names):
                    LOG.info(f"{self.vis}: Removing antenna(s) {', '.join(ants_to_remove)} from consideration for"
                             f" refant because flagging sub-score is zero.")
                    scores = {k: v for k, v in scores.items() if k not in ants_to_remove}
                else:
                    LOG.warning(f"{self.vis}: all antennas considered for refant have flagging sub-score of zero.")

        # Calculate the final score and return the list of ranked
        # reference antennas.  NB: The best antennas have the highest
        # score, so a reverse sort is required.
        return [k for k, _ in sorted(scores.items(), key=operator.itemgetter(1), reverse=True)]

# ------------------------------------------------------------------------------

# RefAntHeuristics::_get_names

# Description:
# ------------
# This private member function gets the antenna names from the MS.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The numpy array of strings containing the antenna names, returned via the
# function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _get_names(self):
        antenna_table = os.path.join(self.vis, 'ANTENNA')
        with casa_tools.TableReader(antenna_table) as table:
            names = table.getcol('NAME').tolist()

        # Remove ignored antennas
        if self.refantignore:
            LOG.warning('Antennas to be ignored: {0}'.format(self.refantignore))
            names = [n for n in names if n not in self.refantignore.split(',')]

        # Return the antenna names
        return names

# ------------------------------------------------------------------------------
# class RefAntGeometry
# ------------------------------------------------------------------------------

# RefAntGeometry
# --------------

# Description:
# ------------
# This class contains the geometry heuristics for the reference antenna.

# Algorithm:
# ----------
# * Calculate the antenna distances from the array center.
# * Normalize the distances by the maximum distance.
# * Calculate the score for each antenna, which is one minus the normalized
#   distance.  The best antennas have the highest score.
# * Sort according to score.

# Public member variables:
# ------------------------
# vis - This python string contains the MS name.

# Public member functions:
# ------------------------
# __init__   - This public member function constructs an instance of the
#              RefAntGeometry() class.
# calc_score - This public member function calculates the geometry score for
#              each antenna.

# Private member functions:
# -------------------------
# _get_info       - This private member function gets the information from the
#                   antenna table of the MS.
# _get_measures   - This private member function gets the measures from the
#                   antenna table of the MS.
# _get_latlongrad - This private member function gets the latitude, longitude
#                   and radius (from the center of the earth) for each antenna.
# _calc_distance  - This private member function calculates the antenna
#                   distances from the array reference from the radii,
#                   longitudes, and latitudes.
# _calc_score     - This private member function calculates the geometry score
#                   for each antenna.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

class RefAntGeometry:

# ------------------------------------------------------------------------------

# RefAntGeometry::__init__

# Description:
# ------------
# This public member function constructs an instance of the RefAntGeometry()
# class.

# Inputs:
# -------
# vis - This python string contains the MS name.

# Outputs:
# --------
# None, returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def __init__( self, vis ):

        # Set the public variables

        self.vis = vis


        # Return None

        return None

# ------------------------------------------------------------------------------

# RefAntGeometry::calc_score

# Description:
# ------------
# This public member function calculates the geometry score for each antenna.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The python dictionary containing the score for each antenna, returned via the
# function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def calc_score( self ):

        # Get the antenna information, measures, and locations

        info = self._get_info()
        measures = self._get_measures( info )
        radii, longs, lats = self._get_latlongrad( info, measures )


        # Calculate the antenna distances and scores

        distance = self._calc_distance( radii, longs, lats )
        score = self._calc_score( distance )


        # Return the scores

        return score

# ------------------------------------------------------------------------------

# RefAntGeometry::_get_info

# Description:
# ------------
# This private member function gets the information from the antenna table of
# the MS.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The python dictionary containing the antenna information, returned via the
# function value.  The dictionary format is:
# 'position'          - This numpy array contains the antenna positions.
# 'flag_row'          - This numpy array of booleans contains the flag row
#                       booleans.  NB: This element is of limited use now and
#                       may be eliminated.
# 'name'              - This numpy array of strings contains the antenna names.
# 'position_keywords' - This python dictionary contains the antenna information.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _get_info(self):

        # Create the local instance of the table tool and open it with
        # the antenna subtable of the MS
        with casa_tools.TableReader(self.vis + '/ANTENNA') as table:

            # Get the antenna information from the antenna table
            info = dict()

            info['position'] = table.getcol('POSITION')
            info['flag_row'] = table.getcol('FLAG_ROW')
            info['name'] = table.getcol('NAME')
            info['position_keywords'] = table.getcolkeywords('POSITION')

        # The flag tool appears to return antenna names as upper case,
        # which seems to be different from the antenna names stored in
        # MSes.  Therefore, these names will be capitalized here.
        # for r in range(len(info['name'])):
        # info['name'][r] = info['name'][r].upper()

        # Return the antenna information

        return info

# ------------------------------------------------------------------------------

# RefAntGeometry::_get_measures

# Description:
# ------------
# This private member function gets the measures from the antenna table of the
# MS.

# Inputs:
# -------
# info - This python dictionary contains the antenna information from private
#        member function _get_info().

# Outputs:
# --------
# The python dictionary containing the antenna measures, returned via the
# function value.  The dictionary format is:
# '<antenna name>' - The python dictionary containing the antenna measures.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _get_measures(self, info):

        # Create the local instances of the measures and quanta tools
        meLoc = casa_tools.measures
        qaLoc = casa_tools.quanta

        # Initialize the measures dictionary and the position and
        # position_keywords variables

        measures = dict()

        position = info['position']
        position_keywords = info['position_keywords']

        rf = position_keywords['MEASINFO']['Ref']

        for row, ant in enumerate(info['name']):

            if not info['flag_row'][row]:

                p = position[0, row]
                pk = position_keywords['QuantumUnits'][0]
                v0 = qaLoc.quantity(p, pk)

                p = position[1, row]
                pk = position_keywords['QuantumUnits'][1]
                v1 = qaLoc.quantity(p, pk)

                p = position[2, row]
                pk = position_keywords['QuantumUnits'][2]
                v2 = qaLoc.quantity(p, pk)

                measures[ant] = meLoc.position(rf=rf, v0=v0, v1=v1, v2=v2)

        # Close the local instances of the measures and quanta tools
        qaLoc.done()
        meLoc.done()

        # Return the measures

        return measures

# ------------------------------------------------------------------------------

# RefAntGeometry::_get_latlongrad

# Description:
# ------------
# This private member function gets the latitude, longitude and radius (from the
# center of the earth) for each antenna.

# Inputs:
# -------
# info     - This python dictionary contains the antenna information from
#            private member function _get_info().
# measures - This python dictionary contains the antenna measures from private
#            member function _get_measures().

# Outputs:
# --------
# The python tuple containing containing radius, longitude, and latitude python
# dictionaries, returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _get_latlongrad( self, info, measures ):

        # Create the local instance of the quanta tool
        qaLoc = casa_tools.quanta

        # Get the radii, longitudes, and latitudes
        radii = dict()
        longs = dict()
        lats = dict()

        for ant in info['name']:

            value = measures[ant]['m2']['value']
            unit = measures[ant]['m2']['unit']
            quantity = qaLoc.quantity( value, unit )
            convert = qaLoc.convert( quantity, 'm' )
            radii[ant] = qaLoc.getvalue( convert )

            value = measures[ant]['m0']['value']
            unit = measures[ant]['m0']['unit']
            quantity = qaLoc.quantity( value, unit )
            convert = qaLoc.convert( quantity, 'rad' )
            longs[ant] = qaLoc.getvalue( convert )

            value = measures[ant]['m1']['value']
            unit = measures[ant]['m1']['unit']
            quantity = qaLoc.quantity( value, unit )
            convert = qaLoc.convert( quantity, 'rad' )
            lats[ant] = qaLoc.getvalue( convert )

        # Delete the local instance of the quanta tool
        qaLoc.done()

        # Return the tuple containing the radius, longitude, and
        # latitude python dictionaries
        return radii, longs, lats

# ------------------------------------------------------------------------------

# RefAntGeometry::_calc_distance

# Description:
# ------------
# This private member function calculates the antenna distances from the array
# reference from the radii, longitudes, and latitudes.

# NB: The array reference is the median location.

# Inputs:
# -------
# radii - This python dictionary contains the radius (from the center of the
#         earth) for each antenna.
# longs - This python dictionary contains the longitude for each antenna.
# lats  - This python dictionary contains the latitude for each antenna.

# Outputs:
# --------
# The python dictionary containing the antenna distances from the array
# reference, returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _calc_distance( self, radii, longs, lats ):

        # Convert the dictionaries to numpy float arrays.  The median
        # longitude is subtracted.
        radiusValues = numpy.array(list(radii.values()))

        longValues = numpy.array(list(longs.values()))
        longValues -= numpy.median(longValues)

        latValues = numpy.array(list(lats.values()))

        # Calculate the x and y antenna locations.  The medians are
        # subtracted.
        x = longValues * numpy.cos(latValues) * radiusValues
        x -= numpy.median(x)

        y = latValues * radiusValues
        y -= numpy.median(y)

        # Calculate the antenna distances from the array reference and
        # return them
        distance = dict()
        names = list(radii.keys())

        for i, ant in enumerate(names):
            distance[ant] = numpy.sqrt(pow(x[i], 2) + pow(y[i], 2))

        return distance

# ------------------------------------------------------------------------------

# RefAntGeometry::_calc_score

# Description:
# ------------
# This private member function calculates the geometry score for each antenna.

# Algorithm:
# ----------
# * Calculate the antenna distances from the array center.
# * Normalize the distances by the maximum distance.
# * Calculate the score for each antenna, which is one minus the normalized
#   distance.  The best antennas have the highest score.
# * Sort according to score.

# Inputs:
# -------
# distance - This python dictionary contains the antenna distances from the
#            array reference.  They are calculated in private member function
#            _calc_distance().

# Outputs:
# --------
# The python dictionary containing the score for each antenna, returned via the
# function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _calc_score(self, distance):

        # Get the number of good data, calculate the fraction of good
        # data, and calculate the good and bad weights
        far = numpy.array(list(distance.values()), float)
        fFar = far / float(numpy.max(far))

        wFar = fFar * len(far)
        wClose = (1.0 - fFar) * len(far)

        # Calculate the score for each antenna and return them
        score = dict()

        names = list(distance.keys())

        for n in range(len(wClose)):
            score[names[n]] = wClose[n][0]

        return score

# ------------------------------------------------------------------------------

# RefAntFlagging
# --------------

# Description:
# ------------
# This class contains the flagging heuristics for the reference antenna.

# Algorithm:
# ----------
# * Get the number of unflagged (good) data for each antenna.
# * Normalize the good data by the maximum good data.
# * Calculate the score for each antenna, which is one minus the normalized
#   number of good data.  The best antennas have the highest score.
# * Sort according to score.

# Public member variables:
# ------------------------
# vis    - This python string contains the MS name.
#
# field  - This python string or list of strings contains the field numbers or
#          or IDs.
# spw    - This python string or list of strings contains the spectral window
#          numbers of IDs.
# intent - This python string or list of strings contains the intent(s).

# Public member functions:
# ------------------------
# __init__   - This public member function constructs an instance of the
#              RefAntFlagging() class.
# calc_score - This public member function calculates the flagging score for
#              each antenna.

# Private member functions:
# -------------------------
# _get_good   - This private member function gets the number of unflagged (good)
#               data from the MS.
# _calc_score - This private member function calculates the flagging score for
#               each antenna.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

class RefAntFlagging:

# ------------------------------------------------------------------------------

# RefAntFlagging::__init__

# Description:
# ------------
# This public member function constructs an instance of the RefAntFlagging()
# class.

# Inputs:
# -------
# vis    - This python string contains the MS name.
#
# field  - This python string or list of strings contains the field numbers or
#          or IDs.
# spw    - This python string or list of strings contains the spectral window
#          numbers of IDs.
# intent - This python string or list of strings contains the intent(s).

# Outputs:
# --------
# None, returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def __init__( self, vis, field, spw, intent ):
        self.vis = vis
        self.field = field
        self.spw = spw
        self.intent = intent

# ------------------------------------------------------------------------------

# RefAntFlagging::calc_score

# Description:
# ------------
# This public member function calculates the flagging score for each antenna.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The python dictionary containing the score for each antenna, returned via the
# function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def calc_score(self) -> Dict[str, float]:
        """
        Calculate the number of unflagged (good) measurements for each
        antenna, determine the score, and return them.

        PIPE-1805: first compute individual scores for each intent, then
        combine into a single representative score.

        Returns:
            Dictionary with antenna ID as key, and score as value.
        """
        all_scores = collections.defaultdict(list)
        for intent in self.intent.split(','):
            # Retrieve number of unflagged scans per antenna for current intent.
            good = self._get_good(intent)
            LOG.info(f'Get good antennas {good} for intent {intent}')

            # Compute score per antenna for current intent.
            score = self._calc_score(good)
            LOG.info(f'Get good antenna score {score} for intent {intent}')

            # Add to combined score.
            for k, v in score.items():
                all_scores[k].append(v)

        # For each antenna, combine the score-per-intent into a single
        # representative score by taking the lowest score.
        combined_scores = {ant: min(scores) for ant, scores in all_scores.items()}

        return combined_scores

# ------------------------------------------------------------------------------

# RefAntFlagging::_get_good

# Description:
# ------------
# This private member function gets the number of unflagged (good) data from the
# MS.

# Inputs:
# -------
# None.

# Outputs:
# --------
# The dictionary containing the number of unflagged (good) data from the MS,
# returned via the function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    def _get_good(self, intent: str) -> Dict[str, float]:
        # Update April 2015 to use the flagging task instead of the agent flagger
        task_args = {'vis'          : self.vis,
                     'mode'         : 'summary',
                     'field'        : self.field,
                     'spw'          : self.spw,
                     'intent'       : intent,
                     'display'      : '',
                     'flagbackup'   : False,
                     'savepars'     : False}
        task = casa_tasks.flagdata(**task_args)
        result = task.execute()

        # Calculate the number of good data for each antenna and return them
        good = dict()
        try:
            antennas = result['antenna']
            for ant in antennas:
                good[ant] = antennas[ant]['total'] - antennas[ant]['flagged']
        except:
            msg = "The CASA 'flagdata' task returned invalid results, unable to rank based on flagging score."
            raise Exception(msg)

        return good

# ------------------------------------------------------------------------------

# RefAntFlagging::_calc_score

# Description:
# ------------
# This private member function calculates the flagging score for each antenna.

# Algorithm:
# ----------
# * Get the number of unflagged (good) data for each antenna.
# * Normalize the good data by the maximum good data.
# * Calculate the score for each antenna, which is one minus the normalized
#   number of good data.  The best antennas have the highest score.
# * Sort according to score.

# Inputs:
# -------
# good - This python dictionary contains the number of unflagged (good) data
#        from the MS.  They are obtained in private member function _get_good().

# Outputs:
# --------
# The python dictionary containing the score for each antenna, returned via the
# function value.

# Modification history:
# ---------------------
# 2012 May 21 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------

    @staticmethod
    def _calc_score(good: Dict[str, float]) -> Dict[str, float]:
        if good:
            # Maximum number of unflagged datapoints across all antennas.
            max_unflagged = max(good.values())

            # For each antenna, compute score as the fraction of unflagged
            # datapoints over the maximum nr. of unflagged datapoints across
            # all antennas, multiplied by the number of antennas for weighting.
            if max_unflagged:
                score = {ant: len(good) * n_unflagged / max_unflagged for ant, n_unflagged in good.items()}
            else:
                # If no unflagged datapoints are found at all, then return a
                # score of 0 for each antenna.
                score = {ant: 0. for ant in good}
        else:
            score = {}

        return score
