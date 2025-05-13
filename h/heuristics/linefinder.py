#*******************************************************************************
# ALMA - Atacama Large Millimeter Array
# Copyright (c) ATC - Astronomy Technology Center - Royal Observatory Edinburgh, 2011
# (in the framework of the ALMA collaboration).
# All rights reserved.
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
#*******************************************************************************
"""Find indice of lines from spectrum data."""

import numpy as np
import pipeline.infrastructure.api as api
from typing import List, Optional
# from asap.asaplinefind import linefinder
# from asap import _asap, scantable, rcParams


class HeuristicsLineFinder(api.Heuristic):
    """
    A heuristics class to invoke line finding on a spectrum.

    This class inherits Heuristic class.
    Methods:
        calculate: Override calculate method in the super class.
        tweak_lines: A helper function to tweak spectral line ranges.
    """

    def calculate(self, spectrum: List[float], threshold: float=7.0, tweak: bool=False, 
                  mask: List[bool]=[], edge: Optional[List[int]]=None) -> List[int]:
        """
        Invoke line finding algorithm and return indices of spectral line ranges.
        
        Line detection is performed based on the excess of the median absolute 
        deviation (MAD) of spectrum. Channels of spectrum larger than threshold*MAD 
        are detected as spectral lines. A tweak to line ranges to cover line wings 
        are optionally available.

        Args:
            spectrum: list of spectrum data.
            threshold: a factor of threshold of line detection with respect to MAD.
            tweak: if True, spectral line ranges are extended to cover line edges.
            mask: mask of spectrum array. An elements of spectrum is valid when 
                  the corresponding element of mask is True. If False, invalid. 
                  The length of list should be equal to that of spectrum.
            edge: the number of elements in left and right edges of spectrum to be
                  excluded from line detection.
        Returns:
            ranges: A list of start and end indices of spectral lines. The indices of 
                    lines are in the order of, e.g., [start1, end1, ..., startN, endN].
        """
        _spectrum = np.array(spectrum)
        if len(mask) == 0:
            mask = np.ones(len(_spectrum), dtype=int)
        else:
            mask = np.array(mask, int)
        if edge is not None:
            if len(edge) == 1:
                if edge[0] != 0:
                    mask[:edge[0]] = 0
                    mask[-edge[0]:] = 0
                    _edge = (edge[0], len(_spectrum)-edge[0])
                else:
                    _edge = (0, len(_spectrum))
            else:
                mask[:edge[0]] = 0
                if edge[1] != 0:
                    mask[-edge[1]:] = 0
                _edge = (edge[0], len(_spectrum)-edge[1])
        else:
            _edge = (0, len(_spectrum))
        indeces = np.arange(len(_spectrum))

        previous_line_indeces = np.array([], int)
        previous_mad = 1e6
        iteration = 0
        max_iteration = 10

        while iteration <= max_iteration:
            iteration += 1
            iteration_mask = np.array(mask)
            iteration_mask[previous_line_indeces] = 0
            variances = np.abs(_spectrum - np.median(_spectrum[iteration_mask==1]))

            good_variances = sorted(variances[iteration_mask == 1])
            good_variances = good_variances[:int(0.8*len(good_variances))]
            mad = np.median(good_variances)

            #line_indeces = indeces[np.logical_and(mask==1, variances > 7*mad)]
            line_indeces = indeces[np.logical_and(mask==1, variances > threshold*mad)]

            if mad > previous_mad or list(line_indeces) == list(previous_line_indeces):
                break
            else:
                previous_mad = mad
                previous_line_indeces = np.array(line_indeces)

        ranges = []
        range_start = None
        range_end = None

        for i in previous_line_indeces:
            if range_start is None:
                range_start = i
                range_end = i
            elif i == range_end + 1:
                range_end = i
#            elif range_end - range_start + 1 > 2:
            elif range_end - range_start + 1 > 1:
                ranges += [range_start, range_end]
                range_start = i
                range_end = i
            else:
                range_start = i
                range_end = i

        if range_start is not None and (range_end - range_start + 1 > 2):
            ranges += [range_start, range_end]
        if tweak:
            ranges = self.tweak_lines(_spectrum, ranges, _edge)

        #return len(ranges)/2
        return ranges

    def tweak_lines(self, spectrum: List[float], ranges: List[int], 
                    edge: List[int], n_ignore: int=1) -> List[int]:
        """
        Return extended line ranges to cover line edge channels.
        
        Spectrum level of channels around input line ranges are analyzed and 
        line ranges are expanded to the extent of channels where the level 
        monotonously decreases (emission lines) or increases (absorption lines).

        Args:
            spectrum: list of spectrum data.
            ranges: list of indice of line range.
            edge: a list of minimum and maximum indices of spectrum to consider 
            (excluding edge channels).
            n_ignore: the maximum number of channels to extend line ranges in 
            each direction.
        Returns:
            ranges: line of indice of start and end of line range.
        """
        med = np.median(spectrum)
        mask = np.array(spectrum) >= med
        for i in range(0, len(ranges), 2):
            if spectrum[ranges[i]] > med:
                # Emission Feature
                Mask = True
            else:
                # Absorption Feature
                Mask = False
            ignore = 0
            for j in range(ranges[i], edge[0]-1, -1):
                if (spectrum[j]-spectrum[j+1] > 0) == Mask:
                    ignore += 1
                if (mask[j] != Mask) or (ignore > n_ignore):
                    ranges[i] = j
                    break
            ignore = 0
            for j in range(ranges[i+1], edge[1]):
                if (spectrum[j]-spectrum[j-1] > 0) == Mask:
                    ignore += 1
                if (mask[j] != Mask) or (ignore > n_ignore):
                    ranges[i+1] = j
                    break
        return ranges
