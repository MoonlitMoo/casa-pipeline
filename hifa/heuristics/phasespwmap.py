import decimal
from typing import List, Tuple

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.domain import SpectralWindow
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


def combine_spwmap(scispws: List[SpectralWindow]) -> List:
    """
    Returns a spectral window map where each science spectral window is mapped
    to the lowest science spectral window ID that matches its Spectral Spec.

    Args:
        scispws: List of spectral window objects for science spectral windows.

    Returns:
        List of spectral window IDs, representing the spectral window map.
    """
    # Create dictionary of spectral specs and their corresponding science
    # spectral window ids.
    spspec_to_spwid_map = utils.get_spectralspec_to_spwid_map(scispws)

    # Identify highest science spw id, and initialize the spwmap for every
    # spectral window id through the max science spectral window id.
    max_scispwid = max([spw.id for spw in scispws])
    combinespwmap = list(range(max_scispwid + 1))

    # Make a reference copy for comparison.
    refspwmap = list(combinespwmap)

    # For each spectral spec, map corresponding science spws to the lowest
    # spw id of the same spectral spec.
    for spspec_spwids in spspec_to_spwid_map.values():
        combine_spwid = min(spspec_spwids)
        for spwid in spspec_spwids:
            combinespwmap[spwid] = combine_spwid

    # Return the new map
    if combinespwmap == refspwmap:
        return []
    else:
        return combinespwmap


def snr_n2wspwmap(scispws: List[SpectralWindow], snrs: List, goodsnrs: List) -> Tuple[bool, List, List]:
    """
    Compute a spectral window map based on signal-to-noise information.

    Args:
        scispws: List of spectral window objects for science spectral windows.
        snrs: List of snr values for scispws
        goodsnrs: Determines whether the SNR is good (True), bad (False), or
            undefined (None). At least one value per receiver band should be good.

    Returns:
        3 element tuple with:
          * boolean declaring if a good mapping was found for all SpWs.
          * list of spectral window IDs, representing the spectral window map.
          * list of booleans (or None) declaring if matched SpW had good SNR.
    """
    # Find the spw with largest good SNR for each receiver band
    snrdict = {}
    for scispw, snr, goodsnr in zip(scispws, snrs, goodsnrs):
        if goodsnr is not True:
            continue
        if scispw.band in snrdict:
            if snr > snrdict[scispw.band]:
                snrdict[scispw.band] = snr
        else:
            snrdict[scispw.band] = snr
    LOG.debug('Maximum SNR per receiver band dictionary %s' % snrdict)

    # Find a matching spw for each science spw
    matchedspws = []
    matchedgoodsnrs = []
    for scispw, snr, goodsnr in zip(scispws, snrs, goodsnrs):

        LOG.debug('Looking for match to spw id %s' % scispw.id)

        # Good SNR so match spw to itself regardless of any other criteria
        if goodsnr is True:
            matchedspws.append(scispw)
            matchedgoodsnrs.append(goodsnr)
            LOG.debug('Good SNR so matched spw id %s to itself' % scispw.id)
            continue

        if scispw.band not in snrdict:
            matchedspws.append(scispw)
            matchedgoodsnrs.append(None)
            LOG.debug('No good SNR spw in receiver band so match spw id %s to itself' % scispw.id)
            continue

        # Loop through the other science windows looking for a match
        bestspw = None
        bestsnr = None
        bestgoodsnr = None
        for matchspw, matchsnr, matchgoodsnr in zip(scispws, snrs, goodsnrs):

            # Skip self
            if matchspw.id == scispw.id:
                LOG.debug('Skipping match of spw %s to itself' % matchspw.id)
                continue

            # Don't match across receiver bands
            if matchspw.band != scispw.band:
                LOG.debug('Skipping bad receiver band match to spw id %s' % matchspw.id)
                continue

            # Don't match across SpectralSpec if a non-empty SpectralSpec is available (PIPE-316).
            if scispw.spectralspec and scispw.spectralspec != matchspw.spectralspec:
                LOG.debug('Skipping bad spectral spec match to spw id %s' % matchspw.id)
                continue

            # Skip bad SNR matches if at least one good SNR window in the receiver band exists
            if matchspw.band in snrdict and matchgoodsnr is not True:
                LOG.debug('Skipping match with poor SNR spw id %s' % matchspw.id)
                continue

            # First candidate match
            if bestspw is None:
                bestspw = matchspw
                bestsnr = matchsnr
                bestgoodsnr = matchgoodsnr
                LOG.debug('First possible match is to spw id %s' % matchspw.id)
            elif matchsnr > bestsnr:
                bestspw = matchspw
                bestsnr = matchsnr
                bestgoodsnr = matchgoodsnr
                LOG.debug('Found higher SNR match spw id %s' % matchspw.id)
            else:
                LOG.debug('SNR lower than previous match skipping spw id %s' % matchspw.id)

        # Append the matched spw to the list
        if bestspw is None:
            matchedspws.append(scispw)
            matchedgoodsnrs.append(goodsnr)
        else:
            matchedspws.append(bestspw)
            matchedgoodsnrs.append(bestgoodsnr)

    # Find the maximum science spw id
    max_spwid = 0
    for scispw in scispws:
        if scispw.id > max_spwid:
            max_spwid = scispw.id

    # Initialize the spwmap. All spw ids up to the maximum
    # science spw id must be defined.
    phasespwmap = []
    snrmap = []
    for i in range(max_spwid + 1):
        phasespwmap.append(i)
        snrmap.append(None)

    # Make a reference copy for comparison
    refphasespwmap = list(phasespwmap)

    # Set the science window spw map using the matching spw ids
    goodmap = True
    for scispw, matchspw, matchedgoodsnr in zip(scispws, matchedspws, matchedgoodsnrs):
        phasespwmap[scispw.id] = matchspw.id
        snrmap[scispw.id] = matchedgoodsnr
        if matchedgoodsnr is not True:
            goodmap = False

    # Return the new map
    if goodmap is True and phasespwmap == refphasespwmap:
        return True, [], []
    else:
        return goodmap, phasespwmap, snrmap


def simple_n2wspwmap(scispws: List[SpectralWindow], maxnarrowbw: str, maxbwfrac: float, samebb: bool) -> List:
    """
    Compute a simple phase up wide to narrow spectral window map.

    Args:
        scispws: List of spectral window objects for science spectral windows.
        maxnarrowbw: Maximum narrow bandwidth, e.g. '300MHz'
        maxbwfrac: Width must be > maxbwfrac * maximum bandwidth for a match
        samebb: If possible match within a baseband

    Returns:
        List of spectral window IDs, representing the spectral window map.
    """
    quanta = casa_tools.quanta

    # Find the maximum science spw bandwidth for each science receiver band.
    bwmaxdict = {}
    for scispw in scispws:
        bandwidth = scispw.bandwidth
        if scispw.band in bwmaxdict:
            if bandwidth > bwmaxdict[scispw.band]:
                bwmaxdict[scispw.band] = bandwidth
        else:
            bwmaxdict[scispw.band] = bandwidth

    # Convert the maximum narrow bandwidth to the correct format
    maxnbw = quanta.convert(quanta.quantity(maxnarrowbw), 'Hz')
    maxnbw = measures.Frequency(quanta.getvalue(maxnbw)[0], measures.FrequencyUnits.HERTZ)

    # Find a matching spw each science spw
    matchedspws = []
    failedspws = []
    for scispw in scispws:

        #  Wide spw, match spw to itself.
        if scispw.bandwidth > maxnbw:
            matchedspws.append(scispw)
            LOG.debug('Matched spw id %s to itself' % scispw.id)
            continue

        # Loop through the other science
        # windows looking for a match
        bestspw = None
        for matchspw in scispws:

            # Skip self
            if matchspw.id == scispw.id:
                LOG.debug('Skipping match with self for spw id %s' % matchspw.id)
                continue

            # Don't match across receiver bands
            if matchspw.band != scispw.band:
                LOG.debug('Skipping bad receiver band match for spw id %s' % matchspw.id)
                continue

            # Skip if the match window is narrower than the window in question
            # or narrower than a certain fraction of the maximum bandwidth. 
            if matchspw.bandwidth <= scispw.bandwidth or \
                    matchspw.bandwidth < decimal.Decimal(str(maxbwfrac)) * bwmaxdict[scispw.band]:
                LOG.debug('Skipping bandwidth match condition spw id %s' % matchspw.id)
                continue

            # First candidate match
            if bestspw is None:
                bestspw = matchspw

            # Find the spw with the closest center frequency
            elif not samebb:
                if abs(scispw.centre_frequency.value - matchspw.centre_frequency.value) < \
                        abs(scispw.centre_frequency.value - bestspw.centre_frequency.value):
                    bestspw = matchspw

            else:
                # If the candidate match is in the same baseband as the science spw but the current best
                # match is not then switch matches.
                if matchspw.baseband == scispw.baseband and bestspw.baseband != scispw.baseband:
                    bestspw = matchspw
                else:
                    if abs(scispw.centre_frequency.value - matchspw.centre_frequency.value) < \
                            abs(scispw.centre_frequency.value - bestspw.centre_frequency.value):
                        bestspw = matchspw

        # Append the matched spw to the list
        if bestspw is None:
            LOG.debug('    Simple spw mapping failed for spw id %s' % scispw.id)
            matchedspws.append(scispw)
            failedspws.append(scispw)
        else:
            matchedspws.append(bestspw)

    # Issue a warning if any spw failed spw mapping
    if len(failedspws) > 0:
        LOG.warning('Cannot map narrow spws %s to wider ones - defaulting these to standard mapping' %
                    [spw.id for spw in failedspws])

    # Find the maximum science spw id
    max_spwid = 0
    for scispw in scispws:
        if scispw.id > max_spwid:
            max_spwid = scispw.id

    # Initialize the spwmap. All spw ids up to the maximum science spw id must be
    # defined in the map passed to CASA.
    phasespwmap = []
    for i in range(max_spwid + 1):
        phasespwmap.append(i)

    # Make a reference copy for comparison
    refphasespwmap = list(phasespwmap)

    # Set the science window spw map using the matching spw ids
    for scispw, matchspw in zip(scispws, matchedspws):
        phasespwmap[scispw.id] = matchspw.id

    # Return the new map
    if phasespwmap == refphasespwmap:
        return []
    else:
        return phasespwmap


def update_spwmap_for_band_to_band(spwmap: List[int], dg_refspws: List[SpectralWindow],
                                   dg_srcspws: List[SpectralWindow]) -> List[int]:
    """
    This method updates the input SpW mapping to remap diffgain on-source SpWs to
    associated diffgain reference SpWs within the same baseband, and returns the
    updated SpW mapping.

    Args:
        spwmap: SpW mapping to update
        dg_refspws: diffgain reference SpWs
        dg_srcspws: diffgain on-source SpWs

    Returns:
        List representing the updated SpW mapping.
    """
    # Ensure that the length of the input SpW map is sufficient to include all
    # diffgain SpWs; if necessary, add the missing SpWs, where each missing SpW
    # is mapped to itself.
    max_spw_id = max(spw.id for spw in dg_refspws + dg_srcspws)
    if len(spwmap) < max_spw_id + 1:
        spwmap.extend(list(range(len(spwmap), max_spw_id + 1)))

    # Modify the SpW mapping to ensure that diffgain on-source SpWs are remapped
    # to a diffgain reference SpW with the same baseband.
    # PIPE-2059: this mapping can in principle be to any of the diffgain SpWs
    # that match the baseband, but here it will preferentially match to the
    # diffgain reference SpW with the highest ID.
    for dg_srcspw in dg_srcspws:
        for dg_refspw in dg_refspws:
            if dg_srcspw.baseband == dg_refspw.baseband:
                spwmap[dg_srcspw.id] = spwmap[dg_refspw.id]

    return spwmap
