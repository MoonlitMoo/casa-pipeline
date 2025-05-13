import os
from typing import Dict, List, Optional

import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.domain import measures
from pipeline.domain import SpectralWindow
from pipeline.h.heuristics import tsysspwmap
from pipeline.h.tasks.common import calibrationtableaccess as caltableaccess
from pipeline.h.tasks.common import commonresultobjects
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


class AtmHeuristics(object):
    def __init__(self, context: Context, vis: str, spw: List[SpectralWindow]):
        self.context = context
        self.vis = vis
        self.science_spws = spw
        self.opacities = {}
        self.calculated = False

    def _calculate(self):
        LOG.info("Calculating opacities for {}...".format(os.path.basename(self.vis)))

        # get channel information for each spw
        centre_freq = []
        width = []
        resolution = []
        spw_to_band = {}

        for ispw, spw in enumerate(self.science_spws):
            spw_to_band[spw.id] = ispw

            channels = spw.channels

            # translate from Stewart's domain measures to CASA quantities
            freqs = np.zeros([len(channels)], np.double)
            channel_unit = channels[0].low.units['symbol']
            freq = np.zeros([len(channels)], np.double)
            channel_width = np.zeros([len(channels)], np.double)
            res = np.zeros([len(channels)], np.double)

            for i in np.arange(len(freqs)):
                freq[i] = float(channels[i].low.value +
                                channels[i].high.value) / 2.0
                channel_width[i] = float(channels[i].low.value -
                                         channels[i].high.value)
                res[i] = float(channels[i].low.value - channels[i].high.value)

            centre_frequency = spw.centre_frequency
            centre_freq.append(float(centre_frequency.value))
            centre_freq_unit = centre_frequency.units['symbol']
            resolution.append(abs(channel_width[0]))
            resolution_unit = channel_unit
            bandwidth = spw.bandwidth
            width.append(float(bandwidth.value))
            width_unit = bandwidth.units['symbol']

        # canonical atmospheric params
        pressure = 563.0
        humidity = 20.0
        temperature = 273.0
        pwv = 1.0

        # tropical = 1
        # mid_latitude_summer = 2
        mid_latitude_winter = 3

        fcentre = casa_tools.quanta.quantity(centre_freq, centre_freq_unit)
        fresolution = casa_tools.quanta.quantity(resolution, resolution_unit)
        fwidth = casa_tools.quanta.quantity(width, width_unit)

        # setup atm
        casa_tools.atmosphere.initAtmProfile(
            humidity=humidity,
            temperature=casa_tools.quanta.quantity(temperature, "K"),
            altitude=casa_tools.quanta.quantity(5059, "m"),
            pressure=casa_tools.quanta.quantity(pressure, 'mbar'),
            atmType=mid_latitude_winter)
        casa_tools.atmosphere.initSpectralWindow(len(centre_freq), fcentre, fwidth, fresolution)
        casa_tools.atmosphere.setUserWH2O(casa_tools.quanta.quantity(pwv, 'mm'))

        for spw in self.science_spws:
            band = spw_to_band[spw.id]

            numchan = casa_tools.atmosphere.getNumChan(band)
            refchan = casa_tools.atmosphere.getRefChan(band)
            reffreq = casa_tools.atmosphere.getRefFreq(band)
            reffreq = casa_tools.quanta.convert(reffreq, 'GHz')
            chansep = casa_tools.atmosphere.getChanSep(band)
            chansep = casa_tools.quanta.convert(chansep, 'GHz')

            # axis object describing channel/freq axis
            freqs = np.zeros([numchan], np.double)
            for i in np.arange(numchan):
                freqs[i] = reffreq['value'][0] + float(i - refchan) * \
                  chansep['value'][0] 
            axis = commonresultobjects.ResultAxis(name='Frequency',
                                                  units='GHz', data=freqs)

            # calculate opacities
            dry = np.array(casa_tools.atmosphere.getDryOpacitySpec(band)[1])
            wet = np.array(casa_tools.atmosphere.getWetOpacitySpec(band)[1]['value'])

            # object containing result
            opacity = commonresultobjects.SpectrumResult(
                axis=axis,
                data=wet+dry,
                datatype='opacity',
                spw=spw.id)

            self.opacities[spw.id] = opacity

        self.calculated = True

    def _calculate_median_tsys(self, table: str, intent: str) -> Dict:
        ms = self.context.observing_run.get_ms(name=self.vis)

        # Get the Tsys spw map from caltable.
        unmapped, mapped = tsysspwmap(ms, table)

        # Get list of science spw ids to consider.
        sci_spwids = [spw.id for spw in self.science_spws]

        # Initialize dictionary of Tsys measurements per science spw.
        tsys = {spwid: [] for spwid in sci_spwids}

        # Identify which science spws have been mapped to a Tsys.
        sci_spw_mapped = [spwid for spwid in sci_spwids if spwid not in unmapped]

        # Only go through the Tsys caltable if there were any mapped science
        # spws.
        if sci_spw_mapped:
            # Compute a translation dictionary from Tsys spw to corresponding
            # science spw.
            tsys_to_sci_spwmap = {mapped[spwid]: spwid for spwid in sci_spw_mapped}

            # Get field ids to consider, based on intents used for QA.
            fieldids = [field.id for field in ms.get_fields(intent=intent)]

            # Load the tsys caltable to assess.
            tsystable = caltableaccess.CalibrationTableDataFiller.getcal(table)

            # Go through each row of Tsys caltable:
            for row in tsystable.rows:
                # Get spw for current row.
                row_spwid = row.get('SPECTRAL_WINDOW_ID')

                # Extract info from rows matching the spws and fields
                # (intents) to consider.
                if row_spwid in tsys_to_sci_spwmap and row.get('FIELD_ID') in fieldids:
                    # Get tsys spectrum and corresponding flags.
                    spec = row.get('FPARAM')
                    flag = row.get('FLAG')

                    # Add unflagged tsys measurements to overall list for this
                    # spw.
                    tsys[tsys_to_sci_spwmap[row_spwid]].extend(list(spec[np.logical_not(flag)]))

        # Calculate median for each spw; for each spwid where no Tsys was
        # available, this will be NaN.
        median_tsys = {spwid: np.median(tsys[spwid]) for spwid in sci_spwids}

        return median_tsys

    def spwid_rank_by_frequency(self) -> List[str]:
        """
        Return the spw id of the science spw with highest centre
        frequency.
        """
        # construction of spw_freqs assumes freqs for all spws have
        # in same units
        spw_freqs = [float(spw.centre_frequency.value) for spw in
                     self.science_spws]
        spw_ids = [spw.id for spw in self.science_spws]

        result = spw_ids[np.argsort(spw_freqs)[::-1]]
        result = list(map(str, result))

        for ispw, spw_id in enumerate(spw_ids):
            LOG.info('spw: %s median opacity: %s' % (spw_id, spw_freqs[ispw]))
        LOG.info('spw rank: %s' % result)

        return result

    def spwid_rank_by_opacity(self) -> List[str]:
        if not self.calculated:
            self._calculate()

        median_opacity = np.zeros([len(self.science_spws)])
        spw_ids = np.array([spw.id for spw in self.science_spws])

        for ispw, spw_id in enumerate(spw_ids):
            median_opacity[ispw] = np.median(self.opacities[spw_id].data)

        result = spw_ids[np.argsort(median_opacity)[::-1]]
        result = list(map(str, result))

        for ispw, spw_id in enumerate(spw_ids):
            LOG.info('spw: %s median opacity: %s' % (spw_id, median_opacity[ispw]))
        LOG.info('spw rank: %s' % result)

        return result

    # Metric to rank spws by a combination of spw bandwidth (higher is better)
    # and median opacity (lower is better), see CAS-10407.
    def spwid_rank_by_opacity_and_bandwidth(self) -> List[str]:
        if not self.calculated:
            self._calculate()

        metric = np.zeros([len(self.science_spws)])
        spw_ids = np.array([spw.id for spw in self.science_spws])

        for ispw, spw in enumerate(self.science_spws):
            metric[ispw] = float(np.sqrt(spw.bandwidth.to_units(measures.FrequencyUnits.GIGAHERTZ))) * \
                           np.exp(-1.0 * np.median(self.opacities[spw.id].data))

        result = spw_ids[np.argsort(metric)[::-1]]
        result = list(map(str, result))

        for ispw, spw_id in enumerate(spw_ids):
            LOG.info('spw: %s => spw score metric: %s' % (spw_id, metric[ispw]))
        LOG.info('spw rank: %s' % result)

        return result

    # Metric to rank spws by a combination of spw bandwidth (higher is better)
    # and median Tsys (lower is better), see CAS-10407.
    def spwid_rank_by_tsys_and_bandwidth(self, intent: str) -> Optional[List[str]]:

        # Check if Tsys caltable is available for vis.
        tsystable = None
        caltables = self.context.callibrary.active.get_caltable(caltypes='tsys')
        for caltable in caltables:
            if caltableaccess.CalibrationTableDataFiller._readvis(caltable) in self.vis:
                tsystable = caltable
                break

        # If Tsys caltable is available, then calculate the median Tsys,
        # otherwise return without ranked list.
        if tsystable:
            median_tsys = self._calculate_median_tsys(tsystable, intent)
        else:
            LOG.info("No unapplied Tsys table found in callibrary for {}; "
                     "cannot rank spws by Tsys and "
                     "bandwidth.".format(os.path.basename(self.vis)))
            return None

        # If no median Tsys could be calculated for any spw (e.g. due to
        # flagging, or due to science spws not being mapped to corresponding
        # Tsys spws), then return without ranked list.
        if not np.any(np.isfinite(list(median_tsys.values()))):
            LOG.warning("No valid median Tsys values found for spws for {} "
                        "(Too much flagging? Science spws not mapped to Tsys "
                        "spws?); cannot rank spws by Tsys and bandwidth."
                        "".format(os.path.basename(self.vis)))
            return None

        # Initialize the metric, get spwids.
        metric = np.zeros([len(self.science_spws)])
        spw_ids = np.array([spw.id for spw in self.science_spws])

        # For each spw, calculate metric: sqrt(bandwidth) / median(Tsys), if
        # a valid median Tsys is available.
        for ispw, spw in enumerate(self.science_spws):
            if np.isfinite(median_tsys[spw.id]):
                metric[ispw] = float(np.sqrt(spw.bandwidth.to_units(measures.FrequencyUnits.MEGAHERTZ))) / \
                               median_tsys[spw.id]

        # Sort spws by highest to lowest metric; convert to list of strings.
        result = spw_ids[np.argsort(metric)[::-1]]
        result = list(map(str, result))

        for ispw, spw_id in enumerate(spw_ids):
            LOG.info('spw: %s => spw score metric: %s' % (spw_id, metric[ispw]))
        LOG.info('spw rank: %s' % result)

        return result
