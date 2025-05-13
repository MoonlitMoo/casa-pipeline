import math
import os
import collections
from typing import Union, List, Dict, Sequence, Optional

import numpy as np

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import uvrange
from pipeline.infrastructure.tablereader import find_EVLA_band
from pipeline.hifv.heuristics import standard as standard
from pipeline.hifv.tasks.setmodel.vlasetjy import standard_sources
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.domain.measures import FrequencyUnits

LOG = infrastructure.get_logger(__name__)


class FluxbootInputs(vdp.StandardInputs):
    """Fluxboot Inputs class used with Fluxboot and Fluxboot Results.

    The class inherits from vdp.StandardInputs.

    If a caltable is specified, then the fluxgains stage from the scripted pipeline is skipped
    and we proceed directly to the flux density bootstrapping.
    """
    caltable = vdp.VisDependentProperty(default=None)
    refantignore = vdp.VisDependentProperty(default='')
    fitorder = vdp.VisDependentProperty(default=-1)
    refant = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, caltable=None, refantignore=None, fitorder=None, refant=None):
        """
        Args:
            vis(str or list):  measurement set
            caltable(str):  fluxgaincal table from user input.  If None, task uses default name.
                If a caltable is specified, then the fluxgains stage from the scripted pipeline is skipped
                and we proceed directly to the flux density bootstrapping.
            refantignore(str):  csv string of referance antennas to ignore   'ea24, ea18, ea12'
            fitorder(int):  User input value of the fit order.  Default is -1 (heuristics will determine)
            refant(str): A csv string of reference antenna(s). When used, disables refantignore.
        """

        if fitorder is None:
            fitorder = -1

        super(FluxbootInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.caltable = caltable
        self.refantignore = refantignore
        self.fitorder = fitorder
        self.spix = 0.0
        self.refant = refant


class FluxbootResults(basetask.Results):
    """Fluxboot Results class used with Fluxboot and Fluxboot Inputs.

    The class inherits from basetask.Results

    """
    def __init__(self, final=None, pool=None, preceding=None, sources=None,
                 flux_densities=None, spws=None, weblog_results=None, spindex_results=None,
                 vis=None, caltable=None, fluxscale_result=None):

        """
        Args:
                final(list): final list of tables (not used in this task)
                pool(list): pool list (not used in this task)
                preceding(list): preceding list (not used in this task)
                sources(list):  list of string source names
                flux_densities(list):  list of float value flux densities
                spws(list): list of integer spw values
                weblog_results(list):  list of dictionaries containing keys: source, freq, data, error, and fitted data
                spindex_results(list): list of dictionaries with fitting results for weblog display
                vis(str or list):  measurement set
                caltable(str):  fluxgaincal table that was used (from user input or task determination)
                fluxscale_result(list):  Output from CASA task fluxscale
        """

        if sources is None:
            sources = []
        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if flux_densities is None:
            flux_densities = []
        if spws is None:
            spws = []
        if weblog_results is None:
            weblog_results = []
        if spindex_results is None:
            spindex_results = []
        if caltable is None:
            caltable = ''
        if fluxscale_result is None:
            fluxscale_result = []

        super(FluxbootResults, self).__init__()
        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.sources = sources
        self.flux_densities = flux_densities
        self.spws = spws
        self.weblog_results = weblog_results
        self.spindex_results = spindex_results
        self.caltable = caltable
        self.fluxscale_result = fluxscale_result
        self.fbversion = 'fb'

    def merge_with_context(self, context):
        """Add results to context for later use in the final calibration
        """
        m = context.observing_run.measurement_sets[0]
        context.evla['msinfo'][m.name].fluxscale_sources = self.sources
        context.evla['msinfo'][m.name].fluxscale_flux_densities = self.flux_densities
        context.evla['msinfo'][m.name].fluxscale_spws = self.spws
        context.evla['msinfo'][m.name].fluxscale_result = self.fluxscale_result
        context.evla['msinfo'][m.name].fbversion = self.fbversion


@task_registry.set_equivalent_casa_task('hifv_fluxboot')
class Fluxboot(basetask.StandardTaskTemplate):
    Inputs = FluxbootInputs

    def prepare(self):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = m.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        sources, flux_densities, spws, weblog_results,\
        spindex_results, caltable, fluxscale_result = self._do_fluxboot(band2spw)

        return FluxbootResults(sources=sources, flux_densities=flux_densities, spws=spws,
                               weblog_results=weblog_results,
                               spindex_results=spindex_results, vis=self.inputs.vis, caltable=caltable,
                               fluxscale_result=fluxscale_result)

    def analyse(self, results):
        return results

    def _do_fluxboot(self, band2spw):

        calMs = 'calibrators.ms'

        self.sources = []
        self.flux_densities = []
        self.spws = []

        # Is this a VLASS execution?
        vlassmode = False
        for result in self.inputs.context.results:
            try:
                resultinputs = result.read()[0].inputs
                if 'vlass' in resultinputs['checkflagmode']:
                    vlassmode = True
            except:
                continue
        try:
            self.setjy_results = self.inputs.context.results[0].read()[0].setjy_results
        except Exception as e:
            self.setjy_results = self.inputs.context.results[0].read().setjy_results

        if self.inputs.caltable is None:
            # Original Fluxgain stage from the scripted pipeline

            caltable = 'fluxgaincal.g'

            LOG.info("Setting models for standard primary calibrators")

            standard_source_names, standard_source_fields = standard_sources(calMs)

            m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
            field_spws = m.get_vla_field_spws()
            spw2band = m.get_vla_spw2band()

            # Look in spectral window domain object as this information already exists!
            with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
                spw_bandwidths = table.getcol('TOTAL_BANDWIDTH')
                reference_frequencies = table.getcol('REF_FREQUENCY')

            center_frequencies = [rf + spwbw / 2 for rf, spwbw in zip(reference_frequencies, spw_bandwidths)]

            for i, fields in enumerate(standard_source_fields):
                for myfield in fields:
                    domainfield = m.get_fields(myfield)[0]
                    if 'AMPLITUDE' in domainfield.intents:
                        jobs = []
                        VLAspws = field_spws[myfield]
                        strlistVLAspws = ','.join(str(spw) for spw in VLAspws)
                        spws = [spw for spw in m.get_spectral_windows(strlistVLAspws)]

                        for spw in spws:
                            reference_frequency = center_frequencies[spw.id]
                            try:
                                EVLA_band = spw2band[spw.id]
                            except Exception as e:
                                LOG.info('Unable to get band from spw id - using reference frequency instead')
                                EVLA_band = find_EVLA_band(reference_frequency)

                            LOG.info("Center freq for spw " + str(spw.id) + " = " + str(reference_frequency)
                                     + ", observing band = " + EVLA_band)

                            model_image = standard_source_names[i] + '_' + EVLA_band + '.im'

                            LOG.info("Setting model for field " + str(m.get_fields()[myfield].id) + " spw " + str(
                                spw.id) + " using " + model_image)

                            try:
                                # fluxdensity=-1 should not matter since the model image take precedence
                                job = self._fluxgains_setjy(calMs, str(m.get_fields()[myfield].id), str(spw.id),
                                                            model_image)
                                jobs.append(job)
                            except Exception as e:
                                # something has gone wrong, return an empty result
                                LOG.error('Unable merge setjy jobs for flux scaling operation for field '
                                          + str(myfield) + ', spw ' + str(spw.id))
                                LOG.exception(e)

                        LOG.info("Merging flux scaling operation for setjy jobs for " + self.inputs.vis)
                        jobs_and_components = utils.merge_jobs(jobs, casa_tasks.setjy, merge=('spw',))
                        for job, _ in jobs_and_components:
                            try:
                                self._executor.execute(job)
                            except Exception as e:
                                LOG.warning("SetJy issue with field id=" + str(job.kw['field']) + " and spw="
                                            + str(job.kw['spw']))

            self.ignorerefant = self.inputs.context.evla['msinfo'][m.name].ignorerefant

            # PIPE-1637: adding ',' in the manual and auto refantignore parameter
            refantignore = self.inputs.refantignore + ','.join(['', *self.ignorerefant])

            refantfield = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string
            # PIPE-595: if refant list is not provided, compute refants else use provided refant list.
            if len(self.inputs.refant) == 0:
                refantobj = findrefant.RefAntHeuristics(vis=calMs, field=refantfield,
                                                        geometry=True, flagging=True, intent='',
                                                        spw='', refantignore=refantignore)

                RefAntOutput = refantobj.calculate()
            else:
                RefAntOutput = self.inputs.refant.split(",")

            refAnt = ','.join(RefAntOutput)

            LOG.info("The pipeline will use antenna(s) " + refAnt + " as the reference")

            fluxphase = 'fluxphaseshortgaincal.g'

            for band, spwlist in band2spw.items():
                append = False
                isdir = os.path.isdir(fluxphase)
                if isdir:
                    append = True
                    LOG.info("Appending to existing table: {!s}".format(fluxphase))

                new_gain_solint1 = self.inputs.context.evla['msinfo'][m.name].new_gain_solint1[band]

                LOG.info("Making gain tables for flux density bootstrapping")
                LOG.info("Short solint = " + new_gain_solint1 + " for band {!s}".format(band))

                self._do_gaincal(calMs, fluxphase, 'p', [''],
                                 solint=new_gain_solint1, minsnr=3.0, refAnt=refAnt,
                                 spw=','.join(spwlist), append=append)

            # ----------------------------------------------------------------------------
            # New Heuristics, CAS-9186
            field_objects = m.get_fields(intent=['AMPLITUDE', 'BANDPASS', 'PHASE'])

            # run gaincal with solnorm=True per calibrator field, pre-applying
            # short-solint phase solution and setting append=True for all fields
            # after the first, to obtain (temporary) scan-averaged, normalized
            # amps for flagging, fluxflag.g
            fluxflagtable = 'fluxflag.g'

            for i, field in enumerate(field_objects):
                for band, spwlist in band2spw.items():
                    calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

                    scanlist = [int(scan) for scan in calibrator_scan_select_string.split(',')]
                    scanids_perband = ','.join([str(scan.id) for scan in m.get_scans(scan_id=scanlist, spw=','.join(spwlist))])

                    calscanslist = list(map(int, scanids_perband.split(',')))
                    scanobjlist = m.get_scans(scan_id=calscanslist,
                                              scan_intent=['AMPLITUDE', 'BANDPASS', 'PHASE'])
                    fieldidlist = []
                    for scanobj in scanobjlist:
                        fieldobj, = scanobj.fields
                        if str(fieldobj.id) not in fieldidlist:
                            fieldidlist.append(str(fieldobj.id))

                    if str(field.id) in fieldidlist:
                        append = False
                        isdir = os.path.isdir(fluxflagtable)
                        if isdir:
                            append = True
                            LOG.info("Appending to existing table: {!s}".format(fluxflagtable))

                        gain_solint2 = self.inputs.context.evla['msinfo'][m.name].gain_solint2[band]
                        LOG.info("Long solint = " + gain_solint2 + " for band {!s}".format(band))

                        self._do_gaincal(calMs, fluxflagtable, 'ap', [fluxphase],
                                         solint=gain_solint2, minsnr=5.0, refAnt=refAnt, field=field.name,
                                         solnorm=True, append=append, fluxflag=True,
                                         vlassmode=vlassmode, spw=','.join(spwlist))

            # use flagdata to clip fluxflag.g outside the range 0.9-1.1
            flagjob = casa_tasks.flagdata(vis=fluxflagtable, mode='clip', correlation='ABS_ALL',
                                          datacolumn='CPARAM', clipminmax=[0.9, 1.1], clipoutside=True,
                                          action='apply', flagbackup=False, savepars=False)
            self._executor.execute(flagjob)

            # use applycal to apply fluxflag.g to calibrators_band.ms, applymode='flagonlystrict'
            applycaljob = casa_tasks.applycal(vis=calMs, field="", spw="", intent="",
                                              selectdata=False, docallib=False, gaintable=[fluxflagtable],
                                              gainfield=[''], interp=[''], spwmap=[], calwt=[False], parang=False,
                                              applymode='flagonlystrict', flagbackup=True)

            self._executor.execute(applycaljob)

            # -------------------------------------------------------------------------------

            for band, spwlist in band2spw.items():
                append = False
                isdir = os.path.isdir(caltable)
                if isdir:
                    append = True
                    LOG.info("Appending to existing table: {!s}".format(caltable))

                gain_solint2 = self.inputs.context.evla['msinfo'][m.name].gain_solint2[band]

                self._do_gaincal(calMs, caltable, 'ap', [fluxphase],
                                 solint=gain_solint2, minsnr=5.0, refAnt=refAnt, append=append, spw=','.join(spwlist))

            LOG.info("Gain table " + caltable + " is ready for flagging.")
        else:
            caltable = self.inputs.caltable
            LOG.warning("Caltable " + caltable + "has been flagged and will be used in the flux density bootstrapping.")

        # ---------------------------------------------------------------------
        # Fluxboot stage

        LOG.info("Doing flux density bootstrapping using caltable {!s}.".format(caltable))
        try:
            # The fluxscale_result is a list
            powerfit_results = []
            weblog_results = []
            spindex_results = []
            fluxscale_result = []
            fluxscale_result_list = self._do_fluxscale(calMs, caltable)
            LOG.info("Using fit from fluxscale.")
            for single_fs_result in fluxscale_result_list:
                powerfit_results_single, weblog_results_single, spindex_results_single, single_fs_result = self._do_powerfit(single_fs_result)
                weblog_results.extend(weblog_results_single)
                spindex_results.extend(spindex_results_single)
                fluxscale_result.append(single_fs_result)
                setjy_result = self._do_setjy(calMs, single_fs_result)
        except Exception as e:
            LOG.warning(str(e))
            LOG.warning("A problem was detected while running fluxscale.  Please review the CASA log.")
            powerfit_results = []
            weblog_results = []
            spindex_results = []
            fluxscale_result = []

        return self.sources, self.flux_densities, self.spws, weblog_results,\
               spindex_results, caltable, fluxscale_result

    def _do_fluxscale(self, calMs: str, caltable: str) -> List:
        """Set up and execute the CASA task fluxscale

        Args:
            calMs(str): hardwired for this task to be calibrators.ms
            caltable(str): fluxgaincal table that was used (from user input or task determination)

        Return:
            fluxscale_result(list):  Result from the CASA task fluxscale

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        flux_field_select_string = self.inputs.context.evla['msinfo'][m.name].flux_field_select_string
        fluxcalfields = flux_field_select_string
        fluxcalfieldlist = str.split(fluxcalfields, ',')

        if len(fluxcalfieldlist) > 1:
            fieldmsg = ''
            for fluxcalfield in fluxcalfieldlist:
                fieldobj = m.get_fields(field_id=int(fluxcalfield))
                fieldmsg += "{!s}: {!s}, ".format(str(fieldobj[0].id), fieldobj[0].name)
            LOG.warning("Fields {!s} have CALIBRATE_FLUX intents. All will be used for flux calibration, "
                        "this may not be desired.".format(fieldmsg))

        calibrator_field_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string
        calfieldliststrings = str.split(calibrator_field_select_string, ',')
        calfieldlist = []
        for field in calfieldliststrings:
            fieldobj = m.get_fields(field_id=int(field))
            nfldobj = len(fieldobj[0].intents)
            if (nfldobj == 1 and 'POINTING' in fieldobj[0].intents) or \
               (nfldobj == 1 and 'SYSTEM_CONFIGURATION' in fieldobj[0].intents) or \
               (nfldobj == 1 and 'UNSPECIFIED#UNSPECIFIED' in fieldobj[0].intents) or \
               (nfldobj == 2 and 'POINTING' in fieldobj[0].intents and 'SYSTEM_CONFIGURATION' in fieldobj[0].intents) or \
               (nfldobj == 2 and 'POINTING' in fieldobj[0].intents and 'UNSPECIFIED#UNSPECIFIED' in fieldobj[0].intents) or \
               (nfldobj == 2 and 'SYSTEM_CONFIGURATION' in fieldobj[0].intents and 'UNSPECIFIED#UNSPECIFIED' in fieldobj[0].intents) or \
               (nfldobj == 3 and 'POINTING' in fieldobj[0].intents and 'SYSTEM_CONFIGURATION' in fieldobj[0].intents and 'UNSPECIFIED#UNSPECIFIED' in fieldobj[0].intents) or \
               (nfldobj > 1 and 'POINTING' in fieldobj[0].intents and 'TARGET' in fieldobj[0].intents):

                LOG.warning("Field {!s}: {!s}, "
                            "has intents {!s}. Due to POINTING/SYS_CONFIG intents, "
                            "it is not used in the "
                            "fluxscale() transfer keyword.".format(field, fieldobj[0].name, fieldobj[0].intents))
            else:
                calfieldlist.append(field)

        fluxscale_result = []

        # Filter down to only science spectral windows
        scispws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]

        for field in calfieldlist:
            fitorder = self.inputs.fitorder
            spwlist = []

            for scan in m.get_scans(field=field):
                for spw in list(scan.spws):
                    spwlist.append(spw.id)

            spwlist = list(np.unique(spwlist))
            spwlist.sort()
            spwlist = [str(spwid) for spwid in spwlist if spwid in scispws]

            if self.inputs.fitorder == -1 and field not in fluxcalfieldlist:
                fitorder = self.find_fitorder(spwlist)
            elif self.inputs.fitorder > -1:
                LOG.info("Keyword override:  Using input fitorder={!s}".format(fitorder))
            elif self.inputs.fitorder < -1:
                raise Exception

            if (field not in fluxcalfieldlist) and spwlist:
                task_args = {'vis': calMs,
                             'caltable': caltable,
                             'fluxtable': 'fluxgaincalFcal_{!s}.g'.format(field),
                             'reference': [fluxcalfields],
                             'transfer': [field],
                             'append': False,
                             'refspwmap': [-1],
                             'fitorder': fitorder}

                job = casa_tasks.fluxscale(**task_args)
                fs_result = self._executor.execute(job)
                fluxscale_result.append(fs_result)

        return fluxscale_result

    def find_fitorder(self, spwlist: List[str] = []) -> int:
        """Determine the fitorder for a given list of spectral windows

        Args:
            spwlist(list): list of string values for spw ids

        Return:
            fitorder(int): integer value of the determined fitorder for a given band (list of spws)
        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2bandall = m.get_vla_spw2band()
        if spwlist == []:
            spws = m.get_spectral_windows()   # List is done per band
            spwidlist = [spw.id for spw in spws if 'AMPLITUDE' in spw.intents]
        else:
            spws = m.get_spectral_windows(task_arg=','.join(spwlist))
            spwidlist = [int(spw) for spw in spwlist]

        spw2band = {}
        for key, value in spw2bandall.items():
            if key in spwidlist:
                spw2band[key] = value
        bands = list(spw2band.values())

        minfreq = min([spw.min_frequency for spw in spws])
        maxfreq = max([spw.max_frequency for spw in spws])
        deltaf = maxfreq - minfreq
        centerfreq = (maxfreq + minfreq) / 2.0
        fractional_bandwidth = deltaf / centerfreq

        unique_bands = list(np.unique(bands))

        lower_bands = '4PLSCXU'

        # Single band observation first
        if len(unique_bands) == 1:
            if unique_bands[0] in 'KAQ':
                fitorder = 1
            if unique_bands[0] in lower_bands:
                if fractional_bandwidth > 1.6:
                    fitorder = 4
                elif 0.8 <= fractional_bandwidth < 1.6:
                    fitorder = 3
                elif 0.3 <= fractional_bandwidth < 0.8:
                    fitorder = 2
                elif fractional_bandwidth < 0.3:
                    fitorder = 1
        elif len(unique_bands) == 2 and 'A' in unique_bands and 'Q' in unique_bands:
            fitorder = 1
        elif ((len(unique_bands) > 2) or
              (len(unique_bands) == 2 and (unique_bands[0] in lower_bands or unique_bands[1] in lower_bands))):
            # PIPE-1758: lower dnu/nu = 1.5 for 4th order fit
            if fractional_bandwidth >= 1.5:
                fitorder = 4
            elif 0.8 <= fractional_bandwidth < 1.5:
                fitorder = 3
            elif 0.4 <= fractional_bandwidth < 0.8:
                fitorder = 2
            elif fractional_bandwidth < 0.4:
                fitorder = 1
        else:
            fitorder = 1
            LOG.warning('Heuristics could not determine a fitorder for fluxscale.  Defaulting to fitorder=1.')

        # PIPE-1603, add fluxboot heuristics to use fitorder=0
        mhz_deltaf = deltaf.to_units(FrequencyUnits.MEGAHERTZ)
        if  mhz_deltaf < 257:
            fitorder = 0

        LOG.info('Displaying fit order heuristics...')
        LOG.info('  Number of spws: {!s}'.format(str(len(spws))))
        LOG.info('  Band: {!s}'.format(','.join(unique_bands)))
        LOG.info('  Min frequency: {!s}'.format(str(minfreq)))
        LOG.info('  Max frequency: {!s}'.format(str(maxfreq)))
        LOG.info('  delta nu / nu: {!s}'.format(fractional_bandwidth))
        LOG.info('  Fit order: {!s}'.format(fitorder))

        return fitorder

    def _do_powerfit(self, fluxscale_result: List):
        """Organize the fitting results from fluxscale, re-reference the fit coefficients, and prepare
            dictionaries for weblog display

        Args:
            fluxscale_result(list):  Dictionary outpput from CASA task fluxscale

        Return:
            results(list):  List of lists containing [source, uspws, fitflx, spix, SNR, reffreq, curvature]
                This parameter dates from the original scripted pipeline.
            weblog_results(list):  list of dictionaries containing keys: source, freq, data, error, and fitted data
            spindex_results(list): list of dictionaries with fitting results for weblog display
            fluxscale_result(list):  Output from CASA task fluxscale

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        bands = list(spw2band.values())

        # Look in spectral window domain object as this information already exists!
        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            spw_bandwidths = table.getcol('TOTAL_BANDWIDTH')
            reference_frequencies = table.getcol('REF_FREQUENCY')

        center_frequencies = [rf + spwbw / 2 for rf, spwbw in zip(reference_frequencies, spw_bandwidths)]

        # the variable center_frequencies should already have been filled out
        # with the reference frequencies of the spectral window table

        sources = []
        flux_densities = []
        spws = []

        # Find the field_ids in the dictionary returned from the CASA task fluxscale
        dictkeys = list(fluxscale_result.keys())
        keys_to_remove = ['freq', 'spwName', 'spwID']
        dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

        for field_id in dictkeys:
            sourcename = fluxscale_result[field_id]['fieldName']
            secondary_keys = list(fluxscale_result[field_id].keys())
            secondary_keys_to_remove = ['fitRefFreq', 'spidxerr', 'spidx', 'fitFluxd', 'fieldName',
                                        'fitFluxdErr', 'covarMat']
            spwkeys = [int(spw_id) for spw_id in secondary_keys if spw_id not in secondary_keys_to_remove]

            # fluxscale results give **ALL** spectral windows
            # Take the intersection of the domain object spws and fluxscale results to
            # match the earlier setjy execution in this task

            scispws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]
            newspwkeys = [str(spwint) for spwint in list(set(scispws) & set(spwkeys))]

            for spw_id in newspwkeys:
                flux_d = list(fluxscale_result[field_id][spw_id]['fluxd'])
                flux_d_err = list(fluxscale_result[field_id][spw_id]['fluxdErr'])

                for i in range(0, len(flux_d)):
                    if flux_d[i] != -1.0 and flux_d[i] != 0.0:
                        sources.append(sourcename)
                        flux_densities.append([float(flux_d[i]), float(flux_d_err[i])])
                        spws.append(int(spw_id))

        self.sources = sources
        self.flux_densities = flux_densities
        self.spws = spws

        unique_sources = list(np.unique(sources))
        results = []
        weblog_results = []
        spindex_results = []

        for source in unique_sources:
            indices = []
            for ii in range(len(sources)):
                if sources[ii] == source:
                    indices.append(ii)

            bands_from_spw = []

            if bands == []:
                for ii in range(len(indices)):
                    bands.append(find_EVLA_band(center_frequencies[spws[indices[ii]]]))
            else:
                for ii in range(len(indices)):
                    bands_from_spw.append(spw2band[spws[indices[ii]]])
                bands = bands_from_spw

            unique_bands = list(np.unique(bands))

            fieldobject = m.get_fields(source)
            fieldid = str([str(f.id) for f in fieldobject if str(f.id) in fluxscale_result.keys()][0])

            for band in unique_bands:
                lfreqs = []
                lfds = []
                lerrs = []
                uspws = []

                # Use spw id to band mappings if available
                if list(spw2band.values()):
                    for ii in range(len(indices)):
                        if spw2band[spws[indices[ii]]] == band:
                            lfreqs.append(math.log10(center_frequencies[spws[indices[ii]]]))
                            lfds.append(math.log10(flux_densities[indices[ii]][0]))
                            lerrs.append((flux_densities[indices[ii]][1]) / (flux_densities[indices[ii]][0]) / np.log(10.0))
                            uspws.append(spws[indices[ii]])

                # Use frequencies for band mappings if no spwid-to-band mapping is available
                if not list(spw2band.values()):
                    for ii in range(len(indices)):
                        if find_EVLA_band(center_frequencies[spws[indices[ii]]]) == band:
                            lfreqs.append(math.log10(center_frequencies[spws[indices[ii]]]))
                            lfds.append(math.log10(flux_densities[indices[ii]][0]))
                            lerrs.append((flux_densities[indices[ii]][1]) / (flux_densities[indices[ii]][0]) / np.log(10.0))
                            uspws.append(spws[indices[ii]])

                freqs = fluxscale_result['freq']
                fitflx = fluxscale_result[fieldid]['fitFluxd']   # Fiducial flux for entire fit
                fitflxAtRefFreq = fluxscale_result[fieldid]['fitFluxd']
                fitflxAtRefFreqErr = fluxscale_result[fieldid]['fitFluxdErr']
                fitreff = fluxscale_result[fieldid]['fitRefFreq']
                spidx = fluxscale_result[fieldid]['spidx']
                reffreq = fitreff / 1.e9

                if len(spidx) > 1:
                    spix = fluxscale_result[fieldid]['spidx'][1]
                    spixerr = fluxscale_result[fieldid]['spidxerr'][1]
                else:
                    # Fit order = 0
                    spix = 0.0
                    spixerr = 0.0
                SNR = 0.0
                curvature = 0.0
                curvatureerr = 0.0
                gamma = 0.0
                gammaerr = 0.0
                delta = 0.0
                deltaerr = 0.0

                lerrs = [x for y, x in sorted(zip(freqs[uspws], lerrs))]
                lfds = [x for y, x in sorted(zip(freqs[uspws], lfds))]

                freqs = np.array(sorted(freqs[uspws]))

                logfittedfluxd = np.zeros(len(freqs))
                for i in range(len(spidx)):
                    logfittedfluxd += spidx[i] * (np.log10(freqs/fitreff)) ** i

                fittedfluxd = 10.0 ** logfittedfluxd

                # Single spectral window
                if len(logfittedfluxd) == 1:
                    fittedfluxd = np.array([fitflx])

                # For this band determine a fiducial flux
                bandfreqs = 10.0 ** np.array(lfreqs)
                bandcenterfreq = (np.min(bandfreqs) + np.max(bandfreqs)) / 2.0
                logfiducialflux = 0.0
                for i in range(len(spidx)):
                    logfiducialflux += spidx[i] * (np.log10(bandcenterfreq/fitreff)) ** i

                fitflx = fluxscale_result[fieldid]['fitFluxd']
                fitflxerr = fitflxAtRefFreqErr

                # Again, single spectral window
                if len(logfittedfluxd) == 1:
                    fitflx = np.array([fluxscale_result[fieldid]['fitFluxd']])

                fitorderused = len(spidx) - 1
                if fitorderused > 1:
                    curvature = fluxscale_result[fieldid]['spidx'][2]
                    curvatureerr = fluxscale_result[fieldid]['spidxerr'][2]

                if fitorderused > 2:
                    gamma = fluxscale_result[fieldid]['spidx'][3]
                    gammaerr = fluxscale_result[fieldid]['spidxerr'][3]

                if fitorderused > 3:
                    delta = fluxscale_result[fieldid]['spidx'][4]
                    deltaerr = fluxscale_result[fieldid]['spidxerr'][4]

                # ------------------------------------------------------------------------
                # Re-calculating a new reference frequency for a power-law SED
                if len(logfittedfluxd) > 1 and fitorderused != 0:
                    coef = [fitflx, spix, curvature, gamma, delta]
                    coef_errors = [fitflxerr, spixerr, curvatureerr, gammaerr, deltaerr]

                    ref_freq = reffreq

                    # first coefficient is log S
                    coef[0] = np.log10(coef[0])

                    # first coefficient error in log flux
                    coef_errors[0] = np.log10(10 ** coef[0] + coef_errors[0]) - coef[0]

                    print('\nOriginal coefficients for band {0}:'.format(band))
                    for j in range(len(coef)):
                        if j == 0:
                            print('  {0:.4f} +/- {1:.4f}'.format(10 ** coef[j],
                                                                 10 ** (coef[j] + coef_errors[j]) - 10 ** (
                                                                     coef[j])))
                        else:
                            print('  {0:.4f} +/- {1:.4f}'.format(coef[j], coef_errors[j]))
                    print('------------------------------------------------------')
                    print('Old reffreq: {0:.6f}      New reffreq: {1:.6f}'.format(ref_freq, bandcenterfreq/1.e9))
                    new_coef = self.re_reference_polynomial(coef, ref_freq, bandcenterfreq/1.e9)

                    # bootstrap new coefficient errors
                    new_bootstrap_coeffs = []
                    ntrials = 2000
                    for j in range(ntrials):
                        noisy_coef = np.array(coef) + np.random.randn(len(coef)) * np.array(coef_errors)
                        new_bootstrap_coef = self.re_reference_polynomial(noisy_coef, ref_freq, bandcenterfreq/1.e9)
                        new_bootstrap_coeffs.append(new_bootstrap_coef)

                    bootstrapped_coef_errors = np.std(np.array(new_bootstrap_coeffs), axis=0)

                    print('\nNew coefficients for band {0}:'.format(band))
                    print('\nThese new values will be used in subsequent setjy executions.')
                    for j in range(len(new_coef)):
                        if j == 0:
                            print('  {0:.4f} +/- {1:.4f}'.format(10 ** new_coef[j],
                                                                 10 ** (new_coef[j] + bootstrapped_coef_errors[j]) - 10 ** (
                                                                 new_coef[j])))
                        else:
                            print('  {0:.4f} +/- {1:.4f}'.format(new_coef[j], bootstrapped_coef_errors[j]))

                    # Replace old values with new values for weblog presentation
                    if fitorderused > 0:
                        spix = new_coef[1]
                        spixerr = bootstrapped_coef_errors[1]

                    if fitorderused > 1:
                        curvature = new_coef[2]
                        curvatureerr = bootstrapped_coef_errors[2]

                    if fitorderused > 2:
                        gamma = new_coef[3]
                        gammaerr = bootstrapped_coef_errors[3]

                    if fitorderused > 3:
                        delta = new_coef[4]
                        deltaerr = bootstrapped_coef_errors[4]

                    fitflx = 10 ** new_coef[0]
                    fitflxerr = 10 ** (new_coef[0] + bootstrapped_coef_errors[0]) - 10 ** (new_coef[0])

                # ------------------------------------------------------------------------

                LOG.info(' Source: ' + source +
                         ' Band: ' + band +
                         ' fluxscale fitted spectral index = ' + str(spix) + ' +/- ' + str(spixerr))

                if fitorderused > 1:
                    LOG.info(' Source: ' + source +
                             ' Band: ' + band +
                             ' fluxscale fitted 2nd order coeff = ' + str(curvature) + ' +/- ' + str(curvatureerr))

                if fitorderused > 2:
                    LOG.info(' Source: ' + source +
                             ' Band: ' + band +
                             ' fluxscale fitted 3rd order coeff = ' + str(gamma) + ' +/- ' + str(gammaerr))

                if fitorderused > 3:
                    LOG.info(' Source: ' + source +
                             ' Band: ' + band +
                             ' fluxscale fitted 4th order coeff = ' + str(delta) + ' +/- ' + str(deltaerr))

                results.append([source, uspws, fitflx, spix, SNR, reffreq, curvature])

                spindex_results.append({'source': source,
                                        'band': band,
                                        'bandcenterfreq': bandcenterfreq,
                                        'sortingfreq': bandcenterfreq,
                                        'spix': str(spix),
                                        'spixerr': str(spixerr),
                                        'SNR': SNR,
                                        'fitflx': fitflx,
                                        'fitflxerr': fitflxerr,
                                        'curvature': str(curvature),
                                        'curvatureerr': str(curvatureerr),
                                        'gamma': str(gamma),
                                        'gammaerr': str(gammaerr),
                                        'delta': str(delta),
                                        'deltaerr': str(deltaerr),
                                        'fitorder': str(fitorderused),
                                        'reffreq': str(reffreq),
                                        'fitflxAtRefFreq': str(fitflxAtRefFreq),
                                        'fitflxAtRefFreqErr': str(fitflxAtRefFreqErr)})

                LOG.info("Frequency, data, error, and fitted data:")

                for ii in range(len(freqs)):
                    SS = fittedfluxd[ii]
                    freq = freqs[ii]/1.e9
                    data = 10.0 ** lfds[ii]
                    fderr = lerrs[ii] * (10 ** lfds[ii]) / np.log10(np.e)

                    LOG.info('    ' + str(freq) + '  ' + str(data) + '  ' + str(fderr) + '  ' + str(SS))
                    weblog_results.append({'source': source,
                                           'freq': str(freq),
                                           'data': str(data),
                                           'error': str(fderr),
                                           'fitteddata': str(SS)})

            # If multiple bands, then add another a special entry with the global fit
            fitflxAtRefFreq = fluxscale_result[fieldid]['fitFluxd']
            fitflxAtRefFreqErr = fluxscale_result[fieldid]['fitFluxdErr']
            fitreff = fluxscale_result[fieldid]['fitRefFreq']
            spidx = fluxscale_result[fieldid]['spidx']
            reffreq = fitreff / 1.e9

            if len(spidx) > 1:
                spix = fluxscale_result[fieldid]['spidx'][1]
                spixerr = fluxscale_result[fieldid]['spidxerr'][1]
            else:
                # Fit order = 0
                spix = 0.0
                spixerr = 0.0
            SNR = 0.0
            curvature = 0.0
            curvatureerr = 0.0
            gamma = 0.0
            gammaerr = 0.0
            delta = 0.0
            deltaerr = 0.0

            fitorderused = len(spidx) - 1
            if fitorderused > 1:
                curvature = fluxscale_result[fieldid]['spidx'][2]
                curvatureerr = fluxscale_result[fieldid]['spidxerr'][2]

            if fitorderused > 2:
                gamma = fluxscale_result[fieldid]['spidx'][3]
                gammaerr = fluxscale_result[fieldid]['spidxerr'][3]

            if fitorderused > 3:
                delta = fluxscale_result[fieldid]['spidx'][4]
                deltaerr = fluxscale_result[fieldid]['spidxerr'][4]

            if len(unique_bands) > 1:
                spindex_results.append({'source': source,
                                        'band': 'Multi',
                                        'bandcenterfreq': fluxscale_result[fieldid]['fitRefFreq'],
                                        'sortingfreq': 0.0,
                                        'spix': str(spix),
                                        'spixerr': str(spixerr),
                                        'SNR': SNR,
                                        'fitflx': fitflxAtRefFreq,
                                        'fitflxerr': fitflxAtRefFreqErr,
                                        'curvature': str(curvature),
                                        'curvatureerr': str(curvatureerr),
                                        'gamma': str(gamma),
                                        'gammaerr': str(gammaerr),
                                        'delta': str(delta),
                                        'deltaerr': str(deltaerr),
                                        'fitorder': str(fitorderused),
                                        'reffreq': str(reffreq),
                                        'fitflxAtRefFreq': str(fitflxAtRefFreq),
                                        'fitflxAtRefFreqErr': str(fitflxAtRefFreqErr)})

        self.spix = spix
        self.curvature = curvature

        LOG.info("Setting fluxscale fit in the model column.")

        # Sort weblog results by frequency
        weblog_results = sorted(weblog_results, key=lambda k: (k['source'], k['freq']))

        return results, weblog_results, spindex_results, fluxscale_result

    def _do_setjy(self, calMs: str, fluxscale_result: List) -> bool:
        """Setup and execute setjy using the results from CASA task fluxscale

        Return:
            Boolean
        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        dictkeys = list(fluxscale_result.keys())
        keys_to_remove = ['freq', 'spwName', 'spwID']
        dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

        for fieldid in dictkeys:

            jobs_calMs = []
            jobs_vis = []

            spws = list(fluxscale_result['spwID'])
            scispws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]
            newspws = [str(spwint) for spwint in list(set(scispws) & set(spws))]

            LOG.info('Running setjy for field ' + str(fieldid) + ': ' + str(fluxscale_result[fieldid]['fieldName']))
            task_args = {'vis': calMs,
                         'field': fluxscale_result[fieldid]['fieldName'],
                         'spw': ','.join(newspws),
                         'selectdata': False,
                         'model': '',
                         'listmodels': False,
                         'scalebychan': True,
                         'fluxdensity': [fluxscale_result[fieldid]['fitFluxd'], 0, 0, 0],
                         'spix': list(fluxscale_result[fieldid]['spidx'][1:]),
                         'reffreq': str(fluxscale_result[fieldid]['fitRefFreq']) + 'Hz',
                         'standard': 'manual',
                         'usescratch': True}

            jobs_calMs.append(casa_tasks.setjy(**task_args))

            # Run on the ms
            task_args['vis'] = self.inputs.vis
            jobs_vis.append(casa_tasks.setjy(**task_args))

            if abs(self.spix) > 5.0:
                LOG.warning("abs(spix) > 5.0 - Fail")

            # merge identical jobs into one job with a multi-spw argument
            LOG.info("Merging setjy jobs for {!s}".format(calMs))
            jobs_and_components_calMs = utils.merge_jobs(jobs_calMs, casa_tasks.setjy, merge=('spw',))
            for job, _ in jobs_and_components_calMs:
                self._executor.execute(job)

            LOG.info("Merging setjy jobs for {!s}".format(self.inputs.vis))
            jobs_and_components_vis = utils.merge_jobs(jobs_vis, casa_tasks.setjy, merge=('spw',))
            for job, _ in jobs_and_components_vis:
                self._executor.execute(job)

        LOG.info("Flux density bootstrapping finished")

        return True

    def _fluxgains_setjy(self, calMs: str, field: str, spw: str, modimage: str):
        """Set up parameters setjy task on the fluxgains cal table

        Return:
            setjy task job object

        """

        try:
            task_args = {'vis': calMs,
                         'field': field,
                         'spw': spw,
                         'selectdata': False,
                         'model': modimage,
                         'listmodels': False,
                         'scalebychan': True,
                         'fluxdensity': -1,
                         'standard': standard.Standard()(field),
                         'usescratch': True}

            job = casa_tasks.setjy(**task_args)

            return job
        except Exception as e:
            LOG.info(e)
            return None

    def _do_gaincal(self, calMs: str, caltable: str, calmode: str, gaintablelist: List[str],
                    solint: str = 'int', minsnr: float = 3.0, refAnt: str = None, field: str = '',
                    solnorm: bool = False, append: bool = False,
                    fluxflag: bool = False, vlassmode: bool = False, spw: str = ''):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        minBL_for_cal = m.vla_minbaselineforcal()

        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        scanlist = [int(scan) for scan in calibrator_scan_select_string.split(',')]
        scanids_perband = ','.join([str(scan.id) for scan in m.get_scans(scan_id=scanlist, spw=spw)])

        task_args = {'vis': calMs,
                     'caltable': caltable,
                     'field': field,
                     'spw': spw,
                     'intent': '',
                     'selectdata': False,
                     'solint': solint,
                     'combine': 'scan',
                     'preavg': -1.0,
                     'refant': refAnt.lower(),
                     'minblperant': minBL_for_cal,
                     'minsnr': minsnr,
                     'solnorm': solnorm,
                     'gaintype': 'G',
                     'smodel': [],
                     'calmode': calmode,
                     'append': append,
                     'gaintable': gaintablelist,
                     'gainfield': [''],
                     'interp': [''],
                     'spwmap': [],
                     'uvrange': '',
                     'parang': True}

        if field == '':
            calscanslist = list(map(int, scanids_perband.split(',')))
            scanobjlist = m.get_scans(scan_id=calscanslist,
                                      scan_intent=['AMPLITUDE', 'BANDPASS', 'POLLEAKAGE', 'POLANGLE',
                                                   'PHASE', 'POLARIZATION', 'CHECK'])
            fieldidlist = []
            for scanobj in scanobjlist:
                fieldobj, = scanobj.fields
                if str(fieldobj.id) not in fieldidlist:
                    fieldidlist.append(str(fieldobj.id))

            for fieldidstring in fieldidlist:
                fieldid = int(fieldidstring)
                uvrangestring = uvrange(self.setjy_results, fieldid)
                task_args['field'] = fieldidstring
                task_args['uvrange'] = uvrangestring
                task_args['selectdata'] = True
                if os.path.exists(caltable):
                    task_args['append'] = True

                job = casa_tasks.gaincal(**task_args)
                self._executor.execute(job)

            return True
        elif fluxflag and vlassmode:
            fieldobjlist = m.get_fields(name=field)
            fieldidlist = []
            for fieldobj in fieldobjlist:
                if str(fieldobj.id) not in fieldidlist:
                    fieldidlist.append(str(fieldobj.id))

            for fieldidstring in fieldidlist:
                fieldid = int(fieldidstring)
                uvrangestring = uvrange(self.setjy_results, fieldid)
                task_args['field'] = fieldidstring
                task_args['uvrange'] = uvrangestring
                task_args['selectdata'] = True
                if os.path.exists(caltable):
                    task_args['append'] = True

                job = casa_tasks.gaincal(**task_args)

                self._executor.execute(job)

            return True
        else:
            job = casa_tasks.gaincal(**task_args)

            return self._executor.execute(job)

    def re_reference_polynomial(self, c1: List, original_ref_freq: float, new_ref_freq: float) -> List:
        """Re-reference polynomial

            Return:
                p2.coefficients(np.array):  re-referenced coefficients

        """

        shift = np.log10(new_ref_freq / original_ref_freq)
        p1 = np.poly1d(c1[::-1])
        r2 = np.roots(p1) - shift
        c2 = np.poly(r2)
        c2 = c2 * p1(shift) / c2[-1]
        # c2 = c2 * p1(shift) / np.asarray(c2)[-1]
        p2 = np.poly1d(c2)
        return p2.coefficients[::-1]
