import os
import collections
import shutil
from typing import TYPE_CHECKING, List, Optional, Tuple, Union, Any, Dict

import numpy as np

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import getCalFlaggedSoln
from pipeline.hifv.heuristics import weakbp, do_bandpass, uvrange
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.hifv.heuristics import getBCalStatistics


LOG = infrastructure.get_logger(__name__)


class testBPdcalsInputs(vdp.StandardInputs):
    """Inputs class for the hifv_testBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    weakbp = vdp.VisDependentProperty(default=False)
    refantignore = vdp.VisDependentProperty(default='')
    doflagundernspwlimit = vdp.VisDependentProperty(default=False)
    refant = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, weakbp=None, refantignore=None, doflagundernspwlimit=None, refant=None):
        """
        Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set
            weakbp(Boolean):  weak bandpass heuristics on/off - currently not used - see PIPE-104
            refantignore(str):  csv string of reference antennas to ignore - 'ea24,ea15,ea08'
            doflagunderspwlimit(Boolean): Will identify individual spw when less than nspwlimit bad spw
                                          Used in the flagging of bad deformatters heuristics
            refant(str): A csv string of reference antenna(s). When used, disables refantignore.
        """
        super(testBPdcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore
        self.doflagundernspwlimit = doflagundernspwlimit
        self.gain_solint1 = 'int'
        self.gain_solint2 = 'int'
        self.refant = refant


class testBPdcalsResults(basetask.Results):
    """Results class for the hifv_testBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.Results.

    """
    def __init__(self, final=None, pool=None, preceding=None, gain_solint1=None,
                 shortsol1=None, vis=None, bpdgain_touse=None, gtypecaltable=None,
                 ktypecaltable=None, bpcaltable=None, flaggedSolnApplycalbandpass=None,
                 flaggedSolnApplycaldelay=None, result_amp=None, result_phase=None,
                 amp_collection=None, phase_collection=None, num_antennas=None, ignorerefant=None):
        """
        Args:
            vis(str): String name of the measurement set
            final(List, optional): Calibration list applied - not used
            pool(List, optional): Calibration list assesed - not used
            preceding(List, optional): DEPRECATED results from worker tasks executed by this task
            gain_solint1(Dict):  Dict of csv strings, keyed by band
            shortsol1(Dict):  Integration time determined from heuristics (1,3,10 x max int time) keyed by band
            bpdgain_touse(Dict):  Dictionary of tables per band
            gtypecaltable(Dict): Dictionary of tables per band
            ktypecaltable(Dict): Dictionary of tables per band
            bpcaltable(Dict): Dictionary of tables per band
            flaggedSolnApplycalbandpass(Dict): returned from getCalFlaggedSoln for bpdgain_touse (per band)
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable (per band)
            result_amp(Dict):  Bad deformatters amp flagging list per band
            result_phase(Dict): Bad deformatters phase flagging list per band
            amp_collection(Dict):  Bad deformatters amp weblog table per band
            phase_collection(Dict): Bad deformatters phase weblog table per band
            num_antennas(Dict):  Number of antennas (same per band, but included for weblog formatting)
            ignorerefant(List):  List of antennas removed if a baseband is determined to be bad for >50% of antennas.

        """

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if amp_collection is None:
            amp_collection = collections.defaultdict(list)
        if phase_collection is None:
            phase_collection = collections.defaultdict(list)
        if result_amp is None:
            result_amp = []
        if result_phase is None:
            result_phase = []
        if ignorerefant is None:
            ignorerefant = []

        super(testBPdcalsResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.gain_solint1 = gain_solint1
        self.shortsol1 = shortsol1
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay
        self.ignorerefant = ignorerefant

        self.result_amp = result_amp
        self.result_phase = result_phase
        self.amp_collection = amp_collection
        self.phase_collection = phase_collection
        self.num_antennas = num_antennas

    def merge_with_context(self, context):
        m = context.observing_run.get_ms(self.vis)
        context.evla['msinfo'][m.name].gain_solint1 = self.gain_solint1
        context.evla['msinfo'][m.name].shortsol1 = self.shortsol1
        context.evla['msinfo'][m.name].ignorerefant = self.ignorerefant


@task_registry.set_equivalent_casa_task('hifv_testBPdcals')
class testBPdcals(basetask.StandardTaskTemplate):
    """Class for the testBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = testBPdcalsInputs

    def prepare(self):
        """Bulk of task execution occurs here.

        Args:
            None

        Returns:
            testBPdcalsResults()

        """
        self.ignorerefant = []

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = m.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        gtypecaltable = {}
        ktypecaltable = {}
        bpcaltable = {}
        bpdgain_touse = {}
        flaggedSolnApplycalbandpass = {}
        flaggedSolnApplycaldelay = {}
        gain_solint1 = {}
        shortsol1 = {}

        result_amp = {}
        result_phase = {}
        amp_collection = {}
        phase_collection = {}
        num_antennas = {}

        for band, spwlist in band2spw.items():

            for i in [0, 1, 2]:
                LOG.debug("    RUNNING FIRST PART TESTBPDCALS    ")
                gain_solint1perband, shortsol1perband, vis, bpdgain_tousename, gtypecaltablename, ktypecaltablename, bpcaltablename, \
                flaggedSolnApplycalbandpassperband, flaggedSolnApplycaldelayperband, refant = self._do_testBPdcals(band, spwlist)

                """
                If an entire baseband is determined to be bad for >50% of antennas,
                the pipeline should do the following:
                1.  Do not flag any data due to bad deformatters.
                2.  Remove the first reference antenna from the refant list and ignore that antenna in refant calculations
                        for the **entire pipeline run**
                3.  Recalculate the reference antenna list
                4.  Re-run hifv_testbpdcals and flagbaddef
                5.  Repeat up to three times and then just drive ahead.
                """

                LOG.debug("    RUNNING SECOND PART BADDEFORMATTERS    ")
                result_amp_perband, result_phase_perband, amp_collection_perband, phase_collection_perband, \
                num_antennas_perband, amp_job, phase_job = self._run_baddeformatters(bpcaltablename)

                pct_amp_ant = len(result_amp_perband) / num_antennas_perband
                pct_phase_ant = len(result_phase_perband) / num_antennas_perband
                ant_threshold = 0.5

                if (pct_amp_ant < ant_threshold and pct_phase_ant < ant_threshold) or i == 2:
                    if amp_job:
                        LOG.info("Executing bad deformatters amp flag commands for band {!s}...".format(band))
                        self._executor.execute(amp_job)
                    if phase_job:
                        LOG.info("Executing bad deformatters phase flag commands for band {!s}...".format(band))
                        self._executor.execute(phase_job)
                    break
                else:
                    # Criteria to finish not met - remove the first reference antenna from consideration
                    self.ignorerefant.append(refant)
                    LOG.warning("A baseband is determined to be bad for >50% of antennas.  "
                             "Removing reference antenna(s) {!s} and rerunning the test calibration.".format(','.join(self.ignorerefant)))

            gtypecaltable[band] = gtypecaltablename
            ktypecaltable[band] = ktypecaltablename
            bpcaltable[band] = bpcaltablename
            bpdgain_touse[band] = bpdgain_tousename
            flaggedSolnApplycalbandpass[band] = flaggedSolnApplycalbandpassperband
            flaggedSolnApplycaldelay[band] = flaggedSolnApplycaldelayperband
            gain_solint1[band] = gain_solint1perband
            shortsol1[band] = shortsol1perband

            result_amp[band] = result_amp_perband
            result_phase[band] = result_phase_perband
            amp_collection[band] = amp_collection_perband
            phase_collection[band] = phase_collection_perband
            num_antennas[band] = num_antennas_perband

        return testBPdcalsResults(gain_solint1=gain_solint1, shortsol1=shortsol1, vis=vis,
                                  bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                  ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                  flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                  flaggedSolnApplycaldelay=flaggedSolnApplycaldelay, result_amp=result_amp,
                                  result_phase=result_phase, amp_collection=amp_collection,
                                  phase_collection=phase_collection,
                                  num_antennas=num_antennas, ignorerefant=self.ignorerefant)

    def analyse(self, results):
        """Determine the best parameters by analysing the given jobs before returning any final jobs to execute.

        Override method of basetask.StandardTaskTemplate.analyze()

        Args:
            results (list of class: `~pipeline.infrastructure.jobrequest.JobRequest`):
                the job requests generated by :func:`~SimpleTask.prepare`

        Returns:
            class:`~pipeline.api.Result`
        """
        return results

    def _do_testBPdcals(self, band: str, spwlist: List[str]):
        """Execute testBPdcals heuristics per band and spwlist

        Args:
            band(str):  String band single letter identifier -  'L'  'U'  'X' etc.
            spwlist(List):  List of string values for spws - ['0', '1', '2', '3']

        Returns:
            gain_solint1(str):  solution interval value
            shortsol1(str):  Integration time determined from heuristics (1,3,10 x max int time)
            vis:  MS name
            bpdgain_touse(str):  bp'd gain table used
            gtypecaltable(str):  G-type table from gaincal
            ktypecaltable(str):  K-type table from gaincal
            bpcaltable(str):     BP cal table
            flaggedSolnApplycalbandpass(Dict):  returned from getCalFlaggedSoln for bpdgain_tous
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable
            RefAntOutput(str):  Reference antenna used

        """

        LOG.info("Executing for band {!s}  spws: {!s}".format(band, ','.join(spwlist)))
        self.parang = True
        try:
            self.setjy_results = self.inputs.context.results[0].read()[0].setjy_results
        except Exception as e:
            self.setjy_results = self.inputs.context.results[0].read().setjy_results

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_testBPdcals.s'

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'testdelayinitialgain_{!s}.tbl'.format(band)
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'testdelay_{!s}.tbl'.format(band)
        bpcaltable = tableprefix + str(stage_number) + '_4.' + 'testBPcal_{!s}.tbl'.format(band)
        tablebase = tableprefix + str(stage_number) + '_3.' + 'testBPdinitialgain'
        table_suffix = ['_{!s}.tbl'.format(band), '3_{!s}.tbl'.format(band), '10_{!s}.tbl'.format(band)]
        soltimes = [1.0, 3.0, 10.0]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        soltimes = [m.get_vla_max_integration_time() * x for x in soltimes]
        solints = ['int', str(soltimes[1]) + 's', str(soltimes[2]) + 's']
        soltime = soltimes[0]
        solint = solints[0]

        # Remove tables if they exist
        for tablename in [gtypecaltable, ktypecaltable, bpcaltable, tablebase + table_suffix[0], tablebase + table_suffix[1], tablebase + table_suffix[2]]:
            if os.path.isdir(tablename):
                LOG.info("Removing table: {!s}".format(tablename))
                shutil.rmtree(tablename)

        # PIPE-1637: adding ',' in the manual and auto refantignore parameter
        refantignore = self.inputs.refantignore + ','.join(['', *self.ignorerefant])
        refantfield = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string

        # PIPE-595: if refant list is not provided, compute refants else use provided refant list.
        if len(self.inputs.refant) == 0:
            refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                    geometry=True, flagging=True, intent='',
                                                    spw='', refantignore=refantignore)

            RefAntOutput = refantobj.calculate()
        else:
            RefAntOutput = self.inputs.refant.split(",")

        LOG.info("RefAntOutput: {}".format(RefAntOutput))

        self._do_gtype_delaycal(caltable=gtypecaltable, RefAntOutput=RefAntOutput, spwlist=spwlist)

        LOG.info("Initial phase calibration on delay calibrator complete for band {!s}".format(band))

        fracFlaggedSolns = 1.0

        critfrac = m.get_vla_critfrac()

        # Iterate and check the fraction of Flagged solutions, each time running gaincal in 'K' mode
        flagcount = 0
        while fracFlaggedSolns > critfrac and flagcount < 4:
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable,
                                    RefAntOutput=RefAntOutput, spw=','.join(spwlist))
            flaggedSolnResult = getCalFlaggedSoln(ktypecaltable)
            (fracFlaggedSolns, RefAntOutput) = self._check_flagSolns(flaggedSolnResult, RefAntOutput)
            LOG.info("Fraction of flagged solutions = " + str(flaggedSolnResult['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult['antmedian']['fraction']))
            flagcount += 1

        # Do initial amplitude and phase gain solutions on the BPcalibrator and delay
        # calibrator; the amplitudes are used for flagging; only phase
        # calibration is applied in final BP calibration, so that solutions are
        # not normalized per spw and take out the baseband filter shape

        # Try running with solint of int_time, 3*int_time, and 10*int_time.
        # If there is still a large fraction of failed solutions with
        # solint=10*int_time the source may be too weak, and calibration via the
        # pipeline has failed; will need to implement a mode to cope with weak
        # calibrators (later)

        bpdgain_touse = tablebase + table_suffix[0]

        self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                solint=solint, RefAntOutput=RefAntOutput, spwlist=spwlist)

        flaggedSolnResult1 = getCalFlaggedSoln(tablebase + table_suffix[0])
        LOG.info("For solint = " + solint + " fraction of flagged solutions = " +
                 str(flaggedSolnResult1['all']['fraction']))
        LOG.info("Median fraction of flagged solutions per antenna = " +
                 str(flaggedSolnResult1['antmedian']['fraction']))

        if flaggedSolnResult1['all']['total'] > 0:
            fracFlaggedSolns1 = flaggedSolnResult1['antmedian']['fraction']
        else:
            fracFlaggedSolns1 = 1.0

        gain_solint1 = solint
        shortsol1 = soltime

        if fracFlaggedSolns1 > 0.05:
            soltime = soltimes[1]
            solint = solints[1]

            self._do_gtype_bpdgains(tablebase + table_suffix[1], addcaltable=ktypecaltable,
                                    solint=solint, RefAntOutput=RefAntOutput, spwlist=spwlist)

            flaggedSolnResult3 = getCalFlaggedSoln(tablebase + table_suffix[1])
            LOG.info("For solint = " + solint + " fraction of flagged solutions = " +
                     str(flaggedSolnResult3['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult3['antmedian']['fraction']))

            if flaggedSolnResult3['all']['total'] > 0:
                fracFlaggedSolns3 = flaggedSolnResult3['antmedian']['fraction']
            else:
                fracFlaggedSolns3 = 1.0

            if fracFlaggedSolns3 < fracFlaggedSolns1:
                gain_solint1 = solint
                shortsol1 = soltime

                bpdgain_touse = tablebase + table_suffix[1]

                if fracFlaggedSolns3 > 0.05:
                    soltime = soltimes[2]
                    solint = solints[2]

                    self._do_gtype_bpdgains(tablebase + table_suffix[2], addcaltable=ktypecaltable, solint=solint,
                                            RefAntOutput=RefAntOutput, spwlist=spwlist)
                    flaggedSolnResult10 = getCalFlaggedSoln(tablebase + table_suffix[2])
                    LOG.info("For solint = " + solint + " fraction of flagged solutions = " +
                             str(flaggedSolnResult10['all']['fraction']))
                    LOG.info("Median fraction of flagged solutions per antenna = " +
                             str(flaggedSolnResult10['antmedian']['fraction']))

                    if flaggedSolnResult10['all']['total'] > 0:
                        fracFlaggedSolns10 = flaggedSolnResult10['antmedian']['fraction']
                    else:
                        fracFlaggedSolns10 = 1.0

                    if fracFlaggedSolns10 < fracFlaggedSolns3:
                        gain_solint1 = solint
                        shortsol1 = soltime
                        bpdgain_touse = tablebase + table_suffix[2]

                        if fracFlaggedSolns10 > 0.05:
                            LOG.warning("There is a large fraction of flagged solutions, " +
                                     "there might be something wrong with your data.  " +
                                     "The fraction of flagged solutions is " + str(fracFlaggedSolns10))

        LOG.info("Test amp and phase calibration on delay and bandpass calibrators complete for band {!s}".format(band))
        LOG.info("Using short solint = {!s} for band {!s}".format(str(gain_solint1), band))

        LOG.info("Doing test bandpass calibration for band {!s}".format(band))

        if self.inputs.weakbp:
            # LOG.info("USING WEAKBP HEURISTICS")
            interp = weakbp(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                            ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False,
                            executor=self._executor, spw=','.join(spwlist))
        else:
            # LOG.info("Using REGULAR heuristics")
            interp = ''
            do_bandpass(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                        spw=','.join(spwlist), ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                        solint='inf', append=False, executor=self._executor)

            AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
            AllCalTables.append(ktypecaltable)
            AllCalTables.append(bpdgain_touse)
            AllCalTables.append(bpcaltable)
            ntables = len(AllCalTables)
            interp = [''] * ntables
            LOG.info("Using 'linear,linearflag' for bandpass table")
            interp[-1] = 'linear,linearflag'

        LOG.info("Test bandpass calibration complete")
        LOG.info("Fraction of flagged solutions = {!s}".format(str(flaggedSolnResult['all']['fraction'])))
        LOG.info(
            "Median fraction of flagged solutions per antenna = " + str(flaggedSolnResult['antmedian']['fraction']))

        LOG.info("Executing flagdata in clip mode.")
        self._do_clipflag(bpcaltable)

        LOG.info("Applying test calibrations to BP and delay calibrators for band {!s}".format(band))

        self._do_applycal(ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                          bpcaltable=bpcaltable, interp=interp, spw=','.join(spwlist))

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return gain_solint1, shortsol1, self.inputs.vis, bpdgain_touse, gtypecaltable,\
               ktypecaltable, bpcaltable, flaggedSolnApplycalbandpass, flaggedSolnApplycaldelay, RefAntOutput[0]

    def _do_gtype_delaycal(self, caltable: str = None, RefAntOutput: List[str] = None, spwlist: List[str] = []) -> bool:
        """Perform a G-Type delay calibration with CASA task gaincal

        Args:
            caltable(str): Name of the caltable to be created
            RefAntOutput(List): List of string antenna values to use as reference antennas - ['ea01', 'ea24', ...]
            spwlist(List): List of string values for spws pertaining to the particular band - ['0', '1', '2', ...]

        Returns:
            Boolean

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = self.inputs.context.evla['msinfo'][m.name].delay_field_select_string
        tst_delay_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
        delay_scan_select_string = self.inputs.context.evla['msinfo'][m.name].delay_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        delaycal_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_delay_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': delay_scan_select_string,
                              'solint': 'int',
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': False,
                              'docallib': False,
                              'gaintable': sorted(self.inputs.context.callibrary.active.get_caltable()),
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        fields = delay_field_select_string.split(',')
        for fieldidstring in fields:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            delaycal_task_args['field'] = fieldidstring
            delaycal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                delaycal_task_args['append'] = True

            job = casa_tasks.gaincal(**delaycal_task_args)

            self._executor.execute(job)

        return True

    def _do_ktype_delaycal(self, caltable: str = None, addcaltable: str = None,
                           RefAntOutput: List[str] = None, spw: str = '') -> bool:
        """Perform a K-Type delay calibration with CASA task gaincal

        Args:
            caltable(str): Name of the caltable to be created
            addcaltable(str):  String name of table to temporarily be added to the gaincal gaintable parameter
            RefAntOutput(List): List of string antenna values to use as reference antennas - ['ea01', 'ea24', ...]
            spw(str): csv string values for spws pertaining to the particular band - '0,1,2,3,4,5,6'

        Returns:
            Boolean

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = self.inputs.context.evla['msinfo'][m.name].delay_field_select_string
        delay_scan_select_string = self.inputs.context.evla['msinfo'][m.name].delay_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        delaycal_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': delay_scan_select_string,
                              'solint': 'inf',
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'K',
                              'smodel': [],
                              'calmode': 'p',
                              'append': False,
                              'docallib': False,
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        for fieldidstring in delay_field_select_string.split(','):
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            delaycal_task_args['field'] = fieldidstring
            delaycal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                delaycal_task_args['append'] = True

            job = casa_tasks.gaincal(**delaycal_task_args)

            self._executor.execute(job)

        return True

    def _check_flagSolns(self, flaggedSolnResult: Dict, RefAntOutput: List[str] = None) -> (float, List[str]):
        """Change reference antenna list based on a critical fraction of flagged solutions
            (defined in the domain ms object)

        Args:
            flaggedSolnResult(Dict): Breakdown of flagged solutions
            RefAntOutput(List): List of string antenna values to use as reference antennas - ['ea01', 'ea24', ...]

        Returns:
            fracFlaggedSolns(float):  fraction of flagged solutions used in this function
            RefAntOutput(List): List of string antenna values to use as reference antennas - ['ea01', 'ea24', ...]
                                Modified if fraction of flagged solutions is greater than critical fraction

        """

        if flaggedSolnResult['all']['total'] > 0:
            fracFlaggedSolns = flaggedSolnResult['antmedian']['fraction']
        else:
            fracFlaggedSolns = 1.0

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        critfrac = m.get_vla_critfrac()

        if fracFlaggedSolns > critfrac:
            RefAntOutput = np.delete(RefAntOutput, 0)
            self.inputs.context.observing_run.measurement_sets[0].reference_antenna = ','.join(RefAntOutput)
            LOG.info("Not enough good solutions, trying a different reference antenna.")
            LOG.info("The pipeline will start with antenna "+RefAntOutput[0].lower()+" as the reference.")

        return fracFlaggedSolns, RefAntOutput

    def _do_gtype_bpdgains(self, caltable: str, addcaltable: str = None, solint: str = 'int',
                           RefAntOutput: List[str] = None, spwlist: List[str] = []) -> bool:
        """Perform a G-Type cal with CASA task gaincal on the bp'd gaintable

        Args:
            caltable(str): Name of the caltable to be created
            addcaltable(str):  String name of table to temporarily be added to the gaincal gaintable parameter
            solint(str):  String value for solint keyword of CASA task gaincal
            RefAntOutput(List): List of string antenna values to use as reference antennas - ['ea01', 'ea24', ...]
            spwlist(List): List of string values for spws pertaining to the particular band - ['0', '1', '2', ...]

        Returns:
            Boolean

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
        delay_scan_select_string = self.inputs.context.evla['msinfo'][m.name].delay_scan_select_string
        bandpass_scan_select_string = self.inputs.context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        if delay_scan_select_string == bandpass_scan_select_string:
            testgainscans = bandpass_scan_select_string
        else:
            testgainscans = bandpass_scan_select_string + ',' + delay_scan_select_string

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        bpdgains_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_bpass_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': testgainscans,
                              'solint': solint,
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 5.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'ap',
                              'append': False,
                              'docallib': False,
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        testgainscanslist = list(map(int, testgainscans.split(',')))
        scanobjlist = m.get_scans(scan_id=testgainscanslist)
        fieldidlist = []
        for scanobj in scanobjlist:
            fieldobj, = scanobj.fields
            if str(fieldobj.id) not in fieldidlist:
                fieldidlist.append(str(fieldobj.id))

        for fieldidstring in fieldidlist:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            bpdgains_task_args['field'] = fieldidstring
            bpdgains_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                bpdgains_task_args['append'] = True

            job = casa_tasks.gaincal(**bpdgains_task_args)

            self._executor.execute(job)

        return True

    def _do_clipflag(self, bpcaltable: str):
        """Execute CASA task flagdata on the bpcaltable

        Args:
            bpcaltable(str):  caltable to flag

        Returns:
            Executed job

        """

        task_args = {'vis': bpcaltable,
                     'mode': 'clip',
                     'datacolumn': 'CPARAM',
                     'clipminmax': [0.0, 2.0],
                     'correlation': 'ABS_ALL',
                     'clipoutside': True,
                     'flagbackup': False,
                     'savepars': False,
                     'action': 'apply'}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self, ktypecaltable: str = None, bpdgain_touse: str = None, bpcaltable: str = None,
                     interp: str = None, spw: str = ''):
        """Run CASA task applycal with tables from priorcals task plus those generated in testBPdcals

        Args:
            ktypecaltable(str): output from K-type gaincal
            bpgain_touse(str): gaintable determined to be used from heuristics
            bpcaltable(str): BP caltable to use
            interp(str): applycal CASA task keyword
            spw(str): csv string values for spws pertaining to the particular band - '0,1,2,3,4,5,6'

        Returns:
            Executed job

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        testgainscans = self.inputs.context.evla['msinfo'][m.name].testgainscans

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpdgain_touse)
        AllCalTables.append(bpcaltable)

        ntables = len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': spw,
                              'intent': '',
                              'selectdata': True,
                              'scan': testgainscans,
                              'docallib': False,
                              'gaintable': AllCalTables,
                              'gainfield': [''],
                              'interp': interp,
                              'spwmap': [],
                              'calwt': [False]*ntables,
                              'parang': self.parang,
                              'applymode': 'calflagstrict',
                              'flagbackup': True}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)

    def _run_baddeformatters(self, bpcaltable: str):
        """Setting control parameters as method arguments

        Args:
            bpcaltable(str): BP cal table to use

        Return:
            result_amp(List): Bad deformatters amp flagging
            result_phase(List): Bad deformatters phase flagging
            amp_collection(Dict): Collection for weblog display
            phase_collection(Dict):  Collection for weblog display
            num_antennas(int): Number of antennas (for weblog convenience)
            amp_job(Dict):  flagdata result from the amplitude execution
            phase_job(Dict):  flagdata result from the phase execution

        """

        method_args = {'testq': 'amp',  # Which quantity to test? ['amp','phase','real','imag']
                       'tstat': 'rat',  # Which stat to use?['min','max','mean','var']or'rat'=min/max or 'diff'=max-min
                       'doprintall': True,  # Print detailed flagging stats
                       'testlimit': 0.15,  # Limit for test (flag values under/over this limit)
                       'testunder': True,
                       'nspwlimit': 4,  # Number of spw per baseband to trigger flagging entire baseband
                       'doflagundernspwlimit': self.inputs.doflagundernspwlimit,
                       # Flag individual spws when below nspwlimit
                       'doflagemptyspws': False,  # Flag data for spws with no unflagged channel solutions in any poln?
                       'calBPtablename': bpcaltable,  # Define the table
                       'flagreason': 'bad_deformatters_amp or RFI'}  # Define the REASON given for the flags

        (result_amp, amp_collection, num_antennas, amp_job) = self._do_flag_baddeformatters(**method_args)

        method_args = {'testq': 'phase',
                       'tstat': 'diff',
                       'doprintall': True,
                       'testlimit': 50,
                       'testunder': False,
                       'nspwlimit': 4,
                       'doflagundernspwlimit': self.inputs.doflagundernspwlimit,
                       'doflagemptyspws': False,
                       'calBPtablename': bpcaltable,
                       'flagreason': 'bad_deformatters_phase or RFI'}

        (result_phase, phase_collection, num_antennas, phase_job) = self._do_flag_baddeformatters(**method_args)

        return result_amp, result_phase, amp_collection, phase_collection, num_antennas, amp_job, phase_job

    def _do_flag_baddeformatters(self, testq: str = None, tstat: str = None, doprintall: bool = True,
                                 testlimit: float = None, testunder: bool = True, nspwlimit: int = 4,
                                 doflagundernspwlimit: bool = True, doflagemptyspws: bool = False,
                                 calBPtablename: str = None, flagreason: str = None):
        """Determine bad deformatters in the MS and flag them
           Looks for bandpass solutions that have small ratio of min/max amplitudes

        Args:
            testq(str): Which quantity to test? ['amp','phase','real','imag']                    Original script: 'amp'
            tstat(str): Which stat to use? ['min','max','mean','var'] or 'rat'=min/max or 'diff'=max-min
                        Original script: 'rat'
            doprintall(bool): Print detailed flagging stats                                      Original script: True
            testlimit(float): Limit for test (flag values under/over this limit)                 Original script: 0.15
            testunder(bool): Will flag values under limit                                        Original script: True
            nspwlimit(int): Number of spw per baseband to trigger flagging entire baseband       Original script: 4
            doflagundernspwlimit(bool): Flag individual spws when below nspwlimit                Original script: True
            doflagemptyspws(bool): Flag data for spws with no unflagged channel solutions in any poln?
            calBPtablename(str): caltable name
            flagreason(str): Reason for flagging

        Returns:
            flaglist(List): phase or amp flagging commands list
            weblogflagdict(Dict): collection for weblog display
            num_antennas(int):  number of antennas
            job(Dict):  Result of flagdata execution

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        num_antennas = len(m.antennas)
        startdate = m.start_time['m0']['value']

        LOG.info("Start date for flag bad deformatters is: " + str(startdate))

        if startdate <= 56062.7:
            doflagdata = False
        else:
            doflagdata = True

        # Define the table to run this on
        # calBPtablename ='testBPcal.b'
        # Define the REASON given for the flags
        # flagreason = 'bad_deformatters_amp or RFI'

        LOG.info("Will test on quantity: "+testq)
        LOG.info("Will test using statistic: "+tstat)

        if testunder:
            LOG.info("Will flag values under limit = "+str(testlimit))
        else:
            LOG.info("Will flag values over limit = "+str(testlimit))

        LOG.info("Will identify basebands with more than "+str(nspwlimit)+" bad spw")

        if doflagundernspwlimit:
            LOG.info("Will identify individual spw when less than "+str(nspwlimit)+" bad spw")

        if doflagemptyspws:
            LOG.info("Will identify spw with no unflagged channels")

        LOG.info("Will use flag REASON = "+flagreason)

        if doflagdata:
            LOG.info("Will flag data based on what we found")
        else:
            LOG.info("Will NOT flag data based on what we found")

        calBPstatresult = getBCalStatistics(calBPtablename)
        flaglist = []
        extflaglist = []
        weblogflagdict = collections.defaultdict(list)

        for iant in calBPstatresult['antband']:
            antName = calBPstatresult['antDict'][iant]
            badspwlist = []
            flaggedspwlist = []
            for rrx in calBPstatresult['antband'][iant]:
                for bband in calBPstatresult['antband'][iant][rrx]:
                    # List of spw in this baseband
                    spwl = calBPstatresult['rxBasebandDict'][rrx][bband]

                    nbadspws = 0
                    badspws = []
                    flaggedspws = []
                    if len(spwl) > 0:
                        if doprintall:
                            LOG.info(' Ant %s (%s) %s %s processing spws=%s' %
                                     (str(iant), antName, rrx, bband, str(spwl)))

                        for ispw in spwl:
                            testvalid = False
                            if ispw in calBPstatresult['antspw'][iant]:
                                for poln in calBPstatresult['antspw'][iant][ispw]:
                                    # Get stats of this ant/spw/poln
                                    nbp = calBPstatresult['antspw'][iant][ispw][poln]['inner']['number']

                                    if nbp > 0:
                                        if tstat == 'rat':
                                            bpmax = calBPstatresult['antspw'][iant][ispw][poln]['inner'][testq]['max']
                                            bpmin = calBPstatresult['antspw'][iant][ispw][poln]['inner'][testq]['min']

                                            if bpmax == 0.0:
                                                tval = 0.0
                                            else:
                                                tval = bpmin/bpmax
                                        elif tstat == 'diff':
                                            bpmax = calBPstatresult['antspw'][iant][ispw][poln]['inner'][testq]['max']
                                            bpmin = calBPstatresult['antspw'][iant][ispw][poln]['inner'][testq]['min']

                                            tval = bpmax-bpmin
                                        else:
                                            # simple test on quantity
                                            tval = calBPstatresult['antspw'][iant][ispw][poln]['inner'][testq][tstat]
                                        if not testvalid:
                                            testval = tval
                                            testvalid = True
                                        elif testunder:
                                            if tval < testval:
                                                testval = tval
                                        else:
                                            if tval > testval:
                                                testval = tval
                                # Test on extrema of the polarizations for this ant/spw
                                if not testvalid:
                                    # these have no unflagged channels in any poln
                                    flaggedspws.append(ispw)
                                else:
                                    if (testunder and testval < testlimit) or (not testunder and testval > testlimit):
                                        nbadspws += 1
                                        badspws.append(ispw)
                                        if doprintall:
                                            LOG.info('  Found Ant %s (%s) %s %s spw=%s %s %s=%6.4f' %
                                                     (str(iant), antName, rrx, bband, str(ispw), testq, tstat, testval))

                            else:
                                # this spw is missing from this antenna/rx
                                if doprintall:
                                    LOG.info('  Ant %s (%s) %s %s spw=%s missing solution' %
                                             (str(iant), antName, rrx, bband, str(ispw)))

                    # Test to see if this baseband should be entirely flagged
                    if nbadspws > 0 and nbadspws >= nspwlimit:
                        # Flag all spw in this baseband
                        bbspws = calBPstatresult['rxBasebandDict'][rrx][bband]
                        badspwlist.extend(bbspws)
                        LOG.info('Ant %s (%s) %s %s bad baseband spws=%s' %
                                 (str(iant), antName, rrx, bband, str(bbspws)))
                    elif nbadspws > 0 and doflagundernspwlimit:
                        # Flag spws individually
                        badspwlist.extend(badspws)
                        LOG.info('Ant %s (%s) %s %s bad spws=%s' % (str(iant), antName, rrx, bband, str(badspws)))
                    if len(flaggedspws) > 0:
                        flaggedspwlist.extend(flaggedspws)
                        LOG.info('Ant %s (%s) %s %s no unflagged solutions spws=%s ' %
                                 (str(iant), antName, rrx, bband, str(flaggedspws)))

            if len(badspwlist) > 0:
                spwstr = ''
                for ispw in badspwlist:
                    if spwstr == '':
                        spwstr = str(ispw)
                    else:
                        spwstr += ','+str(ispw)
                #
                # reastr = 'bad_deformatters'
                reastr = flagreason
                # Add entry for this antenna
                # flagstr = "mode='manual' antenna='"+str(iant)+"' spw='"+spwstr+"' reason='"+reastr+"'"
                # Use name for flagging
                flagstr = "mode='manual' antenna='"+antName+"' spw='"+spwstr+"'"
                flaglist.append(flagstr)
                weblogflagdict[antName].append(spwstr)

            if doflagemptyspws and len(flaggedspwlist) > 0:
                spwstr = ''
                for ispw in flaggedspwlist:
                    if spwstr == '':
                        spwstr = str(ispw)
                    else:
                        spwstr += ','+str(ispw)
                #
                # Add entry for this antenna
                reastr = 'no_unflagged_solutions'
                # flagstr = "mode='manual' antenna='"+str(iant)+"' spw='"+spwstr+"' reason='"+reastr+"'"
                # Use name for flagging
                flagstr = "mode='manual' antenna='"+antName+"' spw='"+spwstr+"'"
                extflaglist.append(flagstr)
                weblogflagdict[antName].append(spwstr)

        # Get basebands matched with spws.  spws is a single element list with a single csv string
        tempDict = {}
        for antNamekey, spws in weblogflagdict.items():
            basebands = []
            for spwstr in spws[0].split(','):
                spw = m.get_spectral_window(spwstr)
                basebands.append(spw.name.split('#')[0] + '  ' + spw.name.split('#')[1])
            basebands = list(set(basebands))  # Unique basebands
            tempDict[antNamekey] = {'spws': spws, 'basebands': basebands}

        weblogflagdict = tempDict

        nflagcmds = len(flaglist) + len(extflaglist)
        if nflagcmds < 1:
            LOG.info("No bad basebands/spws found")
        else:
            LOG.info("Possible bad basebands/spws found:")

            for flagstr in flaglist:
                LOG.info("    "+flagstr)
            if len(extflaglist) > 0:
                LOG.info("    ")
                for flagstr in extflaglist:
                    LOG.info("    "+flagstr)
                flaglist.extend(extflaglist)

            if doflagdata:
                LOG.info("Setting flag commands for bad deformatters in the ms (quantity {!s})...".format(testq))

                task_args = {'vis': self.inputs.vis,
                             'mode': 'list',
                             'action': 'apply',
                             'inpfile': flaglist,
                             'savepars': True,
                             'flagbackup': True}

                job = casa_tasks.flagdata(**task_args)

                # self._executor.execute(job)

                # get the total fraction of data flagged for all antennas
                # flagging_stats = getCalFlaggedSoln(calBPtablename)
                # total = 0
                # flagged = 0
                # for antenna in flagging_stats['ant']:
                #     for pol in flagging_stats['ant'][antenna]:
                #         flagged += flagging_stats['ant'][antenna][pol]['flagged']
                #         total += flagging_stats['ant'][antenna][pol]['total']
                # fraction_flagged = flagged / total

                # LOG.info('Flagged ({}) Total ({}) Fraction ({})'.format(flagged, total, fraction_flagged))
                return flaglist, weblogflagdict, num_antennas, job

        # If the flag commands are not executed.
        return [], collections.defaultdict(list), num_antennas, None
