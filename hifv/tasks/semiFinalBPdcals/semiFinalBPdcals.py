import os
import collections
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

LOG = infrastructure.get_logger(__name__)


class semiFinalBPdcalsInputs(vdp.StandardInputs):
    """Inputs class for the hifv_semiFinalBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    weakbp = vdp.VisDependentProperty(default=False)
    refantignore = vdp.VisDependentProperty(default='')
    refant = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, weakbp=None, refantignore=None, refant=None):
        """
        Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set
            weakbp(Boolean):  weak bandpass heuristics on/off - currently not used - see PIPE-104
            refantignore(str):  csv string of reference antennas to ignore - 'ea24,ea15,ea08'
            refant(str): A csv string of reference antenna(s). When used, disables refantignore.

        """
        super(semiFinalBPdcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore
        self.refant = refant


class semiFinalBPdcalsResults(basetask.Results):
    """Results class for the hifv_semiFinalBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.Results.

    """
    def __init__(self, final=None, pool=None, preceding=None, bpdgain_touse=None,
                 gtypecaltable=None, ktypecaltable=None, bpcaltable=None, flaggedSolnApplycalbandpass=None,
                 flaggedSolnApplycaldelay=None):
        """
        Args:
            final(List, optional): Calibration list applied - not used
            pool(List, optional): Calibration list assesed - not used
            preceding(List, optional): DEPRECATED results from worker tasks executed by this task
            bpdgain_touse(Dict):  Dictionary of tables per band
            gtypecaltable(Dict): Dictionary of tables per band
            ktypecaltable(Dict): Dictionary of tables per band
            bpcaltable(Dict): Dictionary of tables per band
            flaggedSolnApplycalbandpass(Dict): returned from getCalFlaggedSoln for bpdgain_touse (per band)
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable (per band)

        """

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(semiFinalBPdcalsResults, self).__init__()

        # self.vis = None
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay


@task_registry.set_equivalent_casa_task('hifv_semiFinalBPdcals')
class semiFinalBPdcals(basetask.StandardTaskTemplate):
    """Class for the semiFinalBPdcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = semiFinalBPdcalsInputs

    def prepare(self):
        """Bulk of task execution occurs here.

        Args:
            None

        Returns:
            semiFinalBPdcalsResults()

        """
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

        for band, spwlist in band2spw.items():

            bpdgain_tousename, gtypecaltablename, ktypecaltablename, bpcaltablename, \
            flaggedSolnApplycalbandpassperband, flaggedSolnApplycaldelayperband = self._do_semifinal(band, spwlist)

            gtypecaltable[band] = gtypecaltablename
            ktypecaltable[band] = ktypecaltablename
            bpcaltable[band] = bpcaltablename
            bpdgain_touse[band] = bpdgain_tousename
            flaggedSolnApplycalbandpass[band] = flaggedSolnApplycalbandpassperband
            flaggedSolnApplycaldelay[band] = flaggedSolnApplycaldelayperband

        return semiFinalBPdcalsResults(bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                       ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                       flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                       flaggedSolnApplycaldelay=flaggedSolnApplycaldelay)

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

    def _do_semifinal(self, band: str, spwlist: List[str]):
        """Execute semiFinalBPdcals heuristics per band and spwlist

        Args:
            band(str):  String band single letter identifier -  'L'  'U'  'X' etc.
            spwlist(List):  List of string values for spws - ['0', '1', '2', '3']

        Returns:
            bpdgain_touse(str):  bp'd gain table used
            gtypecaltable(str):  G-type table from gaincal
            ktypecaltable(str):  K-type table from gaincal
            bpcaltable(str):     BP cal table
            flaggedSolnApplycalbandpass(Dict):  returned from getCalFlaggedSoln for bpdgain_tous
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable

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

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_semiFinalBPdcals.s'

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'semiFinaldelayinitialgain_{!s}.tbl'.format(band)
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'delay_{!s}.tbl'.format(band)
        bpcaltable = tableprefix + str(stage_number) + '_4.' + 'BPcal_{!s}.tbl'.format(band)
        tablebase = tableprefix + str(stage_number) + '_3.' + 'BPinitialgain'

        table_suffix = ['_{!s}.tbl'.format(band), '3_{!s}.tbl'.format(band), '10_{!s}.tbl'.format(band)]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        self.ignorerefant = self.inputs.context.evla['msinfo'][m.name].ignorerefant
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

        self._do_gtype_delaycal(caltable=gtypecaltable, RefAntOutput=RefAntOutput, spwlist=spwlist)

        fracFlaggedSolns = 1.0

        critfrac = m.get_vla_critfrac()

        # Iterate and check the fraciton of Flagged solutions, each time running gaincal in 'K' mode
        flagcount = 0
        while fracFlaggedSolns > critfrac and flagcount < 4:
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable,
                                    RefAntOutput=RefAntOutput, spw=','.join(spwlist))
            flaggedSolnResult = getCalFlaggedSoln(ktypecaltable)
            fracFlaggedSolns = self._check_flagSolns(flaggedSolnResult, RefAntOutput)

            LOG.info("Fraction of flagged solutions = " + str(flaggedSolnResult['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult['antmedian']['fraction']))
            flagcount += 1

        LOG.info("Delay calibration complete for band {!s}".format(band))

        # Do initial gaincal on BP calibrator then semi-final BP calibration
        gain_solint1 = self.inputs.context.evla['msinfo'][m.name].gain_solint1[band]
        self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                solint=gain_solint1, RefAntOutput=RefAntOutput, spwlist=spwlist)

        bpdgain_touse = tablebase + table_suffix[0]

        LOG.debug("WEAKBP: " + str(self.inputs.weakbp))

        if self.inputs.weakbp:
            LOG.debug("USING WEAKBP HEURISTICS")
            interp = weakbp(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                            ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False,
                            executor=self._executor, spw=','.join(spwlist))
        else:
            LOG.debug("Using REGULAR heuristics")
            do_bandpass(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                        spw=','.join(spwlist), ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                        solint='inf', append=False, executor=self._executor)

            AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
            AllCalTables.append(ktypecaltable)
            # AllCalTables.append(bpdgain_touse)
            AllCalTables.append(bpcaltable)
            ntables = len(AllCalTables)
            interp = [''] * ntables
            LOG.info("Using 'linear,linearflag' for bandpass table")
            interp[-1] = 'linear,linearflag'

        self._do_applycal(ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                          bpcaltable=bpcaltable, interp=interp, spw=','.join(spwlist))

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, flaggedSolnApplycalbandpass, \
               flaggedSolnApplycaldelay

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

        # need to add scan?
        # ref antenna string needs to be lower case for gaincal
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

        # need to add scan?
        # ref antenna string needs to be lower case for gaincal

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

        # refant_csvstring = self.inputs.context.observing_run.measurement_sets[0].reference_antenna
        # refantlist = [x for x in refant_csvstring.split(',')]

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        critfrac = m.get_vla_critfrac()

        if fracFlaggedSolns > critfrac:
            RefAntOutput = np.delete(RefAntOutput, 0)
            self.inputs.context.observing_run.measurement_sets[0].reference_antenna = ','.join(RefAntOutput)
            LOG.info("Not enough good solutions, trying a different reference antenna.")
            LOG.info("The pipeline start with antenna "+RefAntOutput[0]+" as the reference.")

        return fracFlaggedSolns

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
        bandpass_scan_select_string = self.inputs.context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        # need to add scan?
        # ref antenna string needs to be lower case for gaincal

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        bpdgains_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_bpass_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': bandpass_scan_select_string,
                              'solint': solint,
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
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        bpscanslist = list(map(int, bandpass_scan_select_string.split(',')))
        scanobjlist = m.get_scans(scan_id=bpscanslist)
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

    def _do_applycal(self, ktypecaltable: str = None, bpdgain_touse: str = None, bpcaltable: str = None,
                     interp: str = None, spw: str = ''):
        """Run CASA task applycal with tables from priorcals task plus those generated in testBPdcals
        Note that this iteration in the pipeline does not add the bpdgain_touse table.

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
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        LOG.info("Applying semi-final delay and BP calibrations to all calibrators")

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        # AllCalTables.append(bpdgain_touse)
        AllCalTables.append(bpcaltable)

        ntables=len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': spw,
                              'intent': '',
                              'selectdata': True,
                              'scan': calibrator_scan_select_string,
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
