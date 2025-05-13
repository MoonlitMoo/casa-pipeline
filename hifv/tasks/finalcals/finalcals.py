import os
import shutil
import collections
from typing import TYPE_CHECKING, List, Optional, Tuple, Union, Any, Dict

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import getCalFlaggedSoln
from pipeline.infrastructure.tablereader import find_EVLA_band
from pipeline.hifv.heuristics import standard as standard
from pipeline.hifv.heuristics import weakbp, do_bandpass, uvrange
from pipeline.hifv.tasks.setmodel.vlasetjy import standard_sources
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class FinalcalsInputs(vdp.StandardInputs):
    """Inputs class for the hifv_finalcals pipeline task.  Used on VLA measurement sets.

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
            refant(List): A csv string of reference antenna(s). When used, disables refantignore.

        """
        super(FinalcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore
        self.refant = refant


class FinalcalsResults(basetask.Results):
    """Results class for the hifv_finalcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.Results.

    """
    def __init__(self, final=None, pool=None, preceding=None, vis=None, bpdgain_touse=None,
                 gtypecaltable=None, ktypecaltable=None, bpcaltable=None,
                 phaseshortgaincaltable=None, finalampgaincaltable=None,
                 finalphasegaincaltable=None, flaggedSolnApplycalbandpass=None,
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
            phaseshortgaincaltable(Dict):  Dictionary of short phase tables per band
            finalampgaincaltable(Dict):  Amp gain table to be passed to applycal
            finalphasegaincaltable(Dict):  Phase gain table to be paseed to applycal
            flaggedSolnApplycalbandpass(Dict): returned from getCalFlaggedSoln for bpdgain_touse (per band)
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable (per band)

        """
        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(FinalcalsResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.phaseshortgaincaltable = phaseshortgaincaltable
        self.finalampgaincaltable = finalampgaincaltable
        self.finalphasegaincaltable = finalphasegaincaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay

    def merge_with_context(self, context):
        if not self.final:
            LOG.error('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)


@task_registry.set_equivalent_casa_task('hifv_finalcals')
class Finalcals(basetask.StandardTaskTemplate):
    """Class for the finalcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = FinalcalsInputs

    def prepare(self):
        """Bulk of task execution occurs here.

        Args:
            None

        Returns:
            FinalcalsResults()

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = m.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        self.pool = []
        self.final = []

        bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, phaseshortgaincaltable, \
        finalampgaincaltable, finalphasegaincaltable, \
        flaggedSolnApplycalbandpass, flaggedSolnApplycaldelay = self._do_finalscals(band2spw)

        return FinalcalsResults(vis=self.inputs.vis, pool=self.pool, final=self.final,
                                bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                phaseshortgaincaltable=phaseshortgaincaltable,
                                finalampgaincaltable=finalampgaincaltable,
                                finalphasegaincaltable=finalphasegaincaltable,
                                flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                flaggedSolnApplycaldelay=flaggedSolnApplycaldelay)

    def _do_finalscals(self, band2spw):
        """Execute finalcals heuristics

        Args:
            band2spw(Dict):  Band dictionary with key/value as band/spws

        Returns:
            bpdgain_touse(str):  bp'd gain table used
            gtypecaltable(str):  G-type table from gaincal
            ktypecaltable(str):  K-type table from gaincal
            bpcaltable(str):     BP cal table
            phaseshortgaincaltable(Dict): Dictionary of short phase tables per band
            finalampgaincaltable(Dict):  Amp gain table to be passed to applycal
            finalphasegaincaltable(Dict):  Phase gain table to be paseed to applycal
            flaggedSolnApplycalbandpass(Dict):  returned from getCalFlaggedSoln for bpdgain_tous
            flaggedSolnApplycaldelay(Dict): returned from getCalFlaggedSoln for ktypecaltable

        """

        self.parang = True
        try:
            self.setjy_results = self.inputs.context.results[0].read()[0].setjy_results
        except Exception as e:
            self.setjy_results = self.inputs.context.results[0].read().setjy_results

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_finalcals.s'

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'finaldelayinitialgain.tbl'
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'finaldelay.tbl'
        bpcaltable = tableprefix + str(stage_number) + '_4.' + 'finalBPcal.tbl'
        tablebase = tableprefix + str(stage_number) + '_3.' + 'finalBPinitialgain'
        table_suffix = ['.tbl', '3.tbl', '10.tbl']

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

        refAnt = ','.join(RefAntOutput)

        LOG.info("The pipeline will use antenna(s) " + refAnt + " as the reference")

        for band, spwlist in band2spw.items():
            LOG.info("Executing G-type delaycal for band {!s}  spws: {!s}".format(band, ','.join(spwlist)))
            self._do_gtype_delaycal(caltable=gtypecaltable, refAnt=refAnt, spwlist=spwlist)

        for band, spwlist in band2spw.items():
            LOG.info("Executing K-type delaycal for band {!s}  spws: {!s}".format(band, ','.join(spwlist)))
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable, refAnt=refAnt,
                                    spw=','.join(spwlist))

        LOG.info("Delay calibration complete")

        # Do initial gaincal on BP calibrator then semi-final BP calibration
        for band, spwlist in band2spw.items():
            gain_solint1 = self.inputs.context.evla['msinfo'][m.name].gain_solint1[band]
            self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                    solint=gain_solint1, refAnt=refAnt, spwlist=spwlist)

        bpdgain_touse = tablebase + table_suffix[0]
        LOG.info("Initial BP gain calibration complete")

        for band, spwlist in band2spw.items():
            append = False
            isdir = os.path.isdir(bpcaltable)
            if isdir:
                append = True
                LOG.info("Appending to existing table: {!s}".format(bpcaltable))

            if self.inputs.weakbp:
                LOG.debug("USING WEAKBP HEURISTICS")

                interp = weakbp(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                                ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=append,
                                executor=self._executor, spw=','.join(spwlist))
            else:
                LOG.debug("Using REGULAR heuristics")
                interp = ''
                do_bandpass(self.inputs.vis, bpcaltable, context=self.inputs.context, RefAntOutput=RefAntOutput,
                            spw=','.join(spwlist), ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=append,
                            executor=self._executor)

        LOG.info("Bandpass calibration complete")

        # Derive an average phase solution for the bandpass calibrator to apply
        # to all data to make QA plots easier to interpret.

        refantmode = 'flex'
        intents = list(m.intents)
        if [intent for intent in intents if 'POL' in intent]:
            # set to strict
            refantmode = 'strict'

        avgpgain = tableprefix + str(stage_number) + '_5.' + 'averagephasegain.tbl'

        for band, spwlist in band2spw.items():
            self._do_avgphasegaincal(avgpgain, refAnt,
                                     ktypecaltable=ktypecaltable, bpcaltable=bpcaltable, refantmode=refantmode,
                                     spw=','.join(spwlist))

        # In case any antenna is flagged by this process, unflag all solutions
        # in this gain table (if an antenna does exist or has bad solutions from
        # other steps, it will be flagged by those gain tables).

        self._do_unflag(avgpgain)
        self._do_applycal(ktypecaltable=ktypecaltable,
                          bpcaltable=bpcaltable, avgphasegaincaltable=avgpgain, interp=interp)

        # ---------------------------------------------------

        calMs = 'finalcalibrators.ms'
        isdir = os.path.isdir(calMs)
        if isdir:
            shutil.rmtree(calMs)
        split_result = self._do_split(calMs, '')  # , ','.join(spwlist))

        # Run setjy execution per band
        all_sejy_result = self._doall_setjy(calMs)

        LOG.info("Using power-law fits results from fluxscale.")
        for fs_result in self.inputs.context.evla['msinfo'][m.name].fluxscale_result:
            powerfit_setjy = self._do_powerfitsetjy(calMs, fs_result)

        phaseshortgaincaltable = tableprefix + str(stage_number) + '_6.' + 'phaseshortgaincal.tbl'
        finalampgaincaltable = tableprefix + str(stage_number) + '_7.' + 'finalampgaincal.tbl'
        finalphasegaincaltable = tableprefix + str(stage_number) + '_8.' + 'finalphasegaincal.tbl'

        for band, spwlist in band2spw.items():
            new_gain_solint1 = self.inputs.context.evla['msinfo'][m.name].new_gain_solint1[band]
            phaseshortgaincal_results = self._do_calibratorgaincal(calMs, phaseshortgaincaltable,
                                                                   new_gain_solint1, 3.0, 'p', [''], refAnt,
                                                                   refantmode=refantmode, spw=','.join(spwlist))

            gain_solint2 = self.inputs.context.evla['msinfo'][m.name].gain_solint2[band]
            finalampgaincal_results = self._do_calibratorgaincal(calMs, finalampgaincaltable, gain_solint2, 5.0,
                                                                 'ap', [phaseshortgaincaltable], refAnt,
                                                                 refantmode=refantmode, spw=','.join(spwlist))

            finalphasegaincal_results = self._do_calibratorgaincal(calMs, finalphasegaincaltable, gain_solint2,
                                                                   3.0, 'p', [finalampgaincaltable], refAnt,
                                                                   refantmode=refantmode, spw=','.join(spwlist))

        tablesToAdd = [(ktypecaltable, '', ''), (bpcaltable, 'linear,linearflag', ''),
                       (avgpgain, '', ''), (finalampgaincaltable, '', ''),
                       (finalphasegaincaltable, '', '')]
        # tablesToAdd = [(table, interp, gainfield) for table, interp, gainfield in tablesToAdd]

        callist = []
        for addcaltable, interp, gainfield in tablesToAdd:
            LOG.info("Finalcals stage:  Adding " + addcaltable + " to callibrary.")
            calto = callibrary.CalTo(self.inputs.vis)
            calfrom = callibrary.CalFrom(gaintable=addcaltable, interp=interp, calwt=False,
                                         caltype='finalcal', gainfield=gainfield)
            calapp = callibrary.CalApplication(calto, calfrom)
            callist.append(calapp)
            self.pool.append(calapp)
            self.final.append(calapp)

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, phaseshortgaincaltable,\
               finalampgaincaltable, finalphasegaincaltable, flaggedSolnApplycalbandpass, flaggedSolnApplycaldelay

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

    def _do_gtype_delaycal(self, caltable: str =None, refAnt: str = None, spwlist: List[str] = []) -> bool:
        """Perform a G-Type delay calibration with CASA task gaincal

        Args:
            caltable(str): Name of the caltable to be created
            refAnt(str): Antenna values to use as reference antennas - 'ea01,ea24,...'
            spwlist(List): List of string values for spws pertaining to the particular band - ['0', '1', '2', ...]

        Returns:
            Boolean

        """
        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

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
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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
                           refAnt: str = None, spw: str = '') -> bool:
        """Perform a K-Type delay calibration with CASA task gaincal

        Args:
            caltable(str): Name of the caltable to be created
            addcaltable(str):  String name of table to temporarily be added to the gaincal gaintable parameter
            refAnt(str): Antenna values to use as reference antennas - 'ea01,ea24,...'
            spw(str): csv string values for spws pertaining to the particular band - '0,1,2,3,4,5,6'

        Returns:
            Boolean

        """

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

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
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'K',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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

    def _do_gtype_bpdgains(self, caltable: str, addcaltable: str = None, solint: str = 'int',
                           refAnt: str = None, spwlist: List[str] = []) -> bool:
        """Perform a G-Type cal with CASA task gaincal on the bp'd gaintable

        Args:
            caltable(str): Name of the caltable to be created
            addcaltable(str):  String name of table to temporarily be added to the gaincal gaintable parameter
            solint(str):  String value for solint keyword of CASA task gaincal
            refAnt(str): Antenna values to use as reference antennas - 'ea01,ea24,...'
            spwlist(List): List of string values for spws pertaining to the particular band - ['0', '1', '2', ...]

        Returns:
            Boolean

        """

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
        bandpass_scan_select_string = self.inputs.context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        bpdgains_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_bpass_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': '',
                              'solint': solint,
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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

    def _do_avgphasegaincal(self, caltable: str, refAnt: str, ktypecaltable: str = None,
                            bpcaltable: str = None, refantmode: str = 'flex',
                            spw: str = '') -> bool:
        """Perform a G-Type cal with CASA task gaincal for average phase

        Args:
            caltable(str): Name of the caltable to be created
            refAnt(str): Antenna values to use as reference antennas - 'ea01,ea24,...'
            ktypecaltable(str):  K cal table
            bpcaltable(str):  BP caltable
            refantmode(str):  Default of flex
            spw(str):  spw string
        """

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        bandpass_field_select_string = self.inputs.context.evla['msinfo'][m.name].bandpass_field_select_string
        bandpass_scan_select_string = self.inputs.context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpcaltable)

        avgphasegaincal_task_args = {'vis': self.inputs.vis,
                                     'caltable': caltable,
                                     'field': '',
                                     'spw': spw,
                                     'selectdata': True,
                                     'uvrange': '',
                                     'scan': '',
                                     'solint': 'inf',
                                     'combine': 'scan',
                                     'preavg': -1.0,
                                     'refant': refAnt.lower(),
                                     'minblperant': minBL_for_cal,
                                     'minsnr': 1.0,
                                     'solnorm': False,
                                     'gaintype': 'G',
                                     'smodel': [],
                                     'calmode': 'p',
                                     'append': append,
                                     'docallib': False,
                                     'gaintable': AllCalTables,
                                     'gainfield': [''],
                                     'interp': [''],
                                     'spwmap': [],
                                     'parang': self.parang,
                                     'refantmode': refantmode}

        bpscanslist = list(map(int, bandpass_scan_select_string.split(',')))
        scanobjlist = m.get_scans(scan_id=bpscanslist)
        allfieldidlist = []
        for scanobj in scanobjlist:
            fieldobj, = scanobj.fields
            if str(fieldobj.id) not in allfieldidlist:
                allfieldidlist.append(str(fieldobj.id))

        # See vlascanheuristics - only use the first bandpass calibrator
        fieldidlist = [fieldid for fieldid in allfieldidlist if fieldid in bandpass_field_select_string]

        for fieldidstring in fieldidlist:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            avgphasegaincal_task_args['field'] = fieldidstring
            avgphasegaincal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                avgphasegaincal_task_args['append'] = True

            job = casa_tasks.gaincal(**avgphasegaincal_task_args)

            self._executor.execute(job)

        return True

    def _do_unflag(self, gaintable: str):
        """Execute flagdata with mode unflag

        Args:
            gaintable(str):  Name of table for parameter vis

        Returns:
            Executed job

        """

        task_args = {'vis': gaintable,
                     'mode': 'unflag',
                     'action': 'apply',
                     'display': '',
                     'flagbackup': False,
                     'savepars': False}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self, ktypecaltable: str = None, bpcaltable: str = None,
                     avgphasegaincaltable: str = None, interp: str = None, spw: str = ''):
        """Run CASA task applycal

        Args:
            ktypecaltable(str): output from K-type gaincal
            bpcaltable(str): BP caltable to use
            avgphasegaincaltable(str):  gain table
            interp(str): applycal CASA task keyword
            spw(str): csv string values for spws pertaining to the particular band - '0,1,2,3,4,5,6'

        Returns:
            Executed job

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpcaltable)
        AllCalTables.append(avgphasegaincaltable)

        ntables = len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': spw,
                              'intent': '',
                              'selectdata': True,
                              'scan': calibrator_scan_select_string,
                              'docallib': False,
                              'gaintable': AllCalTables,
                              'gainfield': [''],
                              'interp': [interp],
                              'spwmap': [],
                              'calwt': [False] * ntables,
                              'parang': self.parang,
                              'applymode': 'calflagstrict',
                              'flagbackup': True}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)

    def _do_split(self, calMs: str, spw: str = ''):
        """Split off the corrected data column / cal scans

        Args:
            calMs(str): name of output MS
            spw(str):  spw selection

        Returns:
            Executed job

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        task_args = {'vis': m.name,
                     'outputvis': calMs,
                     'datacolumn': 'corrected',
                     'keepmms': True,
                     'field': '',
                     'spw': spw,
                     'width': 1,
                     'antenna': '',
                     'timebin': '0s',
                     'timerange': '',
                     'scan': calibrator_scan_select_string,
                     'intent': '',
                     'array': '',
                     'uvrange': '',
                     'correlation': '',
                     'observation': '',
                     'keepflags': False}

        job = casa_tasks.split(**task_args)

        return self._executor.execute(job)

    def _doall_setjy(self, calMs: str) -> bool:
        """Execute setjy with the appropriate model image

        Args:
            calMS: calibrator MS

        Returns:
            Boolean

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        field_spws = m.get_vla_field_spws()
        spw2band = m.get_vla_spw2band()

        standard_source_names, standard_source_fields = standard_sources(calMs)

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
                        except:
                            LOG.info('Unable to get band from spw id - using reference frequency instead')
                            EVLA_band = find_EVLA_band(reference_frequency)

                        LOG.info("Center freq for spw " + str(spw.id) + " = " + str(
                            reference_frequency) + ", observing band = " + EVLA_band)

                        model_image = standard_source_names[i] + '_' + EVLA_band + '.im'

                        LOG.info(
                            "Setting model for field " + str(myfield) + " spw " + str(spw.id) + " using " + model_image)

                        # Double check, but the fluxdensity=-1 should not matter since
                        #  the model image take precedence
                        try:
                            job = self._do_setjy(calMs, str(myfield), str(spw.id), model_image)
                            jobs.append(job)
                            # result.measurements.update(setjy_result.measurements)
                        except Exception:
                            # something has gone wrong, return an empty result
                            LOG.warning(
                                "SetJy issue with field id=" + str(job.kw['field']) + " and spw=" + str(job.kw['spw']))

                    LOG.info("Merging flux scaling operation for setjy jobs for " + self.inputs.vis)
                    jobs_and_components = utils.merge_jobs(jobs, casa_tasks.setjy, merge=('spw',))
                    for job, _ in jobs_and_components:
                        try:
                            self._executor.execute(job)
                        except Exception:
                            LOG.warning(
                                "SetJy issue with field id=" + str(job.kw['field']) + " and spw=" + str(job.kw['spw']))

        return True

    def _do_setjy(self, calMs: str, field: str, spw: str, model_image: str) -> bool:
        """Execute CASA task setjy

        Args:
            field(str):  field id
            spw(str): spw id
            model_image(str):  Model name on disk

        Returns:
            Job to execute

        """

        try:
            task_args = {'vis': calMs,
                         'field': field,
                         'spw': spw,
                         'selectdata': False,
                         'model': model_image,
                         'listmodels': False,
                         'scalebychan': True,
                         'fluxdensity': -1,
                         'standard': standard.Standard()(field),
                         'usescratch': True}

            job = casa_tasks.setjy(**task_args)

            return job
        except Exception as e:
            LOG.info(str(e))
            return None

    def _do_powerfitsetjy(self, calMs: str, fluxscale_result: Dict):
        """Execute setjy with results from fluxscale

        Args:
            calMs(str): input name of calibrator MS
            fluxscale_result(Dict): output result from CASA task fluxscale

        Returns:
            Boolean

        """

        LOG.info("Setting power-law fit in the model column")

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        # fluxscale_result = self.inputs.context.evla['msinfo'][m.name].fluxscale_result
        dictkeys = list(fluxscale_result.keys())
        keys_to_remove = ['freq', 'spwName', 'spwID']
        dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

        for fieldid in dictkeys:
            jobs_calMs = []

            spws = list(fluxscale_result['spwID'])
            scispws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]
            newspws = [str(spwint) for spwint in list(set(scispws) & set(spws))]

            try:
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

                # job = casa_tasks.setjy(**task_args)
                jobs_calMs.append(casa_tasks.setjy(**task_args))

            except Exception as e:
                LOG.info(e)

            # merge identical jobs into one job with a multi-spw argument
            LOG.info("Merging setjy jobs for finalcalibrators.ms")
            jobs_and_components_calMs = utils.merge_jobs(jobs_calMs, casa_tasks.setjy, merge=('spw',))
            for job, _ in jobs_and_components_calMs:
                self._executor.execute(job)

        return True

    def _do_calibratorgaincal(self, calMs: str, caltable: str, solint: str, minsnr: float, calmode: str,
                              gaintablelist: List[str], refAnt: str,
                              refantmode: str = 'flex', spw: str = '') -> bool:
        """Execute the CASA task gaincal with the calibrator MS using a provided list of gaintables

        Args:
            calMs(str): input name of calibrator MS
            caltable(str):  output caltable
            solint(str):  gaincal solution interval
            minsnr(float):  Min SNR
            calmode(str):   Specified mode
            gaintablelist(List):  String list of tables, if any
            refAnt(str): csv string of reference antennas
            refantmode(str):  Default of flex
            spw(str):  spw string

        Returns:
            Boolean

        """

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s} ".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        scanlist = [int(scan) for scan in calibrator_scan_select_string.split(',')]
        scanids_perband = ','.join([str(scan.id) for scan in m.get_scans(scan_id=scanlist, spw=spw)])

        minBL_for_cal = m.vla_minbaselineforcal()

        task_args = {'vis': calMs,
                     'caltable': caltable,
                     'field': '',
                     'spw': spw,
                     'intent': '',
                     'selectdata': False,
                     'solint': solint,
                     'combine': 'scan',
                     'preavg': -1.0,
                     'refant': refAnt.lower(),
                     'minblperant': minBL_for_cal,
                     'minsnr': minsnr,
                     'solnorm': False,
                     'gaintype': 'G',
                     'smodel': [],
                     'calmode': calmode,
                     'append': append,
                     'gaintable': gaintablelist,
                     'gainfield': [''],
                     'interp': [''],
                     'spwmap': [],
                     'parang': self.parang,
                     'uvrange': '',
                     'refantmode': refantmode}

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


