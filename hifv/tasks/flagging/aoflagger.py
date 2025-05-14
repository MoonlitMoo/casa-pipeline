import datetime
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hifv.heuristics import (RflagDevHeuristic, mssel_valid,
                                      set_add_model_column_parameters)
from pipeline.infrastructure import (casa_tasks, casa_tools, task_registry,
                                     utils)
from pipeline.infrastructure.contfilehandler import contfile_to_spwsel

from pipeline.infrastructure.utils import subprocess

LOG = infrastructure.get_logger(__name__)

# CHECKING FLAGGING OF ALL CALIBRATORS


class AoflaggerInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    flag_target = vdp.VisDependentProperty(default='')
    aoflagger_file = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, flag_target=None, aoflagger_file=None):
        super(AoflaggerInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.flag_target = flag_target
        self.aoflagger_file = aoflagger_file


class AoflaggerResults(basetask.Results):
    def __init__(self, jobs=None, results=None, summaries=None, vis_averaged=None, dataselect=None):

        if jobs is None:
            jobs = []
        if results is None:
            results = []
        if summaries is None:
            summaries = []
        if vis_averaged is None:
            vis_averaged = {}
        if dataselect is None:
            dataselect = {}

        super(AoflaggerResults, self).__init__()

        self.jobs = jobs
        self.results = results
        self.summaries = summaries
        self.vis_averaged = vis_averaged
        self.dataselect = dataselect

    def __repr__(self):
        s = 'Aoflagger results:\n'
        for job in self.jobs:
            s += '%s performed. Statistics to follow?' % str(job)
        return s


@task_registry.set_equivalent_casa_task('hifv_aoflagger')
class Aoflagger(basetask.StandardTaskTemplate):
    Inputs = AoflaggerInputs

    def prepare(self):

        LOG.info(f"Aoflagger task: {self.inputs.flag_target}, using {self.inputs.aoflagger_file}")

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        self.tint = ms.get_vla_max_integration_time()

        # a list of strings representing polarizations from science spws
        sci_spwlist = ms.get_spectral_windows(science_windows_only=True)
        sci_spwids = [spw.id for spw in sci_spwlist]
        pols_list = [ms.polarizations[dd.pol_id].corr_type_string for dd in ms.data_descriptions if dd.spw.id in sci_spwids]
        pols = [pol for pols in pols_list for pol in pols]
        self.corr_type_string = list(set(pols))

        # a string representing selected polarizations, only parallel hands
        # this is only preserved to maintain the existing behavior of checkflagmode=''/'semi'
        self.corrstring = ms.get_vla_corrstring()

        # a string representing science spws
        self.sci_spws = ','.join([str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True)])

        summaries = []      # Flagging statistics summaries for VLA QA scoring (CAS-10910/10916/10921)
        vis_averaged = {}   # Time-averaged MS and stats for summary plots


        # abort if the mode selection is not recognized
        # NOTE: Removed vlass ones
        if self.inputs.flag_target not in ('primary', 'secondary', 'science',
                                           'primary-corrected', 'secondary-corrected', 'science-corrected'):
            LOG.warning("Unrecognized option for flag_target. RFI flagging not executed.")
            return AoflaggerResults(summaries=summaries)

        fieldselect, scanselect, intentselect, columnselect = self._select_data()

        # PIPE-1335: abort if both fieldselect and scanselect are empty strings.
        if not (fieldselect or scanselect):
            LOG.warning("No scans with selected intent(s) from flag_target={!r}. RFI flagging not executed.".format(
                self.inputs.flag_target))
            return AoflaggerResults(summaries=summaries)

        # abort if the data selection criteria lead to NUll selection
        if not mssel_valid(self.inputs.vis, field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws):
            LOG.warning("Null data selection from flag_target={!r}. RFI flagging not executed.".format(
                self.inputs.flag_target))
            return AoflaggerResults(summaries=summaries)

        # PIPE-502/995: run the before-flagging summary in most checkflagmodes, including 'vlass-imaging'
        # PIPE-757: skip in all VLASS calibration checkflagmodes: 'bpd-vlass', 'allcals-vlass', and 'target-vlass'
        job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='before',
                                  field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws)
        summarydict = self._executor.execute(job)
        if summarydict is not None:
            summaries.append(summarydict)

        # PIPE-502/995/987: save before-flagging time-averged MS and its amp-related stats for weblog

        vis_averaged_before, vis_ampstats_before = self._create_timeavg_ms(suffix='before')
        vis_averaged.update(before=vis_averaged_before, before_amp=vis_ampstats_before)
        plotms_dataselect = {'field':  fieldselect,
                             'scan': scanselect,
                             'spw': self.sci_spws,
                             'intent': intentselect,
                             'ydatacolumn': 'data',
                             'correlation': self.corrstring}
        vis_averaged['plotms_dataselect'] = plotms_dataselect

        # PIPE-987: backup flagversion before rfi flagging
        now_str = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='save',
                                     versionname='hifv_aoflagger_{}_stage{}_{}'.format(
                                         self.inputs.flag_target, self.inputs.context.task_counter, now_str),
                                     comment='flagversion before running hifv_aoflagger()',
                                     merge='replace')
        self._executor.execute(job)


        # Do the actual flagging
        if self.inputs.flag_target in ('primary', 'secondary', 'science'):
            datacolumn = 'DATA'
        else:
            datacolumn = 'CORRECTED_DATA'

        self.do_rfi_flag(fieldselect=fieldselect, datacolumn=datacolumn)

        # PIPE-502/757/995: get after-flagging statistics, NOT for bpd-vlass and allcals-vlass
        job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='after',
                                  field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws)
        summarydict = self._executor.execute(job)
        if summarydict is not None:
            summaries.append(summarydict)

        # PIPE-502/995/987: save after-flagging time-averaged MS and its amp-related stats for weblog
        vis_averaged_after, vis_ampstats_after = self._create_timeavg_ms(suffix='after')
        vis_averaged.update(after=vis_averaged_after, after_amp=vis_ampstats_after)

        checkflag_result = AoflaggerResults()
        checkflag_result.summaries = summaries
        checkflag_result.vis_averaged = vis_averaged
        checkflag_result.dataselect = {'field': fieldselect,
                                       'scan': scanselect,
                                       'intent': intentselect,
                                       'spw': self.sci_spws}

        return checkflag_result

    def analyse(self, results):
        return results

    def do_rfi_flag(self, fieldselect='', datacolumn='DATA'):
        """Do RFI flagging using multiple passes of rflag/tfcrop/extend."""

        if datacolumn == 'CORRECTED_DATA':
            LOG.info("UV subtracting vis datacolumn is corrected and we want to run on the residuals")
            job = casa_tasks.uvsub(vis=self.inputs.vis)
            self._executor.execute(job)
        cmd = f"aoflagger -fields {fieldselect} -column {datacolumn} -strategy {self.inputs.aoflagger_file} {self.inputs.vis}"

        LOG.info(f"Running '{cmd}' via subprocess")
        result = subprocess.run(cmd)
        LOG.info(f"Finished with exit code {result}")

        if datacolumn == 'CORRECTED_DATA':
            LOG.info("Reversing UV subtraction on vis")
            job = casa_tasks.uvsub(vis=self.inputs.vis, reverse=True)
            self._executor.execute(job)

        # Remove the flag version if it already exists
        job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='list')
        result = self._executor.execute(job)
        flag_versions = {v['name'] for k, v in result.items() if k != "MS"}
        if self.inputs.flag_target in flag_versions:
            LOG.info(f"Removing old flag version {self.inputs.flag_target}")
            job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='delete', versionname=self.inputs.flag_target)
            self._executor.execute(job)
        # Save current flag state
        LOG.info(f"Saving current flag state as {self.inputs.flag_target}")
        job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='save', versionname=self.inputs.flag_target)
        self._executor.execute(job)

    def _select_data(self):
        """Selects data according to the specified checkflagmode.

        This method constructs selection strings for fields, scans, and intents based on the
        `checkflagmode` input. It also determines the appropriate data column to use
        ('corrected' or 'data').

        Returns:
            tuple: A tuple containing:
                - field_select_string (str): Comma-separated list of field IDs.
                - scan_select_string (str): Comma-separated list of scan IDs.
                - intent_select_string (str): String representing the intent selection.
                - column_select_string (str): String representing the data column selection.
        """

        # start with default
        fieldselect = scanselect = intentselect = ''
        columnselect = 'corrected' if "corrected" in self.inputs.flag_target else 'data'

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        msinfo = self.inputs.context.evla['msinfo'].get(ms.name, None)
        sci_spw_list = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        # If primary, select Bandpass/Delay (BPD) calibrators
        if self.inputs.flag_target in ('primary', 'primary-corrected'):
            fieldselect = msinfo.checkflagfields
            # PIPE-1335: down-select scans using both msinfo.checkflagfields and msinfo.testgainscans.
            # msinfo.testgainscans alone may include scans of fields not in msinfo.checkflagfields
            # (e.g., second calibrators with bandpass or delay intents). See the 21A-311 case from PIPE-1335.
            if msinfo.testgainscans:
                testpbd_scans = {
                    s.id for s in ms.get_scans(
                        scan_id=list(map(int, msinfo.testgainscans.split(','))),
                        field=msinfo.checkflagfields, spw=sci_spw_list)}
            else:
                testpbd_scans = set()
            scanselect = ','.join(map(str, sorted(testpbd_scans)))

        # Select all calibrators excluding BPD calibrators
        if self.inputs.flag_target in ('secondary', 'secondary-corrected'):
            if msinfo.testgainscans:
                testpbd_scans = {
                    s.id for s in ms.get_scans(
                        scan_id=list(map(int, msinfo.testgainscans.split(','))),
                        field=msinfo.checkflagfields, spw=sci_spw_list)}
            else:
                testpbd_scans = set()
            allcals_scans = {
                s.id for s in ms.get_scans(
                    scan_id=list(map(int, msinfo.calibrator_scan_select_string.split(','))),
                    field=msinfo.calibrator_field_select_string, spw=sci_spw_list)}
            scanselect = ','.join(map(str, sorted(allcals_scans-testpbd_scans)))

            # PIPE-1335: only construct the field selection string if the scan selection string is not empty.
            # Note that an exclusion based on msinfo.checkflagfield might accidentaly reject fields observed
            # in different intents across scans. See the 21B-136 case from PIPE-1335.
            if scanselect:
                fields_in_scans = {
                    f.id for scan in ms.get_scans(
                        scan_id=list(allcals_scans - testpbd_scans),
                        field=msinfo.calibrator_field_select_string, spw=sci_spw_list)
                    for f in scan.fields}
                fieldselect = ','.join(map(str, sorted(fields_in_scans)))

        # select targets
        if self.inputs.flag_target in ('science', 'science-corrected'):
            fieldids = [field.id for field in ms.get_fields(intent='TARGET')]
            fieldselect = ','.join([str(fieldid) for fieldid in fieldids])
            intentselect = '*TARGET*'

        LOG.debug('FieldSelect:  {}'.format(repr(fieldselect)))
        LOG.debug('ScanSelect:   {}'.format(repr(scanselect)))
        LOG.debug('IntentSelect: {}'.format(repr(intentselect)))
        LOG.debug('ColumnSelect: {}'.format(repr(columnselect)))

        return fieldselect, scanselect, intentselect, columnselect

    def _create_timeavg_ms(self, suffix='before'):

        stage_number = self.inputs.context.task_counter
        vis_averaged_name = [self.inputs.vis, 'hifv_aoflagger', 's' + str(stage_number),
                             suffix, self.inputs.flag_target, 'averaged']
        vis_averaged_name = '.'.join(list(filter(None, vis_averaged_name)))

        LOG.info('Saving the time-averaged visibility of selected data to {}'.format(vis_averaged_name))
        LOG.debug('Estimating the amplitude range of unflagged averaged data for {} : {}'.format(vis_averaged_name, suffix))

        # do cross-scan averging for calibrator checkflagmodes
        if self.inputs.flag_target in ('science'):
            timespan = ''
        else:
            timespan = 'scan'

        fieldselect, scanselect, intentselect, columnselect = self._select_data()

        shutil.rmtree(vis_averaged_name, ignore_errors=True)
        job = casa_tasks.mstransform(vis=self.inputs.vis, outputvis=vis_averaged_name,
                                     field=fieldselect, spw=self.sci_spws, scan=scanselect,
                                     intent=intentselect, datacolumn=columnselect,
                                     correlation=self.corrstring,
                                     timeaverage=True, timebin='1e8', timespan=timespan,
                                     keepflags=False, reindex=False)
        job.execute()

        with casa_tools.MSReader(vis_averaged_name) as msfile:
            vis_ampstats = msfile.statistics(column='data', complex_value='amp', useweights=False, useflags=True,
                                             reportingaxes='', doquantiles=False,
                                             timeaverage=False, timebin='0s', timespan='')

        return vis_averaged_name, vis_ampstats
