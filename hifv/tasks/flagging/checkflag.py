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

from .displaycheckflag import checkflagSummaryChart

LOG = infrastructure.get_logger(__name__)

# CHECKING FLAGGING OF ALL CALIBRATORS
# use rflag mode of flagdata


class CheckflagInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    checkflagmode = vdp.VisDependentProperty(default='')
    overwrite_modelcol = vdp.VisDependentProperty(default=False)

    def __init__(self, context, vis=None, checkflagmode=None, overwrite_modelcol=None, growflags=None):
        super(CheckflagInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.checkflagmode = checkflagmode
        self.overwrite_modelcol = overwrite_modelcol
        if growflags is None:
            self.growflags = self.checkflagmode not in ('', 'semi')
        else:
            self.growflags = growflags


class CheckflagResults(basetask.Results):
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

        super(CheckflagResults, self).__init__()

        self.jobs = jobs
        self.results = results
        self.summaries = summaries
        self.vis_averaged = vis_averaged
        self.dataselect = dataselect

    def __repr__(self):
        s = 'Checkflag (rflag mode) results:\n'
        for job in self.jobs:
            s += '%s performed. Statistics to follow?' % str(job)
        return s


@task_registry.set_equivalent_casa_task('hifv_checkflag')
class Checkflag(basetask.StandardTaskTemplate):
    Inputs = CheckflagInputs

    def prepare(self):

        LOG.info("Checkflag task: {}".format(repr(self.inputs.checkflagmode)))

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
        if self.inputs.checkflagmode not in ('bpd-vla', 'allcals-vla', 'target-vla',
                                             'semi', '', 'bpd', 'allcals', 'target',
                                             'bpd-vlass', 'allcals-vlass', 'target-vlass', 'vlass-imaging'):
            LOG.warning("Unrecognized option for checkflagmode. RFI flagging not executed.")
            return CheckflagResults(summaries=summaries)

        fieldselect, scanselect, intentselect, columnselect = self._select_data()

        # PIPE-1335: abort if both fieldselect and scanselect are empty strings.
        if not (fieldselect or scanselect):
            LOG.warning("No scans with selected intent(s) from checkflagmode={!r}. RFI flagging not executed.".format(
                self.inputs.checkflagmode))
            return CheckflagResults(summaries=summaries)

        # abort if the data selection criteria lead to NUll selection
        if not mssel_valid(self.inputs.vis, field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws):
            LOG.warning("Null data selection from checkflagmode={!r}. RFI flagging not executed.".format(
                self.inputs.checkflagmode))
            return CheckflagResults(summaries=summaries)

        if self.inputs.checkflagmode == 'vlass-imaging':
            LOG.info('Checking for model column')
            self._check_for_modelcolumn()

        # PIPE-502/995: run the before-flagging summary in most checkflagmodes, including 'vlass-imaging'
        # PIPE-757: skip in all VLASS calibration checkflagmodes: 'bpd-vlass', 'allcals-vlass', and 'target-vlass'
        if self.inputs.checkflagmode not in ('bpd-vlass', 'allcals-vlass', 'target-vlass'):
            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='before',
                                      field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws)
            summarydict = self._executor.execute(job)
            if summarydict is not None:
                summaries.append(summarydict)

        # PIPE-502/995/987: save before-flagging time-averged MS and its amp-related stats for weblog
        if self.inputs.checkflagmode in ('allcals-vla', 'bpd-vla', 'target-vla',
                                         'bpd', 'allcals',
                                         'bpd-vlass', 'allcals-vlass', 'vlass-imaging'):
            vis_averaged_before, vis_ampstats_before = self._create_timeavg_ms(suffix='before')
            vis_averaged.update(before=vis_averaged_before, before_amp=vis_ampstats_before)
            plotms_dataselect = {'field':  fieldselect,
                                 'scan': scanselect,
                                 'spw': self.sci_spws,
                                 'intent': intentselect,
                                 'ydatacolumn': 'data',
                                 'correlation': self.corrstring}
            vis_averaged['plotms_dataselect'] = plotms_dataselect
            # plots['before'] = self._create_summaryplots(suffix='before', plotms_args=plot_selectdata)

        # PIPE-987: backup flagversion before rfi flagging
        now_str = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='save',
                                     versionname='hifv_checkflag_{}_stage{}_{}'.format(
                                         self.inputs.checkflagmode, self.inputs.context.task_counter, now_str),
                                     comment='flagversion before running hifv_checkflag()',
                                     merge='replace')
        self._executor.execute(job)

        # decide on if we use cont.dat for target-vla
        use_contdat = False
        if self.inputs.checkflagmode == 'target-vla':
            fielddict = contfile_to_spwsel(self.inputs.vis, self.inputs.context)
            if fielddict != {}:
                LOG.info('cont.dat file present.  Using VLA Spectral Line Heuristics for checkflagmode=target-vla.')
                use_contdat = True

        if use_contdat:
            # cont.dat is present for target-vla, do the field-by-field flagging
            for field in fielddict:
                fieldselect_cont = utils.fieldname_for_casa(field)
                self.do_rfi_flag(fieldselect=fieldselect_cont, scanselect=scanselect,
                                 intentselect=intentselect, spwselect=fielddict[field])
                # PIPE-1342: do a second pass of rflag in the 'target-vla' mode (equivalent to running hifv_targetvla)
                if self.inputs.checkflagmode == 'target-vla':
                    self.do_vla_targetflag(fieldselect=fieldselect_cont, scanselect=scanselect,
                                           intentselect=intentselect, spwselect=fielddict[field])
        else:
            # all other situations
            self.do_rfi_flag(fieldselect=fieldselect, scanselect=scanselect,
                             intentselect=intentselect, spwselect=self.sci_spws)
            # PIPE-1342: do a second pass of rflag in the 'target-vla' mode (equivalent to running hifv_targetvla)
            if self.inputs.checkflagmode == 'target-vla':
                self.do_vla_targetflag(fieldselect='', scanselect=scanselect,
                                       intentselect=intentselect, spwselect='')

        # PIPE-502/757/995: get after-flagging statistics, NOT for bpd-vlass and allcals-vlass
        if self.inputs.checkflagmode not in ('bpd-vlass', 'allcals-vlass'):
            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='after',
                                      field=fieldselect, scan=scanselect, intent=intentselect, spw=self.sci_spws)
            summarydict = self._executor.execute(job)
            if summarydict is not None:
                summaries.append(summarydict)

        # PIPE-502/995/987: save after-flagging time-averaged MS and its amp-related stats for weblog
        if self.inputs.checkflagmode in ('allcals-vla', 'bpd-vla', 'target-vla',
                                         'bpd', 'allcals',
                                         'bpd-vlass', 'allcals-vlass', 'vlass-imaging'):
            vis_averaged_after, vis_ampstats_after = self._create_timeavg_ms(suffix='after')
            vis_averaged.update(after=vis_averaged_after, after_amp=vis_ampstats_after)

        checkflag_result = CheckflagResults()
        checkflag_result.summaries = summaries
        checkflag_result.vis_averaged = vis_averaged
        checkflag_result.dataselect = {'field': fieldselect,
                                       'scan': scanselect,
                                       'intent': intentselect,
                                       'spw': self.sci_spws}

        return checkflag_result

    def analyse(self, results):
        return results

    def _do_extendflag(self, mode='extend', field=None,  scan=None, intent='', spw='',
                       ntime='scan', extendpols=True, flagbackup=False,
                       growtime=100.0, growfreq=60.0, growaround=False,
                       flagneartime=False, flagnearfreq=False):

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'scan': scan,
                     'intent': intent,
                     'spw': spw,
                     'ntime': ntime,
                     'combinescans': False,
                     'extendpols': extendpols,
                     'growtime': growtime,
                     'growfreq': growfreq,
                     'growaround': growaround,
                     'flagneartime': flagneartime,
                     'flagnearfreq': flagnearfreq,
                     'action': 'apply',
                     'display': '',
                     'extendflags': False,
                     'flagbackup': flagbackup,
                     'savepars': False}

        job = casa_tasks.flagdata(**task_args)
        result = self._executor.execute(job)

        return job, result

    def _do_tfcropflag(self, mode='tfcrop', field=None, correlation=None, scan=None, intent='', spw='',
                       ntime=0.45, datacolumn='corrected', flagbackup=False,
                       freqcutoff=3.0, timecutoff=4.0, savepars=True,
                       extendflags=False):

        # pass 'extendflags' to flagdata(mode='tfcrop') if boolean
        if isinstance(extendflags, bool):
            extendflags_tfcrop = extendflags
        else:
            extendflags_tfcrop = False

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'correlation': correlation,
                     'scan': scan,
                     'intent': intent,
                     'spw': spw,
                     'ntime': ntime,
                     'combinescans': False,
                     'datacolumn': datacolumn,
                     'freqcutoff': freqcutoff,
                     'timecutoff': timecutoff,
                     'freqfit': 'line',
                     'flagdimension': 'freq',
                     'action': 'apply',
                     'display': '',
                     'extendflags': extendflags_tfcrop,
                     'flagbackup': flagbackup,
                     'savepars': savepars}

        job = casa_tasks.flagdata(**task_args)
        result = self._executor.execute(job)

        # a seperate flagdata(mode='extent',...) call if 'extendflags' is a dictionary
        if isinstance(extendflags, dict):
            self._do_extendflag(field=field, scan=scan, intent=intent, spw=spw, **extendflags)

        return

    def _do_rflag(self, mode='rflag', field=None, correlation=None, scan=None, intent='', spw='',
                  ntime='scan', datacolumn='corrected', flagbackup=False, timedevscale=4.0,
                  freqdevscale=4.0, timedev='', freqdev='', savepars=True,
                  calcftdev=True, useheuristic=True, ignore_sefd=False,
                  extendflags=False):
        """Run rflag heuristics.

        calcftdev: a single-pass 'rflag' (False) or a 'calculate'->'apply' aips-style two-pass operation (True)
        useheuristics: run the freqdev/timedev threshold heuristics (True), or act as a pass-through (False)
                       only affect operation when calcftdev is True.
        extendflags: set the "extendflags" plan.
            True/False: toggle the 'extendflags' argument in the 'rflag' flagdata() call
            Dictionary: do flag extension with a seperate flagdata() call
        """
        # pass 'extendflags' to flagdata(mode='rflag') if boolean
        if isinstance(extendflags, bool):
            extendflags_rflag = extendflags
        else:
            extendflags_rflag = False

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'correlation': correlation,
                     'scan': scan,
                     'intent': intent,
                     'spw': spw,
                     'ntime': ntime,
                     'combinescans': False,
                     'datacolumn': datacolumn,
                     'winsize': 3,
                     'timedevscale': timedevscale,
                     'freqdevscale': freqdevscale,
                     'timedev': timedev,
                     'freqdev': freqdev,
                     'action': 'apply',
                     'display': '',
                     'extendflags': extendflags_rflag,
                     'flagbackup': flagbackup,
                     'savepars': savepars}

        if calcftdev:
            task_args['action'] = 'calculate'

            job = casa_tasks.flagdata(**task_args)
            jobresult = self._executor.execute(job)

            if jobresult is None:
                LOG.debug("This is likely a dryrun test! Proceed with timedev/freqdev=''.")
                ftdev = None
            else:
                if jobresult['nreport'] == 0:
                    LOG.info("Null data selection for the Rflag sequence. Continue.")
                    return
                if useheuristic:
                    ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
                    rflagdev = RflagDevHeuristic(ms, ignore_sefd=ignore_sefd)
                    ftdev = rflagdev(jobresult['report0'])
                else:
                    ftdev = jobresult['report0']

            if ftdev is not None:
                task_args['timedev'] = ftdev['timedev']
                task_args['freqdev'] = ftdev['freqdev']
            task_args['action'] = 'apply'

        job = casa_tasks.flagdata(**task_args)
        jobresult = self._executor.execute(job)

        # a seperate flagdata(mode='extent',...) call if 'extendflags' is a dictionary
        if isinstance(extendflags, dict):
            self._do_extendflag(field=field, scan=scan, intent=intent, spw=spw, **extendflags)

        return

    def do_rfi_flag(self, fieldselect='', scanselect='', intentselect='', spwselect=''):
        """Do RFI flagging using multiple passes of rflag/tfcrop/extend."""

        rflag_standard, tfcrop_standard, growflag_standard = self._select_rfi_standard()
        flagbackup = False
        calcftdev = True

        # set ignore_sedf=True/flagbackup=False to maintain the same behavior as the deprecated do_*vlass() methods
        ignore_sefd = self.inputs.checkflagmode in ('target-vlass', 'bpd-vlass', 'allcals-vlass', 'vlass-imaging')

        # set calcftdev=False to turn off the new heuristic for some older modes
        calcftdev = self.inputs.checkflagmode not in ('semi', '', 'target', 'bpd', 'allcals')

        if rflag_standard is not None:

            for datacolumn, correlation, scale, extendflags in rflag_standard:
                if '_' in correlation:
                    polselect = correlation.split('_')[1]
                    if not (polselect in self.corr_type_string or polselect == self.corrstring):
                        continue
                    if not mssel_valid(self.inputs.vis, field=fieldselect, spw=spwselect, correlation=polselect,
                                       scan=scanselect, intent=intentselect):
                        continue
                if datacolumn == 'residual':
                    # PIPE-1256: determine if we can use the 'RESIDUAL' column in the 'bpd-vlass/vla' mode.
                    #   The usage of 'RESIDUAL' is only valid if the model of bpd source(s) is properly filled *AND*
                    #   the first-order gain/passband calibration has been applied in 'CORRECTED'.
                    #   Here we check each field from the data selection and see if they all meet the above requirements.
                    #   We only examine the parallel hand amplitude:
                    #       - setjy() has only I models for 3C48/3C138/3C286/3C147.
                    #       - setjy(fluxdensity=-1) will fill the cross-hand with zero values.
                    LOG.info("Determining if we can use the RESIDUAL column for rflag:")
                    if self._is_model_setjy():
                        LOG.info("  MODEL_DATA is present and none of the model(s) from selected data is a 1Jy point source.")
                    else:
                        datacolumn = 'corrected'
                        correlation = correlation.replace('REAL_', 'ABS_')
                        LOG.info("  MODEL_DATA s not found or the model(s) from selected data contains 1Jy point source(s).")
                    LOG.info("  Use the {} column and correlation = {!r} for rflag".format(datacolumn.upper(), correlation))
                method_args = {'mode': 'rflag',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'intent': intentselect,
                               'spw': spwselect,
                               'ntime': 'scan',
                               'timedevscale': scale,
                               'freqdevscale': scale,
                               'datacolumn': datacolumn,
                               'flagbackup': flagbackup,
                               'savepars': False,
                               'calcftdev': calcftdev,
                               'useheuristic': True,
                               'ignore_sefd': ignore_sefd,
                               'extendflags': extendflags}

                self._do_rflag(**method_args)

        if tfcrop_standard is not None:

            for datacolumn, correlation, tfcropThreshMultiplier, extendflags in tfcrop_standard:
                if '_' in correlation:
                    polselect = correlation.split('_')[1]
                    if not (polselect in self.corr_type_string or polselect == self.corrstring):
                        continue
                    if not mssel_valid(self.inputs.vis, field=fieldselect, spw=spwselect, correlation=polselect,
                                       scan=scanselect, intent=intentselect):
                        continue
                timecutoff = 4. if tfcropThreshMultiplier is None else tfcropThreshMultiplier
                freqcutoff = 3. if tfcropThreshMultiplier is None else tfcropThreshMultiplier
                method_args = {'mode': 'tfcrop',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'intent': intentselect,
                               'spw': spwselect,
                               'timecutoff': timecutoff,
                               'freqcutoff': freqcutoff,
                               'ntime': self.tint,
                               'datacolumn': datacolumn,
                               'flagbackup': flagbackup,
                               'savepars': False,
                               'extendflags': extendflags}

                self._do_tfcropflag(**method_args)

        if growflag_standard is not None:
            self._do_extendflag(
                field=fieldselect, scan=scanselect, intent=intentselect, spw=spwselect, flagbackup=flagbackup, **growflag_standard)

        return

    def do_vla_targetflag(self, fieldselect='', scanselect='', intentselect='', spwselect=''):
        """"Perform a simple second 'rflag' pass.

        This method is equivalent to hifv_targetflag(intents='*TARGET*'), which is phasing out.
        See PIPE-1342.
        """

        task_args = {'vis': self.inputs.vis,
                     'mode': 'rflag',
                     'field': fieldselect,
                     'correlation': 'ABS_'+self.corrstring,
                     'scan': scanselect,
                     'intent': intentselect,
                     'spw': spwselect,
                     'ntime': 'scan',
                     'combinescans': False,
                     'datacolumn': 'corrected',
                     'winsize': 3,
                     'timedevscale': 4.0,
                     'freqdevscale': 4.0,
                     'action': 'apply',
                     'display': '',
                     'extendflags': False,
                     'flagbackup': False,
                     'savepars': True}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)

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
        columnselect = 'corrected'

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        msinfo = self.inputs.context.evla['msinfo'].get(ms.name, None)
        sci_spw_list = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        # Select Bandpass/Delay (BPD) calibrators
        if self.inputs.checkflagmode in ('bpd-vla', 'bpd-vlass', '', 'bpd'):
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
        if self.inputs.checkflagmode in ('allcals-vla', 'allcals-vlass', 'allcals'):
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
        if self.inputs.checkflagmode in ('target-vla', 'target-vlass', 'vlass-imaging', 'target'):
            fieldids = [field.id for field in ms.get_fields(intent='TARGET')]
            fieldselect = ','.join([str(fieldid) for fieldid in fieldids])
            intentselect = '*TARGET*'

        # select all calibrators
        if self.inputs.checkflagmode == 'semi':
            fieldselect = msinfo.calibrator_field_select_string
            scanselect = msinfo.calibrator_scan_select_string

        if self.inputs.checkflagmode == 'vlass-imaging':
            # use the 'data' column by default as 'vlass-imaging' is working on target-only MS.
            columnselect = 'data'

        LOG.debug('FieldSelect:  {}'.format(repr(fieldselect)))
        LOG.debug('ScanSelect:   {}'.format(repr(scanselect)))
        LOG.debug('IntentSelect: {}'.format(repr(intentselect)))
        LOG.debug('ColumnSelect: {}'.format(repr(columnselect)))

        return fieldselect, scanselect, intentselect, columnselect

    def _select_rfi_standard(self):
        """Set rflag data selection and threshold-multiplier in individual rflag iterations.

        Note from BK:
        Set up threshold multiplier values for calibrators and targets separately.
        Xpol are used for cross-hands, Ppol are used for parallel hands. As
        noted above, I'm still refining these values; I suppose they could be
        input parameters for the task, if needed.

        rflag_standard: list of tuple, with each tuple (a, b, c, d) describing the specifications of one self._do_rflag call:

                            a) data column selection
                            b) correlation selection
                            c) ftdev threshold multiplier
                            d) extendflag setting
                                Boolean (True/False):
                                    use the default basic extendflagging scheme; see the 'extendflags' subprameter of flagdata(mode='rflag'.
                                A dictionary (e.g. {'growtime':100,'growfreq':100,'')
                                    do extendflag using seperate flagdata(mode='extend',..) call following flagdata(model='rflag',extendflags=False,..)

        tfcrop_standard: list of tuple, with each tuple (a, b, c, d) describing the specifications of one self._do_rflag call:

                            a) data column selection
                            b) correlation selection
                            c) tfcrop threshold multiplier
                            d) extendflag setting
                                Boolean (True/False):
                                    use the default basic extendflagging scheme; see the 'extendflags' subprameter of flagdata(mode='rflag'.
                                A dictionary (e.g. {'growtime':100,'growfreq':100,'')
                                    do extendflag using seperate flagdata(mode='extend',..) call following flagdata(model='rflag',extendflags=False,..

        growflag_standrd: dictionary to specify the final optional "growflags".
        """
        rflag_standard = tfcrop_standard = growflag_standard = None

        if self.inputs.checkflagmode == 'bpd-vla':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            #           with an optional growflag step specified by the 'growflags' task argument
            rflag_standard = [('corrected', 'ABS_RL', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_RR', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_LL', 5.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            if self.inputs.growflags:
                growflag_standard = {'growtime': 100,
                                     'growfreq': 100,
                                     'growaround': True,
                                     'flagneartime': False,
                                     'flagnearfreq': True}

        if self.inputs.checkflagmode == 'bpd-vlass':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]
            growflag_standard = {'growtime': 100,
                                 'growfreq': 100,
                                 'growaround': True,
                                 'flagneartime': True,
                                 'flagnearfreq': True}

        if self.inputs.checkflagmode == 'allcals-vla':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            #           with an optional growflag step specified by the 'growflags' task argument
            rflag_standard = [('corrected', 'ABS_RL', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_RR', 5.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LL', 5.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            if self.inputs.growflags:
                growflag_standard = {'growtime': 100,
                                     'growfreq': 100,
                                     'growaround': True,
                                     'flagneartime': False,
                                     'flagnearfreq': True}

        if self.inputs.checkflagmode == 'allcals-vlass':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]
            growflag_standard = {'growtime': 100,
                                 'growfreq': 100,
                                 'growaround': True,
                                 'flagneartime': True,
                                 'flagnearfreq': True}

        if self.inputs.checkflagmode == 'target-vla':
            # PIPE-685: follow the VLASS flagging scheme described in CAS-11598
            # PIPE-987: disable growflags
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_RR', 4.5, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LL', 4.5, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]

        if self.inputs.checkflagmode == 'target-vlass':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_RR', 7.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LL', 7.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]

        if self.inputs.checkflagmode == 'vlass-imaging':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('data', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('data', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual_data', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual_data', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('data', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('data', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('data', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('data', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]

        if self.inputs.checkflagmode == 'target':
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_RR', 7.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_LL', 7.0, {'growtime': 100., 'growfreq': 60.})]
            tfcrop_standard = [('corrected', 'ABS_RL', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_RR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LL', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_RR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LL', None, {'growtime': 100., 'growfreq': 60.})]

        if self.inputs.checkflagmode == 'bpd':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('residual', 'REAL_RR', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('residual', 'REAL_LL', 4.0, {'growtime': 100., 'growfreq': 60.})]
            tfcrop_standard = [('corrected', 'ABS_RL', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_RR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LL', None, {'growtime': 100., 'growfreq': 60.})]
            if self.inputs.growflags:
                growflag_standard = {'growtime': 100,
                                     'growfreq': 100,
                                     'growaround': True,
                                     'flagneartime': True,
                                     'flagnearfreq': False}

        if self.inputs.checkflagmode == 'allcals':
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 60.}),
                              ('corrected', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 60.})]
            tfcrop_standard = [('corrected', 'ABS_RL', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_RR', None, {'growtime': 100., 'growfreq': 60.}),
                               ('corrected', 'ABS_LL', None, {'growtime': 100., 'growfreq': 60.})]
            if self.inputs.growflags:
                growflag_standard = {'growtime': 100,
                                     'growfreq': 100,
                                     'growaround': True,
                                     'flagneartime': True,
                                     'flagnearfreq': False}

        if self.inputs.checkflagmode in ('', 'semi'):
            rflag_standard = [('corrected', 'ABS_'+self.corrstring, 4.0, False)]
            if self.inputs.growflags:
                growflag_standard = {'growtime': 100,
                                     'growfreq': 100,
                                     'growaround': True,
                                     'flagneartime': True,
                                     'flagnearfreq': False}

        LOG.debug('rflag_standard:     {}'.format(repr(rflag_standard)))
        LOG.debug('tfcrop_standard:    {}'.format(repr(tfcrop_standard)))
        LOG.debug('growflag_standard:  {}'.format(repr(growflag_standard)))

        return rflag_standard, tfcrop_standard, growflag_standard

    def _check_for_modelcolumn(self):
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames() or self.inputs.overwrite_modelcol:
                LOG.info('Writing model data to {}'.format(ms.basename))
                imaging_parameters = set_add_model_column_parameters(self.inputs.context)
                job = casa_tasks.tclean(**imaging_parameters)
                tclean_result = self._executor.execute(job)
            else:
                LOG.info('Using existing MODEL_DATA column found in {}'.format(ms.basename))

    def _create_timeavg_ms(self, suffix='before'):

        stage_number = self.inputs.context.task_counter
        vis_averaged_name = [self.inputs.vis, 'hifv_checkflag', 's'+str(stage_number),
                             suffix, self.inputs.checkflagmode, 'averaged']
        vis_averaged_name = '.'.join(list(filter(None, vis_averaged_name)))

        LOG.info('Saving the time-averaged visibility of selected data to {}'.format(vis_averaged_name))
        LOG.debug('Estimating the amplitude range of unflagged averaged data for {} : {}'.format(vis_averaged_name, suffix))

        # do cross-scan averging for calibrator checkflagmodes
        if self.inputs.checkflagmode in ('target-vla', 'vlass-imaging'):
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

    def _create_summaryplots(self, suffix='before', plotms_args={}):
        """Preload the display class to generate before-flagging plot(s)."""
        summary_plots = {}
        results_tmp = basetask.ResultsList()
        results_tmp.inputs = self.inputs.as_dict()
        results_tmp.stage_number = self.inputs.context.task_counter
        results_tmp.plots = {}
        summary_plots = checkflagSummaryChart(
            self.inputs.context, results_tmp, suffix=suffix, plotms_args=plotms_args).plot()

        return summary_plots

    def _is_model_setjy(self):
        """Check the model column status of selected fields.

        return True, if the below requirements are met:
            - the model column is present.
            - none of selected field(s) contain a model of 1Jy point source at the phasecenter (in the parallel hands)
        """
        fieldselect, scanselect, intentselect, columnselect = self._select_data()
        is_model_setjy = True

        # set False if the MODEL column is not present.
        with casa_tools.TableReader(self.inputs.vis) as table:
            if 'MODEL_DATA' not in table.colnames():
                is_model_setjy = False

        if is_model_setjy:
            with casa_tools.MSReader(self.inputs.vis) as msfile:
                # we expect fieldselect is not an empty string here...
                for field in fieldselect.split(','):
                    staql = {'field': field, 'spw': self.sci_spws, 'scan': scanselect,
                             'scanintent': intentselect, 'polarization': '', 'uvdist': ''}
                    if msfile.msselect(staql, onlyparse=False):
                        vis_ampstats = msfile.statistics(field=field, scan=scanselect, intent=intentselect,
                                                         correlation='RR,LL', column='model',
                                                         complex_value='amp', useweights=False, useflags=False,
                                                         reportingaxes='', doquantiles=False,
                                                         timeaverage=False, timebin='0s', timespan='')
                        vis_ampstats = vis_ampstats['']
                        LOG.debug('checking the MODEL amplitude stats of field = {!r}:\n{!r}'.format(
                            field, vis_ampstats))
                        if vis_ampstats['min'] == 1 and vis_ampstats['max'] == 1:
                            is_model_setjy = False
                            break
                    msfile.reset()

        return is_model_setjy

