import functools
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag
from pipeline.hif.tasks import gaincal
from pipeline.infrastructure.refantflag import identify_fully_flagged_antennas_from_flagcmds, \
    mark_antennas_for_refant_update, aggregate_fully_flagged_antenna_notifications, FullyFlaggedAntennasNotification
from typing import List, Set

LOG = infrastructure.get_logger(__name__)


class PolcalflagResults(basetask.Results):
    def __init__(self):
        super(PolcalflagResults, self).__init__()

        self.vis = None
        self.cafresult = None
        self.plots = dict()

        # Set of antennas that should be moved to the end of the refant list.
        self.refants_to_demote: Set[str] = set()

        # Set of entirely flagged antennas that should be removed from refants.
        self.refants_to_remove: Set[str] = set()

        # further information about entirely flagged antennas used in QA scoring
        self.fully_flagged_antenna_notifications: List[FullyFlaggedAntennasNotification] = []

        # records callibrary files used in applycal calls
        self.callib_map = {}

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        # Update reference antennas for MS.
        ms = context.observing_run.get_ms(name=self.vis)
        ms.update_reference_antennas(ants_to_demote=self.refants_to_demote,
                                     ants_to_remove=self.refants_to_remove)

    def __repr__(self):
        return 'PolcalflagResults'


class PolcalflagInputs(vdp.StandardInputs):

    minsnr = vdp.VisDependentProperty(default=2.0)
    phaseupsolint = vdp.VisDependentProperty(default='int')
    refant = vdp.VisDependentProperty(default='')
    solint = vdp.VisDependentProperty(default='inf')

    @vdp.VisDependentProperty
    def intent(self):
        # By default, this task will run for POLARIZATION, POLANGLE, and POLLEAKAGE
        # intents.
        return 'POLARIZATION,POLANGLE,POLLEAKAGE'

    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_polcalflag')
@task_registry.set_casa_commands_comment('Flag polarization calibrator outliers.')
class Polcalflag(basetask.StandardTaskTemplate):
    Inputs = PolcalflagInputs

    def prepare(self):

        inputs = self.inputs

        # Initialize results.
        result = PolcalflagResults()

        # Store the vis in the result
        result.vis = inputs.vis

        # Create a shortcut to the plotting function that pre-supplies the inputs and context
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Check for any polarization intents
        eb_intents = inputs.context.observing_run.get_ms(inputs.vis).intents
        if 'POLARIZATION' not in eb_intents and 'POLANGLE' not in eb_intents and 'POLLEAKAGE' not in eb_intents:
            LOG.info('No polarization intents found.')
            return result

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-polcalflag" flagging state')
        flag_backup_name_prepcf = 'before_pcflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_prepcf)
        self._executor.execute(task)

        # Since this task is run before hifa_timegaincal, we need to compute
        # local phase and amplitude cal tables for the polarization intents.

        # Create phase caltable and merge it into the local context.
        # PIPE-1154: always use combine='' for phase solves of polarisation
        # calibrators, with no explicit spw mapping nor override for interp.
        LOG.info('Compute phase gaincal table.')
        self._do_gaincal(intent=inputs.intent, gaintype='G', calmode='p', combine='', solint=inputs.phaseupsolint,
                         minsnr=inputs.minsnr, refant=inputs.refant, merge=True)

        # Create amplitude caltable and merge it into the local context.
        # CAS-10491: for scan-based (solint='inf') amplitude solves that
        # will be applied to the calibrator, set interp to 'nearest'.
        LOG.info('Compute amplitude gaincal table.')
        amp_interp = 'nearest,linear' if inputs.solint == 'inf' else 'linear,linear'
        self._do_gaincal(intent=inputs.intent, gaintype='T', calmode='a', combine='', solint=inputs.solint,
                         minsnr=inputs.minsnr, refant=inputs.refant, interp=amp_interp, merge=True)

        # Ensure that any flagging applied to the MS by this applycal is
        # reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags
            LOG.info('Applying pre-existing cal tables.')
            callib_map = self._do_applycal(merge=False)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(callib_map)

            # Create back-up of flags after applycal but before correctedampflag.
            LOG.info('Creating back-up of "after_pcflag_applycal" flagging state')
            flag_backup_name_after_pcflag_applycal = 'after_pcflag_applycal'
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='save',
                                          versionname=flag_backup_name_after_pcflag_applycal)
            self._executor.execute(task)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify polarization calibrator outliers to flag.')

            # Call correctedampflag for the polarization calibrator intent.
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent=inputs.intent)
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # Store correctedampflag result
            result.cafresult = cafresult

            # Get new flag commands
            cafflags = cafresult.flagcmds()

            # If new outliers were identified...make "after flagging" plots that
            # include both applycal flags and correctedampflag flags
            if cafflags:
                # Make "after calibration, after flagging" plots for the weblog
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(flagcmds=cafflags, suffix='after')

                # Restore the "after_pcflag_applycal" backup of the flagging
                # state, so that the "before plots" only show things needing
                # to be flagged by correctedampflag
                LOG.info('Restoring back-up of "after_pcflag_applycal" flagging state.')
                task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore',
                                              versionname=flag_backup_name_after_pcflag_applycal)
                self._executor.execute(task)

            # Make "after calibration, before flagging" plots for the weblog
            LOG.info('Creating "after calibration, before flagging" plots')
            result.plots['before'] = plot_fn(flagcmds=cafflags, suffix='before')

        finally:
            # Restore the "pre-polcalflag" backup of the flagging state.
            LOG.info('Restoring back-up of "pre-polcalflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_prepcf)
            self._executor.execute(task)

        # If new outliers were identified...
        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(context=inputs.context, vis=inputs.vis, table=inputs.vis, inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            _ = self._executor.execute(fstask)

            # Mark antennas that need to be demoted or removed from the reference antenna list.
            result = self._identify_refants_to_update(result)

        return result

    def analyse(self, results):
        return results

    def _do_applycal(self, merge):
        inputs = self.inputs

        # SJW - always just one job
        ac_intents = [inputs.intent]

        applycal_tasks = []
        for intent in ac_intents:
            task_inputs = applycal.IFApplycalInputs(inputs.context, vis=inputs.vis, intent=intent, flagsum=False,
                                                    flagbackup=False)
            task = applycal.SerialIFApplycal(task_inputs)
            applycal_tasks.append(task)

        # as there's just one job
        callib_map = {}
        for task in applycal_tasks:
            applycal_result = self._executor.execute(task, merge=merge)
            callib_map.update(applycal_result.callib_map)

        return callib_map

    def _do_gaincal(self, caltable=None, intent=None, gaintype='G', calmode=None, combine=None, solint=None,
                    antenna=None, uvrange='', minsnr=None, refant=None, minblperant=None, interp=None, append=None,
                    merge=True):
        inputs = self.inputs
        ms = inputs.ms

        # Get the science spws and scans for specified intent.
        request_spws = ms.get_spectral_windows()
        targeted_scans = ms.get_scans(scan_intent=intent)

        # boil it down to just the valid spws for these fields and request
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}

        for spectral_spec, tuning_spw_ids in utils.get_spectralspec_to_spwid_map(scan_spws).items():
            tuning_spw_str = ','.join([str(i) for i in sorted(tuning_spw_ids)])
            LOG.info('Processing spectral spec {}, spws {}'.format(spectral_spec, tuning_spw_str))

            scans_with_data = ms.get_scans(spw=tuning_spw_str, scan_intent=intent)
            if not scans_with_data:
                LOG.info('No data to process for spectral spec {}. Continuing...'.format(spectral_spec))
                continue

            # of the fields that we are about to process, does any field have
            # multiple intents?
            mixed_intents = False
            fields_in_scans = {fld for scan in scans_with_data for fld in scan.fields}
            singular_intents = frozenset(intent.split(','))
            if len(singular_intents) > 1:
                for field in fields_in_scans:
                    intents_to_scans = {si: ms.get_scans(scan_intent=si, field=field.id, spw=tuning_spw_str)
                                        for si in singular_intents}
                    valid_intents = [k for k, v in intents_to_scans.items() if v]
                    if len(valid_intents) > 1:
                        mixed_intents = True
                        break

            if mixed_intents and solint == 'inf':
                # multiple items, one for each intent. Each intent will result
                # in a separate job.
                # Make sure data for the intent exists (PIPE-367)
                intents_to_scans = {si: [scan for scan in scans_with_data if si in scan.intents]
                                    for si in singular_intents}
                valid_intents = [k for k, v in intents_to_scans.items() if v]
                task_intents = valid_intents

            else:
                # one item, and hence one job, with 'PHASE,BANDPASS,...'
                task_intents = [','.join(singular_intents)]

            for intent in task_intents:
                # Initialize gaincal inputs.
                task_inputs = gaincal.GTypeGaincal.Inputs(
                    inputs.context,
                    vis=inputs.vis,
                    caltable=caltable,
                    intent=intent,
                    spw=tuning_spw_str,
                    solint=solint,
                    gaintype=gaintype,
                    calmode=calmode,
                    minsnr=minsnr,
                    combine=combine,
                    refant=refant,
                    antenna=antenna,
                    uvrange=uvrange,
                    minblperant=minblperant,
                    solnorm=False,
                    append=append)

                # if we need to generate multiple caltables, make the caltable
                # names unique by inserting the intent to prevent them overwriting
                # each other
                if len(task_intents) > 1:
                    root, ext = os.path.splitext(task_inputs.caltable)
                    task_inputs.caltable = '{!s}.{!s}{!s}'.format(root, intent, ext)

                # Modify output table filename to append "prelim".
                if task_inputs.caltable.endswith('.tbl'):
                    task_inputs.caltable = task_inputs.caltable[:-4] + '.prelim.tbl'
                else:
                    task_inputs.caltable += '.prelim'

                # Initialize and execute gaincal task.
                task = gaincal.GTypeGaincal(task_inputs)
                result = self._executor.execute(task)

                # Modify the result so that this caltable is only applied to
                # the intent from which the calibration was derived, and modify
                # the interp if provided.
                calapp_overrides = dict(intent=intent)
                if interp:
                    calapp_overrides['interp'] = interp

                # Create modified CalApplication and replace CalApp in result
                # with this new one.
                modified = callibrary.copy_calapplication(result.final[0], **calapp_overrides)
                result.pool[0] = modified
                result.final[0] = modified

                # If requested, merge result into the local context.
                if merge:
                    result.accept(inputs.context)

    def _identify_refants_to_update(self, result):
        """Updates the Polcalflag result with lists of "bad" and "poor"
        antennas, for reference antenna update.

        Identifies "bad" antennas as those that got flagged in all spws
        (entire timestamp) which are to be removed from the reference antenna
        list.

        Identifies "poor" antennas as those that got flagged in at least
        one spw, but not all, which are to be moved to the end of the reference
        antenna list.

        :param result: PolcalflagResults object
        :return: PolcalflagResults object
        """
        # Get the MS object.
        ms = self.inputs.context.observing_run.get_ms(name=self.inputs.vis)

        # Set of all spws affected by this flagging task.
        all_spwids = set(map(int, result.cafresult.inputs['spw'].split(',')))

        # Identify antennas to demote as refant.
        fully_flagged_antennas = identify_fully_flagged_antennas_from_flagcmds(ms, result.cafresult.flagcmds())

        # Update result to mark antennas for demotion/removal as refant.
        result = mark_antennas_for_refant_update(ms, result, fully_flagged_antennas, all_spwids)

        # Aggregate the list of fully flagged antennas by intent, field and spw for subsequent QA scoring
        result.fully_flagged_antenna_notifications = aggregate_fully_flagged_antenna_notifications(
            fully_flagged_antennas, all_spwids)

        return result

    @staticmethod
    def _get_ant_id_to_name_dict(ms):
        """
        Return dictionary with antenna ID mapped to antenna name.
        If no unique antenna name can be assigned to each antenna ID,
        then return empty dictionary.

        :param ms: MeasurementSet
        :return: dictionary
        """
        # Create an antenna id-to-name translation dictionary.
        antenna_id_to_name = {ant.id: ant.name
                              for ant in ms.antennas
                              if ant.name.strip()}

        # Check that each antenna ID is represented by a unique non-empty
        # name, by testing that the unique set of antenna names is same
        # length as list of IDs. If not, then unset the translation
        # dictionary to revert back to flagging by ID.
        if len(set(antenna_id_to_name.values())) != len(ms.antennas):
            LOG.info('No unique name available for each antenna ID:'
                     ' flagging by antenna ID instead of by name.')
            antenna_id_to_name = {}

        return antenna_id_to_name


def create_plots(inputs, context, flagcmds, suffix=''):
    """
    Return amplitude vs time plots for the given data column.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis)
    output_dir = context.output_dir

    # Amplitude vs time plots
    amp_time_plots = AmpVsXChart('time', context, output_dir, calto, suffix=suffix).plot()

    # Amplitude vs UV distance plots shall contain only the fields that were flagged
    flagged_spws = {flagcmd.spw for flagcmd in flagcmds}
    spw_field_dict = {int(spw): ','.join(sorted({flagcmd.field for flagcmd in flagcmds if flagcmd.spw==spw})) for spw in flagged_spws}
    amp_uvdist_plots = AmpVsXChart('uvdist', context, output_dir, calto, suffix=suffix, field=spw_field_dict).plot()

    for spw, field in spw_field_dict.items():
        LOG.info(f'Fields flagged for {inputs.vis} spw {spw}: {field}')

    return {'time': amp_time_plots, 'uvdist': amp_uvdist_plots}


class AmpVsXChart(applycal_displays.SpwSummaryChart):
    """
    Plotting class that creates an amplitude vs X plot for each spw, where X
    is given as a constructor argument.
    """
    def __init__(self, xaxis, context, output_dir, calto, **overrides):
        plot_args = {
            'ydatacolumn': 'corrected',
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'coloraxis': 'corr',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0]
        }
        plot_args.update(**overrides)

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp',
                                          intent='POLARIZATION,POLANGLE,POLLEAKAGE', **plot_args)
