import functools

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag

LOG = infrastructure.get_logger(__name__)


class TargetflagResults(basetask.Results):
    def __init__(self):
        super(TargetflagResults, self).__init__()
        self.cafresult = None
        self.plots = {}
        self.callib_map = {}

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'TargetflagResults'


class TargetflagInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_targetflag')
@task_registry.set_casa_commands_comment('Flag target source outliers.')
class Targetflag(basetask.StandardTaskTemplate):
    Inputs = TargetflagInputs

    def prepare(self):

        inputs = self.inputs

        # Initialize results.
        result = TargetflagResults()

        cafflags = []

        # create a shortcut to the plotting function that pre-supplies the inputs and context
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Check for any target intents
        eb_intents = inputs.context.observing_run.get_ms(inputs.vis).intents
        if 'TARGET' not in eb_intents:
            LOG.info('No target intents found.')
            return result

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-targetflag" flagging state')
        flag_backup_name_pretgtf = 'before_tgtflag'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name_pretgtf)
        self._executor.execute(task)

        # Ensure that any flagging applied to the MS by this applycal are
        # reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags to the MS. Should typically include Tsys,
            # bandpass, and spwphaseup tables, as well as WVR if 12-m antennas
            # are present, and antpos if any position corrections were made.
            LOG.info('Applying pre-existing cal tables.')
            acinputs = applycal.IFApplycalInputs(
                context=inputs.context, vis=inputs.vis, intent='TARGET', flagsum=False, flagbackup=False)
            actask = applycal.SerialIFApplycal(acinputs)
            acresult = self._executor.execute(actask, merge=True)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(acresult.callib_map)

            # Create back-up of flags after applycal but before correctedampflag.
            LOG.info('Creating back-up of "after_tgtflag_applycal" flagging state')
            flag_backup_name_after_tgtflag_applycal = 'after_tgtflag_applycal'
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name_after_tgtflag_applycal)
            self._executor.execute(task)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify target source outliers to flag.')

            # This task is called by the framework for each EB in the vis list.

            # Call correctedampflag for the target intent. For that intent it
            # will loop over spw and field IDs to inspect the flags individually
            # per mosaic pointing.
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent='TARGET', niter=1)
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

                # Restore the "after_tgtflag_applycal" backup of the flagging
                # state, so that the "before plots" only show things needing
                # to be flagged by correctedampflag
                LOG.info('Restoring back-up of "after_tgtflag_applycal" flagging state.')
                task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_after_tgtflag_applycal)
                self._executor.execute(task)
                # Make "after calibration, before flagging" plots for the weblog
                LOG.info('Creating "after calibration, before flagging" plots')
                result.plots['before'] = plot_fn(flagcmds=cafflags, suffix='before')

        finally:
            # Restore the "pre-targetflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-targetflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_pretgtf)
            self._executor.execute(task)

        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(context=inputs.context, vis=inputs.vis, table=inputs.vis, inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            _ = self._executor.execute(fstask)

        return result

    def analyse(self, results):
        return results

def create_plots(inputs, context, flagcmds, suffix=''):
    """
    Return amplitude vs time plots for the given data column.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param flagcmds: list of FlagCmd objects
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
            'coloraxis': 'field',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0]
        }
        plot_args.update(**overrides)

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp', intent='TARGET',
                                          **plot_args)
