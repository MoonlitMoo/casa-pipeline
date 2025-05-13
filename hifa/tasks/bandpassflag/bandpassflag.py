import functools
from typing import Dict

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag
from pipeline.hif.tasks import gaincal
from pipeline.hifa.tasks import bandpass
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.refantflag import identify_fully_flagged_antennas_from_flagcmds, \
    mark_antennas_for_refant_update, aggregate_fully_flagged_antenna_notifications
from .resultobjects import BandpassflagResults
from ..bandpass.almaphcorbandpass import ALMAPhcorBandpassInputs

__all__ = [
    'BandpassflagInputs',
    'BandpassflagResults',
    'Bandpassflag'
]

LOG = infrastructure.get_logger(__name__)


class BandpassflagInputs(ALMAPhcorBandpassInputs):
    """
    BandpassflagInputs defines the inputs for the Bandpassflag pipeline task.
    """
    # Lower sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # catchNegativeOutliers['scalardiff']
    antblnegsig = vdp.VisDependentProperty(default=3.4)

    # Upper sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # flag_nsigma['scalardiff']
    antblpossig = vdp.VisDependentProperty(default=3.2)

    # Lower sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # relaxationSigma
    antnegsig = vdp.VisDependentProperty(default=4.0)

    # Upper sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # positiveSigmaAntennaBased
    antpossig = vdp.VisDependentProperty(default=4.6)

    # Maximum number of iterations to evaluate flagging heuristics.
    niter = vdp.VisDependentProperty(default=2)

    # Relaxed value to set the threshold scaling factor to under certain
    # conditions; equivalent to:
    # relaxationFactor
    relaxed_factor = vdp.VisDependentProperty(default=2.0)

    # Threshold for maximum fraction of timestamps that are allowed
    # to contain outliers; equivalent to:
    # checkForAntennaBasedBadIntegrations
    tmantint = vdp.VisDependentProperty(default=0.063)

    # Initial threshold for maximum fraction of "bad baselines" over "all
    # baselines" that an antenna may be a part of; equivalent to:
    # tooManyBaselinesFraction
    tmbl = vdp.VisDependentProperty(default=0.175)

    # Initial threshold for maximum fraction of "outlier timestamps" over
    # "total timestamps" that a baseline may be a part of; equivalent to:
    # tooManyIntegrationsFraction
    tmint = vdp.VisDependentProperty(default=0.085)

    # Solutions below this SNR are rejected
    minsnr = vdp.VisDependentProperty(default=2.0)

    def __init__(self, context, output_dir=None, vis=None, caltable=None, intent=None, field=None, spw=None,
                 antenna=None, hm_phaseup=None, phaseupsolint=None, phaseupbw=None, phaseupsnr=None, phaseupnsols=None,
                 hm_bandpass=None, solint=None, maxchannels=None, evenbpints=None, bpsnr=None, minbpsnr=None, bpnsols=None,
                 combine=None, refant=None, minblperant=None, minsnr=None, solnorm=None, antnegsig=None, antpossig=None,
                 tmantint=None, tmint=None, tmbl=None, antblnegsig=None, antblpossig=None, relaxed_factor=None,
                 niter=None, mode='channel', hm_auto_fillgaps=None):
        super().__init__(
            context, output_dir=output_dir, vis=vis, caltable=caltable, intent=intent, field=field, spw=spw,
            antenna=antenna, hm_phaseup=hm_phaseup, phaseupsolint=phaseupsolint, phaseupbw=phaseupbw,
            phaseupsnr=phaseupsnr, phaseupnsols=phaseupnsols, hm_bandpass=hm_bandpass, solint=solint,
            maxchannels=maxchannels, evenbpints=evenbpints, bpsnr=bpsnr, minbpsnr=minbpsnr, bpnsols=bpnsols,
            combine=combine, refant=refant, minblperant=minblperant, minsnr=minsnr, solnorm=solnorm, mode=mode,
            hm_auto_fillgaps=hm_auto_fillgaps
        )

        # flagging parameters
        self.antnegsig = antnegsig
        self.antpossig = antpossig
        self.tmantint = tmantint
        self.tmint = tmint
        self.tmbl = tmbl
        self.antblnegsig = antblnegsig
        self.antblpossig = antblpossig
        self.relaxed_factor = relaxed_factor
        self.niter = niter

    def as_dict(self):
        # temporary workaround to hide uvrange from Input Parameters accordion
        d = super(BandpassflagInputs, self).as_dict()
        if 'uvrange' in d:
            del d['uvrange']
        return d


@task_registry.set_equivalent_casa_task('hifa_bandpassflag')
@task_registry.set_casa_commands_comment(
    'This task performs a preliminary bandpass solution and temporarily applies it, then calls hif_correctedampflag to'
    ' evaluate the flagging heuristics, looking for outlier visibility points by statistically examining the scalar'
    ' difference of the corrected amplitudes minus model amplitudes, and then flagging those outliers. The philosophy'
    ' is that only outlier data points that have remained outliers after calibration will be flagged. Note that the'
    ' phase of the data is not assessed.'
)
class Bandpassflag(basetask.StandardTaskTemplate):
    Inputs = BandpassflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results for current MS.
        result = BandpassflagResults(inputs.vis)

        # Create a shortcut to the plotting function that pre-supplies the inputs and context.
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-bandpassflag" flagging state')
        flag_backup_name_prebpf = 'before_bpflag'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name_prebpf)
        self._executor.execute(task)

        # Run a preliminary standard phaseup and bandpass calibration:
        # Create inputs for bandpass task.
        LOG.info('Creating preliminary phased-up bandpass calibration.')
        bpinputs = bandpass.ALMAPhcorBandpass.Inputs(
            context=inputs.context, vis=inputs.vis, caltable=inputs.caltable,
            field=inputs.field, intent=inputs.intent, spw=inputs.spw,
            antenna=inputs.antenna, hm_phaseup=inputs.hm_phaseup,
            phaseupbw=inputs.phaseupbw, phaseupsnr=inputs.phaseupsnr,
            phaseupnsols=inputs.phaseupnsols,
            phaseupsolint=inputs.phaseupsolint, hm_bandpass=inputs.hm_bandpass,
            solint=inputs.solint, maxchannels=inputs.maxchannels,
            evenbpints=inputs.evenbpints, bpsnr=inputs.bpsnr, minbpsnr=inputs.minbpsnr,
            bpnsols=inputs.bpnsols, combine=inputs.combine,
            refant=inputs.refant, solnorm=inputs.solnorm,
            minblperant=inputs.minblperant, minsnr=inputs.minsnr, hm_auto_fillgaps=inputs.hm_auto_fillgaps)
        # Create and execute bandpass task.
        bptask = bandpass.ALMAPhcorBandpass(bpinputs)
        bpresult = self._executor.execute(bptask)

        # Add the phase-up table produced by the bandpass task to the
        # callibrary in the local context.
        LOG.debug('Adding preliminary phase-up and bandpass tables to temporary context.')
        for prev_result in bpresult.preceding:
            for calapp in prev_result:
                inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        # Accept the bandpass result into the local context so as to add the
        # bandpass table to the callibrary.
        bpresult.accept(inputs.context)

        # Do amplitude solve on scan interval.
        LOG.info('Create preliminary amplitude gaincal table.')
        gacalinputs = gaincal.GTypeGaincal.Inputs(context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                                                  gaintype='T', antenna='', calmode='a', solint='inf')
        gacaltask = gaincal.GTypeGaincal(gacalinputs)
        gacalresult = self._executor.execute(gacaltask)

        # CAS-10491: for scan-based amplitude solves that will be applied
        # to the calibrator, set interp to 'nearest' => modify result from
        # gaincal to update interp before merging into the local context.
        self._mod_last_interp(gacalresult.pool[0], 'nearest,linear')
        self._mod_last_interp(gacalresult.final[0], 'nearest,linear')
        LOG.debug('Adding preliminary amplitude caltable to temporary context.')
        gacalresult.accept(inputs.context)

        # Ensure that any flagging applied to the MS by applycal are reverted
        # at the end, even in the case of exceptions.
        try:
            # Apply all caltables registered in the callibrary in the local
            # context to the MS.
            LOG.info('Applying pre-existing caltables and preliminary phase-up, bandpass, and amplitude caltables.')
            acinputs = applycal.IFApplycalInputs(context=inputs.context, vis=inputs.vis, field=inputs.field,
                                                 intent=inputs.intent, flagsum=False, flagbackup=False)
            actask = applycal.SerialIFApplycal(acinputs)
            acresult = self._executor.execute(actask)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(acresult.callib_map)

            # Create "after calibration, before flagging" plots for the weblog.
            LOG.info('Creating "after calibration, before flagging" plots')
            result.plots['before'] = plot_fn(suffix='before')

            # Call Correctedampflag to find and flag amplitude outliers.
            LOG.info('Running correctedampflag to identify outliers to flag.')
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                field=inputs.field, spw=inputs.spw, antnegsig=inputs.antnegsig,
                antpossig=inputs.antpossig, tmantint=inputs.tmantint,
                tmint=inputs.tmint, tmbl=inputs.tmbl,
                antblnegsig=inputs.antblnegsig,
                antblpossig=inputs.antblpossig,
                relaxed_factor=inputs.relaxed_factor, niter=inputs.niter)
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # If flags were found, create the "after calibration, after
            # flagging" plots for the weblog.
            cafflags = cafresult.flagcmds()
            if cafflags:
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(suffix='after')

        finally:
            # Restore the "pre-bandpassflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-bandpassflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_prebpf)
            self._executor.execute(task)

        # Store flagging task result.
        result.cafresult = cafresult

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

    def analyse(self, result):
        return result

    def _mod_last_interp(self, l, interp):
        l.calfrom[-1] = self._copy_with_interp(l.calfrom[-1], interp)

    @staticmethod
    def _copy_with_interp(old_calfrom, interp):
        return callibrary.CalFrom(gaintable=old_calfrom.gaintable,
                                  gainfield=old_calfrom.gainfield,
                                  interp=interp,
                                  spwmap=old_calfrom.spwmap,
                                  caltype=old_calfrom.caltype,
                                  calwt=old_calfrom.calwt)

    def _identify_refants_to_update(self, result):
        """Updates the Bandpassflag result with lists of "bad" and "poor"
        antennas, for reference antenna update.

        Identifies "bad" antennas as those that got flagged in all spws
        (entire timestamp) which are to be removed from the reference antenna
        list.

        Identifies "poor" antennas as those that got flagged in at least
        one spw, but not all, which are to be moved to the end of the reference
        antenna list.

        :param result: BandpassflagResults object
        :return: BandpassflagResults object
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
    def _get_ant_id_to_name_dict(ms) -> Dict[int, str]:
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

    @staticmethod
    def _copy_calfrom_with_gaintable(old_calfrom, gaintable):
        return callibrary.CalFrom(gaintable=gaintable,
                                  gainfield=old_calfrom.gainfield,
                                  interp=old_calfrom.interp,
                                  spwmap=old_calfrom.spwmap,
                                  caltype=old_calfrom.caltype,
                                  calwt=old_calfrom.calwt)


def create_plots(inputs, context, suffix=''):
    """
    Return amplitude vs time and amplitude vs UV distance plots for the given
    data column.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis, spw=inputs.spw)
    output_dir = context.output_dir

    amp_uvdist_plots = AmpVsXChart('uvdist', context, output_dir, calto, suffix=suffix).plot()
    amp_time_plots = AmpVsXChart('time', context, output_dir, calto, suffix=suffix).plot()

    return {'uvdist': amp_uvdist_plots, 'time': amp_time_plots}


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

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp', intent='BANDPASS',
                                          **plot_args)
