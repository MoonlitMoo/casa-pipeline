"""
Created on 01 Jun 2017

@author: Vincent Geers (UKATC)
"""
import functools
import os.path
from typing import Dict, Optional, Tuple

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import MeasurementSet
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag
from pipeline.hif.tasks import gaincal
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.callibrary import CalTo
from pipeline.infrastructure.launcher import Context
from .resultobjects import GfluxscaleflagResults

__all__ = [
    'GfluxscaleflagInputs',
    'GfluxscaleflagResults',
    'Gfluxscaleflag'
]

LOG = infrastructure.get_logger(__name__)


class GfluxscaleflagInputs(vdp.StandardInputs):
    """
    GfluxscaleflagInputs defines the inputs for the Gfluxscaleflag pipeline task.
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

    @vdp.VisDependentProperty
    def field(self):
        # By default, return the fields corresponding to the input
        # intents.
        fieldids = [field.name
                    for field in self.ms.get_fields(intent=self.intent)]
        return ','.join(fieldids)

    @vdp.VisDependentProperty
    def intent(self):
        # By default, this task will run for AMPLITUDE, PHASE, CHECK, and
        # DIFFGAIN* intents.
        intents_to_flag = 'AMPLITUDE,PHASE,CHECK,DIFFGAINREF,DIFFGAINSRC'

        # Check if any of the AMPLITUDE intent fields were also used for
        # BANDPASS, in which case it has already been flagged by
        # hifa_bandpassflag, and this task will just do PHASE, CHECK, and
        # DIFFGAIN* fields. This assumes that there will only be 1 field for
        # BANDPASS and 1 field for AMPLITUDE (which can be the same), which is
        # valid as of Cycle 5.
        for field in self.ms.get_fields(intent='AMPLITUDE'):
            for fieldintent in field.intents:
                if 'BANDPASS' in fieldintent:
                    intents_to_flag = 'PHASE,CHECK,DIFFGAINREF,DIFFGAINSRC'
        return intents_to_flag

    minsnr = vdp.VisDependentProperty(default=2.0)
    niter = vdp.VisDependentProperty(default=2)
    phaseupsolint = vdp.VisDependentProperty(default='int')
    refant = vdp.VisDependentProperty(default='')

    # Relaxed value to set the threshold scaling factor to under certain
    # conditions; equivalent to:
    # relaxationFactor
    relaxed_factor = vdp.VisDependentProperty(default=2.0)

    solint = vdp.VisDependentProperty(default='inf')

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(
            science_windows_only=True)
        return ','.join([str(spw.id) for spw in science_spws])

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

    def __init__(self, context, output_dir=None, vis=None, intent=None, field=None, spw=None, solint=None,
                 phaseupsolint=None, minsnr=None, refant=None, antnegsig=None, antpossig=None, tmantint=None,
                 tmint=None, tmbl=None, antblnegsig=None, antblpossig=None, relaxed_factor=None, niter=None):
        super(GfluxscaleflagInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.field = field
        self.intent = intent
        self.spw = spw

        # gaincal parameters
        self.solint = solint
        self.phaseupsolint = phaseupsolint
        self.minsnr = minsnr
        self.refant = refant

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


@task_registry.set_equivalent_casa_task('hifa_gfluxscaleflag')
@task_registry.set_casa_commands_comment(
    'This task calls hif_correctedampflag to evaluate flagging heuristics on the phase calibrator and flux calibrator, '
    'looking for outlier visibility points by statistically examining the scalar difference of corrected amplitudes '
    'minus model amplitudes, and flagging those outliers.'
)
class Gfluxscaleflag(basetask.StandardTaskTemplate):
    Inputs = GfluxscaleflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results.
        result = GfluxscaleflagResults()

        # Store the vis in the result
        result.vis = inputs.vis
        result.plots = dict()

        # create a shortcut to the plotting function that pre-supplies the inputs and context
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-gfluxscaleflag" flagging state')
        flag_backup_name_pregfsf = 'before_gfsflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_pregfsf)
        self._executor.execute(task)

        # Create phase caltable(s) and merge into the local context.
        self._do_phasecal()

        # Create amplitude caltable and merge it into the local context.
        # CAS-10491: for scan-based (solint='inf') amplitude solves that
        # will be applied to the calibrator, set interp to 'nearest'.
        LOG.info('Compute amplitude gaincal table.')
        amp_interp = 'nearest,linear' if inputs.solint == 'inf' else 'linear,linear'
        self._do_gaincal(intent=inputs.intent, gaintype='T', calmode='a', combine='', solint=inputs.solint,
                         minsnr=inputs.minsnr, refant=inputs.refant, interp=amp_interp)

        # Ensure that any flagging applied to the MS by this applycal are
        # reverted at the end, even in the case of exceptions.
        try:
            # Apply the new caltables to the MS.
            LOG.info('Applying phase-up, bandpass, and amplitude cal tables.')
            # Apply the calibrations.
            callib_map = self._do_applycal(merge=False)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(callib_map)

            # Make "after calibration, before flagging" plots for the weblog
            LOG.info('Creating "after calibration, before flagging" plots')
            result.plots['before'] = plot_fn(inputs.intent, suffix='before')

            # Run correctedampflag to identify outliers for intents specified in
            # intents_for_flagging; let "field" and "spw" be initialized
            # automatically based on intents and context.
            LOG.info('Running correctedampflag to identify outliers to flag.')
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                antnegsig=inputs.antnegsig, antpossig=inputs.antpossig,
                tmantint=inputs.tmantint, tmint=inputs.tmint, tmbl=inputs.tmbl,
                antblnegsig=inputs.antblnegsig, antblpossig=inputs.antblpossig,
                relaxed_factor=inputs.relaxed_factor, niter=inputs.niter)
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # If flags were found in the bandpass calibrator...
            cafflags = cafresult.flagcmds()
            if cafflags:
                # Create the "after calibration, after flagging" plots for the weblog.
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(inputs.intent, suffix='after')

        finally:
            # Restore the "pre-gfluxscaleflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-gfluxscaleflag" flagging state.')
            task = casa_tasks.flagmanager(
                vis=inputs.vis, mode='restore', versionname=flag_backup_name_pregfsf)
            self._executor.execute(task)

        # If new outliers were identified...
        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(
                context=inputs.context, vis=inputs.vis, table=inputs.vis,
                inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            fsresult = self._executor.execute(fstask)

        # Store flagging task result.
        result.cafresult = cafresult

        return result

    def analyse(self, result):
        return result

    def _do_applycal(self, merge: bool) -> Dict:
        inputs = self.inputs

        # SJW - always just one job
        ac_intents = [inputs.intent]

        applycal_tasks = []
        for intent in ac_intents:
            task_inputs = applycal.IFApplycalInputs(inputs.context, vis=inputs.vis, intent=intent, flagsum=False,
                                                    flagbackup=False)
            task = applycal.SerialIFApplycal(task_inputs)
            applycal_tasks.append(task)

        callib_map = {}
        for task in applycal_tasks:
            applycal_result = self._executor.execute(task, merge=merge)
            callib_map.update(applycal_result.callib_map)

        return callib_map

    def _do_gaincal(self, field: Optional[str] = None, intent: Optional[str] = None, gaintype: str = 'G',
                    calmode: Optional[str] = None, combine: Optional[str] = None, solint: Optional[str] = None,
                    minsnr: Optional[float] = None, refant: Optional[str] = None, spwmap: Optional[list] = None,
                    interp: Optional[str] = None):
        inputs = self.inputs
        ms = inputs.ms

        # Identify which science spws were selected by inputs parameter.
        request_spws = ms.get_spectral_windows(task_arg=inputs.spw)

        # Identify which scans covered the requested intent, field, and any of
        # the requested spws.
        targeted_scans = ms.get_scans(scan_intent=intent, spw=inputs.spw, field=field)

        # Among the requested spws, identify which have a scan among the
        # targeted scans.
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}

        # Create separate phase caltables for each spectral spec.
        for spectral_spec, tuning_spw_ids in utils.get_spectralspec_to_spwid_map(scan_spws).items():
            tuning_spw_str = ','.join([str(i) for i in sorted(tuning_spw_ids)])
            LOG.info('Processing spectral spec {}, spws {}'.format(spectral_spec, tuning_spw_str))

            scans_with_data = ms.get_scans(spw=tuning_spw_str, scan_intent=intent, field=field)
            if not scans_with_data:
                LOG.info('No data to process for spectral spec {}. Continuing...'.format(spectral_spec))
                continue

            # of the fields that we are about to process, does any field have
            # multiple intents?
            mixed_intents = False
            fields_in_scans = {fld for scan in scans_with_data for fld in scan.fields}
            singular_intents = frozenset(intent.split(','))
            if len(singular_intents) > 1:
                for fld in fields_in_scans:
                    intents_to_scans = {si: ms.get_scans(scan_intent=si, field=fld.id, spw=tuning_spw_str)
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
                    field=field,
                    intent=intent,
                    spw=tuning_spw_str,
                    solint=solint,
                    gaintype=gaintype,
                    calmode=calmode,
                    minsnr=minsnr,
                    combine=combine,
                    refant=refant,
                    solnorm=False)

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

                # modify the result so that this caltable is only applied to
                # the intent from which the calibration was derived
                calapp_overrides = dict(intent=intent)

                # Phase solution caltables should be registered with
                # calwt=False (PIPE-1154).
                if calmode == 'p':
                    calapp_overrides['calwt'] = False

                # Adjust the field if provided.
                if field:
                    calapp_overrides['field'] = field

                # Adjust the interp if provided.
                if interp:
                    calapp_overrides['interp'] = interp

                # Adjust the spw map if provided.
                if spwmap:
                    calapp_overrides['spwmap'] = spwmap

                # Create modified CalApplication and replace CalApp in result
                # with this new one.
                original_calapp = result.final[0]
                modified_calapp = callibrary.copy_calapplication(original_calapp, **calapp_overrides)
                result.pool[0] = modified_calapp
                result.final[0] = modified_calapp

                # Merge the result to the local context to register new caltable
                # in local context callibrary.
                result.accept(inputs.context)

    def _do_phasecal(self):
        # Note: at present, phaseupsolint is specified as a fixed
        # value, defined in inputs. In the future, phaseupsolint may
        # need to be set based on exposure times; if so, see discussion
        # in CAS-10158 and logic in hifa.tasks.fluxscale.GcorFluxscale.
        inputs = self.inputs

        # PIPE-1154: the phase solves for flux calibrator should always use
        # combine='', gaintype='G', and no spwmap or interp.
        if 'AMPLITUDE' in inputs.intent:
            LOG.info('Compute phase gaincal table for flux calibrator.')
            self._do_gaincal(intent='AMPLITUDE', gaintype='G', calmode='p', combine='', solint=inputs.phaseupsolint,
                             minsnr=inputs.minsnr, refant=inputs.refant)

        # PIPE-2082: the phase solves for the diffgain calibrator should always
        # use combine='', gaintype='G', and no spwmap or interp.
        if ('DIFFGAINSRC' in inputs.intent or 'DIFFGAINREF' in inputs.intent) \
                and inputs.ms.get_fields(intent='DIFFGAINREF,DIFFGAINSRC'):
            LOG.info('Compute phase gaincal table for diffgain calibrator.')
            self._do_gaincal(intent='DIFFGAINREF,DIFFGAINSRC', gaintype='G', calmode='p', combine='',
                             solint=inputs.phaseupsolint, minsnr=inputs.minsnr, refant=inputs.refant)

        # PIPE-1154: for PHASE calibrator and CHECK source fields, create
        # separate phase solutions for each combination of intent, field, and
        # use optimal gaincal parameters based on spwmapping registered in the
        # measurement set.
        for intent in ['CHECK', 'PHASE']:
            if intent in inputs.intent:
                self._do_phasecal_per_field_for_intent(intent)

    def _do_phasecal_per_field_for_intent(self, intent: str):
        inputs = self.inputs

        # Create separate phase solutions for each field covered by requested
        # intent.
        for field in inputs.ms.get_fields(intent=intent):
            # Get optimal phase solution parameters for current intent and
            # field, based on spw mapping info in MS.
            combine, interp, spwmap = self._get_phasecal_params(self.inputs.ms, intent, field.name)

            # Create phase caltable and merge it into the local context.
            LOG.info(f'Compute phase gaincal table for intent={intent}, field={field.name}.')
            self._do_gaincal(field=field.name, intent=intent, gaintype='G', calmode='p', combine=combine,
                             solint=inputs.phaseupsolint, minsnr=inputs.minsnr, refant=inputs.refant, spwmap=spwmap,
                             interp=interp)

    @staticmethod
    def _get_phasecal_params(ms: MeasurementSet, intent: str, field: str) -> Tuple[str, Optional[str], list]:
        # By default, no spw mapping or combining, no interp.
        combine = ''
        interp = None
        spwmap = []

        # Try to fetch spwmapping info from MS for requested intent and field.
        spwmapping = ms.spwmaps.get((intent, field), None)

        # If a mapping was found, use the spwmap, and update combine and interp
        # depending on whether it is a combine spw mapping.
        if spwmapping:
            spwmap = spwmapping.spwmap
            if spwmapping.combine:
                combine = 'spw'
                interp = 'linearPD,linear'

        return combine, interp, spwmap


def create_plots(inputs, context, intents, suffix=''):
    """
    Return amplitude vs time and amplitude vs UV distance plots for the given
    intents.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param intents: intents to plot
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis, spw=inputs.spw, field=inputs.field)
    output_dir = context.output_dir

    amp_uvdist_plots, amp_time_plots = [], []
    for intent in intents.split(','):
        amp_uvdist_plots.extend(
            AmpVsXChart('uvdist', intent, context, output_dir, calto, suffix=suffix).plot())
        amp_time_plots.extend(
            AmpVsXChart('time', intent, context, output_dir, calto, suffix=suffix).plot())

    return {
        'uvdist': amp_uvdist_plots,
        'time': amp_time_plots
    }


class AmpVsXChart(applycal_displays.PlotmsFieldSpwComposite):
    """
    Plotting class that creates an amplitude vs X plot for each field and spw,
    where X is given as a constructor argument.
    """
    def __init__(self, xaxis: str, intent: str, context: Context, output_dir: str, calto: CalTo, **overrides):
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

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp', intent=intent,
                                          **plot_args)
