import copy
import os
import traceback
import collections

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.extern.almarenorm import alma_renorm
from pipeline.infrastructure import task_registry
from pipeline.infrastructure import casa_tools
from pipeline.h.heuristics import caltable as caltable_heuristic

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, vis, createcaltable, threshold, spw, excludechan, calTableCreated,
                 stats, rnstats, alltdm, atmAutoExclude, atmWarning, atmExcludeCmd, bwthreshspw, caltable, calapps, exception=None):
        super().__init__()
        self.vis = vis
        self.createcaltable = createcaltable
        self.threshold = threshold
        self.spw = spw
        self.excludechan = excludechan
        self.calTableCreated = calTableCreated
        self.stats = stats
        self.rnstats = rnstats
        self.alltdm = alltdm
        self.atmAutoExclude = atmAutoExclude
        self.atmWarning = atmWarning
        self.atmExcludeCmd = atmExcludeCmd
        self.bwthreshspw = bwthreshspw
        self.caltable = caltable
        self.calapps = calapps
        self.exception = exception

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        if not self.calapps:
            LOG.info('No results to merge')
            return
        else:
            for calapp in self.calapps:
                LOG.debug('Adding calibration to callibrary:\n'
                          '%s\n%s' % (calapp.calto, calapp.calfrom))
                context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        return (f'RenormResults:\n'
                f'\tvis={self.vis}\n'
                f'\tcreatecaltable={self.createcaltable}\n'
                f'\tthreshold={self.threshold}\n'
                f'\tspw={self.spw}\n'
                f'\texcludechan={self.excludechan}\n'
                f'\talltdm={self.alltdm}\n'
                f'\tstats={self.stats}\n'
                f'\tatmAutoExclude={self.atmAutoExclude}\n'
                f'\tbwthreshspw={self.bwthreshspw}\n'
                f'\tcaltable={self.caltable}\n')


class RenormInputs(vdp.StandardInputs):
    createcaltable = vdp.VisDependentProperty(default=False)
    threshold = vdp.VisDependentProperty(default=1.02)
    spw = vdp.VisDependentProperty(default='')
    excludechan = vdp.VisDependentProperty(default={})
    atm_auto_exclude = vdp.VisDependentProperty(default=False)
    bwthreshspw = vdp.VisDependentProperty(default={})

    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, vis=None, createcaltable=None, threshold=None, spw=None,
                 excludechan=None, atm_auto_exclude=None, bwthreshspw=None, caltable=None, parallel=None):
        super().__init__()
        self.context = context
        self.vis = vis
        self.createcaltable = createcaltable
        self.threshold = threshold
        self.spw = spw
        self.excludechan = excludechan
        self.atm_auto_exclude = atm_auto_exclude
        self.bwthreshspw = bwthreshspw
        self.caltable = caltable
        self.parallel = parallel

    @vdp.VisDependentProperty
    def caltable(self):
        """
        Get the caltable argument for these inputs.

        If set to a table-naming heuristic, this should give a sensible name
        considering the current CASA task arguments.
        """
        namer = caltable_heuristic.AmpCaltable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)


class SerialRenorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):
        inp = self.inputs

        # PIPE-2150: safely create the "RN_plots" directory before calling the renormalization external code to prevent
        # potential race condition when checking/examining the directory existence in the tier0 setup.
        # This workaround might be removed after the changes from PIPE-2151
        os.makedirs('RN_plots', exist_ok=True)

        # Create inputs for the call to the ALMA renorm function.
        alma_renorm_inputs = {
            'vis': inp.vis,
            'spw': [int(x) for x in inp.spw.split(',') if x],  # alma_renorm expects SpWs as list of integers
            'create_cal_table': inp.createcaltable,
            'threshold': inp.threshold,
            'excludechan': copy.deepcopy(inp.excludechan),  # create copy, PIPE-1612.
            'atm_auto_exclude': inp.atm_auto_exclude,
            'bwthreshspw': inp.bwthreshspw,
            'caltable': inp.caltable
        }

        # Call the ALMA renormalization function and collect its output in task
        # result.
        try:
            LOG.info("Calling the renormalization heuristic function.")
            alltdm, atmExcludeCmd, atmWarning, calTableCreated, rnstats, stats = \
                alma_renorm(**alma_renorm_inputs)

            rnstats_light = self._get_rnstats_light(stats, rnstats)

            calapps = self._get_calapps(calTableCreated)

            result = RenormResults(inp.vis, inp.createcaltable, inp.threshold, inp.spw,
                                   inp.excludechan, calTableCreated, stats, rnstats_light, alltdm,
                                   inp.atm_auto_exclude, atmWarning, atmExcludeCmd, inp.bwthreshspw, inp.caltable, calapps)

        except Exception as e:
            LOG.error('Failure in running renormalization heuristic: {}'.format(e))
            LOG.error(traceback.format_exc())
            result = RenormResults(inp.vis, inp.createcaltable, inp.threshold, inp.spw,
                                   inp.excludechan, False, {}, {}, False, inp.atm_auto_exclude, {}, {}, {}, inp.caltable, [], e)

        return result

    def analyse(self, results):
        return results

    def _get_rnstats_light(self, stats, rnstats):
        """
        Extracts and summarizes the presence of NaN values in rnstats.

        Args:
            stats (dict): Dictionary containing statistics for each source and spectral window.
            rnstats (dict): Dictionary containing numerical values for each source and spectral window.

        Returns:
            dict: A dictionary indicating whether any NaN values are present for each source and spectral window.
                If the source or spectral window is not present in rnstats, it is marked as invalid (True).
        """
        rnstats_light = {'invalid': collections.defaultdict(dict)}

        for source, spws in stats.items():
            for spw in spws:
                rnstats_light['invalid'][source][spw] = np.isnan(rnstats['N'].get(source, {}).get(spw, np.nan)).any()

        return rnstats_light

    def _get_calapps(self, calTableCreated):
        """
        Collects the necessary calapps.
        """

        inp = self.inputs

        calapps = []
        if inp.createcaltable and calTableCreated:
            msObj = inp.context.observing_run.get_ms(inp.vis)

            origin = callibrary.CalAppOrigin(task=SerialRenorm, inputs=inp.to_casa_args())

            with casa_tools.TableReader(inp.caltable) as table:
                field_ids = table.getcol('FIELD_ID')
                spw_ids = table.getcol('SPECTRAL_WINDOW_ID')

            # Get unique field/spw combinations
            field_spw = collections.defaultdict(set)
            for f_id, s_id in zip(field_ids, spw_ids):
                field_name = msObj.get_fields(field_id=f_id)[0].name
                field_spw[field_name].add(str(s_id))

            for field_name in field_spw:
                spw = ','.join(field_spw[field_name])

                calto_args = {'vis': inp.vis,
                              'intent': 'TARGET',
                              'field': field_name,
                              'spw': spw}
                calto = callibrary.CalTo(**calto_args)

                # The renorm results are applied like a Tsys calibration
                calfrom_args = {'gaintable': inp.caltable,
                                'caltype': 'tsys',
                                'interp': 'nearest,linear'}
                calfrom = callibrary.CalFrom(**calfrom_args)

                calapps.append(callibrary.CalApplication(calto, calfrom, origin))

        return calapps

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Renormalize data affected by strong line emission.')
class Renorm(sessionutils.ParallelTemplate):
    Inputs = RenormInputs
    Task = SerialRenorm
