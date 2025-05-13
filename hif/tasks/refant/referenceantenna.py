import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from ...heuristics import findrefant

__all__ = [
    'RefAntInputs',
    'RefAntResults',
    'RefAnt',
    'SerialRefAnt',
]

LOG = infrastructure.get_logger(__name__)


class RefAntInputs(vdp.StandardInputs):
    # PIPE-1691: hif_refant is now implicitly a parallel task, but by default
    # running with parallel=False.
    parallel = sessionutils.parallel_inputs_impl(default=False)

    @vdp.VisDependentProperty
    def field(self):
        # return each field in the current ms that has been observed
        # with the desired intent
        fields = self.ms.get_fields(intent=self.intent)

        unique_field_names = {f.name for f in fields}
        field_ids = {f.id for f in fields}

        # Fields with different intents may have the same name. Check for this
        # and return the IDs if necessary
        if len(unique_field_names) == len(field_ids):
            return ','.join(unique_field_names)
        else:
            return ','.join([str(i) for i in field_ids])

    flagging = vdp.VisDependentProperty(default=True)
    geometry = vdp.VisDependentProperty(default=True)
    intent = vdp.VisDependentProperty(default='AMPLITUDE,BANDPASS,PHASE,POLARIZATION')
    refant = vdp.VisDependentProperty(default='')
    refantignore = vdp.VisDependentProperty(default='')
    spw = vdp.VisDependentProperty(default='')

    hm_refant = vdp.VisDependentProperty(default='automatic')

    @hm_refant.convert
    def hm_refant(self, value):
        return 'manual' if value == 'manual' else 'automatic'

    @vdp.VisDependentProperty
    def spw(self):
        intents = {'PHASE', 'BANDPASS', 'AMPLITUDE', 'POLARIZATION'}
        spws = [spw for spw in self.ms.get_spectral_windows() if not intents.isdisjoint(spw.intents)]
        return ','.join([str(spw.id) for spw in spws])

    def to_casa_args(self):
        # refant does not use CASA tasks
        raise NotImplementedError

    def __init__(self, context, vis=None, output_dir=None, field=None, spw=None, intent=None, hm_refant=None,
                 refant=None, geometry=None, flagging=None, refantignore=None, parallel=None):
        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.field = field
        self.spw = spw
        self.intent = intent

        self.hm_refant = hm_refant
        self.refant = refant
        self.geometry = geometry
        self.flagging = flagging
        self.refantignore = refantignore

        self.parallel = parallel


class RefAntResults(basetask.Results):
    def __init__(self, vis, refant):
        super(RefAntResults, self).__init__()
        self._vis = vis
        self._refant = ','.join([str(ant) for ant in refant])

    def merge_with_context(self, context):
        if self._vis is None or self._refant is None:
            LOG.error('No results to merge')
            return

        # Do we also need to locate the antenna in the measurement set?
        # This might become necessary when using different sessions
        ms = context.observing_run.get_ms(name=self._vis)

        if ms:
            LOG.debug('Setting refant for {!s} to {!r}'.format(ms.basename, self._refant))
            ms.reference_antenna = self._refant

    def __str__(self):
        if self._vis is None or self._refant is None:
            return ('Reference antenna results:\n'
                    '\tNo reference antenna selected')
        else:
            return ('Reference antenna results:\n'
                    '{!s}: refant={!r}'.format(os.path.basename(self._vis), self._refant))

    def __repr__(self):
        return 'RefAntResults({!r}, {!r})'.format(self._vis, self._refant)


class SerialRefAnt(basetask.StandardTaskTemplate):
    Inputs = RefAntInputs

    def __init__(self, inputs):
        super().__init__(inputs)

    def prepare(self, **parameters):
        inputs = self.inputs

        # Get the reference antenna list
        if inputs.hm_refant == 'manual':
            refant = inputs.refant.split(',')

        elif inputs.hm_refant == 'automatic':
            casa_intents = utils.to_CASA_intent(inputs.ms, inputs.intent)

            heuristics = findrefant.RefAntHeuristics(vis=inputs.vis, field=inputs.field, spw=inputs.spw,
                                                     intent=casa_intents, geometry=inputs.geometry,
                                                     flagging=inputs.flagging, refantignore=inputs.refantignore)
            refant = heuristics.calculate()

        else:
            raise NotImplemented('Unhandled hm_refant value: {!r}'.format(inputs.hm_refant))

        return RefAntResults(inputs.vis, refant)

    def analyse(self, results):
        return results


@task_registry.set_equivalent_casa_task('hif_refant')
@task_registry.set_casa_commands_comment(
    'Antennas are prioritized and enumerated based on fraction flagged and position in the array. The best antenna is '
    'used as a reference antenna unless it gets flagged, in which case the next-best antenna is used.\n'
    'This stage performs a pipeline calculation without running any CASA commands to be put in this file.'
)
class RefAnt(sessionutils.ParallelTemplate):
    Inputs = RefAntInputs
    Task = SerialRefAnt
