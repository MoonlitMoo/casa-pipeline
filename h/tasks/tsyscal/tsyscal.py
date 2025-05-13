import collections
from operator import itemgetter, attrgetter
from typing import Dict, List, Set

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import MeasurementSet
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.h.heuristics.tsysspwmap import tsysspwmap
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from . import resultobjects

__all__ = [
    'Tsyscal',
    'TsyscalInputs',
]

LOG = infrastructure.get_logger(__name__)


class TsyscalInputs(vdp.StandardInputs):
    """
    TsyscalInputs defines the inputs for the Tsyscal pipeline task.
    """
    chantol = vdp.VisDependentProperty(default=1)

    @vdp.VisDependentProperty
    def caltable(self):
        """
        Get the caltable argument for these inputs.

        If set to a table-naming heuristic, this should give a sensible name
        considering the current CASA task arguments.
        """
        namer = caltable_heuristic.TsysCaltable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)

    def __init__(self, context, output_dir=None, vis=None, caltable=None, chantol=None):
        super(TsyscalInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.caltable = caltable

        # solution parameters
        self.chantol = chantol

    # Convert to CASA gencal task arguments.
    def to_casa_args(self):
        return {
            'vis': self.vis,
            'caltable': self.caltable
        }


@task_registry.set_equivalent_casa_task('h_tsyscal')
@task_registry.set_casa_commands_comment('The Tsys calibration and spectral window map is computed.')
class Tsyscal(basetask.StandardTaskTemplate):
    Inputs = TsyscalInputs

    def prepare(self):
        inputs = self.inputs

        # make a note of the current inputs state before we start fiddling
        # with it. This origin will be attached to the final CalApplication.
        origin = callibrary.CalAppOrigin(task=Tsyscal, inputs=inputs.to_casa_args())

        # construct the Tsys cal file
        gencal_args = inputs.to_casa_args()
        gencal_job = casa_tasks.gencal(caltype='tsys', **gencal_args)
        self._executor.execute(gencal_job)

        tsys_table = gencal_args['caltable']

        LOG.todo('tsysspwmap heuristic re-reads measurement set!')
        LOG.todo('tsysspwmap heuristic won\'t handle missing file')
        nospwmap, spwmap = tsysspwmap(ms=inputs.ms, tsystable=tsys_table, tsysChanTol=inputs.chantol)

        calfrom_defaults = dict(caltype='tsys', spwmap=spwmap, interp='linear,linear')

        is_single_dish = utils.contains_single_dish(inputs.context)
        calapps = get_calapplications(inputs.ms, tsys_table, calfrom_defaults, origin, spwmap, is_single_dish)

        return resultobjects.TsyscalResults(pool=calapps, unmappedspws=nospwmap)

    def analyse(self, result):
        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.pool if ca.exists()]
        result.final[:] = on_disk

        missing = [ca for ca in result.pool if ca not in on_disk]
        result.error.clear()
        result.error.update(missing)

        return result


# Holds an observing intent and the preferred/fallback gainfield args to be used for that intent
GainfieldMapping = collections.namedtuple('GainfieldMapping', 'intent preferred fallback')


def get_solution_map(ms: MeasurementSet, is_single_dish: bool) -> List[GainfieldMapping]:
    """
    Get gainfield solution map. Different solution maps are returned for
    single dish and interferometric data.

    :param ms: MS to analyse
    :param is_single_dish: True if MS is single dish data
    :return: list of GainfieldMappings
    """
    # define function to get Tsys fields for intent
    def f(intent):
        if ',' in intent:
            head, tail = intent.split(',', 1)
            # the 'if o' test filters out results for intents that do not have
            # fields, e.g., PHASE for SD data
            return ','.join(o for o in (f(head), f(tail)) if o)
        return ','.join(str(s) for s in get_tsys_fields_for_intent(ms, intent))

    # return different gainfield maps for single dish and interferometric
    if is_single_dish:
        return [
            GainfieldMapping(intent='BANDPASS', preferred=f('BANDPASS'), fallback='nearest'),
            GainfieldMapping(intent='AMPLITUDE', preferred=f('AMPLITUDE'), fallback='nearest'),
            # non-empty magic string to differentiate between no field found and a null fallback
            GainfieldMapping(intent='TARGET', preferred=f('TARGET'), fallback='___EMPTY_STRING___')
        ]

    else:
        # Intent mapping extracted from CAS-12213 ticket.
        # PIPE-2080: updated to add mapping for DIFFGAINREF, DIFFGAINSRC intent.
        #
        # ObjectToBeCalibrated 	TsysSolutionToUse 	IfNoSolutionPresentThenUse
        # BANDPASS cal 	        all BANDPASS cals 	fallback to 'nearest'
        # FLUX cal 	            all FLUX cals 	    fallback to 'nearest'
        # DIFFGAIN[REF|SRC]     all DIFFGAIN cals 	fallback to BANDPASS
        # PHASE cal 	        all PHASE cals 	    all TARGETs
        # TARGET 	            all TARGETs 	    all PHASE cals
        # CHECK_SOURCE        	all TARGETs     	all PHASE cals
        return [
            GainfieldMapping(intent='BANDPASS', preferred=f('BANDPASS'), fallback='nearest'),
            GainfieldMapping(intent='AMPLITUDE', preferred=f('AMPLITUDE'), fallback='nearest'),
            GainfieldMapping(intent='DIFFGAINREF', preferred=f('DIFFGAINREF'), fallback=f('BANDPASS')),
            GainfieldMapping(intent='DIFFGAINSRC', preferred=f('DIFFGAINSRC'), fallback=f('BANDPASS')),
            GainfieldMapping(intent='PHASE', preferred=f('PHASE'), fallback=f('TARGET')),
            GainfieldMapping(intent='TARGET', preferred=f('TARGET'), fallback=f('PHASE')),
            GainfieldMapping(intent='CHECK', preferred=f('TARGET'), fallback=f('PHASE')),
        ]


def get_gainfield_map(ms: MeasurementSet, is_single_dish: bool) -> Dict:
    """
    Get the mapping of observing intent to gainfield parameter for a
    measurement set.

    The mapping follows the observing intent to gainfield intent defined in
    CAS-12213.

    :param ms: MS to analyse
    :param is_single_dish: boolean for if SD data or not
    :return: dict of {observing intent: gainfield}
    """

    soln_map = get_solution_map(ms, is_single_dish)
    final_map = {s.intent: s.preferred if s.preferred else s.fallback for s in soln_map}

    # Detect cases where there's no preferred or fallback gainfield mapping,
    # e.g., if there are no Tsys scans on a target or phase calibrator.
    undefined_intents = [k for k, v in final_map.items()
                         if not v  # gainfield mapping is empty..
                         and k in ms.intents]  # ..for a valid intent in the MS
    if undefined_intents:
        msg = 'Undefined Tsys gainfield mapping for {} intents: {}'.format(ms.basename, undefined_intents)
        LOG.error(msg)
        raise AssertionError(msg)

    # convert magic string back to empty string
    converted = {k: v.replace('___EMPTY_STRING___', '') for k, v in final_map.items()}

    return converted


def get_tsys_fields_for_intent(ms: MeasurementSet, intent: str) -> Set[str]:
    """
    Returns the identity of the Tsys field(s) for an intent.

    :param ms: MS to analyse
    :param intent: intent to retrieve fields for.
    :return: set of field identifiers corresponding to intent
    """
    # In addition to the science intent scan, a field must also have a Tsys
    # scan observed for a Tsys solution to be considered present. The
    # exception is science mosaics, which are handled as a special case.

    # We need to know which science intent scans have Tsys scans; the ones
    # that don't will be checked for science mosaics separately. This lets
    # us handle single field, single pointing science targets alongside mosaic
    # targets mixed together in the same EB. Theoretically, at least...
    intent_fields = ms.get_fields(intent=intent)

    # contains fields of this intent that also have a companion Tsys scan
    intent_fields_with_tsys = [f for f in intent_fields if 'ATMOSPHERE' in f.intents]

    # contains fields without a companion Tsys scan. These might be science
    # mosaics.
    intent_fields_without_tsys = [f for f in intent_fields if f not in intent_fields_with_tsys]

    tsys_fields_for_mosaics = []
    if intent == 'TARGET':
        # In science mosaics, the fields comprising the TARGET pointings do
        # not have Tsys scans observed on those fields. Instead, there is a
        # Tsys-only field roughly at the centre of the mosaic that is
        # referenced by the same parent source as the TARGET pointing fields.

        # Double check that the fields without Tsys scans are indeed science
        # mosaics with a separate Tsys field. Note that a mosaic consisting of
        # a source with a single TARGET pointing and a single Tsys scan would
        # also be classified as a mosaic by this logic.
        mosaic_fields = [f for f in intent_fields_without_tsys if 'ATMOSPHERE' in f.source.intents]

        # Collect the Tsys fields referenced by the parent source of the
        # science mosaic fields missing Tsys scans.
        tsys_fields_for_mosaics = [f
                                   for pointing in mosaic_fields
                                   for f in pointing.source.fields if 'ATMOSPHERE' in f.intents]

    r = {field.id for field in intent_fields_with_tsys}
    r.update({field.id for field in tsys_fields_for_mosaics})

    # when field names are not unique, as is usually the case for science
    # mosaics, then we must reference the numeric field ID instead
    field_identifiers = utils.get_field_identifiers(ms)
    return {field_identifiers[i] for i in r}


def get_calapplications(ms: MeasurementSet, tsys_table: str, calfrom_defaults: Dict, origin: callibrary.CalAppOrigin,
                        spw_map: List, is_single_dish: bool) -> List[callibrary.CalApplication]:
    """
    Get a list of CalApplications that apply a Tsys caltable to a measurement
    set using the gainfield mapping defined in CAS-12213.

    Note: this function only provides the gainfield argument for the CalFrom
    constructor. Any other required CalFrom constructor arguments should be
    provided to this function via the calfrom_defaults parameter.

    :param ms: MeasurementSet to apply calibrations to
    :param tsys_table: name of Tsys table
    :param calfrom_defaults: dict of CalFrom constructor arguments
    :param origin: CalOrigin for the created CalApplications
    :param spw_map: Tsys SpW map
    :param is_single_dish: boolean declaring if current MS is for Single-Dish
    :return: list of CalApplications
    """
    # Get the map of intent:gainfield
    soln_map = get_gainfield_map(ms, is_single_dish)

    # Create the static dict of calfrom arguments. Only the 'gainfield' argument changes from calapp to calapp; the
    # other arguments remain unchanged.
    calfrom_args = dict(calfrom_defaults)
    calfrom_args['gaintable'] = tsys_table

    # get the mapping of field ID to unambiguous identifier for more user friendly logs
    field_id_to_identifier = utils.get_field_identifiers(ms)

    # create a domain object mapping of science spw to Tsys spw
    domain_spw_map = {ms.spectral_windows[i]: ms.spectral_windows[j] for i, j in enumerate(spw_map)}

    # Now loop through the MS intents, creating a specific Tsys registration for each intent.
    calapps = []
    for intent in ms.intents:
        # get the preferred Tsys gainfield for this intent, falling back to 'nearest' if not specified
        gainfield = soln_map.get(intent, 'nearest')

        # The CASA callibrary cannot handle registrations with multiple fldmap fields, e.g., fldmap='1,2'.
        if ',' in gainfield:
            LOG.info('Calculating workaround for CASA callibrary fldmap incompatibility: '
                     '{} intent -> fldmap={!r}'.format(intent, gainfield))
            # get the fields for this non-Tsys intent
            fields_with_intent = ms.get_fields(intent=intent)
            # get the Tsys fields that were to be applied in the fldmap
            tsys_fields = ms.get_fields(task_arg=gainfield)

            # holds mapping of field,spw -> Tsys field
            field_to_tsys_field = collections.defaultdict(dict)

            # For the fields for the current non-Tsys intent, we'll emulate the
            # CASA gainfield='nearest' option by selecting the spatially
            # closest Tsys field with the same tuning.
            me = casa_tools.measures
            qa = casa_tools.quanta
            for non_tsys_field in fields_with_intent:
                non_tsys_direction = non_tsys_field.mdirection

                # For multi-tuning EBs, the spws may need different gainfield arguments. For example, in
                # uid://A002/Xcf3a9c/X3a3d, the phase cal has no Tsys scans and was observed with three different
                # tunings. Each tuning is distinct and was used for a different target field. Each target field *does*
                # have a Tsys scan. Due to the lack of Tsys scans on the phase cal, the target fields are selected as
                # the fallback gainfields (e.g., # gainfield='A,B,C'), which due to the CASA callibrary
                # limitation we now need to boil down to the closest target that was observed using the same
                # spectral setup.

                # get the spws used to observe with intent XXX for this non-Tsys field
                non_tsys_scans = ms.get_scans(scan_intent=intent, field=non_tsys_field.id)
                non_tsys_spws = {dd.spw for scan in non_tsys_scans for dd in scan.data_descriptions}
                # filter out non-science windows, leaving just the spws for this non-Tsys field
                non_tsys_spws = [spw for spw in non_tsys_spws if spw.type in ('TDM', 'FDM')]

                for non_tsys_spw in sorted(non_tsys_spws, key=attrgetter('id')):
                    # check to see if there's data for the field/intent/spw combination
                    scans = ms.get_scans(scan_intent=intent, spw=non_tsys_spw.id, field=non_tsys_field.id)
                    if not scans:
                        # If there's no data, there's no calibration required.
                        continue

                    # for each spw used for the intent=XXX scan, find the Tsys field that was observed using the Tsys
                    # spw which is mapped to this science spw
                    tsys_spw_for_spw = domain_spw_map[non_tsys_spw]
                    tsys_fields_with_required_spw = [f for f in tsys_fields if tsys_spw_for_spw in f.valid_spws]

                    LOG.debug('Candidate Tsys fields for spw {}: {}'
                              ''.format(non_tsys_spw.id, ','.join(f.name for f in tsys_fields_with_required_spw)))

                    separations = []
                    for tsys_field in tsys_fields_with_required_spw:
                        tsys_direction = tsys_field.mdirection
                        separation = me.separation(tsys_direction, non_tsys_direction)
                        separation_degs = qa.getvalue(qa.convert(separation, 'deg'))[0]
                        separations.append((tsys_field, separation_degs))

                    if not separations:
                        msg = ('Failed Tsys calibration for {} spw {}: no Tsys scan with the same tuning identified'
                               ''.format(non_tsys_field.name, non_tsys_spw.id))
                        LOG.error(msg)
                        continue

                    closest = min(separations, key=itemgetter(1))[0]
                    LOG.info('Tying {} field #{} spw #{} to closest Tsys field {} spw #{}'.format(
                        intent, field_id_to_identifier[non_tsys_field.id], non_tsys_spw.id,
                        field_id_to_identifier[closest.id], tsys_spw_for_spw.id)
                    )
                    field_to_tsys_field[non_tsys_field][non_tsys_spw.id] = closest

            # create a CalTo specifically for the intent fields with their selected Tsys field
            for non_tsys_field, spw_to_tsys_field in field_to_tsys_field.items():
                field_arg = field_id_to_identifier[non_tsys_field.id]
                for spw, tsys_field in spw_to_tsys_field.items():
                    gainfield_arg = field_id_to_identifier[tsys_field.id]
                    calto = callibrary.CalTo(vis=ms.name, intent=intent, spw=spw, field=field_arg)
                    calfrom = callibrary.CalFrom(gainfield=gainfield_arg, **calfrom_args)
                    calapp = callibrary.CalApplication(calto, calfrom, origin)
                    calapps.append(calapp)

        else:
            LOG.info('Setting Tsys gainfield={!r} for {} data in {}'.format(gainfield, intent, ms.basename))

            # With gainfield set appropriately, construct the CalApplication and add it to the results
            calto = callibrary.CalTo(vis=ms.name, intent=intent)
            calfrom = callibrary.CalFrom(gainfield=gainfield, **calfrom_args)
            calapp = callibrary.CalApplication(calto, calfrom, origin)
            calapps.append(calapp)

    return calapps
