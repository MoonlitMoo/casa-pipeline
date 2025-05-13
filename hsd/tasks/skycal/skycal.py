"""The skycal task module to calibrate sky background."""
import collections
import copy
import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
from pipeline.infrastructure.utils import relative_path
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from ..common import SingleDishResults
from typing import TYPE_CHECKING, Dict, List, Optional, Union
if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


class SDSkyCalInputs(vdp.StandardInputs):
    """Inputs class for SDSkyCal task."""
    parallel = sessionutils.parallel_inputs_impl()

    calmode = vdp.VisDependentProperty(default='auto')
    elongated = vdp.VisDependentProperty(default=False)
    field = vdp.VisDependentProperty(default='')
    fraction = vdp.VisDependentProperty(default='10%')
    noff = vdp.VisDependentProperty(default=-1)
    outfile = vdp.VisDependentProperty(default='')
    scan = vdp.VisDependentProperty(default='')
    spw = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default=0.5)

    @vdp.VisDependentProperty
    def infiles(self) -> str:
        """Return name of MS. Alias for "vis" attribute."""
        return self.vis

    @infiles.convert
    def infiles(self, value: Union[str, List[str]]) -> Union[str, List[str]]:
        """Convert value into expected type.

        Currently, no conversion is performed.

        Args:
            value: Name of MS, or the list of names.

        Returns:
            Converted value. Currently return input value as is.
        """
        self.vis = value
        return value

    def __init__(
            self,
            context: 'Context',
            calmode: Optional[str] = None,
            fraction: Optional[float] = None,
            noff: Optional[int] = None,
            width: Optional[float] = None,
            elongated: Optional[bool] = None,
            output_dir: Optional[str] = None,
            infiles: Optional[str] = None,
            outfile: Optional[str] = None,
            field: Optional[str] = None,
            spw: Optional[str] = None,
            scan: Optional[str] = None,
            parallel: Optional[Union[bool, str]] = None
            ):
        """Initialize SDK2JyCalInputs instance.

        Args:
            context: Pipeline context.
            calmode: Calibration mode.
            fraction: Value of fraction of OFF scan data against total data number.
            noff: Number of OFF scan data.
            width: Value of pixel width, e.g., a median spatial separation between
            neighboring two data in time.
            elongated: Whether observed area is elongated in one direction (True)
            or not (False).
            output_dir: Name of output directory.
            infiles: Name of MS or list of names.
            outfile: Name of the output file.
            field: Field selection.
            spw: Spectral window (spw) selection.
            scan: Scan selection.
            parallel: Execute using CASA HPC functionality, if available.
                      Default is None, which intends to turn on parallel
                      processing if possible.
        """
        super(SDSkyCalInputs, self).__init__()

        # context and vis must be set first so that properties that require
        # domain objects can be function
        self.context = context
        self.infiles = infiles
        self.output_dir = output_dir
        self.outfile = outfile

        self.calmode = calmode
        self.fraction = fraction
        self.noff = noff
        self.width = width
        self.elongated = elongated

        self.field = field
        self.spw = spw
        self.scan = scan

        self.parallel = parallel

    def to_casa_args(self) -> Dict:
        """Convert Inputs instance to the list of keyword arguments for sdcal.

        Returns:
            Keyword arguments for sdcal.
        """
        args = super().to_casa_args()

        # overwrite is always True
        args['overwrite'] = True

        # parameter name for input data is 'infile'
        args['infile'] = args.pop('infiles')

        # vis and parallel are not necessary
        del args['vis']
        del args['parallel']

        return args


class SDSkyCalResults(SingleDishResults):
    """Class to hold processing result of SDSkyCal task."""

    def __init__(
            self,
            task: Optional[str] = None,
            success: Optional[bool] = None,
            outcome: Optional[str] = None
            ) -> None:
        """Initialize SDSkyCalResults instance.

        Args:
            task: Name of task.
            success: A boolean to indicate if the task execution was successful
            (True) or not (False).
            outcome: Outcome of the task.
        """
        super(SDSkyCalResults, self).__init__(task, success, outcome)
        self.final = self.outcome

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        The CalApplication instance updated by the skycal task is added to
        the pipeline context.

        Args:
            context: Pipeline context.
        """
        super(SDSkyCalResults, self).merge_with_context(context)

        if self.outcome is None:
            return

        for calapp in self.outcome:
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def _outcome_name(self) -> str:
        """Return string representing the outcome.

        Returns:
            string of outcome.
        """
        return str(self.outcome)


class SerialSDSkyCal(basetask.StandardTaskTemplate):
    """Generate sky calibration table."""

    Inputs = SDSkyCalInputs
    ElevationDifferenceThreshold = 3.0  # deg

    def prepare(self) -> SDSkyCalResults:
        """Prepare arguments for CASA job and execute it.

        Returns:
           SDSkyCalResults object.
        """
        args = self.inputs.to_casa_args()
        LOG.trace('args: {}'.format(args))

        # retrieve ms domain object
        ms = self.inputs.ms
        calibration_strategy = ms.calibration_strategy
        default_field_strategy = calibration_strategy['field_strategy']

        # take calmode from calibration strategy if it is set to 'auto'
        if args['calmode'] is None or args['calmode'].lower() == 'auto':
            args['calmode'] = calibration_strategy['calmode']

        assert args['calmode'] in ['ps', 'otfraster', 'otf']

        # spw selection ---> task.prepare
        if args['spw'] is None or len(args['spw']) == 0:
            spw_list = ms.get_spectral_windows(science_windows_only=True)
            args['spw'] = ','.join(map(str, [spw.id for spw in spw_list]))

        # field selection ---> task.prepare
        if args['field'] is None or len(args['field']) == 0:
            field_strategy = default_field_strategy
        else:
            field_strategy = {}
            field_ids = casa_tools.ms.msseltoindex(vis=ms.name, field=args['field'])
            for field_id in field_ids:
                for target_id, reference_id in default_field_strategy.items():
                    if field_id == target_id:
                        field_strategy[field_id] = default_field_strategy[field_id]
                        continue
                    elif field_id == reference_id:
                        field_strategy[target_id] = field_id
                        continue

        # scan selection
        if args['scan'] is None:
            args['scan'] = ''

        # intent
        if args['calmode'] in ['otf', 'otfraster']:
            args['intent'] = 'OBSERVE_TARGET#ON_SOURCE'

        calapps = []
        for target_id, reference_id in field_strategy.items():
            myargs = copy.deepcopy(args)

            # output file
            reference_field_name = ms.get_fields(reference_id)[0].clean_name
            if myargs['outfile'] is None or len(myargs['outfile']) == 0:
                namer = caltable_heuristic.SDSkyCaltable()
                # filenamer requires field name instead of id
                myargs['field'] = reference_field_name
                try:
                    # we temporarily need 'vis'
                    myargs['vis'] = myargs['infile']
                    myargs['outfile'] = relative_path(namer.calculate(output_dir=self.inputs.output_dir,
                                                                      stage=self.inputs.context.stage,
                                                                      **myargs))
                finally:
                    del myargs['vis']
            else:
                myargs['outfile'] = myargs['outfile'] + '.{}'.format(reference_field_name)

            # field
            myargs['field'] = str(reference_id)

            LOG.debug('args for sdcal: {}'.format(myargs))

            # create job
            job = casa_tasks.sdcal(**myargs)

            # execute job
            LOG.debug('Table cache before sdcal: {}'.format(casa_tools.table.showcache()))
            try:
                self._executor.execute(job)
            finally:
                LOG.debug('Table cache after sdcal: {}'.format(casa_tools.table.showcache()))

            # check if caltable is empty
            with casa_tools.TableReader(myargs['outfile']) as tb:
                is_caltable_empty = tb.nrows() == 0
            if is_caltable_empty:
                continue

            # make a note of the current inputs state before we start fiddling
            # with it. This origin will be attached to the final CalApplication.
            origin = callibrary.CalAppOrigin(task=SerialSDSkyCal,
                                             inputs=args)

            calto = callibrary.CalTo(vis=myargs['infile'],
                                     spw=myargs['spw'],
                                     field=str(target_id),
                                     intent='TARGET')

            # create SDCalFrom object
            calfrom = callibrary.CalFrom(gaintable=myargs['outfile'],
                                         gainfield=str(reference_id),
                                         interp='linear,linear',
                                         caltype=myargs['calmode'])

            # create CalApplication object
            calapp = callibrary.CalApplication(calto, calfrom, origin)
            calapps.append(calapp)

        results = SDSkyCalResults(task=self.__class__,
                                  success=True,
                                  outcome=calapps)
        return results

    def analyse(self, result: SDSkyCalResults) -> SDSkyCalResults:
        """Analyse SDSkyCalResults instance produced by prepare.

        Args:
            result: SDSkyCalResults instance.

        Returns:
            Updated SDSkyCalResults instance.
        """
        # compute elevation difference between ON and OFF and
        # warn if it exceeds elevation threshold
        threshold = self.ElevationDifferenceThreshold
        context = self.inputs.context
        resultdict = compute_elevation_difference(context, result)
        ms = self.inputs.ms

        spectralspecs = ms.get_spectral_specs()
        spectralspecs_spwids = {}
        for ss in spectralspecs:
            spw_ss = ms.get_spectral_windows(science_windows_only=True, spectralspecs = [ss])
            list_spwids = [x.id for x in spw_ss]
            if len(list_spwids) == 0:
                continue
            else:
                spectralspecs_spwids[ss] = list_spwids

        for list_spwids in spectralspecs_spwids.values():
            spwid0 = list_spwids[0]
            for field_id, eldfield in resultdict.items():
                for antenna_id, eldant in eldfield.items():
                    assert spwid0 in eldant
                    eldiff0 = eldant[spwid0].eldiff0
                    eldiff1 = eldant[spwid0].eldiff1
                    eldiff = numpy.append(eldiff0, eldiff1)
                    if len(eldiff) > 0:
                        eldmax = numpy.max(numpy.abs(eldiff))
                        if eldmax >= threshold:
                            field_name = ms.fields[field_id].name
                            antenna_name = ms.antennas[antenna_id].name
                            LOG.warning('Elevation difference between ON and OFF for {} field {} antenna {} spw {} was {}deg'
                                        ' exceeding the threshold of {}deg'
                                        ''.format(ms.basename, field_name, antenna_name, list_spwids, eldmax, threshold))

        return result


@task_registry.set_equivalent_casa_task('hsd_skycal')
@task_registry.set_casa_commands_comment('Generates sky calibration table according to calibration strategy.')
class SDSkyCal(sessionutils.ParallelTemplate):
    """Class to generate sky calibration table."""

    Inputs = SDSkyCalInputs
    Task = SerialSDSkyCal


def compute_elevation_difference(context: 'Context', results: SDSkyCalResults) -> Dict:
    """Compute elevation difference.

    Args:
        context: Pipeline context.
        results: SDSkyCalResults instance.

    Returns:
        dictionary[field_id][antenna_id][spw_id]
            Value of the dictionary should be ElevationDifference and the value should
            contain the result from one MS (given that SDSkyCal is per-MS task).
    """
    ElevationDifference = collections.namedtuple('ElevationDifference',
                                                 ['timeon', 'elon', 'timecal', 'elcal',
                                                  'time0', 'eldiff0', 'time1', 'eldiff1'])

    if not isinstance(results, SDSkyCalResults):
        raise TypeError('Results type should be SDSkyCalResults')

    calapps = results.outcome

    resultdict = {}

    for calapp in calapps:
        calto = calapp.calto
        vis = calto.vis
        ms = context.observing_run.get_ms(vis)
        target_field = calto.field
        if target_field.isdigit():
            field_id_on = int(target_field)
        else:
            fields = ms.get_fields(name=target_field)
            assert len(fields) > 0
            field_id_on = fields[0].id

        #if ms.basename not in resultdict:
        #    resultdict[ms.basename] = {}

        antenna_ids = [ant.id for ant in ms.antennas]

        # representative spw
        science_spw = ms.get_spectral_windows(science_windows_only=True)
#         # choose representative spw based on representative frequency if it is available
#         if hasattr(ms, 'representative_target') and ms.representative_target[1] is not None:
#             qa = casa_tools.quanta
#             rep_freq = ms.representative_target[1]
#             centre_freqs = [qa.quantity(spw.centre_frequency.str_to_precision(16)) for spw in science_spw]
#             freq_diffs = [abs(qa.sub(cf, rep_freq).convert('Hz')['value']) for cf in centre_freqs]
#             spw_id = science_spw[numpy.argmin(freq_diffs)].id
#         else:
#             spw_id = science_spw[0].id

        calfroms = calapp.calfrom

        for calfrom in calfroms:
            caltable = calfrom.gaintable

            # FIELD_ID
            gainfield = calfrom.gainfield
            if gainfield.isdigit():
                field_id_off = int(gainfield)
            else:
                fields = ms.get_fields(name=gainfield)
                assert len(fields) > 0
                field_id_off = fields[0].id
            LOG.info('Computing elevation difference for "{}" Field ID {} (ON) and {} (OFF)'
                     ''.format(ms.basename, field_id_on, field_id_off))

            resultfield = {}

            for antenna_id in antenna_ids:

                resultant = {}

                for spw in science_spw:
                    spw_id = spw.id

                    # get timestamp from caltable
                    with casa_tools.TableReader(caltable) as tb:
                        selected = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}'.format(spw_id, antenna_id))
                        timecal = selected.getcol('TIME') / 86400.0  # sec -> day
                        selected.close()

                    # access DataTable to get elevation
                    ro_datatable_name = os.path.join(context.observing_run.ms_datatable_name,
                                                     os.path.basename(ms.origin_ms), 'RO')
                    with casa_tools.TableReader(ro_datatable_name) as tb:
                        selected = tb.query('IF=={}&&ANTENNA=={}&&FIELD_ID=={}&&SRCTYPE==0'
                                            ''.format(spw_id, antenna_id, field_id_on))
                        timeon = selected.getcol('TIME')
                        elon = selected.getcol('EL')
                        selected.close()
                        selected = tb.query('IF=={}&&ANTENNA=={}&&FIELD_ID=={}&&SRCTYPE!=0'
                                            ''.format(spw_id, antenna_id, field_id_off))
                        timeoff = selected.getcol('TIME')
                        eloff = selected.getcol('EL')
                        selected.close()

                    elcal = eloff[[numpy.argmin(numpy.abs(timeoff - t)) for t in timecal]]

                    del timeoff, eloff

                    eldiff0 = []
                    eldiff1 = []
                    time0 = []
                    time1 = []
                    for t, el in zip(timeon, elon):
                        dt = timecal - t
                        idx0 = numpy.where(dt < 0)[0]
                        if len(idx0) > 0:
                            i = numpy.argmax(timecal[idx0])
                            time0.append(t)
                            eldiff0.append(el - elcal[idx0[i]])
                        idx1 = numpy.where(dt >= 0)[0]
                        if len(idx1) > 0:
                            i = numpy.argmin(timecal[idx1])
                            time1.append(t)
                            eldiff1.append(el - elcal[idx1[i]])
                    eldiff0 = numpy.asarray(eldiff0)
                    eldiff1 = numpy.asarray(eldiff1)
                    time0 = numpy.asarray(time0)
                    time1 = numpy.asarray(time1)

                    result = ElevationDifference(timeon=timeon, elon=elon,
                                                 timecal=timecal, elcal=elcal,
                                                 time0=time0, eldiff0=eldiff0,
                                                 time1=time1, eldiff1=eldiff1)

                    resultant[spw_id] = result

                resultfield[antenna_id] = resultant

            resultdict[field_id_on] = resultfield

    return resultdict
