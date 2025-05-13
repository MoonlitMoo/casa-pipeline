"""Offline ATM correction stage."""
import collections
import os
from typing import List, Optional, Tuple, Union

import numpy as np

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.extern import SDcalatmcorr
from pipeline.h.heuristics import fieldnames
from pipeline.hsd.tasks.common.inspection_util import generate_ms, inspect_reduction_group, merge_reduction_group
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.utils import relative_path
from .. import common

LOG = logging.get_logger(__name__)


ATMModelParam = collections.namedtuple('ATMModelParam', 'atmtype maxalt dtem_dh h0')
ATMModelParam.__str__ = lambda self: f'atmtype {self.atmtype}, dtem_dh {self.dtem_dh}K/km, h0 {self.h0}km.'


# default atmtype list that is used when atmtype is 'auto'
DEFAULT_ATMTYPE_LIST = [1, 2, 3, 4]


class SDATMCorrectionInputs(vdp.StandardInputs):
    """Inputs class for SDATMCorrection task."""
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    parallel = sessionutils.parallel_inputs_impl()

    atmtype = vdp.VisDependentProperty(default='auto')
    dtem_dh = vdp.VisDependentProperty(default=-5.6)
    h0 = vdp.VisDependentProperty(default=2.0)
    maxalt = vdp.VisDependentProperty(default=120)
    intent = vdp.VisDependentProperty(default='TARGET')

    @atmtype.convert
    def atmtype(self, value: Union[int, str, List[Union[int, str]]]) -> Union[str, List[str]]:
        """Convert atmtype into str or a list of str.

        Args:
            value: atmtype value(s)

        Returns:
            atmtype as string type or a list of strings
        """
        # check if value is compatible with list
        if (not isinstance(value, (str, dict))) and isinstance(value, collections.abc.Iterable):
            list_value = list(value)
            value = [
                v if isinstance(v, str) else str(v) for v in list_value
            ]

            if len(value) == 1:
                value = value[0]
        else:
            value = str(value)
        return value

    def __to_float_value(self, value: Union[float, str, dict, List[Union[float, str, dict]]], default_unit: str) -> Union[float, List[float]]:
        """Convert input value into float value or list of float values.

        This method converts any value into float value. If input is a list,
        then return value is a list of float values obtained by converting
        each element of the input list. Return value(s) are interpreted
        as a quantity with default_unit.

        Args:
            value: Input value. The value can be a numerical value or
                   a quantity in the form of a dictionary (casa quantity)
                   or a string. A list of these values is also acceptable.
            default_unit: Unit string for conversion

        Returns:
            Float value or list of float values in the unit specified by
            default_unit. If the unit for input quantity is incompatible
            with default_unit, the method will emit a warning message and
            return value will be set to 0.
        """
        # check if value is compatible with list
        if (not isinstance(value, (str, dict))) and isinstance(value, collections.abc.Iterable):
            list_value = list(value)
            ret = [self.__to_float_value(v, default_unit) for v in list_value]

            if len(ret) == 1:
                ret = ret[0]

            return ret

        # non-list value
        qa = casa_tools.quanta
        if isinstance(value, dict):
            qvalue = value
        else:
            qvalue = qa.quantity(value)

        if qvalue['unit'] == '':
            ret = qvalue['value']
        elif qa.compare(qvalue, qa.quantity(0, default_unit)):
            ret = qa.convert(qvalue, default_unit)['value']
        else:
            LOG.warning(f'incompatible unit: input {value} requires unit {default_unit}')
            ret = 0.
        return ret

    @h0.convert
    def h0(self, value: Union[float, str, dict, List[Union[float, str, dict]]]) -> Union[float, List[float]]:
        """Convert any h0 value into float or a list of float.

        Input value(s) can be numerical value or a quantity in the
        form of a dictionary (casa quantity) or a string.
        A list of these values is also acceptable.

        Args:
            value: h0 value(s)

        Returns:
            h0 value(s) in the unit of km
        """
        return self.__to_float_value(value, 'km')

    @dtem_dh.convert
    def dtem_dh(self, value: Union[float, str, dict, List[Union[float, str, dict]]]) -> Union[float, List[float]]:
        """Convert any dtem_dh value into float or a list of float.

        Input value(s) can be numerical value or a quantity in the
        form of a dictionary (casa quantity) or a string.
        A list of these values is also acceptable.

        Args:
            value: dtem_dh value(s)

        Returns:
            dtem_dh value(s) in the unit of K/km
        """
        return self.__to_float_value(value, 'K/km')

    @maxalt.convert
    def maxalt(self, value: Union[float, str, dict, List[Union[float, str, dict]]]) -> Union[float, List[float]]:
        """Convert any maxalt value into float or a list of float.

        Input value(s) can be numerical value or a quantity in the
        form of a dictionary (casa quantity) or a string.
        A list of these values is also acceptable.

        Args:
            value: maxalt value(s)

        Returns:
            maxalt value(s) in the unit of km
        """
        return self.__to_float_value(value, 'km')

    @vdp.VisDependentProperty
    def infiles(self) -> str:
        """Return infiles.

        infiles is an alias of vis

        Returns:
            infiles string
        """
        return self.vis

    @infiles.convert
    def infiles(self, value: str) -> str:
        """Update infiles and vis consistently.

        Args:
            value: new infiles value

        Returns:
            input value
        """
        self.vis = value
        return value

    @vdp.VisDependentProperty
    def antenna(self) -> str:
        """Return antenna selection.

        By default, empty string (all antennas) is returned.

        Returns:
            antenna selection string
        """
        return ''

    @antenna.convert
    def antenna(self, value: str) -> str:
        """Convert antenna selection string.

        Args:
            value: input antenna selection

        Returns:
            converted antenna selection
        """
        antennas = self.ms.get_antenna(value)
        # if all antennas are selected, return ''
        if len(antennas) == len(self.ms.antennas):
            return ''
        return utils.find_ranges([a.id for a in antennas])

    @vdp.VisDependentProperty
    def field(self) -> str:
        """Return field selection string.

        By default, only fields that matches given intent are returned.

        Returns:
            field selection string
        """
        # this will give something like '0542+3243,0343+242'
        field_finder = fieldnames.IntentFieldnames()
        intent_fields = field_finder.calculate(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    @vdp.VisDependentProperty
    def spw(self) -> str:
        """Return spw selection string.

        By default, channelized spws are returned.

        Returns:
            spw selection string
        """
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @vdp.VisDependentProperty
    def pol(self) -> str:
        """Return pol selection string.

        By default, polarizatons corresponding to selected spws are selected.

        Returns:
            pol selecton string
        """
        # filters polarization by self.spw
        selected_spwids = [int(spwobj.id) for spwobj in self.ms.get_spectral_windows(self.spw, with_channels=True)]
        pols = set()
        for idx in selected_spwids:
            pols.update(self.ms.get_data_description(spw=idx).corr_axis)

        return ','.join(pols)

    def __init__(self,
                 context: Context,
                 atmtype: Optional[Union[int, str, List[int], List[str]]] = None,
                 dtem_dh: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] = None,
                 h0: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] = None,
                 maxalt: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] = None,
                 infiles: Optional[Union[str, List[str]]] = None,
                 antenna: Optional[Union[str, List[str]]] = None,
                 field: Optional[Union[str, List[str]]] = None,
                 spw: Optional[Union[str, List[str]]] = None,
                 pol: Optional[Union[str, List[str]]] = None,
                 parallel: Optional[Union[bool, str]] = None):
        """Initialize Inputs instance for hsd_atmcor.

        Args:
            context: pipeline context
            atmtype: enumeration for atmospheric transmission model, or 'auto'
            dtem_dh: temperature gradient [K/km]
            h0: scale height for water [km]. Defaults to None.
            maxalt: maximum altitude of the model [km]. Defaults to None.
            infiles: MS selection. Defaults to None.
            antenna: antenna selection. Defaults to None.
            field: field selection. Defaults to None.
            spw: spw selection. Defaults to None.
            pol: polarization selection. Defaults to None.
            parallel: Execute using CASA HPC functionality, if available.
                      Default is None, which intends to turn on parallel
                      processing if possible.
        """
        super().__init__()

        self.context = context
        self.atmtype = atmtype
        self.dtem_dh = dtem_dh
        self.h0 = h0
        self.maxalt = maxalt
        self.infiles = infiles
        self.antenna = antenna
        self.field = field
        self.spw = spw
        self.pol = pol
        self.parallel = parallel

    def _identify_datacolumn(self, vis: str) -> str:
        """Identify data column.

        Args:
            vis: MS name

        Raises:
            Exception: no datacolumn exists

        Returns:
            datacolumn parameter
        """
        datacolumn = ''
        with casa_tools.TableReader(vis) as tb:
            colnames = tb.colnames()

        names = (('CORRECTED_DATA', 'corrected'),
                 ('FLOAT_DATA', 'float_data'),
                 ('DATA', 'data'))
        for name, value in names:
            if name in colnames:
                datacolumn = value
                break

        if len(datacolumn) == 0:
            raise Exception('No datacolumn is found.')

        return datacolumn

    def get_caltable_from_callibrary(self) -> str:
        """Retrieve k2jycal caltable name from callibrary.

        Returns:
            Name of the caltable.
            Return empty string if k2jycal caltable is not applied.
        """
        applied_state = self.context.callibrary.applied
        calto = callibrary.CalTo(vis=self.vis)
        state_for_vis = applied_state.trimmed(self.context, calto)
        caltables = state_for_vis.get_caltable(caltypes=('amp', 'gaincal'))

        k2jycal_caltable = ''
        if len(caltables) > 0:
            k2jycal_caltable = caltables.pop()

        return k2jycal_caltable

    def get_gainfactor(self) -> Union[float, str]:
        """Retrieve k2jycal table from callibrary.

        Returns:
            name of the k2jycal table or 1.0
        """
        k2jycal_caltable = self.get_caltable_from_callibrary()
        gainfactor = 1.0
        if k2jycal_caltable:
            gainfactor = k2jycal_caltable
        return gainfactor

    def to_casa_args(self) -> dict:
        """Return task arguments for sdatmcor.

        Note that it might return invalid argument list when
        the user intends to run heuristics for ATM parameter.
        Please check if require_atm_heuristics method returns
        True to make sure the return value is valid.

        Returns:
            task arguments for sdatmcor
        """
        args = super().to_casa_args()

        # infile
        args.pop('infiles', None)
        infile = args.pop('vis')
        args['infile'] = infile

        # datacolumn
        args['datacolumn'] = self._identify_datacolumn(infile)

        # atmtype
        if isinstance(args['atmtype'], str) and args['atmtype'].isdigit():
            args['atmtype'] = int(args['atmtype'])

        # maxalt is not available
        args.pop('maxalt')

        # outfile
        if 'outfile' not in args:
            basename = os.path.basename(infile.rstrip('/'))
            suffix = '.atmcor.atmtype{}'.format(args['atmtype'])
            outfile = basename + suffix
            args['outfile'] = relative_path(os.path.join(self.output_dir, outfile))

        # ganfactor
        args['gainfactor'] = self.get_gainfactor()

        # overwrite is always True
        args['overwrite'] = True

        # spw -> outputspw
        args['outputspw'] = args.pop('spw', '')

        # pol -> correlation
        args['correlation'] = args.pop('pol', '')

        # correlation selection should be empty
        # to avoid strange error in VI/VB2 framework
        args['correlation'] = ''

        # process ON_SOURCE data only
        args['intent'] = 'OBSERVE_TARGET#ON_SOURCE'

        # remove parallel
        del args['parallel']

        return args

    def require_atm_heuristics(self) -> bool:
        """Check if ATM heuristics is required.

        ATM heuristics is required if any of the following
        conditions are met.

            - atmtype is either 'auto' or list of type IDs
            - dtem_dh is a list of float values
            - h0 is a list of float values

        Returns:
            True if ATM heuristics is required. Otherwise, False.
        """
        check_atmtype = isinstance(self.atmtype, list) or self.atmtype.lower() == 'auto'
        check_dtem_dh = isinstance(self.dtem_dh, list)
        check_h0 = isinstance(self.h0, list)
        return check_atmtype or check_dtem_dh or check_h0


class SDATMCorrectionResults(common.SingleDishResults):
    """Results instance for hsd_atmcor."""

    def __init__(self,
                 task: Optional[basetask.StandardTaskTemplate] =None,
                 success: Optional[bool] =None,
                 outcome: Optional[dict] =None):
        """Initialize results instance for hsd_atmcor.

        The outcome must be a dict that contains:

            - 'task_args': actual argument list of the sdatmcor
                           (The "inputs" dictionary associated with
                           the results object holds "nominal"
                           argument list)
            - 'atm_heuristics': status string of the ATM heuristics
            - 'model_list': list of attempted ATM models
            - 'best_model_index': index for the best ATM model

        Args:
            task: task class. Defaults to None.
            success: task execution was successful or not. Defaults to None.
            outcome: outcome of the task execution. Defaults to None.
        """
        super().__init__(task, success, outcome)
        self.task_args = outcome['task_args']
        self.atmcor_ms_name = self.task_args['outfile']
        self.best_atmtype = self.task_args['atmtype']
        self.atm_heuristics = outcome['atm_heuristics']
        self.best_model_index = outcome['best_model_index']
        self.model_list = outcome['model_list']
        self.out_mses = []

    def merge_with_context(self, context: Context):
        """Merge execution result of atmcor stage into pipeline context.

        Args:
            pipeline context
        """
        super().merge_with_context(context)

        # register output MS domain object and reduction_group to context
        target = context.observing_run
        for ms in self.out_mses:
            # remove existing MS in context if the same MS is already in list.
            oldms_index = None
            for index, oldms in enumerate(target.get_measurement_sets()):
                if ms.name == oldms.name:
                    oldms_index = index
                    break
            if oldms_index is not None:
                LOG.info('Replace {} in context'.format(ms.name))
                del target.measurement_sets[oldms_index]

            # Adding mses to context
            LOG.info('Adding {} to context'.format(ms.name))
            target.add_measurement_set(ms)
            # Initialize callibrary
            calto = callibrary.CalTo(vis=ms.name)
            LOG.info('Registering {} with callibrary'.format(ms.name))
            context.callibrary.add(calto, [])
            # register output MS to processing group
            reduction_group = inspect_reduction_group(ms)
            merge_reduction_group(target, reduction_group)

    def _outcome_name(self) -> str:
        """Return representative string for the outcome.

        Any string that represents outcome is returned.
        In case of hsd_atmcor, output MS name is returned.

        Returns:
            output MS name for sdatmcor
        """
        return os.path.basename(self.atmcor_ms_name)


@task_registry.set_equivalent_casa_task('hsd_atmcor')
@task_registry.set_casa_commands_comment(
    'Apply offline correction of atmospheric transmission model.'
)
class SerialSDATMCorrection(basetask.StandardTaskTemplate):
    """Offline ATM correction task."""

    Inputs = SDATMCorrectionInputs

    def prepare(self) -> SDATMCorrectionResults:
        """Execute task and produce results instance.

        Raises:
            Exception: execution of sdatmcor was failed

        Returns:
            results instance for hsd_atmcor stage
        """
        # args for sdatmcor
        if self.inputs.require_atm_heuristics():
            # select best ATM model
            atm_heuristics, args, best_model_index, model_list = self._perform_atm_heuristics()
        else:
            atm_heuristics = 'N'
            args = self.inputs.to_casa_args()
            best_model_index = -1
            model_list = [
                ATMModelParam(args['atmtype'], self.inputs.maxalt, args['dtem_dh'], args['h0'])
            ]

        LOG.info('Processing parameter for sdatmcor: %s', args)
        job = casa_tasks.sdatmcor(**args)
        task_exec_status = self._executor.execute(job)
        LOG.info('atmcor: task_exec_status = %s', task_exec_status)

        if not os.path.exists(args['outfile']):
            raise Exception('Output MS does not exist. It seems sdatmcor failed.')

        if task_exec_status is None:
            # no news is good news, this is a sign of success
            is_successful = True
        elif task_exec_status is False:
            # it indicates any problem
            is_successful = False
        else:
            # unexpected, mark as failed
            is_successful = False

        results = SDATMCorrectionResults(
            task=self.__class__,
            success=is_successful,
            outcome={
                'task_args': args,
                'atm_heuristics': atm_heuristics,
                'best_model_index': best_model_index,
                'model_list': model_list,
            }
        )

        return results

    def analyse(self, result: SDATMCorrectionResults) -> SDATMCorrectionResults:
        """Analyse results produced by prepare method.

        Generate domain object of MS with offline ATM correction.

        Args:
            result: results instance

        Returns:
            input results instance
        """
        in_ms = self.inputs.ms
        new_ms = generate_ms(result.atmcor_ms_name, in_ms)
        new_ms.set_data_column(DataType.ATMCORR, 'DATA')
        result.out_mses.append(new_ms)
        return result

    def _perform_atm_heuristics(self) -> Tuple[str, dict, int, List[Tuple[int, float, float, float]]]:
        """Perform ATM model heuristics.

        Perform ATM model heuristics, SDcalatmcorr.

        Returns:
            Four tuple, status of ATM model heuristics, argument list for sdatmcor,
            index of best ATM model, and list of attempted ATM models.
        """
        # create weblog directry
        stage_number = self.inputs.context.task_counter
        stage_dir = os.path.join(
            self.inputs.context.report_dir,
            f'stage{stage_number}'
        )
        os.makedirs(stage_dir, exist_ok=True)

        # perform atmtype heuristics if atmtype is 'auto'
        # run Harold's script here
        LOG.info('Performing atmtype heuristics')
        atm_heuristics = 'Fallback'
        default_model = ATMModelParam(atmtype=1, maxalt=120, dtem_dh=-5.6, h0=2.0)
        # best_model will fall back to default_model if heuristics is failed
        best_model = default_model
        args = self.inputs.to_casa_args()
        ms_name = args['infile']
        model_list = [default_model]
        best_model_index = -1
        LOG.info(f'default_model: {default_model}')

        # handle list inputs
        if isinstance(args['atmtype'], list):
            atmtype_list = [int(x) for x in args['atmtype']]
        else:
            # should be 'auto'
            atmtype_list = DEFAULT_ATMTYPE_LIST

        try:
            heuristics_result = SDcalatmcorr.selectModelParams(
                mslist=[ms_name],
                context=self.inputs.context,
                decisionmetric='intabsdiff',
                atmtype=atmtype_list,
                maxalt=self.inputs.maxalt,
                lapserate=self.inputs.dtem_dh,
                scaleht=self.inputs.h0,
                plotsfolder=stage_dir,
                defatmtype=default_model.atmtype,
                defmaxalt=default_model.maxalt,
                deflapserate=default_model.dtem_dh,
                defscaleht=default_model.h0
            )
            best_model = ATMModelParam(*heuristics_result[0][ms_name])

            status = heuristics_result[3][ms_name]
            if status == 'bestfitmodel':
                atm_heuristics = 'Y'
                model_list = [ATMModelParam(*x) for x in heuristics_result[1][ms_name]]
                best_model_index = model_list.index(best_model)
                LOG.info(f'Best ATM model is {best_model}.')
            else:
                LOG.info(f'ATM heuristics failed. Using default model {default_model}.')
                model_list = [best_model]

        except Exception as e:
            LOG.info(f'ATM heuristics failed. Falling back to default model {default_model}.')
            LOG.info('Original error:')
            LOG.info(str(e))
            if LOG.isEnabledFor(logging.DEBUG):
                import traceback
                LOG.debug(traceback.format_exc())

        # construct argument list for sdatmcor
        inputs_local = utils.pickle_copy(self.inputs)
        inputs_local.atmtype = best_model.atmtype
        inputs_local.maxalt = best_model.maxalt
        inputs_local.dtem_dh = best_model.dtem_dh
        inputs_local.h0 = best_model.h0
        args = inputs_local.to_casa_args()

        return atm_heuristics, args, best_model_index, model_list


class HpcSDATMCorrection(sessionutils.ParallelTemplate):
    """Parallel implementation of offline ATM correction task."""

    Inputs = SDATMCorrectionInputs
    Task = SerialSDATMCorrection
