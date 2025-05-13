import os
import string

from casatasks.private import flaghelper

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure.filenamer import sanitize_for_ms

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'Flagtargetsdata',
    'FlagtargetsdataInputs',
    'FlagtargetsdataResults'
]


class FlagtargetsdataInputs(vdp.StandardInputs):
    """Defines the input parameters for the `hifv_flagtargetsdata` pipeline task.

    This class is used for flagging target data in VLA measurement sets (MSes). It 
    extends `vdp.StandardInputs` and provides attributes and methods to manage 
    relevant input parameters for the `flagdata` task in CASA.

    Attributes:
        processing_data_type (list[DataType]): Defines the priority order of input 
            visibility data types.
        flagbackup (vdp.VisDependentProperty): Determines whether a backup of the 
            flagging state should be created before applying flags. Defaults to `True`.
        template (vdp.VisDependentProperty): Indicates whether a flagging template 
            should be used. Defaults to `True`.
        filetemplate (vdp.VisDependentProperty): The filename of the flagging 
            template. This template is applied to all relevant measurement sets. 
            The property is automatically derived from the `vis` input.
        inpfile (vdp.VisDependentProperty): The full path to the flagging command 
            file. This file is generated based on `vis` and stored in `output_dir`.

    Args:
        context (object): The execution context for the pipeline.
        vis (str, optional): The name of the pre-split measurement set.
        output_dir (str, optional): The directory where output files will be stored.
        flagbackup (bool, optional): Whether to create a backup of flagging data 
            before applying flags. Defaults to `None`, which resolves to the class default.
        template (bool, optional): Whether to use a flagging template. Defaults to 
            `None`, which resolves to the class default.
        filetemplate (str, optional): The filename of the flagging template to use. 
            Flags from this template will be applied to all relevant measurement sets.

    Methods:
        to_casa_args(vis):
            Converts the class attributes into a dictionary of parameters required 
            for the CASA `flagdata` task.

    """
    # Search order of input vis
    # PIPE-2313: moved REGCAL_CONTLINE_SCIENCE to end of priority in rare case that the
    # others don't exist
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL,
                            DataType.RAW,
                            DataType.REGCAL_CONTLINE_SCIENCE]

    flagbackup = vdp.VisDependentProperty(default=True)
    template = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def filetemplate(self):
        vis_root = sanitize_for_ms(self.vis)
        return vis_root + '.flagtargetstemplate.txt'

    @filetemplate.convert
    def filetemplate(self, value):
        if isinstance(value, str):
            return list(
                value.replace('[', '').replace(']', '').replace("'", "").split(',')
                )
        else:
            return value

    @vdp.VisDependentProperty
    def inpfile(self):
        vis_root = sanitize_for_ms(self.vis)
        return os.path.join(self.output_dir, vis_root + '.flagtargetscmds.txt')

    def __init__(
            self,
            context,
            vis=None,
            output_dir=None,
            flagbackup=None,
            template=None,
            filetemplate=None
            ):
        """
        Args:
            vis (str): String name of the pre-split measurement set
            output_dir (str):  Output directory
            flagbackup (bool):  Back up flags or not
            template (bool):  Used template or not
            filetemplate (str):  String filename of the flagging template to use; flags
              from template will be applied to all relevant MSes
        """

        super().__init__()

        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.flagbackup = flagbackup
        self.template = template
        self.filetemplate = filetemplate

    def to_casa_args(self, vis):
        """
        Translate the input parameters of this class to task parameters
        required by the CASA task flagdata. The returned object is a
        dictionary of flagdata arguments as keyword/value pairs.

        Args:
            vis (str): String name of the measurement set to be flagged

        Return:
            Dict: dictionary of CASA task inputs
        """
        return {'vis': vis,
                'mode': 'list',
                'action': 'apply',
                'inpfile': self.inpfile,
                'flagbackup': self.flagbackup}


class FlagtargetsdataResults(basetask.Results):
    """Stores and processes results for the `hifv_flagtargetsdata` pipeline task.

    This class handles the results of flagging operations performed on VLA measurement 
    sets (MSes). It provides access to flagging summaries, executed flagging commands, 
    and the list of processed measurement sets. It extends `basetask.Results`.

    Attributes:
        pipeline_casa_task (str): The name of the CASA task associated with these results, 
            set to `'Flagtargetsdata'`.
        summaries (list): A list of flagging summaries, where each summary contains 
            details about flagged and total data points.
        _flagcmds (list): A list of string flagging commands executed during processing.
        mses (list): A list of measurement sets that were processed.

    Args:
        summaries (list): A list containing flagging summaries for each processed MS.
        flagcmds (list): A list of string commands that were used for flagging.
        mses (list, optional): A list of measurement sets that were processed. 
            Defaults to an empty list if not provided.

    Methods:
        flagcmds():
            Returns the list of flagging commands executed.

        merge_with_context(context):
            Merges results with a given pipeline context.
            See `pipeline.infrastructure.api.Results.merge_with_context` for details.

        __repr__():
            Generates a human-readable string representation of the flagging results,
            summarizing the flagging statistics for each processed measurement set.
    """
    def __init__(self, summaries, flagcmds, mses=None):
        """
        Args:
            summaries (List):  Flagging summaries
            flagcmds (List):  List of string flagging commands
            mses (List): List of measurement sets that were processed
        """
        if mses is None:
            mses = []
        super().__init__()
        self.pipeline_casa_task = 'Flagtargetsdata'
        self.summaries = summaries
        self._flagcmds = flagcmds
        self.mses = mses

    def flagcmds(self):
        return self._flagcmds

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        pass

    def __repr__(self):
        # Step through the summary list and print a few things.
        # SUBTRACT flag counts from previous agents, because the counts are
        # cumulative.
        s = 'Flagtargetsdata flagging results:\n'

        # iterate through list of summaries
        for vis_idx, summary in enumerate(self.summaries):
            s += f'\t{self.mses[vis_idx].name}\n'
            for idx in range(0, len(summary)):
                flagcount = int(summary[idx]['flagged'])
                totalcount = int(summary[idx]['total'])

                # From the second summary onwards, subtract counts from the previous
                # one
                if idx > 0:
                    flagcount = flagcount - int(summary[idx-1]['flagged'])

                countper = 100.0*flagcount/totalcount
                s += (f"\t\tSummary {idx} ({summary[idx]['name']}) :  "
                      f"Flagged : {flagcount} out of {totalcount} ({countper:.2f}%)\n")

        return s


@task_registry.set_equivalent_casa_task('hifv_flagtargetsdata')
@task_registry.set_casa_commands_comment('Flagtargetsdata')
class Flagtargetsdata(basetask.StandardTaskTemplate):
    """Handles the `hifv_flagtargetsdata` pipeline task for VLA measurement sets.

    This class performs automated flagging of target data on VLA measurement sets (MSes)
    using predefined flagging commands and templates. It inherits from 
    `basetask.StandardTaskTemplate` and integrates with CASA's `flagdata` task.

    Attributes:
        Inputs (class): The input parameters class for the task (`FlagtargetsdataInputs`).

    Methods:
        prepare():
            Executes the flagging process, applying the necessary flagging commands 
            to the measurement sets and generating flagging summaries.

        analyse(results):
            Analyzes the flagging results. This implementation simply returns the 
            results unchanged.

        _get_flag_commands():
            Generates a list of flagging commands to be executed by CASA's `flagdata` 
            task.

        _read_flagfile(filename):
            Reads a flagging template file, filtering out comments and empty lines, 
            and returns a list of valid flagging commands.

        _create_mses(inputs):
            Creates and returns a list of measurement sets (MSes) to be processed, 
            checking for various data types.

    Returns:
        FlagtargetsdataResults: An object containing flagging summaries, executed 
        flagging commands, and the processed measurement sets.
    """
    Inputs = FlagtargetsdataInputs

    def prepare(self):

        inputs = self.inputs

        mses = self._create_mses(inputs)

        summaries = []
        flag_cmds_list = []
        for ms in mses:
            flag_cmds = self._get_flag_commands()
            flag_str = '\n'.join(flag_cmds)

            with open(inputs.inpfile, 'w') as stream:
                stream.writelines(flag_str)

            LOG.debug('Flag commands for %s:\n%s', ms.name, flag_str)

            # Map the pipeline inputs to a dictionary of CASA task arguments
            task_args = inputs.to_casa_args(ms.name)

            # create and execute a flagdata job using these task arguments
            job = casa_tasks.flagdata(**task_args)
            summary_dict = self._executor.execute(job)

            agent_summaries = dict((v['name'], v) for v in summary_dict.values())

            ordered_agents = ['before', 'template']

            summary_reps = [agent_summaries[agent]
                            for agent in ordered_agents
                            if agent in agent_summaries]
            summaries.append(summary_reps)
            flag_cmds_list.append(flag_cmds)

        return FlagtargetsdataResults(
            summaries=summaries, flagcmds=flag_cmds_list, mses=mses
            )

    def analyse(self, results):
        """
        Analyse the results of the flagging operation.

        This method does not perform any analysis, so the results object is
        returned exactly as-is, with no data massaging or results items
        added. If additional statistics needed to be calculated based on the
        post-flagging state, this would be a good place to do it.
        """
        return results

    def _get_flag_commands(self):
        """
        Get the flagging commands as a string suitable for flagdata.
        """
        # create a local variable for the inputs associated with this instance
        inputs = self.inputs

        # create list which will hold the flagging commands
        flag_cmds = ["mode='summary' name='before'"]

        # flag template?
        if inputs.template:
            if not os.path.exists(inputs.filetemplate):
                LOG.warning('Template flag file \'%s\' for \'%s\' not found.'
                            % (inputs.filetemplate, inputs.ms.basename))
            else:
                flag_cmds.extend(self._read_flagfile(inputs.filetemplate))
            flag_cmds.append("mode='summary' name='template'")

        return flag_cmds

    @staticmethod
    def _read_flagfile(filename):
        if not os.path.exists(filename):
            LOG.warning('%s does not exist' % filename)
            return []

        # strip out comments and empty lines to leave the real commands.
        # This is so we can compare the number of valid commands to the number
        # of commands specified in the file and complain if they differ
        return [cmd for cmd in flaghelper.readFile(filename)
                if not cmd.strip().startswith('#')
                and not all(c in string.whitespace for c in cmd)]

    def _create_mses(self, inputs):
        """
        Create the MS list for multi-ms processing; checks for various datatypes
        """

        mses = []
        for dtype in [DataType.REGCAL_CONT_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE]:
            ms = inputs.context.observing_run.get_measurement_sets_of_type([dtype])
            if ms:
                mses.append(ms[0])

        return mses
