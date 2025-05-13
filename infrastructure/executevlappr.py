"""
Execute the pipeline processing request.

Code first as module and convert to class if appropriate
Factor and document properly  when details worked out

Raises:
    exceptions.PipelineException
"""
import os
import sys
import traceback
from typing import TYPE_CHECKING, Tuple, Union

from . import casa_tools
from . import exceptions
from . import project
from . import utils
from .. import cli
from .executeppr import _getCommands, _getIntents, _getPerformanceParameters, _getPprObject, save_existing_context

if TYPE_CHECKING:
    from pipeline.extern.XmlObjectifier import XmlObject


def executeppr(pprXmlFile: str, importonly: bool = True, loglevel: str = 'info',
               plotlevel: str = 'summary', interactive: bool = True) -> None:
    """
    Runs Pipeline Processing Request (PPR).

    Executes pipeline tasks based on instructions described in pprXmlFile.

    Args:
        pprXmlFile: A path to PPR file.
        importonly: Whether or not to indicate to stop processing after
            importing data. If True, execution of PPR stops after
            h*_importdata stage. The parameter has no effect if there is no
            h*_importdata stage in PPR.
        loglevel: A logging level. Available levels are, 'critical', 'error',
            'warning', 'info', 'debug', 'todo', and 'trace'.
        plotlevel: A plot level. Available levels are, 'all', 'default', and
            'summary'
        interactive: If True, print pipeline log to STDOUT.
    """
    # save existing context to disk
    save_existing_context()

    # Useful mode parameters
    echo_to_screen = interactive
    workingDir = None

    try:
        # Decode the processing request
        casa_tools.post_to_log("Analyzing pipeline processing request ...", echo_to_screen=echo_to_screen)
        info, structure, relativePath, intentsDict, asdmList, procedureName, commandsList = \
            _getFirstRequest(pprXmlFile)

        # Set the directories
        if 'SCIPIPE_ROOTDIR' in os.environ:
            workingDir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'working')
            rawDir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'rawdata')
        else:
            # PIPE-2093: if $SCIPIPE_ROOTDIR doesn't exist, we likely run in a local dev/test environment.
            # Then we will override the typical production workingDir/rawDIR values that are traditionally
            # constructed from $SCIPIPE_ROOTDIR and the PPR <RelativePath> field. Note that we assume that
            # any executeppr call here happens inside the "working/" directory.
            workingDir = os.path.abspath(os.path.join('..', 'working'))
            rawDir = os.path.abspath(os.path.join('..', 'rawdata'))

        # Get the pipeline context
        context = cli.h_init(loglevel=loglevel, plotlevel=plotlevel)

    except Exception:
        casa_tools.post_to_log("Beginning pipeline run ...", echo_to_screen=echo_to_screen)
        casa_tools.post_to_log("For processing request: " + pprXmlFile, echo_to_screen=echo_to_screen)
        traceback.print_exc(file=sys.stdout)
        errstr = traceback.format_exc()
        casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)
        casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
        errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
        return

    # Request decoded, starting run.
    casa_tools.post_to_log("Beginning pipeline run ...", echo_to_screen=echo_to_screen)
    casa_tools.post_to_log("For processing request: " + pprXmlFile, echo_to_screen=echo_to_screen)

    # Check for common error conditions.
    if relativePath == "":
        casa_tools.post_to_log("    Undefined relative data path", echo_to_screen=echo_to_screen)
        casa_tools.post_to_log("Terminating pipeline execution ...", echo_to_screen=echo_to_screen)
        errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
        return
    elif len(asdmList) < 1:
        casa_tools.post_to_log("    Empty ASDM list", echo_to_screen=echo_to_screen)
        casa_tools.post_to_log("Terminating pipeline execution ...", echo_to_screen=echo_to_screen)
        errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
        return
    elif len(commandsList) < 1:
        casa_tools.post_to_log("    Empty commands list", echo_to_screen=echo_to_screen)
        casa_tools.post_to_log("Terminating pipeline execution ...", echo_to_screen=echo_to_screen)
        errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
        return

    # List project summary information
    casa_tools.post_to_log("Project summary", echo_to_screen=echo_to_screen)
    for item in info:
        casa_tools.post_to_log("    " + item[1][0] + item[1][1], echo_to_screen=echo_to_screen)
    ds = dict(info)
    context.project_summary = project.ProjectSummary(
        proposal_code=ds['proposal_code'][1],
        proposal_title='unknown',
        piname='unknown',
        observatory=ds['observatory'][1],
        telescope=ds['telescope'][1])

    # List project structure information
    casa_tools.post_to_log("Project structure", echo_to_screen=echo_to_screen)
    for item in structure:
        casa_tools.post_to_log("    " + item[1][0] + item[1][1], echo_to_screen=echo_to_screen)

    context.project_structure = project.ProjectStructure(
        ppr_file=pprXmlFile,
        recipe_name=procedureName)

    # Create performance parameters object
    context.project_performance_parameters = _getPerformanceParameters(intentsDict)

    # Print the relative path
    casa_tools.post_to_log("Directory structure", echo_to_screen=echo_to_screen)
    casa_tools.post_to_log("    Working directory: " + workingDir, echo_to_screen=echo_to_screen)
    casa_tools.post_to_log("    Raw data directory: " + rawDir, echo_to_screen=echo_to_screen)

    # Construct the ASDM list
    casa_tools.post_to_log("Number of ASDMs: " + str(len(asdmList)), echo_to_screen=echo_to_screen)
    files = []
    sessions = []
    defsession = 'session_1'
    for asdm in asdmList:
        session = defsession
        sessions.append(session)
        files.append(os.path.join(rawDir, asdm[1]))
        casa_tools.post_to_log("    Session: " + session + "  ASDM: " + asdm[1], echo_to_screen=echo_to_screen)

    # Paths for all these ASDM should be the same
    #     Add check for this ?

    # Beginning execution
    casa_tools.post_to_log("\nStarting procedure execution ...\n", echo_to_screen=echo_to_screen)
    casa_tools.post_to_log("Procedure name: " + procedureName + "\n", echo_to_screen=echo_to_screen)

    # Names of import tasks that need special treatment:
    import_tasks = ('h_importdata', 'hifa_importdata', 'hifv_importdata')
    restore_tasks = ('h_restoredata', 'hifv_restoredata')
    breakpoint_task = 'breakpoint'
    # Loop over the commands
    for command in commandsList:

        # Get task name and arguments lists.
        pipeline_task_name = command[0]
        task_args = command[1]
        casa_tools.set_log_origin(fromwhere=pipeline_task_name)

        # PIPE-2388: Skipping breakpoint command
        if pipeline_task_name == breakpoint_task:
            casa_tools.post_to_log("Found {}, not performing any action.".format(pipeline_task_name), echo_to_screen=echo_to_screen)
            continue

        # Execute the command
        casa_tools.post_to_log("Executing command ..." + pipeline_task_name, echo_to_screen=echo_to_screen)
        try:
            pipeline_task = cli.get_pipeline_task_with_name(pipeline_task_name)

            # List parameters
            for keyword, value in task_args.items():
                casa_tools.post_to_log("    Parameter: " + keyword + " = " + repr(value), echo_to_screen=echo_to_screen)

            # For import/restore tasks, set vis and session explicitly (not inferred from context).
            if pipeline_task_name in import_tasks or pipeline_task_name in restore_tasks:
                task_args['vis'] = files
                task_args['session'] = sessions

            # If spectral mode is set to True, skip the Hanning task.
            spectral_mode = intentsDict.get('SPECTRAL_MODE', False)
            if spectral_mode and pipeline_task_name == 'hifv_hanning':
                casa_tools.post_to_log("SPECTRAL_MODE=True.  Hanning smoothing will not be executed.")
                continue

            results = pipeline_task(**task_args)
            casa_tools.post_to_log('Results ' + str(results), echo_to_screen=echo_to_screen)

            if importonly and pipeline_task_name in import_tasks:
                casa_tools.post_to_log("Terminating execution after running " + pipeline_task_name,
                                        echo_to_screen=echo_to_screen)
                break

        except Exception:
            # Log message if an exception occurred that was not handled by
            # standardtask template (not turned into failed task result).
            casa_tools.post_to_log("Unhandled error in executevlappr while running pipeline task {}"
                                    "".format(pipeline_task_name), echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)
            errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
            break

        # Stop execution if result is a failed task result or a list
        # containing a failed task result.
        tracebacks = utils.get_tracebacks(results)
        if len(tracebacks) > 0:
            # Save the context
            cli.h_save()

            casa_tools.set_log_origin(fromwhere='')

            errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
            previous_tracebacks_as_string = "{}".format("\n".join([tb for tb in tracebacks]))
            raise exceptions.PipelineException(previous_tracebacks_as_string)

    # Save the context
    cli.h_save()

    casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)

    casa_tools.set_log_origin(fromwhere='')

    return


# Return the intents list, the ASDM list, and the processing commands
# for the first processing request. There should in general be only
# one but the schema permits more. Generalize later if necessary.
def _getFirstRequest(pprXmlFile: 'XmlObject') -> Union[Tuple[list, str, dict, list, list], Tuple[list, list, str, dict, list, str, list]]:
    # Initialize
    info = []
    relativePath = ""
    intentsDict = {}
    commandsList = []
    asdmList = []

    # Turn the XML file into an object
    pprObject = _getPprObject(pprXmlFile=pprXmlFile)

    # Count the processing requests.
    numRequests = _getNumRequests(pprObject=pprObject)
    if numRequests <= 0:
        casa_tools.post_to_log("Terminating execution: No valid processing requests")
        return info, relativePath, intentsDict, asdmList, commandsList
    elif numRequests > 1:
        casa_tools.post_to_log("Warning: More than one processing request")
    casa_tools.post_to_log('Number of processing requests: ', numRequests)

    # Get brief project summary
    info = _getProjectSummary(pprObject)

    # Get project structure. Set to empty list for VLA.
    structure = []

    # Get the intents dictionary
    numIntents, intentsDict = _getIntents(pprObject=pprObject, requestId=0, numRequests=numRequests)
    casa_tools.post_to_log('Number of intents: {}'.format(numIntents))
    casa_tools.post_to_log('Intents dictionary: {}'.format(intentsDict))

    # Get the commands list
    procedureName, numCommands, commandsList = _getCommands(pprObject=pprObject, requestId=0, numRequests=numRequests)
    casa_tools.post_to_log('Number of commands: {}'.format(numCommands))
    casa_tools.post_to_log('Commands list: {}'.format(commandsList))

    # Count the scheduling block sets. Normally there should be only
    # one although the schema allows multiple sets. Check for this
    # condition and process only the first.
    numSbSets = _getNumSchedBlockSets(pprObject=pprObject, requestId=0, numRequests=numRequests)
    if numSbSets <= 0:
        casa_tools.post_to_log("Terminating execution: No valid scheduling block sets")
        return info, relativePath, intentsDict, asdmList, commandsList
    elif numSbSets > 1:
        casa_tools.post_to_log("Warning: More than one scheduling block set")
    casa_tools.post_to_log('Number of scheduling block sets: {}'.format(numSbSets))

    # Get the ASDM list
    relativePath, numAsdms, asdmList = _getAsdmList(pprObject=pprObject, numSbSets=numSbSets, numRequests=numRequests)
    casa_tools.post_to_log('Relative path: {}'.format(relativePath))
    casa_tools.post_to_log('Number of Asdms: {}'.format(numAsdms))
    casa_tools.post_to_log('ASDM list: {}'.format(asdmList))

    return info, structure, relativePath, intentsDict, asdmList, procedureName, commandsList


# Given the pipeline processing request object print some project summary
# information. Returns a list of tuples to preserve order (key, (prompt, value))
def _getProjectSummary(pprObject: 'XmlObject') -> list:
    ppr_summary = pprObject.SciPipeRequest.ProjectSummary
    summaryList = []
    summaryList.append(('proposal_code', ('Proposal code: ', ppr_summary.ProposalCode.getValue())))
    summaryList.append(('observatory', ('Observatory: ', ppr_summary.Observatory.getValue())))
    summaryList.append(('telescope', ('Telescope: ', ppr_summary.Telescope.getValue())))
    summaryList.append(('processing_site', ('Processsing site: ', ppr_summary.ProcessingSite.getValue())))
    summaryList.append(('operator', ('Operator: ', ppr_summary.Operator.getValue())))
    summaryList.append(('mode', ('Mode: ', ppr_summary.Mode.getValue())))
    return summaryList


# Given the pipeline processing request object return the number of processing
# requests. For EVLA this should always be 1 but check. Assume a single scheduling block
# per processing request.
def _getNumRequests(pprObject: 'XmlObject') -> int:
    ppr_prequests = pprObject.SciPipeRequest.ProcessingRequests

    numRequests = 0

    # Try single element / single scheduling block first.
    try:
        relative_path = ppr_prequests.ProcessingRequest.DataSet.RelativePath.getValue()
        numRequests = 1
        return numRequests
    except Exception:
        pass

    # Next try multiple processing requests / single scheduling block
    search = 1
    while search:
        try:
            relative_path = ppr_prequests.ProcessingRequest[numRequests].DataSet.RelativePath.getValue()
            numRequests = numRequests + 1
        except Exception:
            search = 0
            if numRequests > 0:
                return numRequests
            else:
                pass

    # Return the number of requests.
    return numRequests


# Given the pipeline processing request object return the number of scheduling
# block sets. For the EVLA there can be only one.
def _getNumSchedBlockSets(pprObject: 'XmlObject', requestId: int, numRequests: int) -> int:
    if numRequests == 1:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.DataSet
    else:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest[requestId].DataSet

    try:
        path = ppr_dset.RelativePath.getValue()
        numSchedBlockSets = 1
    except Exception:
        numSchedBlockSets = 0

    return numSchedBlockSets


# Given the pipeline processing request object return a list of ASDMs
# where each element in the list is a tuple consisting of the path
# to the ASDM, the name of the ASDM, and the UID of the ASDM.
def _getAsdmList(pprObject: 'XmlObject', numSbSets: int, numRequests: int) -> Tuple[str, int, list]:
    if numRequests == 1:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.DataSet
        if numSbSets == 1:
            ppr_dset = ppr_dset
            relativePath = ppr_dset.RelativePath.getValue()
        else:
            ppr_dset = ppr_dset
            relativePath = ""
    else:
        relativePath = ""

    asdmList = []
    try:
        asdmName = ppr_dset.SdmIdentifier.getValue()
        asdmUid = asdmName
        asdmList.append((relativePath, asdmName, asdmUid))
        numAsdms = 1
    except Exception:
        numAsdms = 0

    return relativePath, numAsdms, asdmList
