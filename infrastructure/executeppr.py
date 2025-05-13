"""
Execute the pipeline processing request.

Code first as module and convert to class if appropriate
Factor and document properly  when details worked out

Raises:
    exceptions.PipelineException
"""
import os
import shutil
import sys
import traceback
from typing import TYPE_CHECKING, Tuple, Union

from ..extern import XmlObjectifier

from . import casa_tools
from . import exceptions
from . import filenamer
from . import project
from . import utils
from .. import cli

if TYPE_CHECKING:
    from . launcher import Context


def executeppr(pprXmlFile: str, importonly: bool = True, breakpoint: str = 'breakpoint', bpaction: str = 'ignore',
               loglevel: str = 'info', plotlevel: str = 'default', interactive: bool = True) -> None:
    """
    Runs Pipeline Processing Request (PPR).

    Executes pipeline tasks based on instructions described in pprXmlFile.

    Args:
        pprXmlFile: A path to PPR file.
        importonly: Whether or not to indicate to stop processing after
            importing data. If True, execution of PPR stops after
            h*_importdata stage. The parameter has no effect if there is no
            h*_importdata stage in PPR.
        breakpoint: A name of command that should be considered as a break point.
        bpaction: An action to be taken at the breakpoint.
            Available actions are,
            'ignore': ignores breakpoint in pprXmlFile.
            'break': stop execution at the breakpoint in pprXmlFile.
            'resume': resume the last context and restart processing after the
                breakpoint in pprXmlFile.
        loglevel: A logging level. Available levels are, 'critical', 'error',
            'warning', 'info', 'debug', 'todo', and 'trace'.
        plotlevel: A plot level. Available levels are, 'all', 'default', and
            'summary'
        interactive: If True, print pipeline log to STDOUT.

    Examples:
       Only import EBs.
       >>> executeppr('PPR_uid___A001_X14c3_X1dd.xml')

       Full execution of PPR.
       >>> executeppr('PPR_uid___A001_X14c3_X1dd.xml', importonly=False)

       Run pipeline tasks up to the 'breakpoint' in PPR and save context.
       >>> executeppr('PPR_uid___A001_X14c3_X1dd.xml', importonly=False, bpaction='break')

       Resume execution from the 'breakpoint' in PPR.
       >>> executeppr('PPR_uid___A001_X14c3_X1dd.xml', importonly=False, bpaction='resume')
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

        # Check for the breakpoint
        bpset = False
        if breakpoint != '':
            for command in commandsList:
                if command[0] == breakpoint:
                    casa_tools.post_to_log("    Found break point: " + breakpoint, echo_to_screen=echo_to_screen)
                    casa_tools.post_to_log("    Break point action is: " + bpaction, echo_to_screen=echo_to_screen)
                    bpset = True
                    break

        # Get the pipeline context
        #     Resumes from the last context. Consider adding name
        if bpset and bpaction == 'resume':
            context = cli.h_resume(filename='last')
            casa_tools.post_to_log("    Resuming from last context", echo_to_screen=echo_to_screen)
        else:
            context = cli.h_init(loglevel=loglevel, plotlevel=plotlevel)
            casa_tools.post_to_log("    Creating new pipeline context", echo_to_screen=echo_to_screen)

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
    ds = dict(structure)
    context.project_structure = project.ProjectStructure(
        ous_entity_id=ds['ous_entity_id'][1],
        ous_part_id=ds['ous_part_id'][1],
        ous_title=ds['ous_title'][1],
        ous_type=ds['ous_type'][1],
        ps_entity_id=ds['ps_entity_id'][1],
        ousstatus_entity_id=ds['ousstatus_entity_id'][1],
        ppr_file=pprXmlFile,
        recipe_name=procedureName)

    # Create performance parameters object
    context.project_performance_parameters = _getPerformanceParameters(intentsDict)

    # Get the session info from the intents dictionary
    if len(intentsDict) > 0:
        sessionsDict = _getSessions(intentsDict)
    else:
        sessionsDict = {}

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

        for key, value in sessionsDict.items():
            if filenamer.sanitize_for_ms(asdm[1]) in value:
                session = key.lower()
                break

        sessions.append(session)
        files.append(os.path.join(rawDir, asdm[1]))
        casa_tools.post_to_log("    Session: " + session + "  ASDM: " + asdm[1], echo_to_screen=echo_to_screen)

    # Paths for all these ASDM should be the same
    #     Add check for this ?

    # Beginning execution
    casa_tools.post_to_log("\nStarting procedure execution ...\n", echo_to_screen=echo_to_screen)
    casa_tools.post_to_log("Procedure name: " + procedureName + "\n", echo_to_screen=echo_to_screen)

    # Names of import tasks that need special treatment:
    import_tasks = ('h_importdata', 'hifa_importdata', 'hifv_importdata',
                    'hsd_importdata', 'hsdn_importdata')
    restore_tasks = ('h_restoredata', 'hifa_restoredata', 'hifv_restoredata',
                     'hsd_restoredata')
    restore_tasks_no_session = ('hsdn_restoredata',)

    # Loop over the commands
    errstr = ''
    foundbp = False
    tracebacks = []
    results = None
    for command in commandsList:

        # Get task name and arguments lists.
        pipeline_task_name = command[0]
        task_args = command[1]
        casa_tools.set_log_origin(fromwhere=pipeline_task_name)

        # Handle break point if one is set
        if bpset:
            # Found the break point
            #    Set the found flag
            #    Ignore it  or
            #    Break the loop or
            #    Resume execution
            if pipeline_task_name == breakpoint:
                foundbp = True
                if bpaction == 'ignore':
                    casa_tools.post_to_log("Ignoring breakpoint " + pipeline_task_name, echo_to_screen=echo_to_screen)
                    continue
                elif bpaction == 'break':
                    casa_tools.post_to_log("Terminating execution at breakpoint " + pipeline_task_name,
                                           echo_to_screen=echo_to_screen)
                    break
                elif bpaction == 'resume':
                    casa_tools.post_to_log("Resuming execution after breakpoint " + pipeline_task_name,
                                           echo_to_screen=echo_to_screen)
                    continue
            # Not the break point so check the resume case
            elif not foundbp and bpaction == 'resume':
                casa_tools.post_to_log("Skipping task " + pipeline_task_name, echo_to_screen=echo_to_screen)
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
            elif pipeline_task_name in restore_tasks_no_session:
                task_args['vis'] = files

            results = pipeline_task(**task_args)
            casa_tools.post_to_log('Results ' + str(results), echo_to_screen=echo_to_screen)

            if importonly and pipeline_task_name in import_tasks:
                casa_tools.post_to_log("Terminating execution after running " + pipeline_task_name,
                                       echo_to_screen=echo_to_screen)
                break

        except Exception:
            # Log message if an exception occurred that was not handled by
            # standardtask template (not turned into failed task result).
            casa_tools.post_to_log("Unhandled error in executeppr while running pipeline task {}"
                                   "".format(pipeline_task_name), echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)

        # Check whether any exceptions were raised either during the task
        # (shown as traceback in task results) or after the task during results
        # acceptance.
        if results:
            tracebacks.extend(utils.get_tracebacks(results))
        if errstr:
            tracebacks.append(errstr)
        # If we have a traceback from an exception, then create local error
        # exit file, and export both error file and weblog to the products
        # directory.
        if len(tracebacks) > 0:
            errorfile = utils.write_errorexit_file(workingDir, 'errorexit', 'txt')
            export_on_exception(context, errorfile)

            # Save the context
            cli.h_save()
            casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
            casa_tools.set_log_origin(fromwhere='')

            previous_tracebacks_as_string = "{}".format("\n".join([tb for tb in tracebacks]))
            raise exceptions.PipelineException(previous_tracebacks_as_string)

    # Save the context
    cli.h_save()
    casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
    casa_tools.set_log_origin(fromwhere='')

    return


def save_existing_context() -> None:
    """Save existing context to disk.

    Save existing global pipeline context to avoid
    being overwritten by newly created one.
    """
    try:
        cli.h_save()
    except Exception:
        # Since h_save raises exception if no context is registered,
        # just continue processing in this case
        pass
    else:
        # Last-saved context in the current working directory should be
        # the one saved by the h_save task above.
        context_files = sorted(
            (f for f in os.listdir() if f.endswith('.context')),
            key=lambda f: os.stat(f).st_mtime
        )
        assert len(context_files) > 0
        context_file = context_files[-1]
        casa_tools.post_to_log(f'Saved existing context {context_file} to disk.')


def export_on_exception(context: 'Context', errorfile: str) -> None:
    # Define path for exporting output products, and ensure path exists.
    products_dir = utils.get_products_dir(context)
    utils.ensure_products_dir_exists(products_dir)

    # Copy error file to products.
    if os.path.exists(errorfile):
        shutil.copyfile(errorfile, os.path.join(products_dir, os.path.basename(errorfile)))

    # Attempt to export weblog.
    try:
         utils.export_weblog_as_tar(context, products_dir, filenamer.PipelineProductNameBuilder)
    except:
        casa_tools.post_to_log("Unable to export weblog to products.")


# Return the intents list, the ASDM list, and the processing commands
# for the first processing request. There should in general be only
# one but the schema permits more. Generalize later if necessary.
def _getFirstRequest(pprXmlFile: str) -> Union[Tuple[list, str, dict, list, list], Tuple[list, list, str, dict, list, str, list]]:
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

    # Get project structure.
    structure = _getProjectStructure(pprObject)

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

    # Get the ASDM list
    relativePath, numAsdms, asdmList = _getAsdmList(pprObject=pprObject, sbsetId=0, numSbSets=numSbSets, requestId=0,
                                                    numRequests=numRequests)
    casa_tools.post_to_log('Relative path: {}'.format(relativePath))
    casa_tools.post_to_log('Number of Asdms: {}'.format(numAsdms))
    casa_tools.post_to_log('ASDM list: {}'.format(asdmList))

    return info, structure, relativePath, intentsDict, asdmList, procedureName, commandsList


# Give the path to the pipeline processing request XML file return the pipeline
# processing request object.
def _getPprObject(pprXmlFile: str) -> XmlObjectifier.XmlObject:
    pprObject = XmlObjectifier.XmlObject(fileName=pprXmlFile)
    return pprObject


# Given the pipeline processing request object print some project summary
# information. Returns a list of tuples to preserve order (key, (prompt, value))
def _getProjectSummary(pprObject: XmlObjectifier.XmlObject) -> list:
    ppr_summary = pprObject.SciPipeRequest.ProjectSummary
    summaryList = []
    summaryList.append(('proposal_code', ('Proposal code: ', ppr_summary.ProposalCode.getValue())))
    summaryList.append(('observatory', ('Observatory: ', ppr_summary.Observatory.getValue())))
    summaryList.append(('telescope', ('Telescope: ', ppr_summary.Telescope.getValue())))
    return summaryList


# Given the pipeline processing request object print some project structure
# information.
def _getProjectStructure(pprObject: XmlObjectifier.XmlObject) -> list:

    # backwards compatibility test
    ppr_project = pprObject.SciPipeRequest.ProjectStructure
    try:
        entityid = ppr_project.OUSStatusRef.getAttribute('entityId')
    except Exception:
        ppr_project = ppr_project.AlmaStructure

    structureList = []
    structureList.append(
        ('ous_entity_type', ('ObsUnitSet Entity Type: ', ppr_project.ObsUnitSetRef.getAttribute('entityTypeName'))))
    structureList.append(
        ('ous_entity_id', ('ObsUnitSet Entity Id: ', ppr_project.ObsUnitSetRef.getAttribute('entityId'))))
    structureList.append(
        ('ous_part_id', ('ObsUnitSet Part Id: ', ppr_project.ObsUnitSetRef.getAttribute('partId'))))
    structureList.append(
        ('ous_title', ('ObsUnitSet Title: ', ppr_project.ObsUnitSetTitle.getValue())))
    structureList.append(
        ('ous_type', ('ObsUnitSet Type: ', ppr_project.ObsUnitSetType.getValue())))
    structureList.append(
        ('ps_entity_type',
         ('ProjectStatus Entity Type: ', ppr_project.ProjectStatusRef.getAttribute('entityTypeName'))))
    structureList.append(
        ('ps_entity_id', ('ProjectStatus Entity Id: ', ppr_project.ProjectStatusRef.getAttribute('entityId'))))
    structureList.append(
        ('ousstatus_entity_type', ('OUSStatus Entity Type: ', ppr_project.OUSStatusRef.getAttribute('entityTypeName'))))
    structureList.append(
        ('ousstatus_entity_id', ('OUSStatus Entity Id: ', ppr_project.OUSStatusRef.getAttribute('entityId'))))

    return structureList


# Given the pipeline processing request object return the number of processing
# requests. This should normally be 1.
def _getNumRequests(pprObject: XmlObjectifier.XmlObject) -> int:

    ppr_prequests = pprObject.SciPipeRequest.ProcessingRequests

    numRequests = 0

    # Try single element / single scheduling block first.
    try:
        relative_path = ppr_prequests.ProcessingRequest.DataSet.SchedBlockSet.SchedBlockIdentifier.RelativePath.getValue()
        numRequests = 1
        return numRequests
    except Exception:
        pass

    # Try single element / multiple scheduling block next.
    try:
        relative_path = ppr_prequests.ProcessingRequest.DataSet.SchedBlockSet.SchedBlockIdentifier[0].RelativePath.getValue()
        numRequests = 1
        return numRequests
    except Exception:
        pass

    # Next try multiple elements  / single scheduling block
    search = 1
    while search:
        try:
            relative_path = ppr_prequests.ProcessingRequest[numRequests].DataSet.SchedBlockSet.SchedBlockIdentifier.RelativePath.getValue()
            numRequests = numRequests + 1
        except Exception:
            search = 0
            if numRequests > 0:
                return numRequests
            else:
                pass

    # Next try multiple elements  / multiple scheduling block
    search = 1
    while search:
        try:
            relative_path = ppr_prequests.ProcessingRequest[numRequests].DataSet.SchedBlockSet.SchedBlockIdentifier[0].RelativePath.getValue()
            numRequests = numRequests + 1
        except Exception:
            search = 0
            if numRequests > 0:
                return numRequests
            else:
                pass

    # Return the number of requests.
    return numRequests


# Given the pipeline processing request object return a list of processing
# intents in the form of a keyword and value dictionary
def _getIntents(pprObject: XmlObjectifier.XmlObject, requestId: int, numRequests: int) -> Tuple[int, dict]:

    if numRequests == 1:
        ppr_intents = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.ProcessingIntents
    else:
        ppr_intents = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest[requestId].ProcessingIntents

    intentsDict = {}
    numIntents = 0
    try:
        intentName = ppr_intents.Intents.Keyword.getValue()
        try:
            intentValue = ppr_intents.Intents.Value.getValue()
        except Exception:
            intentValue = ""
        numIntents = 1
        intentsDict[intentName] = intentValue
    except Exception:
        search = 1
        while search:
            try:
                intentName = ppr_intents.Intents[numIntents].Keyword.getValue()
                try:
                    intentValue = ppr_intents.Intents[numIntents].Value.getValue()
                except Exception:
                    intentValue = ""
                numIntents = numIntents + 1
                intentsDict[intentName] = intentValue
            except Exception:
                search = 0

    return numIntents, intentsDict


def _getPerformanceParameters(intentsDict: dict) -> project.PerformanceParameters:
    performanceParams = project.PerformanceParameters()

    # No performance parameters
    if len(intentsDict) <= 0:
        return performanceParams

    # Set supported attributes
    for key in intentsDict:
        # Parameter not defined in __init__ method
        if not hasattr(performanceParams, key):
            continue
        setattr(performanceParams, key, intentsDict[key])

    return performanceParams


def _getSessions(intentsDict: dict) -> dict:
    sessionsDict = {}

    searching = True
    ptr = 1
    while searching:
        key = 'SESSION_' + str(ptr)
        if key in intentsDict:
            asdmList = intentsDict[key].split(' | ')
            asdmList = [asdm.translate(str.maketrans(':/', '__')) for asdm in asdmList]
            sessionsDict[key] = asdmList
            ptr = ptr + 1
        else:
            searching = False
            break

    return sessionsDict


# Given the pipeline processing request object return a list of processing
# commands where each element in the list is a tuple consisting of the
# processing command name and the parameter set dictionary.
def _getCommands(pprObject: XmlObjectifier.XmlObject, requestId: int, numRequests: int) -> Tuple[str, int, list]:
    if numRequests == 1:
        ppr_cmds = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.ProcessingProcedure
    else:
        ppr_cmds = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest[requestId].ProcessingProcedure

    try:
        procedureName = ppr_cmds.ProcedureTitle.getValue()
    except Exception:
        procedureName = "Undefined"
    commandsList = []
    numCommands = 0

    try:
        cmdName = ppr_cmds.ProcessingCommand.Command.getValue()
        ppr_params = ppr_cmds.ProcessingCommand.ParameterSet
        numParams, paramsDict = _getParameters(ppr_params)
        numCommands = 1
        commandsList.append((cmdName, paramsDict))
    except Exception:
        search = 1
        while search:
            try:
                cmdName = ppr_cmds.ProcessingCommand[numCommands].Command.getValue()
                ppr_params = ppr_cmds.ProcessingCommand[numCommands].ParameterSet
                numParams, paramsDict = _getParameters(ppr_params)
                numCommands = numCommands + 1
                commandsList.append((cmdName, paramsDict))
            except Exception:
                search = 0

    return procedureName, numCommands, commandsList


# Given the pipeline processing request object return the number of scheduling
# block sets.
def _getNumSchedBlockSets(pprObject: XmlObjectifier.XmlObject, requestId: int, numRequests: int) -> int:
    if numRequests == 1:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.DataSet
    else:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest[requestId].DataSet

    try:
        path = ppr_dset.SchedBlockSet.SchedBlockIdentifier.RelativePath.getValue()
        numSchedBlockSets = 1
    except Exception:
        numSchedBlockSets = 0

    return numSchedBlockSets


# Given the pipeline processing request object return a list of ASDMs
# where each element in the list is a tuple consisting of the path
# to the ASDM, the name of the ASDM, and the UID of the ASDM.
def _getAsdmList(pprObject: XmlObjectifier.XmlObject, sbsetId: int, numSbSets: int, requestId: int, numRequests: int) -> Tuple[str, int, list]:
    if numRequests == 1:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest.DataSet
        if numSbSets == 1:
            ppr_dset = ppr_dset.SchedBlockSet.SchedBlockIdentifier
        else:
            ppr_dset = ppr_dset.SchedBlockSet[sbsetId].SchedBlockIdentifier
        relativePath = ppr_dset.RelativePath.getValue()
    else:
        ppr_dset = pprObject.SciPipeRequest.ProcessingRequests.ProcessingRequest[requestId].DataSet
        if numSbSets == 1:
            ppr_dset = ppr_dset.SchedBlockSet.SchedBlockIdentifier
        else:
            ppr_dset = ppr_dset.SchedBlockSet[sbsetId].SchedBlockIdentifier
        relativePath = ppr_dset.RelativePath.getValue

    numAsdms = 0
    asdmList = []

    try:
        asdmName = ppr_dset.AsdmIdentifier.AsdmDiskName.getValue()
        asdmUid = ppr_dset.AsdmIdentifier.AsdmRef.ExecBlockId.getValue()
        asdmList.append((relativePath, asdmName, asdmUid))
        numAsdms = 1
    except Exception:
        search = 1
        while search:
            try:
                asdmName = ppr_dset.AsdmIdentifier[numAsdms].AsdmDiskName.getValue()
                asdmUid = ppr_dset.AsdmIdentifier[numAsdms].AsdmRef.ExecBlockId.getValue()
                numAsdms = numAsdms + 1
                asdmList.append((relativePath, asdmName, asdmUid))
            except Exception:
                search = 0

    return relativePath, numAsdms, asdmList


# Given a parameter set object retrieve the parameter set dictionary for
# each command.
def _getParameters(ppsetObject: XmlObjectifier.XmlObject) -> Tuple[int, dict]:
    numParams = 0
    paramsDict = {}

    try:
        paramName = ppsetObject.Parameter.Keyword.getValue()
        try:
            paramValue = ppsetObject.Parameter.Value.getValue()
        except Exception:
            paramValue = ""
        numParams = 1
        paramsDict[paramName] = paramValue
    except Exception:
        search = 1
        while search:
            try:
                paramName = ppsetObject.Parameter[numParams].Keyword.getValue()
                try:
                    paramValue = ppsetObject.Parameter[numParams].Value.getValue()
                except Exception:
                    paramValue = ""
                numParams = numParams + 1
                paramsDict[paramName] = paramValue
            except Exception:
                search = 0

    return numParams, paramsDict
