"""
The exportdata module provides base classes for preparing data products
on disk for upload to the archive.

To test these classes, register some data with the pipeline using ImportData,
then execute:

import pipeline
vis = [ '<MS name>' ]

# Create a pipeline context and register some data
context = pipeline.Pipeline().context
inputs = pipeline.tasks.ImportData.Inputs(context, vis=vis)
task = pipeline.tasks.ImportData(inputs)
results = task.execute()
results.accept(context)

# Run some other pipeline tasks, e.g flagging, calibration,
# and imaging in a similar manner

# Execute the export data task. The details of
# what gets exported depends on what tasks were run
# previously but may include the following
# TBD
inputs = pipeline.tasks.exportdata.Exportdata.Inputs(context,
      vis, output_dir, sessions, pprfile, products_dir)
task = pipeline.tasks.exportdata.ExportData(inputs)
  results = task.execute()
"""
import collections
import copy
import fnmatch
import io
import os
import shutil
import sys
import tarfile
import uuid

from typing import List, Tuple, Optional, Union

import astropy.io.fits as apfits

from pipeline.infrastructure.launcher import Context
from pipeline.h.tasks.exportdata.aqua import AquaXmlGenerator
from pipeline.h.tasks.exportdata.aqua import export_to_disk as export_aqua_to_disk
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.vdp as vdp
from pipeline import environment
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.infrastructure import utils
from pipeline.infrastructure.filenamer import fitsname, PipelineProductNameBuilder
from pipeline.domain import DataType
from ..common import manifest

# the logger for this module
LOG = infrastructure.get_logger(__name__)

StdFileProducts = collections.namedtuple('StdFileProducts', 'ppr_file weblog_file casa_commands_file casa_pipescript casa_restore_script')


class ExportDataInputs(vdp.StandardInputs):
    """
    ExportDataInputs manages the inputs for the ExportData task.

    .. py:attribute:: context

    the (:class:`~pipeline.infrastructure.launcher.Context`) holding all
    pipeline state

    .. py:attribute:: output_dir

    the directory containing the output of the pipeline

    .. py:attribute:: session

    a string or list of strings containing the sessions(s) associated
    with each vis. Default to a single session containing all vis.
    Vis without a matching session are assigned to the last session
    in the list.

    .. py:attribute:: vis

    a string or list of strings containing the MS name(s) on which to
    operate

    .. py:attribute:: pprfile

    the pipeline processing request.

    .. py:attribute:: calintents

    the list of calintents defining the calibrator source images to be
    saved.  Defaults to all calibrator intents.

    .. py:attribute:: calimages

    the list of calibrator source images to be saved.  Defaults to all
    calibrator images matching calintents. If defined overrides
    calintents and the calibrator images in the context.

    .. py:attribute:: targetimages

    the list of target source images to be saved.  Defaults to all
    target images. If defined overrides the list of target images in
    the context.

    .. py:attribute:: products_dir

    the directory where the data productions will be written
    """

    processing_data_type = [DataType.RAW, DataType.REGCAL_CONTLINE_ALL,
                            DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE,
                            DataType.REGCAL_CONT_SCIENCE, DataType.SELFCAL_CONT_SCIENCE,
                            DataType.REGCAL_LINE_SCIENCE, DataType.SELFCAL_LINE_SCIENCE]

    calimages = vdp.VisDependentProperty(default=[])
    calintents = vdp.VisDependentProperty(default='')
    exportmses = vdp.VisDependentProperty(default=False)
    pprfile = vdp.VisDependentProperty(default='')
    session = vdp.VisDependentProperty(default=[])
    targetimages = vdp.VisDependentProperty(default=[])
    imaging_products_only = vdp.VisDependentProperty(default=False)
    tarms = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def products_dir(self):
        if self.context.products_dir is None:
            return os.path.abspath('./')
        else:
            return self.context.products_dir

    @vdp.VisDependentProperty
    def exportcalprods(self):
        return not (self.imaging_products_only or self.exportmses)

    def __init__(self, context, output_dir=None, session=None, vis=None, exportmses=None, tarms=None,
                 pprfile=None, calintents=None, calimages=None, targetimages=None,
                 products_dir=None, imaging_products_only=None):
        """
        Initialise the Inputs, initialising any property values to those given
        here.

        :param context: the pipeline Context state object
        :type context: :class:`~pipeline.infrastructure.launcher.Context`
        :param output_dir: the working directory for pipeline data
        :type output_dir: string
        :param session: the  sessions for which data are to be exported
        :type session: a string or list of strings
        :param vis: the measurement set(s) for which products are to be exported
        :type vis: a string or list of strings
        :param pprfile: the pipeline processing request
        :type pprfile: a string
        :param calimages: the list of calibrator images to be saved
        :type calimages: a list
        :param targetimages: the list of target images to be saved
        :type targetimages: a list
        :param products_dir: the data products directory for pipeline data
        :type products_dir: string
        """
        super(ExportDataInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.session = session
        self.exportmses = exportmses
        self.tarms = tarms
        self.pprfile = pprfile
        self.calintents = calintents
        self.calimages = calimages
        self.targetimages = targetimages
        self.products_dir = products_dir
        self.imaging_products_only = imaging_products_only


class ExportDataResults(basetask.Results):
    def __init__(self, pprequest='', sessiondict=None, msvisdict=None, calvisdict=None, calimages=None, targetimages=None, weblog='',
                 pipescript='', restorescript='', commandslog='', manifest=''):
        """
        Initialise the results object with the given list of JobRequests.
        """
        super(ExportDataResults, self).__init__()

        if sessiondict is None:
            sessiondict = collections.OrderedDict()
        if msvisdict is None:
            msvisdict = collections.OrderedDict()
        if calvisdict is None:
            calvisdict = collections.OrderedDict()
        if calimages is None:
            calimages = []
        if targetimages is None:
            targetimages = []

        self.pprequest = pprequest
        self.sessiondict = sessiondict
        self.msvisdict = msvisdict
        self.calvisdict = msvisdict
        self.calimages = calimages
        self.targetimages = targetimages
        self.weblog = weblog
        self.pipescript = pipescript
        self.restorescript = restorescript
        self.commandslog = commandslog
        self.manifest = manifest

    def __repr__(self):
        s = 'ExportData results:\n'
        return s


@task_registry.set_equivalent_casa_task('h_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class ExportData(basetask.StandardTaskTemplate):
    """
    ExportData is the base class for exporting data to the products
    subdirectory. It performs the following operations:

    - Saves the pipeline processing request in an XML file
    - Saves the final flags per ASDM in a compressed / tarred CASA flag
      versions file
    - Saves the final calibration apply list per ASDM in a text file
    - Saves the final set of caltables per session in a compressed /
      tarred file containing CASA tables
    - Saves the final web log in a compressed / tarred file
    - Saves the final CASA command log in a text file
    - Saves the final pipeline script in a Python file
    - Saves the final pipeline restore script in a Python file
    - Saves the images in FITS cubes one per target and spectral window
    """

    # link the accompanying inputs to this task
    Inputs = ExportDataInputs

    # Override the default behavior for multi-vis tasks
    is_multi_vis_task = True

    # name builder
    NameBuilder = PipelineProductNameBuilder

    def prepare(self):
        """
        Prepare and execute an export data job appropriate to the
        task inputs.
        """
        # Create a local alias for inputs, so we're not saying
        # 'self.inputs' everywhere
        inputs = self.inputs

        # Create products directory if necessary.
        utils.ensure_products_dir_exists(inputs.products_dir)

        # Initialize the standard OUS status ID string.
        oussid = inputs.context.get_oussid()

        # Define the results object
        result = ExportDataResults()

        # Make the standard vislist and the sessions lists.
        #    These lists are constructed for the calibration mses only no matter the value of
        #    inputs.imaging_products_only
        session_list, session_names, session_vislists, vislist = self._make_lists(inputs.context, inputs.session,
                                                                                  inputs.vis, imaging_only_mses=False)

        # Export the standard per OUS file products
        #    The pipeline processing request
        #    A compressed tarfile of the weblog
        #    The pipeline processing script
        #    The pipeline restore script (if exportporting calibartion products)
        #    The CASA commands log
        recipe_name = self.get_recipename(inputs.context)
        if not recipe_name:
            prefix = oussid
        else:
            prefix = oussid + '.' + recipe_name
        stdfproducts = self._do_standard_ous_products(
            inputs.context, inputs.exportcalprods, prefix, inputs.pprfile, session_list, vislist, inputs.output_dir,
            inputs.products_dir)
        if stdfproducts.ppr_file:
            result.pprequest = os.path.basename(stdfproducts.ppr_file)
        result.weblog = os.path.basename(stdfproducts.weblog_file)
        result.pipescript = os.path.basename(stdfproducts.casa_pipescript)
        if not inputs.exportcalprods:
            result.restorescript = 'Undefined'
        else:
            result.restorescript = os.path.basename(stdfproducts.casa_restore_script)
        result.commandslog = os.path.basename(stdfproducts.casa_commands_file)

        # Make the standard ms dictionary and export per ms products
        #    Currently these are compressed tar files of per MS flagging tables and per MS text files of calibration
        #    apply instructions
        msvisdict = collections.OrderedDict()
        calvisdict = collections.OrderedDict()

        _, exportmses_session_names, exportmses_session_vislists, exportmses_vislist = self._make_lists(
            inputs.context, inputs.session, None, imaging_only_mses=None)

        if not inputs.imaging_products_only:
            if inputs.exportmses:
                msvisdict = self._do_ms_products(inputs.context, exportmses_vislist, inputs.products_dir)
            if inputs.exportcalprods:
                calvisdict = self._do_standard_ms_products(inputs.context, vislist, inputs.products_dir)
        result.msvisdict = msvisdict
        result.calvisdict = calvisdict

        # Make the standard sessions dictionary and export per session products
        #    Currently these are compressed tar files of per session calibration tables
        sessiondict = collections.OrderedDict()
        if not inputs.imaging_products_only:
            if inputs.exportcalprods:
                sessiondict = self._do_standard_session_products(inputs.context, oussid, session_names, session_vislists,
                                                                 inputs.products_dir)
            elif inputs.exportmses:
                # still needs sessiondict
                for i in range(len(exportmses_session_names)):
                    sessiondict[exportmses_session_names[i]] = \
                        ([os.path.basename(visfile) for visfile in exportmses_session_vislists[i]], )
        result.sessiondict = sessiondict

        # Export calibrator images to FITS
        calimages_list, calimages_fitslist, calimages_fitskeywords = self._export_images(inputs.context, True, inputs.calintents,
                                                                                         inputs.calimages, inputs.products_dir, oussid)
        result.calimages=(calimages_list, calimages_fitslist)

        # Export science target images to FITS
        targetimages_list, targetimages_fitslist, targetimages_fitskeywords = self._export_images(inputs.context, False, 'TARGET',
                                                                                                  inputs.targetimages, inputs.products_dir, oussid)
        result.targetimages=(targetimages_list, targetimages_fitslist)

        # Export the pipeline manifest file
        #
        pipemanifest = self._make_pipe_manifest(inputs.context, oussid, stdfproducts, sessiondict, msvisdict,
                                                inputs.exportmses, calvisdict, inputs.exportcalprods,
                                                [os.path.basename(image) for image in calimages_fitslist], calimages_fitskeywords,
                                                [os.path.basename(image) for image in targetimages_fitslist], targetimages_fitskeywords)
        casa_pipe_manifest = self._export_pipe_manifest(prefix, 'pipeline_manifest.xml', inputs.products_dir,
                                                        pipemanifest)
        result.manifest = os.path.basename(casa_pipe_manifest)

        # Return the results object, which will be used for the weblog
        return result

    def analyse(self, results):
        """
        Analyse the results of the export data operation.

        This method does not perform any analysis, so the results object is
        returned exactly as-is, with no data massaging or results items
        added.

        :rtype: :class:~`ExportDataResults`
        """
        return results

    def get_recipename(self, context):
        """
        Get the recipe name
        """

        # Get the parent ous ousstatus name. This is the sanitized ous
        # status uid
        ps = context.project_structure
        if ps is None or ps.recipe_name == 'Undefined':
            recipe_name = ''
        else:
            recipe_name = ps.recipe_name

        return recipe_name

    def _has_imaging_data(self, context, vis):
        """Check if the given vis contains any imaging data."""
        imaging_datatypes = [DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONT_SCIENCE,
                             DataType.REGCAL_CONT_SCIENCE, DataType.SELFCAL_LINE_SCIENCE, DataType.REGCAL_LINE_SCIENCE]
        ms_object = context.observing_run.get_ms(name=vis)
        return any(ms_object.get_data_column(datatype) for datatype in imaging_datatypes)

    def _make_lists(
        self,
        context: Context,
        session: List[str],
        vis: Optional[Union[List[str], str]] = None,
        imaging_only_mses: Optional[bool] = False,
    ) -> Tuple[List[str], List[str], List[List[str]], List[str]]:
        """Create the vis and sessions lists.

        This method processes input parameters to generate lists of sessions, session names, 
        visibility files, and measurement sets based on the provided context and flags.

        Args:
            context (Context): The Pipeline context object.
            session (List[str]): Session names
            vis (Optional[Union[List[str], str]]): A single visibility file name, a list of such names, 
                or None. If None, the method uses all measurement sets registered in the context.
            imaging_only_mses (Optional[bool] = False): A flag to determine how to filter measurement 
                sets based on imaging data:
                True: Includes only measurement sets with imaging data.
                False: Includes only those without imaging data.
                None: Disables filtering, including all measurement sets.

        Returns:
            session_list (List[str]): A list of session identifiers.
            session_names (List[str]): A list of names corresponding to each session in session_list.
            session_vislists (List[List[str]]): For each session, a list of visibility files associated
                with that session.
            vislist (List[str]): The final filtered list of visibility files.
        """
        # process the 'vis' parameter and ensure vislist is a list
        vislist = vis
        if vislist is None:
            vislist = [ms.name for ms in context.observing_run.measurement_sets]
        if isinstance(vislist, str):
            vislist = [vislist]

        # filter based on imaging_only_mses condition
        if isinstance(imaging_only_mses, bool):
            if imaging_only_mses:
                vislist = [vis for vis in vislist if self._has_imaging_data(context, vis)]
            else:
                vislist = [vis for vis in vislist if not self._has_imaging_data(context, vis)]

        # Get the session list and the visibility files associated with each session.
        session_list, session_names, session_vislists = self._get_sessions(context, session, vislist)

        return session_list, session_names, session_vislists, vislist

    def _do_standard_ous_products(self, context, exportcalprods, oussid, pprfile, session_list, vislist, output_dir,
                                  products_dir):
        """
        Generate the per ous standard products
        """

        # Locate and copy the pipeline processing request.
        #     There should normally be at most one pipeline processing request.
        #     In interactive mode there is no PPR.
        ppr_files = self._export_pprfile(context, output_dir, products_dir, oussid, pprfile)
        if ppr_files:
            ppr_file = os.path.basename(ppr_files[0])
        else:
            ppr_file = None

        # Export a tar file of the web log
        weblog_file = self._export_weblog(context, products_dir)

        # Export the processing log independently of the web log
        casa_commands_file = self._export_casa_commands_log(context, context.logs['casa_commands'], products_dir,
                                                            oussid)

        # Export the processing script independently of the web log
        casa_pipescript = self._export_casa_script(context, context.logs['pipeline_script'], products_dir, oussid)

        # Export the restore script independently of the web log
        if not exportcalprods:
            casa_restore_script = 'Undefined'
        else:
            casa_restore_script = self._export_casa_restore_script(context, context.logs['pipeline_restore_script'],
                                                                   products_dir, oussid, vislist, session_list)

        return StdFileProducts(ppr_file, weblog_file, casa_commands_file, casa_pipescript, casa_restore_script)

    def _do_ms_products(self, context, vislist, products_dir):
        """
        Tar up the final calibrated mses and put them in the products
        directory.
        Used for reprocessing applications
        """

        # Loop over the measurements sets in the working directory and tar
        # them up.
        mslist = []
        for visfile in vislist:
            ms_file = self._export_final_ms( context, visfile, products_dir)
            mslist.append(ms_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are the ms files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                 os.path.basename(mslist[i])

        return visdict

    def _do_standard_ms_products(self, context, vislist, products_dir):
        """
        Generate the per ms standard products
        """

        # Loop over the measurements sets in the working directory and
        # save the final flags using the flag manager.
        flag_version_name = 'Pipeline_Final'
        for visfile in vislist:
            self._save_final_flagversion(visfile, flag_version_name)

        # Copy the final flag versions to the data products directory
        # and tar them up.
        flag_version_list = []
        for visfile in vislist:
            flag_version_file = self._export_final_flagversion(context, visfile, flag_version_name, products_dir)
            flag_version_list.append(flag_version_file)

        # Loop over the measurements sets in the working directory, and
        # create the calibration apply file(s) in the products directory.
        apply_file_list = []
        for visfile in vislist:
            apply_file =  self._export_final_applylist(context, \
                visfile, products_dir)
            apply_file_list.append(apply_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are a tuple containing the flags and applycal files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                (os.path.basename(flag_version_list[i]), \
                 os.path.basename(apply_file_list[i]))

        return visdict

    def _do_standard_session_products(self, context, oussid, session_names, session_vislists, products_dir,
                                      imaging=False):
        """
        Generate the per ms standard products
        """

        # Export tar files of the calibration tables one per session
        caltable_file_list = []
        for i in range(len(session_names)):
            caltable_file = self._export_final_calfiles(context, oussid,
                session_names[i], session_vislists[i], products_dir, imaging=imaging)
            caltable_file_list.append(caltable_file)

        # Create the ordered session dictionary
        #    The keys are the session names
        #    The values are a tuple containing the vislist and the caltables
        sessiondict = collections.OrderedDict()
        for i in range(len(session_names)):
            sessiondict[session_names[i]] = \
               ([os.path.basename(visfile) for visfile in session_vislists[i]], \
                 os.path.basename(caltable_file_list[i]))

        return sessiondict

    def _do_if_auxiliary_products(self, oussid, output_dir, products_dir, vislist, imaging_products_only, pipeline_stats_file=None):
        """
        Generate the auxiliary products
        """
        if imaging_products_only:
            contfile_name = 'cont.dat'
            fluxfile_name = 'Undefined'
            antposfile_name = 'Undefined'
        else:
            fluxfile_name = 'flux.csv'
            antposfile_name = 'antennapos.csv'
            contfile_name = 'cont.dat'
        empty = True

        # Get the flux, antenna position, and continuum subtraction
        # files and test to see if at least one of them exists
        flux_file = os.path.join(output_dir, fluxfile_name)
        antpos_file = os.path.join(output_dir, antposfile_name)
        cont_file = os.path.join(output_dir, contfile_name)
        if os.path.exists(flux_file) or os.path.exists(antpos_file) or os.path.exists(cont_file):
            empty = False

        # Export the general and target source template flagging files
        #    The general template flagging files are not required for the restore but are
        #    informative to the user.
        #    Whether or not the target template files should be exported to the archive depends
        #    on the final place of the target flagging step in the work flow and
        #    how flags will or will not be stored back into the ASDM.

        targetflags_filelist = []
        if self.inputs.imaging_products_only:
            flags_file_list = utils.glob_ordered('*.flagtargetstemplate.txt')
        elif not vislist:
            flags_file_list = utils.glob_ordered('*.flagtemplate.txt')
            flags_file_list.extend(utils.glob_ordered('*.flagtsystemplate.txt'))
        else:
            flags_file_list = utils.glob_ordered('*.flag*template.txt')
        for file_name in flags_file_list:
            flags_file = os.path.join(output_dir, file_name)
            if os.path.exists(flags_file):
                empty = False
                targetflags_filelist.append(flags_file)
            else:
                targetflags_filelist.append('Undefined')

        # PIPE-1834: look for timetracker json report files
        timetracker_file_list = utils.glob_ordered('*.timetracker.json')
        if timetracker_file_list:
            empty = False

        # PIPE-1802: look for the selfcal/restore resources
        selfcal_resources_list = []
        if hasattr(self.inputs.context, 'selfcal_resources') and isinstance(self.inputs.context.selfcal_resources, list):
            selfcal_resources_list = self.inputs.context.selfcal_resources
        if selfcal_resources_list:
            empty = False

        # PIPE-2094: check for the pipeline stats file
        if pipeline_stats_file and os.path.exists(pipeline_stats_file):
            empty = False

        if empty:
            return None

        # Define the name of the output tarfile
        tarfilename = f'{oussid}.auxproducts.tgz'
        LOG.info('Saving auxiliary data products in %s', tarfilename)

        # Open tarfile
        with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:

            # Save flux file
            if os.path.exists(flux_file):
                tar.add(flux_file, arcname=os.path.basename(flux_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(flux_file), tarfilename)
            else:
                LOG.info('Auxiliary data product flux.csv does not exist')

            # Save antenna positions file
            if os.path.exists(antpos_file):
                tar.add(antpos_file, arcname=os.path.basename(antpos_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(antpos_file), tarfilename)
            else:
                LOG.info('Auxiliary data product antennapos.csv does not exist')

            # Save continuum regions file
            if os.path.exists(cont_file):
                tar.add(cont_file, arcname=os.path.basename(cont_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(cont_file), tarfilename)
            else:
                LOG.info('Auxiliary data product cont.dat does not exist')

            # Save target flag files
            for flags_file in targetflags_filelist:
                if os.path.exists(flags_file):
                    tar.add(flags_file, arcname=os.path.basename(flags_file))
                    LOG.info('Saving auxiliary data product %s in %s', os.path.basename(flags_file), tarfilename)
                else:
                    LOG.info('Auxiliary data product flagging target templates file does not exist')

            # PIPE-1834: Save timetracker json report files
            for timetracker_file in timetracker_file_list:
                if os.path.exists(timetracker_file):
                    tar.add(timetracker_file, arcname=os.path.basename(timetracker_file))
                    LOG.info('Saving auxiliary data product %s in %s', os.path.basename(timetracker_file), tarfilename)
                else:
                    LOG.info('Auxiliary data product timetracker json report file does not exist')

            # PIPE-1802: Save selfcal restore resources
            for selfcal_resource in selfcal_resources_list:
                if os.path.exists(selfcal_resource):
                    tar.add(selfcal_resource, arcname=selfcal_resource)
                    LOG.info('Saving auxiliary data product %s in %s', selfcal_resource, tarfilename)

            # PIPE-2094: Save pipeline statistics file
            if pipeline_stats_file and os.path.exists(pipeline_stats_file):
                tar.add(pipeline_stats_file, arcname=pipeline_stats_file)
                LOG.info('Saving pipeline statistics file %s in %s', pipeline_stats_file, tarfilename)
            else:
                LOG.info("Pipeline statistics file does not exist.")
            tar.close()

        return tarfilename

    def _make_pipe_manifest(self, context, oussid, stdfproducts, sessiondict, msvisdict, exportmses, calvisdict,
                            exportcalprods, calimages, calimages_fitskeywords, targetimages, targetimages_fitskeywords):
        """
        Generate the manifest file
        """

        # Separate the calibrator images into per ous and per ms images
        # based on the image values of prefix.
        per_ous_calimages = []
        per_ous_calimages_keywords = []
        per_ms_calimages = []
        per_ms_calimages_keywords = []
        for i, image in enumerate(calimages):
            if (image.startswith(oussid) or
                any(image.startswith(session_name) for session_name in sessiondict) or
                image.startswith('oussid') or
                image.startswith('unknown') or
                image.startswith('session')):
                per_ous_calimages.append(image)
                per_ous_calimages_keywords.append(calimages_fitskeywords[i])
            else:
                per_ms_calimages.append(image)
                per_ms_calimages_keywords.append(calimages_fitskeywords[i])

        # Initialize the manifest document and the top level ous status.
        pipemanifest = self._init_pipemanifest(oussid)
        ouss = pipemanifest.set_ous(oussid)
        pipemanifest.add_casa_version(ouss, environment.casa_version_string)
        pipemanifest.add_pipeline_version(ouss, environment.pipeline_revision)
        pipemanifest.add_procedure_name(ouss, context.project_structure.recipe_name)
        pipemanifest.add_environment_info(ouss)

        if stdfproducts.ppr_file:
            pipemanifest.add_pprfile(ouss, os.path.basename(stdfproducts.ppr_file), oussid, level='member', package=context.project_structure.recipe_name)

        # Add the flagging and calibration products
        for session_name in sessiondict:
            session = pipemanifest.set_session(ouss, session_name)
            if exportcalprods:
                pipemanifest.add_caltables(session, sessiondict[session_name][1], session_name, level='member', package=context.project_structure.recipe_name)
            for vis_name in sessiondict[session_name][0]:
                immatchlist = [imname for imname in per_ms_calimages if imname.startswith(vis_name)]
                (ms_file, flags_file, calapply_file) = (None, None, None)
                if exportmses:
                    ms_file = msvisdict[vis_name]
                if exportcalprods:
                    (flags_file, calapply_file) = calvisdict[vis_name]
                pipemanifest.add_asdm_imlist(session, vis_name, ms_file, flags_file, calapply_file, immatchlist,
                                             'calibrator')

        # Add a tar file of the web log
        pipemanifest.add_weblog(ouss, os.path.basename(stdfproducts.weblog_file), oussid, level='member', package=context.project_structure.recipe_name)

        # Add the processing log independently of the web log
        pipemanifest.add_casa_cmdlog(ouss, os.path.basename(stdfproducts.casa_commands_file), oussid, level='member', package=context.project_structure.recipe_name)

        # Add the processing script independently of the web log
        pipemanifest.add_pipescript(ouss, os.path.basename(stdfproducts.casa_pipescript), oussid, level='member', package=context.project_structure.recipe_name)

        # Add the restore script independently of the web log
        if stdfproducts.casa_restore_script != 'Undefined':
            pipemanifest.add_restorescript(ouss, os.path.basename(stdfproducts.casa_restore_script), oussid, level='member', package=context.project_structure.recipe_name)

        # Add the calibrator images
        pipemanifest.add_images(ouss, per_ous_calimages, 'calibrator', per_ous_calimages_keywords)
        pipemanifest.add_images(ouss, per_ms_calimages, 'calibrator', per_ms_calimages_keywords)

        # Add the target images
        pipemanifest.add_images(ouss, targetimages, 'target', targetimages_fitskeywords)

        return pipemanifest

    def _init_pipemanifest(self, oussid):
        """
        Initialize the pipeline manifest
        """
        return manifest.PipelineManifest(oussid)

    def _export_pprfile(self, context, output_dir, products_dir, oussid, pprfile):
        # Prepare the search template for the pipeline processing request file.
        #    Was a template in the past
        #    Forced to one file now but keep the template structure for the moment
        if pprfile == '':
            ps = context.project_structure
            if ps is None or ps.ppr_file == '':
                pprtemplate = None
            else:
                pprtemplate = os.path.basename(ps.ppr_file)
        else:
            pprtemplate = os.path.basename(pprfile)

        # Locate the pipeline processing request(s) and  generate a list
        # to be copied to the data products directory. Normally there
        # should be only one match but if there are more copy them all.
        pprmatches = []
        if pprtemplate is not None:
            for file in os.listdir(os.path.abspath(output_dir)): # the file list will be names without path
                if fnmatch.fnmatch(file, pprtemplate):
                    LOG.debug('Located pipeline processing request %s', file)
                    pprmatches.append(os.path.join(output_dir, file))

        # Copy the pipeline processing request files.
        pprmatchesout = []
        for file in pprmatches:
            if oussid:
                outfile = os.path.join(products_dir, oussid + '.pprequest.xml')
            else:
                outfile = file
            pprmatchesout.append(outfile)
            LOG.info('Copying pipeline processing file %s to %s', os.path.basename(file), os.path.basename(outfile))
            shutil.copy(file, outfile)

        return pprmatchesout

    def _export_final_ms(self, context: object, vis: str, products_dir: str) -> Optional[str]:
        """Export a CASA measurement set (MS) to the products directory.

        If `self.inputs.tarms` is True, the MS will be saved as a compressed tarball (`.tgz`) in the products directory. 
        Otherwise, the MS will be copied directly to the products directory as a directory structure.

        Args:
            context (object): The current pipeline context object (not used directly in this method).
            vis (str): The path to the measurement set to be exported.
            products_dir (str): The directory where the final MS will be stored.

        Returns:
            Optional[str]: The name of the exported file (compressed tarball or directory), or `None` if an error occurs.
        """
        # Define the name of the output tarfile or directory
        visname = os.path.basename(vis)

        if self.inputs.tarms:
            # Export as tarball
            tarfilename = visname + '.tgz'
            LOG.info('Storing final MS %s in %s', visname, tarfilename)

            # Create the tar file
            tar_path = os.path.join(products_dir, tarfilename)
            try:
                with tarfile.open(tar_path, "w:gz") as tar:
                    tar.add(visname)
                return tarfilename
            except Exception as e:
                LOG.error('Failed to create tarball %s: %s', tar_path, str(e))
                return None
        else:
            # Copy MS directory directly
            target_path = os.path.join(products_dir, visname)
            LOG.info('Copying final MS %s to %s', visname, target_path)
            try:
                shutil.copytree(visname, target_path)
                return visname
            except Exception as e:
                LOG.error('Failed to copy MS %s: %s', target_path, str(e))
                return None        

    def _save_final_flagversion(self, vis, flag_version_name):
        """
        Save the final flags to a final flag version.
        """

        LOG.info('Saving final flags for %s in flag version %s', os.path.basename(vis), flag_version_name)
        task = casa_tasks.flagmanager(vis=vis, mode='save', versionname=flag_version_name, comment="Final pipeline flags")
        self._executor.execute(task)

    def _export_final_flagversion(self, context, vis, flag_version_name, products_dir):
        """
        Save the final flags version to a compressed tarfile in products.
        """
        # Define the name of the output tarfile
        visname = os.path.basename(vis)
        tarfilename = visname + '.flagversions.tgz'
        LOG.info('Storing final flags for %s in %s', visname, tarfilename)

        # Define the versions list file to be saved
        flag_version_list = os.path.join(visname + '.flagversions', 'FLAG_VERSION_LIST')
        tar_info = tarfile.TarInfo(flag_version_list)
        LOG.info('Saving flag version list')

        # retrieve all flagversions saved
        task = casa_tasks.flagmanager(vis=visname, mode='list')
        flag_dict = self._executor.execute(task)
        # remove MS key entry if it exists; MS key does not conform with other entries
        # more information about flagmanager return dictionary here:
        # https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.flagging.flagmanager.html#mode
        flag_dict.pop('MS', None)
        flag_dict = {x['name']:x['comment'] for x in flag_dict.values()}

        # Define the versions list file to be saved
        # Define the directory to be saved, and where to store in tar archive.
        if not isinstance(flag_version_name, list):
            flag_version_name = [flag_version_name]
        # PIPE-933: Add flag version 'after_deterministic_flagging' to tarfile
        if "after_deterministic_flagging" in flag_dict and "after_deterministic_flagging" not in flag_version_name:
            flag_version_name.append("after_deterministic_flagging")

        # Create the tar file and populate it
        tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")

        line = ""
        for f in flag_version_name:
            LOG.info('Saving flag version %s', f)
            flagsname = os.path.join(vis + '.flagversions', 'flags.' + f)
            flagsarcname = os.path.join(visname + '.flagversions', 'flags.' + f)
            line += "{} : {}\n".format(f, flag_dict[f])
            tar.add(flagsname, arcname=flagsarcname)
        
        line = line.encode(sys.stdout.encoding)
        tar_info.size = len(line)
        tar.addfile(tar_info, io.BytesIO(line))
        tar.close()

        return tarfilename

    def _export_final_applylist(self, context, vis, products_dir, imaging=False):
        """
        Save the final calibration list to a file. For now this is
        a text file. Eventually it will be the CASA callibrary file.
        """
        applyfile_name = self.NameBuilder.calapply_list(vis=vis, aux_product=imaging)
        LOG.info('Storing calibration apply list for %s in  %s', os.path.basename(vis), applyfile_name)

        try:
            calto = callibrary.CalTo(vis=vis)
            applied_calstate = context.callibrary.applied.trimmed(context, calto)

            # Log the list in human readable form. Better way to do this ?
            nitems = 0
            for calto, calfrom in applied_calstate.merged().items():
                LOG.info('Apply to:  Field: %s  Spw: %s  Antenna: %s',
                         calto.field, calto.spw, calto.antenna)
                nitems = nitems + 1
                for item in calfrom:
                    LOG.info('    Gaintable: %s  Caltype: %s  Gainfield: %s  Spwmap: %s  Interp: %s',
                              os.path.basename(item.gaintable),
                              item.caltype,
                              item.gainfield,
                              item.spwmap,
                              item.interp)

            # Open the file
            if nitems > 0:
                with open(os.path.join(products_dir, applyfile_name), "w") as applyfile:
                    applyfile.write('# Apply file for %s\n' % (os.path.basename(vis)))
                    applyfile.write(applied_calstate.as_applycal())
            else:
                applyfile_name = 'Undefined'
                LOG.info('No calibrations for MS %s', os.path.basename(vis))
        except:
            applyfile_name = 'Undefined'
            LOG.info('No calibrations for MS %s', os.path.basename(vis))

        return applyfile_name

    def _get_sessions(self, context, sessions, vis):
        """
        Return a list of sessions where each element of the list contains
        the vis files associated with that session. In future this routine
        will be driven by the context but for now use the user defined sessions
        """

        # If the input session list is empty put all the visibility files
        # in the same session.
        if len(sessions) == 0:
            wksessions = []
            for visname in vis:
                session = context.observing_run.get_ms(name=visname).session
                wksessions.append(session)
        else:
            wksessions = sessions

        # Determine the number of unique sessions.
        session_seqno = 0; session_dict = {}
        for i in range(len(wksessions)):
            if wksessions[i] not in session_dict:
                session_dict[wksessions[i]] = session_seqno
                session_seqno = session_seqno + 1

        # Initialize the output session names and visibility file lists
        session_names = []
        session_vis_list = []
        for key, value in sorted(session_dict.items(), key=lambda k_v: (k_v[1], k_v[0])):
            session_names.append(key)
            session_vis_list.append([])

        # Assign the visibility files to the correct session
        for j in range(len(vis)):
            # Match the session names if possible
            if j < len(wksessions):
                for i in range(len(session_names)):
                    if wksessions[j] == session_names[i]:
                        session_vis_list[i].append(vis[j])
            # Assign to the last session
            else:
                session_vis_list[len(session_names)-1].append(vis[j])

        # Log the sessions
        for i in range(len(session_vis_list)):
            LOG.info('Visibility list for session %s is %s', session_names[i], session_vis_list[i])

        return wksessions, session_names, session_vis_list

    def _export_final_calfiles(self, context, oussid, session, vislist, products_dir, imaging=False):
        """
        Save the final calibration tables in a tarfile one file
        per session.
        """
        # Define the name of the output tarfile
        tarfilename = self.NameBuilder.caltables(ousstatus_entity_id=oussid,
                                                 session_name=session,
                                                 aux_product=imaging)
        LOG.info('Saving final caltables for %s in %s', session, tarfilename)

        # Create the tar file
        caltables = set()

        for visfile in vislist:
            LOG.info('Collecting final caltables for %s in %s', os.path.basename(visfile), tarfilename)

            # Create the list of applied caltables for that vis
            try:
                calto = callibrary.CalTo(vis=visfile)
                calstate = context.callibrary.applied.trimmed(context, calto)
                caltables.update(calstate.get_caltable())
            except:
                LOG.info('No caltables for MS %s', os.path.basename(visfile))

        if not caltables:
            return 'Undefined'

        with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:
            # Tar the session list.
            for table in caltables:
                tar.add(table, arcname=os.path.basename(table))

        return tarfilename

    def _export_weblog(self, context, products_dir):
        """
        Save the processing web log to a tarfile
        """
        return utils.export_weblog_as_tar(context, products_dir, self.NameBuilder)

    def _export_casa_commands_log(self, context, casalog_name, products_dir, oussid):
        """
        Save the CASA commands file.
        """
        casalog_file = os.path.join(context.report_dir, casalog_name)

        ps = context.project_structure
        out_casalog_file = self.NameBuilder.casa_script(casalog_name,
                                                        project_structure=ps,
                                                        ousstatus_entity_id=oussid,
                                                        output_dir=products_dir)

        LOG.info('Copying casa commands log %s to %s', casalog_file, out_casalog_file)
        shutil.copy(casalog_file, out_casalog_file)

        return os.path.basename(out_casalog_file)

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        """
        Save the CASA restore scropt.
        """
        script_file = os.path.join(context.report_dir, script_name)

        # Get the output file name
        ps = context.project_structure
        out_script_file = self.NameBuilder.casa_script(script_name,
                                                       project_structure=ps,
                                                       ousstatus_entity_id=oussid,
                                                       output_dir=products_dir)

        LOG.info('Creating casa restore script %s', script_file)

        # This is hardcoded.
        tmpvislist = []

        #ALMA default
        ocorr_mode = 'ca'

        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, filext = os.path.splitext(filename)
            tmpvislist.append(filename)
        task_string = "    hif_restoredata(vis=%s, session=%s, ocorr_mode='%s')" % (tmpvislist, session_list, ocorr_mode)

        template = '''h_init()
try:
%s
finally:
    h_save()
''' % task_string

        with open(script_file, 'w') as casa_restore_file:
            casa_restore_file.write(template)

        LOG.info('Copying casa restore script %s to %s', script_file, out_script_file)
        shutil.copy(script_file, out_script_file)

        return os.path.basename(out_script_file)

    def _export_casa_script(self, context, casascript_name, products_dir, oussid):
        """
        Save the CASA script.
        """

        ps = context.project_structure
        casascript_file = os.path.join(context.report_dir, casascript_name)
        out_casascript_file = self.NameBuilder.casa_script(casascript_name,
                                                           project_structure=ps,
                                                           ousstatus_entity_id=oussid,
                                                           output_dir=products_dir)

        LOG.info('Copying casa script file %s to %s', casascript_file, out_casascript_file)
        shutil.copy(casascript_file, out_casascript_file)

        return os.path.basename(out_casascript_file)

    def _export_pipe_manifest(self, oussid, manifest_name, products_dir, pipemanifest):
        """
        Save the manifest file.
        """
        out_manifest_file = self.NameBuilder.manifest(manifest_name,
                                                      ousstatus_entity_id=oussid,
                                                      output_dir=products_dir)
        LOG.info('Creating manifest file %s', out_manifest_file)
        # We can add the manifest element only now after the manifest name is known
        ouss = pipemanifest.get_ous()
        pipemanifest.add_manifest(ouss, os.path.basename(out_manifest_file), ous_name=oussid, level='member', package=self.inputs.context.project_structure.recipe_name)
        pipemanifest.write(out_manifest_file)

        return out_manifest_file

    def _export_images(self, context, calimages, calintents, images, products_dir, ous_name):
        """
        Export the images to FITS files.
        """

        # Create the image list
        images_list = []
        if len(images) == 0:
            # Get the image library
            if calimages:
                LOG.info('Exporting calibrator source images')
                if calintents == '':
                    intents = ['PHASE', 'BANDPASS', 'CHECK', 'AMPLITUDE', 'POLARIZATION']
                else:
                    intents = calintents.split(',')
                cleanlist = context.calimlist.get_imlist()
            else:
                LOG.info('Exporting target source images')
                intents = ['TARGET']
                cleanlist = context.sciimlist.get_imlist()

            for image_number, image in enumerate(cleanlist):
                # We need to store the image
                cleanlist[image_number]['fitsfiles'] = []
                cleanlist[image_number]['auxfitsfiles'] = []
                version = image.get('version', 1)
                # Image name probably includes path
                if image['sourcetype'] in intents:
                    if image['multiterm']:
                        for nt in range(image['multiterm']):
                            imagename = image['imagename'].replace('.image', '.image.tt%d' % (nt))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        if (image['imagename'].find('.pbcor') != -1):
                            imagename = image['imagename'].replace('.image.pbcor', '.alpha')
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                            imagename = '%s.error' % (image['imagename'].replace('.image.pbcor', '.alpha'))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            imagename = image['imagename'].replace('.image', '.alpha')
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                            imagename = '%s.error' % (image['imagename'].replace('.image', '.alpha'))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                    elif (image['imagename'].find('image.sd') != -1): # single dish
                        imagename = image['imagename']
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        imagename = image['imagename'].replace('image.sd', 'image.sd.weight')
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                    else:
                        imagename = image['imagename']
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))

                    # Add PBs for interferometry
                    if (image['imagename'].find('.image') != -1) and (image['imagename'].find('.image.sd') == -1):
                        if (image['imagename'].find('.pbcor') != -1):
                            if (image['multiterm']):
                                imagename = image['imagename'].replace('.image.pbcor', '.pb.tt0')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                            else:
                                imagename = image['imagename'].replace('.image.pbcor', '.pb')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            if (image['multiterm']):
                                imagename = image['imagename'].replace('.image', '.pb.tt0')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                            else:
                                imagename = image['imagename'].replace('.image', '.pb')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))

                    # Add auto-boxing masks for interferometry
                    if (image['imagename'].find('.image') != -1) and (image['imagename'].find('.image.sd') == -1):
                        if (image['imagename'].find('.pbcor') != -1):
                            imagename = image['imagename'].replace('.image.pbcor', '.mask')
                            imagename2 = image['imagename'].replace('.image.pbcor', '.cleanmask')
                            if os.path.exists(imagename) and not os.path.exists(imagename2):
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            imagename = image['imagename'].replace('.image', '.mask')
                            imagename2 = image['imagename'].replace('.image', '.cleanmask')
                            if os.path.exists(imagename) and not os.path.exists(imagename2):
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))

                    # Add POLI/POLA images for polarization calibrators
                    if image['sourcetype'] == 'POLARIZATION':
                        if image['imagename'].find('.pbcor') != -1:
                            for polcal_imtype in ('POLI', 'POLA'):
                                imagename = image['imagename'].replace('.pbcor', '').replace('IQUV', polcal_imtype)
                                if os.path.exists(imagename):
                                    images_list.append((imagename, version))
                                    cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            for polcal_imtype in ('POLI', 'POLA'):
                                imagename = image['imagename'].replace('IQUV', polcal_imtype)
                                if os.path.exists(imagename):
                                    images_list.append((imagename, version))
                                    cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
        else:
            # Assume only the root image name was given.
            cleanlib = imagelibrary.ImageLibrary()
            for image in images:
                if calimages:
                    imageitem = imagelibrary.ImageItem(imagename=image,
                                                       sourcename='UNKNOWN',
                                                       spwlist='UNKNOWN',
                                                       sourcetype='CALIBRATOR')
                else:
                    imageitem = imagelibrary.ImageItem(imagename=image,
                                                       sourcename='UNKNOWN',
                                                       spwlist='UNKNOWN',
                                                       sourcetype='TARGET')
                cleanlib.add_item(imageitem)
                if os.path.basename(image) == '':
                    images_list.append((os.path.join(context.output_dir, image), 1))
                else:
                    images_list.append((image, 1))
            cleanlist = cleanlib.get_imlist()
            # Need to add the FITS names
            for i in range(len(cleanlist)):
                cleanlist[i]['fitsfiles'] = [fitsname(products_dir, images_list[i][0])]
                cleanlist[i]['auxfitsfiles'] = []

        # Convert to FITS.
        fits_list = []
        fits_keywords_list = []
        for image_ver in images_list:
            image, version = image_ver
            fitsfile = fitsname(products_dir, image, version)
            # skip if image doesn't exist
            if not os.path.exists(image):
                LOG.info('Skipping unexisting image %s', os.path.basename(image))
                continue
            LOG.info('Saving final image %s to FITS file %s', os.path.basename(image), os.path.basename(fitsfile))

            # PIPE-325: abbreviate 'spw' and/or 'virtspw' for FITS header when spw string is "too long"
            with casa_tools.ImageReader(image) as img:
                info = img.miscinfo()
                if 'spw' in info:
                    if len(info['spw']) >= 68:
                        spw_sorted = sorted([int(x) for x in info['spw'].split(',')])
                        info['spw'] = '{},...,{}'.format(spw_sorted[0], spw_sorted[-1])
                        img.setmiscinfo(info)
                if 'virtspw' in info:
                    if len(info['virtspw']) >= 68:
                        spw_sorted = sorted([int(x) for x in info['virtspw'].split(',')])
                        info['virtspw'] = '{},...,{}'.format(spw_sorted[0], spw_sorted[-1])
                        img.setmiscinfo(info)

            task = casa_tasks.exportfits(imagename=image, fitsimage=fitsfile, velocity=False, optical=False,
                                         bitpix=-32, minpix=0, maxpix=-1, overwrite=True, dropstokes=False,
                                         stokeslast=True)
            self._executor.execute(task)
            fits_list.append(fitsfile)
            # Fetch header keywords for manifest
            try:
                ff = apfits.open(fitsfile)
                fits_keywords = dict()
                # Loop through FITS keywords.
                for key in ['object', 'obsra', 'obsdec', 'intent', 'specmode',
                            'spw', 'virtspw', 'spwisvrt',
                            'naxis1', 'ctype1', 'cunit1', 'crpix1', 'crval1', 'cdelt1',
                            'naxis2', 'ctype2', 'cunit2', 'crpix2', 'crval2', 'cdelt2',
                            'naxis3', 'ctype3', 'cunit3', 'crpix3', 'crval3', 'cdelt3',
                            'naxis4', 'ctype4', 'cunit4', 'crpix4', 'crval4', 'cdelt4',
                            'bmaj', 'bmin', 'bpa', 'robust', 'weight',
                            'effbw', 'level', 'ctrfrq', 'obspatt', 'arrays', 'modifier', 'session']:
                    fits_keywords[key] = str(ff[0].header.get(key, 'N/A'))

                if 'nspwnam' in ff[0].header:
                    nspwnam = ff[0].header['nspwnam']
                    fits_keywords['nspwnam'] = str(nspwnam)
                    for i in range(1, nspwnam+1):
                        key = 'spwnam{:02d}'.format(i)
                        fits_keywords[key] = str(ff[0].header.get(key, 'N/A'))

                if 'nsessio' in ff[0].header:
                    nsession = ff[0].header['nsessio']
                    session = ''
                    for i in range(1, nsession+1):
                        key = 'sessio{:02d}'.format(i)
                        session += str(ff[0].header.get(key, 'N/A'))
                    fits_keywords['session'] = session

                # Some names and/or values need to be mapped
                fits_keywords['imagemin'] = str(ff[0].header.get('datamin', 'N/A'))
                fits_keywords['imagemax'] = str(ff[0].header.get('datamax', 'N/A'))
                fits_keywords['rms'] = str(ff[0].header.get('datarms', 'N/A'))
                fits_keywords['producttype'] = str(ff[0].header.get('specmode', 'N/A'))
                fits_keywords['pl_datatype'] = str(ff[0].header.get('datatype', 'N/A'))
                fits_keywords['pol'] = str(ff[0].header.get('stokes', 'N/A'))
                imagetype = str(ff[0].header['type'])
                if imagetype == 'flux':
                    fits_keywords['datatype'] = 'pb'
                elif imagetype == 'pbcorimage':
                    fits_keywords['datatype'] = 'pbcor'
                elif imagetype == 'cleanmask':
                    fits_keywords['datatype'] = 'mask'
                elif imagetype == 'singledish':
                    fits_keywords['datatype'] = 'sd'
                else:
                    fits_keywords['datatype'] = 'N/A'

                fits_keywords['format'] = 'fits'
                fits_keywords['ous'] = ous_name

                ff.close()
            except Exception as e:
                LOG.info('Fetching FITS keywords for {} failed: {}'.format(fitsfile, e))
                fits_keywords = dict()
            fits_keywords_list.append(fits_keywords)

        new_cleanlist = copy.deepcopy(cleanlist)

        return new_cleanlist, fits_list, fits_keywords_list

    @staticmethod
    def _add_to_manifest(manifest_file, aux_fproducts, aux_caltablesdict, aux_calapplysdict, aqua_report, ous_name, package="N/A"):

        pipemanifest = manifest.PipelineManifest('')
        pipemanifest.import_xml(manifest_file)
        ouss = pipemanifest.get_ous()

        if aqua_report:
            pipemanifest.add_aqua_report(ouss, os.path.basename(aqua_report), ous_name, level='member', package=package)

        if aux_fproducts:
            # Add auxiliary data products file
            pipemanifest.add_aux_products_file(ouss, os.path.basename(aux_fproducts), ous_name, level='member', package=package)

        # Add the auxiliary caltables
        if aux_caltablesdict:
            for session_name in aux_caltablesdict:
                session = pipemanifest.get_session(ouss, session_name)
                if session is None:
                    session = pipemanifest.set_session(ouss, session_name)
                pipemanifest.add_auxcaltables(session, aux_caltablesdict[session_name][1], session_name)
                for vis_name in aux_caltablesdict[session_name][0]:
                    pipemanifest.add_auxasdm(session, vis_name, aux_calapplysdict[vis_name])

        pipemanifest.write(manifest_file)

    def _export_aqua_report(self, context: Context, oussid: str, products_dir: str, report_generator: AquaXmlGenerator,
                            weblog_filename: str):
        """Save the AQUA report.

        Note the method is mostly a duplicate of the conterpart
             in hifa/tasks/exportdata/almaexportdata

        Args:
            context : pipeline context
            oussid : OUS status ID
            products_dir (str): path of product directory
            report_generator: AQUA XML Generator
            weblog_filename (str): weblog tarball filename

        Returns:
            AQUA report file path
        """

        aqua_file = os.path.join(context.output_dir, context.logs['aqua_report'])

        LOG.info('Generating pipeline AQUA report')
        try:
            report_xml = report_generator.get_report_xml(context)
            export_aqua_to_disk(report_xml, aqua_file)
        except Exception as e:
            LOG.exception('Error generating the pipeline AQUA report', exc_info=e)
            return 'Undefined'

        ps = context.project_structure
        out_aqua_file = self.NameBuilder.aqua_report(context.logs['aqua_report'],
                                                     project_structure=ps,
                                                     ousstatus_entity_id=oussid,
                                                     output_dir=products_dir)

        LOG.info(f'Copying AQUA report {aqua_file} to {out_aqua_file}')
        shutil.copy(aqua_file, out_aqua_file)

        # put aqua report into html directory, so it can be linked to the weblog
        aqua_html_path = os.path.join(context.report_dir, aqua_file)
        LOG.info(f'Copying AQUA report {aqua_file} to {aqua_html_path}')
        shutil.copy(aqua_file, context.report_dir)

        products_weblog_tarball = os.path.join(context.products_dir, weblog_filename)

        if os.path.isfile(products_weblog_tarball) and tarfile.is_tarfile(products_weblog_tarball):

            with tarfile.open(products_weblog_tarball, "r:gz") as tar:

                files_to_keep = []
                aqua_html_path_in_tarball = None
                for member in tar.getmembers():
                    if aqua_file in member.name:
                        aqua_html_path_in_tarball = member
                    else:
                        files_to_keep.append(member)

                # PIPE-1942: we create a temp tarball file under a random UUID name, add/update the latest AUQA report XML
                # and all other existing weblog content into it, and overwrite the old weblog tarball with this new one.
                # We do this because tarfile does not support updating files in place. We also avoid using Python/tempfile
                # because 1) it might create a large temp file in /tmp on the computing node; 2) it will create a file
                # without the group read/write permission.

                # Create a new tarball with the updated aqua file, keeping all others the same
                temp_weblog_tarball = str(uuid.uuid4())
                with tarfile.open(temp_weblog_tarball, "w:gz") as new_tar:

                    LOG.debug(f'Created a temp tarball file: {temp_weblog_tarball}')

                    # Add all the existing files (excluding the aqua report XML) from the old tarball to the new tarball
                    for member in files_to_keep:
                        new_tar.addfile(member, tar.extractfile(member))

                    # If an aqua file was already in the old weblog tarball, then add it under the same relative path/name.
                    # Else, add it into the weblog tarball.
                    if aqua_html_path_in_tarball is not None:
                        LOG.debug(f'Updating {aqua_html_path_in_tarball.name} in contents of {temp_weblog_tarball}')
                        new_tar.add(aqua_file, arcname=aqua_html_path_in_tarball.name)
                    else:
                        LOG.debug(f'Adding {aqua_html_path} to contents of {temp_weblog_tarball}')
                        new_tar.add(f'{aqua_html_path}')

            LOG.info(f'Adding/updating the AQUA report in {products_weblog_tarball}')
            LOG.debug(f'Moving {temp_weblog_tarball} to {products_weblog_tarball}')
            shutil.move(temp_weblog_tarball, products_weblog_tarball)

        return os.path.basename(out_aqua_file)
