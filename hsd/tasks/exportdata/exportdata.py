"""
The exportdata for SD module.

This module provides base classes for preparing data products on disk
for upload to the archive.

To test these classes, register some data with the pipeline using ImportData,
then execute:

import pipeline
# Create a pipeline context and register some data
context = pipeline.Pipeline().context
output_dir = "."
products_dir = "./products"
inputs = pipeline.tasks.singledish.SDExportData.Inputs(context, output_dir,
                                                       products_dir)
task = pipeline.tasks.singledish.SDExportData (inputs)
results = task.execute ()
"""

import collections
import glob
import os
import shutil
import string
import tarfile
from typing import Dict, Generator, List, Optional

import pipeline.h.tasks.exportdata.exportdata as exportdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.utils import absolute_path
import pipeline.infrastructure.project as project
from pipeline.infrastructure import task_registry
from pipeline.hsd.tasks.importdata.importdata import SDImportDataResults
from pipeline.hsd.tasks.restoredata.restoredata import SDRestoreDataResults
from . import almasdaqua

# the logger for this module
LOG = infrastructure.get_logger(__name__)


class SDExportDataInputs(exportdata.ExportDataInputs):
    """Inputs class for SDExportData.

    Inputs class must be separated per task class even if
    it's effectively the same.
    """

    pass


@task_registry.set_equivalent_casa_task('hsd_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class SDExportData(exportdata.ExportData):
    """A class for exporting single dish data to the products subdirectory.

    It performs the following additional operations after ExportData does:

    - Exports auxiliary tar files of the calibration tables one per session
    - Exports auxiliary MS products (calibration apply files) in text files
    - Saves a K2JY reference file and flag files into tarball
    - Saves the AQUA report file
    """

    Inputs = SDExportDataInputs

    def prepare(self) -> exportdata.ExportDataResults:
        """
        Prepare and execute an export data job appropriate to the task inputs.

        This is almost equivalent to ALMAExportData.prepare().

        Returns:
            ExportDataResults object
        """
        results = super(SDExportData, self).prepare()

        oussid = self.inputs.context.get_oussid()

        # Make the imaging list of names of MeasurementSet and the sessions lists.
        _, session_names, session_vislists, vislist = \
            self._make_lists(self.inputs.context, self.inputs.session,
                             self.inputs.vis, imaging_only_mses=None)

        LOG.info('vislist={}'.format(vislist))

        if vislist:
            # Export the auxiliary caltables if any
            #    These are currently the uvcontinuum fit tables.
            auxcaltables = \
                self._do_aux_session_products(self.inputs.context, oussid,
                                              session_names, session_vislists,
                                              self.inputs.products_dir)

            # Export the auxiliary cal apply files if any
            #    These are currently the uvcontinuum fit tables.
            auxcalapplys = \
                self._do_aux_ms_products(self.inputs.context, vislist,
                                         self.inputs.products_dir)
        else:
            auxcaltables = None
            auxcalapplys = None

        # Export the auxiliary file products into a single tar file
        #    These are optional for reprocessing but informative to the user
        #    The calibrator source fluxes file
        #    The antenna positions file
        #    The continuum regions file
        #    The target flagging file
        recipe_name = self.get_recipename(self.inputs.context)
        if not recipe_name:
            prefix = oussid
        else:
            prefix = oussid + '.' + recipe_name
        auxfproducts = \
            self._do_auxiliary_products(self.inputs.context, oussid,
                                        self.inputs.output_dir,
                                        self.inputs.products_dir)

        # Export the AQUA report
        pipe_aqua_reportfile = self._export_aqua_report(context=self.inputs.context,
                                                        oussid=prefix,
                                                        products_dir=self.inputs.products_dir,
                                                        report_generator=almasdaqua.AlmaAquaXmlGenerator(),
                                                        weblog_filename=results.weblog)

        # Update the manifest
        if auxfproducts is not None or pipe_aqua_reportfile is not None:
            manifest_file = os.path.join(self.inputs.context.products_dir,
                                         results.manifest)
            recipe_name = self.inputs.context.project_structure.recipe_name
            self._add_to_manifest(manifest_file, auxfproducts, auxcaltables,
                                  auxcalapplys, pipe_aqua_reportfile, oussid, recipe_name)

        return results

    def _do_aux_session_products(self, context: Context, oussid: str,
                                 session_names: List[str],
                                 session_vislists: List[List[str]],
                                 products_dir: str) -> \
            Dict[str, List[str]]:
        """Export auxiliary calibration tables to products directory and return session dictionary.

        Args:
            context : pipeline context
            oussid : OUS status ID
            session_names : list of session names
            session_vislists : list of MeasurementSet names associated with each session
            products_dir : path of products directory

        Returns:
            ordered dictionary object contains session name(key) and a list of file name of
            MeasurementSet and the name of auxiliary calibration product associated with the
            session (value).
        """
        # Make the standard sessions dictionary and export per session products
        #    Currently these are compressed tar files of per session calibration tables
        # Export tar files of the calibration tables one per session
        LOG.info('_do_aux_session_products')
        caltable_file_list = []
        for i in range(len(session_names)):
            caltable_file = self._export_final_baseline_calfiles(
                context, oussid, session_names[i], session_vislists[i],
                products_dir)
            caltable_file_list.append(caltable_file)

        # Create the ordered session dictionary
        #    The keys are the session names
        #    The values are a tuple containing the vislist and the caltables
        sessiondict = collections.OrderedDict()
        for i in range(len(session_names)):
            sessiondict[session_names[i]] = (
                [os.path.basename(visfile) for visfile
                 in session_vislists[i]], os.path.basename(caltable_file_list[i])
            )

        return sessiondict

    def __get_last_baseline_table(self, vis: str) -> Optional[str]:
        """Sort baseline table names and return the last of them.

        Args:
            vis : MeasurementSet name

        Returns:
            the last baseline table name
        """
        basename = os.path.basename(vis.rstrip('/'))
        bl_tables = glob.glob('{}.*hsd_baseline*.bl.tbl'.format(basename))
        if len(bl_tables) > 0:
            bl_tables.sort()
            name = bl_tables[-1]
            LOG.debug('bl cal table for {} is {}'.format(vis, name))
            return name
        else:
            return None

    def _export_final_baseline_calfiles(self, context: Context, oussid: str,
                                        session: str, vislist: List[str],
                                        products_dir: str) -> str:
        """Save the final baseline tables in a tarfile one file per session.

        This method is an exact copy of same method in superclass
        except for handling baseline caltables.

        Args:
            context : pipeline context
            oussid : OUS status ID
            session : session name
            vislist : list of MeasurementSet names
            products_dir : products directory

        Returns:
            tar file name
        """
        # Save the current working directory and move to the pipeline
        # working directory. This is required for tarfile IO

        # Define the name of the output tarfile
        tarfilename = self.NameBuilder.caltables(ousstatus_entity_id=oussid,
                                                 session_name=session,
                                                 aux_product=True)
        # tarfilename = '{}.{}.auxcaltables.tgz'.format(oussid, session)
        # tarfilename = '{}.{}.caltables.tgz'.format(oussid, session)
        LOG.info('Saving final caltables for %s in %s', session, tarfilename)

        # Create the tar file

#             caltables = set()

        bl_caltables = set()

        for visfile in vislist:
            LOG.info('Collecting final caltables for %s in %s',
                     os.path.basename(visfile), tarfilename)

            # Create the list of baseline caltable for that vis
            name = self.__get_last_baseline_table(visfile)
            if name is not None:
                bl_caltables.add(name)

            with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz')\
                    as tar:
                # Tar the session list.
                # for table in caltables:
                #     tar.add(table, arcname=os.path.basename(table))

                for table in bl_caltables:
                    tar.add(table, arcname=os.path.basename(table))

        return tarfilename

    def _do_aux_ms_products(self, context: Context, vislist: List[str],
                            products_dir: str) -> \
            Dict[str, str]:
        """Export auxiliary MS products.

        This method exports calibration apply files per MeasurementSet which
        store calibration commands to apply baseline tables.

        Args:
            context : pipeline context
            vislist : list of MeasurementSet names
            products_dir : path of products directory

        Returns:
            an ordered dictionary which contains MeasurementSet name (key) and
            calibration apply file name(value)
        """
        # Loop over the MeasurementSets in the working directory, and
        # create the calibration apply file(s) in the products directory.
        apply_file_list = []
        for visfile in vislist:
            apply_file = self._export_final_baseline_applylist(context, visfile,
                                                               products_dir)
            apply_file_list.append(apply_file)

        # Create the ordered MeasurementSet names dictionary
        #    The keys are the base MeasurementSet names
        #    The values are a tuple containing the flags and applycal files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                os.path.basename(apply_file_list[i])

        return visdict

    def _export_final_baseline_applylist(self, context: Context, vis: str,
                                         products_dir: str) -> str:
        """Save commands to apply the final baseline table to a file.

        For now this is a text file. Eventually it will be the CASA callibrary
        file.

        Args:
            context : pipeline context
            vis : MeasurementSet name
            products_dir : path of products directory

        Returns:
            the name of calibration apply file
        """
        applyfile_name = self.NameBuilder.calapply_list(os.path.basename(vis),
                                                        aux_product=True)
        # applyfile_name = os.path.basename(vis) + '.auxcalapply.txt'
        LOG.info('Storing calibration apply list for %s in  %s',
                 os.path.basename(vis), applyfile_name)

        try:
            # Log the list in human readable form. Better way to do this ?
            cmd = string.Template("sdbaseline(infile='${infile}', "
                                  "datacolumn='corrected', spw='${spw}', "
                                  "blmode='apply', bltable='${bltable}', "
                                  "blfunc='poly', outfile='${outfile}', "
                                  "overwrite=True)")

            # Create the list of baseline caltable for that vis
            name = self.__get_last_baseline_table(vis)
            ms = context.observing_run.get_ms(vis)
            science_spws = ms.get_spectral_windows(science_windows_only=True)
            spw = ','.join([str(s.id) for s in science_spws])
            if name is not None:
                applied_calstate = \
                    cmd.safe_substitute(infile=vis, bltable=name, spw=spw,
                                        outfile=vis.rstrip('/') + '_bl')

                # Open the file.
                with open(os.path.join(products_dir, applyfile_name), "w") \
                        as applyfile:
                    applyfile.write('# Apply file for %s\n'
                                    % (os.path.basename(vis)))
                    applyfile.write(applied_calstate)
        except Exception:
            applyfile_name = 'Undefined'
            LOG.info('No calibrations for MS %s' % os.path.basename(vis))

        return applyfile_name

    def _detect_jyperk(self, context: Context) -> str:
        """Detect K2Jy file and return it.

        Args:
            context : pipeline context

        Raises:
            RuntimeError: raise if multiple K2Jy files are detected

        Returns:
            path of K2Jy file
        """
        reffile_list = set(self.__get_reffile(context.results))

        if len(reffile_list) == 0:
            # if no reffile is found, return None
            LOG.debug('No K2Jy factor file found.')
            return None
        if len(reffile_list) > 1:
            raise RuntimeError("K2Jy conversion file must be only one. %s found."
                               % (len(reffile_list)))

        jyperk = reffile_list.pop()

        if not os.path.exists(jyperk):
            # if reffile doesn't exist, return None
            LOG.debug('K2Jy file \'%s\' not found' % jyperk)
            return None

        LOG.info('Exporting {0} as a product'.format(jyperk))
        return absolute_path(jyperk)

    @staticmethod
    def __get_reffile(results: Generator[str, None, None]):
        """Find SDK2JyCalResults and yield K2JY reference file.

        Args:
            results : a list of ResultsProxy
                      (contains Results object of every tasks)

        Yields:
            K2JY reference file, default: jyperk.csv
        """
        for proxy in results:
            result = proxy.read()
            if not isinstance(result, basetask.ResultsList):
                result = [result]
            for r in result:
                if str(r).find('SDK2JyCalResults') != -1 and hasattr(r, 'reffile'):
                    reffile = r.reffile
                    if reffile is not None and os.path.exists(reffile):
                        yield reffile

    def _do_auxiliary_products(self, context: Context, oussid: str,
                               output_dir: str, products_dir: str) -> str:
        """Save a K2JY reference file and flag files into tarball.

        Args:
            context : pipeline context
            oussid : OUS Status UID
            output_dir : path of output directory
            products_dir : path of products directory

        Returns:
            tarball file name
        """
        # Track whether any auxiliary products exist to be exported.
        aux_prod_exists = False

        # Get the jyperk file, check whether it exists.
        jyperk = self._detect_jyperk(context)
        if jyperk and os.path.exists(jyperk):
            aux_prod_exists = True

        # Export the general and target source template flagging files
        #    The general template flagging files are not required
        #    for the restore but are informative to the user.
        #    Whether or not the target template files should be exported to
        #    the archive depends on the final place of the target flagging
        #    step in the work flow and how flags will or will not be stored
        #    back into the ASDM.
        flags_file_list = [os.path.join(output_dir, fname)
                           for fname in glob.glob('*.flag*template.txt')]
        if flags_file_list:
            aux_prod_exists = True

        # If no auxiliary product was found, skip creation of tarfile
        # and return early.
        if not aux_prod_exists:
            return None

        # Create the tarfile.
        # Define the name of the output tarfile.
        tarfilename = self.NameBuilder.auxiliary_products(
            'auxproducts.tgz', ousstatus_entity_id=oussid)
        # tarfilename = '{}.auxproducts.tgz'.format(oussid)
        LOG.info('Saving auxiliary data products in %s', tarfilename)

        # Open tarfile.
        with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') \
                as tar:

            # Save flux file.
            if jyperk and os.path.exists(jyperk):
                tar.add(jyperk, arcname=os.path.basename(jyperk))
                LOG.info('Saving auxiliary data product '
                         '{} in {}'.format(os.path.basename(jyperk), tarfilename))
            elif isinstance(jyperk, str):
                LOG.info('Auxiliary data product '
                         '{} does not exist'.format(os.path.basename(jyperk)))

            # Save flag files.
            for flags_file in flags_file_list:
                if os.path.exists(flags_file):
                    tar.add(flags_file, arcname=os.path.basename(flags_file))
                    LOG.info('Saving auxiliary data product '
                             '{} in {}'.format(os.path.basename(flags_file), tarfilename))
                else:
                    LOG.info('Auxiliary data product '
                             '{} does not exist'.format(os.path.basename(flags_file)))

            tar.close()

        return tarfilename

    def _export_casa_restore_script(self, context: Context, script_name: str,
                                    products_dir: str, oussid: str,
                                    vislist: List[str],
                                    session_list: List[str]) -> str:
        """Save the CASA restore script.

        Args:
            context : pipeline context
            script_name : name of the restore script
            products_dir : name of the product directory
            oussid : OUS Status ID
            vislist : list of MeasurementSet names
            session_list : list of session

        Returns:
            path of output CASA script file
        """
        tmpvislist = []
        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, filext = os.path.splitext(filename)
            tmpvislist.append(filename)
        restore_task_name = 'hsd_restoredata'
        hm_rasterscan = self._get_hm_rasterscan_value(context)
        args = collections.OrderedDict(vis=tmpvislist, session=session_list,
                                       ocorr_mode='ao', hm_rasterscan=hm_rasterscan)
        return self._export_casa_restore_script_template(context, script_name,
                                                         products_dir, oussid,
                                                         restore_task_name,
                                                         args)

    def _get_hm_rasterscan_value(self, context: Context) -> str:
        """Retrieve hm_rasterscan value from Results object.

        This method checks if either SDImportDataResults or
        SDRestoreDataResults object is registered to Pipeline context.
        If exists, hm_rasterscan value is retrieved from the results
        object. If no SDImportDataResults nor SDRestoreDataResults
        object is registered, 'time' will be returned.

        Args:
            context: Pipeline context

        Returns:
            The hm_rasterscan value.
        """
        results_filter = filter(
            lambda x: isinstance(x, basetask.ResultsList) and isinstance(x[0], (SDImportDataResults, SDRestoreDataResults)),
            map(lambda x: x.read(), context.results)
        )
        importdata_results = next(results_filter, None)
        if importdata_results:
            hm_rasterscan = importdata_results.inputs.get('hm_rasterscan', 'time')
        else:
            hm_rasterscan = 'time'

        return hm_rasterscan

    def _export_casa_restore_script_template(self, context: Context,
                                             script_name: str,
                                             products_dir: str,
                                             oussid: str,
                                             restore_task_name: str,
                                             restore_task_args: Dict[str, str])\
            -> str:
        """Generate and export CASA restore script.

        Args:
            context : pipeline context
            script_name : Name of the restore script
            products_dir : Name of the product directory
            oussid : OUS Status ID
            restore_task_name : Name of the restoredata task
            restore_task_args : Set of the parameters for the restoredata task.
                                If an order of the parameter matters, it can be
                                collections.OrderedDict.

        Returns:
            path of output CASA script file
        """
        # Generate the file list

        # Get the output file name
        ps = context.project_structure
        script_file = os.path.join(context.report_dir, script_name)
        out_script_file = self.NameBuilder.casa_script(script_name,
                                                       project_structure=ps,
                                                       ousstatus_entity_id=oussid,
                                                       output_dir=products_dir)
        # if ps is None or ps.ousstatus_entity_id == 'unknown':
        #     script_file = os.path.join(context.report_dir, script_name)
        #     out_script_file = os.path.join(products_dir, script_name)
        # else:
        #     script_file = os.path.join(context.report_dir, script_name)
        #     out_script_file = os.path.join(products_dir, oussid + '.' + script_name)

        LOG.info('Creating casa restore script %s' % script_file)

        # This is hardcoded.
        # tmpvislist = []

        # ALMA TP default
        # ocorr_mode = 'ao'

        # for vis in vislist:
        #    filename = os.path.basename(vis)
        #    if filename.endswith('.ms'):
        #        filename, filext = os.path.splitext(filename)
        #    tmpvislist.append(filename)
        # task_string = "    hsd_restoredata(vis=%s, session=%s, ocorr_mode='%s')" % (tmpvislist, session_list,
        #                                                                             ocorr_mode)
        args_string = ', '.join(['{}={!r}'.format(k, v) for k, v
                                 in restore_task_args.items()])
        task_string = "    {}({})".format(restore_task_name, args_string)

        state_commands = []
        for o in (context.project_summary, context.project_structure,
                  context.project_performance_parameters):
            state_commands += ['context.set_state({!r}, {!r}, {!r})'.format(
                cls, name, value) for cls, name, value in project.get_state(o)]

        template = '''context = h_init()
%s
try:
%s
finally:
    h_save()
''' % ('\n'.join(state_commands), task_string)

        with open(script_file, 'w') as casa_restore_file:
            casa_restore_file.write(template)

        LOG.info('Copying casa restore script '
                 '%s to %s' % (script_file, out_script_file))
        shutil.copy(script_file, out_script_file)

        return os.path.basename(out_script_file)
