import collections
import json
import os
import shutil
import traceback

import pipeline.h.tasks.exportdata.exportdata as exportdata
from pipeline.h.tasks.common import manifest
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure.utils import utils
from pipeline.infrastructure.renderer import stats_extractor
from pipeline.infrastructure import task_registry
from . import almaifaqua

LOG = infrastructure.get_logger(__name__)

AuxFileProducts = collections.namedtuple('AuxFileProducts', 'flux_file antenna_file cont_file flagtargets_list')


class ALMAExportDataInputs(exportdata.ExportDataInputs):

    imaging_products_only = vdp.VisDependentProperty(default=False)

    def __init__(self, context, output_dir=None, session=None, vis=None, exportmses=None, tarms=None,
                 pprfile=None, calintents=None,
                 calimages=None, targetimages=None, products_dir=None, imaging_products_only=None):
        super(ALMAExportDataInputs, self).__init__(context, output_dir=output_dir, session=session, vis=vis,
                                                   exportmses=exportmses, tarms=tarms, pprfile=pprfile, calintents=calintents,
                                                   calimages=calimages, targetimages=targetimages,
                                                   products_dir=products_dir,
                                                   imaging_products_only=imaging_products_only)


@task_registry.set_equivalent_casa_task('hifa_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class ALMAExportData(exportdata.ExportData):

    # link the accompanying inputs to this task
    Inputs = ALMAExportDataInputs

    def prepare(self):

        results = super().prepare()

        oussid = self.inputs.context.get_oussid()

        # Make the imaging vislist and the sessions lists.
        #     Force this regardless of the value of imaging_only_products
        _, session_names, session_vislists, vislist = super()._make_lists(
            self.inputs.context, self.inputs.session, self.inputs.vis, imaging_only_mses=True)

        # Depends on the existence of imaging mses
        if vislist:
            # Export the auxiliary caltables if any
            #    These are currently the uvcontinuum fit tables.
            auxcaltables = self._do_aux_session_products(self.inputs.context, oussid, session_names, session_vislists,
                                                         self.inputs.products_dir)

            # Export the auxiliary cal apply files if any
            #    These are currently the uvcontinuum fit tables.
            auxcalapplys = self._do_aux_ms_products(self.inputs.context, vislist, self.inputs.products_dir)
        else:
            auxcaltables = None
            auxcalapplys = None

        # Create and export the pipeline stats file
        pipeline_stats_file = None
        try:
            pipeline_stats_file = self._export_stats_file(context=self.inputs.context, oussid=oussid)
        except Exception as e:
            LOG.info("Unable to output pipeline statistics file: {}".format(e))
            LOG.debug(traceback.format_exc())
            pass

        # Export the auxiliary file products into a single tar file
        #    These are optional for reprocessing but informative to the user
        #    The calibrator source fluxes file
        #    The antenna positions file
        #    The continuum regions file
        #    The target flagging file
        #    The pipeline statistics file (if it exists)
        recipe_name = self.get_recipename(self.inputs.context)
        if not recipe_name:
            prefix = oussid
        else:
            prefix = oussid + '.' + recipe_name
        auxfproducts = self._do_if_auxiliary_products(prefix, self.inputs.output_dir, self.inputs.products_dir, vislist,
                                                   self.inputs.imaging_products_only, pipeline_stats_file)

        # Export the AQUA report
        pipe_aqua_reportfile = self._export_aqua_report(context=self.inputs.context,
                                                        oussid=prefix,
                                                        products_dir=self.inputs.products_dir,
                                                        report_generator=almaifaqua.AlmaAquaXmlGenerator(),
                                                        weblog_filename=results.weblog)

        # Update the manifest
        if auxfproducts is not None or pipe_aqua_reportfile is not None:
            manifest_file = os.path.join(self.inputs.products_dir, results.manifest)
            recipe_name = self.inputs.context.project_structure.recipe_name
            self._add_to_manifest(manifest_file, auxfproducts, auxcaltables, auxcalapplys, pipe_aqua_reportfile, oussid, recipe_name)

        return results

    def _do_aux_session_products(self, context, oussid, session_names, session_vislists, products_dir):

        # Make the standard sessions dictionary and export per session products
        #    Currently these are compressed tar files of per session calibration tables
        sessiondict = super(ALMAExportData, self)._do_standard_session_products(
            context, oussid, session_names, session_vislists, products_dir, imaging=True)

        return sessiondict

    def _do_aux_ms_products(self, context, vislist, products_dir):

        # Loop over the measurements sets in the working directory, and
        # create the calibration apply file(s) in the products directory.
        apply_file_list = []
        for visfile in vislist:
            apply_file = super(ALMAExportData, self)._export_final_applylist(
                context, visfile, products_dir, imaging=True)
            apply_file_list.append(apply_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are a tuple containing the flags and applycal files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                os.path.basename(apply_file_list[i])

        return visdict

    def _export_stats_file(self, context, oussid='') -> str:
        """Generate and output the stats file.

        Args: 
          context: the pipieline context
          oussid: the ous id

        Returns:
          The filename of the outputfile.
        """
        statsfile_name = "pipeline_stats_{}.json".format(oussid)
        stats_file = os.path.join(context.output_dir, statsfile_name)
        LOG.info('Generating pipeline statistics file')

        stats_dict = stats_extractor.generate_stats(context)

        # Write the stats file to disk
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_dict, f, ensure_ascii=False, indent=4, sort_keys=True)

        return stats_file

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        """
        Save the CASA restore scropt.
        """
        # Generate the file list

        # Get the output file name
        ps = context.project_structure
        script_file = os.path.join(context.report_dir, script_name)
        out_script_file = self.NameBuilder.casa_script(script_name,
                                                       project_structure=ps,
                                                       ousstatus_entity_id=oussid,
                                                       output_dir=products_dir)

        LOG.info('Creating casa restore script %s', script_file)

        # This is hardcoded.
        tmpvislist = []

        # ALMA default
        ocorr_mode = 'ca'

        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, filext = os.path.splitext(filename)
            tmpvislist.append(filename)
        task_string = "    hifa_restoredata (vis=%s, session=%s, ocorr_mode='%s')" % (tmpvislist, session_list,
                                                                                      ocorr_mode)

        template = '''h_init()
try:
%s
finally:
    h_save()
''' % task_string

        with open(script_file, 'w') as casa_restore_file:
            casa_restore_file.write(template)

        LOG.info('Copying casa restore script %s to %s' % (script_file, out_script_file))
        shutil.copy(script_file, out_script_file)

        return os.path.basename(out_script_file)
