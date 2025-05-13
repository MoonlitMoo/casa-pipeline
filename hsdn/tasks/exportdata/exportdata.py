"""
Single-Dish Exportdata task dedicated to NRO data.

Please see hsd/tasks/exportdata/exportdata.py for generic
description on how Exportdata task works.
"""
import collections
import os
from typing import List, Optional
from xml.etree.ElementTree import Element

import pipeline.h.tasks.exportdata.exportdata as exportdata
import pipeline.hsd.tasks.exportdata.exportdata as sdexportdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.project import ProjectStructure
from pipeline.hsdn.tasks.importdata.importdata import NROImportDataResults
from pipeline.hsdn.tasks.restoredata.restoredata import NRORestoreDataResults
from . import manifest
from . import nrotemplategenerator

# the logger for this module
LOG = infrastructure.get_logger(__name__)


class NROPipelineNameBuilder(exportdata.PipelineProductNameBuilder):
    """Name(str) building utility methods class for NRO Pipeline.

    Methods in this class overrides those in PipelineProductNameBuilder and
    constructs names (paths) of products in the absence of valid OUS status ID
    nor session name in Nobeyama datasets."""

    @classmethod
    def _build_from_oussid(self, basename: str, ousstatus_entity_id: Optional[str]=None,
                           output_dir: Optional[str]=None) -> str:
        """Build a string for use as path.

        Args:
            basename : base name of path
            ousstatus_entity_id : OUS Status ID. not in use.
            output_dir : output directory path

        Returns:
            path string
        """
        return self._join_dir(basename, output_dir)

    @classmethod
    def _build_from_ps_oussid(self, basename: str,
                              project_structure: Optional[ProjectStructure]=None,
                              ousstatus_entity_id: Optional[str]=None,
                              output_dir: Optional[str]=None) -> str:
        """Build a string for use as path.

        Args:
            basename : base name of path
            project_structure : project structure element, not in use.
            ousstatus_entity_id : OUS Status ID, not in use.
            output_dir : output directory path

        Returns:
            path string
        """
        return self._join_dir(basename, output_dir)

    @classmethod
    def _build_from_oussid_session(self, basename: str,
                                   ousstatus_entity_id: Optional[str]=None,
                                   session_name: Optional[str]=None,
                                   output_dir: Optional[str]=None):
        """Build a string for use as path.

        Args:
            basename : base name of path
            ousstatus_entity_id : OUS Status ID, not in use.
            session_name : session name, not in use.
            output_dir : output directory path

        Returns:
            path string
        """
        return self._join_dir(basename, output_dir)


class NROExportDataInputs(sdexportdata.SDExportDataInputs):
    """Inputs class for NROExportData.

    Inputs class must be separated per task class even if
    it's effectively the same."""

    pass


@task_registry.set_equivalent_casa_task('hsdn_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class NROExportData(sdexportdata.SDExportData):
    """A class for exporting Nobeyama data to the products subdirectory.

    It performs the following additional operations after SDExportData does:

    - Export reduction template in a Python file
    - Export nroscale CSV file
    """

    Inputs = NROExportDataInputs

    NameBuilder = NROPipelineNameBuilder

    def prepare(self) -> exportdata.ExportDataResults:
        """Prepare and execute an export data job appropriate to the task inputs.

        Returns:
            object of exportdata.ExportDataResults
        """
        results = super(NROExportData, self).prepare()

        # manifest file
        manifest_file = os.path.join(self.inputs.context.products_dir,
                                     results.manifest)

        # export NRO data reduction template
        template_script = \
            self._export_reduction_template(self.inputs.products_dir)

        if template_script is not None:
            self._update_manifest(manifest_file, script=template_script)

        # export NRO scaling file template
        template_file = \
            self._export_nroscalefile_template(self.inputs.products_dir)

        if template_file is not None:
            self._update_manifest(manifest_file, scalefile=template_file)

        return results

    def _export_reduction_template(self, products_dir: str) -> str:
        """Export reduction template script.

        Exports rebase_and_image.py. It is a Python script to perform baseline
        subtraction and imaging by CASA tasks.

        Args:
            products_dir : product directory path

        Returns:
            script name
        """
        script_name = 'rebase_and_image.py'
        config_name = 'rebase_and_image_config.py'
        script_path = os.path.join(products_dir, script_name)
        config_path = os.path.join(products_dir, config_name)

        status = nrotemplategenerator.generate_script(self.inputs.context,
                                                      script_path,
                                                      config_path)
        return script_name if status is True else None

    def _export_nroscalefile_template(self, products_dir: str) -> str:
        """Export nroscale CSV file.

        Args:
            products_dir : product directory path

        Returns:
            file name of nroscalefile, default:'nroscalefile.csv'
        """
        datafile_name = 'nroscalefile.csv'
        datafile_path = os.path.join(products_dir, datafile_name)

        status = nrotemplategenerator.generate_csv(self.inputs.context,
                                                   datafile_path)
        return datafile_name if status is True else None

    def _update_manifest(self, manifest_file: str, script: str=None,
                         scalefile: str=None):
        """Add Nobeyama specific products to manifest file.

        Args:
            manifest_file : manifest file path
            script : the name of template reduction script
            scalefile : the name of NRO scale file
        """
        pipemanifest = manifest.NROPipelineManifest('')
        pipemanifest.import_xml(manifest_file)
        ouss = pipemanifest.get_ous()

        if script:
            pipemanifest.add_reduction_script(ouss, script)
            pipemanifest.write(manifest_file)

        if scalefile:
            pipemanifest.add_scalefile(ouss, scalefile)
            pipemanifest.write(manifest_file)

    def _export_casa_restore_script(self, context: Context, script_name: str,
                                    products_dir: str, oussid: str,
                                    vislist: List[str],
                                    session_list: List[str]) -> str:
        """Generate and export CASA restore script.

        Args:
            context : pipeline context
            script_name : Name of the restore script
            products_dir : Name of the product directory
            oussid : OUS Status ID
            vislist : a list of MeasurementSet names
            session_list : a list of session id

        Returns:
            path of output CASA script file
        """
        tmpvislist = list(map(os.path.basename, vislist))
        restore_task_name = 'hsdn_restoredata'
        hm_rasterscan = self._get_hm_rasterscan_value(context)
        args = collections.OrderedDict(vis=tmpvislist,
                                       reffile='./nroscalefile.csv',
                                       hm_rasterscan=hm_rasterscan)
        return self._export_casa_restore_script_template(context, script_name,
                                                         products_dir, oussid,
                                                         restore_task_name,
                                                         args)

    def _get_hm_rasterscan_value(self, context: Context) -> str:
        """Retrieve hm_rasterscan value from Results object.

        This method checks if either NROImportDataResults or
        NRORestoreDataResults object is registered to Pipeline context.
        If exists, hm_rasterscan value is retrieved from the results
        object. If no NROImportDataResults nor NRORestoreDataResults
        object is registered, 'time' will be returned.

        Args:
            context: Pipeline context

        Returns:
            The hm_rasterscan value.
        """
        results_filter = filter(
            lambda x: isinstance(x, basetask.ResultsList) and isinstance(x[0], (NROImportDataResults, NRORestoreDataResults)),
            map(lambda x: x.read(), context.results)
        )
        importdata_results = next(results_filter, None)
        if importdata_results:
            hm_rasterscan = importdata_results.inputs.get('hm_rasterscan', 'time')
        else:
            hm_rasterscan = 'time'

        return hm_rasterscan
