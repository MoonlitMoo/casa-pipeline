import os
import shutil
import tarfile

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.restoredata import restoredata
from pipeline.infrastructure import task_registry
from pipeline.infrastructure import casa_tasks
from ..finalcals import applycals
from ..hanning import hanning
from ..importdata import importdata

LOG = infrastructure.get_logger(__name__)


class VLARestoreDataInputs(restoredata.RestoreDataInputs):
    bdfflags = vdp.VisDependentProperty(default=False)
    ocorr_mode = vdp.VisDependentProperty(default='co')
    asis = vdp.VisDependentProperty(default='Receiver CalAtmosphere')
    gainmap = vdp.VisDependentProperty(default=False)

    def __init__(self, context, copytoraw=None, products_dir=None, rawdata_dir=None,
                 output_dir=None, session=None, vis=None, bdfflags=None, lazy=None, asis=None,
                 ocorr_mode=None, gainmap=None):
        super(VLARestoreDataInputs, self).__init__(context, copytoraw=copytoraw,
                                                   products_dir=products_dir, rawdata_dir=rawdata_dir,
                                                   output_dir=output_dir, session=session,
                                                   vis=vis, bdfflags=bdfflags, lazy=lazy, asis=asis,
                                                   ocorr_mode=ocorr_mode)

        self.gainmap = gainmap

@task_registry.set_equivalent_casa_task('hifv_restoredata')
class VLARestoreData(restoredata.RestoreData):

    Inputs = VLARestoreDataInputs

    def prepare(self):
        """
        Prepare and execute an export data job appropriate to the
        task inputs.
        """
        # Create a local alias for inputs, so we're not saying
        # 'self.inputs' everywhere
        inputs = self.inputs

        # Force inputs.vis and inputs.session to be a list.
        sessionlist = inputs.session
        if isinstance(sessionlist, str):
            sessionlist = [sessionlist, ]
        tmpvislist = inputs.vis
        if isinstance(tmpvislist, str):
            tmpvislist = [tmpvislist, ]
        vislist = []
        for vis in tmpvislist:
            if os.path.dirname(vis) == '':
                vislist.append(os.path.join(inputs.rawdata_dir, vis))
            else:
                vislist.append(vis)

        # Download ASDMs from the archive or products_dir to rawdata_dir.
        #   TBD: Currently assumed done somehow
        # Copy the required calibration products from someplace on disK
        #   default ../products to ../rawdata
        if inputs.copytoraw:
            self._do_copy_manifest_toraw('*pipeline_manifest.xml')
            pipemanifest = self._do_get_manifest('*pipeline_manifest.xml', '*cal*pipeline_manifest.xml')
            self._do_copytoraw(pipemanifest)
        else:
            pipemanifest = self._do_get_manifest('*pipeline_manifest.xml', '*cal*pipeline_manifest.xml')

        # Retrieve smoothing and spectral line spws information from the manifest
        spws_to_smooth = None  # If not found in the manifest, assume old data.
        specline_spws = 'none'
        params = None
        ms_name = "{}.ms".format(os.path.basename(vislist[0]))
        for asdm in pipemanifest.get_ous().findall(f".//asdm[@name=\'{ms_name}\']"):
            params = getattr(asdm.find('restoredata'), 'attrib', None)
        if params:
            spws_to_smooth = params.get('smoothed_spws', None)
            specline_spws = params.get('specline_spws', 'none')

            LOG.debug("Found smoothed_spws: {} and specline_spws: {} in the manifest".format(spws_to_smooth, specline_spws))
        else:
            LOG.debug("Didn't find smoothed_spws, specline_spws in the manifest.")

        # If there was an empty string, indicating no specline spws, in the manifest,
        # set the specline_spws to 'none' for hifv_importdata.
        if len(specline_spws) == 0:
            specline_spws = 'none'

        # Convert ASDMS assumed to be on disk in rawdata_dir. After this step
        # has been completed the MS and MS.flagversions directories will exist
        # and MS,flagversions will contain a copy of the original MS flags,
        # Flags.Original.
        #    TBD: Add error handling
        import_results = self._do_importasdm(sessionlist=sessionlist, vislist=vislist, specline_spws=specline_spws)

        for ms in self.inputs.context.observing_run.measurement_sets:
            self._do_hanningsmooth(spws_to_smooth=spws_to_smooth)

        # Restore final MS.flagversions and flags
        self._do_restore_flags(pipemanifest)

        # Get the session list and the visibility files associated with
        # each session.
        session_names, session_vislists = self._get_sessions()

        # Restore calibration tables
        self._do_restore_caltables(pipemanifest, session_names=session_names, session_vislists=session_vislists)

        # Import calibration apply lists
        self._do_restore_calstate(pipemanifest)

        # Apply the calibrations.
        apply_results = self._do_applycal()

        # Return the results object, which will be used for the weblog
        return restoredata.RestoreDataResults(import_results, apply_results)

    # Override generic method and use an ALMA specific one. Not much difference
    # now but should simplify parameters in future
    def _do_importasdm(self, sessionlist, vislist, specline_spws):
        inputs = self.inputs
        container = vdp.InputsContainer(
            importdata.VLAImportData, inputs.context,
            vis=vislist, session=sessionlist, save_flagonline=False,
            lazy=inputs.lazy, bdfflags=inputs.bdfflags,
            asis=inputs.asis, ocorr_mode=inputs.ocorr_mode,
            specline_spws=specline_spws)
        importdata_task = importdata.VLAImportData(container)
        return self._executor.execute(importdata_task, merge=True)

    def _do_restore_flags(self, pipemanifest, flag_version_name=None):
        if flag_version_name is None:
            try_flag_version_names = ['statwt_1', 'Pipeline_Final']
        else:
            try_flag_version_names = [flag_version_name]
        inputs = self.inputs
        if pipemanifest is not None:
            ouss = pipemanifest.get_ous()
        else:
            ouss = None

        # Loop over MS list in working directory
        for ms in inputs.context.observing_run.measurement_sets:

            # Remove imported MS.flagversions from working directory
            flagversion = ms.basename + '.flagversions'
            flagversionpath = os.path.join(inputs.output_dir, flagversion)
            if os.path.exists(flagversionpath):
                LOG.info('Removing default flagversion for %s' % ms.basename)
                shutil.rmtree(flagversionpath)

            # Untar MS.flagversions file in rawdata_dir to output_dir
            if ouss is not None:
                tarfilename = os.path.join(inputs.rawdata_dir,
                                           pipemanifest.get_final_flagversions(ouss)[ms.basename])
            else:
                tarfilename = os.path.join(inputs.rawdata_dir,
                                           ms.basename + '.flagversions.tgz')
            LOG.info('Extracting %s' % flagversion)
            LOG.info('    From %s' % tarfilename)
            LOG.info('    Into %s' % inputs.output_dir)
            with tarfile.open(tarfilename, 'r:gz') as tar:
                tar.extractall(path=inputs.output_dir)

            # Restore final flags version using flagmanager
            try_version = None
            for flagname in try_flag_version_names:
                if os.path.exists(os.path.join(flagversionpath, 'flags.{}'.format(flagname))):
                    try_version = flagname
                    break
            LOG.info('Restoring final flags for %s from flag version %s' % (ms.basename, try_version))
            task = casa_tasks.flagmanager(vis=ms.name,
                                          mode='restore',
                                          versionname=try_version)
            try:
                self._executor.execute(task)
            except Exception:
                LOG.error("Application of final flags failed for %s" % ms.basename)
                raise

    def _do_hanningsmooth(self, spws_to_smooth):
        container = vdp.InputsContainer(hanning.Hanning, self.inputs.context,
                                        spws_to_smooth=spws_to_smooth)
        hanning_task = hanning.Hanning(container)
        return self._executor.execute(hanning_task, merge=True)

    def _do_applycal(self):

        flagsum = True
        flagdetailedsum = True
        if self.inputs.gainmap:
            flagsum = False
            flagdetailedsum = False

        container = vdp.InputsContainer(applycals.Applycals, self.inputs.context, intent='',
                                        field='', spw='', gainmap=self.inputs.gainmap,
                                        flagsum=flagsum, flagdetailedsum=flagdetailedsum)
        applycal_task = applycals.Applycals(container)
        return self._executor.execute(applycal_task, merge=True)
