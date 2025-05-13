import os
import sys
import shutil
import collections
import tarfile
import io
import xml.etree.cElementTree as eltree

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.exportdata import exportdata
from pipeline.h.tasks.common import manifest
from pipeline.infrastructure import task_registry
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import utils
from pipeline.infrastructure.filenamer import fitsname
from . import vlaifaqua

LOG = infrastructure.get_logger(__name__)


class VLAExportDataInputs(exportdata.ExportDataInputs):
    gainmap = vdp.VisDependentProperty(default=False)
    exportcalprods = vdp.VisDependentProperty(default=False)
    imaging_products_only = vdp.VisDependentProperty(default=False)

    @exportcalprods.postprocess
    def exportcalprods(self, value):
        # calibration products are exported when
        # (1) not imaging_product_only and not exportmses
        # OR
        # (2) not imaging_product_only and both exportmses and exportcalprods are True
        if self.imaging_products_only:
            return False
        elif not self.exportmses:
            return True
        else:
            return value

    def __init__(self, context, output_dir=None, session=None, vis=None, exportmses=None,
                 tarms=None, exportcalprods=None,
                 pprfile=None, calintents=None, calimages=None, targetimages=None, products_dir=None, gainmap=None,
                 imaging_products_only=None):
        super().__init__(context, output_dir=output_dir, session=session, vis=vis,
                         exportmses=exportmses, tarms=tarms, pprfile=pprfile, calintents=calintents,
                         calimages=calimages, targetimages=targetimages,
                         products_dir=products_dir, imaging_products_only=imaging_products_only)
        self.gainmap = gainmap
        self.exportcalprods = exportcalprods


@task_registry.set_equivalent_casa_task('hifv_exportdata')
class VLAExportData(exportdata.ExportData):

    # link the accompanying inputs to this task
    Inputs = VLAExportDataInputs

    def prepare(self):
        results = super().prepare()

        # PIPE-1205
        PbcorFits = collections.namedtuple('PbcorFits', 'pbcorimage pbcorfits nonpbcor_fits')
        # results.targetimages[0] is the same as self.inputs.context.sciimlist.get_imlist()

        # for each target in the targetimages results
        for target in results.targetimages[0]:
            fitsqueue = []
            if target['multiterm']:
                for nt in range(target['multiterm']):
                    pbcorimage = target['imagename'] + f'.pbcor.tt{nt}'
                    non_pbcorimage = target['imagename'] + f'.tt{nt}'
                    fitsfile = fitsname(self.inputs.products_dir, pbcorimage)
                    nonpbcor_fits = fitsname(self.inputs.products_dir, non_pbcorimage)
                    if os.path.exists(pbcorimage) and fitsfile not in target['fitsfiles']:
                        fitsqueue.append(PbcorFits(pbcorimage, fitsfile, nonpbcor_fits))
            else:
                pbcorimage = target['imagename'] + '.pbcor'
                fitsfile = fitsname(self.inputs.products_dir, pbcorimage)
                nonpbcor_fits = fitsname(self.inputs.products_dir, target['imagename'])
                if os.path.exists(pbcorimage) and fitsfile not in target['fitsfiles']:
                    fitsqueue.append(PbcorFits(pbcorimage, fitsfile, nonpbcor_fits))

            for ee in fitsqueue:
                # make the pbcor FITS images
                self._shorten_spwlist(ee.pbcorimage)
                task = casa_tasks.exportfits(imagename=ee.pbcorimage, fitsimage=ee.pbcorfits, velocity=False, optical=False,
                                        bitpix=-32, minpix=0, maxpix=-1, overwrite=True, dropstokes=False,
                                        stokeslast=True)
                self._executor.execute(task)

                # add new pbcor fits to 'fitsfiles'
                target['fitsfiles'].append(ee.pbcorfits)
                # add new pbcor fits to fitslist
                results.targetimages[1].append(ee.pbcorfits)

                # if there's a non-pbcor image in 'fitsfiles' move it to 'auxfitsfiles'
                if ee.nonpbcor_fits in target['fitsfiles']:
                    target['auxfitsfiles'].append(ee.nonpbcor_fits)
                    target['fitsfiles'].remove(ee.nonpbcor_fits)

        oussid = self.inputs.context.get_oussid()  # returns string of 'unknown' for VLA

        # Make the imaging vislist and the sessions lists.
        #     Force this regardless of the value of imaging_only_products
        _, _, _, vislist = super()._make_lists(
            self.inputs.context, self.inputs.session, self.inputs.vis, imaging_only_mses=None)

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
        auxfproducts = self._do_if_auxiliary_products(prefix, self.inputs.output_dir, self.inputs.products_dir, vislist,
                                                   self.inputs.imaging_products_only)

        # Export the AQUA report
        pipe_aqua_reportfile = self._export_aqua_report(context=self.inputs.context,
                                                        oussid=prefix,
                                                        products_dir=self.inputs.products_dir,
                                                        report_generator=vlaifaqua.VLAAquaXmlGenerator(),
                                                        weblog_filename=results.weblog)

        # Update the manifest
        if auxfproducts is not None or pipe_aqua_reportfile is not None:
            manifest_file = os.path.join(self.inputs.products_dir, results.manifest)
            recipe_name = self.inputs.context.project_structure.recipe_name
            self._add_to_manifest(manifest_file, auxfproducts, False, [], pipe_aqua_reportfile, oussid, recipe_name)

        # Set specline_spws and smoothed_spw info that was used in the calibration run
        self._export_specline_smoothing(results, vislist)

        return results

    def _shorten_spwlist(self, image):
        # PIPE-325: abbreviate 'spw' and/or 'virtspw' for FITS header when spw string is "too long"
        # TODO: elevate this function to h exportdata after the PL2021 release so that it can be used
        #   here as well as in h_exportdata.  Too close to a release candidate at the moment to disrupt
        #   ALMA validation.
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

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        """
        Save the CASA restore script.
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

        # VLA ocorr_value
        ocorr_mode = 'co'

        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, _ = os.path.splitext(filename)
            tmpvislist.append(filename)

        task_string = "    hifv_restoredata (vis=%s, session=%s, ocorr_mode='%s', gainmap=%s)" % (
            tmpvislist, session_list, ocorr_mode, self.inputs.gainmap)

        # Is this a VLASS execution?
        vlassmode = False
        for result in context.results:
            try:
                resultinputs = result.read()[0].inputs
                if 'vlass' in resultinputs['checkflagmode']:
                    vlassmode = True
            except:
                continue

        # Is hif_selfcal/hif_uvcontsub executed?
        selfcal = False
        uvcontsub = False
        for result in context.results:
            result_meta = result.read()
            if hasattr(result_meta, 'pipeline_casa_task'):
                if result_meta.pipeline_casa_task.startswith('hif_selfcal'):
                    selfcal = True
                if result_meta.pipeline_casa_task.startswith('hif_uvcontsub'):
                    uvcontsub = True

        if vlassmode:
            task_string += "\n    hifv_fixpointing()"

        task_string += "\n    hifv_statwt()"
        task_string += "\n    hifv_mstransform()"
        if uvcontsub:
            task_string += "\n    hif_uvcontsub()"
        if selfcal:
            task_string += "\n    hif_selfcal(restore_only=True)"

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

    def _export_final_flagversion(self, context, vis, flag_version_name, products_dir):
        """
        PIPE-1553: include additional flag versions in tarfile
        """

        # Define the name of the output tarfile
        visname = os.path.basename(vis)
        tarfilename = visname + '.flagversions.tgz'
        if os.path.exists(tarfilename):
            os.remove(tarfilename)
        LOG.info('Storing final flags for %s in %s', visname, tarfilename)

        # Create the tar file
        tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")

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
        flag_keys = [y['name'] for y in flag_dict.values()]

        export_final_flags_dict = dict()
        if flag_version_name in flag_keys:
            flag_comment = [y for y in flag_dict.values() if y['name'] == flag_version_name][0]['comment']
            export_final_flags_dict[flag_version_name] = flag_comment

        # rename flagversions to make them more deterministic
        if 'Applycal_Final' not in flag_keys:
            applycal_flags = sorted([y for y in flag_dict.values() if 'applycal' in y['name']],
                                    key=lambda x: x['name'])
            if applycal_flags:
                export_final_flags_dict['Applycal_Final'] = applycal_flags[-1]['comment']
                task = casa_tasks.flagmanager(vis=vis, mode='rename', oldname=applycal_flags[-1]['name'],
                                              versionname='Applycal_Final', comment=applycal_flags[-1]['comment'])
                self._executor.execute(task)
        else:
            applycal_comment = [y for y in flag_dict.values() if y['name'] == 'Applycal_Final'][0]['comment']
            export_final_flags_dict['Applycal_Final'] = applycal_comment

        if 'hifv_checkflag_target-vla' not in flag_keys:
            target_RFI_key = [y for y in flag_dict.values() if 'hifv_checkflag_target-vla' in y['name']]
            if target_RFI_key:
                export_final_flags_dict['hifv_checkflag_target-vla'] = target_RFI_key[-1]['comment']
                task = casa_tasks.flagmanager(vis=vis, mode='rename', oldname=target_RFI_key[-1]['name'],
                                              versionname='hifv_checkflag_target-vla', comment=target_RFI_key[-1]['comment'])
                self._executor.execute(task)
        else:
            hifv_comment = [y for y in flag_dict.values() if y['name'] == 'hifv_checkflag_target-vla'][0]['comment']
            export_final_flags_dict['hifv_checkflag_target-vla'] = hifv_comment

        if 'statwt_1' in flag_keys: 
            statwt_comment = [y for y in flag_dict.values() if y['name'] == 'statwt_1'][0]['comment']
            export_final_flags_dict['statwt_1'] = statwt_comment

        # recreate tar file
        line = ""
        for flag_version, flag_version_comment in export_final_flags_dict.items():
            # Define the directory to be saved, and where to store in tar archive.
            flagsname = os.path.join(vis + '.flagversions', 'flags.' + flag_version)
            flagsarcname = os.path.join(visname + '.flagversions', 'flags.' + flag_version)
            LOG.info('Saving flag version %s', flag_version)
            tar.add(flagsname, arcname=flagsarcname)
            line += "{} : {}\n".format(flag_version, flag_version_comment)

        line = line.encode(sys.stdout.encoding)
        tar_info.size = len(line)
        tar.addfile(tar_info, io.BytesIO(line))
        tar.close()

        return tarfilename

    def _export_specline_smoothing(self, results, vislist):
        """
        Export the specline_spws and smoothed_spws information to the manifest
        """
        # hifv_importdata only allows one set of specline_spws to be specified for all MSes, so pick the first MS
        mses = self.inputs.context.observing_run.get_measurement_sets()[0]
        spws = mses.get_spectral_windows(science_windows_only=True)

        hanning_smoothed = None
        with casa_tools.TableReader(mses.name + '/SPECTRAL_WINDOW') as table:
            if 'OFFLINE_HANNING_SMOOTH' in table.colnames():
                hanning_smoothed = table.getcol('OFFLINE_HANNING_SMOOTH')
                LOG.debug('Found smoothed spw information in SPECTRAL_WINDOW table')
            else:
                LOG.info("Could not find hanning smoothing information in SPECTRAL WINDOW table.")

        smoothed_spws = []
        if hanning_smoothed is not None:
            for spw in spws:
                real_spwid = self.inputs.context.observing_run.virtual2real_spw_id(spw.id, mses)
                if hanning_smoothed[real_spwid]:
                    smoothed_spws.append(spw.id)

        smoothed_spws_str = ''
        if len(smoothed_spws) > 0:
            smoothed_spws_str = utils.find_ranges(smoothed_spws)

        specline_spws = ''
        specline_spws_list = [str(spw.id) for spw in spws if spw.specline_window]
        if len(specline_spws_list) > 0:
            specline_spws = utils.find_ranges(specline_spws_list)

        # Add the specline_spws and smoothed_spws information to the mainfest
        manifest_inputs = {}
        manifest_inputs['specline_spws'] = specline_spws

        # Only include hanning smoothing information if available
        if hanning_smoothed is not None:
            manifest_inputs['smoothed_spws'] = smoothed_spws_str

        pipemanifest = manifest.PipelineManifest('')
        manifest_file = os.path.join(self.inputs.products_dir, results.manifest)
        pipemanifest.import_xml(manifest_file)

        for asdm in pipemanifest.get_ous().findall(f".//asdm[@name=\'{vislist[0]}\']"):
            newinputs = {key: str(value) for (key, value) in manifest_inputs.items()}  # stringify the values
            eltree.SubElement(asdm, "restoredata", newinputs)
        pipemanifest.write(manifest_file)
