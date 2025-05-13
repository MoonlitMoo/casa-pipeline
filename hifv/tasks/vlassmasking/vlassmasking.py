import copy
import os

import numpy as np
from casatasks import imsmooth

from pipeline import infrastructure
from pipeline.hifv.heuristics.vip_helper_functions import (edit_pybdsf_islands,
                                                           mask_from_catalog,
                                                           run_bdsf)
from pipeline.infrastructure import (basetask, casa_tasks, casa_tools,
                                     task_registry, vdp)

LOG = infrastructure.get_logger(__name__)


class VlassmaskingResults(basetask.Results):
    """Results class for the hifv_vlassmasking pipeline smoothing task.  Used on VLASS measurement sets.

    The class inherits from basetask.Results

    """

    def __init__(self, catalog_fits_file=None,
                 catalog_search_size=None,
                 tier1mask=None, tier2mask=None, outfile=None,
                 combinedmask=None, number_islands_found=None,
                 number_islands_found_onedeg=None,
                 num_rejected_islands=None, num_rejected_islands_onedeg=None,
                 pixelfractions=None,
                 relativefraction=None,
                 relativefraction_onedeg=None,
                 plotmask=None,
                 maskingmode=None):
        """
        Args:
            final(list): final list of tables (not used in this task)
            pool(list): pool list (not used in this task)
            preceding(list): preceding list (not used in this task)
            catalog_fits_files(str): Path to a PyBDSF sky catalog in FITS format.
                                     Default at AOC is '/home/vlass/packages/VLASS1Q.fits'
            outfile(str): Output file - sum of masks
            combinedmask(str):  Final combined mask

        """
        super().__init__()

        self.catalog_fits_file = catalog_fits_file
        self.catalog_search_size = catalog_search_size
        self.tier1mask = tier1mask
        self.tier2mask = tier2mask
        self.outfile = outfile
        self.combinedmask = combinedmask
        self.number_islands_found = number_islands_found
        self.number_islands_found_onedeg = number_islands_found_onedeg
        self.num_rejected_islands = num_rejected_islands
        self.num_rejected_islands_onedeg = num_rejected_islands_onedeg
        self.pixelfractions = pixelfractions
        self.relativefraction = relativefraction
        self.relativefraction_onedeg = relativefraction_onedeg
        self.plotmask = plotmask
        self.maskingmode = maskingmode

        self.pipeline_casa_task = 'Vlassmasking'

    def merge_with_context(self, context):
        """
        Args:
            context(:obj:): Pipeline context object
        """
        # Update the mask name in clean list pending.
        if len(context.clean_list_pending[0]) == 0:
            LOG.error('Clean list pending is empty. Mask name was not set for imaging target.')
            return
        elif 'mask' in context.clean_list_pending[0].keys() and context.clean_list_pending[0]['mask']:
            LOG.info('Updating existing clean list pending mask selection with {}.'.format(self.combinedmask))
        else:
            LOG.info('Setting clean list pending mask selection to {}.'.format(self.combinedmask))

        # Is mask is a list then insert new mask to appropriate position (0 if tier-1 mode i.e.
        # iter1, 1 if tier-2 mode i.e. iter2)
        clp_mask = context.clean_list_pending[0]['mask']
        if type(clp_mask) is list:
            context.clean_list_pending[0]['mask'].insert(1 if self.maskingmode == 'vlass-se-tier-2' else 0, self.combinedmask)
        # Cleaning with pb mask only must always be on last place, see PIPE-977
        elif clp_mask == 'pb':
            context.clean_list_pending[0]['mask'] = [self.combinedmask, 'pb']
        else:
            context.clean_list_pending[0]['mask'] = [clp_mask, self.combinedmask] if (self.maskingmode == 'vlass-se-tier-2'
                                                                                      and clp_mask) else self.combinedmask
        return

    def __repr__(self):
        # return 'VlassmaskingResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'VlassmaskingResults:'


class VlassmaskingInputs(vdp.StandardInputs):
    """Inputs class for the hifv_vlassmasking pipeline task.  Used in conjunction with VLASS measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    vlass_ql_database = vdp.VisDependentProperty(default='/home/vlass/packages/VLASS1Q.fits')
    maskingmode = vdp.VisDependentProperty(default='vlass-se-tier-1')
    catalog_search_size = vdp.VisDependentProperty(default=1.5)

    def __init__(self, context, vis=None, vlass_ql_database=None, maskingmode=None,
                 catalog_search_size=None):
        """
            Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set
            vlass_ql_database(str): Path to a PyBDSF sky catalog in FITS format.
                                    Default at AOC is '/home/vlass/packages/VLASS1Q.fits'
            maskingmode(str): Two modes are: vlass-se-tier-1 (QL mask) and vlass-se-tier-2 (combined mask)
            catalog_search_size (float): The half-width (in degrees) of the catalog search centered on the
                                        image's reference pixel.
        """

        self.context = context
        self.vis = vis
        self.vlass_ql_database = vlass_ql_database
        self.maskingmode = maskingmode
        self.catalog_search_size = catalog_search_size


@task_registry.set_equivalent_casa_task('hifv_vlassmasking')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Vlassmasking(basetask.StandardTaskTemplate):
    """Class for the hifv_vlassmasking pipeline task.  Used on VLASS measurement sets.

        The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = VlassmaskingInputs
    is_multi_vis_task = True

    def prepare(self):
        """Method where the VLASS Masking operation is executed.

        The mask creation steps are critical pieces of the VIP workflow. Mask creation uses islands
        identified using pyBDSF and the process is described in detail in VLASS Memo #15. If the
        image to be produced is in a region of the sky where no components were identified, this step
        and subsequent steps will not succeed. Thus, several metrics are needed to evaluate the efficacy
        of mask creation in this step.

        Return:
            VlassmaskingResults() type object, with final output mask name and metadata information
        """

        LOG.debug("This Vlassmasking class is running.")

        maskname_base = self._get_maskname_base()

        shapelist = self.inputs.context.clean_list_pending[0]['imsize']
        mask_shape = np.array([shapelist[0], shapelist[1], 1, 1])
        phasecenter = self.inputs.context.clean_list_pending[0]['phasecenter']
        cell = self.inputs.context.clean_list_pending[0]['cell']
        frequency = self.inputs.context.clean_list_pending[0]['reffreq']
        QLmask = '.QLcatmask-tier1.mask'
        catalog_fits_file = ''
        tier1mask = ''
        tier2mask = ''
        outfile = ''
        combinedmask = ''
        plotmask = ''
        number_islands_found = 0
        number_islands_found_onedeg = 0
        num_rejected_islands = 0
        num_rejected_islands_onedeg = 0
        pixelfractions = {'tier1': 0.0,
                          'tier1_onedeg': 0.0,
                          'tier2': 0.0,
                          'tier2_onedeg': 0.0,
                          'final': 0.0,
                          'final_onedeg': 0.0
                          }
        relativefraction = 0.0
        relativefraction_onedeg = 0.0
        widthdeg = 1.0

        # Work around for CAS-13338
        mask_csys_rec = self.inputs.context.clean_list_pending[0]['heuristics'].get_parallel_cont_synthesis_imager_csys(
            phasecenter, imsize=shapelist, cell=cell)

        # Test parameters for reference
        # Location of catalog file at the AOC
        # catalog_fits_file = '/home/vlass/packages/VLASS1Q.fits'
        # imsize = shapelist[0]
        # wprojplanes = 32
        # cfcache = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(wprojplanes) + '_conjT.cf'
        # cfcache_nowb = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(
        #     wprojplanes) + '_conjT_psf_wbawp_False.cf'
        # phasecenter = 'J2000 13:33:35.814 +16.44.04.255'
        # mask_shape = np.array([imsize, imsize, 1, 1])
        # frequency = '3.0GHz'

        LOG.info("Executing with masking mode: {!s}".format(self.inputs.maskingmode))

        if self.inputs.maskingmode == 'vlass-se-tier-1':
            if not os.path.exists(self.inputs.vlass_ql_database) and self.inputs.maskingmode == 'vlass-se-tier-1':
                LOG.error("VLASS Quicklook database file {!s} does not exist.".format(self.inputs.vlass_ql_database))

            LOG.debug("Executing mask_from_catalog masking mode = {!s}".format(self.inputs.maskingmode))

            catalog_fits_file = self.inputs.vlass_ql_database

            tier1mask = maskname_base + QLmask

            number_islands_found, \
                number_islands_found_onedeg = mask_from_catalog(catalog_fits_file=catalog_fits_file,
                                                                catalog_search_size=self.inputs.catalog_search_size,
                                                                mask_shape=mask_shape, frequency=frequency, cell=cell,
                                                                phasecenter=phasecenter,
                                                                mask_name=tier1mask, csys_rec=mask_csys_rec)

            # Compute fraction of pixels enclosed in the tier-1 mask
            with casa_tools.ImageReader(tier1mask) as myia:
                computechunk = myia.getchunk()
                pixelfractions['tier1'] = computechunk.sum() / computechunk.size

            # Compute fraction of pixels enclosed in the inner square degree for the tier-1 mask
            pixelsum, pixelfractions['tier1_onedeg'] = self._computepixelfraction(widthdeg, tier1mask)

            LOG.info(" ")
            LOG.info("Pixel fraction over entire tier-1 mask: {!s}".format(pixelfractions['tier1']))
            LOG.info("Pixel fraction over inner {!s} degree of tier-1 mask: {!s}".format(widthdeg,
                                                                                         pixelfractions['tier1_onedeg']))
            LOG.info(" ")

            combinedmask = tier1mask
            plotmask = combinedmask

            if number_islands_found_onedeg == 0 or pixelfractions['tier1_onedeg'] == 0.0:
                LOG.error("No islands found or pixel fraction is zero.")

        elif self.inputs.maskingmode == 'vlass-se-tier-2':

            LOG.debug("Executing mask_from_catalog masking mode = {!s}".format(self.inputs.maskingmode))

            # Obtain Tier 1 mask name
            imaging_target_mask_list = copy.deepcopy(self.inputs.context.clean_list_pending[0]['mask'])
            if type(imaging_target_mask_list) is list:
                # pb string is a placeholder for cleaning without mask
                try:
                    imaging_target_mask_list.remove('pb')
                except ValueError:
                    pass
                # Always take first mask in the list
                finally:
                    imaging_target_mask_list = imaging_target_mask_list[0] if len(imaging_target_mask_list) > 0 else ''
            tier1mask = maskname_base + QLmask if not imaging_target_mask_list else imaging_target_mask_list

            # Obtain image name
            imagename_base = self._get_bdsf_imagename(maskname_base, iter=1)

            initial_catalog_fits_file = self.bdsfcompute(imagename_base)

            suffix = ".secondmask.mask"
            tier2mask = maskname_base + suffix

            catalog_fits_file, num_rejected_islands, num_rejected_islands_onedeg\
                = edit_pybdsf_islands(catalog_fits_file=initial_catalog_fits_file, phasecenter=phasecenter)

            if not os.path.exists(catalog_fits_file):
                LOG.error("Catalog file {!s} does not exist.".format(catalog_fits_file))

            number_islands_found, \
                number_islands_found_onedeg = mask_from_catalog(catalog_fits_file=catalog_fits_file,
                                                                catalog_search_size=self.inputs.catalog_search_size,
                                                                mask_shape=mask_shape, frequency=frequency, cell=cell,
                                                                phasecenter=phasecenter,
                                                                mask_name=tier2mask, csys_rec=mask_csys_rec)

            # combine first and second order masks
            try:
                outfile = maskname_base + '.sum_of_masks.mask'
                task = casa_tasks.immath(imagename=[tier2mask, tier1mask], expr='IM0+IM1', outfile=outfile)
                runtask = self._executor.execute(task)
            except Exception as e:
                LOG.error(f'Failed to combine mask files {tier1mask} and {tier2mask} with exception {e}')

            myim = casa_tools.imager
            LOG.info("Executing imager.mask()...")
            combinedmask = maskname_base + '.combined-tier2.mask'
            myim.mask(image=outfile, mask=combinedmask, threshold=0.5)
            myim.close()

            # Compute fraction of pixels enclosed in the tier-1 mask
            with casa_tools.ImageReader(tier1mask) as myia:
                computechunk = myia.getchunk()
                tier1pixelsum = computechunk.sum()
                pixelfractions['tier1'] = computechunk.sum() / computechunk.size

            # Compute fraction of pixels enclosed in the inner square degree for the tier-1 mask
            tier1pixelsum_onedeg, pixelfractions['tier1_onedeg'] = self._computepixelfraction(widthdeg, tier1mask)

            LOG.info(" ")
            LOG.info("Pixel fraction over entire tier-1 mask: {!s}".format(pixelfractions['tier1']))
            LOG.info("Pixel fraction over inner {!s} degree of tier-1 mask: {!s}".format(pixelfractions['tier1'],
                                                                                         pixelfractions['tier1_onedeg']))
            LOG.info(" ")

            # Compute fraction of pixels enclosed in the tier-2 mask
            with casa_tools.ImageReader(tier2mask) as myia:
                computechunk = myia.getchunk()
                tier2pixelsum = computechunk.sum()
                pixelfractions['tier2'] = computechunk.sum() / computechunk.size

            # Compute fraction of pixels enclosed in the inner square degree for the tier-2 mask
            tier2pixelsum_onedeg, pixelfractions['tier2_onedeg'] = self._computepixelfraction(widthdeg, tier2mask)

            LOG.info(" ")
            LOG.info("Pixel fraction over entire tier-2 mask: {!s}".format(pixelfractions['tier2']))
            LOG.info("Pixel fraction over inner {!s} degree of tier-2 mask: {!s}".format(widthdeg,
                                                                                         pixelfractions['tier2_onedeg']))
            LOG.info(" ")

            # Compute fraction of pixels enclosed in the final mask
            with casa_tools.ImageReader(combinedmask) as myia:
                computechunk = myia.getchunk()
                finalpixelsum = computechunk.sum()
                pixelfractions['final'] = computechunk.sum() / computechunk.size

            # Compute fraction of pixels enclosed in the inner square degree for the final combined mask
            finalpixelsum_onedeg, pixelfractions['final_onedeg'] = self._computepixelfraction(widthdeg, combinedmask)

            LOG.info(" ")
            LOG.info("Pixel fraction over entire final combined mask: {!s}".format(pixelfractions['final']))
            LOG.info("Pixel fraction over inner {!s} degree of final combined mask: {!s}".format(widthdeg,
                                                                                                 pixelfractions['final_onedeg']))
            LOG.info(" ")

            # Compute the fractional increase of masked pixels in Final mask relative to Quicklook Mask
            # Compute the fractional increase of masked pixels in Final mask relative to Quicklook Mask in the inner
            # square degree
            relativefraction_str = str(
                (finalpixelsum - tier1pixelsum) / tier1pixelsum) + ' =  (('+str(finalpixelsum) + ' - '+str(tier1pixelsum) + ') /' + str(tier1pixelsum) + ')'
            relativefraction_onedeg_str = str((finalpixelsum_onedeg - tier1pixelsum_onedeg) / tier1pixelsum_onedeg) + ' =  (('+str(
                finalpixelsum_onedeg) + ' - '+str(tier1pixelsum_onedeg) + ') /' + str(tier1pixelsum_onedeg) + ')'

            LOG.info("Relative fraction: {!s}".format(relativefraction_str))
            LOG.info("Relative fraction (inner square degree): {!s}".format(relativefraction_onedeg_str))

            relativefraction = (finalpixelsum - tier1pixelsum) / tier1pixelsum
            relativefraction_onedeg = (finalpixelsum_onedeg - tier1pixelsum_onedeg) / tier1pixelsum_onedeg

            plotmask = combinedmask

        else:
            LOG.error("Invalid maskingmode input.")

        return VlassmaskingResults(catalog_fits_file=catalog_fits_file,
                                   catalog_search_size=1.5,
                                   tier1mask=tier1mask, tier2mask=tier2mask, outfile=outfile,
                                   combinedmask=combinedmask, number_islands_found=number_islands_found,
                                   number_islands_found_onedeg=number_islands_found_onedeg,
                                   num_rejected_islands=num_rejected_islands,
                                   num_rejected_islands_onedeg=num_rejected_islands_onedeg,
                                   pixelfractions=pixelfractions,
                                   relativefraction=relativefraction,
                                   relativefraction_onedeg=relativefraction_onedeg,
                                   plotmask=plotmask,
                                   maskingmode=self.inputs.maskingmode)

    def analyse(self, results):
        return results

    def bdsfcompute(self, imagename_base):

        imsmooth(imagename=imagename_base + ".tt0", major='5arcsec', minor='5arcsec', pa='0deg',
                 outfile=imagename_base + ".smooth5.tt0")

        fitsimage = imagename_base + ".smooth5.fits"
        export_task = casa_tasks.exportfits(imagename=imagename_base + ".smooth5.tt0",
                                            fitsimage=fitsimage)
        runtask = self._executor.execute(export_task)

        bdsf_result = run_bdsf(infile=fitsimage)

        # Return the catalogue fits file name
        return fitsimage.replace('.fits', '.cat.fits')

    def _get_maskname_base(self):
        """
        Returns base name for the mask.

        If context.clean_list_pending is populated, then it will take 'imagename' parameter from the first imlist entry.
        The 'STAGENUMBER' substring is replaced by the current stage number.
        """
        # context.stage is defined as '{context.task_counter}_{context.subtask_counter}'
        # Because this is a not multi_vis task (is_multi_vis_task=False, in contrast of hif_makeimages), the mask creation is done
        # as a subtask, with subtask_counter increases for each MS. Here we use '{context.task_counter}_0' to keep the
        # consistency with the naming convention of imaging products.

        if 'imagename' in self.inputs.context.clean_list_pending[0].keys():
            return self.inputs.context.clean_list_pending[0]['imagename'].replace('STAGENUMBER',
                                                                                  '{}_0'.format(
                                                                                      self.inputs.context.task_counter))
        else:
            return 'VIP_'

    def _get_bdsf_imagename(self, imagename_base, iter=1):
        """
        Image file name to be used as input to BDSF blob finder.

        Obtain the image name from the latest MakeImagesResult object in context.results. The iter parameter controlls
        which iteration image name should be returned. In the current VLASS-SE-CONT workflow, iter=1.

        If hif_makimages result is not found, then construct image name from imagename_base argument. This replicates 
        the image name used in the VLASS Memo 15 VIP script.
        """
        for result in self.inputs.context.results[::-1]:
            result_meta = result.read()
            if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith('hif_makeimages'):
                return result_meta.results[0].iterations[iter]['image']

        # In case hif_makeimages result was not found.
        return imagename_base + 'iter1b.image'

    def _computepixelfraction(self, widthdeg, maskimage):

        with casa_tools.ImageReader(maskimage) as myia:
            # myia = casa_tools.image
            # myia.open(maskimage)
            mask_csys = myia.coordsys()

            xpixel = mask_csys.torecord()['direction0']['crpix'][0]
            ypixel = mask_csys.torecord()['direction0']['crpix'][1]
            xdelta = mask_csys.torecord()['direction0']['cdelt'][0]  # in radians
            ydelta = mask_csys.torecord()['direction0']['cdelt'][1]  # in radians
            onedeg = 1.0 * np.pi / 180.0  # conversion
            # widthdeg = 0.4  # degrees
            boxhalfxwidth = np.abs((onedeg * widthdeg / 2.0) / xdelta)
            boxhalfywidth = np.abs((onedeg * widthdeg / 2.0) / ydelta)

            blcx = xpixel - boxhalfxwidth
            blcy = ypixel - boxhalfywidth
            if blcx < 0:
                blcx = 0
            if blcy < 0:
                blcy = 0
            blc = [blcx, blcy]

            trcx = xpixel + boxhalfxwidth
            trcy = ypixel + boxhalfywidth
            if trcx > myia.getchunk().shape[0]:
                trcx = myia.getchunk().shape[0]
            if trcy > myia.getchunk().shape[1]:
                trcy = myia.getchunk().shape[1]
            trc = [trcx, trcy]

            myrg = casa_tools.regionmanager
            r1 = myrg.box(blc=blc, trc=trc)

            y = myia.getregion(r1)
            pixelsum = y.sum()
            pixelfraction = y.sum() / y.size

            # myia.done()
            myrg.done()

        return pixelsum, pixelfraction
