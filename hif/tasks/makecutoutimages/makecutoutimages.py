import ast
import collections
import math
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry
from pipeline.infrastructure.utils import get_stokes, imstat_items

LOG = infrastructure.get_logger(__name__)


class MakecutoutimagesResults(basetask.Results):
    def __init__(self, final=None, pool=None, preceding=None,
                 subimagelist=None, subimagenames=None, image_size=None,
                 stats=None):
        super().__init__()

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if subimagelist is None:
            subimagelist = []
        if subimagenames is None:
            subimagenames = []

        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.subimagelist = subimagelist[:]
        self.subimagenames = subimagenames[:]
        self.image_size = image_size
        self.stats = stats

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        # subimagelist is a list of dictionaries
        # Use the same format and information from sciimlist, save for the image name and image plot

        for subitem in self.subimagelist:
            try:
                imageitem = imagelibrary.ImageItem(
                    imagename=subitem['imagename'] + '.subim', sourcename=subitem['sourcename'],
                    spwlist=subitem['spwlist'], specmode=subitem['specmode'],
                    sourcetype=subitem['sourcetype'],
                    multiterm=subitem['multiterm'],
                    metadata=subitem['metadata'],
                    imageplot=subitem['imageplot'])
                if 'TARGET' in subitem['sourcetype']:
                    context.subimlist.add_item(imageitem)
            except:
                pass

    def __repr__(self):
        return 'MakecutoutimagesResults:'


class MakecutoutimagesInputs(vdp.StandardInputs):

    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    @vdp.VisDependentProperty
    def offsetblc(self):
        return []   # Units of arcseconds

    @vdp.VisDependentProperty
    def offsettrc(self):
        return []   # Units of arcseconds

    def __init__(self, context, vis=None, offsetblc=None, offsettrc=None):
        super(MakecutoutimagesInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.offsetblc = offsetblc
        self.offsettrc = offsettrc


@task_registry.set_equivalent_casa_task('hif_makecutoutimages')
class Makecutoutimages(basetask.StandardTaskTemplate):
    Inputs = MakecutoutimagesInputs
    is_multi_vis_task = True

    def prepare(self):

        imlist = self.inputs.context.sciimlist.get_imlist()
        imagenames = []

        is_vlass_se_cont = is_vlass_se_cube = False
        try:
            if self.inputs.context.imaging_mode.startswith('VLASS-SE-CONT'):
                is_vlass_se_cont = True
            if self.inputs.context.imaging_mode.startswith('VLASS-SE-CUBE'):
                is_vlass_se_cube = True
        except Exception:
            pass

        # PIPE-1048: hif_makecutoutimages should only process final products in the VLASS-SE-CONT mode
        if is_vlass_se_cont:
            imlist = [imlist[-1]]

        # Per VLASS Tech Specs page 22
        for imageitem in imlist:
            if imageitem['multiterm']:
                imagenames.extend(utils.glob_ordered(imageitem['imagename'] + '.tt0'))  # non-pbcor
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace(
                    '.image', '.residual') + '*.tt0'))  # non-pbcor
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor') + '*.tt0'))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor') + '*.tt0.rms'))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.psf') + '*.tt0'))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace(
                    '.image', '.image.residual.pbcor') + '*.tt0'))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.pb') + '*.tt0'))
                # PIPE-631/1039: make alpha/.tt1 image cutouts in the VLASS-SE-CONT mode
                if is_vlass_se_cont:
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'] + '.tt1'))  # non-pbcor
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace(
                        '.image', '.residual') + '*.tt1'))  # non-pbcor
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor') + '*.tt1'))
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor') + '*.tt1.rms'))
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.psf') + '*.tt1'))
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace(
                        '.image', '.image.residual.pbcor') + '*.tt1'))
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.alpha')))
                    imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.alpha.error')))
            else:
                imagenames.extend(utils.glob_ordered(imageitem['imagename']))  # non-pbcor
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.residual')))  # non-pbcor
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor')))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.pbcor.rms')))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.psf')))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.image.residual.pbcor')))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'].replace('.image', '.pb')))

        subimagenames = []
        subimage_size = None
        for imagename in imagenames:
            subimagename = imagename + '.subim'
            if not os.path.exists(subimagename):
                LOG.info(f"Make a cutout image under the image name: {subimagename}")
                _, _ = self._do_subim(imagename)
                subimagenames.append(subimagename)
            else:
                LOG.info(
                    f"A cutout image named {subimagename} already exists, and we will reuse this image for weblog.")
                subimagenames.append(subimagename)
            subimage_size = self._get_image_size(subimagename)

        if is_vlass_se_cube:
            stats = self._do_stats(subimagenames)
        else:
            stats = None

        return MakecutoutimagesResults(subimagelist=imlist, subimagenames=subimagenames, image_size=subimage_size,
                                       stats=stats)

    def analyse(self, results):
        return results

    def _do_imhead(self, imagename):

        task = casa_tasks.imhead(imagename=imagename)

        return self._executor.execute(task)

    def _do_subim(self, imagename):

        inputs = self.inputs

        # Get image header
        imhead_dict = self._do_imhead(imagename)

        # Read in image size from header
        imsizex = math.fabs(imhead_dict['refpix'][0]*imhead_dict['incr'][0]*(180.0/math.pi)*2)  # degrees
        imsizey = math.fabs(imhead_dict['refpix'][1]*imhead_dict['incr'][1]*(180.0/math.pi)*2)  # degrees

        image_size_x = 1.0  # degrees:  size of cutout
        image_size_y = 1.0  # degrees:  size of cutout

        # If less than or equal to 1 deg + 2 arcminute buffer, use the image size and no buffer
        buffer_deg = 2.0 / 60.0   # Units of degrees
        if imsizex <= (1.0 + buffer_deg):
            image_size_x = imsizex
            image_size_y = imsizey
            buffer_deg = 0.0

        imsize = [imhead_dict['shape'][0], imhead_dict['shape'][1]]  # pixels

        xcellsize = 3600.0 * (180.0 / math.pi) * math.fabs(imhead_dict['incr'][0])
        ycellsize = 3600.0 * (180.0 / math.pi) * math.fabs(imhead_dict['incr'][1])

        fld_subim_size_x = utils.round_half_up(
            3600.0 * (image_size_x + buffer_deg) / xcellsize)   # Cutout size with buffer in pixels
        fld_subim_size_y = utils.round_half_up(
            3600.0 * (image_size_y + buffer_deg) / ycellsize)   # Cutout size with buffer in pixels

        # equivalent blc,trc for extracting requested field, in pixels:
        blcx = imsize[0] // 2 - (fld_subim_size_x / 2)
        blcy = imsize[1] // 2 - (fld_subim_size_y / 2)
        trcx = imsize[0] // 2 + (fld_subim_size_x / 2) + 1
        trcy = imsize[1] // 2 + (fld_subim_size_y / 2) + 1

        if blcx < 0.0:
            blcx = 0
        if blcy < 0.0:
            blcy = 0
        if trcx > imsize[0]:
            trcx = imsize[0] - 1
        if trcy > imsize[1]:
            trcy = imsize[1] - 1

        if inputs.offsetblc and inputs.offsettrc:
            offsetblc = inputs.offsetblc
            offsettrc = inputs.offsettrc
            buffer_deg = 0.0

            if isinstance(offsetblc, str):
                offsetblc = ast.literal_eval(offsetblc)
            if isinstance(offsettrc, str):
                offsettrc = ast.literal_eval(offsettrc)

            fld_subim_size_x_blc = utils.round_half_up(3600.0 * (offsetblc[0] / 3600.0 + buffer_deg / 2.0) / xcellsize)
            fld_subim_size_y_blc = utils.round_half_up(3600.0 * (offsetblc[1] / 3600.0 + buffer_deg / 2.0) / ycellsize)
            fld_subim_size_x_trc = utils.round_half_up(3600.0 * (offsettrc[0] / 3600.0 + buffer_deg / 2.0) / xcellsize)
            fld_subim_size_y_trc = utils.round_half_up(3600.0 * (offsettrc[1] / 3600.0 + buffer_deg / 2.0) / ycellsize)

            blcx = imsize[0] // 2 - fld_subim_size_x_blc
            blcy = imsize[1] // 2 - fld_subim_size_y_blc
            trcx = imsize[0] // 2 + fld_subim_size_x_trc + 1
            trcy = imsize[1] // 2 + fld_subim_size_y_trc + 1

            if blcx < 0.0:
                blcx = 0
            if blcy < 0.0:
                blcy = 0
            if trcx > imsize[0]:
                trcx = imsize[0] - 1
            if trcy > imsize[1]:
                trcy = imsize[1] - 1

            LOG.info("Using user defined offsets in arcseconds of: blc:({!s}), trc:({!s})".format(
                ','.join([str(i) for i in offsetblc]), ','.join([str(i) for i in offsettrc])))

        fld_subim = str(blcx) + ',' + str(blcy) + ',' + str(trcx) + ',' + str(trcy)
        LOG.info('Using field subimage blc,trc of {!s},{!s}, {!s},{!s}, which includes a buffer '
                 'of {!s} arcminutes.'.format(blcx, blcy, trcx, trcy, buffer_deg*60))

        # imsubimage(imagename=clnpbcor, outfile=clnpbcor + '.subim', box=fld_subim)

        # Quicklook parameters
        imsubimageparams = {'imagename': imagename,
                            'outfile': imagename + '.subim',
                            'box': fld_subim}

        task = casa_tasks.imsubimage(**imsubimageparams)
        px = (trcx - blcx) + 1
        py = (trcy - blcy) + 1
        subimage_size = {'pixels_x': px,
                         'pixels_y': py,
                         'arcsec_x': px * xcellsize,
                         'arcsec_y': py * ycellsize}

        return self._executor.execute(task), subimage_size

    def _get_image_size(self, imagename):

        with casa_tools.ImageReader(imagename) as image:
            image_summary = image.summary(list=False)

        image_shape = image_summary['shape']
        image_incr = image_summary['incr']

        xcellsize = 3600.0 * (180.0 / math.pi) * math.fabs(image_incr[0])
        ycellsize = 3600.0 * (180.0 / math.pi) * math.fabs(image_incr[1])
        px = image_shape[0]
        py = image_shape[1]
        image_size = {'pixels_x': px,
                      'pixels_y': py,
                      'arcsec_x': px * xcellsize,
                      'arcsec_y': py * ycellsize}

        return image_size

    def _do_stats(self, subimagenames):
        """Extract essential stats from images.
        
        The return stats is a nested dictionary container: stats[spw_key][im_type][stats_type]
        """
        stats = collections.OrderedDict()

        for subimagename in subimagenames:

            with casa_tools.ImageReader(subimagename) as image:

                image_miscinfo = image.miscinfo()
                virtspw = image_miscinfo['virtspw']
                if virtspw not in stats:
                    stats[virtspw] = collections.OrderedDict()

                if '.psf.' in subimagename:
                    pass
                elif '.image.' in subimagename and '.pbcor' not in subimagename:
                    # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                    if '.tt1.' not in subimagename:
                        # PIPE-1401: Because the non-pbcor image from tclean() could miss mask table, with artifically
                        # low-amp pixels at the edge below pblimit (CAS-13818), we use a PB-based mask when run imstats().
                        pbname = subimagename.replace('.image.', '.pb.')
                        item_stats = imstat_items(
                            image, items=['peak', 'madrms', 'max/madrms'], mask=f'mask("{pbname}")')
                        stats[virtspw]['image'] = item_stats
                        # additional non-stats image properties are extracted here.
                        beam = image.restoringbeam(channel=0, polarization=0)
                        stats[virtspw]['beam'] = {'bmaj': beam['major']['value'],
                                                  'bmin': beam['minor']['value'], 'bpa': beam['positionangle']['value']}
                        stats[virtspw]['stokes'] = get_stokes(subimagename)
                        cs = image.coordsys()
                        stats[virtspw]['reffreq'] = cs.referencevalue(format='n')['numeric'][3]

                elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                    # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                    if '.tt1.' not in subimagename:
                        pbname = subimagename.replace('.residual.', '.pb.')
                        item_stats = imstat_items(
                            image, items=['peak', 'madrms', 'max/madrms'], mask=f'mask("{pbname}")')
                        stats[virtspw]['residual'] = item_stats
                elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                    pass
                elif '.rms.' in subimagename:
                    if '.tt1.' not in subimagename:
                        item_stats = imstat_items(image, items=['max', 'median', 'pct<800e-6', 'pct_masked'])
                        stats[virtspw]['rms'] = item_stats
                elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                    pass
                elif '.pb.' in subimagename:
                    if '.tt1.' not in subimagename:
                        stats[virtspw]['pb'] = imstat_items(image, items=['max', 'min', 'median'])
                else:
                    pass

        return stats
