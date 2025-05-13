import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
import pipeline.infrastructure.mpihelpers as mpihelpers

LOG = infrastructure.get_logger(__name__)


class MakermsimagesResults(basetask.Results):
    def __init__(self, rmsimagelist=None, rmsimagenames=None):
        super().__init__()

        if rmsimagelist is None:
            rmsimagelist = []
        if rmsimagenames is None:
            rmsimagenames = []

        self.rmsimagelist = rmsimagelist[:]
        self.rmsimagenames = rmsimagenames[:]

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        # rmsimagelist is a list of dictionaries
        # Use the same format and information from sciimlist, save for the image name and image plot
        for rmsitem in self.rmsimagelist:
            try:
                imageitem = imagelibrary.ImageItem(
                    imagename=rmsitem['imagename'] + '.rms', sourcename=rmsitem['sourcename'],
                    spwlist=rmsitem['spwlist'], specmode=rmsitem['specmode'],
                    sourcetype=rmsitem['sourcetype'],
                    multiterm=rmsitem['multiterm'],
                    metadata=rmsitem['metadata'],
                    imageplot=rmsitem['imageplot'])
                if 'TARGET' in rmsitem['sourcetype']:
                    context.rmsimlist.add_item(imageitem)
            except:
                pass

    def __repr__(self):
        return 'MakermsimagesResults:'


class MakermsimagesInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    def __init__(self, context, vis=None):
        super().__init__()
        # set the properties to the values given as input arguments
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hif_makermsimages')
class Makermsimages(basetask.StandardTaskTemplate):
    Inputs = MakermsimagesInputs
    is_multi_vis_task = True

    def prepare(self):

        imlist = self.inputs.context.sciimlist.get_imlist()

        imagenames = []
        for imageitem in imlist:
            if imageitem['multiterm']:
                imagenames.extend(utils.glob_ordered(imageitem['imagename'] + '.pbcor.tt0'))
                imagenames.extend(utils.glob_ordered(imageitem['imagename'] + '.pbcor.tt1'))
            else:
                imagenames.extend(utils.glob_ordered(imageitem['imagename'] + '.pbcor'))

        tier0_imdev_enabled = True
        rmsimagenames = []
        queued_job_rmsimagename = []

        for imagename in imagenames:
            rmsimagename = imagename + '.rms'
            if not os.path.exists(rmsimagename) and 'residual' not in imagename:
                LOG.info(f"Generating RMS image {rmsimagename} from {imagename}")
                job_to_execute = casa_tasks.imdev(**self._get_imdev_args(imagename))
                if tier0_imdev_enabled and mpihelpers.is_mpi_ready():
                    executable = mpihelpers.Tier0JobRequest(
                        casa_tasks.imdev, job_to_execute.kw, executor=self._executor)
                    queued_job = mpihelpers.AsyncTask(executable)
                else:
                    queued_job = mpihelpers.SyncTask(job_to_execute, self._executor)
                queued_job_rmsimagename.append((queued_job, rmsimagename))

        for queue_job, rmsimagename in queued_job_rmsimagename:
            queue_job.get_result()
            if os.path.exists(rmsimagename):
                rmsimagenames.append(rmsimagename)

        LOG.info("RMS image list: " + ','.join(rmsimagenames))

        return MakermsimagesResults(rmsimagelist=imlist, rmsimagenames=rmsimagenames)

    def analyse(self, results):
        return results

    def _get_imdev_args(self, imagename):
        """Get default CASA/imdev parameters."""
        imdevparams = {'imagename': imagename,
                       'outfile': imagename + '.rms',
                       'region': "",
                       'box': "",
                       'chans': "",
                       'stokes': "",
                       'mask': "",
                       'overwrite': True,
                       'stretch': False,
                       'grid': [10, 10],
                       'anchor': "ref",
                       'xlength': "60arcsec",
                       'ylength': "60arcsec",
                       'interp': "cubic",
                       'stattype': "xmadm",
                       'statalg': "chauvenet",
                       'zscore': -1,
                       'maxiter': -1
                       }

        return imdevparams

    def _do_imdev(self, imagename):

        # Quicklook parameters
        imdevparams = {'imagename': imagename,
                       'outfile': imagename + '.rms',
                       'region': "",
                       'box': "",
                       'chans': "",
                       'stokes': "",
                       'mask': "",
                       'overwrite': True,
                       'stretch': False,
                       'grid': [10, 10],
                       'anchor': "ref",
                       'xlength': "60arcsec",
                       'ylength': "60arcsec",
                       'interp': "cubic",
                       'stattype': "xmadm",
                       'statalg': "chauvenet",
                       'zscore': -1,
                       'maxiter': -1
                       }

        task = casa_tasks.imdev(**imdevparams)

        return self._executor.execute(task)
