import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class PbcorResults(basetask.Results):
    def __init__(self, pbcorimagenames={}, multitermlist=[]):
        super(PbcorResults, self).__init__()
        self.pbcorimagenames = pbcorimagenames
        self.multitermlist = multitermlist

    def __repr__(self):
        # return 'PbcorResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'PbcorResults:'


class PbcorInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    def __init__(self, context, vis=None):
        super(PbcorInputs, self).__init__()
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_pbcor')
class Pbcor(basetask.StandardTaskTemplate):
    Inputs = PbcorInputs

    is_multi_vis_task = True

    def prepare(self):

        sci_imlist = self.inputs.context.sciimlist.get_imlist()
        pbcor_dict = {}

        # by default, only .tt0 is processed
        multiterm_ext_list = ['.tt0']

        # PIPE-1048/1074 (for the VLASS-SE-CONT mode):
        #   hifv_pbcor will only pbcorrect final products, including both tt0 and tt1 images.
        try:
            if self.inputs.context.imaging_mode.startswith('VLASS-SE-CONT'):
                sci_imlist = [sci_imlist[-1]]
                multiterm_ext_list = ['.tt0', '.tt1']
        except Exception:
            pass

        term_ext_list = ['']
        for sci_im in sci_imlist:

            imgname = sci_im['imagename']
            basename = imgname[:imgname.rfind('.image')]
            keep = sci_im['metadata'].get('keep', True)
            pbname = basename + '.pb'

            # PIPE-2205: do not run impbcor on VLA cube images
            if '.cube.' in basename:
                continue
            
            pbcor_images = []
            term_ext_list = multiterm_ext_list if sci_im['multiterm'] else ['']

            for term_ext in term_ext_list:

                pb_term_ext = '' if term_ext == '' else '.tt0'
                task = casa_tasks.impbcor(imagename=basename+'.image'+term_ext, pbimage=pbname+pb_term_ext,
                                          outfile=basename+'.image.pbcor'+term_ext, mode='divide', cutoff=-1.0, stretch=False)
                self._executor.execute(task)
                pbcor_images.append(basename+'.image.pbcor'+term_ext)

                task = casa_tasks.impbcor(imagename=basename + '.residual'+term_ext, pbimage=pbname+pb_term_ext,
                                          outfile=basename + '.image.residual.pbcor'+term_ext, mode='divide', cutoff=-1.0, stretch=False)
                self._executor.execute(task)
                pbcor_images.append(basename + '.image.residual.pbcor'+term_ext)

            pbcor_images.append(pbname+pb_term_ext)

            LOG.info("PBCOR image names: " + ','.join(pbcor_images))
            pbcor_dict[(basename, keep)] = pbcor_images

        return PbcorResults(pbcorimagenames=pbcor_dict, multitermlist=term_ext_list)

    def analyse(self, results):
        return results
