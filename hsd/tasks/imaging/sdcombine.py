"""SDImageCombine classes"""

from typing import TYPE_CHECKING, Dict, List, NewType, Optional

import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.imagelibrary as imagelibrary
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from .resultobjects import SDImagingResultItem

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    Direction = NewType( 'Direction', Dict )

LOG = infrastructure.get_logger(__name__)


class SDImageCombineInputs(vdp.StandardInputs):
    """
    Inputs for image plane combination.
    """

    inimages = vdp.VisDependentProperty(default='')
    outfile = vdp.VisDependentProperty(default='')
    org_directions = vdp.VisDependentProperty(default='')
    specmodes = vdp.VisDependentProperty(default='')

    @inimages.convert
    def inimages(self, value):
        if isinstance(value, str):
            _check_image(value)
        else:
            for v in value:
                _check_image(v)
        return value

    def __init__(self,
                 context:        'Context',
                 inimages:       List[str],
                 outfile:        str,
                 org_directions: List[Optional['Direction']],
                 specmodes:      List[str]):
        """
        Construct SDImageCombineInputs instance.

        Args:
            context        : Pipeline context
            inimages       : Imagenames to combine
            outfile        : Output image name
            org_directions : List of direction of origin for ephemeris objects
            specmodes      : List of specmodes
        """
        super(SDImageCombineInputs, self).__init__()

        self.context = context
        self.inimages = inimages
        self.outfile = outfile
        self.org_directions = org_directions
        self.specmodes = specmodes


class SDImageCombine(basetask.StandardTaskTemplate):
    Inputs = SDImageCombineInputs

    is_multi_vis_task = True

    def prepare(self):
        infiles = self.inputs.inimages
        outfile = self.inputs.outfile
        org_directions = self.inputs.org_directions
        specmodes = self.inputs.specmodes
        inweights = [name+".weight" for name in infiles]
        outweight = outfile + ".weight"
        num_in = len(infiles)
        if num_in == 0:
            LOG.warning("No input image to combine. %s is not generated." % outfile)
            result = SDImagingResultItem(task=None,
                                         success=False, outcome=None)
            return result

        # safe path to the image
        #   - escape colon which has special meaning in LEL
        def get_safe_path(path):
            #p = os.path.relpath(path, self.inputs.context.output_dir)
            LOG.debug('original path = "{}"'.format(path))
            p = path.replace(':', r'\:') if ':' in path else path
            LOG.debug('safe path = "{}"'.format(p))
            return p

        # check uniformity of org_directions and feed org_direction
        threshold = 1E-5   #deg
        me = casa_tools.measures
        qa = casa_tools.quanta
        if len(org_directions) > 1:
            for idx in range(1, len(org_directions)):
                if org_directions[0] is None:
                    if org_directions[idx] is not None:
                        raise RuntimeError( "inconsistent org_directions {}".org_directions )
                else:
                    separation = qa.convert(me.separation( org_directions[idx], org_directions[0] ), 'deg')['value']
                    if separation > threshold:
                        raise RuntimeError( "inconsistent org_directions (separation={} deg) {}".format(separation, org_directions) )
        org_direction = org_directions[0]

        # check uniformity of specmodes
        if len(set(specmodes)) > 1:
            raise RuntimeError( "inconsistent specmodes ({})".format(specmodes) )
        specmode = specmodes[0]

        # combine weight images
        LOG.info("Generating combined weight image.")
        expr = [("IM%d" % idx) for idx in range(num_in)]
        status = self._do_combine(inweights, outweight, str("+").join(expr))
        if status is True:
            # combine images with weight
            LOG.info("Generating combined image.")
            in_images = list(infiles) + list(inweights) + [outweight]
            expr = ["IM%d*IM%d" % (idx, idx+num_in) for idx in range(num_in)]
            expr = "(%s)/IM%d" % (str("+").join(expr), len(in_images)-1)
            status = self._do_combine(in_images, outfile, expr)

        if status is True:
            # Need to replace NaNs in masked pixels
            with casa_tools.ImageReader(outfile) as ia:
                # save default mask name
                default_mask = ia.maskhandler('default')[0]

                # create mask for NaN pixels
                nan_mask = 'nan'
                ia.calcmask('!ISNAN("{}")'.format(get_safe_path(ia.name())), name=nan_mask, asdefault=True)

                stat = ia.statistics()
                shape = ia.shape()
                # replacemaskedpixels fails if all pixels are valid
                if len(stat['npts']) > 0 and shape.prod() > stat['npts'][0]:
                    ia.replacemaskedpixels(0.0, update=False)

                # restore default mask and delete tempral NaN mask
                ia.maskhandler('set', default_mask)
                ia.maskhandler('delete', nan_mask)

            # PIPE-313 re-evaluate image mask based on the combined weight image
            # according to sdimaging code, image pixels will be masked if
            # weight is less than (minweight * median(weight image)) where
            # minweight is task parameter whose default is 0.1. Here, we use
            # dedault value to align with the parameter setting for sdimaging
            # (see worker.py).
            minweight = 0.1
            with casa_tools.ImageReader(outweight) as ia:
                # exclude 0 (and negative weights)
                ia.calcmask('"{}" > 0.0'.format(get_safe_path(ia.name())), name='nonzero')

                stat = ia.statistics(robust=True)
                median_weight = stat['median']

            # re-evaluate mask
            threshold = minweight * median_weight[0]
            for imagename in [outfile, outweight]:
                with casa_tools.ImageReader(imagename) as ia:
                    # new mask name
                    updated_mask = 'mask_combine'

                    # calculate mask from weight image
                    ia.calcmask('"{}" >= {}'.format(get_safe_path(outweight), threshold),
                                name=updated_mask,
                                asdefault=True)

                    # remove non-default masks
                    masks = ia.maskhandler('get')
                    masks.pop(masks.index(updated_mask))
                    if len(masks) > 0:
                        ia.maskhandler('delete', masks)

            image_item = imagelibrary.ImageItem(imagename=outfile,
                                                sourcename='',  # will be filled in later
                                                spwlist=[],  # will be filled in later
                                                specmode=specmode,
                                                sourcetype='TARGET',
                                                org_direction=org_direction)
            outcome = {'image': image_item}
            result = SDImagingResultItem(task=None,
                                         success=True,
                                         outcome=outcome)
        else:
            # Combination failed due to missing valid data
            result = SDImagingResultItem(task=None,
                                         success=False,
                                         outcome=None)

        return result

    def analyse(self, result):
        return result

    def _do_combine(self, infiles, imagename, expr):
        if os.path.exists(imagename):
            shutil.rmtree(imagename)
        combine_args = dict(imagename=infiles, outfile=imagename,
                            mode='evalexpr', expr=expr)
        LOG.debug('Executing immath task: args=%s' % (combine_args))
        combine_job = casa_tasks.immath(**combine_args)

        # execute job
        self._executor.execute(combine_job)

        return True


def _check_image(imagename):
    assert os.path.exists(imagename), 'Input image "{0}" does not exist.'.format(imagename)
    assert os.path.exists(imagename.rstrip('/') + '.weight'), 'Weight image for "{0}" does not exist.'.format(imagename)
