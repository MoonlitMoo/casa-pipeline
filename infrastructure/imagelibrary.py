import os

from . import logging
from . import filenamer as fn

LOG = logging.get_logger(__name__)


# This class contains a list ImageItem objects encoded as dictionaries.
class ImageLibrary(object):
    def __init__(self):
        self._images = []

    # Return the image list.
    def get_imlist(self):
        return self._images

    # Clear the image list
    def clear_imlist(self):
        del self._images[:]

    # Add image item to the list as a dictionary
    def add_item(self, imageitem, overwrite=True):
        if overwrite:
            if self.find_imageitem(imageitem) < 0:
                self._images.append(dict(imageitem))
            else:
                LOG.warning('Image item %s already in list' % imageitem.imagename)
        else:
            version_count = self.product_in_list(imageitem)
            if version_count <= 1:
                self._images.append(dict(imageitem))
            else:
                fitsname = fn.fitsname('', imageitem.imagename)
                LOG.info('Image product item {} already in image list'.format(fitsname))
                imageitem.version = version_count
                LOG.info('Adding new image version to list v{}: {}'.format(imageitem.version, imageitem.imagename))

                (aa, bb) = os.path.splitext(fitsname)
                fitsfile = ''.join((aa, '.v', str(imageitem.version), bb))
                LOG.info('{} would be exported as {}'.format(imageitem.imagename, fitsfile))

                self._images.append(dict(imageitem))

    # Remove image item from the list
    def delete_item(self, imageitem):
        index = self.find_imageitem(imageitem)
        if index >= 0:
            del self._images[index]

    # Return the index of the item in the list or -1.
    def find_imageitem(self, imageitem):
        for i in range(len(self._images)):
            if imageitem.imagename == self._images[i]['imagename']:
                return i
        return -1

    # check for existing entry using fits product name.
    # this is for the case of adding new products from the same spw. PIPE-345
    def product_in_list(self, imageitem):
        for idx, img in enumerate(self._images):
            if (fn.fitsname('', imageitem.imagename) == fn.fitsname('', img.get('imagename', '')) and
                   1 == img.get('version')):
                self._images[idx]['version_count'] += 1
                return self._images[idx]['version_count']
        else:
            return 0


# This class contains information for image data product
class ImageItem:
    def __init__(self, imagename, sourcename, spwlist, specmode, sourcetype, multiterm=None, imageplot='',
                 metadata={}, imaging_params={},
                 org_direction=None):
        self.imagename = imagename
        self.sourcename = sourcename
        self.spwlist = spwlist
        self.specmode = specmode
        self.sourcetype = sourcetype
        self.multiterm = multiterm
        self.imageplot = imageplot
        self.metadata = metadata
        self.imaging_params = imaging_params  # params used to create image. iterations dict w/imaging_params struct
        self.org_direction = org_direction
        self.version = 1
        self.version_count = 1

    def __iter__(self):
        return iter(vars(self).items())


# This class defines the image meta data that will be stored for
# each final cleaned image.
class ImageMetadata (dict):
    # This is a test.
    _keys = ['IMCENTER', 'IMRES']

    def __init__(self):
        dict.__init__(self)
        for key in ImageMetadata._keys:
            self[key] = None

    def __setitem__(self, key, val):
        if key not in ImageMetadata._keys:
            raise KeyError
        dict.__setitem__(self, key, val)
