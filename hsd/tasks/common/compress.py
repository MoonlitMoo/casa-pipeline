import bz2
import pickle
import time

from pipeline.infrastructure.utils import get_obj_size
import pipeline.infrastructure.logging as logging

LOG = logging.get_logger(__name__)


# object compression/decopmpression utility
class CompressedObj(object):
    def __init__(self, obj, protocol=pickle.HIGHEST_PROTOCOL, compresslevel=9):
        self.compressed = compress_object(obj, protocol=protocol, compresslevel=compresslevel)

    def decompress(self):
        return decompress_object(self.compressed)


def compress_object(obj, protocol=pickle.HIGHEST_PROTOCOL, compresslevel=9):
    size_org = get_obj_size(obj)
    start = time.time()
    try:
        compressed = bz2.compress(pickle.dumps(obj, protocol), compresslevel=compresslevel)
    except:
        compressed = obj
    end = time.time()
    size_comp = get_obj_size(compressed)
    LOG.debug('compress: size before {0} after {1} ({2} %)'.format(size_org, size_comp, float(size_comp)/float(size_org) * 100))
    LOG.debug('elapsed {0} sec'.format(end - start))
    return compressed


def decompress_object(obj):
    size_comp = get_obj_size(obj)
    start = time.time()
    decompressed = pickle.loads(bz2.decompress(obj))
    end = time.time()
    size_org = get_obj_size(decompressed)
    LOG.debug('decompress: size before {0} after {1} ({2} %)'.format(size_org, size_comp, float(size_comp)/float(size_org) * 100))
    LOG.debug('elapsed {0} sec'.format(end - start))
    return decompressed


class CompressedIter(object):
    def __init__(self, obj):
        self.obj = obj
        self._count = 0

    def __next__(self):
        if self._count < len(self.obj):
            v = self.obj[self._count]
            self._count += 1
            if isinstance(v, CompressedObj):
                return v.decompress()
            else:
                return v
        else:
            raise StopIteration()


class CompressedList(list):
    def __iter__(self):
        return CompressedIter(self)
