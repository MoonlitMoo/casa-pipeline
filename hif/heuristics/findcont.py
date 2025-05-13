import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.extern.findContinuum import findContinuum
from pipeline.extern.findContinuum import countChannelsInRanges
from pipeline.extern.findContinuum import numberOfChannelsInCube
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class FindContHeuristics(object):
    def __init__(self, context):
        self.context = context

    def find_continuum(self, dirty_cube, pb_cube=None, psf_cube=None, single_continuum=False, is_eph_obj=False,
                       ref_ms_name='', nbin=1, dynrange_bw=None):
        with casa_tools.ImageReader(dirty_cube) as image:
            stats = image.statistics()

        if stats['min'][0] == stats['max'][0]:
            LOG.error('Cube %s is constant at level %s.' % (dirty_cube, stats['max'][0]))
            return ['NONE'], 'none'

        # Run continuum finder on cube
        channel_selection, png_name, aggregate_bw, all_continuum, warning_strings, joint_mask_name = \
            findContinuum(img=dirty_cube,
                          pbcube=pb_cube,
                          psfcube=psf_cube,
                          singleContinuum=single_continuum,
                          returnAllContinuumBoolean=True,
                          returnWarnings=True,
                          vis=ref_ms_name,
                          nbin=nbin,
                          spectralDynamicRangeBandWidth=dynrange_bw)

        # PIPE-74
        channel_counts = countChannelsInRanges(channel_selection)
        if 1 == len(channel_counts):
            single_range_channel_fraction = channel_counts[0]/float(numberOfChannelsInCube(dirty_cube))
        else:
            single_range_channel_fraction = 999.

        flags = []
        if channel_selection == '':
            frequency_ranges_GHz = ['NONE']
        else:
            if all_continuum:
                flags.append('ALLCONT')

            if is_eph_obj:
                refer = 'SOURCE'
            else:
                refer = 'LSRK'

            frequency_ranges_GHz = [{'range': item, 'refer': refer} for item in utils.chan_selection_to_frequencies(dirty_cube, channel_selection, 'GHz')]

        if warning_strings[0]:
            flags.append('LOWBANDWIDTH')
        if warning_strings[1]:
            flags.append('LOWSPREAD')

        cont_ranges_and_flags = {'ranges': frequency_ranges_GHz, 'flags': flags}

        return cont_ranges_and_flags, png_name, single_range_channel_fraction, warning_strings, os.path.basename(joint_mask_name)
