import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class T2_4DetailsHanningRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='hanning.mako',
                 description='Hanning Smoothing',
                 always_rerender=False):
        super().__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        table_rows = make_hanning_table(pipeline_context, results)
        mako_context.update({'table_rows': table_rows})


HanningTR = collections.namedtuple('HanningTR', 'vis spw_id spw_name center_freq spw_bw chan_size smoothed reason')


def make_hanning_table(context, results):

    # Will hold all the input and output MS(s)
    rows = []

    # Loop over the results
    for single_result in results:
        vis = os.path.basename(single_result.inputs['vis'])
        ms = context.observing_run.get_ms(name=vis)
        for spw in ms.get_spectral_windows(science_windows_only=True):
            spw_name = context.observing_run.virtual_science_spw_shortnames.get(
                context.observing_run.virtual_science_spw_ids.get(
                    context.observing_run.real2virtual_spw_id(
                        int(spw.id), ms), 'N/A'), 'N/A')
            smoothed, reason = single_result.smoothed_spws[spw.id]
            if smoothed:
                smoothed_str = "Yes"
            else:
                smoothed_str = "No"
            tr = HanningTR(vis, spw.id, spw_name, spw.centre_frequency,
                           spw.bandwidth, spw.channels[0].getWidth(),
                           smoothed_str, reason)
            rows.append(tr)

    return utils.merge_td_columns(rows)
