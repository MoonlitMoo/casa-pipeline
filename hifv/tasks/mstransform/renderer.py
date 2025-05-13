import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class T2_4MDetailsVlaMstransformRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlamstransform.mako',
                 description='Create science target MS',
                 always_rerender=False):
        super().__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        table_rows = make_mstransform_table(pipeline_context, results)
        mako_context.update({'table_rows': table_rows})


MstransformTR = collections.namedtuple('MstransformTR', 'vis outputvis outputvis_line')


def make_mstransform_table(context, results):

    # Will hold all the input and output MS(s)
    rows = []

    # Loop over the results
    for single_result in results:
        vis_cell = os.path.basename(single_result.inputs['vis'])
        outputvis_cell = os.path.basename(single_result.inputs['outputvis'])
        outputvis_line_name = os.path.basename(single_result.inputs['outputvis_for_line'])
        if os.path.exists(outputvis_line_name):
            outputvis_line_cell = outputvis_line_name
        else:
            outputvis_line_cell = "N/A"
        tr = MstransformTR(vis_cell, outputvis_cell, outputvis_line_cell)
        rows.append(tr)

    return utils.merge_td_columns(rows)
