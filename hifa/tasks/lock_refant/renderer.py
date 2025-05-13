"""
Weblog renderer for hifa_lock_refant
"""
import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

__all__ = [
    'T2_4MDetailsLockRefantRenderer'
]

LOG = logging.get_logger(__name__)
RefantTR = collections.namedtuple('RefantTR', 'vis refant')


class T2_4MDetailsLockRefantRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='lockrefant.mako', description='Lock refant list', always_rerender=False):
        super(T2_4MDetailsLockRefantRenderer, self).__init__(uri=uri, description=description,
                                                             always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, result):
        refant_table_rows = make_refant_table(context, result)
        ctx.update({
            'refant_table': refant_table_rows,
        })


def make_refant_table(context, results):
    rows = []

    for result in results:
        ms = context.observing_run.get_ms(result.inputs['vis'])
        vis_cell = ms.basename
        # insert spaces in refant list to allow browser to break string if it wants
        refant_cell = ms.reference_antenna.replace(',', ', ')
        tr = RefantTR(vis_cell, refant_cell)
        rows.append(tr)

    return utils.merge_td_columns(rows)
