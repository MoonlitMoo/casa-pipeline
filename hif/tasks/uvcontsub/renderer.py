import collections
import os

import pipeline.infrastructure
#import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

#LOG = logging.get_logger(__name__)

UVcontSubParams = collections.namedtuple('UVcontSubParams', 'ms freqrange fitorder source_intent scispw')

class T2_4MDetailsUVcontSubRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='uvcontsub.mako',
                 description='UV continuum fit and subtraction',
                 always_rerender=False):
        super(T2_4MDetailsUVcontSubRenderer, self).__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        rows = []

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            rows.extend(self.get_table_rows(context, result, ms))

        table_rows = utils.merge_td_columns(rows)
        ctx.update({
            'table_rows': table_rows
        })

    def get_table_rows(self, context, result, ms):
        table_rows = []

        for field_intent_spw in result.field_intent_spw_list:
            source_intent = f'{field_intent_spw["field"]} {field_intent_spw["intent"]}'
            spw = f'{field_intent_spw["spw"]}'
            frange = result.topo_freq_fitorder_dict[field_intent_spw['field']][field_intent_spw['spw']]['freq'].replace(';', ', ')
            fitorder = result.topo_freq_fitorder_dict[field_intent_spw['field']][field_intent_spw['spw']]['fitorder']
            row = UVcontSubParams(ms.basename,  frange, fitorder, source_intent, spw)
            table_rows.append(row)

        return table_rows
