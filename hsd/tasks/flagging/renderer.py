import os
import shutil

import pipeline.infrastructure.logging as logging
import pipeline.h.tasks.flagging.renderer as super_renderer

LOG = logging.get_logger(__name__)


class T2_4MDetailsFlagDeterAlmaSdRenderer(super_renderer.T2_4MDetailsFlagDeterBaseRenderer):
    def __init__(self, uri='flagdeterbase.mako',
                 description='Deterministic flagging', always_rerender=False):

        super(T2_4MDetailsFlagDeterAlmaSdRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        super().update_mako_context(mako_context, pipeline_context, result)

        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % result.stage_number)

        # copy flagpointing.txt
        for r in result:
            src = r.inputs['filepointing']
            if os.path.exists(src):
                LOG.trace('Copying %s to %s' % (src, weblog_dir))
                shutil.copy(src, weblog_dir)

            try:
                idx = list(map(lambda x: x['name'], r.summaries)).index('pointing')
                assert idx > 0
                previous_summary = r.summaries[idx - 1]
                pointing_summary = r.summaries[idx]
                flagged = pointing_summary['flagged'] - previous_summary['flagged']
                total = pointing_summary['total']
                LOG.info('{}: Flagged fraction for pointing flags {:.4f}%'.format(
                    os.path.basename(r.inputs['vis']),
                    flagged / total * 100
                ))
            except ValueError:
                pass


        # insert pointing agent
        agent = mako_context['agents']
        pos = agent.index('online')
        agent.insert(pos + 1, 'pointing')

        # update mako_context
        mako_context.update({'agents': agent})
