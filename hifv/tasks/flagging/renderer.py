import collections
import os
import shutil
from typing import Optional, List

import pipeline.h.tasks.common.flagging_renderer_utils as flagutils
import pipeline.h.tasks.common.displays.flagging as flagging
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from . import displaycheckflag

LOG = logging.get_logger(__name__)

FlagTotal = collections.namedtuple('FlagSummary', 'flagged total')


class T2_4MDetailsFlagDeterVLARenderer(basetemplates.T2_4MDetailsDefaultRenderer):

    def __init__(self, uri='flagdetervla.mako',
                 description='VLA Deterministic flagging', always_rerender=False):

        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, result):
        ctx = super().get_display_context(context, result)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % result.stage_number)

        non_science_agents = ['before', 'anos', 'shadow', 'intents']
        # return all agents so we get ticks and crosses against each one
        agents = non_science_agents + ['online', 'template', 'autocorr', 'edgespw', 'clip', 'quack', 'baseband']
        # Note that the call to common.flagging_renderer_utils.flags_for_result
        flag_totals = {}
        for r in result:
            flag_totals = utils.dict_merge(flag_totals,
                flagutils.flags_for_result(r, context, non_science_agents=non_science_agents))

            # copy template files across to weblog directory
            toggle_to_filenames = {'online'   : 'fileonline',
                                   'template' : 'filetemplate'}
            inputs = r.inputs
            for toggle, filenames in toggle_to_filenames.items():
                src = inputs[filenames]
                if inputs[toggle] and os.path.exists(src):
                    LOG.trace('Copying %s to %s' % (src, weblog_dir))
                    shutil.copy(src, weblog_dir)

        flagcmd_files = {}
        for r in result:
            # write final flagcmds to a file
            ms = context.observing_run.get_ms(r.inputs['vis'])
            flagcmds_filename = '%s-agent_flagcmds.txt' % ms.basename
            flagcmds_path = os.path.join(weblog_dir, flagcmds_filename)
            with open(flagcmds_path, 'w') as flagcmds_file:
                terminated = '\n'.join(r.flagcmds())
                flagcmds_file.write(terminated)

            flagcmd_files[ms.basename] = flagcmds_path

        flagplots = {os.path.basename(r.inputs['vis']): self.flagplot(r, context)
                     for r in result}

        ctx.update({
            'flags': flag_totals,
            'agents': agents,
            'dirname': weblog_dir,
            'flagcmds': flagcmd_files,
            'flagplots': flagplots})

        return ctx

    @staticmethod
    def flagplot(result, context):
        plotter = flagging.PlotAntsChart(context, result)
        return plotter.plot()


# not used in 4.5.2+ and C3R4+
class T2_4MDetailstargetflagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlatargetflag.mako',
                 description='Targetflag (All targets through RFLAG)', always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        summary_plots = {}

        '''
        for result in results:
            
            plotter = targetflagdisplay.targetflagSummaryChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots
        '''

        ctx.update({'summary_plots': summary_plots,
                    'dirname': weblog_dir})

        return ctx


class T2_4MDetailscheckflagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='checkflag.mako', description='Checkflag summary',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        flag_totals = {}
        for r in results:
            if r.summaries:
                flag_totals = utils.dict_merge(flag_totals,
                                               self.flags_for_result(r, context))

        summary_plots = {}
        percentagemap_plots = {}
        dataselect = {}

        for result in results:

            ms = os.path.basename(result.inputs['vis'])
            if 'plotms_dataselect' in result.vis_averaged:
                plotms_dataselect = result.vis_averaged['plotms_dataselect']
            else:
                plotms_dataselect = {}

            plots = []
            if 'before' in result.vis_averaged:
                plotter = displaycheckflag.checkflagSummaryChart(context, result,
                                                                 suffix='before',
                                                                 plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())
            if 'after' in result.vis_averaged:
                plotter = displaycheckflag.checkflagSummaryChart(context, result,
                                                                 suffix='after',
                                                                 plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())
                plotter = displaycheckflag.checkflagSummaryChart(context, result,
                                                                 suffix='after-autoscale', plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())

            summary_plots[ms] = plots
            if result.inputs['checkflagmode'] == 'vlass-imaging':
                percentagemap_plots[ms] = [displaycheckflag.checkflagPercentageMap(context, result).plot()]
            else:
                percentagemap_plots[ms] = []
            percentagemap_plots[ms] = [p for p in percentagemap_plots[ms] if p is not None]

            dataselect[ms] = result.dataselect

        ctx.update({'flags': flag_totals,
                    'agents': ['before', 'after'],
                    'summary_plots': summary_plots,
                    'dataselect': dataselect,
                    'percentagemap_plots': percentagemap_plots,
                    'dirname': weblog_dir})

        return ctx

    def flags_for_result(self, result, context):
        ms = context.observing_run.get_ms(result.inputs['vis'])
        summaries = result.summaries

        by_antenna = self.flags_by_antenna(summaries)
        by_spw = self.flags_by_spw(summaries)
        by_field = self.flags_by_field(summaries)

        return {ms.basename: {'by_antenna': by_antenna, 'by_spw': by_spw, 'by_field': by_field}}

    @staticmethod
    def flags_by_antenna(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for ant_id in summary['antenna']:
                total[summary['name']][ant_id] = flagutils.FlagTotal(
                    summary['antenna'][ant_id]['flagged'], summary['antenna'][ant_id]['total'])
        return total

    @staticmethod
    def flags_by_spw(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for spw_id in summary['spw']:
                total[summary['name']][spw_id] = flagutils.FlagTotal(
                    summary['spw'][spw_id]['flagged'], summary['spw'][spw_id]['total'])
        return total

    @staticmethod
    def flags_by_field(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for field_id in summary['field']:
                total[summary['name']][field_id] = flagutils.FlagTotal(
                    summary['field'][field_id]['flagged'], summary['field'][field_id]['total'])
        return total

class T2_4MDetailsaoflaggerRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='aoflagger.mako', description='Aoflagger summary',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        flag_totals = {}
        for r in results:
            if r.summaries:
                flag_totals = utils.dict_merge(flag_totals,
                                               self.flags_for_result(r, context))

        summary_plots = {}
        percentagemap_plots = {}
        dataselect = {}

        for result in results:

            ms = os.path.basename(result.inputs['vis'])
            if 'plotms_dataselect' in result.vis_averaged:
                plotms_dataselect = result.vis_averaged['plotms_dataselect']
            else:
                plotms_dataselect = {}

            plots = []
            if 'before' in result.vis_averaged:
                plotter = displaycheckflag.aoflaggerSummaryChart(context, result,
                                                                 suffix='before',
                                                                 plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())
            if 'after' in result.vis_averaged:
                plotter = displaycheckflag.aoflaggerSummaryChart(context, result,
                                                                 suffix='after',
                                                                 plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())
                plotter = displaycheckflag.aoflaggerSummaryChart(context, result,
                                                                 suffix='after-autoscale', plotms_args=plotms_dataselect)
                plots.extend(plotter.plot())

            summary_plots[ms] = plots
            percentagemap_plots[ms] = [p for p in percentagemap_plots[ms] if p is not None]

            dataselect[ms] = result.dataselect

        ctx.update({'flags': flag_totals,
                    'agents': ['before', 'after'],
                    'summary_plots': summary_plots,
                    'dataselect': dataselect,
                    'percentagemap_plots': percentagemap_plots,
                    'dirname': weblog_dir})

        return ctx

    def flags_for_result(self, result, context):
        ms = context.observing_run.get_ms(result.inputs['vis'])
        summaries = result.summaries

        by_antenna = self.flags_by_antenna(summaries)
        by_spw = self.flags_by_spw(summaries)
        by_field = self.flags_by_field(summaries)

        return {ms.basename: {'by_antenna': by_antenna, 'by_spw': by_spw, 'by_field': by_field}}

    @staticmethod
    def flags_by_antenna(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for ant_id in summary['antenna']:
                total[summary['name']][ant_id] = flagutils.FlagTotal(
                    summary['antenna'][ant_id]['flagged'], summary['antenna'][ant_id]['total'])
        return total

    @staticmethod
    def flags_by_spw(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for spw_id in summary['spw']:
                total[summary['name']][spw_id] = flagutils.FlagTotal(
                    summary['spw'][spw_id]['flagged'], summary['spw'][spw_id]['total'])
        return total

    @staticmethod
    def flags_by_field(summaries):
        total = collections.defaultdict(dict)
        for summary in summaries:
            for field_id in summary['field']:
                total[summary['name']][field_id] = flagutils.FlagTotal(
                    summary['field'][field_id]['flagged'], summary['field'][field_id]['total'])
        return total

class T2_4MDetailsFlagtargetsdataRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='flagtargetsdata.mako',
                 description='Flagtargetsdata flagging', always_rerender=False):

        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        results = results[0]

        flag_totals = {}
        flagcmd_files = {}
        for i, ms in enumerate(results.mses):
            summary = results.summaries[i]
            if summary:
                flag_totals = utils.dict_merge(flag_totals,
                                               self.flags_for_result(ms, summary))
            flagcmds_filename = '%s-agent_flagcmds.txt' % ms.name
            flagcmds_path = os.path.join(weblog_dir, flagcmds_filename)
            flag_commands = results.flagcmds()[i]
            with open(flagcmds_path, 'w') as flagcmds_file:
                terminated = '\n'.join(flag_commands)
                flagcmds_file.write(terminated)

            flagcmd_files[ms.name] = flagcmds_path

        ctx.update({'flags': flag_totals,
                    'agents': ['before', 'template'],
                    'dirname': weblog_dir,
                    'flagcmds': flagcmd_files})

        return ctx

    def update_mako_context(self, mako_context, pipeline_context, results):
        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % results.stage_number)
        results = results[0]

        flag_totals = {}
        flagcmd_files = {}
        for i, ms in enumerate(results.mses):
            summary = results.summaries[i]
            if summary:
                flag_totals = utils.dict_merge(flag_totals,
                                               self.flags_for_result(ms, summary))

            # copy template files across to weblog directory
            toggle_to_filenames = {'template': 'filetemplate'}

            inputs = results.inputs
            for toggle, filenames in toggle_to_filenames.items():
                src = inputs[filenames]
                if inputs[toggle] and os.path.exists(src):
                    LOG.trace('Copying %s to %s' % (src, weblog_dir))
                    shutil.copy(src, weblog_dir)

            # write final flagcmds to a file
            flagcmds_filename = '%s-agent_flagcmds.txt' % ms.name
            flagcmds_path = os.path.join(weblog_dir, flagcmds_filename)
            flag_commands = results.flagcmds()[i]
            with open(flagcmds_path, 'w') as flagcmds_file:
                terminated = '\n'.join(flag_commands)
                flagcmds_file.write(terminated)

            flagcmd_files[ms.name] = flagcmds_path

        # return all agents so we get ticks and crosses against each one
        agents = ['before', 'template']

        mako_context.update({
            'flags': flag_totals,
            'agents': agents,
            'dirname': weblog_dir,
            'flagcmds': flagcmd_files})

    def flags_for_result(self,
                         ms,
                         summary,
                         intents_to_summarise: Optional[List[str]] = None,
                         non_science_agents: Optional[List[str]] = None):
        if intents_to_summarise is None:
            intents_to_summarise = ['BANDPASS', 'PHASE', 'AMPLITUDE', 'TARGET']

        if non_science_agents is None:
            non_science_agents = []

        by_intent = flagutils.flags_by_intent(ms, summary, intents_to_summarise)
        by_spw = flagutils.flags_by_science_spws(ms, summary)
        merged = utils.dict_merge(by_intent, by_spw)

        adjusted = flagutils.adjust_non_science_totals(merged, non_science_agents)

        return {ms.name: adjusted}
