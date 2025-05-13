"""
Created on 18 Mar 2020

@author: Dirk Muders (MPIfR)
"""
import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.h.tasks.applycal.renderer import copy_callibrary
from pipeline.hif.tasks.correctedampflag.renderer import T2_4MDetailsCorrectedampflagRenderer
from pipeline.hifa.tasks.gfluxscaleflag.renderer import get_plot_dicts
from pipeline.infrastructure import basetask

LOG = logging.get_logger(__name__)


class T2_4MDetailsTargetflagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the TargetFlag task.
    """
    def __init__(self, uri='targetflag.mako',
                 description='Target outlier flagging',
                 always_rerender=False):
        super(T2_4MDetailsTargetflagRenderer, self).__init__(uri=uri, description=description,
                                                             always_rerender=always_rerender)

        # Attach correctedampflag renderer.
        self.cafrenderer = T2_4MDetailsCorrectedampflagRenderer(uri=uri, description=description,
                                                                always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):

        #
        # Get flagging reports, summaries
        #
        cafresults = basetask.ResultsList()
        for result in results:
            if result.cafresult:
                cafresults.append(result.cafresult)
        cafresults.stage_number = results.stage_number
        self.cafrenderer.update_mako_context(mako_context, pipeline_context, cafresults)

        #
        # Get diagnostic plots.
        #
        time_plots = get_plot_dicts(pipeline_context, results, 'time')
        uvdist_plots = get_plot_dicts(pipeline_context, results, 'uvdist')

        # Update the mako context.
        mako_context.update({
            'time_plots': time_plots,
            'uvdist_plots': uvdist_plots
        })
        
        # PIPE-615: store callibrary tables in the weblog directory
        copy_callibrary(results, pipeline_context.report_dir)
