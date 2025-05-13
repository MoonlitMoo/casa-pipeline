"""
Created on 25 Mar 2020

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


class T2_4MDetailsPolcalflagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the PolcalFlag task.
    """
    def __init__(self, uri='polcalflag.mako',
                 description='Polcal outlier flagging',
                 always_rerender=False):
        super(T2_4MDetailsPolcalflagRenderer, self).__init__(uri=uri, description=description,
                                                             always_rerender=always_rerender)

        # Attach correctedampflag renderer.
        self.cafrenderer = T2_4MDetailsCorrectedampflagRenderer(uri=uri, description=description,
                                                                always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):

        # Initialize items that are to be exported to the
        # mako context
        updated_refants = {}

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

        #
        # Check for updated reference antenna lists.
        #
        for result in results:
            vis = result.vis
            # If the reference antenna list was updated, retrieve new refant
            # list.
            if result.refants_to_remove or result.refants_to_demote:
                ms = pipeline_context.observing_run.get_ms(name=vis)
                updated_refants[vis] = ms.reference_antenna

        # Update the mako context.
        mako_context.update({
            'time_plots': time_plots,
            'uvdist_plots': uvdist_plots,
            'updated_refants': updated_refants
        })
        
        # PIPE-615: store callibrary tables in the weblog directory
        copy_callibrary(results, pipeline_context.report_dir)
