import collections
import copy
import os
from typing import List, Dict

import pipeline.infrastructure.utils as utils
from pipeline import Context
from pipeline.h.tasks.tsysflag.renderer import T2_4MDetailsTsysflagRenderer
from pipeline.infrastructure import filenamer
from pipeline.infrastructure.basetask import ResultsList
from pipeline.infrastructure.renderer import basetemplates
from pipeline.infrastructure.renderer.logger import Plot


class T2_4MDetailsTsysflagContaminationRenderer(T2_4MDetailsTsysflagRenderer):
    def __init__(
        self,
        uri="tsysflagcontamination.mako",
        description="Flag Tsys astrophysical line contamination",
        always_rerender=False,
    ):
        super().__init__(
            uri=uri, description=description, always_rerender=always_rerender
        )

    def update_mako_context(
        self, mako_context: dict, pipeline_context: Context, results: ResultsList
    ):
        super().update_mako_context(mako_context, pipeline_context, results)

        subpages = {}
        plot_pages = {}
        for result in results:
            vis = os.path.basename(result.inputs["vis"])

            ms = pipeline_context.observing_run.get_ms(name=vis)
            spw_map = self._get_tsys_map(pipeline_context, ms)

            plots = self.post_process_plots(
                pipeline_context, spw_map, getattr(result, "plots", [])
            )
            if plots:
                plot_pages[vis] = plots

            renderer = TsysContaminationPlotRenderer(pipeline_context, result, plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                # the filename is sanitised - the MS name is not. We need to
                # map MS to sanitised filename for link construction.
                subpages[vis] = renderer.path

        mako_context["contamination_plots"] = plot_pages
        mako_context["contamination_subpages"] = subpages

    def _get_tsys_map(self, context, ms):
        # Get the Tsys spw map by retrieving it from the first tsys CalFrom
        # that is present in the callibrary
        try:
            spwmap = utils.get_calfroms(context, ms.name, "tsys")[0].spwmap
        except IndexError:
            # proceed with 1:1 mapping
            spwmap = list(range(len(ms.spectral_windows)))

        # we're only concerned about science spws
        science_spws = ms.get_spectral_windows(science_windows_only=True)
        science_spw_ids = [spw.id for spw in science_spws]

        # contruct a dict mapping Tsys spw to science spws
        tsys_map = {}
        for tsys_spw in spwmap:
            science_spws = {
                science_id
                for science_id, mapped_tsys in enumerate(spwmap)
                if tsys_spw == mapped_tsys and science_id in science_spw_ids
            }
            tsys_map[tsys_spw] = sorted(science_spws)

        return tsys_map

    def post_process_plots(self, pcontext: Context, spw_map: Dict[int, List[int]], plots: List[Plot]) -> List[Plot]:
        """
        Transform the 'foreign' plot wrappers coming from extern code into
        native wrappers more aligned with the pipeline and standard pipeline
        presentation.

        We want to make as few modifications as possible to the extern
        contribution, keeping it free of pipeline context, domain objects,
        etc. until it can be properly adopted and refactored. This function
        modifies the one concession we did make for the pipeline, having the
        extern code return plot wrappers, to return plot wrappers as we'd
        expect from pipeline-native code.
        """
        updated = copy.deepcopy(plots)

        for plot in updated:
            ms = pcontext.observing_run.get_ms(plot.parameters["vis"])
            # TsysDataClassFile recognises the intents 'bandpass', 'phasecal' and 'science'

            # extern code sets:
            #   - field ID to an int, can be lookup up in MS
            orig_field = plot.parameters["field"]
            field_list = ms.get_fields(field_id=orig_field)
            plot.parameters["field"] = (
                field_list[0].name if field_list else str(orig_field)
            )

            # extern code sets:
            #   - spw, from which we can identify Tsys spw and science spw
            tsys_spw = plot.parameters["tsys_spw"]
            plot.parameters["spw"] = spw_map[tsys_spw]

            # extern code sets:
            #   - intent to a string, can be mapped to a pipeline intent
            extern_to_pipeline_intent_map = {
                "bandpass": "BANDPASS",
                "phasecal": "PHASE",
                "science": "TARGET",
            }
            orig_intent = plot.parameters["intent"]
            plot.parameters["intent"] = extern_to_pipeline_intent_map.get(
                orig_intent, orig_intent
            )

        return updated


class TsysContaminationPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)
        title = "T<sub>sys</sub> contamination plots for %s" % vis
        outfile = filenamer.sanitize("tsysflagcontamination-%s.html" % vis)

        # need to wrap result in a list to give common implementation for the
        # following code that extracts spwmap and gaintable
        if not isinstance(result, collections.abc.Iterable):
            result = [result]

        super(TsysContaminationPlotRenderer, self).__init__(
            "tsysflagcontamination_plots.mako", context, result, plots, title, outfile
        )

    def update_json_dict(self, d, plot):
        # Tsys spw and intent are not automatically added by the base
        # implementation
        d.update(
            {
                "tsys_spw": str(plot.parameters["tsys_spw"]),
                "intent": plot.parameters["intent"],
            }
        )
