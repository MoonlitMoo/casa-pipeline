"""Renderer module for skycal task."""
import collections
import os

from typing import TYPE_CHECKING, Any, Dict, List
if TYPE_CHECKING:
    from pipeline.domain.field import Field
    from pipeline.infrastructure.launcher import Context
    from pipeline.domain import MeasurementSet
    from pipeline.infrastructure.basetask import ResultsList
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.infrastructure import casa_tools
from . import skycal as skycal_task
from . import display as skycal_display

LOG = logging.get_logger(__name__)


class T2_4MDetailsSingleDishSkyCalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """Weblog renderer class for skycal task."""

    def __init__(self,
                 uri: str = 'skycal.mako',
                 description: str = 'Single-Dish Sky Calibration',
                 always_rerender: bool = False) -> None:
        """Initialize T2_4MDetailsSingleDishSkyCalRenderer instance.

        Args:
            uri: Name of Mako template file. Defaults to 'skycal.mako'.
            description: Description of the task. This is embedded into the task detail page.
                         Defaults to 'Single-Dish Sky Calibration'.
            always_rerender: Always rerender the page if True. Defaults to False.
        """
        super(T2_4MDetailsSingleDishSkyCalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self,
                            ctx: Dict[str, Any],
                            context: 'Context',
                            results: 'ResultsList') -> None:
        """Update context for weblog rendering.

        Args:
            ctx: Context for weblog rendering.
            context: Pipeline context.
            results: ResultsList instance. Should hold a list of SDSkyCalResults instance.
        """
        stage_dir = os.path.join(context.report_dir,
                                 'stage%d' % results.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        # threshold for elevation difference between ON and OFF
        threshold = skycal_task.SerialSDSkyCal.ElevationDifferenceThreshold

        applications = []
        summary_amp_vs_freq = collections.defaultdict(list)
        details_amp_vs_freq = collections.defaultdict(list)
        summary_amp_vs_time = collections.defaultdict(list)
        details_amp_vs_time = collections.defaultdict(list)
        summary_interval_vs_time = collections.defaultdict(list)
        details_interval_vs_time = collections.defaultdict(list)
        summary_elev_diff = collections.defaultdict(list)
        details_elev_diff = collections.defaultdict(list)
        amp_vs_freq_subpages = {}
        amp_vs_time_subpages = {}
        interval_vs_time_subpages = {}
        elev_diff_subpages = {}
        reference_coords = collections.defaultdict(dict)
        for result in results:
            if not result.final:
                continue

            # get ms domain object
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            # calibration table summary
            ms_applications = self.get_skycal_applications(context, result, ms)
            applications.extend(ms_applications)

            # iterate over CalApplication instances
            final_original = result.final

            summaries_freq = []
            details_freq = []
            summaries_time = []
            details_time = []
            summaries_elev = []
            details_elev = []
            summaries_interval = []
            details_interval = []
            for calapp in final_original:
                result.final = [calapp]
                gainfield = calapp.calfrom[0].gainfield

                # Amp vs. Freq: summary plots
                summary_plotter = skycal_display.SingleDishSkyCalAmpVsFreqSummaryChart(context, result, gainfield)
                summaries_freq.extend(summary_plotter.plot())

                # Amp vs. Freq: detail plots
                detail_plotter = skycal_display.SingleDishSkyCalAmpVsFreqDetailChart(context, result, gainfield)
                details_freq.extend(detail_plotter.plot())

                # Amp vs. Time: summary plots
                summary_plotter = skycal_display.SingleDishSkyCalAmpVsTimeSummaryChart(context, result, calapp)
                summaries_time.extend(summary_plotter.plot())

                # Amp vs. Time: detail plots
                detail_plotter = skycal_display.SingleDishSkyCalAmpVsTimeDetailChart(context, result, calapp)
                details_time.extend(detail_plotter.plot())

                # Interval vs. Time: summary plots
                summary_plotter = skycal_display.SingleDishSkyCalIntervalVsTimeDisplay(context, result, calapp, figtype='summary')
                summaries_interval.extend(summary_plotter.plot())

                # Interval vs. Time: detail plots
                detail_plotter = skycal_display.SingleDishSkyCalIntervalVsTimeDisplay(context, result, calapp, figtype='detail')
                details_interval.extend(detail_plotter.plot())

                # reference coordinates
                #LOG.debug('calapp=%s'%(calapp))
                calmode = utils.get_origin_input_arg(calapp, 'calmode')
                LOG.debug('calmode=\'%s\''%(calmode))
                field_domain = ms.get_fields(gainfield)[0]
                if calmode == 'ps':
                    reference_coord = self._get_reference_coord(context, ms, field_domain)
                    reference_coords[vis][field_domain.name] = reference_coord

            result.final = final_original

            # Compute elevation difference
            eldiff = skycal_task.compute_elevation_difference(context, result)

            # Elevation difference
            elplots = skycal_display.plot_elevation_difference(context, result, eldiff,
                                                               threshold=threshold)
            summaries_elev = [p for p in elplots if p.parameters['ant'] == '']
            details_elev = [p for p in elplots if p.parameters['ant'] != '']

            summary_amp_vs_freq[vis].extend(summaries_freq)
            details_amp_vs_freq[vis].extend(details_freq)
            summary_amp_vs_time[vis].extend(summaries_time)
            details_amp_vs_time[vis].extend(details_time)
            summary_elev_diff[vis].extend(summaries_elev)
            details_elev_diff[vis].extend(details_elev)
            summary_interval_vs_time[vis].extend(summaries_interval)
            details_interval_vs_time[vis].extend(details_interval)

        # sort plots
        for vis in summary_amp_vs_freq:
            ms = context.observing_run.get_ms(vis)
            name_id_map = dict((f.clean_name, f.id) for f in ms.fields)

            def sort_by_field_spw(plot):
                field_name = plot.parameters['field']
                spw_id = plot.parameters['spw']
                field_id = name_id_map[field_name]
                return field_id, spw_id

            for summary_dict in (summary_amp_vs_freq, summary_amp_vs_time,):
                if vis not in summary_dict:
                    continue

                _plot_list = summary_dict[vis]

                if len(_plot_list) == 0:
                    continue

                LOG.debug('sorting plot list for %s xaxis %s yaxis %s' %
                         (vis, _plot_list[0].x_axis, _plot_list[0].y_axis))
                LOG.debug('before: %s' % [(p.parameters['field'], p.parameters['spw']) for p in _plot_list])
                _plot_list.sort(key=sort_by_field_spw)
                LOG.debug(' after: %s' % [(p.parameters['field'], p.parameters['spw']) for p in _plot_list])

        # Sky Level vs Frequency
        flattened = [plot for inner in details_amp_vs_freq.values() for plot in inner]
        renderer = basetemplates.JsonPlotRenderer(uri='hsd_generic_x_vs_y_ant_field_spw_plots.mako',
                                                  context=context,
                                                  result=result,
                                                  plots=flattened,
                                                  title ='Sky Level vs Frequency',
                                                  outfile='sky_level_vs_frequency.html')
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
        for vis in details_amp_vs_freq:
            amp_vs_freq_subpages[vis] = os.path.basename(renderer.path)

        # Sky Level vs Time
        flattened = [plot for inner in details_amp_vs_time.values() for plot in inner]
        renderer = basetemplates.JsonPlotRenderer(uri='hsd_generic_x_vs_y_ant_field_spw_plots.mako',
                                                  context=context,
                                                  result=result,
                                                  plots=flattened,
                                                  title ='Sky Level vs Time',
                                                  outfile='sky_level_vs_time.html')
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
        for vis in details_amp_vs_time:
            amp_vs_time_subpages[vis] = os.path.basename(renderer.path)

        # Interval Ratio (Off-source/On-source) vs Time
        flattened = [plot for inner in details_interval_vs_time.values() for plot in inner]
        renderer = basetemplates.JsonPlotRenderer(uri='hsd_generic_x_vs_y_ant_field_spw_plots.mako',
                                                  context=context,
                                                  result=result,
                                                  plots=flattened,
                                                  title ='Interval Ratio (Off-source/On-source) vs Time',
                                                  outfile='interval_ratio_vs_time.html')
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
        for vis in details_interval_vs_time:
            interval_vs_time_subpages[vis] = os.path.basename(renderer.path)

        # Elevation difference
        flattened = [plot for inner in details_elev_diff.values() for plot in inner]
        renderer = basetemplates.JsonPlotRenderer(uri='generic_x_vs_y_ant_field_plots.mako',
                                                  context=context,
                                                  result=result,
                                                  plots=flattened,
                                                  title='Elevation Difference vs. Time',
                                                  outfile='elevation_difference_vs_time.html')
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
        for vis in details_elev_diff:
            elev_diff_subpages[vis] = os.path.basename(renderer.path)

        LOG.debug('reference_coords=%s' % reference_coords)

        # update Mako context
        ctx.update({'applications': applications,
                    'summary_amp_vs_freq': summary_amp_vs_freq,
                    'amp_vs_freq_subpages': amp_vs_freq_subpages,
                    'summary_amp_vs_time': summary_amp_vs_time,
                    'amp_vs_time_subpages': amp_vs_time_subpages,
                    'summary_interval_vs_time': summary_interval_vs_time,
                    'interval_vs_time_subpages': interval_vs_time_subpages,
                    'summary_elev_diff': summary_elev_diff,
                    'elev_diff_subpages': elev_diff_subpages,
                    'reference_coords': reference_coords})

    def get_skycal_applications(self, context: 'Context', result: skycal_task.SDSkyCalResults, ms: 'MeasurementSet') -> List[Dict]:
        """Get application information from SDSkyCalResults instance and set them into a list.

        Args:
            context: Pipeline context.
            result: SDSkyCalResults instance.
            ms: MeasurementSet domain object.

        Returns:
            A list "application" containing dictionary; they are used to a table in skycal.mako file.
        """
        applications = []

        calmode_map = {'ps': 'Position-switch',
                       'otfraster': 'OTF raster edge'}

        calapps = result.outcome
        for calapp in calapps:
            caltype = calmode_map[utils.get_origin_input_arg(calapp, 'calmode')]
            gaintable = os.path.basename(calapp.gaintable)
            spw = calapp.spw.replace(',', ', ')
            intent = calapp.intent.replace(',', ', ')
            antenna = calapp.antenna
            if antenna == '':
                antenna = ', '.join([a.name for a in ms.antennas])
            field = ms.get_fields(calapp.field)[0].name

            applications.append({'ms': ms.basename,
                                 'gaintable': gaintable,
                                 'spw': spw,
                                 'intent': intent,
                                 'field': field,
                                 'antenna': antenna,
                                 'caltype': caltype})

        return applications

    def _get_reference_coord(self, context: 'Context', ms: 'MeasurementSet', field: 'Field') -> str:
        """Get celestial coordinates and the reference.

        Args:
            context: Pipeline context.
            ms: MeasurementSet domain object.
            field: field domain object.

        Returns:
            Reference, RA and Declination.

        Raises:
            RuntimeError: No OFF source data exist for given field in the ms.
        """
        LOG.debug('_get_reference_coord({ms}, {field})'.format(ms=ms.basename, field=field.name))
        spws = ms.get_spectral_windows(science_windows_only=True)
        data_desc_ids = [ms.get_data_description(spw=spw.id).id for spw in spws]
        reference_states = [state for state in ms.states if 'REFERENCE' in state.intents]
        state_ids = [state.id for state in reference_states]
        field_id = field.id
        with casa_tools.TableReader(ms.name) as tb:
            t = tb.query(f'ANTENNA1==ANTENNA2 && FIELD_ID=={field_id} && DATA_DESC_ID IN {data_desc_ids} && STATE_ID IN {state_ids}')
            if t.nrows() == 0:
                t.close()
                raise RuntimeError(f'No OFF source data for field "{field.name}" (id {field_id}) in {ms.basename}')

            rownumbers = t.rownumbers()
            antenna_id = t.getcell('ANTENNA1', 0)
            timestamp = t.getcell('TIME', 0)
            t.close()
            timeref = tb.getcolkeyword('TIME', 'MEASINFO')['Ref']
            timeunit = tb.getcolkeyword('TIME', 'QuantumUnits')[0]
        with casa_tools.MSMDReader(ms.name) as msmd:
            pointing_direction = msmd.pointingdirection(rownumbers[0])
            antenna_position = msmd.antennaposition(antenna_id)
        qa = casa_tools.quanta
        me = casa_tools.measures
        epoch = me.epoch(rf=timeref, v0=qa.quantity(timestamp, timeunit))

        LOG.debug('pointing_direction=%s' % pointing_direction)

        direction = pointing_direction['antenna1']['pointingdirection']
        me.doframe(antenna_position)
        me.doframe(epoch)
        # 2018/04/18 TN
        # CAS-10874 single dish pipeline should use a direction reference
        # taken from input MS
        origin_basename = os.path.basename(ms.origin_ms)
        datatable_name = os.path.join(context.observing_run.ms_datatable_name, origin_basename)
        datatable = DataTable()
        datatable.importdata(datatable_name, minimal=False, readonly=True)
        outref = datatable.direction_ref

        converted = me.measure(direction, rf=outref)
        LOG.debug('converted direction=%s' % converted)
        if outref.upper() == 'GALACTIC':
            xformat = 'dms'
        else:
            xformat = 'hms'
        coord = '{ref} {ra} {dec}'.format(ref=outref,
                                          ra=qa.formxxx(converted['m0'], format=xformat),
                                          dec=qa.formxxx(converted['m1'], format='dms'))

        return coord
