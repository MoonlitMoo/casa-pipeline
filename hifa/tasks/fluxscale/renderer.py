"""
Created on 23 Oct 2014

@author: sjw
"""
import collections
import decimal
import itertools
import math
import operator
import os

import numpy as np
import matplotlib.pyplot as plt

import pipeline.domain.measures as measures
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.domain.measures import FluxDensityUnits, FrequencyUnits
from pipeline.h.tasks.common import atmutil
from pipeline.h.tasks.importdata.fluxes import ORIGIN_XML, ORIGIN_ANALYSIS_UTILS
from pipeline.infrastructure.renderer import logger
from . import display as gfluxscale
from ..importdata.dbfluxes import ORIGIN_DB

LOG = logging.get_logger(__name__)


CATALOGUE_SOURCES = (ORIGIN_ANALYSIS_UTILS, ORIGIN_DB, ORIGIN_XML)


class T2_4MDetailsGFluxscaleRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='gfluxscale.mako', 
                 description='Transfer fluxscale from amplitude calibrator',
                 always_rerender=False):
        super(T2_4MDetailsGFluxscaleRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # All antenna, sort by baseband
        ampuv_allant_plots = collections.defaultdict(dict)
        for intents in ['AMPLITUDE']:
            plots = self.create_plots(pipeline_context, results, gfluxscale.GFluxscaleSummaryChart, intents)
            self.sort_plots_by_baseband(plots)
            for vis, vis_plots in plots.items():
                if len(vis_plots) > 0:
                    ampuv_allant_plots[vis][intents] = vis_plots

        # List of antenna for the fluxscale result, sorted by baseband
        ampuv_ant_plots = collections.defaultdict(dict)
        for intents in ['AMPLITUDE']:
            plots = self.create_plots_ants(pipeline_context, results, gfluxscale.GFluxscaleSummaryChart, intents)
            self.sort_plots_by_baseband(plots)
            for vis, vis_plots in plots.items():
                if len(vis_plots) > 0:
                    ampuv_ant_plots[vis][intents] = vis_plots

        flux_comparison_plots = self.create_flux_comparison_plots(pipeline_context, results)

        # Generate rows for table of computed flux densities.
        table_rows = make_flux_table(pipeline_context, results)

        # Generate rows for table of adopted flux densities.
        adopted_rows = make_adopted_table(results)

        # Generate flagging commands summary table (PIPE-2155).
        flagtable = make_flag_table(pipeline_context, results)

        mako_context.update({
            'adopted_table': adopted_rows,
            'ampuv_allant_plots': ampuv_allant_plots,
            'ampuv_ant_plots': ampuv_ant_plots,
            'flagtable': flagtable,
            'flux_plots': flux_comparison_plots,
            'table_rows': table_rows
        })

    @staticmethod
    def sort_plots_by_baseband(d):
        for vis, plots in d.items():
            plots = sorted(plots, key=lambda plot: plot.parameters['baseband'])
            d[vis] = plots

    @staticmethod
    def create_flux_comparison_plots(context, results):
        output_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        d = {}

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            d[vis] = create_flux_comparison_plots(context, output_dir, result)

        return d

    def create_plots(self, context, results, plotter_cls, intents, renderer_cls=None):
        """
        Create plots and return a dictionary of vis:[Plots].  No antenna or UVrange selection.
        """
        d = {}
        for result in results:
            plots = self.plots_for_result(context, result, plotter_cls, intents, renderer_cls)
            d = utils.dict_merge(d, plots)

        return d

    def create_plots_ants(self, context, results, plotter_cls, intents, renderer_cls=None):
        """
        Create plots and return a dictionary of vis:[Plots].
        Antenna and UVrange selection determined by heuristics.
        """
        d = {}
        for result in results:
            # PIPE-33: when all antennas are selected on the ampcal, suppress the second set of amp(uvdist;model) plot
            if result.resantenna == '':
                continue

            plots = self.plots_for_result(context, result, plotter_cls, intents, renderer_cls, ant=result.resantenna,
                                          uvrange=result.uvrange)
            d = utils.dict_merge(d, plots)
        return d

    @staticmethod
    def plots_for_result(context, result, plotter_cls, intents, renderer_cls=None, ant='', uvrange=''):
        vis = os.path.basename(result.inputs['vis'])

        output_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)

        # create a fake CalTo object so we can use the applycal class
        fields = result.inputs['reference']
        calto = callibrary.CalTo(result.inputs['vis'], field=fields)

        plotter = plotter_cls(context, output_dir, calto, intents, ant=ant, uvrange=uvrange)
        plots = plotter.plot()

        d = collections.defaultdict(dict)
        d[vis] = plots

        if renderer_cls is not None:
            renderer = renderer_cls(context, result, plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())        

        return d


FluxTR = collections.namedtuple('FluxTR', 'vis field spw freqbw i q u v fluxratio spix')


def make_flag_table(context, results):
    weblog_dir = os.path.join(context.report_dir, f"stage{results.stage_number}")
    flagtable = {}
    for result in results:
        for table_name, flagcmds in result.ampcal_flagcmds.items():
            table_basename = os.path.basename(table_name)
            flagcmd_abspath = write_flagcmds_to_disk(weblog_dir, table_basename, flagcmds)
            flagcmd_relpath = os.path.relpath(flagcmd_abspath, context.report_dir)
            flagtable[table_basename] = flagcmd_relpath

    return flagtable


def write_flagcmds_to_disk(weblog_dir, basename, flagcmds):
    filename = os.path.join(weblog_dir, f"{basename}-flag_commands.txt")
    with open(filename, 'w') as flagfile:
        flagfile.writelines([f"# Flag commands for {basename}\n#\n"])
        flagfile.writelines([f"{flagcmd}\n" for flagcmd in flagcmds])
        if not flagcmds:
            flagfile.writelines(["# No flag commands generated\n"])

    return filename


def make_flux_table(context, results):
    # will hold all the flux stat table rows for the results
    rows = []

    for single_result in results:
        ms_for_result = context.observing_run.get_ms(single_result.vis)
        vis_cell = os.path.basename(single_result.vis)

        transintent = set(single_result.inputs['transintent'].split(','))
        
        # measurements will be empty if calibrated visibility flux derivation failed
        if len(single_result.measurements) == 0:
            continue

        for field_arg in sorted(single_result.measurements, key=lambda f: ms_for_result.get_fields(f)[0].id):
            field = ms_for_result.get_fields(field_arg)[0]

            intents = " ". join(sorted(field.intents.intersection(transintent)))
            field_cell = '%s (#%s) %s' % (field.name, field.id, intents)

            for measurement in sorted(single_result.measurements[field_arg], key=operator.attrgetter('spw_id')):
                spw = ms_for_result.get_spectral_window(measurement.spw_id)
                freqbw = '%s %s' % (str(spw.centre_frequency), str(spw.bandwidth))
                fluxes = collections.defaultdict(lambda: 'N/A')

                for stokes in ['I', 'Q', 'U', 'V']:
                    try:
                        flux = getattr(measurement, stokes)
                        unc = getattr(measurement.uncertainty, stokes)
                        flux_jy = flux.to_units(measures.FluxDensityUnits.JANSKY)
                        if stokes == 'I':
                            flux_jy_I = flux_jy
                        unc_jy = unc.to_units(measures.FluxDensityUnits.JANSKY)
                        sp_str, sp_scale = utils.get_si_prefix(flux_jy, lztol=0)
                        if flux_jy != 0 and unc_jy != 0:
                            unc_ratio = decimal.Decimal('100')*(unc_jy/flux_jy)
                            if unc_ratio >= 0.1:
                                unc_ratio_str = '{:.1f}'.format(unc_ratio)
                            else:
                                unc_ratio_str = np.format_float_positional(
                                    unc_ratio, precision=1, fractional=False, trim='-')
                            unc_value = float(unc_jy)/sp_scale
                            if unc_value >= 0.001:
                                unc_value_str = '{:.3f}'.format(unc_value)
                            else:
                                unc_value_str = np.format_float_positional(
                                    unc_value, precision=1, fractional=False, trim='-')
                            fluxes[stokes] = '{:.3f} &#177 {} {} ({}%)'.format(
                                float(flux_jy)/sp_scale, unc_value_str, sp_str+'Jy', unc_ratio_str)
                        else:
                            fluxes[stokes] = '{:.3f} {}'.format(float(flux_jy)/sp_scale, sp_str+'Jy')
                    except:
                        pass

                try:
                    fluxes['spix'] = '%s' % getattr(measurement, 'spix')
                except:
                    fluxes['spix'] = '0.0'

                # Get the corresponding catalog flux
                catfluxes = collections.defaultdict(lambda: 'N/A')
                flux_ratio = 'N/A'

                cat_measurements = [o for o in field.flux_densities if o.origin in CATALOGUE_SOURCES]
                for catmeasurement in cat_measurements:
                    if catmeasurement.spw_id != int(measurement.spw_id):
                        continue
                    for stokes in ['I', 'Q', 'U', 'V']:
                        try:                        
                            catflux = getattr(catmeasurement, stokes)
                            catflux_jy = catflux.to_units(measures.FluxDensityUnits.JANSKY)
                            if stokes == 'I':
                                catflux_jy_I = catflux_jy
                            catfluxes[stokes] = ' %s' % (catflux)
                        except:
                            pass
                    try:
                        catfluxes['spix'] = '%s' % getattr(catmeasurement, 'spix')
                    except:
                        catfluxes['spix'] = '0.0'
                    if fluxes['I'] != 'N/A' and catfluxes['I'] != 'N/A':
                        flux_ratio = '%0.3f' % (float(flux_jy_I) / float(catflux_jy_I))
                    break

                # Get the corresponding fluxscale derived fluxes.
                fsfluxes = collections.defaultdict(lambda: 'N/A')
                fs_measurements = single_result.fluxscale_measurements

                if str(field.id) in fs_measurements:
                    for fs_measurement in fs_measurements[str(field.id)]:
                        if fs_measurement.spw_id != int(measurement.spw_id):
                            continue

                        for stokes in ['I', 'Q', 'U', 'V']:
                            try:
                                fsflux = getattr(fs_measurement, stokes)
                                fsunc = getattr(fs_measurement.uncertainty, stokes)
                                fsflux_jy = fsflux.to_units(measures.FluxDensityUnits.JANSKY)
                                fsunc_jy = fsunc.to_units(measures.FluxDensityUnits.JANSKY)
                                sp_str, sp_scale = utils.get_si_prefix(fsflux_jy, lztol=0)
                                if fsflux_jy != 0 and fsunc_jy != 0:
                                    fsunc_ratio = decimal.Decimal('100') * (fsunc_jy / fsflux_jy)
                                    if fsunc_ratio >= 0.1:
                                        fsunc_ratio_str = '{:.1f}'.format(fsunc_ratio)
                                    else:
                                        fsunc_ratio_str = np.format_float_positional(
                                            fsunc_ratio, precision=1, fractional=False, trim='-')
                                    fsunc_value = float(fsunc_jy)/sp_scale
                                    if fsunc_value >= 0.001:
                                        fsunc_value_str = '{:.3f}'.format(fsunc_value)
                                    else:
                                        fsunc_value_str = np.format_float_positional(
                                            fsunc_value, precision=1, fractional=False, trim='-')
                                    fsfluxes[stokes] = '{:.3f} &#177 {} {} ({}%)'.format(
                                        float(fsflux_jy)/sp_scale, fsunc_value_str, sp_str+'Jy', fsunc_ratio_str)
                                else:
                                    fsfluxes[stokes] = '{:.3f} {}'.format(float(fsflux_jy)/sp_scale, sp_str+'Jy')

                            except:
                                pass
                        try:
                            fsfluxes['spix'] = '%s' % getattr(fs_measurement, 'spix')
                        except:
                            fsfluxes['spix'] = '0.0'
                        break

                # Create the table row for current result (vis), field, and spw.
                tr = FluxTR(vis_cell, field_cell, measurement.spw_id, freqbw, 
                            fsfluxes['I'],
                            fsfluxes['Q'],
                            fsfluxes['U'],
                            fsfluxes['V'],
                            flux_ratio,
                            fluxes['spix'])
                rows.append(tr)

                tr = FluxTR(vis_cell, field_cell, measurement.spw_id, freqbw,
                            fluxes['I'],
                            fluxes['Q'],
                            fluxes['U'],
                            fluxes['V'],
                            flux_ratio,
                            fluxes['spix'])
                rows.append(tr)

                tr = FluxTR(vis_cell, field_cell, measurement.spw_id, freqbw,
                            catfluxes['I'],
                            catfluxes['Q'],
                            catfluxes['U'],
                            catfluxes['V'],
                            flux_ratio,
                            fluxes['spix'])
                rows.append(tr)

    return utils.merge_td_columns(rows)


AdoptedTR = collections.namedtuple('AdoptedTR', 'vis fields')


def make_adopted_table(results):
    # will hold all the flux stat table rows for the results
    rows = []

    for adopted_result in [r for r in results if r.applies_adopted]:
        vis_cell = os.path.basename(adopted_result.vis)

        field_cell = ', '.join(str(x) for x in adopted_result.measurements)

        tr = AdoptedTR(vis_cell, field_cell)
        rows.append(tr)

    return utils.merge_td_columns(rows)


def create_flux_comparison_plots(context, output_dir, result, showatm=True):
    ms = context.observing_run.get_ms(result.vis)

    plots = []

    for field_id, measurements in result.measurements.items():
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)

        fields = ms.get_fields(task_arg=field_id)
        assert len(fields) == 1
        field = fields[0]

        ax.set_xlabel('Frequency (GHz)')
        ax.set_ylabel('Flux Density (Jy)')

        # Avoid offset values (PIPE-644)
        ax.yaxis.set_major_formatter(plt.ScalarFormatter(useOffset=False))

        # PIPE-1550: cycle through different symbols and colours simultaneously
        # (diagonally across the 7x10 matrix of unique combinations, rather than row-by-row);
        # note that this assumes that the numbers of symbols and colours are coprime
        symbols_and_colours = zip(itertools.cycle('osDv^<>'),
                                  itertools.cycle(plt.get_cmap('tab10').colors))  # standard Tableau colormap

        x_min = 1e99
        x_max = 0
        # Cycle through flux measurements, ordered by SpW ID, to plot the
        # calibrated fluxes.
        # PIPE-2083: keep a record of which SpW IDs are getting plotted here,
        # to restrict which SpWs it needs to overplot catalogue fluxes for.
        spws_plotted = []
        for m in sorted(measurements, key=operator.attrgetter('spw_id')):
            # cycle colours so that windows centred on the same frequency are distinguishable
            symbol, colour = next(symbols_and_colours)

            spws_plotted.append(m.spw_id)
            spw = ms.get_spectral_window(m.spw_id)
            x = spw.centre_frequency.to_units(FrequencyUnits.GIGAHERTZ)
            x_unc = decimal.Decimal('0.5') * spw.bandwidth.to_units(FrequencyUnits.GIGAHERTZ)

            # Plot calibrated fluxes. PIPE-566: if both flux and uncertainty
            # are zero, then do not plot the value to avoid affecting the
            # automatic y-range.
            y = m.I.to_units(FluxDensityUnits.JANSKY)
            y_unc = m.uncertainty.I.to_units(FluxDensityUnits.JANSKY)
            if not (y == 0 and y_unc == 0):
                label = 'spw {}'.format(spw.id)
                ax.errorbar(
                    x, y, xerr=x_unc, yerr=y_unc,
                    marker=symbol, color=colour, ls="-", label=label)

            x_min = min(x_min, x - x_unc)
            x_max = max(x_max, x + x_unc)

        # Plot fluxes from ASDM, catalog, and/or analysisUtils.
        catalogue_fluxes = {
            ORIGIN_XML: 'ASDM',
            ORIGIN_DB: 'online catalogue',
            ORIGIN_ANALYSIS_UTILS: 'analysisUtils'
        }

        ages = []
        for origin, label in catalogue_fluxes.items():
            # PIPE-2083: select which catalog fluxes to plot, based on origin
            # and for which SpWs the flux measurements were plotted earlier.
            fluxes = [f for f in field.flux_densities if f.origin == origin and f.spw_id in spws_plotted]
            if not fluxes:
                continue

            ages.extend([f.age for f in fluxes])
            spws = [ms.get_spectral_window(f.spw_id) for f in fluxes]
            x = [spw.centre_frequency.to_units(FrequencyUnits.GIGAHERTZ) for spw in spws]
            y = [f.I.to_units(FluxDensityUnits.JANSKY) for f in fluxes]
            spix = [float(f.spix) for f in fluxes]
            # sort by frequency
            x, y, spix = list(zip(*sorted(zip(x, y, spix))))
            # PIPE-644: always plot catalog fluxes in black.
            colour = "k"
            ax.plot(x, y, marker='o', color=colour, label='Data source:\n{}'.format(label))

            s_xmin = scale_flux(x[0], y[0], x_min, spix[0])
            s_xmax = scale_flux(x[-1], y[-1], x_max, spix[-1])
            ax.plot([x[0], x_min], [y[0], s_xmin], color=colour, label='Spectral index\nextrapolation',
                    linestyle='dotted')
            ax.plot([x[-1], x_max], [y[-1], s_xmax], color=colour, label='_nolegend_', linestyle='dotted')

        # Check if catalog fluxes share a single age that is not None, and take
        # this to represent to catalog flux age; otherwise set age to N/A.
        uniq_ages = set([age for age in ages if age is not None])
        if len(uniq_ages) == 1:
            age = uniq_ages.pop()
        else:
            age = 'N/A'

        # Add plot title.
        # PIPE-644: include age of catalog fluxes in title.
        title_str = "Flux calibration: {} (age = {} days)".format(field.name, age)
        ax.set_title(title_str)

        # Plot atmospheric transmission.
        if showatm:
            atm_color = 'm'

            # Create 2nd axis for atmospheric transmission.
            axes_atm = ax.twinx()
            axes_atm.set_ylabel('ATM Transmission', color=atm_color, labelpad=2)
            axes_atm.set_ylim(0, 1.05)
            axes_atm.tick_params(direction='out', colors=atm_color)
            axes_atm.yaxis.set_major_formatter(plt.FuncFormatter(lambda t, pos: '{}%'.format(int(t * 100))))
            axes_atm.yaxis.tick_right()

            # Select antenna to use for determining atmospheric transmission:
            # Preferably use highest ranked reference antenna, otherwise
            # use antenna ID = 0.
            ant_id = 0
            if hasattr(ms, 'reference_antenna') and isinstance(ms.reference_antenna, str):
                ant_id = ms.get_antenna(search_term=ms.reference_antenna.split(',')[0])[0].id

            # For each spw in the flux measurements, compute and plot the
            # atmospheric transmission vs. frequency.
            spw_ids = sorted([m.spw_id for m in measurements])
            for spw_id in spw_ids:
                atm_freq, atm_transmission = atmutil.get_transmission(vis=result.vis, spw_id=spw_id, antenna_id=ant_id)
                axes_atm.plot(atm_freq, atm_transmission, color=atm_color, linestyle='-')

        # Include plot legend.
        leg = ax.legend(loc='best', numpoints=1, prop={'size': 8})
        leg.get_frame().set_alpha(0.5)
        figfile = '{}-field{}-flux_calibration.png'.format(ms.basename, field_id)

        # Save figure to file.
        full_path = os.path.join(output_dir, figfile)
        fig.savefig(full_path)

        # Create a wrapper for current plot, and append to list of plots.
        parameters = {
            'vis': ms.basename,
            'field': field.name,
            'field_id': field.id,
            'intent': sorted(set(field.intents))
        }
        wrapper = logger.Plot(full_path, x_axis='frequency', y_axis='Flux Density', parameters=parameters)
        plots.append(wrapper)

    return plots


def scale_flux(f1, s1, f2, spix):
    """Returns flux at a frequency by extrapolating via the spectral index.

    :param f1: frequency 1
    :param s1: flux density at frequency 1
    :param f2: frequency 2
    :param spix: spectral index
    :return: flux density at frequency 2
    """
    return math.pow(10, spix * math.log10(f2/f1) + math.log10(s1))
