"""
Created on 5 Sep 2014

@author: sjw
"""
import collections
import operator
import os
import shutil
from functools import reduce

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.domain.measures import FrequencyUnits
from pipeline.infrastructure import casa_tools

LOG = logging.get_logger(__name__)


class T2_4MDetailsImportDataRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='importdata.mako', 
                 description='Register measurement sets with the pipeline', 
                 always_rerender=False):
        super(T2_4MDetailsImportDataRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % result.stage_number)

        setjy_results = []
        for r in result:
            setjy_results.extend(r.setjy_results)

        measurements = []        
        for r in setjy_results:
            measurements.extend(r.measurements)

        num_mses = reduce(operator.add, [len(r.mses) for r in result])

        flux_table_rows = make_flux_table(pipeline_context, setjy_results)

        repsource_table_rows, repsource_name_is_none = make_repsource_table(pipeline_context, result)
        repsource_defined = not any('N/A' in td for tr in repsource_table_rows for td in tr[1:])

        # copy flux.csv file across to weblog directory
        fluxcsv_filename = 'flux.csv'
        if os.path.exists(fluxcsv_filename):
            LOG.trace('Copying %s to %s' % (fluxcsv_filename, weblog_dir))
            shutil.copy(fluxcsv_filename, weblog_dir)

        fluxcsv_files = {ms.basename: os.path.join('stage%s' % result.stage_number, fluxcsv_filename)
                         for r in result
                         for ms in r.mses}

        mako_context.update({
            'flux_imported': True if measurements else False,
            'flux_table_rows': flux_table_rows,
            'repsource_defined': repsource_defined,
            'repsource_name_is_none': repsource_name_is_none,
            'repsource_table_rows': repsource_table_rows,
            'num_mses': num_mses,
            'fluxcsv_files': fluxcsv_files,
            'weblog_dir': weblog_dir
        })


FluxTR = collections.namedtuple('FluxTR', 'vis field intent spw i q u v spix ageNMP')


def make_flux_table(context, results):
    # will hold all the flux stat table rows for the results
    rows = []

    for single_result in results:
        ms_for_result = context.observing_run.get_ms(single_result.vis)
        vis_cell = os.path.basename(single_result.vis)

        # measurements will be empty if fluxscale derivation failed
        if len(single_result.measurements) == 0:
            continue

        for field_arg in sorted(single_result.measurements, key=lambda f: ms_for_result.get_fields(f)[0].id):
            field = ms_for_result.get_fields(field_arg)[0]
            field_cell = '%s (#%s)' % (field.name, field.id)

            for measurement in sorted(single_result.measurements[field_arg], key=operator.attrgetter('spw_id')):
                fluxes = collections.defaultdict(lambda: 'N/A')
                for stokes in ['I', 'Q', 'U', 'V']:
                    try:                        
                        flux = getattr(measurement, stokes)
                        fluxes[stokes] = '%s' % flux
                    except:
                        pass

                if measurement.age:
                    age = measurement.age
                elif measurement.age == 0 or measurement.age == 0.0:
                    age = '0'
                else:
                    age = 'N/A'

                # Get the intent for each 'field/spw' combination
                spw_intents = ms_for_result.get_spectral_window(measurement.spw_id).intents

                # get one spw/field intent
                scan_intents_list = [scan.intents for scan in ms_for_result.get_scans(field=field.name, spw=measurement.spw_id)]
                scan_intents = set().union(*scan_intents_list)

                # Set of intents to include from PIPE-1006 + PIPE-1724
                field_spw_intents = ", ".join(sorted(scan_intents.intersection(
                    {'PHASE', 'BANDPASS', 'FLUX', 'CHECK', 'POLARIZATION', 'AMPLITUDE', 'DIFFGAINREF', 'DIFFGAINSRC'})))

                tr = FluxTR(vis_cell, field_cell, field_spw_intents, measurement.spw_id, 
                            fluxes['I'], fluxes['Q'], fluxes['U'], fluxes['V'],
                            measurement.spix, age)
                rows.append(tr)

    return utils.merge_td_columns(rows)


RepsourceTR = collections.namedtuple('RepsourceTR', 'vis source rfreq rbwidth spwid bwidth dynrange')


def make_repsource_table(context, results):
    # will hold all the representative source table rows for the results

    qa = casa_tools.quanta

    rows = []

    repsource_name_is_none = False
    for r in results:
        for ms in r.mses:

            # Skip if not ALMA data
            #    What about single dish
            if ms.antenna_array.name != 'ALMA':
                continue

            # ASDM
            vis = ms.basename

            # If either the representative frequency or bandwidth is undefined then
            # the representative target is undefined
            reptarget_name, reptarget_freq, reptarget_bw = ms.representative_target
            reptarget_defined = (reptarget_name not in (None, 'None', 'none') and
                                 reptarget_freq is not None and
                                 reptarget_bw is not None)
            # when no rep.target is defined, its name is None, but a string 'none' means an incomplete definition
            repsource_name_is_none = reptarget_name == 'none'
            if not reptarget_defined:
                rows.append(RepsourceTR(vis, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'))
                continue

            # Is the representative source in the context or not
            if not context.project_performance_parameters.representative_source:
                source_name = None
            else:
                source_name = context.project_performance_parameters.representative_source

            # Is the representative spw in the context or not
            if not context.project_performance_parameters.representative_spwid:
                source_spwid = None
            else:
                source_spwid = context.project_performance_parameters.representative_spwid

            dynrange_bw = ms.science_goals['spectralDynamicRangeBandWidth']
            if dynrange_bw is not None:
                dynrange_bw = qa.tos(dynrange_bw, 5)
            else:
                dynrange_bw = 'Not available'  # cannot use N/A because this will hide the entire row

            # Determine the representative source name and spwid for the ms
            repsource_name, repsource_spwid = ms.get_representative_source_spw(source_name=source_name,
                                                                               source_spwid=source_spwid)

            # Populate the table rows
            # No source
            if repsource_name is None: 
                if not reptarget_name:
                    tr = RepsourceTR(vis, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')
                else:
                    tr = RepsourceTR(vis, reptarget_name, 'Unknown', 'Unknown', 'Unknown', 'Unknown', dynrange_bw)
                rows.append(tr)
                continue

            # No spwid
            if repsource_spwid is None:
                tr = RepsourceTR(vis, repsource_name, qa.tos(reptarget_freq, 5),
                                 qa.tos(reptarget_bw, 5), 'Unknown', 'Unknown', dynrange_bw)
                rows.append(tr)
                continue

            # Get center frequency and channel width for representative spw id
            repsource_spw = ms.get_spectral_window(repsource_spwid)
            repsource_chanwidth = qa.quantity(
                float(repsource_spw.channels[0].getWidth().to_units(FrequencyUnits.MEGAHERTZ)), 'MHz')

            tr = RepsourceTR(vis, repsource_name, qa.tos(reptarget_freq, 5),
                             qa.tos(reptarget_bw, 5), str(repsource_spwid),
                             qa.tos(repsource_chanwidth, 5), dynrange_bw)
            rows.append(tr)

    return utils.merge_td_columns(rows), repsource_name_is_none
