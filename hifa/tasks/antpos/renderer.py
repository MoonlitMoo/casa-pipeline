import collections
import os
import numpy

from pipeline.infrastructure import casa_tools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class T2_4MDetailsALMAAntposRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='almaantpos.mako', 
                 description='Correct for antenna position offsets',
                 always_rerender=False):
        super(T2_4MDetailsALMAAntposRenderer, self).__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        table_rows, _ = make_antpos_table(pipeline_context, results)
        # Sort by total offset, from highest-to-lowest, see: PIPE-77
        table_rows_by_offset, rep_wavelength = make_antpos_table(pipeline_context, results, sort_by=lambda x: float(getattr(x, 'total')), reverse=True)
        threshold_in_wavelengths = results[0].inputs['threshold']
        threshold_in_mm = threshold_in_wavelengths * rep_wavelength
        mako_context.update({'table_rows': table_rows,
                             'table_rows_by_offset': table_rows_by_offset,
                             'threshold_in_mm': threshold_in_mm,
                             'threshold_in_wavelengths': threshold_in_wavelengths})

AntposTR = collections.namedtuple('AntposTR', 'vis antenna x y z total total_wavelengths')


def make_antpos_table(context, results, sort_by=lambda x: getattr(x, 'antenna'), reverse=False):
    """
    Creates an antenna positions table, returning the table rows and also the representative wavelength for the data (to later be used to determine whether offsets
    are above or below a threshold given in units of this wavelength.)
    """
    # Will hold all the antenna offset table rows for the results
    rows = []

    # Loop over the results
    for single_result in results:
        vis_cell = os.path.basename(single_result.inputs['vis'])

        ms = context.observing_run.get_ms(single_result.inputs['vis'])
        if hasattr(ms, 'representative_target') and ms.representative_target[1] is not None:
            rep_freq = casa_tools.quanta.getvalue(casa_tools.quanta.convert(ms.representative_target[1]))[0]
        else:
            # If there is no representative frequency, use the center of the first spw, see PIPE-77.
            first_spw = ms.get_spectral_windows()[0]
            rep_freq = float(first_spw.centre_frequency.value)

        rep_wavelength = casa_tools.quanta.getvalue(casa_tools.quanta.convert(casa_tools.quanta.constants('c'), 'm/s'))[0]/rep_freq * 1000 # convert to mm

        # Construct the antenna list and the xyz offsets
        antenna_list = single_result.antenna.split(',')
        xyzoffsets_list = make_xyzoffsets_list(single_result.offsets, rep_wavelength)

        # No offsets
        if len(antenna_list) == 0 or len(antenna_list) != len(xyzoffsets_list):
            continue

        # Loop over the individual antennas and offsets
        for item in zip (antenna_list, xyzoffsets_list):
            antname = item[0]
            xoffset = '%0.2e' % item[1][0]
            yoffset = '%0.2e' % item[1][1]
            zoffset = '%0.2e' % item[1][2]
            total_offset = '%0.2f' % item[1][3]
            total_offset_in_wavelengths = '%0.2f' % item[1][4]
            tr = AntposTR(vis_cell, antname, xoffset, yoffset, zoffset, total_offset, total_offset_in_wavelengths)
            rows.append(tr)

    # First sort by the secondary (passed-in) sort
    rows.sort(key=sort_by, reverse=reverse)

    # Then do primary sort by measurement set name
    rows.sort(key=lambda x: getattr(x, 'vis'))

    return utils.merge_td_columns(rows), rep_wavelength


def make_xyzoffsets_list (offsets_list, rep_wavelength):
    if len(offsets_list) == 0:
        return []

    xyz_list = []
    for i in range (0, len(offsets_list), 3):
        x, y, z = offsets_list[i], offsets_list[i+1], offsets_list[i+2]
        # PIPE-77: Add total offset in mm and also in units of wavelength
        total_offset = numpy.linalg.norm([x, y, z]) * 1000.0
        total_offset_in_wavelengths = total_offset/rep_wavelength
        xyz_list.append((x, y, z, total_offset, total_offset_in_wavelengths))
    return xyz_list
