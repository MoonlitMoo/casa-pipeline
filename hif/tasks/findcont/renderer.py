"""
Created on 24 Aug 2015

@author: sjw
"""
import collections
import copy
import glob
import operator
import os
import shutil

import numpy
import matplotlib

import pipeline.domain.measures as measures
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.logger as logger
import pipeline.h.tasks.common.displays as displays
from pipeline.infrastructure import casa_tools

LOG = logging.get_logger(__name__)


TR = collections.namedtuple('TR', 'field spw min max frame status spectrum jointmask')


class T2_4MDetailsFindContRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self,
                 uri='findcont.mako',
                 description='Detect continuum frequency ranges',
                 always_rerender=False):
        super(T2_4MDetailsFindContRenderer, self).__init__(uri=uri,
                                                           description=description,
                                                           always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # as a multi-vis task, there's only one result for FindCont
        result = results[0]

        table_rows, raw_rows = self._get_table_rows(pipeline_context, result)

        mako_context.update({'table_rows': table_rows, 'raw_rows': raw_rows})

        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % results[0].stage_number)

        # copy cont.dat file across to weblog directory
        contdat_filename = 'cont.dat'
        contdat_weblink = os.path.join('stage%s' % results[0].stage_number, contdat_filename)
        contdat_path_link = '<a href="{!s}" class="replace-pre" data-title="{!s}">View</a>' \
                            ' or <a href="{!s}" download="{!s}">download</a> {!s} file.'.format(contdat_weblink, contdat_filename,
                                                                                                contdat_weblink, contdat_weblink, contdat_filename)
        if os.path.exists(contdat_filename):
            LOG.trace('Copying %s to %s' % (contdat_filename, weblog_dir))
            shutil.copy(contdat_filename, weblog_dir)

        mako_context.update({'contdat_path_link': contdat_path_link})

    def _get_table_rows(self, context, result):
        ranges_dict = result.result_cont_ranges

        # structure used to recognise non-detections
        non_detection = (['NONE'], [])

        rows = []
        for field in sorted(set(ranges_dict.keys())):
            for spw in map(str, sorted(map(int, set(ranges_dict[field].keys())))):
                plotfile = self._get_plotfile(context, result, field, spw)
                jointmaskplot = self._get_jointmaskplot(context, result, field, spw)  # PIPE-201

                status = ranges_dict[field][spw]['status']

                ranges_and_flags_for_spw = ranges_dict[field][spw].get('cont_ranges', ['NONE'])
                if ranges_and_flags_for_spw != ['NONE']:
                    ranges_for_spw = ranges_and_flags_for_spw['ranges']
                    flags_for_spw = ranges_and_flags_for_spw['flags']
                else:
                    ranges_for_spw = ['NONE']
                    flags_for_spw = []

                if ranges_for_spw in non_detection:
                    rows.append(TR(field='<b>{:s}</b>'.format(field), spw=spw, min='None', max='',
                                   frame='None', status=status, spectrum=plotfile, jointmask=jointmaskplot))
                else:
                    raw_ranges_for_spw = [item['range'] for item in ranges_for_spw if isinstance(item, dict)]
                    refers = numpy.array([item['refer'] for item in ranges_for_spw if isinstance(item, dict)])
                    if (refers == 'TOPO').all():
                        refer = 'TOPO'
                    elif (refers == 'LSRK').all():
                        refer = 'LSRK'
                    elif (refers == 'SOURCE').all():
                        refer = 'SOURCE'
                    else:
                        refer = 'UNDEFINED'
                    sorted_ranges = sorted(raw_ranges_for_spw, key=operator.itemgetter(0))
                    if 'ALL' in flags_for_spw or 'ALLCONT' in flags_for_spw:
                        status += ' , All cont.'
                    for (range_min, range_max) in sorted_ranges:
                        # default units for Frequency is GHz, which matches the
                        # units of cont_ranges values
                        min_freq = measures.Frequency(range_min).str_to_precision(5)
                        max_freq = measures.Frequency(range_max).str_to_precision(5)
                        rows.append(TR(field='<b>{:s}</b>'.format(field), spw=spw, min=min_freq, max=max_freq, frame=refer, status=status,
                                       spectrum=plotfile, jointmask=jointmaskplot))

        return utils.merge_td_columns(rows), rows

    def _get_plotfile(self, context, result, field, spw):
        ranges_dict = result.result_cont_ranges
        raw_plotfile = ranges_dict[field][spw].get('plotfile', None)

        if raw_plotfile in (None, 'none', ''):
            return 'No plot available'

        # move plotfile from working directory to weblog directory if required
        src = os.path.join(context.output_dir, raw_plotfile)
        dst = os.path.join(context.report_dir, 'stage%s' % result.stage_number, raw_plotfile)
        if os.path.exists(src):
            shutil.move(src, dst)

        # create a plot object so we can access (thus generate) the thumbnail
        plot_obj = logger.Plot(dst)

        fullsize_relpath = os.path.relpath(dst, context.report_dir)
        thumbnail_relpath = os.path.relpath(plot_obj.thumbnail, context.report_dir)
        title = 'Detected continuum ranges for %s spw %s' % (field, spw)

        html_args = {
            'fullsize': fullsize_relpath,
            'thumbnail': thumbnail_relpath,
            'title': title,
            'alt': title,
            'rel': 'findcont_plots_%s' % field
        }

        html = ('<a href="{fullsize}"'
                '   title="{title}"'
                '   data-fancybox="{rel}"'
                '   data-caption="{title}">'
                '    <img data-src="{thumbnail}"'
                '         title="{title}"'
                '         alt="{alt}"'
                '         class="lazyload img-responsive">'
                '</a>'.format(**html_args))

        return html

    def _get_jointmaskplot(self, context, result, field, spw):

        joint_mask_name = result.joint_mask_names.get((field, spw), '')

        if joint_mask_name.endswith('.joint.mask2'):
            masktype = 'jointmask2'
        elif joint_mask_name.endswith('.joint.mask'):
            masktype = 'jointmask'
        elif joint_mask_name.endswith('.amendedJointMask.original'):
            masktype = 'amendedmask'
        else:
            return 'No plot available'

        with casa_tools.ImageReader(joint_mask_name) as image:
            info = image.miscinfo()
            info['type'] = masktype
            info['virtspw'] = spw
            info['field'] = field
            image.setmiscinfo(info)

        # create a plot object so we can access (thus generate) the thumbnail
        reportdir = context.report_dir+'/stage{}/'.format(result.stage_number)

        plot_obj = displays.sky.SkyDisplay().plot(context, joint_mask_name, reportdir=reportdir, intent='', collapseFunction='mean',
                                                  **{'cmap': copy.copy(matplotlib.cm.YlOrRd)})

        fullsize_relpath = os.path.relpath(plot_obj.abspath, context.report_dir)
        thumbnail_relpath = os.path.relpath(plot_obj.thumbnail, context.report_dir)
        title = 'Mask used for spectrum for %s spw %s' % (field, spw)

        html_args = {
            'fullsize': fullsize_relpath,
            'thumbnail': thumbnail_relpath,
            'title': title,
            'alt': title,
            'rel': 'findcont_plots_%s' % field
        }

        html = ('<a href="{fullsize}"'
                '   title="{title}"'
                '   data-fancybox="{rel}"'
                '   data-caption="{title}">'
                '    <img data-src="{thumbnail}"'
                '         title="{title}"'
                '         alt="{alt}"'
                '         class="lazyload img-responsive">'
                '</a>'.format(**html_args))

        return html
