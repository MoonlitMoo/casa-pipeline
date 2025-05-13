import collections
import os
import shutil

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common import flagging_renderer_utils as flagutils
from pipeline.hsd.tasks.applycal import renderer as sdapplycal
from pipeline.hsd.tasks.common import utils as sdutils

LOG = logging.get_logger(__name__)

JyperKTRV = collections.namedtuple('JyperKTRV', 'virtualspw msname realspw antenna pol factor')
JyperKTR  = collections.namedtuple('JyperKTR', 'spw msname antenna pol factor')


class T2_4MDetailsNRORestoreDataRenderer(sdapplycal.T2_4MDetailsSDApplycalRenderer):
    def __init__(self, uri='hsdn_restoredata.mako',
                 description='Restoredata with scale adjustment among beams for NRO FOREST data.',
                 always_rerender=False):
        super(T2_4MDetailsNRORestoreDataRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):

        ctx_result = ctx['result']
        ctx_result0 = ctx_result[0]
        ctx_result0_inputs = ctx_result0.inputs
        inputs = {}
        inputs_keys = ('rawdata_dir', 'products_dir', 'vis', 'output_dir', 'reffile', 'hm_rasterscan')
        for key in inputs_keys:
            if key in ctx_result0_inputs:
                inputs[key] = ctx_result0_inputs[key]
        result_inputs = inputs
        LOG.debug('result_inputs = {0}'.format(result_inputs))
        ctx['result'].inputs = result_inputs
        reffile = None
        spw_factors = collections.defaultdict(lambda: [])
        valid_spw_factors = collections.defaultdict(lambda: collections.defaultdict(lambda: []))

        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        trfunc_r = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTR(_rspwid, _vis, _antenna, _pol, _factor)
        trfunc_v = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTRV(_vspwid, _vis, _rspwid, _antenna, _pol, _factor)
        trfunc = trfunc_v if dovirtual else trfunc_r

        res0 = results[0]

        ampcal_results = res0.ampcal_results
        applycal_results = res0.applycal_results

        for r in ampcal_results:
            metadata = None
            # Read reffile and insert the elements into a list "lines".
            reffile = r.reffile
            if not os.path.exists(reffile):
                LOG.warning('The factor file is not found in current directory: os.path.exists(reffile) = {0}'.format(
                    os.path.exists(reffile)))
                metadata = ['No Data : No Data']
                break
            else:
                LOG.info('os.path.exists(reffile) = {0}'.format(os.path.exists(reffile)))
                with open(reffile, 'r') as f:
                    lines = f.readlines()
                # Count the line numbers for the beginning of metadata part and the end of it.
                if len(lines) == 0:
                    LOG.warning('The factor file is invalid format: size of reffile = {0}'.format(len(lines)))
                    metadata = ['No Data : No Data']
                    break
                else:
                    count = 0
                    beginpoint = 0
                    endpoint = 0
                    for elem in lines:
                        count += 1
                        if elem.startswith('#---Fill'):
                            beginpoint = count
                            LOG.debug('beginpoint = {0}'.format(beginpoint))
                        if elem.startswith('#---End'):
                            endpoint = count
                            LOG.debug('endpoint = {0}'.format(endpoint))
                            continue
                    # Insert the elements (from beginpoint to endpoint) into a list "metadata_tmp".
                    metadata_tmp = []
                    elem = ""
                    key = ""
                    value = ""
                    multivalue = ""
                    felem = ""
                    count = 0
                    for elem in lines:
                        count += 1
                        if count < beginpoint + 1:
                            continue
                        if count >= endpoint:
                            continue
                        elem = elem.replace('\r','')
                        elem = elem.replace('\n','')
                        elem = elem.replace('#','')
                        elem = elem.lstrip()
                        check = elem.split()
                        # The lines without "#" are regarded as all FreeMemo's values.
                        if len(elem) == 0:
                            LOG.debug('Skipped the blank line of the reffile.')
                            continue
                        else:
                            if not ":" in check[0]:
                                key = 'FreeMemo'
                                value = elem
                                elem = key + ':' + value
                            else:
                                onepair = elem.split(':', 1)
                                key = "".join(onepair[0])
                                value = "".join(onepair[1])
                                elem = key + ':' + value
                        metadata_tmp.append(elem)

                    if len(metadata_tmp) == 0:
                        LOG.info('The factor file is invalid format. [No Data : No Data] is inserted instead of blank.')
                        metadata = ['No Data : No Data']
                        break
                    else:
                        LOG.debug('metadata_tmp: {0}'.format(metadata_tmp))
                        # Arrange "metadata_tmp" list to "metadata" list to connect FreeMemo values.
                        metadata = []
                        elem = ""
                        for elem in metadata_tmp:
                            onepair = elem.split(':', 1)
                            key = "".join(onepair[0])
                            value = "".join(onepair[1])
                            if 'FreeMemo' in key:
                                multivalue += value + '<br>'
                                elem = key + ':' + multivalue
                            else:
                                elem = key + ':' + value
                                metadata.append(elem)
                        felem = 'FreeMemo:' + multivalue
                        metadata.append(felem)
                        LOG.info('metadata: {0}'.format(metadata))
        ctx.update({'metadata': metadata})

        for r in ampcal_results:
            # rearrange scaling factors
            ms = context.observing_run.get_ms(name=r.vis)
            vis = ms.basename
            spw_band = {}
            for spw in ms.get_spectral_windows(science_windows_only=True):
                spwid = spw.id
                vspwid = context.observing_run.real2virtual_spw_id(spwid, ms)
                ddid = ms.get_data_description(spw=spwid)

                if vspwid not in spw_band:
                    spw_band[vspwid] = spw.band

                for ant in ms.get_antenna():
                    LOG.debug('ant = {0}'.format(ant))
                    ant_name = ant.name
                    corrs = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))

                     # an attempt to collapse pol rows
                     # corr_collector[factor] = [corr0, corr1, ...]
                    corr_collector = collections.defaultdict(lambda: [])
                    for corr in corrs:
                        factor = self.__get_factor(r.factors, vis, spwid, ant_name, corr)

                        corr_collector[factor].append(corr)
                    for factor, corrlist in corr_collector.items():
                        corr = str(', ').join(corrlist)
                        jyperk = factor if factor is not None else 'N/A (1.0)'

                        tr = trfunc(vspwid, vis, spwid, ant_name, corr, jyperk)
                        spw_factors[vspwid].append(tr)
                        if factor is not None:
                            valid_spw_factors[vspwid][corr].append(factor)
            reffile = r.reffile
            LOG.debug('reffile = {0}'.format(reffile))
        stage_dir = os.path.join(context.report_dir, 'stage%s' % ampcal_results.stage_number)

        # input file to correct relative amplitude
        reffile_copied = None
        if reffile is not None and os.path.exists(reffile):
            shutil.copy2(reffile, stage_dir)
            reffile_copied = os.path.join(stage_dir, os.path.basename(reffile))
        # order table rows so that spw comes first
        row_values = []
        for factor_list in spw_factors.values():
            row_values += list(factor_list)

        # set context
        ctx.update({'jyperk_rows': utils.merge_td_columns(row_values),
                    'reffile': reffile_copied,
                    'dovirtual': dovirtual})

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % applycal_results.stage_number)
        LOG.debug('weblog_dir = {0}'.format(weblog_dir))

        intents_to_summarise = ['TARGET']
        flag_totals = {}
        for r in applycal_results:
            LOG.debug('r in applycal_results = {0}'.format(r))

            if r.inputs['flagsum'] == True:
                flag_totals = utils.dict_merge(flag_totals,
                                               flagutils.flags_for_result(
                                                   r, context, intents_to_summarise=intents_to_summarise
                                               ))
        calapps = {}
        for r in applycal_results:
            calapps = utils.dict_merge(calapps,
                                       self.calapps_for_result(r))
        LOG.debug('calapps = {0}'.format(calapps))

        caltypes = {}
        for r in applycal_results:
            caltypes = utils.dict_merge(caltypes,
                                        self.caltypes_for_result(r))
        LOG.debug('caltypes = {0}'.format(caltypes))

        filesizes = {}
        for r in applycal_results:
            vis = r.inputs['vis']
            ms = context.observing_run.get_ms(vis)
            filesizes[ms.basename] = ms._calc_filesize()

        # return all agents so we get ticks and crosses against each one
        agents = ['before', 'applycal']

        ctx.update({
            'flags': flag_totals,
            'calapps': calapps,
            'caltypes': caltypes,
            'agents': agents,
            'dirname': weblog_dir,
            'filesizes': filesizes
        })

        # CAS-5970: add science target plots to the applycal page
        (science_amp_vs_freq_summary_plots, science_amp_vs_freq_subpages, uv_max) = self.create_single_dish_science_plots(context, applycal_results)

        # delete extra entry specific to hsd_applycal
        science_amp_vs_freq_summary_plots.pop('__hsd_applycal__', None)
        for vis, plots_per_source in science_amp_vs_freq_summary_plots.items():
            # NRO data is always single field so just omit field name
            assert len(plots_per_source) == 1
            plots = plots_per_source[0][1]
            science_amp_vs_freq_summary_plots[vis] = plots

        ctx.update({
            'science_amp_vs_freq_plots': science_amp_vs_freq_summary_plots,
            'science_amp_vs_freq_subpages': science_amp_vs_freq_subpages,
            'uv_max': uv_max,
        })
        LOG.debug('ctx = {0}'.format(ctx))

    @staticmethod
    def __get_factor(factor_dict, vis, spwid, ant_name, pol_name):
        """
        Returns a factor corresponding to vis, spwid, ant_name, and pol_name from
        a factor_dict[vis][spwid][ant_name][pol_name] = factor
        If factor_dict lack corresponding factor, the method returns None.
        """
        if (vis not in factor_dict or
                spwid not in factor_dict[vis] or
                ant_name not in factor_dict[vis][spwid] or
                pol_name not in factor_dict[vis][spwid][ant_name]):
            return None
        return factor_dict[vis][spwid][ant_name][pol_name]
