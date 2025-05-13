"""Renderer for k2jycal task."""
import os
import collections
import shutil

from typing import TYPE_CHECKING, Any, Dict, Optional

import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils

from . import display as display
from ..common import utils as sdutils

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)

JyperKTRV = collections.namedtuple('JyperKTRV', 'virtualspw msname realspw antenna pol factor')
JyperKTR  = collections.namedtuple('JyperKTR',  'spw msname antenna pol factor')


class T2_4MDetailsSingleDishK2JyCalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """Weblog renderer class for k2jycal task."""

    def __init__(self, uri: str = 'hsd_k2jycal.mako',
                 description: str = 'Generate Kelvin to Jy calibration table.',
                 always_rerender: bool = False) -> None:
        """Initialize T2_4MDetailsSingleDishK2JyCalRenderer instance.

        Args:
            template: Name of Mako template file. Defaults to 'hsd_k2jycal.mako'.
            description: Description of the task. This is embedded into the task detail page.
                         Defaults to 'Generate Kelvin to Jy calibration table.'.
            always_rerender: Always rerender the page if True. Defaults to False.
        """
        super(T2_4MDetailsSingleDishK2JyCalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx: Dict[str, Any], context: 'Context', results: 'ResultsList') -> None:
        """Update context for weblog rendering.

        Args:
            ctx: Context for weblog rendering
            context: Pipeline context
            results: ResultsList instance. Should hold a list of SDK2JyCalResults instance.
        """
        spw_factors = collections.defaultdict(lambda: [])
        valid_spw_factors = collections.defaultdict(lambda: collections.defaultdict(lambda: []))

        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        trfunc_r = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTR(_rspwid, _vis, _antenna, _pol, _factor)
        trfunc_v = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTRV(_vspwid, _vis, _rspwid, _antenna, _pol, _factor)
        trfunc = trfunc_v if dovirtual else trfunc_r
        reffile_list = []
        for r in results:
            # rearrange jyperk factors
            ms = context.observing_run.get_ms(name=r.vis)
            vis = ms.basename
            spw_band = {}
            for spw in ms.get_spectral_windows(science_windows_only=True):
                spwid = spw.id
                # virtual spw id
                vspwid = context.observing_run.real2virtual_spw_id(spwid, ms)
                ddid = ms.get_data_description(spw=spwid)
                if vspwid not in spw_band:
                    spw_band[vspwid] = spw.band
                for ant in ms.get_antenna():
                    ant_name = ant.name
                    corrs = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))
#                     # an attempt to collapse pol rows
#                     # corr_collector[factor] = [corr0, corr1, ...]
#                     corr_collector = collections.defaultdict(lambda: [])
                    for corr in corrs:
                        factor = self.__get_factor(r.factors, vis, spwid, ant_name, corr)
#                         corr_collector[factor].append(corr)
#                     for factor, corrlist in corr_collector.items():
#                         corr = str(', ').join(corrlist)
                        jyperk = factor if factor is not None else 'N/A (1.0)'
#                         tr = JyperKTR(vis, spwid, ant_name, corr, jyperk)
                        tr = trfunc(vspwid, vis, spwid, ant_name, corr, jyperk)
                        spw_factors[vspwid].append(tr)
                        if factor is not None:
                            valid_spw_factors[vspwid][corr].append(factor)
            reffile_list.append(r.reffile)
        stage_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        # histogram plots of Jy/K factors
        hist_plots = []
        for vspwid, valid_factors in valid_spw_factors.items():
            if len(valid_factors) > 0:
                task = display.K2JyHistDisplay(stage_dir, vspwid, valid_factors, spw_band[vspwid])
                hist_plots += task.plot()
        # input Jy/K files
        reffile_list = list(dict.fromkeys(reffile_list))   # remove duplicated filenames
        reffile_copied_list = []
        for reffile in reffile_list:
            if reffile is not None and os.path.exists(reffile):
                LOG.debug('copying %s to %s' % (reffile, stage_dir))
                shutil.copy2(reffile, stage_dir)
                reffile_copied_list.append( os.path.join(stage_dir, os.path.basename(reffile)) )
        reffile_copied = None if len(reffile_copied_list) == 0 else reffile_copied_list
        # order table rows so that spw comes first
        row_values = []
        for factor_list in spw_factors.values():
            row_values += list(factor_list)
        ctx.update({'jyperk_rows': utils.merge_td_columns(row_values),
                    'reffile_list': reffile_copied,
                    'jyperk_hist': hist_plots,
                    'dovirtual': dovirtual})

    @staticmethod
    def __get_factor(
        factor_dict: Dict[str, Dict[int, Dict[str, Dict[str, float]]]],
        vis: str, spwid: int, ant_name: str, pol_name: str
    ) -> Optional[float]:
        """Return Jy/K conversion factor for given meta data.

        Returns a factor corresponding to vis, spwid, ant_name, and pol_name from
        a factor_dict[vis][spwid][ant_name][pol_name] = factor
        If factor_dict lack corresponding factor, the method returns None.

        Args:
            factor_dict: Jy/K factors with meta data
            vis: Name of MS
            spwid: Spectral window id
            ant_name: Name of antenna
            pol_name: Polarization type

        Returns:
            Conversion factor for given meta data. If no corresponding factor
            exists in factor_dict, None is returned.
        """
        if (vis not in factor_dict or
                spwid not in factor_dict[vis] or
                ant_name not in factor_dict[vis][spwid] or
                pol_name not in factor_dict[vis][spwid][ant_name]):
            return None
        return factor_dict[vis][spwid][ant_name][pol_name]
