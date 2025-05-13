import itertools
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger

from . import applycal
from . import common

LOG = infrastructure.get_logger(__name__)


class BandpassDetailChart(common.PlotbandpassDetailBase):
    def __init__(self, context, result, xaxis, yaxis, **kwargs):
        super(BandpassDetailChart, self).__init__(context, result, xaxis, yaxis, **kwargs)

    def plot(self):
        # PIPE-110: create separate calls to plotbandpass for DSB and non-DSB
        # receivers.
        missing_dsb = [(spw_id, ant_id)
                       for spw_id in self._figfile
                       for ant_id in self._antmap
                       if not os.path.exists(self._figfile[spw_id][ant_id]) and self._rxmap.get(spw_id, "") == "DSB"]
        if missing_dsb:
            self._create_plotbandpass_task(missing_dsb, showimage=True)

        missing_nondsb = [(spw_id, ant_id)
                          for spw_id in self._figfile
                          for ant_id in self._antmap
                          if not os.path.exists(self._figfile[spw_id][ant_id]) and self._rxmap.get(spw_id, "") != "DSB"]
        if missing_nondsb:
            self._create_plotbandpass_task(missing_nondsb, showimage=False)

        # Create plot wrappers.
        wrappers = []
        for spw_id in self._figfile:
            # PIPE-110: show image sideband for DSB receivers.
            showimage = self._rxmap.get(spw_id, "") == "DSB"
            for antenna_id, figfile in self._figfile[spw_id].items():
                ant_name = self._antmap[antenna_id]
                if os.path.exists(figfile):
                    task = self.create_task(spw_id, antenna_id, showimage=showimage)
                    wrapper = logger.Plot(figfile,
                                          x_axis=self._xaxis,
                                          y_axis=self._yaxis,
                                          parameters={'vis': self._vis_basename,
                                                      'ant': ant_name,
                                                      'spw': spw_id},
                                          command=str(task))
                    wrappers.append(wrapper)
                else:
                    LOG.trace('No plotbandpass detail plot found for %s spw '
                              '%s antenna %s: %s not found',
                              self._vis_basename, spw_id, ant_name, figfile)
        return wrappers

    def _create_plotbandpass_task(self, missing, showimage=False):
        LOG.trace('Executing new plotbandpass job for missing figures')
        spw_ids = ','.join({str(spw_id) for spw_id, _ in missing})
        ant_ids = ','.join({str(ant_id) for _, ant_id in missing})
        try:
            task = self.create_task(spw_ids, ant_ids, showimage=showimage)
            task.execute()
        except Exception as ex:
            LOG.error('Could not create plotbandpass details plots')
            LOG.exception(ex)
            return None


class BandpassSummaryChart(common.PlotbandpassDetailBase):
    def __init__(self, context, result, xaxis, yaxis, **kwargs):
        super(BandpassSummaryChart, self).__init__(context, result, xaxis, yaxis, overlay='baseband', **kwargs)

        # overlaying baseband, so we need to merge the individual spw keys
        # into joint keys, and the filenames into a list as the output could
        # be any of the filenames, depending on whether spws were flagged
        spw_ids = [spw_id for spw_id in self._figfile]
        ant_ids = [list(ant_ids.keys()) for _, ant_ids in self._figfile.items()]
        ant_ids = set(itertools.chain(*ant_ids))

        self._figfile = dict((ant_id, [self._figfile[spw_id][ant_id] for spw_id in spw_ids])
                             for ant_id in ant_ids)

        # PIPE-110: if any of the spws corresponds to a DSB receiver, then show
        # the image sideband.
        self._showimage = "DSB" in [self._rxmap.get(spw_id, "") for spw_id in spw_ids]

    def plot(self):
        missing = [ant_id
                   for ant_id in self._antmap
                   if not any([os.path.exists(f) for f in self._figfile[ant_id]])]
        if missing:
            LOG.trace('Executing new plotbandpass job for missing figures')
            ant_ids = ','.join([str(ant_id) for ant_id in missing])
            try:
                task = self.create_task('', ant_ids, showimage=self._showimage)
                task.execute()
            except Exception as ex:
                LOG.error('Could not create plotbandpass summary plots')
                LOG.exception(ex)
                return None

        wrappers = []
        for antenna_id, figfiles in self._figfile.items():
            for figfile in figfiles:
                if os.path.exists(figfile):
                    task = self.create_task('', antenna_id, showimage=self._showimage)
                    wrapper = logger.Plot(figfile,
                                          x_axis=self._xaxis,
                                          y_axis=self._yaxis,
                                          parameters={'vis': self._vis_basename,
                                                      'ant': self._antmap[antenna_id]},
                                          command=str(task))
                    wrappers.append(wrapper)
                    break
            else:
                LOG.trace('No plotbandpass summary plot found for antenna '
                          '%s' % self._antmap[antenna_id])
        return wrappers


class BandpassAmpVsFreqSummaryChart(BandpassSummaryChart):
    """
    Create an amp vs freq plot for each antenna
    """
    def __init__(self, context, result):
        # request plots per spw, overlaying all antennas
        super(BandpassAmpVsFreqSummaryChart, self).__init__(context, result, xaxis='freq', yaxis='amp', showatm=True)


class BandpassPhaseVsFreqSummaryChart(BandpassSummaryChart):
    """
    Create an phase vs freq plot for each antenna
    """
    def __init__(self, context, result):
        # request plots per spw, overlaying all antennas
        super(BandpassPhaseVsFreqSummaryChart, self).__init__(context, result, xaxis='freq', yaxis='phase',
                                                              showatm=True, markersize=6)


class BandpassAmpVsFreqDetailChart(BandpassDetailChart):
    """
    Create an amp vs freq plot for each spw/antenna combination.
    """
    def __init__(self, context, result):
        # request plots per antenna and spw
        super(BandpassAmpVsFreqDetailChart, self).__init__(context, result, xaxis='freq', yaxis='amp', showatm=True)


class BandpassPhaseVsFreqDetailChart(BandpassDetailChart):
    """
    Create an amp vs freq plot for each spw/antenna combination.
    """
    def __init__(self, context, result):
        # request plots per antenna and spw
        super(BandpassPhaseVsFreqDetailChart, self).__init__(context, result, xaxis='freq', yaxis='phase', showatm=True,
                                                             markersize=6)


class BandpassAmpVsUVDetailChart(applycal.SpwSummaryChart):
    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'clearplots': True,
            'coloraxis': 'corr',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0],
            'showgui': False,
        }
        plot_args.update(**overrides)

        super(BandpassAmpVsUVDetailChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp',
                                                         intent=intent, **plot_args)


class BandpassAmpVsTimeDetailChart(applycal.SpwSummaryChart):
    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'clearplots': True,
            'coloraxis': 'corr',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0],
            'showgui': False,
        }
        plot_args.update(**overrides)

        super(BandpassAmpVsUVDetailChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp',
                                                         intent=intent, **plot_args)
