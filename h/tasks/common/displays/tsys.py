import collections
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.logger as logger
from . import common
from pipeline.infrastructure import casa_tasks

LOG = infrastructure.get_logger(__name__)


class TsysSummaryChart(object):
    def __init__(self, context, result, calapp, xaxis='freq', yaxis='tsys'):
        self._context = context
        self._result = result

        self._calapp = calapp
        self._caltable = calapp.gaintable
        self._vis = calapp.vis

        self._xaxis = xaxis
        self._yaxis = yaxis

        ms = context.observing_run.get_ms(self._vis)

        # Set showfdm to True except for "NRO" array.
        self._showfdm = ms.antenna_array.name != 'NRO'

        # Get science spws from MS.
        science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        # Get list of Tsys spws in caltable.
        wrapper = common.CaltableWrapperFactory.from_caltable(self._caltable)
        tsys_in_caltable = set(wrapper.spw)

        # Create a mapping between Tsys and science spws. Sometimes, not all
        # science spws have a matching Tsys window.
        self._tsysmap = collections.defaultdict(list)
        for spw, tsys_spw in enumerate(calapp.spwmap):
            if spw in science_spw_ids and tsys_spw in tsys_in_caltable:
                self._tsysmap[tsys_spw].append(spw)

        # Get base name of figure file(s).
        self._figfile = self._get_figfile()

        # Get mapping from Tsys spw to receiver type.
        self._rxmap = utils.get_receiver_type_for_spws(ms, list(self._tsysmap.keys()))

    def plot(self):
        plots = []
        # Create plot for each Tsys spw.
        for tsys_spw in self._tsysmap:
            plots.append(self._get_plot_wrapper(tsys_spw))

        for p in plots:
            if not p:
                LOG.info("Tsys summary plot wrappers not generated")
            elif not os.path.exists(p.abspath):
                LOG.info("Tsys summary plot not generated for {} spw {}"
                         "".format(p.parameters['vis'], p.parameters['tsys_spw']))

        return [p for p in plots if p and os.path.exists(p.abspath)]

    def _get_figfile(self):
        return os.path.join(self._context.report_dir,
                            'stage%s' % self._result.stage_number,
                            'tsys-%s-summary.png' % os.path.basename(self._vis))

    def _create_task(self, spw_arg, showimage=False):
        task_args = {'vis': self._vis,
                     'caltable': self._caltable,
                     'xaxis': self._xaxis,
                     'yaxis': self._yaxis,
                     'interactive': False,
                     'spw': str(spw_arg),
                     'subplot': 11,
                     'figfile': self._figfile,
                     'overlay': 'antenna,time',
                     'showatm': True,
                     'showfdm': self._showfdm,
                     'chanrange': '90%',  # CAS-7011
                     'showimage': showimage,
                     }
        return casa_tasks.plotbandpass(**task_args)

    def _get_plot_wrapper(self, tsys_spw):
        # PIPE-110: Use showimage=True for DSB receiver spws.
        showimage = self._rxmap.get(tsys_spw, "") == 'DSB'
        task = self._create_task(tsys_spw, showimage=showimage)

        # Get prediction of final name of figure files(s), assuming
        # plotbandpass injects spw ID into every plot filename.
        root, ext = os.path.splitext(self._figfile)
        pb_figfile = '%s.spw%0.2d%s' % (root, tsys_spw, ext)

        if not os.path.exists(pb_figfile):
            LOG.trace("Creating new plot: {}".format(pb_figfile))
            try:
                task.execute()
            except Exception as ex:
                LOG.error("Could not create plot {}".format(pb_figfile))
                LOG.exception(ex)
                return None

        parameters = {'vis': os.path.basename(self._vis),
                      'spw': self._tsysmap[tsys_spw],
                      'tsys_spw': tsys_spw}

        wrapper = logger.Plot(pb_figfile,
                              x_axis=self._xaxis,
                              y_axis=self._yaxis,
                              parameters=parameters,
                              command=str(task))
        return wrapper


class TsysPerAntennaChart(common.PlotbandpassDetailBase):
    def __init__(self, context, result, **kwargs):
        super(TsysPerAntennaChart, self).__init__(context, result, 'freq', 'tsys', overlay='time', showatm=True,
                                                  showfdm=True, chanrange='90%', **kwargs)

        # Get MS and bandpass solution.
        ms = context.observing_run.get_ms(self._vis)
        calapp = result.final[0]

        # Set showfdm to True except for "NRO" array.
        if ms.antenna_array.name == 'NRO':
            self._kwargs['showfdm'] = False

        # Get science spws from MS.
        science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        # Get list of Tsys spws in caltable.
        wrapper = common.CaltableWrapperFactory.from_caltable(self._caltable)
        tsys_in_caltable = set(wrapper.spw)

        # Create a mapping between Tsys and science spws. Sometimes, not all
        # science spws have a matching Tsys window.
        self._tsysmap = collections.defaultdict(list)
        for spw, tsys_spw in enumerate(calapp.spwmap):
            if spw in science_spw_ids and tsys_spw in tsys_in_caltable:
                self._tsysmap[tsys_spw].append(spw)

        # Get mapping from Tsys spw to receiver type.
        self._rxmap = utils.get_receiver_type_for_spws(ms, list(self._tsysmap.keys()))

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
        for tsys_spw_id in self._figfile:
            # PIPE-110: show image sideband for DSB receivers.
            showimage = self._rxmap.get(tsys_spw_id, "") == "DSB"
            # some science windows may not have a Tsys window
            science_spws = self._tsysmap.get(tsys_spw_id, 'N/A')
            for antenna_id, figfile in self._figfile[tsys_spw_id].items():
                ant_name = self._antmap[antenna_id]
                if os.path.exists(figfile):
                    task = self.create_task(tsys_spw_id, antenna_id, showimage=showimage)
                    wrapper = logger.Plot(figfile,
                                          x_axis=self._xaxis,
                                          y_axis=self._yaxis,
                                          parameters={'vis': self._vis_basename,
                                                      'ant': ant_name,
                                                      'spw': ','.join([str(i) for i in science_spws]),
                                                      'tsys_spw': tsys_spw_id},
                                          command=str(task))
                    wrappers.append(wrapper)
                else:
                    LOG.trace('No plotbandpass detail plot found for %s spw '
                              '%s antenna %s: %s not found',
                              self._vis_basename, tsys_spw_id, ant_name, figfile)
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
