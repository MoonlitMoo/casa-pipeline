import collections
import itertools
import operator
import os
import re
from typing import Union, List, Dict

import cachetools
import matplotlib.dates
import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.domain import MeasurementSet
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

COLSHAPE_FORMAT = re.compile(r'\[(?P<num_pols>\d+), (?P<num_rows>\d+)\]')


class PlotbandpassDetailBase(object):
    def __init__(self, context, result, xaxis, yaxis, **kwargs):
        # identify the bandpass solution for the target
        calapps = [c for c in result.final
                   if (c.intent == '' or 'TARGET' in c.intent)]

        if len({c.gaintable for c in calapps}) > 1:
            raise ValueError('Target solutions != 1')
        calapp = calapps[0]

        self._vis = calapp.vis
        self._vis_basename = os.path.basename(self._vis)
        self._caltable = calapp.gaintable

        self._xaxis = xaxis
        self._yaxis = yaxis
        self._kwargs = kwargs

        ms = context.observing_run.get_ms(self._vis)

        # we should only request plots for the antennas and spws in the
        # caltable, which may be a subset of those in the measurement set
        caltable_wrapper = CaltableWrapperFactory.from_caltable(self._caltable)
        antenna_ids = set(caltable_wrapper.antenna)
        spw_ids = set(caltable_wrapper.spw)

        # use antenna name rather than ID where possible
        antenna_arg = ','.join([str(i) for i in antenna_ids])
        antennas = ms.get_antenna(antenna_arg)
        self._antmap = dict((a.id, a.name) for a in antennas)

        # the number of polarisations for a spw may not be equal to the number
        # of shape of the column. For example, X403 has XX,YY for some spws
        # but XX for the science data.
        self._pols = {}
        for spw in spw_ids:
            dd = ms.get_data_description(spw=int(spw))
            num_pols = dd.num_polarizations
            pols = ','.join([dd.get_polarization_label(p)
                             for p in range(num_pols)])
            self._pols[spw] = pols

        # Get mapping from spw to receiver type.
        self._rxmap = utils.get_receiver_type_for_spws(ms, spw_ids)

        # Get base name of figure file(s).
        overlay = self._kwargs.get('overlay', '')
        fileparts = {
            'caltable': os.path.basename(calapp.gaintable),
            'x': self._xaxis,
            'y': self._yaxis,
            'overlay': '-%s' % overlay if overlay else ''
        }
        png = '{caltable}-{y}_vs_{x}{overlay}.png'.format(**fileparts)

        self._figroot = os.path.join(context.report_dir,
                                     'stage%s' % result.stage_number,
                                     png)

        # plotbandpass injects spw ID and antenna name into every plot filename
        self._figfile = collections.defaultdict(dict)
        root, ext = os.path.splitext(self._figroot)

        scan_ids_in_caltable = sorted(list(set(caltable_wrapper.scan)))
        scan_to_suffix = {scan_id: '.t%02d' % i
                          for i, scan_id in enumerate(scan_ids_in_caltable)}

        # Get prediction of final name of figure file for each spw and ant,
        # assuming plotbandpass injects spw ID and ant into every plot
        # filename.
        for spw_id, ant_id in itertools.product(spw_ids, antenna_ids):
            if 'time' not in overlay:
                # filter the caltable down to the data containing the spw and
                # antenna. From this we can read the scan, and thus derive what
                # suffix plotbandpass will add to the png.
                filtered = caltable_wrapper.filter(spw=[spw_id], antenna=[ant_id])

                scan_ids = set(filtered.scan)
                # TODO this breaks if the spw is present in more than one scan!
                # We're having to reverse engineer plotbandpass' naming scheme.
                # Perhaps we should glob for files created somehow?
                if len(scan_ids) != 1:
                    time = '.t00'
                else:
                    time = scan_to_suffix[scan_ids.pop()]
            else:
                time = ''

            ant_name = self._antmap[ant_id]
            real_figfile = '%s.%s.spw%0.2d%s%s' % (root, ant_name, spw_id,
                                                   time, ext)
            self._figfile[spw_id][ant_id] = real_figfile

    def create_task(self, spw_arg, antenna_arg, showimage=False):
        task_args = {'vis': self._vis,
                     'caltable': self._caltable,
                     'xaxis': self._xaxis,
                     'yaxis': self._yaxis,
                     'interactive': False,
                     'spw': spw_arg,
                     'antenna': antenna_arg,
                     'subplot': 11,
                     'figfile': self._figroot,
                     'showimage': showimage,
                     }
        task_args.update(**self._kwargs)

        return casa_tasks.plotbandpass(**task_args)

    def plot(self):
        pass


class PlotmsCalLeaf(object):
    """
    Class to execute plotms and return a plot wrapper. It passes the spw and
    ant arguments through to plotms without further manipulation, creating
    exactly one plot.

    If a list of calapps is provided as input, the caltables from each calapp
    will be overplotted on the same plot.
    """

    def __init__(self, context, result, calapp : Union[List[callibrary.CalApplication], callibrary.CalApplication],
                 xaxis, yaxis, spw='', ant='', pol='', correlation='', plotrange=[], coloraxis=''):
        self._context = context
        self._result = result
        self._xaxis = xaxis
        self._yaxis = yaxis
        self._spw = spw
        self._correlation = correlation
        self._plotrange = plotrange
        self._coloraxis = coloraxis

        # Make calapp a list if it isn't already, as the rest of the code assumes this is a list
        if not isinstance(calapp, list):
            calapp = [calapp]

        self._calapp = calapp
        self._caltable = [cal.gaintable for cal in calapp]

        # Assume that there is one vis for all calapps (may need to be modififed in the future)
        self._vis = self._calapp[0].vis
        self._intent = ",".join([cal.intent for cal in self._calapp])

        # Use antenna name rather than ID if possible
        self._ant_ids = ant
        if ant != '':
            ms = self._context.observing_run.get_ms(self._vis)
            domain_antennas = ms.get_antenna(ant)
            idents = [a.name if a.name else a.id for a in domain_antennas]
            ant = ','.join(idents)
        self._ant = ant

        self._figfile = self._get_figfile()

        self._title = "{}".format(os.path.basename(self._vis).split('.')[0])
        if spw:
            self._title += ' spw {}'.format(spw)
        if ant:
            self._title += ' ant {}'.format(', '.join(ant.split(',')))

        # These task_args are the same whether one caltable is plotted
        # on its own, or multiple caltables are overplotted.
        self.task_args = {
            'xaxis': self._xaxis,
            'yaxis': self._yaxis,
            'showgui': False,
            'spw': str(self._spw),
            'antenna': self._ant,
            'correlation': self._correlation,
            'plotrange': self._plotrange,
            'coloraxis': self._coloraxis,
            'title': self._title,
            'clearplots': True}

    def plot(self):
        plots = [self._get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def _get_figfile(self):
        caltable_name = os.path.basename(self._calapp[0].gaintable)

        fileparts = {
            'caltable': caltable_name,
            'x': self._xaxis,
            'y': self._yaxis,
            'spw': '' if self._spw == '' else 'spw%0.2d-' % int(self._spw),
            'ant': '' if self._ant == '' else 'ant%s-' % self._ant.replace(',', '_'),
            'intent': '' if self._intent == '' else '%s-' % self._intent.replace(',', '_'),
            'correlation': '' if self._correlation == '' else '%s-' % self._correlation.replace('/', 'ratio')
        }
        png = '{caltable}-{spw}{ant}{intent}{correlation}{y}_vs_{x}.png'.format(**fileparts)

        # Maximum filename size for Lustre filesystems is 255 bytes. These
        # plots can exceed this limit due to including the names of all
        # antennas. Truncate over-long filename while keeping it unique by
        # replacing it with the hash.
        if len(png) > 251:  # 255 - '.png'
            new_png = '{!s}.png'.format(hash(png))
            LOG.info('Renaming plot to avoid exceeding filesystem limit on filename length.\n'
                     'Old: {!s}\nNew: {!s}'.format(png, new_png))
            png = new_png

        return os.path.join(self._context.report_dir, 'stage%s' % self._result.stage_number, png)

    def _get_plot_wrapper(self):
        tasks = self._create_tasks()
        if not os.path.exists(self._figfile):
            LOG.trace('Creating new plot: %s' % self._figfile)
            try:
                for task in tasks:
                    task.execute()
            except Exception as ex:
                LOG.error('Could not create plot %s' % self._figfile)
                LOG.exception(ex)
                return None

        parameters = {'vis': os.path.basename(self._vis),
                      'caltable': ",".join(self._caltable)}

        for attr in ['spw', 'ant', 'intent']:
            val = getattr(self, '_%s' % attr)
            if val != '':
                parameters[attr] = val

        wrapper = logger.Plot(self._figfile,
                              x_axis=self._xaxis,
                              y_axis=self._yaxis,
                              parameters=parameters,
                              command='; '.join(map(str, tasks)))
        return wrapper

    def _create_tasks(self):
        symbol_array = ['autoscaling', 'diamond', 'square'] # Note: autoscaling can be 'pixel (cross)' or 'circle' depending on number of points.
        task_list = []

        # Create a plotms task for each caltable. See PIPE-1377 and PIPE-1409.
        for n, caltable in enumerate(self._caltable):
            # plotms uses the 'vis' input parameter to specify caltables to plot
            self.task_args['vis'] = caltable
            self.task_args['plotindex'] = n

            # If there are multiple caltables to overplot, clearplots must be False for all
            # but the first plot.
            if n != 0:
                self.task_args['clearplots'] = False

            # Alter plot symbols by cycling through the list of available symbols for each subsequent over-plot.
            self.task_args['symbolshape'] = symbol_array[n % len(symbol_array)]
            self.task_args['customsymbol'] = True

            # The plotfile must be specified for only the last plotms command
            if n == (len(self._caltable) - 1):
                self.task_args['plotfile'] = self._figfile

            task_list.append(casa_tasks.plotms(**self.task_args))

        return task_list


class PlotbandpassLeaf(object):
    """
    Class to execute plotbandpass and return a plot wrapper. It passes the spw
    and ant arguments through to plotbandpass without further manipulation. More
    than one plot may be created though not necessarily returned, as
    plotbandpass may create many plots depending on the input arguments.
    """

    def __init__(self, context, result, calapp, xaxis, yaxis, spw='', ant='', pol='',
                 overlay='', showatm=True):
        self._context = context
        self._result = result

        self._calapp = calapp
        self._caltable = calapp.gaintable
        self._vis = calapp.vis
        ms = self._context.observing_run.get_ms(self._vis)

        self._xaxis = xaxis
        self._yaxis = yaxis

        self._spw = spw
        self._intent = calapp.intent

        # use antenna name rather than ID if possible
        if ant != '':
            domain_antennas = ms.get_antenna(ant)
            idents = [a.name if a.name else a.id for a in domain_antennas]
            ant = ','.join(idents)
        self._ant = ant

        # convert pol ID from integer to string, eg. 0 to XX
        if pol != '':
            dd = ms.get_data_description(spw=int(spw))
            pol = dd.get_polarization_label(pol)
        self._pol = pol

        self._figfile = self._get_figfile()

        # plotbandpass injects antenna name, spw ID and t0 into every plot filename
        root, ext = os.path.splitext(self._figfile)
        # if spw is '', the spw component will be set to the first spw
        if spw == '':
            with casa_tools.TableReader(calapp.gaintable) as tb:
                caltable_spws = set(tb.getcol('SPECTRAL_WINDOW_ID'))
            spw = min(caltable_spws)

        self._pb_figfile = '%s%s%s.t00%s' % (root,
                                             '.%s' % ant if ant else '',
                                             '.spw%0.2d' % spw if spw else '',
                                             ext)

        self._overlay = overlay
        self._showatm = showatm

    def plot(self):
        task = self._create_task()
        plots = [self._get_plot_wrapper(task)]
        return [p for p in plots
                if p is not None
                and os.path.exists(p.abspath)]

    def _get_figfile(self):
        fileparts = {
            'caltable': os.path.basename(self._calapp.gaintable),
            'x': self._xaxis,
            'y': self._yaxis,
            'spw': '' if self._spw == '' else 'spw%s-' % self._spw,
            'ant': '' if self._ant == '' else 'ant%s-' % self._ant.replace(',', '_'),
            'intent': '' if self._intent == '' else '%s-' % self._intent.replace(',', '_'),
            'pol': '' if self._pol == '' else '%s-' % self._pol
        }
        png = '{caltable}-{spw}{pol}{ant}{intent}{y}_vs_{x}.png'.format(**fileparts)

        return os.path.join(self._context.report_dir,
                            'stage%s' % self._result.stage_number,
                            png)

    def _get_plot_wrapper(self, task):
        if not os.path.exists(self._pb_figfile):
            LOG.trace('Creating new plot: %s' % self._pb_figfile)
            try:
                task.execute()
            except Exception as ex:
                LOG.error('Could not create plot %s' % self._pb_figfile)
                LOG.exception(ex)
                return None

        parameters = {'vis': self._vis,
                      'caltable': self._caltable}

        for attr in ['spw', 'ant', 'intent', 'pol']:
            val = getattr(self, '_%s' % attr)
            if val != '':
                parameters[attr] = val

        wrapper = logger.Plot(self._pb_figfile,
                              x_axis=self._xaxis,
                              y_axis=self._yaxis,
                              parameters=parameters,
                              command=str(task))

        return wrapper

    def _create_task(self):
        task_args = {'vis': self._vis,
                     'caltable': self._caltable,
                     'xaxis': self._xaxis,
                     'yaxis': self._yaxis,
                     'antenna': self._ant,
                     'spw': self._spw,
                     'poln': self._pol,
                     'overlay': self._overlay,
                     'figfile': self._figfile,
                     'showatm': self._showatm,
                     'interactive': False,
                     'subplot': 11}

        return casa_tasks.plotbandpass(**task_args)


class LeafComposite(object):
    """
    Base class to hold multiple PlotLeafs, thus generating multiple plots when
    plot() is called.
    """

    def __init__(self, children):
        self._children = children

    def plot(self):
        plots = []
        for child in self._children:
            plots.extend(child.plot())
        return [p for p in plots if p is not None]

    def _create_calapp_contents_dict(self, calapps : List[callibrary.CalApplication], column_name: str) -> Dict[int, List[callibrary.CalApplication]]:
        """
        Creates and returns a dictionary mapping some element (e.g. spw, ant) specified by the input
        column_name to lists of the input calapps that have that element present in their caltables.

        e.g if the column_name is 'ANTENNA1', this funtion will return a dict where the keys are
        all antenna numbers present in any of the input calapps' caltables. For each antenna number key,
        the value is a list of all the input calapps with caltables with that antenna.
        """
        dict_calapp = collections.defaultdict(list)
        for cal in calapps:
            with casa_tools.TableReader(cal.gaintable) as tb:
                for elt in set(tb.getcol(column_name)):
                    if cal not in dict_calapp[elt]:
                        dict_calapp[elt].append(cal)
        return dict_calapp


class PolComposite(LeafComposite):
    """
    Create a PlotLeaf for each polarization in the caltable.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, result, calapp, xaxis, yaxis, ant='', spw='',
                 **kwargs):
        # the number of polarisations for a spw may not be equal to the number
        # of shape of the column. For example, X403 has XX,YY for some spws
        # but XX for the science data. If we're given a spw argument we can
        # bypass the calls for the missing polarisation.
        if spw != '':
            vis = calapp.vis
            ms = context.observing_run.get_ms(vis)

            dd = ms.get_data_description(spw=int(spw))
            num_pols = dd.num_polarizations

        else:
            num_pols = utils.get_num_caltable_polarizations(calapp.gaintable)

        children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                                    spw=spw, ant=ant, pol=pol, **kwargs)
                    for pol in range(num_pols)]
        super(PolComposite, self).__init__(children)


class SpwComposite(LeafComposite):
    """
    Create a PlotLeaf for each spw in the caltable or caltables.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, result, calapp: Union[List[callibrary.CalApplication], callibrary.CalApplication],
                xaxis, yaxis, ant='', pol='', **kwargs):

        if isinstance(calapp, list):
            # Create a dictionary to keep track of which caltables have which spws.
            dict_calapp_spws = self._create_calapp_contents_dict(calapp, 'SPECTRAL_WINDOW_ID')
            table_spws = sorted(dict_calapp_spws.keys())

            # In the following call, dict_calapp_spw[spw] is a list of calapps with that spw present
            children = [self.leaf_class(context, result, dict_calapp_spws[spw], xaxis, yaxis,
                        spw=int(spw), ant=ant, pol=pol, **kwargs)
                        for spw in table_spws]
        else:
            # Identify spws in caltable
            with casa_tools.TableReader(calapp.gaintable) as tb:
                table_spws = sorted(set(tb.getcol('SPECTRAL_WINDOW_ID')))

            children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                                spw=int(spw), ant=ant, pol=pol, **kwargs)
                                for spw in table_spws]

        super().__init__(children)


class SpwAntComposite(LeafComposite):
    """
    Create a PlotLeaf for each spw and antenna in the caltable or caltables.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, result, calapp : Union[List[callibrary.CalApplication], callibrary.CalApplication],
                xaxis, yaxis, pol='', ysamescale=False, **kwargs):
        # Support for lists of calapps was added for PIPE-1409 and PIPE-1377.
        if isinstance(calapp, list):
            # Create a dictionary to keep track of which caltables have which spws.
            dict_calapp_spws = self._create_calapp_contents_dict(calapp, 'SPECTRAL_WINDOW_ID')
            table_spws = sorted(dict_calapp_spws.keys())

            # PIPE-66: if requested, and no explicit (non-empty) plotrange was
            # set, then use the same y-scale for plots of the same spw.
            # TODO: in the future, this could potentially be refactored to use
            # the yselfscale parameter in PlotMS together with "iteraxis", so as
            # to let PlotMS take care of setting the same y-range for a set of
            # plots. Would also need infrastructure.utils.framework.plotms_iterate.
            update_yscale = ysamescale and not kwargs.get("plotrange", "")

            children = []
            for spw in table_spws:
                if update_yscale:
                # If a list of calapps is input, get the ymin and ymax for all the caltables with this spw.
                    ymins = []
                    ymaxes = []
                    for cal in dict_calapp_spws[spw]:
                        caltable_wrapper = CaltableWrapperFactory.from_caltable(cal.gaintable, gaincalamp=True)
                        filtered = caltable_wrapper.filter(spw=[int(spw)])
                        # Save the ymin and ymax values rather than the full filtered.data as that could get large
                        ymins.append(numpy.ma.min(numpy.abs(filtered.data)))
                        ymaxes.append(numpy.ma.max(numpy.abs(filtered.data)))

                    ymin = numpy.ma.min(ymins)
                    ymax = numpy.ma.max(ymaxes)

                    yrange = ymax - ymin
                    ymin = ymin - 0.05 * yrange
                    ymax = ymax + 0.05 * yrange

                    kwargs.update({"plotrange": [0, 0, ymin, ymax]})

                # In the following call, dict_calapp_spw[spw] is the list of calapps with that spw
                children.append(
                    self.leaf_class(context, result, dict_calapp_spws[spw], xaxis, yaxis, spw=int(spw), pol=pol, **kwargs))
        else:
            # Identify spws in caltable
            with casa_tools.TableReader(calapp.gaintable) as tb:
                table_spws = set(tb.getcol('SPECTRAL_WINDOW_ID'))

            caltable_spws = sorted([int(spw) for spw in table_spws])

            # PIPE-66: if requested, and no explicit (non-empty) plotrange was
            # set, then use the same y-scale for plots of the same spw.
            # TODO: in the future, this could potentially be refactored to use
            # the yselfscale parameter in PlotMS together with "iteraxis", so as
            # to let PlotMS take care of setting the same y-range for a set of
            # plots. Would also need infrastructure.utils.framework.plotms_iterate.
            update_yscale = ysamescale and not kwargs.get("plotrange", "")

            children = []
            for spw in caltable_spws:
                if update_yscale:
                    caltable_wrapper = CaltableWrapperFactory.from_caltable(calapp.gaintable, gaincalamp=True)
                    filtered = caltable_wrapper.filter(spw=[spw])
                    ymin = numpy.ma.min(numpy.abs(filtered.data))
                    ymax = numpy.ma.max(numpy.abs(filtered.data))

                    yrange = ymax - ymin
                    ymin = ymin - 0.05 * yrange
                    ymax = ymax + 0.05 * yrange

                    kwargs.update({"plotrange": [0, 0, ymin, ymax]})

                children.append(
                    self.leaf_class(context, result, calapp, xaxis, yaxis, spw=spw, pol=pol, **kwargs))

        super().__init__(children)


class AntComposite(LeafComposite):
    """
    Create a PlotLeaf for each antenna in the caltable or caltables.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, result, calapp : Union[List[callibrary.CalApplication], callibrary.CalApplication],
                xaxis, yaxis, spw='', pol='', **kwargs):
        if isinstance(calapp, list):
            # Create a dictionary to keep track of which caltables have which ants.
            dict_calapp_ants = self._create_calapp_contents_dict(calapp, 'ANTENNA1')
            table_ants = sorted(dict_calapp_ants.keys())

            # In the following call dict_calapp_ants[ant] is the list of calapps with antenna=ant present
            children = [self.leaf_class(context, result, dict_calapp_ants[ant], xaxis, yaxis,
                        ant=int(ant), spw=spw, pol=pol, **kwargs)
                        for ant in table_ants]
        else:
            # Identify ants in caltable
            with casa_tools.TableReader(calapp.gaintable) as tb:
                table_ants = sorted(set(tb.getcol('ANTENNA1')))

            children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                        ant=int(ant), spw=spw, pol=pol, **kwargs)
                        for ant in table_ants]
        super().__init__(children)


class AntSpwComposite(LeafComposite):
    """
    Create a PlotLeaf for each spw and antenna in the caltable.
    """
    leaf_class = None

    def __init__(self, context, result, calapp, xaxis, yaxis, pol='', **kwargs):
        with casa_tools.TableReader(calapp.gaintable) as tb:
            table_ants = set(tb.getcol('ANTENNA1'))

        caltable_antennas = [int(ant) for ant in table_ants]
        children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                                    ant=ant, pol=pol, **kwargs)
                    for ant in caltable_antennas]
        super(AntSpwComposite, self).__init__(children)


class SpwPolComposite(LeafComposite):
    """
    Create a PlotLeaf for each spw and polarization in the caltable.
    """
    leaf_class = None

    def __init__(self, context, result, calapp, xaxis, yaxis, ant='', **kwargs):
        with casa_tools.TableReader(calapp.gaintable) as tb:
            table_spws = set(tb.getcol('SPECTRAL_WINDOW_ID'))

        caltable_spws = [int(spw) for spw in table_spws]
        children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                                    spw=spw, ant=ant, **kwargs)
                    for spw in caltable_spws]
        super(SpwPolComposite, self).__init__(children)


class AntSpwPolComposite(LeafComposite):
    """
    Create a PlotLeaf for each antenna, spw, and polarization in the caltable.
    """
    leaf_class = None

    def __init__(self, context, result, calapp, xaxis, yaxis, **kwargs):
        with casa_tools.TableReader(calapp.gaintable) as tb:
            table_ants = set(tb.getcol('ANTENNA1'))

        caltable_antennas = [int(ant) for ant in table_ants]
        children = [self.leaf_class(context, result, calapp, xaxis, yaxis,
                                    ant=ant, **kwargs)
                    for ant in caltable_antennas]
        super(AntSpwPolComposite, self).__init__(children)


class PlotmsCalAntComposite(AntComposite):
    leaf_class = PlotmsCalLeaf


class PlotmsCalSpwComposite(SpwComposite):
    leaf_class = PlotmsCalLeaf


class PlotmsCalAntSpwComposite(AntSpwComposite):
    leaf_class = PlotmsCalSpwComposite


class PlotmsCalSpwAntComposite(SpwAntComposite):
    leaf_class = PlotmsCalAntComposite


class PlotbandpassAntComposite(AntComposite):
    leaf_class = PlotbandpassLeaf


class PlotbandpassSpwComposite(SpwComposite):
    leaf_class = PlotbandpassLeaf


class PlotbandpassPolComposite(PolComposite):
    leaf_class = PlotbandpassLeaf


class PlotbandpassAntSpwComposite(AntSpwComposite):
    leaf_class = PlotbandpassSpwComposite


class PlotbandpassSpwPolComposite(SpwPolComposite):
    leaf_class = PlotbandpassPolComposite


class PlotbandpassAntSpwPolComposite(AntSpwPolComposite):
    leaf_class = PlotbandpassSpwPolComposite


class CaltableWrapperFactory(object):
    @staticmethod
    def from_caltable(filename, gaincalamp=False):
        LOG.trace('CaltableWrapperFactory.from_caltable(%r)', filename)
        with casa_tools.TableReader(filename) as tb:
            viscal = tb.getkeyword('VisCal')
            caltype = callibrary.CalFrom.get_caltype_for_viscal(viscal)
        if caltype == 'gaincal':
            return CaltableWrapperFactory.create_gaincal_wrapper(filename, gaincalamp)
        if caltype == 'tsys':
            return CaltableWrapperFactory.create_param_wrapper(filename, 'FPARAM')
        if caltype == 'bandpass':
            return CaltableWrapperFactory.create_param_wrapper(filename, 'CPARAM')
        if caltype in ('ps', 'otf', 'otfraster',):
            return CaltableWrapperFactory.create_param_wrapper(filename, 'FPARAM')
        raise NotImplementedError('Unhandled caltype: %s' % viscal)

    @staticmethod
    def create_gaincal_wrapper(path, gaincalamp=False):
        with casa_tools.TableReader(path) as tb:
            time_mjd = tb.getcol('TIME')
            antenna1 = tb.getcol('ANTENNA1')
            spw = tb.getcol('SPECTRAL_WINDOW_ID')
            scan = tb.getcol('SCAN_NUMBER')
            flag = tb.getcol('FLAG').swapaxes(0, 2).swapaxes(1, 2).squeeze(2)
            gain = tb.getcol('CPARAM').swapaxes(0, 2).swapaxes(1, 2).squeeze(2)

            # convert MJD times stored in caltable to matplotlib equivalent
            time_unix = utils.mjd_seconds_to_datetime(time_mjd)
            time_matplotlib = matplotlib.dates.date2num(time_unix)

            # If requested, return the gain amplitudes rather than the phases.
            if gaincalamp:
                data = numpy.ma.MaskedArray(gain, mask=flag)
            else:
                phase = numpy.arctan2(numpy.imag(gain), numpy.real(gain)) * 180.0 / numpy.pi
                data = numpy.ma.MaskedArray(phase, mask=flag)

            return CaltableWrapper(path, data, time_matplotlib, antenna1, spw, scan)

    @staticmethod
    def create_param_wrapper(path, param):
        with casa_tools.TableReader(path) as tb:
            time_mjd = tb.getcol('TIME')
            antenna1 = tb.getcol('ANTENNA1')
            spw = tb.getcol('SPECTRAL_WINDOW_ID')
            scan = tb.getcol('SCAN_NUMBER')

            # convert MJD times stored in caltable to matplotlib equivalent
            time_unix = utils.mjd_seconds_to_datetime(time_mjd)
            time_matplotlib = matplotlib.dates.date2num(time_unix)

            # results in a list of numpy arrays, one for each row in the
            # caltable. The shape of each numpy array is number of
            # correlations, number of channels, number of values for that
            # correlation/channel combination - which is always 1. Squeeze out
            # the unnecessary dimension and swap the channel and correlation
            # axes.
            data_col = tb.getvarcol(param)
            row_data = [data_col['r%s' % (k + 1)].swapaxes(0, 1).squeeze(2)
                        for k in range(len(data_col))]

            flag_col = tb.getvarcol('FLAG')
            row_flag = [flag_col['r%s' % (k + 1)].swapaxes(0, 1).squeeze(2)
                        for k in range(len(flag_col))]

            # PIPE-1706: explicitly pass dtype=object to create "object" array and allow
            # individual elements to have different shapes.
            data = numpy.asarray([numpy.ma.MaskedArray(d, mask=f)
                                  for (d, f) in zip(row_data, row_flag)], dtype=object)

            return CaltableWrapper(path, data, time_matplotlib, antenna1, spw,
                                   scan)


class CaltableWrapper(object):
    @staticmethod
    def from_caltable(filename):
        return CaltableWrapperFactory.from_caltable(filename)

    def __init__(self, filename, data, time, antenna, spw, scan):
        # tag the extra metadata columns onto our data array
        self.filename = filename
        self.data = data
        self.time = time
        self.antenna = antenna
        self.spw = spw
        self.scan = scan

        # get list of which spws, antennas and scans we have data for
        self._spws = frozenset(spw)
        self._antennas = frozenset(antenna)
        self._scans = frozenset(scan)

    def _get_mask(self, allowed, data):
        mask = numpy.zeros_like(data)
        for a in allowed:
            if a not in data:
                raise KeyError('%s is not in caltable data' % a)
            mask = (mask == 1) | (data == a)
        return mask

    def filter(self, spw=None, antenna=None, scan=None):
        # LOG.trace('filter(spw=%s, antenna=%s, scan=%s)' % (spw, antenna, scan))
        if spw is None:
            spw = self._spws
        if antenna is None:
            antenna = self._antennas
        if scan is None:
            scan = self._scans

        # get data selection mask for each selection parameter
        antenna_mask = self._get_mask(antenna, self.antenna)
        spw_mask = self._get_mask(spw, self.spw)
        scan_mask = self._get_mask(scan, self.scan)

        # combine masks to create final data selection mask
        mask = (antenna_mask == 1) & (spw_mask == 1) & (scan_mask == 1)

        # find data for the selection mask
        data = self.data[mask]
        time = self.time[mask]
        antenna = self.antenna[mask]
        spw = self.spw[mask]
        scan = self.scan[mask]

        # create new object for the filtered data
        return CaltableWrapper(self.filename, data, time, antenna, spw, scan)


class PhaseVsBaselineData(object):
    def __init__(self, data, ms: MeasurementSet, corr_id, refant_id):
        # While it is possible to do so, we shouldn't calculate statistics for
        # mixed antennas/spws/scans.
        if len(set(data.antenna)) == 0:
            raise ValueError('No antennas defined in data selection')
        if len(set(data.spw)) == 0:
            raise ValueError('No spw defined in data selection')
        if len(set(data.antenna)) > 1:
            raise ValueError('Data slice contains multiple antennas. Got %s' % data.antenna)
        if len(set(data.spw)) > 1:
            raise ValueError('Data slice contains multiple spws. Got %s' % data.spw)
        #        assert len(set(data.scan)) is 1, 'Data slice contains multiple scans'

        self.data = data
        self.data_for_corr = self.data.data[:, corr_id]
        if len(self.data_for_corr) == 0:
            raise ValueError('No data for spw %s ant %s scan %s' % (data.spw[0],
                                                                    data.antenna[0],
                                                                    data.scan))

        self.ms = ms
        self.corr = corr_id
        self.refant = int(refant_id)

        self.baselines = self.ms.antenna_array.baselines_for_antennas(data.antenna)
        self.median_baseline = numpy.median(self.baselines)
        self.mean_baseline = numpy.median(self.baselines)

        this_antenna_id = int(self.data.antenna[0])
        self.distance_to_refant = self.ms.antenna_array.baseline_lookup[refant_id][this_antenna_id]

        # backing for on-demand properties. It is very likely that these could be
        # made simple instance properties but they are kept on-demand to avoid
        # introducing additional risk into PIPE-1823.
        self._unwrapped_data = None
        self._offsets_from_median = None
        self._rms_offset = None
        self._unwrapped_rms = None
        self._median_offset = None

    def _safe_rms(self, x: numpy.ma.array, calculation: str):
        """
        Safely calculate the RMS of a numpy masked array, logging any
        error and skipping to the next value if an error occurs.
        """
        def rms():
            return numpy.ma.sqrt(numpy.ma.mean(x ** 2))

        def err_handler(t, _):
            ant = set(self.data.antenna).pop()
            spw = set(self.data.spw).pop()
            scan = set(self.data.scan).pop()
            LOG.warning('Floating point error (%s) calculating %s for'
                        ' Scan %s Spw %s Ant %s.' % (t, calculation, scan, spw, ant))

        try:
            return rms()
        except FloatingPointError:
            saved_handler = numpy.seterrcall(err_handler(calculation))
            saved_err = numpy.seterr(all='call')
            try:
                return rms()
            finally:
                numpy.seterrcall(saved_handler)
                numpy.seterr(**saved_err)

    @property
    def antenna(self):
        return self.data.antenna

    @property
    def scan(self):
        return self.data.scan

    @property
    def spw(self):
        return self.data.spw[0]

    @property
    def num_corr_axes(self):
        return len(self.data.data.shape[1])

    @property
    def unwrapped_data(self):
        if self._unwrapped_data is None:
            rads = numpy.deg2rad(self.data_for_corr)
            unwrapped_rads = numpy.unwrap(rads)
            unwrapped_degs = numpy.rad2deg(unwrapped_rads)
            # the operation above removed the mask, so add it back.
            remasked = numpy.ma.MaskedArray(unwrapped_degs, mask=self.data_for_corr.mask)
            self._unwrapped_data = remasked

        return self._unwrapped_data

    @property
    def offsets_from_median(self):
        if self._offsets_from_median is None:
            unwrapped_degs = self.unwrapped_data
            deg_offsets = unwrapped_degs - numpy.ma.median(unwrapped_degs)
            # the operation above removed the mask, so add it back.
            remasked = numpy.ma.MaskedArray(deg_offsets, mask=self.data_for_corr.mask)
            self._offsets_from_median = remasked

        return self._offsets_from_median

    @property
    def rms_offset(self):
        if self._rms_offset is None:
            self._rms_offset = self._safe_rms(self.offsets_from_median, "RMS offset")

        return self._rms_offset

    @property
    def unwrapped_rms(self):
        if self._unwrapped_rms is None:
            self._unwrapped_rms = self._safe_rms(self.unwrapped_data, "unwrapped RMS")

        return self._unwrapped_rms

    @property
    def median_offset(self):
        if self._median_offset is None:
            abs_offset = numpy.ma.abs(self.offsets_from_median)
            self._median_offset = numpy.ma.median(abs_offset)
        return self._median_offset


class XYData(object):
    def __init__(self, delegate, x_axis, y_axis):
        self.__delegate = delegate
        self.__x_axis = x_axis
        self.__y_axis = y_axis

    @property
    def antenna(self):
        return self.__delegate.antenna

    @property
    def corr(self):
        return self.__delegate.corr

    @property
    def data(self):
        return self.__delegate.data

    @property
    def ratio(self):
        return self.__delegate.ratio

    @property
    def scan(self):
        return self.__delegate.scan

    @property
    def spw(self):
        return self.__delegate.spw

    @property
    def x(self):
        return getattr(self.__delegate, self.__x_axis)

    @property
    def y(self):
        return getattr(self.__delegate, self.__y_axis)


class DataRatio(object):
    def __init__(self, before, after):
        # test symmetric differences to find data selection errors
        if set(before.antenna) ^ set(after.antenna):
            raise ValueError('Data slices are for different antennas')
        if set(before.scan) ^ set(after.scan):
            raise ValueError('Data slices are for different scans')
        if before.spw != after.spw:
            raise ValueError('Data slices are for different spws')
        if before.corr != after.corr:
            raise ValueError('Data slices are for different correlations')

        self.__before = before
        self.__after = after

        self.__antennas = frozenset(before.data.antenna).union(set(after.data.antenna))
        self.__spws = frozenset(before.data.spw).union(set(after.data.spw))
        self.__scans = frozenset(before.data.scan).union(set(after.data.scan))
        self.__corr = frozenset((before.corr, after.corr))

        self._cache = cachetools.LRUCache(maxsize=100)

    @property
    def after(self):
        return self.__after

    @property
    def antennas(self):
        return self.__antennas

    @property
    def before(self):
        return self.__before

    @property
    def corr(self):
        return self.__corr

    @property
    def scans(self):
        return self.__scans

    @property
    def spws(self):
        return self.__spws

    @property
    def x(self):
        assert (self.__before.x == self.__after.x)
        return self.__before.x

    @property
    def num_corr_axes(self):
        # having ensured the before/after data are for the same scan and spw,
        # they should have the same number of correlations
        return self.__before.data.data.shape[1]

    @property
    @cachetools.cachedmethod(operator.attrgetter('_cache'))
    def y(self):
        before = self.__before.y
        after = self.__after.y

        if None in (before, after):
            return None
        # avoid divide by zero
        if after == 0:
            return None
        return before / after


class NullScoreFinder(object):
    def get_score(self, *args, **kwargs):
        return None

