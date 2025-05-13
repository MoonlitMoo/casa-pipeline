import collections
import os
from typing import Dict, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure.renderer.htmlrenderer import is_singledish_ms
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.callibrary import CalTo
from . import common

LOG = infrastructure.get_logger(__name__)


class PlotmsLeaf(object):
    """
    Class to execute plotms and return a plot wrapper. It passes the spw and
    ant arguments through to plotms without further manipulation, creating
    exactly one plot.
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, spw='', ant='', field='', scan='', intent='',
                 uvrange='', correlation='', suffix='', **plot_args):

        self._context = context
        self._output_dir = output_dir
        self._suffix = suffix

        self._vis = calto.vis
        self._ms = context.observing_run.get_ms(self._vis)

        self._xaxis = xaxis
        self._yaxis = yaxis

        self._spw = spw
        self._intent = intent
        self._uvrange = uvrange

        # TODO
        # This should be revisited when the confusion between correlation and
        # polarisation is ironed out. I'm not convinced that
        # get_alma_corrstring does anything useful; I suspect an empty string
        # forcing all correlations to be plotted would be equivalent.
        if correlation == '':
            observatory = self._ms.antenna_array.name
            if observatory in ('ALMA', 'NRO'):
                correlation = self._ms.get_alma_corrstring()
            elif observatory in ('VLA', 'EVLA'):
                correlation = self._ms.get_vla_corrstring()
            else:
                # New observatories: you could probably use '' but I can't
                # guarantee it, hence the error is raised
                raise NotImplementedError('Could not expand correlation for observatory: {!s}'.format(observatory))
        self._correlation = correlation

        #         # convert intent to scan selection
        #         if intent != '':
        #             domain_fields = self._ms.get_fields(field)
        #             domain_spws = self._ms.get_spectral_windows(spw)
        #             scans = [s for s in self._ms.get_scans(scan_intent=intent)
        #                      if s.fields.intersection(domain_fields)
        #                      and s.spws.intersection(domain_spws)]
        #             scan = ','.join([str(s.id) for s in scans])
        self._scan = scan

        # use field name rather than ID where possible
        field_label = ''
        if field != '':
            matching = self._ms.get_fields(field)

            if len(matching) == 1:
                field_name = matching[0].name
                field_id = matching[0].id
                # in case the arg is an ID, check that returned field doesn't
                # contain a duplicate name
                matching_by_name = self._ms.get_fields(field_name)
                if len(matching_by_name) == 1:
                    # if the argument was the field ID but the field name is
                    # unique. Therefore, we can use the field name.
                    field = field_name
                    field_label = field_name
                    LOG.trace('One field found for search term %r and name '
                              'appears unique. Using %r as field identifier',
                              field, field_label)
                else:
                    # if the argument was the field ID but the field name is
                    # not unique, prepend the ID with the field name
                    field_label = '%s (field #%s)' % (field_name, field_id)
                    LOG.trace('One field found for search term %r but name '
                              'also refers to other fields. Using %r as field '
                              'identifier', field, field_label)
            else:
                LOG.trace('More than one field found for search term %r. '
                          'Using %r as field identifier', field, field)
                field_label = field

        self._field = str(field)
        self._field_label = str(field_label)

        # use antenna name rather than ID if possible
        if ant != '':
            domain_antennas = self._ms.get_antenna(ant)
            idents = [a.name if a.name else a.id for a in domain_antennas]
            ant = ','.join(idents)
        self._ant = ant

        # get baseband from extra kwargs, deleting it after copying so it
        # doesn't affect the plotms call
        self._baseband = plot_args.get('baseband', '')
        if self._baseband:
            del plot_args['baseband']

        # same procedure for receiver
        self._receiver = plot_args.get('receiver', '')
        if self._receiver:
            del plot_args['receiver']

        #
        # PIPE-157: pipeline hif_applycal spectral plots should overlay image
        # sideband transmission for DSB receivers (once plotms can do this)
        #
        # if showatm=True and spw is a DSB receiver, add showimage=True to the
        # plotms args
        #
        if plot_args.get('showatm', False):
            # use any rather than all on the basis that we'd prefer a warning
            # if plotms doesn't like mixed DSB/non-DSB plots over not getting
            # the sideband image at all
            if any([spw_do.receiver == 'DSB' for spw_do in self._ms.get_spectral_windows(spw)]):
                plot_args['showimage'] = True

        self._plot_args = plot_args
        self._plotfile = self._get_plotfile()

    def _is_null_select(self):
        """Attempt to identify a null data selection to prevent CASA/plotms grpc-related exceptions.

        Note: this doesn't guarantee to detect all null-selection conditions, but can rule out several known
        scenarios. See PIPE-1596/PIPE-1259/PIPE-1255.
        """
        is_null_select = False

        # reject the case that the spw/scan/intent selection leads to zero scan.
        if not self._ms.get_scans(spw=self._spw, field=self._field, scan_intent=self._intent):
            is_null_select = True

        # reject the case that the requested correlation(s) is not in any selected spw, only for VLA
        corrstring = self._correlation
        if self._ms.antenna_array.name in ('VLA', 'EVLA') and corrstring not in ('', '*', None):
            vla_corrlist = self._ms.get_vla_corrlist_from_spw(spw=str(self._spw))
            if not (set(vla_corrlist) & set(corrstring.split(','))):
                is_null_select = True

        return is_null_select

    def plot(self):
        """Get the jobrequest and its plot wrapper ready, before the plot is generated."""
        if self._is_null_select():
            return []
        task = self._get_plot_task()
        return [(task, self._get_plot_wrapper(task))]

    def _get_plotfile(self):
        fileparts = {
            'vis': os.path.basename(self._vis),
            'x': self._xaxis,
            'y': self._yaxis,
            'ant': '' if self._ant == '' else 'ant%s-' % self._ant.replace(',', '_'),
            'field': '' if self._field_label == '' else '-%s' % self._field_label.replace(',', '_'),
            'intent': '' if self._intent == '' else '%s-' % self._intent.replace(',', '_'),
            'uvrange': '' if self._uvrange == '' else 'uvrange%s-' % self._uvrange.replace(',', '_'),
            'receiver': '' if self._receiver == '' else '%s-' % self._receiver.replace(',', '_'),
            'suffix': '' if self._suffix == '' else '-%s' % self._suffix,
            'correlation': '' if self._correlation == '' else '-%s' % self._correlation
        }

        if self._spw == '':
            fileparts['spw'] = ''
        else:
            # format spws for filename sorting
            spws = ['%0.2d' % int(spw) for spw in str(self._spw).split(',')]
            fileparts['spw'] = 'spw%s-' % '_'.join(spws)

        if self._baseband:
            fileparts['spw'] = 'bb%s-' % self._baseband

        png = '{vis}{field}-{receiver}{spw}{ant}{intent}{uvrange}{y}_vs_{x}{suffix}{correlation}.png'.format(**fileparts)
        png = filenamer.sanitize(png)

        # Maximum filename size for Lustre filesystems is 255 bytes. Mosaics
        # can exceed this limit due to including the names of all the field.
        # Truncate over-long field components while keeping them unique by
        # replacing them with the hash of the component
        if len(png) > 251:  # 255 - '.png'
            new_png = '{!s}.png'.format(hash(png))
            LOG.info('Renaming plot to avoid exceeding filesystem limit on filename length.\n'
                     'Old: {!s}\nNew: {!s}'.format(png, new_png))
            png = new_png

        return os.path.join(self._output_dir, png)

    def _get_plot_wrapper(self, task):
        parameters = {'vis': os.path.basename(self._vis)}

        domain_spws = self._ms.get_spectral_windows(self._spw)
        parameters['receiver'] = sorted(set(spw.band for spw in domain_spws))

        if self._field != '':
            parameters['field'] = self._field_label

        if self._baseband != '':
            parameters['baseband'] = self._baseband

        if self._correlation != '':
            parameters['correlation'] = self._correlation

        for attr in ['spw', 'ant', 'intent', 'scan']:
            val = getattr(self, '_%s' % attr)
            if val != '':
                parameters[attr] = val

        wrapper = logger.Plot(self._plotfile,
                              x_axis=self._xaxis,
                              y_axis=self._yaxis,
                              parameters=parameters,
                              command=str(task))

        return wrapper

    def _get_plot_task(self):
        casa_intent = utils.to_CASA_intent(self._ms, self._intent)
        task_args = {
            'vis': self._ms.name,
            'xaxis': self._xaxis,
            'yaxis': self._yaxis,
            'field': str(self._field),
            'spw': str(self._spw),
            'scan': str(self._scan),
            'intent': casa_intent,
            'antenna': self._ant,
            'uvrange': self._uvrange,
            'correlation': self._correlation,
            'plotfile': self._plotfile,
            'clearplots': True,
            'showgui': False
        }

        task_args.update(**self._plot_args)

        task = casa_tasks.plotms(**task_args)
        return task


class SpwComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each spw in the applied calibration.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, ant='', field='', intent='', **kwargs):
        ms = context.observing_run.get_ms(calto.vis)
        children = []
        for spw in ms.get_spectral_windows(calto.spw):
            # For PIPE-690 it was requested to create plots only for a certain spw/field
            # selection. This is accomplished by passing a dictionary to the field
            # override parameter.
            if isinstance(field, dict):
                field_spec = field.get(spw.id, None)
                if field_spec is None:
                    continue
            else:
                field_spec = field
            # only create plots for scans with data
            scans = ms.get_scans(spw=spw.id, field=field_spec, scan_intent=intent)
            if not scans:
                continue

            kwargs_copy = dict(kwargs)
            kwargs_copy['avgchannel'] = kwargs.get('avgchannel', str(spw.num_channels))

            leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis, spw=spw.id, ant=ant, field=field_spec,
                        intent=intent, **kwargs_copy)
            children.append(leaf_obj)

        super(SpwComposite, self).__init__(children)


class BasebandComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each baseband in the applied calibration.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, ant='', field='', intent='', overplot_receivers=False,
                 **kwargs):

        ms = context.observing_run.get_ms(calto.vis)

        receivers = collections.defaultdict(lambda: utils.OrderedDefaultdict(list))

        for spw in ms.get_spectral_windows(calto.spw):
            if intent != '':
                wanted = set(intent.split(','))
                if spw.intents.isdisjoint(wanted):
                    continue

            rx = 'all' if overplot_receivers else spw.band
            receivers[rx][spw.baseband].append(spw.id)

        is_single_receiver = len(receivers) == 1

        children = []
        if 'spws' in kwargs:
            source_spws = kwargs['spws']
            del kwargs['spws']
        else:
            source_spws = ''

        for receiver_id, basebands in receivers.items():
            # keep receiver component out of filename if possible
            if overplot_receivers or is_single_receiver:
                receiver_id = ''

            for baseband_id, spw_ids in basebands.items():
                # spws = ','.join([str(i) for i in spw_ids])
                if source_spws:
                    # Determine valid spws
                    source_spwidlist_str = source_spws.split(',')
                    source_spwidlist = [int(spwstr) for spwstr in source_spwidlist_str]
                    spws = ','.join([str(spwid) for spwid in spw_ids if spwid in source_spwidlist])
                    if spws:
                        LOG.debug('Keyword override for %s vs %s plot: spws=%s' % (yaxis, xaxis, spws))
                else:
                    spws = ','.join([str(i) for i in spw_ids])

                if spws:
                    leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis, spw=spws, ant=ant, field=field,
                                               intent=intent, baseband=str(baseband_id), receiver=receiver_id, **kwargs)
                    children.append(leaf_obj)

        super(BasebandComposite, self).__init__(children)


class AntComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each antenna in the applied calibration.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, spw='', field='', intent='', **kwargs):
        # set the singledish flag for SD runs
        self.singledish = is_singledish_ms( context )

        ms = context.observing_run.get_ms(calto.vis)
        antennas = [int(a.id) for a in ms.get_antenna(calto.antenna)]

        children = [self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                    ant=ant, spw=spw, field=field, intent=intent, **kwargs)
                    for ant in antennas]
        super(AntComposite, self).__init__(children)


class FieldComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each field in the applied calibration.
    """
    # reference to the PlotLeaf class to call
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, spw='', ant='',
                 intent='', **kwargs):
        ms = context.observing_run.get_ms(calto.vis)

        children = []
        for field in ms.get_fields(calto.field):
            wanted = set(intent.split(','))
            intersection = field.intents.intersection(wanted)
            if not intersection:
                continue

            leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                       ant=ant, spw=spw,
                                       intent=','.join(intersection),
                                       field=str(field.id), **kwargs)
            children.append(leaf_obj)

        super(FieldComposite, self).__init__(children)


class FieldSpwComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each field and spw in the applied calibration
    """
    leaf_class = None
    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent='', **kwargs):
        ms = context.observing_run.get_ms(calto.vis)

        wanted = set(intent.split(','))
        children = []
        # Add override for kwargs
        if 'field' in kwargs and kwargs['field'] != '':
            fields = kwargs['field']
            LOG.debug('Override for %s vs %s plot: fields=%s' % (yaxis, xaxis, fields))
            del kwargs['field']

            fields = fields.split(',')
            for field in fields:
                leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                intent=intent,
                                field=field, **kwargs)
                children.append(leaf_obj)
        else:
            fields = ms.get_fields(calto.field)

            for field in fields:
                intersection = field.intents.intersection(wanted)
                if not intersection:
                    continue

                leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                       intent=','.join(intersection),
                                       field=str(field.id), **kwargs)
                children.append(leaf_obj)

        super(FieldSpwComposite, self).__init__(children)


class SpwAntComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each spw and antenna in the applied calibration
    """
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent='', **kwargs):
        ms = context.observing_run.get_ms(calto.vis)

        children = []
        for spw in ms.get_spectral_windows(calto.spw):
            leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                       intent=intent, spw=spw.id,
                                       **kwargs)
            children.append(leaf_obj)

        super(SpwAntComposite, self).__init__(children)


class FieldSpwAntComposite(common.LeafComposite):
    """
    Create a PlotLeaf for each field, spw and antenna in the applied calibration
    """
    leaf_class = None

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent='', **kwargs):
        ms = context.observing_run.get_ms(calto.vis)

        wanted = set(intent.split(','))
        children = []
        for field in ms.get_fields(calto.field):
            intersection = field.intents.intersection(wanted)
            if not intersection:
                continue
            leaf_obj = self.leaf_class(context, output_dir, calto, xaxis, yaxis,
                                       intent=','.join(intersection),
                                       field=str(field.id), **kwargs)
            children.append(leaf_obj)

        super(FieldSpwAntComposite, self).__init__(children)


class PlotmsAntComposite(AntComposite):
    leaf_class = PlotmsLeaf

    def plot(self):
        # set singledish flag
        singledish = getattr(self, "singledish", False)

        # merge separate ant jobs into one job using plotms iterator
        jobs_and_wrappers = super(PlotmsAntComposite, self).plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers, 'antenna', singledish=singledish)
        return successful_wrappers


class PlotmsSpwComposite(SpwComposite):
    leaf_class = PlotmsLeaf

    def plot(self):
        # merge separate spw jobs into one job using plotms iterator
        jobs_and_wrappers = super(PlotmsSpwComposite, self).plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers, 'spw')
        return successful_wrappers


class PlotmsBasebandComposite(BasebandComposite):
    leaf_class = PlotmsLeaf

    def plot(self):
        # merge separate spw jobs into one job using plotms iterator
        jobs_and_wrappers = super(PlotmsBasebandComposite, self).plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers, 'spw')
        return successful_wrappers


class PlotmsFieldComposite(FieldComposite):
    leaf_class = PlotmsLeaf

    def plot(self):
        # merge separate spw jobs into one job using plotms iterator
        jobs_and_wrappers = super(PlotmsFieldComposite, self).plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers, 'field')
        return successful_wrappers


class PlotmsFieldSpwComposite(FieldSpwComposite):
    leaf_class = PlotmsSpwComposite


class PlotmsFieldBasebandComposite(FieldSpwComposite):
    leaf_class = PlotmsBasebandComposite


class PlotmsSpwAntComposite(SpwAntComposite):
    leaf_class = PlotmsAntComposite


class PlotmsFieldSpwAntComposite(FieldSpwAntComposite):
    leaf_class = PlotmsSpwAntComposite


class SpwFieldSummaryChart(PlotmsFieldSpwComposite):
    """
    Like SpwSummaryChart, but creates separate plots for each field
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)

        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        if 'field' in kwargs:
            field = kwargs['field']
            del kwargs['field']
            LOG.debug('Override for %s vs %s plot: field=%s' % (yaxis, xaxis, field))
        else:
            field = calto.field
        # request plots per spw, overlaying all antennas
        super().__init__(context, output_dir, calto, xaxis, yaxis, intent=intent, field=field,
                                              **kwargs)


class SpwSummaryChart(PlotmsSpwComposite):
    """
    Base class for executing plotms per calibrator field and spw
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)

        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        if 'field' in kwargs:
            field = kwargs['field']
            del kwargs['field']
            LOG.debug('Override for %s vs %s plot: field=%s' % (yaxis, xaxis, field))
        else:
            field = calto.field

        # request plots per spw, overlaying all antennas
        super(SpwSummaryChart, self).__init__(context, output_dir, calto, xaxis, yaxis, intent=intent, field=field,
                                              **kwargs)


class BasebandSummaryChart(PlotmsBasebandComposite):
    """
    Base class for executing plotms per baseband
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)
        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        if 'field' in kwargs:
            field = kwargs['field']
            del kwargs['field']
            LOG.debug('Override for %s vs %s plot: field=%s' % (yaxis, xaxis, field))
        else:
            field = calto.field

        # request plots per spw, overlaying all antennas
        # if field is specified in kwargs, it will override the calto.field
        # selection
        super(BasebandSummaryChart, self).__init__(context, output_dir,
                                                   calto, xaxis, yaxis, intent=intent, field=field, **kwargs)


class FieldBasebandSummaryChart(PlotmsFieldBasebandComposite):
    """
    Like BasebandSummaryChart, but creates separate plots for each field
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)
        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        if 'field' in kwargs:
            field = kwargs['field']
            del kwargs['field']
            LOG.debug('Override for %s vs %s plot: field=%s' % (yaxis, xaxis, field))
        else:
            field = calto.field

        super().__init__(context, output_dir, calto, xaxis, yaxis, intent=intent, field=field, **kwargs)


class AmpVsUVSummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs UV distance plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):

        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True,
            # PIPE-310: set yselfscale=True for amp vs UVdist plots
            'yselfscale': True
        }
        plot_args.update(**overrides)

        super(AmpVsUVSummaryChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp',
                                                  intent=intent, **plot_args)


class AmpVsUVFieldSummaryChart(SpwFieldSummaryChart):
    """
    Create an amplitude vs UV distance plot for each spw, overplotting by antenna, with separate plots for each field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):

        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True,
            # PIPE-310: set yselfscale=True for amp vs UVdist plots
            'yselfscale': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp',
                                                  intent=intent, **plot_args)


class AmpVsUVBasebandSummaryChart(BasebandSummaryChart):
    """
    Create an amplitude vs UV distance plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(kwargs)

        super(AmpVsUVBasebandSummaryChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp',
                                                          intent=intent, **plot_args)


class PhaseVsUVSummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs UV distance plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'coloraxis': 'corr',
            'plotrange': [0, 0, -90, 90],
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(PhaseVsUVSummaryChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='phase',
                                                    intent=intent, **plot_args)


class AmpVsTimeSummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'field',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(AmpVsTimeSummaryChart, self).__init__(context, output_dir, calto, xaxis='time', yaxis='amp',
                                                    intent=intent, **plot_args)


class PhaseVsTimeSummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'coloraxis': 'field',
            'plotrange': [0, 0, -90, 90],
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(PhaseVsTimeSummaryChart, self).__init__(context, output_dir, calto, xaxis='time', yaxis='phase',
                                                      intent=intent, **plot_args)


class AmpVsFrequencySummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': True,
            'plotrange': [0, 0, 0, 0],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True,
            'showatm': True,
            # CAS-10875: applycal plots should have global y-scale per source/dataset
            'yselfscale': True,
        }
        plot_args.update(kwargs)

        super(AmpVsFrequencySummaryChart, self).__init__(context, output_dir, calto, xaxis='freq', yaxis='amp',
                                                         intent=intent, **plot_args)

class AmpVsFrequencyFieldSummaryChart(SpwFieldSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna, with separate plots for each field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': True,
            'plotrange': [0, 0, 0, 0],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True,
            'showatm': True,
            # CAS-10875: applycal plots should have global y-scale per source/dataset
            'yselfscale': True,
        }
        plot_args.update(kwargs)

        super().__init__(context, output_dir, calto, xaxis='freq', yaxis='amp',
                         intent=intent, **plot_args)


class AmpVsFrequencyPerBasebandSummaryChart(BasebandSummaryChart):
    """
    Create an Amp vs Frequency plot for each baseband, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {'ydatacolumn': ydatacolumn,
                     'avgchannel': '',
                     'avgtime': '1e8',
                     'avgscan': True,
                     'avgantenna': True,
                     'plotrange': [0, 0, 0, 0],
                     'correlation': '',
                     'coloraxis': 'antenna1',
                     'overwrite': True}
        plot_args.update(kwargs)

        super().__init__(context, output_dir, calto, xaxis='freq', yaxis='amp', intent=intent, **plot_args)


class AmpVsFrequencyPerFieldBasebandSummaryChart(FieldBasebandSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna, with separate plots for each field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': True,
            'plotrange': [0, 0, 0, 0],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True
        }
        plot_args.update(kwargs)

        super().__init__(context, output_dir, calto, xaxis='freq', yaxis='amp',
                         intent=intent, **plot_args)


class PhaseVsFrequencyPerSpwSummaryChart(SpwFieldSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna, with separate plots for each field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': True,
            'plotrange': [0, 0, -90, 90],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True
        }
        plot_args.update(kwargs)

        super(PhaseVsFrequencyPerSpwSummaryChart, self).__init__(context, output_dir, calto, xaxis='freq',
                                                                 yaxis='phase', intent=intent, **plot_args)


class PhaseVsFrequencyPerBasebandSummaryChart(BasebandSummaryChart):
    """
    Create an Phase vs Frequency plot for each baseband, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {'ydatacolumn': ydatacolumn,
                     'avgchannel': '',
                     'avgtime': '1e8',
                     'avgscan': True,
                     'avgantenna': True,
                     'plotrange': [0, 0, -90, 90],
                     'correlation': '',
                     'coloraxis': 'antenna1',
                     'overwrite': True}
        plot_args.update(kwargs)

        super(PhaseVsFrequencyPerBasebandSummaryChart, self).__init__(
            context, output_dir, calto, xaxis='freq', yaxis='phase', intent=intent, **plot_args)


class PhaseVsFrequencyPerFieldBasebandSummaryChart(FieldBasebandSummaryChart):
    """
    Create an Phase vs Frequency plot for each baseband, overplotting by antenna, with separate plots for each field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {'ydatacolumn': ydatacolumn,
                     'avgchannel': '',
                     'avgtime': '1e8',
                     'avgscan': True,
                     'avgantenna': True,
                     'plotrange': [0, 0, -90, 90],
                     'correlation': '',
                     'coloraxis': 'antenna1',
                     'overwrite': True}
        plot_args.update(kwargs)

        super().__init__(context, output_dir, calto, xaxis='freq', yaxis='phase', intent=intent, **plot_args)


class SpwAntDetailChart(PlotmsSpwAntComposite):
    """
    Base class for executing plotms per calibrator field and spw
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)
        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        if 'field' in kwargs:
            field = kwargs['field']
            del kwargs['field']
            LOG.debug('Override for %s vs %s plot: field=%s' % (yaxis, xaxis, field))
        else:
            field = calto.field

        # request plots per spw, overlaying all antennas
        # if field is specified in kwargs, it will override the calto.field
        # selection
        super(SpwAntDetailChart, self).__init__(context, output_dir, calto, xaxis, yaxis, intent=intent, field=field,
                                                **kwargs )


class FieldSpwAntDetailChart(PlotmsFieldSpwAntComposite):
    """
    Base class for executing plotms per calibrator field, spw and antenna
    """

    def __init__(self, context, output_dir, calto, xaxis, yaxis, intent, **kwargs):
        # (calto, intent) = _get_summary_args(context, result, intent)
        LOG.info('%s vs %s plot: %s' % (yaxis, xaxis, calto))

        # request plots per spw, overlaying all antennas
        super(FieldSpwAntDetailChart, self).__init__(context, output_dir, calto, xaxis, yaxis, intent=intent, **kwargs)


class AmpVsFrequencyDetailChart(FieldSpwAntDetailChart):
    """
    Create an amplitude vs frequency plot for each spw and antenna,
    overplotting by field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': False,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True,
            'showatm': True,
        }
        plot_args.update(**overrides)

        super(AmpVsFrequencyDetailChart, self).__init__(context, output_dir, calto, xaxis='freq', yaxis='amp',
                                                        intent=intent, **plot_args)


class PhaseVsFrequencyDetailChart(FieldSpwAntDetailChart):
    """
    Create a phase vs frequency plot for each spw and antenna, overplotting
    by field.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):

        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': False,
            'plotrange': [0, 0, -180, 180],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(PhaseVsFrequencyDetailChart, self).__init__(context, output_dir, calto, xaxis='freq', yaxis='phase',
                                                          intent=intent, **plot_args)


class AmpVsUVDetailChart(FieldSpwAntDetailChart):
    """
    Create an amplitude vs UV distance plot for each field, spw and antenna
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(AmpVsUVDetailChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='amp', intent=intent,
                                                 **plot_args)


class PhaseVsUVDetailChart(SpwAntDetailChart):
    """
    Create an amplitude vs UV distance plot for each spw and antenna
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'coloraxis': 'corr',
            'plotrange': [0, 0, -180, 180],
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(PhaseVsUVDetailChart, self).__init__(context, output_dir, calto, xaxis='uvdist', yaxis='phase', intent=intent,
                                                   **plot_args)


class AmpVsTimeDetailChart(FieldSpwAntDetailChart):
    """
    Create an amplitude vs time plot for each field, spw and antenna,
    overplotting by correlation.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgantenna': False,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(AmpVsTimeDetailChart, self).__init__(context, output_dir, calto, xaxis='time', yaxis='amp', intent=intent,
                                                   **plot_args)


class CAS9154AmpVsTimeDetailChart(SpwAntDetailChart):
    """
    Create an amplitude vs time plot per spw and antenna, overplotting by
    correlation.

    The fields in this plot are already delineated because this plot is
    against the time axis, hence we can overplot by correlation.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgantenna': False,
            'avgtime': '',
            'avgscan': False,
            'coloraxis': 'corr',
            'overwrite': True,
            'xselfscale': True,
            'yselfscale': True,
        }
        plot_args.update(**overrides)

        super(CAS9154AmpVsTimeDetailChart, self).__init__(context, output_dir, calto, xaxis='time', yaxis='amp', intent=intent,
                                                          **plot_args)


class PhaseVsTimeDetailChart(FieldSpwAntDetailChart):
    """
    Create an phase vs time plot for each field, spw and antenna,
    overplotting by correlation.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **overrides):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgtime': '',
            'avgscan': False,
            'avgantenna': False,
            'plotrange': [0, 0, -180, 180],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super(PhaseVsTimeDetailChart, self).__init__(context, output_dir, calto, xaxis='time', yaxis='phase', intent=intent,
                                                     **plot_args)


class CorrectedToModelRatioVsAntenna1SummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """
    def __init__(self, context, output_dir, calto, intent, ydatacolumn='corrected/model', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgantenna': True,
            'avgchannel': '1e8',
            'coloraxis': 'antenna1',
            'overwrite': True
        }
        plot_args.update(kwargs)

        super(CorrectedToModelRatioVsAntenna1SummaryChart, self).__init__(context, output_dir, calto, xaxis='antenna1',
                                                                          yaxis='amp', intent=intent, **plot_args)


class CorrectedToModelRatioVsUVDistanceSummaryChart(SpwSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent, ydatacolumn='corrected/model', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '1e8',
            'coloraxis': 'antenna1',
            'overwrite': True
        }
        plot_args.update(kwargs)

        super(CorrectedToModelRatioVsUVDistanceSummaryChart, self).__init__(context, output_dir, calto, xaxis='uvdist',
                                                                            yaxis='amp', intent=intent, **plot_args)


class RealVsFrequencySummaryChart(SpwSummaryChart):
    """
    Create a real vs time plot for each spw, overplotting by antenna.
    """

    def __init__(self, context, output_dir, calto, intent='', ydatacolumn='corrected', **kwargs):
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': True,
            'plotrange': [0, 0, 0, 0],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True,
            'showatm': True
        }
        plot_args.update(kwargs)

        super(RealVsFrequencySummaryChart, self).__init__(context, output_dir, calto, xaxis='freq', yaxis='real',
                                                          intent=intent, **plot_args)

class RealVsFrequencyDetailChart(SpwAntDetailChart):
    """
    Create a real vs time plot for each spw and antenna
    """
    def __init__(self,
                 context: Context,
                 output_dir: str,
                 calto: CalTo,
                 intent: str='',
                 ydatacolumn: Optional[str]='corrected',
                 **kwargs: Optional[Dict]
    ) -> None:
        """
        Construct RealVsFrequencyDetailChart instance

        Args:
            context:     Pipeline context
            output_dir:  Output directory for plots
            calto:       Target data selection to apply calibration
            intent:      Intent of the field
            ydatacolumn: Data column for y-axis of the plot
            kwargs:      Plotms arguments
        """
        plot_args = {
            'ydatacolumn': ydatacolumn,
            'avgchannel': '',
            'avgtime': '1e8',
            'avgscan': True,
            'avgantenna': False,
            'plotrange': [0, 0, 0, 0],
            'correlation': '',
            'coloraxis': 'antenna1',
            'overwrite': True,
            'showatm': True
        }
        plot_args.update(kwargs)

        # determine SD or not from context
        self.singledish = is_singledish_ms( context )
        super(RealVsFrequencyDetailChart, self).__init__(context, output_dir, calto, xaxis='freq', yaxis='real',
                                                         intent=intent, **plot_args)
