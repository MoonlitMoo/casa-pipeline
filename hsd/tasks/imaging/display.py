"""Set of plotting classes for hsd_imaging task."""
import copy
import itertools
import math
import os
import time
from typing import Callable, Generator, List, Optional, Union

import numpy
from scipy import interpolate
import matplotlib
import matplotlib.axes as axes
import matplotlib.figure as figure
import matplotlib.ticker as ticker
from matplotlib.ticker import MultipleLocator

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.displays.pointing as pointing
import pipeline.infrastructure.renderer.logger as logger
from pipeline.domain import DataType
from pipeline.h.tasks.common import atmutil
from pipeline.hsd.tasks.common.display import DPIDetail, SDImageDisplay, SDImageDisplayInputs
from pipeline.hsd.tasks.common.display import sd_polmap as polmap, SpectralImage, ChannelSelection
from pipeline.hsd.tasks.common.display import SDSparseMapPlotter
from pipeline.hsd.tasks.common.display import NoData
from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResultItem
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.pointing import MapAxesManagerBase
from pipeline.infrastructure.displays.plotstyle import casa5style_plot

RArotation = pointing.RArotation
DECrotation = pointing.DECrotation
DDMMSSs = pointing.DDMMSSs
HHMMSSss = pointing.HHMMSSss

LOG = infrastructure.get_logger(__name__)


class ImageAxesManager(MapAxesManagerBase):
    """Axes manager for figure containing ImageAxes instance."""

    def __init__(self, fig: figure.Figure,
                 xformatter: ticker.Formatter, yformatter: ticker.Formatter,
                 xlocator: ticker.Locator, ylocator: ticker.Locator,
                 xrotation: float, yrotation: float,
                 ticksize: int, colormap: str) -> None:
        """Construct ImageAxesManager instance.

        Args:
            fig: matplotlib.figure.Figure instance
            xformatter: tick formatter for x-axis
            yformatter: tick formatter for y-axis
            xlocator: tick locator for x-axis
            ylocator: tick locator for y-axis
            xrotation: rotation angle of x-axis label
            yrotation: rotation angle of y-axis label
            ticksize: tick label size
            colormap: colormap to use. 'gray' for gray scale. Otherwise, 'jet' is used.
        """
        super(ImageAxesManager, self).__init__()
        self.figure = fig
        self.xformatter = xformatter
        self.yformatter = yformatter
        self.xlocator = xlocator
        self.ylocator = ylocator
        self.xrotation = xrotation
        self.yrotation = yrotation

        self.ticksize = ticksize

        self.cmap = 'gray' if colormap == 'gray' else 'jet'

    def set_colorbar_for(self, axes: axes.Axes,
                         value_min: float, value_max: float, value_unit: str,
                         shrink: float = 1.0):
        """Set colorbar associated with given Axes instance.

        Axes is supposed to contain ImageAxes inside. If Axes already has colorbar,
        it reuses the existing one rather than creating new instance.

        Args:
            axes: Axes instance
            value_min: minimum value of the range of colorbar
            value_max: maximum value of the range of colorbar
            value_unit: unit of the value
            shrink: shrink parameter for colorbar. Defaults to 1.0.
        """
        # axes should contain one AxesImage
        assert len(axes.images) == 1
        colorbar = getattr(axes, 'colorbar', None)
        if colorbar is None:
            norm = matplotlib.colors.Normalize(value_min, value_max)
            colorbar = self.figure.colorbar(matplotlib.cm.ScalarMappable(norm, self.cmap), shrink=shrink, ax=axes)
            for t in colorbar.ax.get_yticklabels():
                fontsize = t.get_fontsize() * 0.5
                t.set_fontsize(fontsize)
            axes.colorbar = colorbar
        else:
            colorbar.mappable.set_clim((value_min, value_max))
            colorbar.draw_all()
            # set_clim and draw_all clears y-label
        fontsize = colorbar.ax.get_yticklabels()[0].get_fontsize()
        colorbar.ax.set_ylabel('[%s]' % value_unit, fontsize=fontsize)


class SingleImageAxesManager(ImageAxesManager):
    """Axes manager for figure containing single ImageAxes instance."""

    def __init__(self, fig: figure.Figure,
                 xformatter: ticker.Formatter, yformatter: ticker.Formatter,
                 xlocator: ticker.Locator, ylocator: ticker.Locator,
                 xrotation: float, yrotation: float,
                 ticksize: int, colormap: str) -> None:
        """Construct SingleImageAxesManager instance.

        Args:
            fig: matplotlib.figure.Figure instance
            xformatter: tick formatter for x-axis
            yformatter: tick formatter for y-axis
            xlocator: tick locator for x-axis
            ylocator: tick locator for y-axis
            xrotation: rotation angle of x-axis label
            yrotation: rotation angle of y-axis label
            ticksize: tick label size
            colormap: colormap to use. 'gray' for gray scale. Otherwise, 'jet' is used.
        """
        super(SingleImageAxesManager, self).__init__(fig, xformatter, yformatter, xlocator, ylocator, xrotation, yrotation, ticksize, colormap)
        self._image_axes = None

    @property
    def image_axes(self) -> axes.Axes:
        """Return Axes instance for the image.

        Returns:
            Axes instance. The instance is created only once and
            return existing one for the subsequent access.
        """
        if self._image_axes is None:
            axes = self.figure.add_axes([0.25, 0.25, 0.5, 0.5])
            axes.xaxis.set_major_formatter(self.xformatter)
            axes.yaxis.set_major_formatter(self.yformatter)
            axes.xaxis.set_major_locator(self.xlocator)
            axes.yaxis.set_major_locator(self.ylocator)
            xlabels = axes.get_xticklabels()
            for label in xlabels:
                label.set_rotation(self.xrotation)
                label.set_fontsize(self.ticksize)
            ylabels = axes.get_yticklabels()
            for label in ylabels:
                label.set_rotation(self.yrotation)
                label.set_fontsize(self.ticksize)
            xlabel, ylabel = self.get_axes_labels()
            axes.set_xlabel(xlabel, size=self.ticksize)
            axes.set_ylabel(ylabel, size=self.ticksize)

            self._image_axes = axes

        return self._image_axes


class SDChannelAveragedImageDisplay(SDImageDisplay):
    """Plotter to create a color map for channel averaged spw."""

    def plot(self) -> List[logger.Plot]:
        """Create list of color maps for channel averaged spw.

        Returns:
            List of plot objects.
        """
        self.init()

        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0, self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = pointing.XYlabel(span,
                                                                              self.direction_reference)

        # Plotting
        fig = self.figure

        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_max + self.dec_min) / 180. * 3.141592653)

        colormap = 'color'
        TickSize = 6

        axes_manager = SingleImageAxesManager(fig, RAformatter, DECformatter,
                                              RAlocator, DEClocator,
                                              RArotation, DECrotation,
                                              TickSize, colormap)
        axes_manager.direction_reference = self.direction_reference
        axes_tpmap = axes_manager.image_axes
        beam_circle = None

        plot_list = []

        data = self.data
        mask = self.mask
        for pol in range(self.npol):
            Total = (data.take([pol], axis=self.id_stokes) * mask.take([pol], axis=self.id_stokes)).squeeze()
            Total = numpy.flipud(Total.transpose())
            tmin = Total.min()
            tmax = Total.max()

            # 2008/9/20 DEC Effect
            im = axes_tpmap.imshow(Total, interpolation='nearest', aspect=self.aspect, extent=Extent)
            del Total

            xlim = axes_tpmap.get_xlim()
            ylim = axes_tpmap.get_ylim()

            # colorbar
            #print "min=%s, max of Total=%s" % (tmin,tmax)
            if not (tmin == tmax):
                #if not ((Ymax == Ymin) and (Xmax == Xmin)):
                #if not all(image_shape[id_direction] <= 1):
                if self.nx > 1 or self.ny > 1:
                    axes_manager.set_colorbar_for(axes_tpmap, tmin, tmax, self.brightnessunit, shrink=0.8)

            # draw beam pattern
            if beam_circle is None:
                beam_circle = pointing.draw_beam(axes_tpmap, 0.5 * self.beam_size, self.aspect, self.ra_min, self.dec_min)

            axes_tpmap.title('Total Power', size=TickSize)
            axes_tpmap.set_xlim(xlim)
            axes_tpmap.set_ylim(ylim)

            FigFileRoot = self.inputs.imagename+'.pol%s'%(pol)
            plotfile = os.path.join(self.stage_dir, FigFileRoot+'_TP.png')
            fig.savefig(plotfile, dpi=DPIDetail)

            im.remove()

            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.inputs.spw
            parameters['pol'] = self.image.stokes[pol] #polmap[pol]
            parameters['ant'] = self.inputs.antenna
            #parameters['type'] = 'sd_channel-averaged'
            parameters['type'] = 'sd_integrated_map'
            parameters['file'] = self.inputs.imagename
            parameters['field'] = self.inputs.source
            #if self.inputs.vis is not None:
            #    parameters['vis'] = self.inputs.vis
            parameters['vis'] = 'ALL'

            plot = logger.Plot(plotfile,
                               x_axis='R.A.',
                               y_axis='Dec.',
                               field=self.inputs.source,
                               parameters=parameters)
            plot_list.append(plot)

        fig.clf()
        del fig

        return plot_list


class SDMomentMapDisplay(SDImageDisplay):
    """Plotter to create a moment map."""

    def __init__(self, inputs: SDImageDisplayInputs) -> None:
        """Create SDMomentMapDisplay instance.

        Args:
            Inputs instance.
        """
        super(self.__class__, self).__init__(inputs)
        self.moment_images = []

    def init(self) -> None:
        """Do some initialization for moment map.

        Execute immoments task to generate moment image as well as
        performing generic initialization defined in the super class.
        """
        self.moment_images = []
        for spec in self.inputs.MomentMapList:
            chans = self.inputs.create_channel_mask(spec.chans)
            LOG.debug('chans = "%s"', chans)
            moment_imagename_list = [self.inputs.moment_imagename(moment, spec.chans) for moment in spec.moments]
            for imagename in moment_imagename_list:
                if os.path.exists(imagename):
                    status = casa_tools.image.removefile(imagename)
            outfile = self.inputs.moment_imagename(spec.moments, spec.chans)
            moments = [moment.value for moment in spec.moments]
            LOG.debug(f'moment_imagename_list={moment_imagename_list}')
            LOG.debug(f'moments={moments}')
            LOG.debug(f'outfile={outfile}')
            job = casa_tasks.immoments(imagename=self.inputs.imagename, moments=moments, chans=chans, outfile=outfile)
            job.execute()
            for imagename in moment_imagename_list:
                assert os.path.exists(imagename)
            self.moment_images.extend(moment_imagename_list)
        super(self.__class__, self).init()

    def plot(self) -> List[logger.Plot]:
        """Create list of moment maps.

        Returns:
            List of plot objects.
        """
        self.init()

        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0, self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = pointing.XYlabel(span,
                                                                              self.direction_reference)

        # Plotting
        fig = self.figure

        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_max + self.dec_min) / 180. * 3.141592653)

        colormap = 'color'
        TickSize = 6

        axes_manager = SingleImageAxesManager(fig, RAformatter, DECformatter,
                                              RAlocator, DEClocator,
                                              RArotation, DECrotation,
                                              TickSize, colormap)
        axes_manager.direction_reference = self.direction_reference
        axes_tpmap = axes_manager.image_axes
        beam_circle = None

        plot_list = []

        for imagename in self.moment_images:

            if not os.path.exists(imagename):
                continue

            split_imagename = imagename.split('.')
            moment_type = split_imagename[-1]
            chans = split_imagename[-2]

            if moment_type == 'maximum':
                map_title = 'Max Intensity Map'
            else:  # 'integrated'
                map_title = 'Integrated Intensity Map'

            image = SpectralImage(imagename)
            data = image.data
            mask = image.mask

            for pol in range(self.npol):

                masked_data = (data.take([pol], axis=self.id_stokes) * mask.take([pol], axis=self.id_stokes)).squeeze()
                Total = numpy.flipud(masked_data.transpose())
                del masked_data

                if moment_type == 'maximum' and chans == 'line_free':
                    # adjust color scale according to the logic described in PIPE-373/PIPE-197
                    with casa_tools.ImageReader(imagename) as ia:
                        # cf. hif/tasks/tclean/tclean.py mom8_stats
                        stats = ia.statistics(robust=True, stretch=True)
                    tmin = stats['median'][0] - stats['medabsdevmed'][0]

                    per_channel_stats = self.inputs.compute_per_channel_stats()
                    line_free_channels = self.inputs.get_line_free_channels()
                    sigma = numpy.median(per_channel_stats['sigma'][line_free_channels])
                    tmax = 10 * sigma

                    # Since there is no guarantee in the above logic that tmax
                    # is always larger than tmin, or both tmin and tmax are
                    # finite value, this if-block is placed as a safeguard to
                    # make sure tmax >= tmin.
                    if (not all(numpy.isfinite([tmin, tmax]))) or tmin > tmax:
                        tmin = Total.min()
                        tmax = Total.max()
                else:
                    tmin = Total.min()
                    tmax = Total.max()

                # 2008/9/20 DEC Effect
                im = axes_tpmap.imshow(
                    Total, interpolation='nearest', aspect=self.aspect, extent=Extent,
                    vmin=tmin, vmax=tmax
                )
                del Total

                xlim = axes_tpmap.get_xlim()
                ylim = axes_tpmap.get_ylim()

                # colorbar
                #print "min=%s, max of Total=%s" % (tmin,tmax)
                if not (tmin == tmax):
                    #if not ((Ymax == Ymin) and (Xmax == Xmin)):
                    #if not all(image_shape[id_direction] <= 1):
                    if self.nx > 1 or self.ny > 1:
                        axes_manager.set_colorbar_for(axes_tpmap, tmin, tmax, self.brightnessunit, shrink=0.8)

                # draw beam pattern
                if beam_circle is None:
                    beam_circle = pointing.draw_beam(axes_tpmap, 0.5 * self.beam_size, self.aspect, self.ra_min,
                                                     self.dec_min)

                axes_tpmap.set_title(map_title, size=TickSize)
                axes_tpmap.set_xlim(xlim)
                axes_tpmap.set_ylim(ylim)

                figfile = imagename + f'.pol{pol}.png'
                plotfile = os.path.join(self.stage_dir, figfile)
                fig.savefig(plotfile, dpi=DPIDetail)

                im.remove()

                parameters = {}
                parameters['intent'] = 'TARGET'
                parameters['spw'] = self.inputs.spw
                parameters['pol'] = self.image.stokes[pol] #polmap[pol]
                parameters['ant'] = self.inputs.antenna
                parameters['type'] = 'sd_moment_map'
                parameters['moment'] = moment_type
                parameters['chans'] = chans
                parameters['file'] = self.inputs.imagename
                parameters['field'] = self.inputs.source
                parameters['vis'] = 'ALL'

                plot = logger.Plot(plotfile,
                                   x_axis='R.A.',
                                   y_axis='Dec.',
                                   field=self.inputs.source,
                                   parameters=parameters)

                plot_list.append(plot)

        fig.clf()
        del fig

        return plot_list


class ChannelMapAxesManager(ImageAxesManager):
    """Creates and manages Axes instances for channel map.

    Channel map consists of the following Axes:

        - Integrated intensity map (top right)
        - Integrated spectrum with spectral line location (top center)
        - Close up of integrated spectrum with ranges for each channel map (top left)
        - Tiled channel map
    """

    def __init__(self, fig: figure.Figure,
                 xformatter: ticker.Formatter, yformatter: ticker.Formatter,
                 xlocator: ticker.Locator, ylocator: ticker.Locator,
                 xrotation: float, yrotation: float,
                 ticksize: int, colormap: str,
                 nh: int, nv: int, brightnessunit: str):
        """Construct ChannelMapAxesManager instance.

        The constructor generates (nh * nv) axes for channel map.

        Args:
            fig: matplotlib.figure.Figure instance
            xformatter: tick formatter for x-axis
            yformatter: tick formatter for y-axis
            xlocator: tick locator for x-axis
            ylocator: tick locator for y-axis
            xrotation: rotation angle of x-axis label
            yrotation: rotation angle of y-axis label
            ticksize: tick label size
            colormap: colormap to use. 'gray' for gray scale. Otherwise, 'jet' is used.
            nh: number of channel maps in horizontal direction
            nv: number of channel maps in vertical direction
            brightnessunit: unit of the data to be displayed
        """
        super(ChannelMapAxesManager, self).__init__(fig, xformatter, yformatter,
                                                    xlocator, ylocator,
                                                    xrotation, yrotation,
                                                    ticksize, colormap)
        self.nh = nh
        self.nv = nv
        self.brightnessunit = brightnessunit
        self.nchmap = nh * nv
        self.left = 2.10 / 3.0
        self.width = 1.0 / 3.0 * 0.75
        self.bottom = 2.0 / 3.0 + 0.2 / 3.0
        self.height = 1.0 / 3.0 * 0.7

        self._axes_integmap = None
        self._axes_integsp_full = None
        self._axes_integsp_zoom = None
        self._axes_chmap = None

    @property
    def axes_integmap(self) -> axes.Axes:
        """Create Axes instance for integrated intensity map.

        Creates and returns Axes instance for integrated intensity
        map, which is located at the top right of the figure.

        Returns:
            Axes instance for integrated intensity map.
        """
        if self._axes_integmap is None:
            axes = self.figure.add_axes([self.left, self.bottom, 0.98 - self.left, self.height])

            axes.xaxis.set_major_formatter(self.xformatter)
            axes.yaxis.set_major_formatter(self.yformatter)
            axes.xaxis.set_major_locator(self.xlocator)
            axes.yaxis.set_major_locator(self.ylocator)
            xlabels = axes.get_xticklabels()
            for label in xlabels:
                label.set_rotation(self.xrotation)
                label.set_fontsize(self.ticksize)
            ylabels = axes.get_yticklabels()
            for label in ylabels:
                label.set_rotation(self.yrotation)
                label.set_fontsize(self.ticksize)
            xlabel, ylabel = self.get_axes_labels()
            axes.set_xlabel(xlabel, size=self.ticksize)
            axes.set_ylabel(ylabel, size=self.ticksize)

            self._axes_integmap = axes

        return self._axes_integmap

    @property
    def axes_integsp_full(self) -> axes.Axes:
        """Create Axes instance for integrated spectrum.

        Creates and returns Axes instance for integrated spectrum,
        which is located at the top center of the figure.

        Returns:
            Axes instance for integrated spectrum.
        """
        if self._axes_integsp_full is None:
            left = 0.6-self.width
            axes = self.figure.add_axes([left, self.bottom, self.width, self.height])
            axes.xaxis.get_major_formatter().set_useOffset(False)
            axes.xaxis.set_tick_params(which='major', labelsize=self.ticksize)
            axes.yaxis.set_tick_params(which='major', labelsize=self.ticksize)

            axes.set_xlabel('Frequency (GHz)', size=self.ticksize)
            axes.set_ylabel('Intensity (%s)' % self.brightnessunit, size=self.ticksize)
            axes.set_title('Integrated Spectrum', size=self.ticksize)

            self._axes_integsp_full = axes

        return self._axes_integsp_full

    @property
    def axes_integsp_zoom(self) -> axes.Axes:
        """Create Axes instance for close up view of integrated spectrum.

        Creates and returns Axes instance for close up view of
        integrated spectrum, which is located at the top left of the figure.

        Returns:
            Axes instance for close up view of integrated spectrum.
        """
        if self._axes_integsp_zoom is None:
            left = 0.3-self.width
            axes = self.figure.add_axes([left, self.bottom, self.width, self.height])
            axes.xaxis.set_tick_params(which='major', labelsize=self.ticksize)
            axes.yaxis.set_tick_params(which='major', labelsize=self.ticksize)
            axes.set_xlabel('Relative Velocity w.r.t. Window Center (km/s)', size=self.ticksize)
            axes.set_ylabel('Intensity (%s)' % self.brightnessunit, size=self.ticksize)
            axes.set_title('Integrated Spectrum (zoom)', size=self.ticksize)

            self._axes_integsp_zoom = axes

        return self._axes_integsp_zoom

    @property
    def axes_chmap(self) -> List[axes.Axes]:
        """Create Axes instances for channel map.

        Creates and returns Axes instance for channel map,
        which is tiled at the bottom of the figure.

        Returns:
            List of Axes instances for channel map.
        """
        if self._axes_chmap is None:
            self._axes_chmap = list(self.__axes_chmap())

        return self._axes_chmap

    def __axes_chmap(self) -> Generator[axes.Axes, None, None]:
        """Create Axes instances for channel map.

        Axes instances are tiled at the bottom of the figure.
        Total number of instances is self.nh * self.nv.

        Yields:
            Axes instances corresponding to individual map.
        """
#         chmap_hfrac = 0.92 # leave some room for colorbar
#         offset = 0.01
        for i in range(self.nchmap):
            x = i % self.nh
            y = self.nv - int(i // self.nh) - 1
            left = 1.0 / float(self.nh) * x #(x + 0.05)
            width = 1.0 / float(self.nh) * 0.85 #0.9
#             # an attempt to mitigate uneven plot size of panels in the right most column.
#             left = chmap_hfrac / float(self.nh) * x + offset
#             width = chmap_hfrac / float(self.nh)-offset
#             if x==self.nh-1: # add width for colorbar to panels in the right most column
#                 width = min(width*1.25, 1-offset-left)
            bottom = 1.0 / float((self.nv+2)) * (y + 0.05)
            height = 1.0 / float((self.nv+2)) * 0.85
            a = self.figure.add_axes([left, bottom, width, height])
            a.set_aspect('equal')
            a.xaxis.set_major_locator(matplotlib.ticker.NullLocator())
            a.yaxis.set_major_locator(matplotlib.ticker.NullLocator())

            yield a


class SDSparseMapDisplay(SDImageDisplay):
    """Plotter to create a sparse profile map."""

    MaxPanel = 8

    def enable_atm(self) -> None:
        """Enable overlay of ATM transmission curve."""
        self.showatm = True

    def disable_atm(self) -> None:
        """Disable overlay of ATM transmission curve."""
        self.showatm = False

    def plot(self) -> List[logger.Plot]:
        """Create list of sparse profile maps.

        Returns:
            List of Plot instances.
        """
        self.init()

        return self.__plot_sparse_map()

    def __plot_sparse_map(self) -> List[logger.Plot]:
        """Create list of sparse profile maps.

        Returns:
            List of Plot instances.
        """
        # Plotting routine
        num_panel = min(max(self.x_max - self.x_min + 1, self.y_max - self.y_min + 1), self.MaxPanel)
        STEP = int((max(self.x_max - self.x_min + 1, self.y_max - self.y_min + 1) - 1) // num_panel) + 1
        NH = (self.x_max - self.x_min) // STEP + 1
        NV = (self.y_max - self.y_min) // STEP + 1

        LOG.info('num_panel=%s, STEP=%s, NH=%s, NV=%s' % (num_panel, STEP, NH, NV))

        chan0 = 0
        chan1 = self.nchan

        plotter = SDSparseMapPlotter(self.figure, NH, NV, STEP, self.brightnessunit)
        plotter.direction_reference = self.direction_reference

        plot_list = []

        refpix = [0, 0]
        refval = [0, 0]
        increment = [0, 0]
        refpix[0], refval[0], increment[0] = self.image.direction_axis(0, unit='deg')
        refpix[1], refval[1], increment[1] = self.image.direction_axis(1, unit='deg')
        plotter.setup_labels_relative(refpix, refval, increment)

        if hasattr(self, 'showatm') and self.showatm is True:
            msid_list = numpy.unique(self.inputs.msid_list)
            for ms_id in msid_list:
                ms = self.inputs.context.observing_run.measurement_sets[ms_id]
                vis = ms.name
                antenna_id = 0 # nominal
                vspw_id = self.inputs.spw
                spw_id = self.inputs.context.observing_run.virtual2real_spw_id(vspw_id, ms)
                atm_freq, atm_transmission = atmutil.get_transmission(vis=vis, antenna_id=antenna_id,
                                                                      spw_id=spw_id, doplot=False)
                frame = self.frequency_frame
                if frame != 'TOPO':
                    # do conversion
                    assoc_id = self.inputs.msid_list.index(ms_id)
                    field_id = self.inputs.fieldid_list[assoc_id]
                    field = ms.fields[field_id]
                    direction_ref = field.mdirection
                    start_time = ms.start_time
                    end_time = ms.end_time
                    me = casa_tools.measures
                    qa = casa_tools.quanta
                    qmid_time = qa.quantity(start_time['m0'])
                    qmid_time = qa.add(qmid_time, end_time['m0'])
                    qmid_time = qa.div(qmid_time, 2.0)
                    time_ref = me.epoch(rf=start_time['refer'],
                                        v0=qmid_time)
                    position_ref = ms.antennas[antenna_id].position

                    if frame == 'REST':
                        with casa_tools.MSReader( ms.name ) as mse:
                            # use 'SOURCE' to get 'REST', Unit of atm_freq is GHz
                            v_to   = mse.cvelfreqs( spwids=[spw_id], outframe='SOURCE' ) / 1.0E+9
                            v_from = mse.cvelfreqs( spwids=[spw_id], outframe='TOPO'   ) / 1.0E+9
                        _frameconv = interpolate.interp1d( v_from, v_to,
                                                           kind='linear',
                                                           bounds_error=False,
                                                           fill_value='extrapolate' )
                    else:
                        # initialize
                        me.done()
                        me.doframe(time_ref)
                        me.doframe(direction_ref)
                        me.doframe(position_ref)

                        def _frameconv(x):
                            # ATM is always in TOPO
                            m = me.frequency(rf='TOPO', v0=qa.quantity(x, 'GHz'))
                            converted = me.measure(v=m, rf=frame)
                            qout = qa.convert(converted['m0'], outunit='GHz')
                            return qout['value']

                    atm_freq = numpy.fromiter(map(_frameconv, atm_freq), dtype=atm_freq.dtype)
                    me.done()
                plotter.set_atm_transmission(atm_transmission, atm_freq)

        # loop over pol
        data = self.data
        mask = self.mask
        for pol in range(self.npol):
            Plot = numpy.zeros((num_panel, num_panel, (chan1 - chan0)), numpy.float32) + NoData
            TotalSP = (data.take([pol], axis=self.id_stokes) * mask.take([pol], axis=self.id_stokes)).squeeze().sum(axis=(0, 1))
            isvalid = numpy.any(mask.take([pol], axis=self.id_stokes).squeeze(), axis=2)
            Nsp = sum(isvalid.flatten())
            LOG.info('Nsp=%s' % Nsp)
            TotalSP /= Nsp

            slice_axes = (self.image.id_direction[0], self.image.id_direction[1], self.id_stokes)

            for x in range(NH):
                x0 = x * STEP
                x1 = (x + 1) * STEP
                for y in range(NV):
                    y0 = y * STEP
                    y1 = (y + 1) * STEP
                    valid_index = isvalid[x0:x1, y0:y1].nonzero()
                    chunk = self._get_array_chunk(data, (x0, y0, pol), (x1, y1, pol+1), slice_axes).squeeze(axis=self.id_stokes) * self._get_array_chunk(mask, (x0, y0, pol), (x1, y1, pol+1), slice_axes).squeeze(axis=self.id_stokes)
                    valid_sp = chunk[valid_index[0], valid_index[1], :]
                    Plot[x][y] = valid_sp.mean(axis=0)
                    del valid_index, chunk, valid_sp
            del isvalid

            FigFileRoot = self.inputs.imagename+'.pol%s_Sparse' % pol
            plotfile = os.path.join(self.stage_dir, FigFileRoot+'_0.png')

            status = plotter.plot(Plot, TotalSP, self.frequency[chan0:chan1],
                                  figfile=plotfile)
            del Plot, TotalSP

            if status:
                parameters = {}
                parameters['intent'] = 'TARGET'
                parameters['spw'] = self.inputs.spw
                parameters['pol'] = self.image.stokes[pol] #polmap[pol]
                parameters['ant'] = self.inputs.antenna
                parameters['type'] = 'sd_sparse_map'
                parameters['file'] = self.inputs.imagename
                parameters['field'] = self.inputs.source
                parameters['vis'] = 'ALL'

                plot = logger.Plot(plotfile,
                                   x_axis='Frequency',
                                   y_axis='Intensity',
                                   field=self.inputs.source,
                                   parameters=parameters)
                plot_list.append(plot)

        plotter.done()

        return plot_list

    def _get_array_chunk(self, data: numpy.ndarray, blc: List[int], trc: List[int], axes: List[int]) -> numpy.ndarray:
        """
        Return a slice of an array.

        data : an array that could be sliced
        blc : a list of minimum index in each dimention of axes to slice
        trc : a list of maximum index in each dimention of axes to slice
              Note trc is used for the second parameter to construct slice.
              Hence, the indices of last elements in returned array is trc-1
        axes : a list of dimention of axes in cube blc and trc corresponds
        """
        array_shape = data.shape
        ndim = len(array_shape)
        full_blc = numpy.zeros(ndim, dtype=int)
        full_trc = numpy.array(array_shape)
        for i in range(len(axes)):
            iax = axes[i]
            full_blc[iax] = max(blc[i], 0)
            full_trc[iax] = min(trc[i], array_shape[iax])
        return data[tuple(list(map(slice, full_blc, full_trc)))]


class SDChannelMapDisplay(SDImageDisplay):
    """Plotter to create a channel map."""

    NUM_CHANNELMAP = 15
    NhPanel = 5
    NvPanel = 3

    def plot(self) -> List[logger.Plot]:
        """Create list of channel maps.

        Returns:
            List of Plot instances.
        """
        self.init()

        if self.stokes_string != 'I':
            return []

        return self.__plot_channel_map()

    def __get_integrated_spectra(self) -> numpy.ma.masked_array:
        """Compute integrated spectrum from the image.

        Image weights provided by the weight image is taken into account.

        Returns:
            Integrated spectrum as masked array.
        """
        imagename = self.inputs.imagename
        weightname = self.inputs.imagename + '.weight'
        new_id_stokes = 0 if self.id_stokes < self.id_spectral else 1
        # un-weighted image
        unweight_ia = casa_tools.image.imagecalc(outfile='', pixels='"%s" * "%s"' % (imagename, weightname))

        # if all pixels are masked, return fully masked array
        unweight_mask = unweight_ia.getchunk(getmask=True)
        if numpy.all(numpy.logical_not(unweight_mask)):
            unweight_ia.close()
            sp_ave = numpy.ma.masked_array(numpy.zeros((self.npol, self.nchan), dtype=numpy.float32),
                                           mask=numpy.ones((self.npol, self.nchan), dtype=bool))
            return sp_ave

        # average image spectra over map area taking mask into account
        try:
            collapsed_ia = unweight_ia.collapse(outfile='', function='mean', axes=self.image.id_direction)
        finally:
            unweight_ia.close()
        try:
            data_integ = collapsed_ia.getchunk(dropdeg=True)
            mask_integ = collapsed_ia.getchunk(dropdeg=True, getmask=True)
        finally:
            collapsed_ia.close()
        # set mask to weight image
        with casa_tools.ImageReader(imagename) as ia:
            maskname = ia.maskhandler('get')[0]
        with casa_tools.ImageReader(weightname) as ia:
            if maskname != 'T':  # 'T' is no mask (usually an image from completely flagged MSes)
                ia.maskhandler('delete', [maskname])
                ia.maskhandler('copy', ['%s:%s' % (imagename, maskname), maskname])
                ia.maskhandler('set', maskname)
            # average weight over map area taking the mask into account
            collapsed_ia = ia.collapse(outfile='', function='mean', axes=self.image.id_direction)
        try:
            weight_integ = collapsed_ia.getchunk(dropdeg=True)
        finally:
            collapsed_ia.close()
        # devive averaged image by averaged weight
        data_weight_integ = numpy.ma.masked_array((data_integ / weight_integ), [not val for val in mask_integ],
                                                  fill_value=0.0)
        sp_ave = numpy.ma.masked_array(numpy.zeros((self.npol, self.nchan), dtype=numpy.float32))
        if self.npol == 1:
            if len(data_weight_integ) == self.nchan:
                sp_ave[0, :] = data_weight_integ
        else:
            for pol in range(self.npol):
                curr_sp = data_weight_integ.take([pol], axis=new_id_stokes).squeeze()
                if len(curr_sp) == self.nchan:
                    sp_ave[pol, :] = curr_sp
        return sp_ave

    def __plot_channel_map(self) -> List[logger.Plot]:
        """Create list of channel maps.

        Returns:
            List of Plot instances.
        """
        colormap = 'color'
        scale_max = False
        scale_min = False

        plot_list = []

        is_freq_chan_reversed_image = False
        if isinstance(self.inputs.result, SDImagingResultItem):
            is_freq_chan_reversed_image = self.inputs.result.frequency_channel_reversed

        # retrieve line list from reduction group
        # key is antenna and spw id
        line_list = self.inputs.valid_lines(is_freq_chan_reversed_image)

        # 2010/6/9 in the case of non-detection of the lines
        if len(line_list) == 0:
            return plot_list

        # Set data
        Map = numpy.zeros((self.NUM_CHANNELMAP,
                           (self.y_max - self.y_min + 1),
                           (self.x_max - self.x_min + 1)),
                          dtype=numpy.float32)

        # Swap (x,y) to match the clustering result
        grid_size_arcsec = self.grid_size * 3600.0
        ExtentCM = ((self.x_max + 0.5) * grid_size_arcsec,
                    (self.x_min - 0.5) * grid_size_arcsec,
                    (self.y_min - 0.5) * grid_size_arcsec,
                    (self.y_max + 0.5) * grid_size_arcsec)
        Extent = (self.ra_max + self.grid_size / 2.0,
                  self.ra_min - self.grid_size / 2.0,
                  self.dec_min - self.grid_size / 2.0,
                  self.dec_max + self.grid_size / 2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size,
                   self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = \
            pointing.XYlabel(span, self.direction_reference)

        # How to coordinate the map
        TickSize = 6

        # Initialize axes
        fig = self.figure
        axes_manager = ChannelMapAxesManager(fig, RAformatter, DECformatter,
                                             RAlocator, DEClocator,
                                             RArotation, DECrotation,
                                             TickSize, colormap,
                                             self.NhPanel, self.NvPanel,
                                             self.brightnessunit)
        axes_manager.direction_reference = self.direction_reference
        axes_integmap = axes_manager.axes_integmap
        beam_circle = None
        axes_integsp1 = axes_manager.axes_integsp_full
        axes_integsp2 = axes_manager.axes_integsp_zoom
        axes_chmap = axes_manager.axes_chmap

        Sp_integ = self.__get_integrated_spectra()
        # loop over detected lines
        ValidCluster = 0
        data = self.data
        mask = self.mask

        # If the frequency channel of the image cube has been reversed to ascending,
        # indices of frequency and velocity must have an offset to get a center value of them.
        if is_freq_chan_reversed_image:
            _offset = 0.5
            _left_edge = self.edge[1]
        else:
            _offset = 0
            _left_edge = self.edge[0]

        # NOTE: Variable names MUST be easy to understand.
        # The variables with the prefixes, 'idx_' and 'indices_', indicate they are an index or
        # sequence (list, set, array, etc.) of indices in the loop below.

        for line_window in line_list:
            _line_center = line_window[0]
            _line_width = line_window[1]
            _line_center_shifted = _line_center - _left_edge

            # Offset calculation of index(int) from line(float) when the line has a decimal point of 0.5
            # and frequency channel of the image is reversed;
            # ex) n=107.5, nchan=128, reversed_n = nchan-1-n = 19.5
            # when the real numeric cut-edge is 0.5 then the calculation of index 1-0.5 is to be neary 0.5 and not 0.5.
            # So, the cut-edge which has 0.5 + offset 0.5 like int(19.5 + 0.5) must be 19, not 20.
            _cut_edge_offset = 0
            if is_freq_chan_reversed_image and _line_center_shifted - int(_line_center_shifted) == 0.5:
                _cut_edge_offset = 1

            # shift channel according to the edge parameter
            idx_line_center = int(_line_center_shifted + 0.5) - _cut_edge_offset
            if float(idx_line_center) == _line_center_shifted:
                velocity_line_center = self.velocity[idx_line_center]
            else:
                if is_freq_chan_reversed_image:
                    velocity_line_center = 0.5 * (self.velocity[idx_line_center + 1] +
                                                  self.velocity[idx_line_center])
                else:
                    velocity_line_center = 0.5 * (self.velocity[idx_line_center] +
                                                  self.velocity[idx_line_center - 1])
            LOG.debug(f'center velocity[{idx_line_center}]: {velocity_line_center}')
            if idx_line_center > 0:
                ChanVelWidth = abs(self.velocity[idx_line_center] - self.velocity[idx_line_center - 1])
            else:
                ChanVelWidth = abs(self.velocity[idx_line_center] - self.velocity[idx_line_center + 1])

            LOG.debug(f"center frequency[{idx_line_center}]: {self.frequency[idx_line_center]}")
            # 2007/9/13 Change the magnification factor 1.2 to your preference (to Dirk)
            # be sure the width of one channel map is integer
            # 2014/1/12 factor 1.4 -> 1.0 since velocity structure was taken into account for the range in validation.py
            indices_slice_width = max(int(_line_width / self.NUM_CHANNELMAP + 0.5), 1)
            idx_left_end = int(idx_line_center - self.NUM_CHANNELMAP / 2.0 * indices_slice_width + 0.5 + _offset)
            # 2007/9/10 remedy for 'out of index' error
            LOG.debug('idx_left_end, indices_slice_width, NchanMap : '
                      f'{idx_left_end}, {indices_slice_width}, {self.NUM_CHANNELMAP}')
            if idx_left_end < 0:
                indices_slice_width = int(idx_line_center * 2.0 / self.NUM_CHANNELMAP)
                if indices_slice_width == 0:
                    continue
                idx_left_end = int(idx_line_center - self.NUM_CHANNELMAP / 2.0 * indices_slice_width)
            elif idx_left_end + indices_slice_width * self.NUM_CHANNELMAP > self.nchan:
                indices_slice_width = int((self.nchan - 1 - idx_line_center) * 2.0 / self.NUM_CHANNELMAP)
                if indices_slice_width == 0:
                    continue
                idx_left_end = int(idx_line_center - self.NUM_CHANNELMAP / 2.0 * indices_slice_width)

            chan0 = max(idx_left_end - 1, 0)
            chan1 = min(idx_left_end + self.NUM_CHANNELMAP * indices_slice_width, self.nchan - 1)
            V0 = min(self.velocity[chan0], self.velocity[chan1]) - velocity_line_center
            V1 = max(self.velocity[chan0], self.velocity[chan1]) - velocity_line_center
            LOG.debug('chan0, chan1, V0, V1, velocity_line_center : '
                      f'{chan0}, {chan1}, {V0}, {V1}, {velocity_line_center}')

            vertical_lines = []

            # vertical lines for integrated spectrum #1
            vertical_lines.append(
                axes_integsp1.axvline(x=self.frequency[chan0], linewidth=0.3, color='r')
            )
            vertical_lines.append(
                axes_integsp1.axvline(x=self.frequency[chan1], linewidth=0.3, color='r')
            )

            # Drawing red vertical lines for integrated spectrum #2
            # For detail, see the MEMO below.
            _indices_width_of_emission_line = indices_slice_width * self.NUM_CHANNELMAP
            _x_chan = numpy.arange(idx_left_end, idx_left_end + _indices_width_of_emission_line)
            _y_vel = self.velocity[idx_left_end:idx_left_end + _indices_width_of_emission_line]
            chan2vel = interpolate.interp1d(_x_chan, _y_vel, bounds_error=False, fill_value='extrapolate')

            # calculate relative velocities for red vertical lines
            _chans = [idx_left_end + i * indices_slice_width - 0.5 for i in range(self.NUM_CHANNELMAP + 1)]
            vertlines = chan2vel(_chans) - velocity_line_center
            
            vertical_lines.extend(
                (axes_integsp2.axvline(x, linewidth=0.3, color='r') for x in vertlines)
            )

            LOG.debug('Relative velocities of the vertical lines: %s', vertlines)

            # MEMO: 
            # - For the velocity plot #2, the red vertical lines are drawn at the relative velocity
            #   values calculated by linear interpolation.
            #   These lines indicate the boundaries of NUM_CHANNELMAP slices, with each slice having
            #   a number of indices_slice_width channels. The velocity values at the boundaries are
            #   calculated by linear interpolating the channel-center velocity values (self.velocity[]).
            #   Extrapolation for 1/2 channels will be done to calculate the velocity values at the
            #   very end of the line_window.
            # - On the other hand, for the frequency plot #1, the channel-center velocity values
            #   (self.velocity[]) are directly used to indicate the bounderies of the line_window as two
            #   red vertical lines, hence no interpolations. Therefore the positions of the red lines of
            #   plot #1 are slightly shifted by 0.5 ch compared to the corresponding lines in plot #2.

            # loop over polarizations
            for pol in range(self.npol):
                plotted_objects = []

                masked_data = (data.take([pol], axis=self.id_stokes) *
                               mask.take([pol], axis=self.id_stokes)).squeeze()

                # Integrated Spectrum
                t0 = time.time()

                # Draw Total Intensity Map
                total = masked_data.sum(axis=2) * ChanVelWidth
                total = numpy.flipud(total.transpose())

                # 2008/9/20 DEC Effect
                plotted_objects.append(
                    axes_integmap.imshow(total, interpolation='nearest',
                                         aspect=self.aspect, extent=Extent)
                )

                xlim = axes_integmap.get_xlim()
                ylim = axes_integmap.get_ylim()

                # colorbar
                LOG.debug(f'total min:{total.min()}, total max:{total.max()}')
                if not (total.min() == total.max()):
                    if not ((self.y_max == self.y_min) and (self.x_max == self.x_min)):
                        axes_manager.set_colorbar_for(axes_integmap, total.min(), total.max(),
                                                      f'{self.brightnessunit} km/s', shrink=0.8)

                # draw beam pattern
                if beam_circle is None:
                    beam_circle = pointing.draw_beam(axes_integmap, self.beam_radius, self.aspect,
                                                     self.ra_min, self.dec_min)

                axes_integmap.set_title('Total Intensity: CenterFreq.= %.3f GHz' %
                                        self.frequency[idx_line_center], size=TickSize)
                axes_integmap.set_xlim(xlim)
                axes_integmap.set_ylim(ylim)

                t1 = time.time()

                # Plot Integrated Spectrum #1
                Sp = Sp_integ[pol, :]
                (F0, F1) = (min(self.frequency[0], self.frequency[-1]),
                            max(self.frequency[0], self.frequency[-1]))
                spmin = Sp.min()
                spmax = Sp.max()
                dsp = spmax - spmin
                spmin -= dsp * 0.1
                spmax += dsp * 0.1
                LOG.debug(f'Freq0, Freq1: {F0}, {F1}')

                axes_integsp1.plot(self.frequency, Sp, '-b', markersize=2,
                                   markeredgecolor='b', markerfacecolor='b')
                axes_integsp1.axis([F0, F1, spmin, spmax])

                t2 = time.time()

                # Plot Integrated Spectrum #2
                axes_integsp2.plot(self.velocity[chan0:chan1 + 1] - velocity_line_center,
                                   Sp[chan0:chan1 + 1], '-b', markersize=2, markeredgecolor='b',
                                   markerfacecolor='b')
                # adjust Y-axis range to the current line
                spmin_zoom = Sp[chan0:chan1 + 1].min()
                spmax_zoom = Sp[chan0:chan1 + 1].max()
                dsp = spmax_zoom - spmin_zoom
                spmin_zoom -= dsp * 0.1
                spmax_zoom += dsp * 0.1
                axes_integsp2.axis([V0, V1, spmin_zoom, spmax_zoom])
                LOG.debug(f'Velo0, Velo1: {V0}, {V1}')

                t3 = time.time()

                # Draw Channel Map
                NMap = 0
                Vmax0 = Vmin0 = 0
                Title = []
                for i in range(self.NUM_CHANNELMAP):
                    ii = self.NUM_CHANNELMAP - i - 1
                    C0 = idx_left_end + indices_slice_width * ii
                    C1 = C0 + indices_slice_width
                    if C0 < 0 or C1 >= self.nchan - 1:
                        continue
                    velo = (self.velocity[C0] + self.velocity[C1 - 1]) / 2.0 - velocity_line_center
                    width = abs(self.velocity[C0] - self.velocity[C1])
                    Title.append('(Vel,Wid) = (%.1f, %.1f) (km/s)' % (velo, width))
                    NMap += 1
                    _mask = masked_data[:, :, C0:C1].sum(axis=2) * ChanVelWidth
                    Map[i] = numpy.flipud(_mask.transpose())
                del masked_data
                Vmax0 = Map.max()
                Vmin0 = Map.min()
                if isinstance(scale_max, bool):
                    Vmax = Vmax0 - (Vmax0 - Vmin0) * 0.1
                else:
                    Vmax = scale_max
                if isinstance(scale_min, bool):
                    Vmin = Vmin0 + (Vmax0 - Vmin0) * 0.1
                else:
                    Vmin = scale_min

                if Vmax == 0 and Vmin == 0:
                    print("No data to create channel maps. Check the flagging criteria.")
                    return plot_list

                for i in range(NMap):
                    if Vmax != Vmin:
                        axes_chmap[i].imshow(Map[i], vmin=Vmin, vmax=Vmax, interpolation='nearest',
                                             aspect='equal', extent=ExtentCM)
                        x = i % self.NhPanel
                        if x == (self.NhPanel - 1):
                            axes_manager.set_colorbar_for(axes_chmap[i], Vmin, Vmax, f'{self.brightnessunit} km/s')

                        axes_chmap[i].set_title(Title[i], size=TickSize)

                t4 = time.time()
                LOG.debug('PROFILE: integrated intensity map: %s sec' % (t1 - t0))
                LOG.debug('PROFILE: integrated spectrum #1: %s sec' % (t2 - t1))
                LOG.debug('PROFILE: integrated spectrum #2: %s sec' % (t3 - t2))
                LOG.debug('PROFILE: channel map: %s sec' % (t4 - t3))

                FigFileRoot = self.inputs.imagename + '.pol%s' % (pol)
                plotfile = os.path.join(self.stage_dir, FigFileRoot + '_ChannelMap_%s.png' % (ValidCluster))
                fig.savefig(plotfile, dpi=DPIDetail)

                for _a in itertools.chain([axes_integmap, axes_integsp1, axes_integsp2], axes_chmap):
                    for obj in itertools.chain(_a.lines[:], _a.texts[:], _a.patches[:], _a.images[:]):
                        if (obj not in vertical_lines) and (obj != beam_circle):
                            obj.remove()

                parameters = {}
                parameters['intent'] = 'TARGET'
                parameters['spw'] = self.spw
                parameters['pol'] = self.image.stokes[pol]  # polmap[pol]
                parameters['ant'] = self.antenna
                parameters['type'] = 'channel_map'
                parameters['file'] = self.inputs.imagename
                parameters['field'] = self.inputs.source
                parameters['vis'] = 'ALL'
                parameters['line'] = [self.frequency[chan0] * 1e9, self.frequency[chan1] * 1e9]  # GHz -> Hz

                plot = logger.Plot(plotfile,
                                   x_axis='R.A.',
                                   y_axis='Dec.',
                                   field=self.inputs.source,
                                   parameters=parameters)
                plot_list.append(plot)

            ValidCluster += 1

            for line in vertical_lines:
                line.remove()

        del axes_manager
        fig.clf()
        del fig

        return plot_list


class SDRmsMapDisplay(SDImageDisplay):
    """Plotter to create a baseline rms map."""

    def plot(self) -> List[logger.Plot]:
        """Create list of baseline rms maps.

        Returns:
            List of Plot instances.
        """
        self.init()

        t1 = time.time()
        plot_list = self.__plot()
        t2 = time.time()
        LOG.debug('__plot: elapsed time %s sec'%(t2-t1))

        return plot_list

    def __get_rms(self) -> numpy.ndarray:
        """Compute baseline rms for each spatial pixel.

        Returns:
            Two-dimensional array of baseline rms.
        """
        # reshape rms to a 3d array in shape, (nx_im, ny_im, npol_data)
        return self.__reshape_grid_table_values(self.inputs.result.outcome['rms'], float)

    def __get_num_valid(self) -> numpy.ndarray:
        """Compute number of valid spectra associated with each spatial pixel.

        Returns:
            Two-dimentional array of number of valid spectra
        """
        # reshape validsp to a 3d array in shape, (nx_im, ny_im, npol_data)
        return self.__reshape_grid_table_values(self.inputs.result.outcome['validsp'], int)

    def __reshape_grid_table_values(self, array2d, dtype=None) -> numpy.ndarray:
        """Reshape input 2-D array into 3-D array.

        The input two-dimensional array with shape (npol, nx * ny) into
        three-dimensional array with shape (nx, ny, npol).

        Args:
            array2d: Input two-dimensional array.
            dtype: Array data type. Defaults to None.

        Returns:
            Reshaped array.
        """
        # reshape 2d array in shape, (npol, nx*ny), to (nx, ny, npol)
        npol_data = len(array2d)
        # retruned value will be transposed
        array3d = numpy.zeros((npol_data, self.ny, self.nx), dtype=dtype)
        for pol in range(npol_data):
            if len(array2d[pol]) == self.nx*self.ny:
                array3d[pol, :, :] = numpy.array(array2d[pol]).reshape((self.ny, self.nx))
        return numpy.flipud(array3d.transpose())

    def __plot(self) -> List[logger.Plot]:
        """Create list of baseline rms maps.

        Returns:
            List of Plot instances.
        """
        fig = figure.Figure()

        colormap = 'color'
        plot_list = []

        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_min + self.dec_max) / 180.0 * 3.141592653)

        # Draw RMS Map
        TickSize = 6

        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0,
                  self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = pointing.XYlabel(span, self.direction_reference)

        axes_manager = SingleImageAxesManager(fig, RAformatter, DECformatter,
                                              RAlocator, DEClocator,
                                              RArotation, DECrotation,
                                              TickSize, colormap)
        axes_manager.direction_reference = self.direction_reference
        rms_axes = axes_manager.image_axes
        beam_circle = None

        rms = self.__get_rms()
        nvalid = self.__get_num_valid()

        # threshold percentages (minimum and maximum)
        beam_pix = self.beam_size * abs(self.ny/(self.dec_max - self.dec_min))
        length = 4*(self.nx*self.ny)**0.5
        thres_min = 1  # 1 percent
        thres_max = (1.0 - (length*beam_pix/6.0)/(self.nx*self.ny))*100

        npol_data = rms.shape[2]
#        for pol in xrange(self.npol):
        for pol in range(npol_data):
            rms_map = rms[:, :, pol] * (nvalid[:, :, pol] > 0)
            rms_map = numpy.flipud(rms_map.transpose())
            rms_map_v = rms_map[~numpy.isnan(rms_map)]
            rms_map_v = rms_map[numpy.nonzero(rms_map)]
            if len(rms_map_v) == 0:
                continue
            # threshold values (minimum and maximum)
            q_min, q_max = numpy.nanpercentile(rms_map_v, [thres_min, thres_max])
            # 2008/9/20 DEC Effect
            image = rms_axes.imshow(rms_map, vmin=q_min, vmax=q_max, interpolation='nearest', aspect=self.aspect, extent=Extent)
            xlim = rms_axes.get_xlim()
            ylim = rms_axes.get_ylim()

            # colorbar
            if not (q_min == q_max):
                if not ((self.y_max == self.y_min) and (self.x_max == self.x_min)):
                    axes_manager.set_colorbar_for(rms_axes, q_min, q_max, self.brightnessunit, shrink=0.8)

            del rms_map

            # draw beam pattern
            if beam_circle is None:
                beam_circle = pointing.draw_beam(rms_axes, self.beam_radius, self.aspect, self.ra_min, self.dec_min)

            rms_axes.set_xlim(xlim)
            rms_axes.set_ylim(ylim)
            rms_axes.set_title('Baseline RMS Map', size=TickSize)

            FigFileRoot = self.inputs.imagename + '.pol%s' % pol
            plotfile = os.path.join(self.stage_dir, FigFileRoot+'_rmsmap.png')
            fig.savefig(plotfile, dpi=DPIDetail)

            image.remove()

            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.spw
            parameters['pol'] = polmap[pol]
            parameters['ant'] = self.antenna
            parameters['file'] = self.inputs.imagename
            parameters['type'] = 'rms_map'
            parameters['field'] = self.inputs.source
            parameters['vis'] = 'ALL'

            plot2 = logger.Plot(plotfile,
                                x_axis='R.A.',
                                y_axis='Dec.',
                                field=self.inputs.source,
                                parameters=parameters)
            plot_list.append(plot2)

        fig.clf()
        del fig

        return plot_list


class SpectralMapAxesManager(MapAxesManagerBase):
    """Creates and manages Axes instances for detailed spectral map."""

    def __init__(self, fig: figure.Figure, nh: int, nv: int,
                 brightnessunit: str, locator: ticker.Locator, ticksize: int) -> None:
        """Create SpectralMapAxesManager instance.

        Total number of Axes instances in the figure is nh * nv.

        Args:
            fig: figure.Figure instance
            nh: number of Axes instances in horizontal direction
            nv: number of Axes instances in vertical direction
            brightnessunit: unit for the image
            locator: locator for x-axis
            ticksize: tick label size
        """
        super(SpectralMapAxesManager, self).__init__()
        self.figure = fig
        self.nh = nh
        self.nv = nv
        self.brightnessunit = brightnessunit
        self.locator = locator
        self.ticksize = ticksize

        self._axes = None

    @property
    def axes_list(self) -> List[axes.Axes]:
        """Return list of Axes instances for the profile map.

        Translation of one-dimensional list into two-dimensional location
        is needed.

          - The first Axes corresponds to the bottom left panel
          - The first to (nh-1)-th Axes form the bottom row of the profile map
          - The nh-th to (2*nh-1)-th Axes form the next row, which is right above
            the bottom row
          - Similarly, the list is translated into nv rows in total
          - The last Axes corresponds to the top right panel

        Returns:
            List of Axes instances.
        """
        if self._axes is None:
            self._axes = list(self.__axes_list())

        return self._axes

    def __axes_list(self) -> Generator[axes.Axes, None, None]:
        """Create list of Axes instances for detailed profile map.

        Yields:
            Axes instance corresponding to each panel of profile map.
        """
        npanel = self.nh * self.nv
        for ipanel in range(npanel):
            x = ipanel % self.nh
            y = int(ipanel // self.nh)
            #x0 = 1.0 / float(self.nh) * (x + 0.1)
            x0 = 1.0 / float(self.nh) * (x + 0.22)
            #x1 = 1.0 / float(self.nh) * 0.8
            x1 = 1.0 / float(self.nh) * 0.75
            y0 = 1.0 / float(self.nv) * (y + 0.15)
            #y1 = 1.0 / float(self.nv) * 0.7
            y1 = 1.0 / float(self.nv) * 0.65
            a = self.figure.add_axes([x0, y0, x1, y1])
            a.xaxis.get_major_formatter().set_useOffset(False)
            a.xaxis.set_major_locator(self.locator)
            a.yaxis.set_label_coords(-0.22, 0.5)
            a.yaxis.get_major_formatter().set_useOffset(False)
            a.title.set_y(0.95)
            a.title.set_size(self.ticksize)
            a.set_ylabel('Intensity (%s)' % (self.brightnessunit), size=self.ticksize)
            a.xaxis.set_tick_params(which='major', labelsize=self.ticksize)
            a.yaxis.set_tick_params(which='major', labelsize=self.ticksize)

            yield a


class SDSpectralMapDisplay(SDImageDisplay):
    """Plotter for detailed spectral map."""

    MaxNhPanel = 5
    MaxNvPanel = 5

    def plot(self) -> List[logger.Plot]:
        """Create detailed profile map.

        If provided image is so large that its number of spatial pixels
        exceed the number of panels for single sparse map, multiple
        image files are created.

        Returns:
            List of detailed profile maps.
        """
        self.init()
        return self.__plot_spectral_map()

    def __get_strides(self) -> List[int]:
        """Return the stride for creating profile map.

        The stride represents the number of spatial pixels that are combined into
        single profile map.

        Returns:
            List of strides in horizontal and vertical directions.
        """
        qa = casa_tools.quanta
        units = self.image.units
        factors = []
        for idx in self.image.id_direction:
            cell = qa.convert(qa.quantity(self.image.increments[idx], units[idx]), 'deg')['value']
            factors.append(int(numpy.round(abs(self.grid_size / cell))))
        return factors

    def __plot_spectral_map(self) -> List[logger.Plot]:
        """Create detailed profile map.

        If provided image is so large that its number of spatial pixels
        exceed the number of panels for single sparse map, multiple
        image files are created.

        Returns:
            List of detailed profile maps.
        """
        fig = figure.Figure()

        (STEPX, STEPY) = self.__get_strides()

        # Raster Case: re-arrange spectra to match RA-DEC orientation
        mode = 'raster'
        if mode.upper() == 'RASTER':
            # the number of panels in each page
            NhPanel = min(max((self.x_max - self.x_min + 1)//STEPX,
                              (self.y_max - self.y_min + 1)//STEPY), self.MaxNhPanel)
            NvPanel = min(max((self.x_max - self.x_min + 1)//STEPX,
                              (self.y_max - self.y_min + 1)//STEPY), self.MaxNvPanel)
            # total number of pages in horizontal and vertical directions
            NH = int((self.x_max - self.x_min) // STEPX // NhPanel + 1)
            NV = int((self.y_max - self.y_min) // STEPY // NvPanel + 1)
            # an array with length of total number of spectra to be plotted (initialized by -1)
            ROWS = numpy.zeros(NH * NV * NhPanel * NvPanel, dtype=int) - 1
            # 2010/6/15 GK Change the plotting direction: UpperLeft->UpperRight->OneLineDown repeat...
            for x in range(0, self.nx, STEPX):
                posx = (self.x_max - x)//STEPX // NhPanel
                offsetx = ( (self.x_max - x)//STEPX ) % NhPanel
                for y in range(0, self.ny, STEPY):
                    posy = (self.y_max - y)//STEPY // NvPanel
                    offsety = NvPanel - 1 - (self.y_max - y)//STEPY % NvPanel
                    row = (self.nx - x - 1) * self.ny + y
                    ROWS[(posy*NH+posx)*NvPanel*NhPanel + offsety*NhPanel + offsetx] = row
        else: ### This block is currently broken (2016/06/23 KS)
            #ROWS = rows[:]
            #NROW = len(rows)
            #Npanel = (NROW - 1) / (self.MaxNhPanel * self.MaxNvPanel) + 1
            #if Npanel > 1:  (NhPanel, NvPanel) = (self.MaxNhPanel, self.MaxNvPanel)
            #else: (NhPanel, NvPanel) = (int((NROW - 0.1) ** 0.5) + 1, int((NROW - 0.1) ** 0.5) + 1)
            raise Exception("non-Raster map is not supported yet.")

        LOG.debug("Generating spectral map")
        LOG.debug("- Stride: [%d, %d]" % (STEPX, STEPY))
        LOG.debug("- Number of panels: [%d, %d]" % (NhPanel, NvPanel))
        LOG.debug("- Number of pages: [%d, %d]" % (NH, NV))
        LOG.debug("- Number of spcetra to be plotted: %d" % (len(ROWS)))

        Npanel = 0
        TickSize = 11 - NhPanel

        # Plotting routine
        connect = True
        if connect is True:
            Mark = '-b'
        else:
            Mark = 'bo'
        chan0 = 0
        chan1 = -1
        if chan1 == -1:
            chan0 = 0
            chan1 = self.nchan - 1
        xmin = min(self.frequency[chan0], self.frequency[chan1])
        xmax = max(self.frequency[chan0], self.frequency[chan1])

        NSp = 0
        xtick = abs(self.frequency[-1] - self.frequency[0]) / 4.0
        Order = int(math.floor(math.log10(xtick)))
        NewTick = int(xtick / (10**Order) + 1) * (10**Order)
        FreqLocator = MultipleLocator(NewTick)

        (xrp, xrv, xic) = self.image.direction_axis(0)
        (yrp, yrv, yic) = self.image.direction_axis(1)

        plot_list = []

        axes_manager = SpectralMapAxesManager(fig, NhPanel, NvPanel, self.brightnessunit,
                                              FreqLocator,
                                              TickSize)
        axes_list = axes_manager.axes_list

        # MS-based procedure
        reference_data = self.context.observing_run.measurement_sets[self.inputs.msid_list[0]]
        is_baselined = reference_data.get_data_column(DataType.BASELINED) is not None

        data = self.data
        mask = self.mask
        for pol in range(self.npol):
            data = (data.take([pol], axis=self.id_stokes) * mask.take([pol], axis=self.id_stokes)).squeeze()
            Npanel = 0

            # to eliminate max/min value due to bad pixel or bad fitting,
            #  1/10-th value from max and min are used instead
#             valid_index = numpy.where(self.num_valid_spectrum[:,:,pol] > 0)
            mask2d = numpy.any(mask.take([pol], axis=self.id_stokes).squeeze(), axis=2)
            valid_index = mask2d.nonzero()
            valid_data = data[valid_index[0], valid_index[1], chan0:chan1]
            ListMax = valid_data.max(axis=1)
            ListMin = valid_data.min(axis=1)
            del valid_index, valid_data
            if len(ListMax) == 0:
                continue
            if is_baselined:
                ymax = numpy.sort(ListMax)[len(ListMax) - len(ListMax)//10 - 1]
                ymin = numpy.sort(ListMin)[len(ListMin)//10]
            else:
                ymax = numpy.sort(ListMax)[-1]
                ymin = numpy.sort(ListMin)[1]
            ymax = ymax + (ymax - ymin) * 0.2
            ymin = ymin - (ymax - ymin) * 0.1
            LOG.debug('ymin=%s, ymax=%s' % (ymin, ymax))
            del ListMax, ListMin

            for irow in range(len(ROWS)):
                row = ROWS[irow]

                _x = row // self.ny
                _y = row % self.ny

                prefix = self.inputs.imagename+'.pol%s_Result' % pol
                plotfile = os.path.join(self.stage_dir, prefix+'_%s.png' % Npanel)

                if not os.path.exists(plotfile):
                    if 0 <= _x < self.nx and 0 <= _y < self.ny:
                        a = axes_list[NSp]
                        a.set_axis_on()
                        world_x = xrv + (_x - xrp) * xic
                        world_y = yrv + (_y - yrp) * yic
                        title = '(IF, POL, X, Y) = (%s, %s, %s, %s)\n%s %s' % (self.spw, pol, _x, _y, HHMMSSss(world_x), DDMMSSs(world_y))
#                         if self.num_valid_spectrum[_x][_y][pol] > 0:
                        if mask2d[_x][_y]:
                            a.plot(self.frequency, data[_x][_y], Mark, markersize=2, markeredgecolor='b',
                                   markerfacecolor='b')
                        else:
                            a.text((xmin + xmax) / 2.0, (ymin + ymax) / 2.0, 'NO DATA', horizontalalignment='center',
                                   verticalalignment='center', size=TickSize)
                        a.title.set_text(title)
                        a.axis([xmin, xmax, ymin, ymax])
                    else:
                        a = axes_list[NSp]
                        a.set_axis_off()
                        a.title.set_text('')

                NSp += 1
                if NSp >= (NhPanel * NvPanel) or (irow == len(ROWS)-1 and mode.upper() != 'RASTER'):
                    NSp = 0

                    prefix = self.inputs.imagename+'.pol%s_Result' % pol
                    plotfile = os.path.join(self.stage_dir, prefix+'_%s.png' % Npanel)
                    if not os.path.exists(plotfile):
                        LOG.debug('Regenerate plot: %s' % plotfile)
                        fig.savefig(plotfile, dpi=DPIDetail)
                    else:
                        LOG.debug('Use existing plot: %s' % plotfile)

                    for _a in axes_list:
                        for obj in itertools.chain(_a.lines[:], _a.texts[:], _a.patches[:], _a.images[:]):
                            obj.remove()
                            del obj

                    parameters = {}
                    parameters['intent'] = 'TARGET'
                    parameters['spw'] = self.inputs.spw
                    parameters['pol'] = self.image.stokes[pol] #polmap[pol]
                    parameters['ant'] = self.inputs.antenna
                    parameters['type'] = 'sd_spectral_map'
                    parameters['file'] = self.inputs.imagename
                    parameters['field'] = self.inputs.source
                    parameters['vis'] = 'ALL'

                    plot = logger.Plot(plotfile,
                                       x_axis='Frequency',
                                       y_axis='Intensity',
                                       field=self.inputs.source,
                                       parameters=parameters)
                    plot_list.append(plot)

                    Npanel += 1
            del data, mask2d
        del ROWS
        del axes_manager
        fig.clf()
        del fig
        print("Returning {} plots from spectralmap".format(len(plot_list)))
        return plot_list


class SDSpectralImageDisplay(SDImageDisplay):
    """Plotter for science spectral window."""

    def __plot(self, display_cls: SDImageDisplay,
               prologue: Optional[Callable[[SDImageDisplay], None]] = None,
               epilogue: Optional[Callable[[SDImageDisplay], None]] = None) -> List[logger.Plot]:
        """Generate Plot list using given display class.

        Args:
            display_cls: display class
            prologue: Function to execute before plot method is called.
                      The function must take SDImageDisplay instance as its argument.
                      Defaults to None.
            epilogue: Function to execute after plot method is called.
                      The function must take SDImageDisplay instance as its argument.
                      Defaults to None.

        Returns:
            List of Plot instances.
        """
        worker = display_cls(self.inputs)
        if prologue:
            prologue(worker)
        plot_list = worker.plot()
        if epilogue:
            epilogue(worker)
        return plot_list

    @casa5style_plot
    def plot(self) -> List[logger.Plot]:
        """Create Plot instances for science spectral windows.

        It creates the following plots from the single inputs.
        They are returned as a plain list.

          - sparse spectral map
          - channel map
          - detailed spectral map
          - baseline rms map
          - moment map (max intensity map)

        Returns:
            List of Plot instances.
        """
        plot_list = []
        t0 = time.time()
        plot_list.extend(
            self.__plot(SDSparseMapDisplay, lambda x: x.enable_atm())
        )
        t1 = time.time()
        LOG.debug('sparse_map: elapsed time %s sec' % (t1-t0))
        plot_list.extend(
            self.__plot(SDChannelMapDisplay)
        )
        t2 = time.time()
        LOG.debug('channel_map: elapsed time %s sec' % (t2-t1))
        # skip spectral map (detailed profile map) if the data is NRO
        if not self.inputs.isnro:
            plot_list.extend(
                self.__plot(SDSpectralMapDisplay)
            )
        t3 = time.time()
        LOG.debug('spectral_map: elapsed time %s sec' % (t3-t2))
        plot_list.extend(
            self.__plot(SDRmsMapDisplay)
        )
        t4 = time.time()
        LOG.debug('rms_map: elapsed time %s sec' % (t4-t3))
        plot_list.extend(
            self.__plot(SDMomentMapDisplay)
        )
        t5 = time.time()
        LOG.debug('moment_map: elapsed time %s sec' % (t5-t4))

        # contamination plots
        plot_list.extend(self.add_contamination_plot())

        return plot_list

    def add_contamination_plot(self) -> List[logger.Plot]:
        """Return list of Plot instances for contamination plot.

        Plot instance is created only when input has valid file
        name of contamination plot for "combined" image.

        Returns:
            List of Plot instances.
        """
        plotfile = os.path.join(self.stage_dir, self.inputs.contamination_plot)
        if self.inputs.antenna == 'COMBINED' and os.path.exists(plotfile):
            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.inputs.spw
            parameters['pol'] = 'I'
            parameters['ant'] = 'COMBINED'
            parameters['type'] = 'sd_contamination_map'
            parameters['file'] = self.inputs.imagename
            parameters['field'] = self.inputs.source
            parameters['vis'] = 'ALL'

            plot = logger.Plot(plotfile,
                               x_axis='Frequency',
                               y_axis='Intensity',
                               field=self.inputs.source,
                               parameters=parameters)

            return [plot]
        else:
            return []


def SDImageDisplayFactory(mode: str) -> Union[SDChannelAveragedImageDisplay, SDSpectralImageDisplay]:
    """Return appropriate display class for plotting.

    If mode is "TP", SDChannelAveragedImageDisplay is returned.
    Otherwise, SDSpectralImageDisplay is returned.

    Args:
        mode: Type of the spectral window.

    Returns:
        Appropriate display class
    """
    LOG.debug('MODE=%s' % (mode))
    if mode == 'TP':
        return SDChannelAveragedImageDisplay

    else:
        # mode should be 'SP'
        return SDSpectralImageDisplay
