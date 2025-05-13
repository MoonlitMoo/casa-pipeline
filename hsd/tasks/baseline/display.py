"""Set of plotting classes for hsd_baseline task."""
import abc
import math
import os
import string
import time

from typing import TYPE_CHECKING, Generator, List, Tuple, Union

import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.displays.pointing as pointing
from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.hsd.tasks.common.display import DPISummary, DPIDetail, SingleDishDisplayInputs, LightSpeed
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.pointing import MapAxesManagerBase
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from ..common import direction_utils as dirutil

from .typing import LineProperty

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from matplotlib.ticker import Formatter, Locator

    from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)

# ShowPlot = True
ShowPlot = False

RArotation = pointing.RArotation
DECrotation = pointing.DECrotation


class ClusterValidationAxesManager(MapAxesManagerBase):
    """Creates and manages Axes instances for cluster validation plot.

    Cluster validation plot displays validation result of all detected
    clusters in one panel.
    """

    # upper limit of the number of clusters to be displayed
    NUM_CLUSTER_MAX = 36

    def __init__(self,
                 clusters_to_plot: List[LineProperty],
                 nh: int,
                 nv: int,
                 aspect_ratio: float,
                 xformatter: 'Formatter',
                 yformatter: 'Formatter',
                 xlocator: 'Locator',
                 ylocator: 'Locator',
                 xrotation: float,
                 yrotation: float,
                 ticksize: Union[float, str],
                 labelsize: Union[float, str],
                 titlesize: Union[float, str]) -> None:
        """Construct ClusterValidationAxesManager instance.

        Args:
            clusters_to_plot: List of detected lines.
                              Its length must not exceed NUM_CLUSTER_MAX.
            nh: Number of plots along horizontal direction
            nv: Number of plots along vertical axis
            aspect_ratio: Aspect ratio of each plot
            xformatter: Formatter for x axis
            yformatter: Formatter for y axis
            xlocator: Locator for x axis
            ylocator: Locator for y axis
            xrotation: Tick label rotation for x axis
            yrotation: Tick label rotation for y axis
            ticksize: Tick label font size
            labelsize: Axis label font size
            titlesize: Title font size

        Raises:
            RuntimeError: len(clusters_to_plot) > self.NUM_CLUSTER_MAX
        """
        super(ClusterValidationAxesManager, self).__init__()
        self.clusters_to_plot = clusters_to_plot
        if len(self.clusters_to_plot) > self.NUM_CLUSTER_MAX:
            raise RuntimeError(f'Length of cluster must not exceed {self.NUM_CLUSTER_MAX}.')
        self.nh = nh
        self.nv = nv
        self.aspect_ratio = aspect_ratio
        self.xformatter = xformatter
        self.yformatter = yformatter
        self.xlocator = xlocator
        self.ylocator = ylocator
        self.xrotation = xrotation
        self.yrotation = yrotation
        self.ticksize = ticksize
        self.labelsize = labelsize
        self.titlesize = titlesize
        self._legend = None
        self._axes = None
        self.legend_y = 0.85

    @property
    def axes_legend(self) -> 'Axes':
        """Return Axes for legend.

        Returns:
            Axes instance for legend
        """
        if self._legend is None:
            # self._legend = plt.axes([0.0, 0.85, 1.0, 0.15])
            self._legend = plt.axes([0.0, self.legend_y, 1.0, 1.0 - self.legend_y])
            self._legend.set_axis_off()

        return self._legend

    @property
    def axes_list(self) -> List[Tuple[LineProperty, 'Axes', float, float]]:
        """Return list of necessary resources to generate cluster validation plots.

        Returns:
            List of tuple of line property (line center, line width, validated),
            Axes instance for plot, and x and y locations of plot title.
        """
        if self._axes is None:
            self._axes = list(self.__axes_list())

        return self._axes

    def __axes_list(self) -> Generator[Tuple[LineProperty, 'Axes', float, float], None, None]:
        """Yield resources to generate cluster validation plot.

        Yields:
            See docstring for axes_list.
        """
        for icluster in self.clusters_to_plot:
            loc = self.clusters_to_plot.index(icluster)
            ix = loc % self.nh
            iy = int(loc // self.nh)

            ( x0, y0, x1, y1, tpos_x, tpos_y ) = self.__calc_axes(plt.gcf(), ix, iy)
            axes = plt.axes([x0, y0, x1, y1])

            # 2008/9/20 DEC Effect
            axes.set_aspect(self.aspect_ratio)
            #axes.set_aspect('equal')
            xlabel, ylabel = self.get_axes_labels()
            # fold ylabel if there are many panels
            if self.nv > 3:
                ylabel = ylabel.replace( '(', '\n(', 1 )

            axes.set_xlabel( xlabel, size=self.labelsize, labelpad=2)
            axes.set_ylabel( ylabel, size=self.labelsize, labelpad=2)
            axes.xaxis.set_major_formatter(self.xformatter)
            axes.yaxis.set_major_formatter(self.yformatter)
            axes.xaxis.set_major_locator(self.xlocator)
            axes.yaxis.set_major_locator(self.ylocator)
            axes.tick_params( axis='x', pad=1, labelrotation=self.xrotation, labelsize=self.labelsize, length=self.ticksize/2 )
            axes.tick_params( axis='y', pad=1, labelrotation=self.yrotation, labelsize=self.labelsize, length=self.ticksize/2 )
            xlabels = axes.get_xticklabels()
            ylabels = axes.get_yticklabels()

            yield icluster, axes, tpos_x, tpos_y

    def __calc_axes(self, fig: 'Figure', ix: int, iy: int) -> Tuple[float, float, float, float, float, float]:
        """Calculate parameters for Axes.

        Args:
            fig: Figure instance
            ix: horizontal location of the Axes
            iy: Vertical location of the Axes

        Returns:
            four float values to form rectangle (left, bottom, width, height) for the Axes,
            plus x and y locations of the title.
        """
        # unit conversion constant for points->inch
        ppi = 72
        # padding between panels (unit: points)
        ( px, py ) = ( 7, 11 )

        # title vertical position
        title_v = 1.7

        # label extent
        label_extent = 0.014

        # axes size limit (unit: points)
        limit = 240

        # figure size (unit: points)
        fx = fig.get_figwidth() * ppi
        fy = fig.get_figheight() * ppi

        # margins at figure edge
        mx1 = fx * 0.01     # left
        mx2 = fx * 0.04     # right
        my1 = 0.0           # bottom
        my2 = fy * 0.08     # top

        # label extents (unit: points)
        lx = fx * label_extent * self.labelsize
        ly = fy * label_extent * self.labelsize

        # panel boundary max including ticks and labels
        max_x = ( fx - mx1 - mx2 - px*(self.nh-1) ) / self.nh
        max_y = ( fy * self.legend_y - my1-my2 - py*(self.nv-1)) / self.nv

        # limit the panel size
        if max_x > limit and max_y - ly*2 > limit:
            max_x = limit
            max_y = limit

        # extent and offset of plot area
        extent_x = max_x * self.nh + px * (self.nh - 1)
        extent_y = max_y * self.nv + py * (self.nv - 1)
        offset_x = ( fx - extent_x ) / 2
        offset_y = ( fy*self.legend_y - extent_y ) / 2

        # calculate axes parameters
        ax = max_x - lx
        ay = max_y - title_v*self.titlesize - ly

        x1 = ax / fx
        if self.nh == 1:
            x0 = 0.5 - x1/2.0
        else:
            x0 = (((max_x+px) * ix + lx + mx1 + offset_x) ) / fx
        y1 = ay / fy
        y0 = ((max_y+py) * (self.nv-iy-1) + ly + my1 + offset_y) / fy

        # relative position of the title
        if self.nh < 4:
            tpos_x = 0.5            # locate title at axes center
        else:
            tpos_x = (ax-lx)/(2*ax) # locate title at panel center
        tpos_y = 1.008              # equiv. to titlepad

        return x0, y0, x1, y1, tpos_x, tpos_y


class ClusterDisplay(object):
    """Plotter to create plots that visualize clustering analysis.

    For each detected cluster, this class creates two types of
    plots described below:

        1. Cluster validation plot (ClusterValidationDisplay):
            displays spatial distribution of detected
            lines associated with the cluster

        2. Cluster property plot (ClusterPropertyDisplay):
            displays property of the cluster in two dimensional
            space of line center against line width of the
            detected lines
    """

    Inputs = SingleDishDisplayInputs

    def __init__(self, inputs: Inputs) -> None:
        """Construct ClusterDisplay instance.

        Args:
            inputs: Inputs instance
        """
        self.inputs = inputs

    @property
    def context(self) -> 'Context':
        """Return Pipeline context.

        Returns:
            Pipeline context
        """
        return self.inputs.context

    def __baselined(self) -> Generator[dict, None, None]:
        """Yield outcome of the baseline subtraction including property of detected lines.

        Yields:
            Dictionary containing a result of baseline subtraction
        """
        for group in self.inputs.result.outcome['baselined']:
            if 'clusters' in group and 'lines' in group:
                yield group

    @casa5style_plot
    def plot(self) -> List[logger.Plot]:
        """Create list of plots for clustering analysis result.

        Returns:
            List of plot objects
        """
        plot_list = []

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % (self.inputs.result.stage_number))
        start_time = time.time()
        reduction_group = self.context.observing_run.ms_reduction_group
        for group in self.__baselined():
            group_id = group['group_id']
            cluster = group['clusters']
            flag_digits = group['flag_digits']
            org_direction = group['org_direction']
            lines = group['lines']
            is_all_invalid_lines = all([l[2] == False for l in lines])
            rep_member_id = group['members'][0]
            rep_member = reduction_group[group_id][rep_member_id]

            ## now judgement to plot is done exclusively in ClusterValidationDisplay._plot()
            #
            # if 'cluster_score' not in cluster or is_all_invalid_lines:
            #     # it should be empty cluster (no detection) or false clusters (detected but
            #     # judged as an invalid clusters) so skip this cycle
            #     continue
            #
            # # skip the cycle for cluster with no lines validated at final stage
            # flags = cluster['cluster_flag']
            # final_flags = ( flags // flag_digits['final'] ) % 10
            # if ( final_flags == 0 ).all():
            #    continue

            if 'index' in group:
                # having key 'index' indicates the result comes from old (Scantable-based)
                # procedure
                antenna = group['index'][0]
                vis = None
            else:
                # having key 'antenna' instead of 'index' indicates the result comes from
                # new (MS-based) procedure
                antenna = rep_member.antenna_id
                vis = rep_member.ms.name
            spw = rep_member.spw_id
            field = rep_member.field_id
            ms = self.context.observing_run.get_ms(vis)
            virtual_spw = self.context.observing_run.real2virtual_spw_id(spw, ms)
            source_name = ms.fields[field].source.name.replace(' ', '_').replace('/', '_')
            iteration = group['iteration']

            t0 = time.time()
            plot_validation = ClusterValidationDisplay(self.context, group_id, iteration, cluster,
                                                       flag_digits, vis,
                                                       virtual_spw, source_name, antenna, lines, stage_dir,
                                                       org_direction )
            validation_plot = plot_validation.plot()
            # if there are no validated lines, then skip all the plots
            if len(validation_plot) == 0:
                continue
            plot_list.extend(validation_plot)
            t1 = time.time()

            plot_property = ClusterPropertyDisplay(group_id, iteration, cluster, virtual_spw, source_name, stage_dir)
            plot_list.extend(plot_property.plot())
            t2 = time.time()

            LOG.debug('PROFILE: ClusterPropertyDisplay elapsed time is %s sec' % (t2-t1))
            LOG.debug('PROFILE: ClusterValidationDisplay elapsed time is %s sec' % (t1-t0))

        end_time = time.time()
        LOG.debug('PROFILE: plot elapsed time is %s sec'%(end_time-start_time))

        return plot_list


class ClusterDisplayWorker(object, metaclass=abc.ABCMeta):
    """Base class for plotter class."""

    MATPLOTLIB_FIGURE_ID = 8907

    def __init__(self,
                 group_id: int, iteration: int, cluster: dict,
                 spw: int, field: str, stage_dir: str) -> None:
        """Construct ClusterDisplayWorker instance.

        Args:
            group_id: Reduction group id
            iteration: Iteration counter for baseline/blflag loop
            cluster: Cluster property
            spw: Virtual spw id
            field: Field name
            stage_dir: Weblog directory
        """
        self.group_id = group_id
        self.iteration = iteration
        self.cluster = cluster
        self.spw = spw
        self.field = field
        self.stage_dir = stage_dir

    def plot(self) -> List[logger.Plot]:
        """Create plots.

        Returns:
            List of Plot objects
        """
        if ShowPlot:
            plt.ion()
        else:
            plt.ioff()
        plt.figure(self.MATPLOTLIB_FIGURE_ID)
        if ShowPlot:
            plt.ioff()

        plt.cla()
        plt.clf()

        return list(self._plot())

    def _create_plot(self, plotfile: str, type: str, x_axis: str, y_axis: str) -> logger.Plot:
        """Create Plot object.

        Args:
            plotfile: Name of the graphic file (PNG)
            type: Type of the plot
            x_axis: Description of X-axis
            y_axis: Description of Y-axis

        Returns:
            Plot object
        """
        parameters = {}
        parameters['intent'] = 'TARGET'
        parameters['spw'] = self.spw # spw id should be virtual one
        parameters['pol'] = 0
        parameters['ant'] = 'all'
        parameters['type'] = type
        plot_obj = logger.Plot(plotfile,
                               x_axis=x_axis,
                               y_axis=y_axis,
                               field=self.field,
                               parameters=parameters)
        return plot_obj

    @abc.abstractmethod
    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Yield plot objects.

        This must be implemented in each subclass.

        Raises:
            NotImplementedError: always raise an exception
        """
        raise NotImplementedError


class ClusterPropertyDisplay(ClusterDisplayWorker):
    """Plotter to create cluster property display."""

    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Yield plot object for cluster property plot.

        Cluster property plot displays property of the cluster
        in two dimensional space of line center against line
        width of the detected lines.

        Yields:
            Plot object for cluster property plot
        """
        lines = self.cluster['detected_lines']
        properties = self.cluster['cluster_property']
        scaling = self.cluster['cluster_scale']

        sorted_properties = sorted(properties)
        width = lines[:, 0]
        center = lines[:, 1]
        plt.plot(center, width, 'bs', markersize=1)
        [xmin, xmax, ymin, ymax] = plt.axis()
        axes = plt.gcf().gca()
        cluster_id = 0
        for [cx, cy, dummy, r] in sorted_properties:
            radius = r * scaling
            aspect = 1.0 / scaling
            x_base = cx
            y_base = cy * scaling
            pointing.draw_beam(axes, radius, aspect, x_base, y_base, offset=0)
            plt.text(x_base, y_base, str(cluster_id), fontsize=10, color='red')
            cluster_id += 1
        plt.xlabel('Line Center (Channel)', fontsize=11)
        plt.ylabel('Line Width (Channel)', fontsize=11)
        plt.axis([xmin - 1, xmax + 1, 0, ymax + 1])
        plt.title('Clusters in the line Center-Width space\n\nRed Oval(s) shows each clustering region. '
                 'Size of the oval represents cluster radius', fontsize=11)

        if ShowPlot:
            plt.draw()

        plotfile = os.path.join(self.stage_dir,
                                'cluster_property_group%s_spw%s_iter%s.png' % (self.group_id, self.spw, self.iteration))
        plt.savefig(plotfile, format='png', dpi=DPISummary)
        plot = self._create_plot(plotfile, 'line_property',
                                 'Line Center', 'Line Width')
        yield plot


class ClusterValidationDisplay(ClusterDisplayWorker):
    """Display class for cluster validation result."""

    Description1 = {
        'detection': 'Clustering Analysis at Detection stage',
        'validation': 'Clustering Analysis at Validation stage',
        'smoothing': 'Clustering Analysis at Smoothing stage',
        'final': 'Clustering Analysis at Final stage'
    }
    Description2 = {
        'detection': 'Yellow Square: Single spectrum is detected in the grid\nCyan Square: More than one spectra are detected in the grid\n',
        'validation': 'Validation by the rate (Number of clustering member [Nmember] v.s. Number of total spectra belong to the Grid [Nspectra])\n Blue Square: Validated: Nmember > ${valid} x Nspectra\nCyan Square: Marginally validated: Nmember > ${marginal} x Nspectra\nYellow Square: Questionable: Nmember > ${questionable} x Nspectrum\n',
        'smoothing': 'Blue Square: Passed continuity check\nCyan Square: Border\nYellow Square: Questionable\n',
        'final': 'Green Square: Final Grid where the line protection channels are calculated and applied to the baseline subtraction\nBlue Square: Final Grid where the calculated line protection channels are applied to the baseline subtraction\n\nIsolated Grids are eliminated.\n'
    }

    def __init__(self,
                 context: 'Context',
                 group_id: int,
                 iteration: int,
                 cluster: dict,
                 flag_digits: dict,
                 vis: str,
                 spw: int,
                 field: str,
                 antenna: int,
                 lines: List[LineProperty],
                 stage_dir: str,
                 org_direction: dirutil.Direction) -> None:
        """Construct ClusterValidationDisplay instance.

        Args:
            context: Pipeline context
            group_id: Reduction group id
            iteration: Iteration counter for baseline/blflag loop
            cluster: Cluster property
            vis: Name of MS
            spw: Virtual spw id
            field: Field name
            antenna: Antenna id (not used)
            lines: List of detected lines
            stage_dir: Weblog directory
            org_direction: direction of the origin
        """
        super(ClusterValidationDisplay, self).__init__(group_id, iteration, cluster, spw, field, stage_dir)
        self.context = context
        self.antenna = antenna
        self.lines = lines
        self.flag_digits = flag_digits
        self.vis = vis
        self.org_direction = org_direction

    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Yield plot object for cluster validation plots.

        Cluster validation plot displays spatial distribution of
        detected lines associated with the cluster. Plots are
        created for each validation step (detection, validation,
        smoothin, and final).

        Returns:
            None (on failure)

        Yields:
            Plot objects for cluster validation plots
        """
        plt.clf()

        marks = ['gs', 'bs', 'cs', 'ys']

        if 'cluster_flag' not in self.cluster:
            return None

        # list up iclusters of clusters to plot
        flags = self.cluster['cluster_flag']
        final_flags = (flags // self.flag_digits['final']) % 10
        valid_clusters = [i for i in range(len(final_flags)) if self.lines[i][2]]
        n_valid_clusters = len(valid_clusters)
        if n_valid_clusters > ClusterValidationAxesManager.NUM_CLUSTER_MAX:
            npanel = ClusterValidationAxesManager.NUM_CLUSTER_MAX
            LOG.warning(
                f'Field {self.field} vspw {self.spw}: '
                'Too many clusters to display. '
                f'Only {npanel} out of {n_valid_clusters} clusters are shown '
                'in the cluster validation plot.'
            )
            clusters_to_plot = valid_clusters[:npanel]
        else:
            clusters_to_plot = valid_clusters

        num_cluster = len(clusters_to_plot)
        # num_cluster = len(self.cluster['cluster_property'])

        # no clusters to plot
        if num_cluster == 0:
            return None

        num_panel_h = int(math.sqrt(num_cluster - 0.1)) + 1
        num_panel_v = int((num_cluster-0.1) // num_panel_h) + 1

        # num_panel_v = num_panel_h
        ra0 = self.cluster['grid']['ra_min']
        dec0 = self.cluster['grid']['dec_min']
        scale_ra = self.cluster['grid']['grid_ra']
        scale_dec = self.cluster['grid']['grid_dec']

        # convert ra0/dec0 to SHIFT_RA/DEC and adjust scale_ra for Ephemeris sources
        if self.org_direction is not None:
            ra1, dec1 = dirutil.direction_recover( ra0, dec0, self.org_direction )
            ra2, dec2 = dirutil.direction_recover( ra0+scale_ra, dec0, self.org_direction )
            scale_ra = ra2 - ra1
            ra0, dec0 = ra1, dec1

        # 2008/9/20 DEC Effect
        aspect_ratio = 1.0 / math.cos(dec0 / 180.0 * 3.141592653)

        # common message for legends
        scale_msg = self.__scale_msg(scale_ra, scale_dec, aspect_ratio)

        # Plotting parameters
        nx = len(self.cluster['cluster_flag'][0])
        ny = len(self.cluster['cluster_flag'][0][0])
        xmin = ra0
        xmax = nx * scale_ra + xmin
        ymin = dec0
        ymax = ny * scale_dec + ymin
        tick_size, label_size, title_size = self.__set_size( num_panel_h, num_panel_v )
        # direction reference
        reference_ms = self.context.observing_run.measurement_sets[0]
        datatable_name = os.path.join(self.context.observing_run.ms_datatable_name, reference_ms.basename)
        datatable = DataTable()
        datatable.importdata(datatable_name, minimal=False, readonly=True)
        direction_reference = datatable.direction_ref
        del datatable

        span = max(xmax - xmin, ymax - ymin)
        (RAlocator, DEClocator, RAformatter, DECformatter) = pointing.XYlabel(span,
                                                                              direction_reference)

        axes_manager = ClusterValidationAxesManager(clusters_to_plot,
                                                    num_panel_h,
                                                    num_panel_v,
                                                    aspect_ratio,
                                                    RAformatter,
                                                    DECformatter,
                                                    RAlocator,
                                                    DEClocator,
                                                    RArotation,
                                                    DECrotation,
                                                    tick_size,
                                                    label_size,
                                                    title_size )
        axes_manager.direction_reference = direction_reference
        axes_db = axes_manager.axes_list
        axes_list = { k: v for ( k, v, x, y ) in axes_db }
        title_pos = { k: [x, y] for ( k, v, x, y ) in axes_db }
        axes_legend = axes_manager.axes_legend

        for (mode, data, threshold, description1, description2) in self.__stages():
            plot_objects = []

            for icluster in clusters_to_plot:
                axes_cluster = axes_list[icluster]
                axes_cluster.axis([xmax, xmin, ymin, ymax])

                # calculate the optimum marker_size for axes
                marker_size = self.__marker_size( axes_cluster, nx, ny )

                xdata = []
                ydata = []
                for i in range(len(threshold)):
                    xdata.append([])
                    ydata.append([])
                for ix in range(nx):
                    for iy in range(ny):
                        for i in range(len(threshold)):
                            if data[icluster][ix][iy] == len(threshold) - i:
                                xdata[i].append(xmin + (0.5 + ix) * scale_ra)
                                ydata[i].append(ymin + (0.5 + iy) * scale_dec)
                                break

                # Convert Channel to Frequency and Velocity
                #ichan = self.lines[icluster][0] + 0.5
                (frequency, width) = self.__line_property(icluster)

                # title_x = xmin + ( xmax-xmin ) * title_pos[icluster][0]
                ( title_x, title_y ) = title_pos[icluster]

                plot_objects.append(
                    axes_cluster.text( title_x, title_y,
                                       "Cluster {}\n"
                                       r"$f_\mathrm{{center}}$={:.4f} GHz $\Delta v$={:.1f} km/s".format(icluster, frequency, width),
                                       transform=axes_cluster.transAxes,
                                       linespacing=1,
                                       fontsize=title_size,
                                       horizontalalignment='center',
                                       verticalalignment='bottom'
                                   )
                )

                if self.lines[icluster][2] == False and mode == 'final':
                    if num_panel_h > 2:
                        _tick_size = tick_size
                    else:
                        _tick_size = tick_size + 1
                    plot_objects.append(
                        axes_cluster.text(0.5 * (xmin + xmax), 0.5 * (ymin + ymax),
                                'INVALID CLUSTER',
                                horizontalalignment='center',
                                verticalalignment='center',
                                size=_tick_size)
                        )
                else:
                    for i in range(len(threshold)):
                        plot_objects.extend(
                            axes_cluster.plot(xdata[i], ydata[i], marks[4 - len(threshold) + i], markersize=marker_size)
                        )

                # Legends
                plot_objects.append(
                    axes_legend.text( 0.5, 0.85, description1,
                             horizontalalignment='center',
                             verticalalignment='baseline', size=8 )
                )
                plot_objects.append(
                    axes_legend.text( 0.5, 0.0, description2+scale_msg,
                             horizontalalignment='center',
                             verticalalignment='baseline', size=8 )
                )
            if ShowPlot:
                plt.draw()

            plotfile = os.path.join(
                self.stage_dir,
                'cluster_group_%s_spw%s_iter%s_%s.png' % (self.group_id, self.spw, self.iteration, mode))
            plt.savefig(plotfile, format='png', dpi=DPISummary)

            for obj in plot_objects:
                obj.remove()

            plot = self._create_plot(plotfile, 'clustering_%s'%(mode),
                                     'R.A.', 'Dec.')
            yield plot

    def __set_size(self, num_panel_h: int, num_panel_v: int) -> Tuple[int, int, int]:
        """Calculate font sizes for tick label, axis label, and title.

        Args:
            num_panel_h: Number of panels along x-axis
            num_panel_v: Number of panels along y-axis

        Returns:
            font sizes for tick label, axis label, and title
        """
        tick_size = 6 + (1 // num_panel_h) * 2
        if num_panel_v > 3:
            label_size = tick_size - 1
            title_size = tick_size
        elif num_panel_h > 3:
            label_size = tick_size
            title_size = tick_size
        else:
            label_size = tick_size
            title_size = tick_size + 1
        return tick_size, label_size, title_size

    def __marker_size(self, axes: 'Axes', nx: int, ny: int, tile_gap: float = 0.0) -> float:
        """Calculate marker size.

        Args:
            axes: Axes instance
            nx: Number of panels along x-axis
            ny: Number of panels along y-axis
            tile_gap: Gaps between panels. Defaults to 0.0.

        Returns:
            Marker size
        """
        axes_bbox = axes.get_position()
        fig_width = axes.get_figure().get_figwidth()
        fig_height = axes.get_figure().get_figheight()
        ppi = 72 # constant for "Points per Inch"

        axes_width  = (axes_bbox.x1 - axes_bbox.x0 ) * fig_width * ppi
        axes_height = (axes_bbox.y1 - axes_bbox.y0 ) * fig_height * ppi

        size_h = axes_width  / (nx*(1.0+tile_gap))
        size_v = axes_height / (ny*(1.0+tile_gap))

        marker_size = min( size_h, size_v )

        return marker_size

    def __stages(self) -> Generator[Tuple[str, int, float, str, str], None, None]:
        """Yield base data for validation plot.

        Yields:
            Tuple of clustering step, numerical data to be plotted,
            threshold, and description of the step.
        """
        for key in self.flag_digits.keys():
            if 'cluster_flag' in self.cluster:
                # Pick up target digit
                _flag = self.cluster['cluster_flag']
                _digit = self.flag_digits[key]
                flag = ( _flag // _digit) % 10
                LOG.debug('flag=%s' % flag)
                threshold = self.cluster[key+'_threshold']
                desc1 = self.Description1[key]
                desc2 = self.Description2[key]
                if key == 'validation':
                    template = string.Template(desc2)
                    valid = '%.1f' % (threshold[0])
                    marginal = '%.1f' % (threshold[1])
                    questionable = '%.1f' % (threshold[2])
                    desc2 = template.safe_substitute(valid=valid,
                                                    marginal=marginal,
                                                    questionable=questionable)
                yield (key, flag, threshold, desc1, desc2)

    def __line_property(self, icluster: int) -> Tuple[float, float]:
        """Compute property of cluster.

        Compute property of cluster: line center frequency [GHz] and
        line width in velocity [km/s].

        Args:
            icluster: Cluster identifier

        Returns:
            Line center frequency and line width in velocity
        """
        reduction_group = self.context.observing_run.ms_reduction_group[self.group_id]
        field = reduction_group[0].field
        source_id = field.source_id
        ms = self.context.observing_run.get_ms(self.vis)
        real_spw = self.context.observing_run.virtual2real_spw_id(self.spw, ms)
        spectral_window = ms.get_spectral_window(real_spw)
        refpix = 0
        refval = spectral_window.channels.chan_freqs[0]
        increment = spectral_window.channels.chan_widths[0]
        with casa_tools.TableReader(os.path.join(self.vis, 'SOURCE')) as tb:
            tsel = tb.query('SOURCE_ID == %s && SPECTRAL_WINDOW_ID == %s' % (source_id, real_spw))
            try:
                if tsel.nrows() == 0:
                    rest_frequency = refval
                else:
                    if tsel.iscelldefined('REST_FREQUENCY', 0):
                        rest_frequency = tsel.getcell('REST_FREQUENCY', 0)[0]
                    else:
                        rest_frequency = refval
            finally:
                tsel.close()

        # line property in channel
        line_center = self.lines[icluster][0]
        line_width = self.lines[icluster][1]

        center_frequency = refval + (line_center - refpix) * increment
        width_in_frequency = abs(line_width * increment)

        center_frequency *= 1.0e-9  # Hz -> GHz
        width_in_velocity = width_in_frequency / rest_frequency * LightSpeed

        return center_frequency, width_in_velocity

    def __scale_msg(self, scale_ra: float, scale_dec: float, aspect_ratio: float) -> str:
        """Construct a string describing size of square in the plot.

        Args:
            scale_ra: Physical size (angle) of horizontal side of the square
            scale_dec: Physical size (angle) of vertical side of the square
            aspect_ratio: Aspect ratio of the plot

        Returns:
            String describing physical size of square in the plot
        """
        if scale_ra >= 1.0:
            unit = 'degree'
            scale_factor = 1.0
        elif scale_ra * 60.0 >= 1.0:
            unit = 'arcmin'
            scale_factor = 60.0
        else:
            unit = 'arcsec'
            scale_factor = 3600.0
        ra_text = scale_ra / aspect_ratio * scale_factor
        dec_text = scale_dec * scale_factor

        return 'Scale of the Square (Grid): %.1f x %.1f (%s)' % (ra_text, dec_text, unit)
