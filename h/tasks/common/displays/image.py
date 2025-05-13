import os
import re
import string
import textwrap

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from matplotlib.colors import ColorConverter, Colormap, Normalize
from matplotlib.patches import Rectangle
from numpy import ma

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)

_valid_chars = "_.%s%s" % (string.ascii_letters, string.digits)

flag_color = {'outlier': 'red',
              'high outlier': 'orange',
              'low outlier': 'yellow',
              'too many flags': 'lightblue',
              'too many entirely flagged': 'darkblue',
              'nmedian': 'darkred',
              'max abs': 'pink',
              'min abs': 'darkcyan',
              'bad quadrant': 'yellow',
              'bad antenna': 'red'}


def _char_replacer(s):
    """
    A small utility function that echoes the argument or returns '_' if the
    argument is in a list of forbidden characters.
    """
    if s not in _valid_chars:
        return '_'
    return s


def sanitize(text):
    filename = ''.join(_char_replacer(c) for c in text)
    return filename


class ImageDisplay(object):

    @staticmethod
    def _findchunks(times):
        """
        Return a list of arrays, each containing the indices of a chunk
        of data i.e. a sequence of equally spaced measurements separated
        from other chunks by larger time gaps.

        Keyword arguments:
        times -- Numeric array of times at which the measurements
                 were taken.
        """
        difference = times[1:] - times[:-1]
        median_diff = np.median(difference)

        chunks = []
        chunk = [0]
        for i in np.arange(len(difference)):
            if difference[i] < 1.5 * median_diff:
                chunk.append(i+1)
            else:
                chunks.append(np.array(chunk))
                chunk = [i+1]
        chunks.append(np.array(chunk))
        return chunks

    @staticmethod
    def _get_plot_filename(result, prefix=''):
        fileparts = {
            'prefix': prefix,
            'datatype': result.datatype,
            'x': result.axes[0].name,
            'y': result.axes[1].name,
            'file': '' if result.filename is None else 'File_%s' % os.path.basename(result.filename),
            'intent': '' if result.intent == '' else 'Intent_%s' % result.intent.replace(',', '_'),
            'fieldname': '' if result.fieldname == '' else 'Field_%s' % result.fieldname.replace(',', '_'),
            'fieldid': '' if result.field_id is None else 'ID_%s' % str(result.field_id).replace(',', '_'),
            'pol': '' if result.pol is None else 'Pol_%s' % result.pol.replace(',', '_'),
        }

        if result.spw == '':
            fileparts['spw'] = ''
        else:
            # format spws for filename sorting
            spws = ['%0.2d' % int(spw) for spw in str(result.spw).split(',')]
            fileparts['spw'] = 'SpW_%s' % '_'.join(spws)

        if result.ant is None or result.ant == '':
            fileparts['ant'] = ''
        else:
            fileparts['ant'] = 'Ant_%s' % utils.find_ranges(result.ant)

        if result.scan is None or result.scan == '':
            fileparts['scan'] = ''
        else:
            fileparts['scan'] = 'Scan_%s' % utils.find_ranges(result.scan)

        if result.time is None or result.time == '':
            fileparts['time'] = ''
        else:
            # represent time sensibly relative to day start
            t = result.time - 86400.0 * np.floor(result.time/86400.0)
            h = int(np.floor(t/3600.0))
            t -= h * 3600.0
            m = int(np.floor(t/60.0))
            t -= m * 60.0
            s = int(np.floor(t))
            fileparts['time'] = '%sh%sm%ss' % (h, m, s)

        png = "{prefix}_{datatype}_{y}_vs_{x}_{file}_{intent}_{fieldname}_" \
              "{fieldid}_{spw}_{pol}_{ant}_{scan}_{time}.png".format(**fileparts)
        png = sanitize(png)

        # Maximum filename size for Lustre filesystems is 255 bytes.
        # ImageDisplayMosaics can exceed this limit due to including the IDs
        # of all antennas. Truncate filename while keeping it unique
        # by replacing with hash.
        if len(png) > 251:  # 255 - '.png'
            new_png = '{!s}.png'.format(hash(png))
            LOG.info('Renaming plot to avoid exceeding filesystem limit on filename length.\n'
                     'Old: {!s}\nNew: {!s}'.format(png, new_png))
            png = new_png

        return png

    def plot(self, context, results, reportdir, prefix='', change='Flagging', dpi=None):

        if not results:
            return []

        # Create a plot for each flagging view in the result.
        plots = []
        for description in sorted(results.descriptions()):
            # Derive output filename.
            plotfile = self._get_plot_filename(results.first(description), prefix)
            plotfile = os.path.join(reportdir, plotfile)

            # Create a plot object for the current flagging view, and store in
            # list of plots.
            plot = logger.Plot(
                plotfile,
                x_axis=results.first(description).axes[0].name, y_axis=results.first(description).axes[1].name,
                field=results.first(description).fieldname,
                parameters={'vis': os.path.basename(results.vis),
                            'intent': results.first(description).intent,
                            'spw': results.first(description).spw,
                            'pol': results.first(description).pol,
                            'ant': results.first(description).ant,
                            'type': results.first(description).datatype,
                            'file': os.path.basename(results.first(description).filename)})
            plots.append(plot)

            # If the plot figure already exists on disk, then skip to next one.
            if os.path.exists(plotfile):
                LOG.trace('Not overwriting existing image at %s' % plotfile)
                continue
            # Otherwise create the plot figure.
            self._create_plot_file(context, results, description, change, plotfile, dpi=dpi)

        return plots

    def _create_plot_file(self, context, results, description, change, plotfile, dpi=None):
        # Retrieve metadata from context and result.
        stagenumber = context.stage
        ms = context.observing_run.get_ms(name=results.vis)
        antennas = ms.antennas
        flagcmds = results.flagcmds()

        # Depending on whether flagging occurred, create a 2 or 3-panel figure,
        # and plot the flagging view data panels.
        if len(flagcmds) > 0:
            nsubplots = 3
            fig, axs = plt.subplots(1, nsubplots, constrained_layout=True, gridspec_kw={'width_ratios': [3, 3, 2]})
            self._plot_panel(fig, axs[0], nsubplots, 1, results.first(description), 'Before %s' % change)
            self._plot_panel(fig, axs[1], nsubplots, 2, results.last(description), 'After')
        else:
            nsubplots = 2
            fig, axs = plt.subplots(1, nsubplots, constrained_layout=True, gridspec_kw={'width_ratios': [3, 1]})
            self._plot_panel(fig, axs[0], nsubplots, 1, results.first(description), '')

        # Reduce the padding of the constrained layout.
        fig.set_constrained_layout_pads(w_pad=0.04, h_pad=0.04)

        # # Plot the legend panel.
        self._plot_legend_panel(axs[-1], antennas, flagcmds)

        # Set figure title.
        figtitle = 'Stage %s - %s' % (stagenumber, description)
        fig.suptitle("\n".join(textwrap.wrap(figtitle, 100)), size='small')

        # Save the figure to file.
        plt.savefig(plotfile, dpi=dpi)
        plt.close(fig)

    def _plot_legend_panel(self, ax, antennas, flagcmds):
        """
        Plot the antenna and flagging legend information into a panel.

        Keyword arguments:
        ax                 -- Matplotlib Axes object for current panel.
        antennas           -- List of antennas.
        flagcmds           -- List of flagging commands.
        """
        # Do not show axes.
        ax.axis('off')

        # Plot the antenna legend.
        xoff = 0.
        yoff = 1.03
        xoffstart = xoff
        yoff = self.plottext(ax, xoffstart, yoff, 'Antenna key:', 40, mult=0.8)
        yoffstart = yoff
        for idx, antenna in enumerate(antennas):
            yoff = self.plottext(ax, xoff, yoff, '%s:%s' % (antenna.id, antenna.name), 40, mult=0.7)
            # Go to next column after every 22 antennas.
            if (idx + 1) % 22 == 0:
                yoff = yoffstart
                xoff += 0.4

        # Key for masked data.
        yoff = 0.30
        xlen = 0.20  # length of colour block
        ylen = 0.02  # height of colour block
        strlen = 20  # max length of string for flag reason
        rectyoff = -0.003  # y-off for colour block, to align with text

        # Always show "no data" and "cannot calculate" in the legend.
        yoff = self.plottext(ax, xoffstart, yoff, 'Key for masked data:', 45, mult=0.8)
        ax.add_patch(Rectangle((xoffstart, yoff+rectyoff), xlen, ylen, facecolor='indigo', edgecolor='indigo',
                               transform=ax.transAxes))
        yoff = self.plottext(ax, xoffstart + 0.25, yoff, 'no data', strlen, mult=0.8)
        ax.add_patch(Rectangle((xoffstart, yoff+rectyoff), xlen, ylen, facecolor='violet', edgecolor='violet',
                               transform=ax.transAxes))
        yoff = self.plottext(ax, xoffstart + 0.25, yoff, 'cannot calculate', strlen, mult=0.8)

        # Add key for data flagged during this stage.
        if len(flagcmds) > 0:
            rulesplotted = set()
            for flagcmd in flagcmds:
                if flagcmd.rulename == 'ignore':
                    continue
                if (flagcmd.rulename, flagcmd.ruleaxis, flag_color[flagcmd.rulename]) not in rulesplotted:
                    color = flag_color[flagcmd.rulename]
                    ax.add_patch(Rectangle((xoffstart, yoff+rectyoff), xlen, ylen, facecolor=color, edgecolor=color,
                                           transform=ax.transAxes))
                    if flagcmd.ruleaxis is not None:
                        yoff = self.plottext(ax, xoffstart + 0.25, yoff,
                                             '%s axis - %s' % (flagcmd.ruleaxis, flagcmd.rulename),
                                             strlen, mult=0.8)
                    else:
                        yoff = self.plottext(ax, xoffstart + 0.25, yoff, flagcmd.rulename, strlen, mult=0.8)
                    rulesplotted.update([(flagcmd.rulename, flagcmd.ruleaxis, color)])

    def _plot_panel(self, fig, ax, nplots, plotnumber, image, subtitle):
        """
        Plot the 2d data into one panel.

        Keyword arguments:
        fig                -- Matplotlib figure object.
        ax                 -- Matplotlib Axes object for current panel.
        nplots             -- The number of sub-plots on the page.
        plotnumber         -- The index of this sub-plot.
        image              -- The 2d data.
        subtitle           -- The title to be given to this subplot.
        """
        cc = ColorConverter()
        sentinels = {}

        flag = image.flag
        data = image.data
        flag_reason_plane = image.flag_reason_plane
        flag_reason_key = image.flag_reason_key
        xtitle = image.axes[0].name
        xdata = image.axes[0].data
        xunits = image.axes[0].units
        ytitle = image.axes[1].name
        ydata = image.axes[1].data
        # yunits = image.axes[1].units
        dataunits = image.units
        datatype = image.datatype

        # set sentinels at points with no data/violet. These should be
        # overwritten by other flag colours in a moment.
        data[flag != 0] = 2.0
        sentinels[2.0] = cc.to_rgb('violet')

        # set points to their flag reason
        data[flag_reason_plane > 0] = flag_reason_plane[flag_reason_plane > 0] + 10.0

        # sentinels to mark flagging.
        sentinel_set = set(np.ravel(flag_reason_plane))
        sentinel_set.discard(0)

        sentinelvalues = np.array(list(sentinel_set), float) + 10.0

        for sentinelvalue in sentinelvalues:
            sentinels[sentinelvalue] = cc.to_rgb(
                flag_color[flag_reason_key[int(sentinelvalue)-10]])

        # plot points with no data indigo.
        nodata = image.nodata
        data[nodata != 0] = 5.0
        sentinels[5.0] = cc.to_rgb('indigo')

        # calculate vmin, vmax without the sentinels. Leaving norm to do
        # this is not sufficient; the standard Normalize gets called
        # by something in matplotlib and initialises vmin and vmax incorrectly.
        sentinel_mask = np.zeros(np.shape(data), bool)
        for sentinel in sentinels:
            sentinel_mask += (data == sentinel)
        actual_data = data[np.logical_not(sentinel_mask)]
        # watch out for nans which mess up vmin, vmax
        actual_data = actual_data[np.logical_not(np.isnan(actual_data))]
        if len(actual_data):
            vmin = actual_data.min()
            vmax = actual_data.max()
        else:
            vmin = vmax = 0.0

        # set my own colormap and normalise to plot sentinels
        cmap = _SentinelMap(plt.cm.gray, sentinels=sentinels)
        norm = _SentinelNorm(vmin=vmin, vmax=vmax, sentinels=list(sentinels.keys()))
        cmap_sentinel_free = _SentinelMap(plt.cm.gray)
        norm_sentinel_free = Normalize(vmin=vmin, vmax=vmax)

        # make antenna x antenna plots square
        aspect = 'auto'
        cb_aspect = 40
        shrink = 0.8
        fraction = 0.15
        pad = 0.015
        if ('ANTENNA' in xtitle.upper()) and ('ANTENNA' in ytitle.upper()):
            aspect = 'equal'
            shrink = 0.4
            fraction = 0.1

        # look out for yaxis values that would trip up matplotlib
        if isinstance(ydata[0], str):
            if re.match(r'\d+&\d+', ydata[0]):
                # baseline - replace & by . and convert to float
                ydata_numeric = []
                for b in ydata:
                    ydata_numeric.append(float(b.replace('&', '.')))

                # highest baseline number is am.am where 'am' is the
                # largest antenna id. If this 34, for example, then
                # highest axis value will be 34.34 - must be changed
                # to 34.99 otherwise scale will not look right
                # (think, next baseline would be 35.00).
                am = int(ydata_numeric[-1])
                ydata_numeric[-1] = am + 0.99

                ydata_numeric = np.array(ydata_numeric)
                major_formatter = ticker.FormatStrFormatter('%05.2f')
                ax.yaxis.set_major_formatter(major_formatter)
            else:
                # any other string just replace by index
                ydata_numeric = np.arange(len(ydata))
        else:
            ydata_numeric = ydata

        # only plot y tick labels on first panel to avoid collision
        # between y tick labels for second panel with greyscale for
        # first
        if plotnumber > 1:
            ax.yaxis.set_major_formatter(ticker.NullFormatter())

        if 'ANTENNA' in xtitle.upper():
            if ydata_numeric[0] == ydata_numeric[-1]:
                # sometimes causes empty plots if min==max
                extent = [0, len(xdata)-1, ydata_numeric[0], ydata_numeric[-1] + 1]
            else:
                extent = [0, len(xdata)-1, ydata_numeric[0], ydata_numeric[-1]]
        else:
            if ydata_numeric[0] == ydata_numeric[-1]:
                # sometimes causes empty plots if min==max
                extent = [xdata[0], xdata[-1], ydata_numeric[0], ydata_numeric[-1]+1]
            else:
                extent = [xdata[0], xdata[-1], ydata_numeric[0], ydata_numeric[-1]]

        # If plotting by antenna, then extend limits of the axis to ensure that
        # the tick marks align correctly with the center of the antenna pixels.
        if 'ANTENNA' in xtitle.upper():
            extent[0] -= 0.5
            extent[1] += 0.5
        if 'ANTENNA' in ytitle.upper():
            extent[2] -= 0.5
            extent[3] += 0.5

        # If plotting by scan ID on y-xis, then adjust limits of axis to ensure
        # that the tick mark aligns correctly with center of scan rows.
        if 'SCAN' in ytitle.upper():
            if len(ydata) == 1:
                extent[2] = -0.5
                extent[3] = 0.5
            else:
                extent[2] -= 0.5
                extent[3] += 0.5

        # Plot the image array; transpose data to get [x,y] into [row,column]
        # expected by matplotlib
        img = ax.imshow(np.transpose(data), cmap=cmap, norm=norm, interpolation='nearest',
                        origin='lower', aspect=aspect, extent=extent)

        # Set y-axis title, only add this to the first panel.
        if plotnumber == 1:
            ax.set_ylabel(ytitle, size='medium')

        # Set x-axis title, add units to title if available.
        xlabel = xtitle
        if xunits:
            xlabel = '%s [%s]' % (xlabel, xunits)
        ax.set_xlabel(xlabel, size='medium')

        # Create the color-bar.
        # plot wedge, make tick numbers smaller, label with units
        if vmin == vmax:
            cb = fig.colorbar(plt.cm.ScalarMappable(norm=norm_sentinel_free, cmap=cmap_sentinel_free), ax=ax,
                              shrink=shrink, fraction=fraction, pad=pad, aspect=cb_aspect, ticks=[-1, 0, 1])
        else:
            cb = fig.colorbar(plt.cm.ScalarMappable(norm=norm_sentinel_free, cmap=cmap_sentinel_free),
                              ax=ax, shrink=shrink, fraction=fraction, pad=pad, aspect=cb_aspect)
        cb.formatter.set_scientific(True)
        cb.formatter.set_powerlimits((-2, 2))
        cb.ax.yaxis.set_offset_position('left')

        # Set size of y-tick labels on the color-bar.
        for label in cb.ax.get_yticklabels():
            label.set_fontsize('small')

        # Set a label for color-bar for the right-most panel, adding units if available.
        data_label = datatype if dataunits is None else '%s (%s)' % (datatype, dataunits)
        if nplots == 2 or plotnumber == 2:
            cb.set_label(data_label, fontsize='medium')

        # Rotate x tick labels to avoid them clashing
        ax.tick_params(axis='x', rotation=35)

        # If plotting with antenna on the x-axis, then modify the tick mark
        # layout.
        if 'ANTENNA' in xtitle.upper():
            # Offset the plot title to allow space for labels above upper
            # x-axis.
            ax.set_title(subtitle, fontsize='medium', y=1.06)

            # Set x-ticks explicitly for each antenna ID.
            xticks = np.arange(0, len(xdata), 1)

            # Set size of x-labels based on number of antennas, with minimum
            # label size of 5.
            xlabel_size = max(np.ceil(10 - len(xdata) // 9), 5)

            # Add labels for even-indices in antenna array.
            ax.set_xticks(xticks[::2])
            ax.set_xticklabels([str(x) for x in xdata[::2]], rotation=90)
            ax.xaxis.set_minor_locator(ticker.FixedLocator(xdata[1::2]))

            # Display ticks outside the plot for both axes and both sides;
            # further rotation tick labels.
            ax.tick_params(axis='both', which='both', direction='out')

            # Set x-label size.
            for label in ax.get_xticklabels():
                label.set_fontsize(xlabel_size)

            # Add labels for odd-indices in antenna array.
            if len(xdata) > 1:
                axt = ax.twiny()
                axt.set_xlim(ax.get_xlim())  # copy over limits.
                axt.set_xticks(xticks[1::2])
                axt.set_xticklabels([str(x) for x in xdata[1::2]], rotation=90)
                axt.xaxis.set_minor_locator(ticker.FixedLocator(xdata[::2]))

                # Display ticks outside the plot for both axes and both sides;
                # further rotation tick labels.
                axt.tick_params(axis='both', which='both', direction='out')

                # Set x-label size.
                for label in axt.get_xticklabels():
                    label.set_fontsize(xlabel_size)
        else:
            # Set plot title.
            ax.set_title(subtitle, fontsize='medium')

            # For x-axis, enable minor ticks
            ax.xaxis.set_minor_locator(ticker.AutoMinorLocator())

        # If plotting by time on y-axis, then disable automatic tickmarks,
        # and add labels manually for chunks of continguous time.
        if ytitle.upper() == 'TIME':
            ax.yaxis.set_major_locator(plt.NullLocator())
            if plotnumber == 1:
                # identify chunks of contiguous time
                chunks = self._findchunks(ydata)
                base_time = 86400.0 * np.floor(ydata[0]/86400.0)
                tim_plot = ydata - base_time

                for chunk in chunks:
                    t = tim_plot[chunk[0]]
                    h = int(np.floor(t/3600.0))
                    t -= h * 3600.0
                    m = int(np.floor(t/60.0))
                    t -= m * 60.0
                    s = int(np.floor(t))
                    tstring = '%sh%sm%ss' % (h, m, s)
                    ax.text(ax.axis()[0]-0.25, ydata[chunk[0]], tstring, fontsize=8, ha='right', va='bottom',
                            clip_on=False)

        # If plotting by baseline on y-axis, add minor tick marks (should mark
        # start of each new antenna)
        if 'BASELINE' in ytitle.upper():
            ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

        # If plotting by scan on y-axis, disable minor tick marks, and map
        # major tick marks to scan IDs.
        if 'SCAN' in ytitle.upper():
            ax.yaxis.set_minor_locator(ticker.NullLocator())
            yticks = np.arange(0, len(ydata), 1)
            ax.set_yticks(yticks)
            ax.set_yticklabels(ydata)

    @staticmethod
    def plottext(ax, xoff, yoff, text, maxchars, ny_subplot=1, mult=1):
        """
        Utility method to plot text and put line breaks in to keep the
        text within a given limit.

        Keyword arguments:
        ax         -- Matplotlib Axes object for current panel.
        xoff       -- world x coord where text is to start.
        yoff       -- world y coord where text is to start.
        text       -- Text to print.
        maxchars   -- Maximum number of characters before a newline is
                      inserted.
        ny_subplot -- Number of sub-plots along the y-axis of the page.
        mult       -- Factor by which the text fontsize is to be multiplied.
        """
        words = text.rsplit()
        words_in_line = 0
        line = ''
        for i in range(len(words)):
            temp = line + words[i] + ' '
            words_in_line += 1
            if len(temp) > maxchars:
                if words_in_line == 1:
                    while len(temp) > 0:
                        ax.text(xoff, yoff, temp[:maxchars], va='center',
                                fontsize=mult*8,
                                transform=ax.transAxes, clip_on=False)
                        temp = temp[min(len(temp), maxchars):]
                        yoff -= 0.03 * ny_subplot * mult
                    words_in_line = 0
                else:
                    ax.text(xoff, yoff, line, va='center', fontsize=mult*8,
                            transform=ax.transAxes, clip_on=False)
                    yoff -= 0.03 * ny_subplot * mult
                    line = words[i] + ' '
                    words_in_line = 1
            else:
                line = temp
        if len(line) > 0:
            ax.text(xoff, yoff, line, va='center', fontsize=mult*8,
                    transform=ax.transAxes, clip_on=False)
            yoff -= 0.02 * ny_subplot * mult
        yoff -= 0.02 * ny_subplot * mult
        return yoff


class _SentinelMap(Colormap):
    """Utility class for plotting sentinel pixels in colours."""

    def __init__(self, cmap, sentinels={}):
        """
        Constructor.

        Keyword arguments:
        """
        self.name = 'SentinelMap'
        cmap._init()
        self.cmap = cmap
        self._lut = cmap._lut
        self.N = cmap.N
        self.sentinels = sentinels
        self._isinit = True

    def __call__(self, scaledData, alpha=1.0, bytes=False):
        """Utility method."""
        rgba = self.cmap(scaledData, alpha, bytes)
        if bytes:
            mult = 255
        else:
            mult = 1

        for sentinel, rgb in self.sentinels.items():
            r, g, b = rgb
            # PIPE-1266: due to finite numerical precision, the scaledData may
            # get slightly altered within Matplotlib, which means any injected
            # sentinel values may not re-appear with the exact same value as
            # those registered in self.sentinels. Therefore, look for values
            # that are close to the sentinel within a very small tolerance.
            mask = np.isclose(scaledData, sentinel, rtol=1.e-12, atol=1.e-12)
            if np.ndim(rgba) == 3:
                rgba[:, :, 0][mask] = r * mult
                rgba[:, :, 1][mask] = g * mult
                rgba[:, :, 2][mask] = b * mult
                if alpha is not None:
                    rgba[:, :, 3] = alpha * mult
            elif np.ndim(rgba) == 2:
                rgba[:, 0][mask] = r * mult
                rgba[:, 1][mask] = g * mult
                rgba[:, 2][mask] = b * mult
                if alpha is not None:
                    rgba[:, 3] = alpha * mult

        return rgba


class _SentinelNorm(Normalize):
    """Normalise but leave sentinel values unchanged."""

    def __init__(self, vmin=None, vmax=None, clip=True, sentinels=[]):
        self.sentinels = sentinels
        super().__init__(vmin, vmax, clip)

    def __call__(self, value, clip=None):

        # remove sentinels, keeping a mask of where they were.
        # PIPE-1266: due to finite numerical precision, the scaledData may get
        # slightly altered within Matplotlib, which means any injected sentinel
        # values may not re-appear with the exact same value as those
        # registered in self.sentinels. Therefore, look for values that are
        # close to the sentinel within a very small tolerance.
        sentinel_mask = np.zeros(np.shape(value), bool)
        for sentinel in self.sentinels:
            sentinel_mask += np.isclose(value, sentinel, rtol=1.e-12, atol=1.e-12)
        sentinel_values = value[sentinel_mask]

        actual_data = value[np.logical_not(sentinel_mask)]
        if len(actual_data):
            value[sentinel_mask] = actual_data.min()
        value = ma.asarray(value)
        value = Normalize.__call__(self, value, clip)

        # restore sentinels
        value[sentinel_mask] = sentinel_values
        return value
