"""Pointing methods and classes."""
import gc
import math
from numbers import Integral
import os
from typing import List, Optional, Tuple, Union

from matplotlib.axes._axes import Axes
import matplotlib.figure as figure
from matplotlib.ticker import (AutoLocator, Formatter, FuncFormatter, Locator,
                               MultipleLocator)
import numpy as np

from pipeline.domain import Antenna, MeasurementSet
from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.domain.datatable import OnlineFlagIndex
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from pipeline.infrastructure.renderer.logger import Plot

LOG = infrastructure.get_logger(__name__)

RArotation = 90
DECrotation = 0

DPISummary = 90

dsyb = r'$^\circ$'
hsyb = ':'
msyb = ':'


def Deg2HMS(x: float, prec: int=0) -> List[str]:
    """
    Convert an angle in degree to hour angle.

    Example:
    >>> Deg2HMS(20.123, prec=7)
    ['01', '20', '29.5']

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        List of strings of hour, minute, and second values in a specified
        precision.

    """
    xx = x % 360
    cqa = casa_tools.quanta
    angle = cqa.angle(cqa.quantity(xx, 'deg'), prec=prec, form=['time'])[0]
    return angle.split(':')


def HHMM(x: float, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with HHMM format.

    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMM(20.123)
    '01:20'

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. HHMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        formatted strings of hour, minute values.

    """
    (h, m, s) = Deg2HMS(x, prec=6)
    return '%s%s%s' % (h, hsyb, m)


def __format_hms(x: float, prec: int=0) -> str:
    """
    Convert an angle in degree to hour angle with hms format.

    Example:
    >>> __format_hms(10.123)
    '00:40:30'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.

    """
    (h, m, s) = Deg2HMS(x, prec)
    return '%s%s%s%s%s' % (h, hsyb, m, msyb, s)


def HHMMSS(x: float, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with HHMMSS format.

    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMMSS(10.123)
    '00:40:30'

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. HHMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.

    """
    return __format_hms(x, prec=6)


def HHMMSSs(x: float, pos=None):
    """
    Convert an angle in degree to hour angle with HHMMSSs format.

    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMMSSs(10.123)
    '00:40:29.5'

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. HHMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.

    """
    return __format_hms(x, prec=7)


def HHMMSSss(x: float, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with HHMMSSss format.

    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMMSSss(10.123)
    '00:40:29.52'

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. HHMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.

    """
    return __format_hms(x, prec=8)


def HHMMSSsss(x: float, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with HHMMSSsss format.

    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMMSSsss(10.123)
    '00:40:29.520'

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. HHMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.

    """
    return __format_hms(x, prec=9)


def Deg2DMS(x: float, prec: int=0) -> List[str]:
    r"""
    Convert an angle in degree to dms angle (ddmmss.s).

    Example:
    >>> Deg2DMS('+01.02.23.4', prec=1)
    ['+01', '02', '23.4']

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        prec: Significant digits.
    Returns:
        A list of strings of degree, arcminute, and arcsecond values in a
        specified precision.

    """
    xxx = (x + 90) % 180 - 90
    xx = abs(xxx)
    sign = '-' if xxx < 0 else '+'
    cqa = casa_tools.quanta
    dms_angle = cqa.angle(cqa.quantity(xx, 'deg'), prec=prec)[0]
    seg = dms_angle.split('.')
    assert len(seg) < 5 and len(seg) > 0
    # force degree in %02d format and add sign. qa.angle always retrun positive angle
    seg[0] = ( '%s%02d' % (sign, int(seg[0][1:])) )
    if len(seg) == 4:
        return (seg[0], seg[1], str('.').join(seg[2:]))
    else:
        return seg


def DDMM(x: float, pos=None) -> str:
    r"""Convert an angle in degree to dms angle with DDMM format.

    DDMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> Deg2DMS(10.123)
    "+10$^\\circ$07'"

    Args:
        x: An angle in degree.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. DDMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        A dms angle with DDMM format.

    """
    (d, m, s) = Deg2DMS(x, prec=6)
    return '%s%s%s\'' % (d, dsyb, m)


def __format_dms(x: float, prec: int=0) -> str:
    r"""
    Convert an angle in degree to dms angle in specified precision.

    Example:
    >>> __format_dms(10.123)
    '+10$^\\circ$07\'23"'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        prec: Significant digits.
    Returns:
        String of degree, arcminute, and arcsecond values in a
        specified precision.

    """
    (d, m, s) = Deg2DMS(x, prec)
    # format desimal part of arcsec value separately
    xx = s.split('.')
    s = xx[0]
    ss = '' if len(xx) ==1 else '.%s' % xx[1]
    return '%s%s%s\'%s\"%s' % (d, dsyb, m, s, ss)


def DDMMSS(x: float, pos=None) -> str:
    r"""
    Convert an angle in degree to dms angle with DDMMSS.

    DDMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> DDMMSS(10.123)
    '+10$^\\circ$07\'23"'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. DDMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSS.

    """
    return __format_dms(x, prec=6)

def DDMMSSs(x: float, pos=None) -> str:
    r"""
    Convert an angle in degree to dms angle with DDMMSSs.

    DDMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> DDMMSSs(10.123)
    '+10$^\\circ$07\'22".8'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. DDMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSSs.

    """
    return __format_dms(x, prec=7)


def DDMMSSss(x: float, pos=None) -> str:
    r"""
    Convert an angle in degree to dms angle with DDMMSSss.

    DDMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> DDMMSSss(10.123)
    '+10$^\\circ$07\'22".80'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos: A position. Note, the parameter is ignored in this function.
            Nevertheless it is necessary to have this parameter because of the
            reason described in comment of the original code. DDMM* methods are
            supposed to passed to matplotlib.ticker.FuncFormatter as the
            parameter. The callable function passed to FuncFormatter must have
            two input parameters, a tick value x and position pos. That is why
            the parameter, 'pos', is defined in this method even though it is
            not used in the function.
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSSss.

    """
    return __format_dms(x, prec=8)


def GLGBlabel(span: float
    ) -> Tuple[MultipleLocator, MultipleLocator, FuncFormatter, FuncFormatter]:
    """
    Create x- and y-axis formatters of plots suitable for a map in galactic coordinate.

    Args:
        span: The span of map axes in the unit of degrees. Both horizontal and
            vertical axes are formatted using this value.
    Returns:
        (GLlocator, GBlocator, GLformatter, GBformatter) for Galactic coordinate

    """
    # RAtick = [15.0, 5.0, 2.5, 1.25, 1/2.0, 1/4.0, 1/12.0, 1/24.0, 1/48.0, 1/120.0, 1/240.0, 1/480.0, 1/1200.0,
    #           1/2400.0, 1/4800.0, 1/12000.0, 1/24000.0, 1/48000.0, -1.0]
    XYtick = [20.0, 10.0, 5.0, 2.0, 1.0, 1/3.0, 1/6.0, 1/12.0, 1/30.0, 1/60.0, 1/180.0, 1/360.0, 1/720.0, 1/1800.0,
              1/3600.0, 1/7200.0, 1/18000.0, 1/36000.0, -1.0]
    #for RAt in RAtick:
    #    if span > (RAt * 3.0) and RAt > 0:
    #        RAlocator = MultipleLocator(RAt)
    #        break
    #if RAt < 0: RAlocator = MultipleLocator(1/96000.)
    #if RAt < 0: RAlocator = AutoLocator()
    for t in XYtick:
        if span > (t * 3.0) and t > 0:
            GLlocator = MultipleLocator(t)
            GBlocator = MultipleLocator(t)
            break
    #if DECt < 0: DEClocator = MultipleLocator(1/72000.0)
    if t < 0:
        GLlocator = AutoLocator()
        GBlocator = AutoLocator()

    if span < 0.0001:
        GLformatter = FuncFormatter(DDMMSSss)
        GBformatter = FuncFormatter(DDMMSSss)
    elif span < 0.001:
        GLformatter = FuncFormatter(DDMMSSs)
        GBformatter = FuncFormatter(DDMMSSs)
    elif span < 0.01:
        GLformatter = FuncFormatter(DDMMSS)
        GBformatter = FuncFormatter(DDMMSS)
    elif span < 1.0:
        GLformatter = FuncFormatter(DDMMSS)
        #GBformatter=FuncFormatter(DDMM)
        GBformatter = FuncFormatter(DDMMSS)
    else:
        GLformatter = FuncFormatter(DDMM)
        GBformatter = FuncFormatter(DDMM)

    return (GLlocator, GBlocator, GLformatter, GBformatter)


def RADEClabel(span: float,
        ofs_coord: bool
    ) -> Tuple[MultipleLocator, MultipleLocator, FuncFormatter, FuncFormatter]:
    """
    Create x- and y-axis formatters of plots suitable for a map in general R.A. and Dec. coordinate.

    Args:
        span: The span of map axes in the unit of degrees. Both horizontal and
            vertical axes are formatted using this value.
        ofs_coord: Format of right ascension (R.A.) labels. If True, labels will
            be in degree (DMS). Otherwise, it will be in hour angle (HMS).
    Returns:
        (RAlocator, DEClocator, RAformatter, DECformatter)

    """
    RAtick = [15.0, 5.0, 2.5, 1.25, 1/2.0, 1/4.0, 1/12.0, 1/24.0, 1/48.0, 1/120.0, 1/240.0, 1/480.0, 1/1200.0, 1/2400.0,
              1/4800.0, 1/12000.0, 1/24000.0, 1/48000.0, -1.0]
    DECtick = [20.0, 10.0, 5.0, 2.0, 1.0, 1/3.0, 1/6.0, 1/12.0, 1/30.0, 1/60.0, 1/180.0, 1/360.0, 1/720.0, 1/1800.0,
               1/3600.0, 1/7200.0, 1/18000.0, 1/36000.0, -1.0]
    for RAt in RAtick:
        if span > (RAt * 3.0) and RAt > 0:
            RAlocator = MultipleLocator(RAt)
            break
    #if RAt < 0: RAlocator = MultipleLocator(1/96000.)
    if RAt < 0: RAlocator = AutoLocator()
    for DECt in DECtick:
        if span > (DECt * 3.0) and DECt > 0:
            DEClocator = MultipleLocator(DECt)
            break
    #if DECt < 0: DEClocator = MultipleLocator(1/72000.0)
    if DECt < 0: DEClocator = AutoLocator()

    if span < 0.0001:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSSss)
        else:
            RAformatter = FuncFormatter(HHMMSSsss)
        DECformatter = FuncFormatter(DDMMSSss)
    elif span < 0.001:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSSs)
        else:
            RAformatter = FuncFormatter(HHMMSSss)
        DECformatter = FuncFormatter(DDMMSSs)
    elif span < 0.01:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSS)
        else:
            RAformatter = FuncFormatter(HHMMSSs)
        DECformatter = FuncFormatter(DDMMSS)
    elif span < 1.0:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSS)
        else:
            RAformatter = FuncFormatter(HHMMSS)
        #DECformatter=FuncFormatter(DDMM)
        DECformatter = FuncFormatter(DDMMSS)
    else:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMM)
        else:
            RAformatter = FuncFormatter(HHMM)
        DECformatter = FuncFormatter(DDMM)

    return (RAlocator, DEClocator, RAformatter, DECformatter)


def XYlabel(span: float, direction_reference: str, ofs_coord: bool=False
            ) -> Tuple[Union[GLGBlabel, RADEClabel]]:
    """
    Create labels for the x- and y-axes in plot.

    Args:
        span: The span of map axes in the unit of degrees. Both horizontal and
            vertical axes are formatted using this value.
        direction_reference: The direction reference (e.g., 'J2000')
        ofs_coord: Format of right ascension (R.A.) labels. If True, labels will
            be in degree (DMS). Otherwise, it will be in hour angle (HMS). The
            parameter is ignored if direction_reference is 'GALACTIC'. The right
            ascension is always in DMS in that case.
    Returns:
        labels for the x- and y-axes in plot.

    """
    if direction_reference.upper() == 'GALACTIC':
        return GLGBlabel(span)
    else:
        return RADEClabel(span, ofs_coord)


class MapAxesManagerBase(object):
    """Base class for MapAxesManager classes.

    Holds information to construct direction coordinates.
    """
    @property
    def direction_reference(self) -> str:
        """Return direction reference frame.

        String representing direction reference frame, such as
        J2000, ICRS, or GALACTIC, is returned. In practice, any
        string set by the user can be returned. It means that
        it is user's responsibility to check the validity of the
        returned value.

        Returns:
            str: Direction reference string.
        """
        return self._direction_reference

    @direction_reference.setter
    def direction_reference(self, value: str) -> None:
        """Set direction reference string.

        Note that the method just accept given string
        without any validity check.

        Args:
            value: direction reference string.
        """
        if isinstance(value, str):
            self._direction_reference = value

    @property
    def ofs_coord(self) -> bool:
        """Check if the plot is in offset coordinate.

        Returns:
            bool: True if offset coordinate else False.
        """
        return self._ofs_coord

    @ofs_coord.setter
    def ofs_coord(self, value: bool) -> None:
        """Turn on/off offset coordinate mode.

        Args:
            value: Turn on (True) or off (False)
                          offset coordinate mode.
        """
        if isinstance(value, bool):
            self._ofs_coord = value

    def __init__(self) -> None:
        """Constructor"""
        self._direction_reference = None
        self._ofs_coord = None

    def get_axes_labels(self) -> Tuple[str, str]:
        """
        Get direction coordinate axes labels.

        If direction reference is either J2000 or ICRS, returned
        labels are 'RA (REF)' and 'Dec (REF)' where REF is
        direction reference string (J2000 or ICRS). In offset
        coordinate mode, labels are prefixed with 'Offset-'.
        If direction reference is GALACTIC, labels will be
        'GL' and 'GB'. Otherwise, labels are 'RA' and 'Dec'.

        Returns:
            xlabel: xlabel in plot. Default is 'RA'.
            ylabel: ylabel in plot. Default is 'Dec'.

        """
        # default label is RA/Dec
        xlabel = 'RA'
        ylabel = 'Dec'
        if isinstance(self.direction_reference, str):
            if self.direction_reference in ['J2000', 'ICRS']:
                if self.ofs_coord:
                    xlabel = 'Offset-RA ({0})'.format(self.direction_reference)
                    ylabel = 'Offset-Dec ({0})'.format(self.direction_reference)
                else:
                    xlabel = 'RA ({0})'.format(self.direction_reference)
                    ylabel = 'Dec ({0})'.format(self.direction_reference)
            elif self.direction_reference.upper() == 'GALACTIC':
                xlabel = 'GL'
                ylabel = 'GB'
        return xlabel, ylabel


class PointingAxesManager(MapAxesManagerBase):
    """Creates and manages Axes instance for pointing plot.

    PointingAxesManager creates and manages matplotlib.axes.Axes
    instance for pointing plot.
    """
    MATPLOTLIB_FIGURE_ID = 9005

    def __init__(self) -> None:
        """Constructor"""
        self._axes = None
        self.is_initialized = False
        self._direction_reference = None
        self._ofs_coord = None

    def init_axes(self,
                  fig,
                  xlocator: Locator, ylocator: Locator,
                  xformatter: Formatter, yformatter: Formatter,
                  xrotation: Integral, yrotation: Integral,
                  aspect: Union[Integral, str],
                  xlim: Optional[Tuple[Integral, Integral]]=None,
                  ylim: Optional[Tuple[Integral, Integral]]=None,
                  reset: bool=False) -> None:
        """
        Initialize matplotlib.axes.Axes instance.

        Args:
            fig: Figure object of matplotlib
            xlocator: Locator instance for x-axis
            ylocator: Locator instance for y-axis
            xformatter: Formatter instance for x-axis
            yformatter: Formatter instance for y-axis
            xrotation: Rotation angle of x-axis label
            yrotation: Rotation angle of y-axis label
            aspect: Aspect ratio for the Axes. Acceptabe values
                    are number (float or int) or 'auto' or 'equal'
            xlim: Range of x-axis
            ylim: Range of y-axis
            reset: Reset Axes instance or not. It means that all
                   the above parameters are applied only when
                   reset is True or when the method is called
                   for the first time.
        """
        self.figure = fig
        self._axes = self.__axes()

        if xlim is not None:
            self._axes.set_xlim(xlim)

        if ylim is not None:
            self._axes.set_ylim(ylim)

        if not self.is_initialized or reset:
            # 2008/9/20 DEC Effect
            self._axes.set_aspect(aspect)
            self._axes.xaxis.set_major_formatter(xformatter)
            self._axes.yaxis.set_major_formatter(yformatter)
            self._axes.xaxis.set_major_locator(xlocator)
            self._axes.yaxis.set_major_locator(ylocator)
            xlabels = self._axes.get_xticklabels()
            for label in xlabels:
                label.set_rotation(xrotation)
                label.set_fontsize(8)
            ylabels = self._axes.get_yticklabels()
            for label in ylabels:
                label.set_rotation(yrotation)
                label.set_fontsize(8)

    @property
    def axes(self) -> Axes:
        """Direct access to Axes instance.

        Returns:
            Axes: Axes instance
        """
        if self._axes is None:
            self._axes = self.__axes()
        return self._axes

    def __axes(self) -> Axes:
        """Create Axes instance.

        Returns:
            Axes: Axes instance created by the method
        """
        axes = self.figure.add_axes([0.15, 0.2, 0.7, 0.7])
        xlabel, ylabel = self.get_axes_labels()
        axes.set_xlabel(xlabel)
        axes.set_ylabel(ylabel)
        axes.set_title('')
        return axes


def draw_beam(axes, r: float, aspect: float, x_base: float, y_base: float,
              offset: float=1.0):
    """
    Draw circle indicating beam size.

    Args:
        axes: Axes instance of the current axes.
        r: Radius of the circle.
        aspect: Aspect ratio of the circle.
        x_base: X-axis coordinate of the center.
        y_base: Y-axis coordinate of the center.
        offset: Offset from the center specified by (x_base, y_base)
    Returns:
        Line2D: matplotlib.lines.Line2D instance
    """
    xy = np.array([[r * (math.sin(t * 0.13) + offset) * aspect + x_base,
                    r * (math.cos(t * 0.13) + offset) + y_base]
                    for t in range(50)])
    line = axes.plot(xy[:, 0], xy[:, 1], 'r-')
    return line[0]


def draw_pointing(axes_manager: PointingAxesManager,
                  RA: np.ndarray,
                  DEC: np.ndarray,
                  FLAG: Optional[np.ndarray]=None,
                  plotfile: Optional[str]=None,
                  connect: bool=True,
                  circle: List[Optional[float]]=[],
                  ObsPattern: Optional[str]=None,
                  plotpolicy: str='ignore'
                  ) -> None:
    """
    Draw pointing plots using matplotlib, export the plots and delete the matplotlib objects.

    Flags are taken into account according to the policy specified by plotpolicy. Options are,

      - 'plot': plot all the data regardless of they are flagged or not
      - 'ignore': do not plot flagged points
      - 'greyed': plot flagged points with different color (grey)

    Args:
        axes_manager: PointingAxesManager instance.
        RA: List of horizontal (longitude) coordinate values.
        DEC: List of vertical (latitude) coordinate values.
        FLAG: List of flags. 1 is valid while 0 is invalid.
        plotfile: A file path. If no path is provided, plot is not exported.
        connect: Connect points by line or not.
        circle: List of radius of the beam. Only first value is used.
        ObsPattern: Observing pattern string. It is included in the plot title.
                    If no string is provided, title is constructed without
                    observing pattern.
        plotpolicy: Policy to handle FLAG. The plotpolicy can be any one of
                    'plot', 'ignore' or 'greyed'.
    Raises:
        ValueError if invalid plotpolicy is received
    """

    if not plotfile:
        return

    span = max(max(RA) - min(RA), max(DEC) - min(DEC))
    xmax = min(RA) - span / 10.0
    xmin = max(RA) + span / 10.0
    ymax = max(DEC) + span / 10.0
    ymin = min(DEC) - span / 10.0
    (RAlocator, DEClocator, RAformatter, DECformatter) = XYlabel(span, axes_manager.direction_reference, ofs_coord=axes_manager.ofs_coord)

    aspect = 1.0 / math.cos(math.radians(DEC[0]))
    # Plotting routine
    if connect:
        Mark = 'g-o'
    else:
        Mark = 'bo'
    fig = figure.Figure()
    axes_manager.init_axes(fig,
                           RAlocator, DEClocator,
                           RAformatter, DECformatter,
                           RArotation, DECrotation,
                           aspect,
                           xlim=(xmin, xmax),
                           ylim=(ymin, ymax))
    fig = axes_manager.figure
    a = axes_manager.axes

    if ObsPattern is None:
        a.title.set_text('Telescope Pointing on the Sky')
    else:
        a.title.set_text('Telescope Pointing on the Sky\nPointing Pattern = %s' % ObsPattern)

    if plotpolicy == 'plot':
        # Original
        a.plot(RA, DEC, Mark, markersize=2, markeredgecolor='b', markerfacecolor='b')
    elif plotpolicy == 'ignore':
        # Ignore Flagged Data
        filter = FLAG == 1
        a.plot(RA[filter], DEC[filter], Mark, markersize=2, markeredgecolor='b', markerfacecolor='b')
    elif plotpolicy == 'greyed':
        # Change Color
        if connect:
            a.plot(RA, DEC, 'g-')
        filter = FLAG == 1
        a.plot(RA[filter], DEC[filter], 'o', markersize=2, markeredgecolor='b', markerfacecolor='b')
        filter = FLAG == 0
        if np.any(filter == True):
            a.plot(RA[filter], DEC[filter], 'o', markersize=2, markeredgecolor='grey', markerfacecolor='grey')
    else:
        raise ValueError(f"invalid plotpolicy value: {plotpolicy}")

    # plot starting position with beam and end position
    if len(circle) != 0:
        draw_beam(a, circle[0], aspect, RA[0], DEC[0], offset=0.0)
        Mark = 'ro'
        a.plot(RA[-1], DEC[-1], Mark, markersize=4, markeredgecolor='r', markerfacecolor='r')
    a.axis([xmin, xmax, ymin, ymax])

    fig.savefig(plotfile, dpi=DPISummary)

    a.cla()
    fig.clf()


class SingleDishPointingChart(object):
    """Generate pointing plots.

    Generate pointing plot for given data, antenna, and field.
    Data and antenna must be given as domain objects.
    """
    def __init__(self,
                 context: infrastructure.launcher.Context,
                 ms: MeasurementSet) -> None:
        """Initialize SingleDishPointingChart class.

        Args:
            context: pipeline context object.
            ms: MeasurementSet domain object.
        """
        self.context = context
        self.ms = ms
        self.datatable = DataTable()
        datatable_name = os.path.join(self.context.observing_run.ms_datatable_name, os.path.basename(self.ms.origin_ms))
        self.datatable.importdata(datatable_name, minimal=False, readonly=True)
        self.axes_manager = PointingAxesManager()

    def __del__(self):
        del self.datatable

    def __get_field(self, field_id: Optional[int], intent: Optional[str] = None):
        """Get field domain object.

        If field_id is not given, None is returned.

        Args:
            field_id: Field ID
            intent: Field intent

        Returns:
            Field: Field domain object or None.
        """
        if field_id is not None:
            fields = self.ms.get_fields(field_id, intent=intent)
            assert len(fields) < 2
            if len(fields) == 1:
                field = fields[0]
                LOG.debug('found field domain for %s'%(field_id))
                return field
            else:
                return None
        else:
            return None

    @casa5style_plot
    def plot(self, revise_plot: bool=False, antenna: Antenna=None, target_field_id: Optional[int]=None,
             reference_field_id: Optional[int]=None, target_only: bool=True, ofs_coord: bool=False) -> Optional[Plot]:
        """Generate a plot object.

        If plot file exists and revise_plot is False, Plot object
        based on existing file is returned.

        Args:
            revise_plot (bool): Overwrite existing plot or not. Defaults to False.
            antenna (Antenna): Antenna domain object. Defaults to None.
            target_field_id (Optional[int]): ID for target (ON_SOURCE) field. Defaults to None.
            reference_field_id (Optional[int]): ID for reference (OFF_SOURCE) field. Defaults to None.
            target_only (bool): Whether plot ON_SOURCE only (True) or both ON_SOURCE and OFF_SOURCE. Defaults to True.
            ofs_coord (bool): Use offset coordinate or not. Defaults to False.

        Returns:
            Optional[Plot]: A Plot object.
        """
        self.antenna = antenna
        self.target_only = target_only
        self.target_field = self.__get_field(target_field_id, intent='TARGET')
        if self.target_only:
            self.reference_field = None
        else:
            self.reference_field = self.__get_field(reference_field_id, intent='REFERENCE')

            # do not produce target+reference plot if no reference field exists
            if self.reference_field is None:
                return None

        self.ofs_coord = ofs_coord

        self.figfile = self._get_figfile()

        if revise_plot is False and os.path.exists(self.figfile):
            return self._get_plot_object()

        ms = self.ms
        antenna_id = self.antenna.id

        target_spws = ms.get_spectral_windows(science_windows_only=True)
        # Search for the first available SPW, antenna combination
        # observing_pattern is None for invalid combination.
        spw_id = None
        if target_field_id is None:
            for s in target_spws:
                field_patterns = list(ms.observing_pattern[antenna_id][s.id].values())
                if field_patterns.count(None) < len(field_patterns):
                    # at least one valid field exists.
                    spw_id = s.id
                    break
        else:
            for s in target_spws:
                observing_pattern = ms.observing_pattern[antenna_id][s.id].get(target_field_id, None)
                if observing_pattern:
                    spw_id = s.id
                    break
        if spw_id is None:
            LOG.info('No data with antenna=%d and spw=%s found in %s' % (antenna_id, str(target_spws), ms.basename))
            LOG.info('Skipping pointing plot')
            return None
        else:
            LOG.debug('Generate pointing plot using antenna=%d and spw=%d of %s' % (antenna_id, spw_id, ms.basename))
        beam_size = casa_tools.quanta.convert(ms.beam_sizes[antenna_id][spw_id], 'deg')
        beam_size_in_deg = casa_tools.quanta.getvalue(beam_size)
        obs_pattern = ms.observing_pattern[antenna_id][spw_id]
        antenna_ids = self.datatable.getcol('ANTENNA')
        spw_ids = self.datatable.getcol('IF')
        if self.target_only:
            # target only
            if self.target_field is None:
                # plot pointings regardless of field
                srctypes = self.datatable.getcol('SRCTYPE')
                func = lambda j, k, l: j == antenna_id and k == spw_id and l == 0
                vfunc = np.vectorize(func)
                dt_rows = vfunc(antenna_ids, spw_ids, srctypes)
            else:
                field_ids = self.datatable.getcol('FIELD_ID')
                srctypes = self.datatable.getcol('SRCTYPE')
                field_id = [self.target_field.id]
                func = lambda f, j, k, l: f in field_id and j == antenna_id and k == spw_id and l == 0
                vfunc = np.vectorize(func)
                dt_rows = vfunc(field_ids, antenna_ids, spw_ids, srctypes)
        else:
            # whole pointings including reference fields
            if self.target_field is None or self.reference_field is None:
                # plot pointings regardless of field
                func = lambda j, k: j == antenna_id and k == spw_id
                vfunc = np.vectorize(func)
                dt_rows = vfunc(antenna_ids, spw_ids)
            else:
                field_ids = self.datatable.getcol('FIELD_ID')
                field_id = [self.target_field.id, self.reference_field.id]
                func = lambda f, j, k: f in field_id and j == antenna_id and k == spw_id
                vfunc = np.vectorize(func)
                dt_rows = vfunc(field_ids, antenna_ids, spw_ids)

        if self.ofs_coord == True:
            racol = 'OFS_RA'
            deccol = 'OFS_DEC'
        else:
            racol = 'RA'
            deccol = 'DEC'
        LOG.debug('column names: {}, {}'.format(racol, deccol))
        if racol not in self.datatable.colnames() or deccol not in self.datatable.colnames():
            return None

        RA = self.datatable.getcol(racol)[dt_rows]
        if len(RA) == 0:  # no row found
            LOG.warning('No data found with antenna=%d, spw=%d, and field=%s in %s.' %
                     (antenna_id, spw_id, str(field_id), ms.basename))
            LOG.warning('Skipping pointing plots.')
            return None
        DEC = self.datatable.getcol(deccol)[dt_rows]
        FLAG = np.zeros(len(RA), dtype=int)
        rows = np.where(dt_rows == True)[0]
        assert len(RA) == len(rows)
        for (i, row) in enumerate(rows):
            pflags = self.datatable.getcell('FLAG_PERMANENT', row)
            # use flag for pol 0
            FLAG[i] = pflags[0][OnlineFlagIndex]

        self.axes_manager.direction_reference = self.datatable.direction_ref
        self.axes_manager.ofs_coord = self.ofs_coord

        draw_pointing(self.axes_manager, RA, DEC, FLAG, self.figfile, circle=[0.5*beam_size_in_deg],
                      ObsPattern=obs_pattern, plotpolicy='greyed')

        ret = self._get_plot_object()

        # re-create thumbnail file
        if revise_plot:
            # remove thumbnail file
            thumb_file = ret.thumbnail
            if os.path.exists(thumb_file):
                os.remove(thumb_file)

            # access thumbnail attribute again to create thumbnail file
            thumb_file = ret.thumbnail

        # execute gc.collect() when the number of uncollected objects reaches 256 (decided ad hoc) or more.
        # figure.Figure creates a huge number of objects, and if plot() is called a significant number of times to plot points,
        # the python kernel cannot collect objects all at once by default GC setting.
        if gc.get_count()[0] > 255:
            gc.collect()

        return ret

    def _get_figfile(self) -> str:
        """
        Generate file path to export a plot.

        Returns:
            str: file path to export a plot.

        """
        session_part = self.ms.session
        ms_part = self.ms.basename
        antenna_part = self.antenna.name
        if self.target_field is None or \
            (self.reference_field is None and not self.target_only):
            identifier = antenna_part
        else:
            clean_name = self.target_field.clean_name
            identifier = antenna_part + '.%s'%(clean_name)
        if self.target_only == True:
            if self.ofs_coord == True:
                basename = 'offset_target_pointing.%s'%(identifier)
            else:
                basename = 'target_pointing.%s'%(identifier)
        else:
            basename = 'whole_pointing.%s'%(identifier)
        figfile = os.path.join(self.context.report_dir,
                               'session%s' % session_part,
                               ms_part,
                               '%s.png'%(basename))
        return figfile

    def _get_plot_object(self) -> Plot:
        """
        Generate a Plot object.

        Returns:
            Plot: A Plot object.

        """
        intent = 'target' if self.target_only == True else 'target,reference'
        if self.target_field is None or \
            (self.reference_field is None and not self.target_only):
            field_name = ''
        else:
            if self.target_only or self.target_field.name == self.reference_field.name:
                field_name = self.target_field.name
            else:
                field_name = self.target_field.name + ',' + self.reference_field.name
        if self.ofs_coord == True:
            xaxis = 'Offset R.A.'
            yaxis = 'Offset Declination'
        else:
            xaxis = 'R.A.'
            yaxis = 'Declination'
        return Plot(self.figfile, x_axis=xaxis, y_axis=yaxis,
                    parameters={'vis': self.ms.basename,
                                'antenna': self.antenna.name,
                                'field': field_name,
                                'intent': intent})
