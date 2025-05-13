import matplotlib.pyplot as plt
import matplotlib.style
import functools
import matplotlib as mpl


def casa5style_plot(func):
    style_dict = {
        'lines.linewidth': 1.0,
        'lines.markeredgewidth': 0.5,
        'lines.dash_joinstyle': 'round',
        'lines.solid_joinstyle': 'round',
        'font.size': 12.0,
        'axes.linewidth': 1.0,
        'axes.titlepad': 4.0,
        'axes.labelpad': 2.0,
        'axes.formatter.limits': [-7, 7],
        'figure.figsize': [8, 6],
        'figure.dpi': 80,
        'figure.subplot.bottom': 0.1,
        'figure.subplot.top': 0.9,
        'savefig.dpi': 100
    }

    def wrapper(*args, **kwargs):
        style = ['classic', style_dict]
        with plt.style.context(style):
            return func(*args, **kwargs)
    return wrapper


def matplotlibrc_formal(method):
    """A custom matplotlib plotting style."""
    @functools.wraps(method)
    def handle_matplotlibrc(self, *args, **kwargs):
        custom_rc = {'xtick.direction': 'in',
                     'ytick.direction': 'in',
                     'font.size': 11,
                     'font.family': 'serif',
                     'image.origin': 'lower',
                     'savefig.bbox': 'tight',
                     'figure.autolayout': True}

        with mpl.rc_context(rc=custom_rc):
            result = method(self, *args, **kwargs)

        return result

    return handle_matplotlibrc


def RescaleXAxisTimeTicks(xlim, adesc):
    """
    Plotting utility routine
    """
    if xlim[1] - xlim[0] < 10/1440.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 1))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.SecondLocator(bysecond=list(range(0, 60, 30))))
    elif xlim[1] - xlim[0] < 0.5/24.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 5))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 1))))
    elif xlim[1] - xlim[0] < 1/24.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 2))))
