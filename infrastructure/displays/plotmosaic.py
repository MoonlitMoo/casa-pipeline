import matplotlib.pyplot as plt
import numpy
import scipy.interpolate
from matplotlib.lines import Line2D
from matplotlib.patches import Circle
from matplotlib.ticker import FuncFormatter

import pipeline.domain.unitformat as unitformat
import pipeline.infrastructure
from pipeline.domain.measures import FrequencyUnits, DistanceUnits, Distance, ArcUnits, EquatorialArc
from pipeline.infrastructure.casa_tools import quanta

LOG = pipeline.infrastructure.get_logger(__name__)

# used when deciding primary beam colour
SEVEN_M = Distance(7, DistanceUnits.METRE)
# used to convert frequency to wavelength
C_MKS = 299792458


def plot_mosaic(ms, source, figfile):
    """
    Produce a plot of the pointings with the primary beam FWHM and field names.
    """
    fields = [f for f in source.fields]

    median_ref_freq = numpy.median([spw.ref_frequency.to_units(FrequencyUnits.HERTZ)
                                    for spw in ms.get_spectral_windows(science_windows_only=True)])

    median_ref_wavelength = Distance(C_MKS / median_ref_freq, DistanceUnits.METRE)

    # dish diameter(s) in meters, primary beam diameter(s) in arcsec
    dish_diameters = [Distance(d, DistanceUnits.METRE) for d in {a.diameter for a in ms.antennas}]
    taper = antenna_taper_factor(ms.antenna_array.name)
    beam_diameters = [float(primary_beam_fwhm(median_ref_wavelength, dish_diameter, taper).to_units(ArcUnits.ARC_SECOND))
                      for dish_diameter in dish_diameters]

    # longitude and latitude in radians
    ra  = numpy.array([quanta.convert(f.mdirection['m0']['value'], 'rad')['value'] for f in fields])
    dec = numpy.array([quanta.convert(f.mdirection['m1']['value'], 'rad')['value'] for f in fields])

    # compute the mean longitude, taking into account possible wrap-around cases
    mean_ra  = numpy.arctan2(numpy.mean(numpy.sin(ra)), numpy.mean(numpy.cos(ra)))
    mean_dec = numpy.mean(dec)  # no special measures needed for mean latitude

    # compute offsets in longitude (taking into account the cos(lat) factor) and latitude, still in radians
    delta_ra  = numpy.cos(dec) * numpy.sin(ra - mean_ra)
    delta_dec = numpy.sin(dec) * numpy.cos(mean_dec) - numpy.cos(dec) * numpy.sin(mean_dec) * numpy.cos(ra - mean_ra)

    # some heuristics to determine the appropriate x- and y-range for plotting, adjusting the figure size as needed
    radians_to_arcsec = 180 / numpy.pi * 60 * 60
    ra_range_arcsec  = (max(delta_ra)  - min(delta_ra))  * radians_to_arcsec
    dec_range_arcsec = (max(delta_dec) - min(delta_dec)) * radians_to_arcsec
    smallest_beam = min(beam_diameters)  # arcsec
    pixels_per_beam = 60.
    min_size_in_pixels = 400.
    max_size_in_pixels = 2000.
    margin_x = 100.0  # margins outside the axes in pixels, approximate (the axes object is automatically resized anyway)
    margin_y = 80.0
    pixels_x = max(min_size_in_pixels, min(max_size_in_pixels, pixels_per_beam * ra_range_arcsec / smallest_beam))
    pixels_y = max(min_size_in_pixels, min(max_size_in_pixels, pixels_per_beam * dec_range_arcsec / smallest_beam))
    pixels_per_smallest_beam = smallest_beam / max(ra_range_arcsec / pixels_x, dec_range_arcsec / pixels_y)
    fontsize = max(6, min(12, 0.1 * pixels_per_smallest_beam))   # font size for labelling the antennae

    dpi = 100  # pixels per inch
    fig = plt.figure(figsize=((pixels_x + margin_x) / dpi, (pixels_y + margin_y) / dpi))
    ax = fig.add_subplot(1, 1, 1)

    # field labels overlap and become unintelligible if there are too many of them
    draw_field_labels = len(fields) <= 500

    legend_labels = {}
    legend_colours = {}
    for dish_diameter, beam_diameter in zip(dish_diameters, beam_diameters):
        for field, rel_ra, rel_dec in zip(fields, delta_ra, delta_dec):
            x = rel_ra  * radians_to_arcsec
            y = rel_dec * radians_to_arcsec
            colour = get_dish_colour(dish_diameter, field)
            cir = Circle((x, y), radius=0.5 * beam_diameter, facecolor='none', edgecolor=colour,
                         linestyle='dotted', alpha=0.6)
            ax.add_patch(cir)
            if draw_field_labels:
                ax.text(x, y, '{}'.format(field.id), ha='center', va='center', fontsize=fontsize, color=colour)
            else:
                ax.plot(x, y, '{}+'.format(colour), markersize=4)  # show just the field centre, but no label

            label = 'T$_{{sys}}$-only field' if is_tsys_only(field) else str(dish_diameter)
            if label not in legend_labels:
                legend_labels[label] = Line2D(list(range(1)), list(range(1)), color=colour, linewidth=2,
                                              linestyle='dotted')
                legend_colours[label] = colour

    title_string = '{}, {}, average freq.={}'.format(ms.basename, source.name,
                                                     unitformat.frequency.format(median_ref_freq))
    title_font_size = 12
    title_text = ax.set_title(title_string, size=title_font_size)
    ra_string = r'{:02d}$^{{\rm h}}${:02d}$^{{\rm m}}${:02.3f}$^{{\rm s}}$'.format(
        *EquatorialArc(mean_ra % (2*numpy.pi), ArcUnits.RADIAN).toHms())
    ax.set_xlabel('Right ascension offset from {}'.format(ra_string))
    dec_string = r'{:02d}$\degree${:02d}$^\prime${:02.1f}$^{{\prime\prime}}$'.format(
        *EquatorialArc(mean_dec, ArcUnits.RADIAN).toDms())
    ax.set_ylabel('Declination offset from {}'.format(dec_string))

    leg_lines = [legend_labels[i] for i in sorted(legend_labels)]
    leg_labels = sorted(legend_labels)
    leg = ax.legend(leg_lines, leg_labels, prop={'size': 10}, loc='best')
    leg.get_frame().set_alpha(0.8)
    for text in leg.get_texts():
        text.set_color(legend_colours[text.get_text()])

    ax.axis('equal')
    arcsec_formatter = FuncFormatter(label_format)
    ax.xaxis.set_major_formatter(arcsec_formatter)
    ax.yaxis.set_major_formatter(arcsec_formatter)
    ax.xaxis.grid(True, which='major')
    ax.yaxis.grid(True, which='major')
    ax.invert_xaxis()
    plt.tight_layout()

    # make sure title text fits into the picture, if not, then reduce the font size
    xmax = title_text.get_window_extent(fig.canvas.get_renderer()).xmax
    figwidth = fig.canvas.get_width_height()[0]
    if xmax > figwidth:
        title_text.set_fontsize(title_font_size * figwidth / (2*xmax-figwidth))

    fig.savefig(figfile, dpi=dpi)
    plt.close(fig)


def get_arc_formatter(precision):
    """
    Presents a value of equatorial arc in user-friendly units.
    """
    s = '{0:.' + str(precision) + 'f}'
    f = unitformat.UnitFormat(prefer_integers=True)
    f.addUnitOfMagnitude(1. / 1000000, s + r' $\mu$as')
    f.addUnitOfMagnitude(1. / 1000, s + ' mas')
    f.addUnitOfMagnitude(1., s + r'$^{{\prime\prime}}$')
    f.addUnitOfMagnitude(60., s + r'$^\prime$')
    f.addUnitOfMagnitude(3600., s + r'$\degree$')
    return f


# Used to label x and y plot axes
AXES_FORMATTER = get_arc_formatter(1)


def label_format(x, _):
    """Labels plot axes for plots specified in units of arcseconds"""
    # x is given in arcsecs, _ is tick position
    return AXES_FORMATTER.format(x)


def get_dish_colour(dish_diameter, field=None):
    if field and is_tsys_only(field):
        return 'r'
    if dish_diameter == SEVEN_M:
        return 'b'
    else:
        return 'k'


def is_tsys_only(field):
    """
    Returns True if the field was only used to observe Tsys fields.
    """
    return 'TARGET' not in field.intents and 'ATMOSPHERE' in field.intents


def primary_beam_fwhm(wavelength, diameter, taper):
    """
    Implements the Baars formula: b*lambda / D.
      if use2007formula==True, use the formula from Baars 2007 book
        (see au.baarsTaperFactor)
      In either case, the taper value is expected to be entered as positive.
        Note: if a negative value is entered, it is converted to positive.
    The effect of the central obstruction on the pattern is also accounted for
    by using a spline fit to Table 10.1 of Schroeder's Astronomical Optics.
    The default values correspond to our best knowledge of the ALMA 12m antennas.
      diameter: outer diameter of the dish in meters
      obscuration: diameter of the central obstruction in meters
    """
    b = baars_taper_factor(taper) * central_obstruction_factor(diameter)
    lambda_m = float(wavelength.to_units(DistanceUnits.METRE))
    diameter_m = float(diameter.to_units(DistanceUnits.METRE))
    return EquatorialArc(b * lambda_m / diameter_m, ArcUnits.RADIAN)


def central_obstruction_factor(diameter, obscuration=0.75):
    """
    Computes the scale factor of an Airy pattern as a function of the
    central obscuration, using Table 10.1 of Schroeder's "Astronomical Optics".
    -- Todd Hunter
    """
    epsilon = obscuration / float(diameter.to_units(DistanceUnits.METRE))
    spline_func = scipy.interpolate.UnivariateSpline([0, 0.1, 0.2, 0.33, 0.4], [1.22, 1.205, 1.167, 1.098, 1.058], s=0)
    return spline_func(epsilon) / 1.22


def baars_taper_factor(taper_dB):
    """
    Converts a taper in dB to the constant X
    in the formula FWHM=X*lambda/D for the parabolic illumination pattern.
    We assume that taper_dB comes in as a positive value.
    - Todd Hunter
    """
    # use Equation 4.13 from Baars 2007 book
    tau = 10 ** (-0.05 * taper_dB)
    return 1.269 - 0.566 * tau + 0.534 * (tau ** 2) - 0.208 * (tau ** 3)


def antenna_taper_factor(array_name):
    # Primary beam taper in dB.
    antenna_taper = {
        'ALMA': 10.0,
        'EVLA': 0.0,
        'VLA': 0.0,
    }
    try:
        return antenna_taper[array_name]
    except KeyError:
        LOG.warning('Unknown array name: {}. Using null antenna taper factor in mosaic plots'.format(array_name))
        return 0.0
