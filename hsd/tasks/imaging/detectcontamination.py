"""
This module provides functionality to detect contamination in spectral data.

Original code provided by Yoshito Shimajiri.
For more details, refer to PIPE-251.
"""

from collections import namedtuple
from math import ceil
import os
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING, Union

import matplotlib
from matplotlib import figure
from matplotlib import gridspec
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.displays.pointing as pointing
from pipeline.infrastructure import casa_tools
from ..common import display as sd_display
from ..common import sdtyping

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.axis import Axis
    from matplotlib.ticker import Formatter, Locator
    from pipeline.infrastructure import Context
    from pipeline.infrastructure.imagelibrary import ImageItem


# Initialize logger for this module
LOG = infrastructure.get_logger(__name__)

# Global parameters
STDDEV_THRESHOLD_FACTOR = -4.0  # Threshold factor in terms of standard deviation to detect strong absorption features
UPPER_INTENSITY_LIMIT_FACTOR = 7.0   # Upper intensity factor of Masked-avedaged spectrum plot (Y-axis)
LOWER_INTENSITY_LIMIT_FACTOR = -7.0  # Lower intensity factor of Masked-avedaged spectrum plot (Y-axis)

# Parameters for slicing the cube image and determining the scope of processing
N_SLICES = 10  # Total number of slices
N_EDGE = 2  # Number of edge slices to be excluded from processing
N_REMAINING = N_SLICES - N_EDGE * 2  # Number of slices remaining after excluding edges

# Default color map for peak S/N map and mask map
DEFAULT_COLORMAP = "rainbow"

# Define a named tuple of the frequency specification.
#  unit: The unit of the frequency (e.g., pixel, Hz, MHz).
#  data: The actual frequency data or values.
FrequencySpec = namedtuple('FrequencySpec', ['unit', 'data'])

# Define a named tuple of the direction specification in astronomical images.
#  ref: The reference frame (e.g., J2000, B1950).
#  minra: The minimum right ascension value.
#  maxra: The maximum right ascension value.
#  mindec: The minimum declination value.
#  maxdec: The maximum declination value.
#  resolution: The resolution of the image in the direction axes.
DirectionSpec = namedtuple('DirectionSpec', ['ref', 'minra', 'maxra', 'mindec', 'maxdec', 'resolution'])

# Define a named tuple of the sizes of each axis in a image cube.
#  x: The size of the X-axis (typically the R.A. direction in astronomical images).
#  y: The size of the Y-axis (typically the Dec direction in astronomical images).
#  sp: The size of the spectral axis (e.g., frequency or velocity).
NAxis = namedtuple('NAxis', ['x', 'y', 'sp'])


def detect_contamination(context: 'Context',
                         item: 'ImageItem',
                         is_frequency_channel_reversed: Optional[bool]=False,
                         do_plot: bool = True) -> bool:
    """
    Detect contamination (the emission at OFF position, which affects the data quality) in the given image item.

    This method defines 'contamination' as a deep absorption feature, which is in most cases
    due to a strong emission feature in the OFF position. Note that a strong absorption feature
    in the ON position may also be detected as 'contamination'.

    This function is the main routine of the module. It detects contamination in the provided image item
    and returns boolean value indicating if potential contamination is detected or not.

    Args:
        context (Context): The pipeline context object.
        item (ImageItem): The image item object to be analyzed.
        is_frequency_channel_reversed (bool, optional): True if the frequency axis is flipped (case for LSB)
            by the imaging process (see worker.py). Defaults to False.
        do_plot (bool): Set True to make figure. Default is True.

    Returns:
        True if potential contamination is detected, False otherwise.
    """
    LOG.info("=================")

    # Read FITS and its header
    cube_regrid, naxis = _read_image(item.imagename)

    # Calculate RMS and Peak S/N maps
    rms_map, peak_sn_map, spectrum_at_peak, idx, idy = \
        _calculate_rms_and_peak_sn_map(cube_regrid, naxis, is_frequency_channel_reversed)

    # Determine the threshold of Peak S/N map for the mask map calculation
    peak_sn_threshold = _determine_peak_sn_threshold(cube_regrid, rms_map)

    # Calculate the mask map and the masked average spectrum
    mask_map = np.where(peak_sn_map < peak_sn_threshold, 1.0, 0.0)
    masked_average_spectrum = np.nanmean(np.where(mask_map > 0.5, cube_regrid, np.nan), axis=(1, 2))

    # Check if an absorption feature is detected
    contaminated = _detect_deep_absorption_feature(masked_average_spectrum)

    # Generate the contamination report figures
    if do_plot:
        # Create a directory for the current stage
        stage_dir = os.path.join(context.report_dir, f'stage{context.task_counter}')
        os.makedirs(stage_dir, exist_ok=True)

        # Define the output file name for the contamination report
        output_name = os.path.join(stage_dir, item.imagename.rstrip('/') + '.contamination.png')
        LOG.info(f'The output file name for the contamination report: {output_name}')

        image_obj = sd_display.SpectralImage(item.imagename)
        freq_spec = _get_frequency_spec(naxis, image_obj)
        dir_spec = _get_direction_spec(image_obj)
        _make_figures(peak_sn_map, mask_map, rms_map, masked_average_spectrum,
                      peak_sn_threshold, spectrum_at_peak, idy, idx, output_name,
                      freq_spec, dir_spec)

    return contaminated


def _pick_quiet_slice(naxis: NAxis,
                      cube_regrid: 'sdtyping.NpArray3D',
                      is_frequency_channel_reversed: bool) -> 'sdtyping.NpArray2D':
    """
    Find the most 'quiet' image slice within the image cube and estimate RMS.

    Slice the cube image into N_SLICES frequeucy-wise (or AXIS3 wise), and return the slice
    which has the smallest rms value among them.  Each N_EDGE slices on both edges are
    excluded from the estimate.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        cube_regrid (NpArray3D) : data chunk loaded from image cube
        is_frequency_channel_reversed (bool) : True if frequency channels are in reversed order. False if not.

    Returns:
        RMS map of the most 'quiet' image of the image cube.
    """
    # Calculate RMS maps for the slices
    sliced_rms_maps = [
        _slice_and_calc_RMS_of_cube_regrid(naxis, cube_regrid, x, is_frequency_channel_reversed)
        for x in range(N_EDGE, N_SLICES - N_EDGE)
    ]

    # Calculate mean RMS values for each slice
    mean_rms_values = np.array([np.nanmean(sliced_rms_maps[x]) for x in range(N_REMAINING)])

    # Select the RMS map with the smallest mean RMS value
    rms_map = sliced_rms_maps[np.argmin(mean_rms_values)]

    LOG.info("RMS: {}".format(np.nanmean(rms_map)))
    return rms_map


def _slice_and_calc_RMS_of_cube_regrid(naxis: NAxis,
                                       cube_regrid: 'sdtyping.NpArray3D',
                                       pos: int,
                                       is_frequency_channel_reversed: bool) -> 'sdtyping.NpArray2D':
    """
    Get one chunk from N_SLICES chunks of cube_regrid, and calculate RMS of it.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        cube_regrid (NpAdday3D): data chunk loaded from image cube
        pos (int): position to slice
        is_frequency_channel_reversed (bool): True if frequency channels are in reversed order. False if not.

    Returns:
        RMS array of the squared standard deviation and mean for the sliced cube.
    """
    # calculate start and end positions for slicing the cube
    _to_int_func = ceil if is_frequency_channel_reversed else int
    start_rms_ch = _to_int_func(naxis.sp * pos / N_SLICES)
    end_rms_ch = _to_int_func(naxis.sp * (pos + 1) / N_SLICES)

    # extract a slice from the cube based on the calculated positions
    sliced_cube = cube_regrid[start_rms_ch:end_rms_ch, :, :]

    # calculate the squared standard deviation and mean for the sliced cube
    stddevsq = np.nanstd(sliced_cube, axis=0) ** 2.
    meansq = np.nanmean(sliced_cube, axis=0) ** 2.

    # compute and return the RMS using the squared standard deviation and mean
    return np.sqrt(stddevsq + meansq)


def _make_figures(peak_sn_map: 'sdtyping.NpArray2D',
                  mask_map: 'sdtyping.NpArray2D',
                  rms_map: 'sdtyping.NpArray2D',
                  masked_average_spectrum: 'sdtyping.NpArray1D',
                  peak_sn_threshold: float,
                  spectrum_at_peak: 'sdtyping.NpArray1D',
                  idy: np.int64,
                  idx: np.int64,
                  output_name: str,
                  freq_spec: Optional[FrequencySpec]=None,
                  dir_spec: Optional[DirectionSpec]=None) -> None:
    """
    Create figures to visualize contamination.

    Args:
        peak_sn_map (NpArray2D): Array of the peak S/N.
        mask_map (NpArray2D): Array of the mask map.
        rms_map (NpArray2D): Array of the RMS map.
        masked_average_spectrum (NpArray1D): Array of the masked average spectrum.
        peak_sn_threshold (float): Threshold value for the peak S/N.
        spectrum_at_peak (NpArray1D): Array of the spectrum at the peak.
        idy (int64): Y-axis (latitude) index of the maximum peak S/N location.
        idx (int64): X-axis (longitude) index of the maximum peak S/N location.
        output_name (str): Name of the output file.
        freq_spec (FrequencySpec, optional): Frequency specification. Defaults to None.
        dir_spec (DirectionSpec, optional): Direction specification. Defaults to None.
    """

    # Initialize the figure with a specified size
    _figure = figure.Figure(figsize=(20, 5))
    _grid = gridspec.GridSpec( 1, 3, width_ratios=[1,1,1], wspace=0.3)

    peak_sn_plot, peak_sn_colorbar = \
        map(_figure.add_subplot, gridspec.GridSpecFromSubplotSpec(1, 2, width_ratios=[20, 1],
                                                                  subplot_spec=_grid[0]))
    mask_map_plot, mask_map_colorbar = \
        map(_figure.add_subplot, gridspec.GridSpecFromSubplotSpec(1, 2, width_ratios=[20, 1],
                                                                  subplot_spec=_grid[1]))
    masked_avg_sp_plot = _figure.add_subplot(_grid[2])
    kw = {}

    # Check if direction specification is provided
    if dir_spec is not None:
        _configure_plot_spec(peak_sn_plot, dir_spec)
        _configure_plot_spec(mask_map_plot, dir_spec)
        dir_unit = dir_spec.ref

        # Convert pixel coordinates to axes coordinates
        scx = (idx + 0.5) / peak_sn_map.shape[1]
        scy = (idy + 0.5) / peak_sn_map.shape[0]

        # Set aspect ratio based on Dec correction factor
        kw['aspect'] = 1.0 / np.cos((dir_spec.mindec + dir_spec.maxdec) / 2 / 180 * np.pi)

        # Set extent for the plot
        _half_resolution = dir_spec.resolution / 2
        kw['extent'] = (dir_spec.maxra + _half_resolution, dir_spec.minra - _half_resolution,
                        dir_spec.mindec - _half_resolution, dir_spec.maxdec + _half_resolution)
    else:
        dir_unit = 'pixel'
        scx = idx
        scy = peak_sn_map.shape[0] - 1 - idy

    # Plot the peak S/N map, mask map, and masked averaged spectrum
    _plot_peak_SN_map(peak_sn_plot, peak_sn_colorbar, peak_sn_map,
                      dir_unit, dir_spec is not None, scx, scy, kw)
    _plot_mask_map(mask_map_plot, mask_map_colorbar, mask_map,
                   peak_sn_threshold, dir_unit, kw)
    _plot_masked_averaged_spectrum(masked_avg_sp_plot, rms_map, masked_average_spectrum,
                                   peak_sn_threshold, spectrum_at_peak, freq_spec)
    # Save the figure to the specified output file
    _figure.savefig(output_name, bbox_inches="tight")


def _plot_peak_SN_map(plot: 'Axes',
                      colorbar: 'Axes',
                      peak_sn_map: 'sdtyping.NpArray2D',
                      dir_unit: str,
                      has_dir_spec: bool,
                      scx: float,
                      scy: float,
                      kw: Dict[str, Union[float, Tuple[float, float]]]) -> None:
    """
    Plot the Peak Signal-to-Noise ratio (Peak S/N) map with specified parameters.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        colorbar (Axes): The matplotlib Axes object to be used for colorbar.
        peak_sn (NpArray2D): The data of the peak S/N.
        dir_unit (str): The unit for the R.A. (Right Ascension) and Dec (Declination) axis labels.
        has_dir_spec (bool): Flag indicating if a direction specification is provided.
        scx (float): The x-coordinate of the maximum peak S/N location.
        scy (float): The y-coordinate of the maximum peak S/N location.
        kw (Dict[str, Union[float, Tuple[float, float]]]): Additional keyword arguments for the imshow().
    """
    # Log the plotting data
    LOG.debug(f'scx = {scx}, scy = {scy}')
    LOG.debug('peak_sn.shape = {}'.format(peak_sn_map.shape))
    LOG.debug('ylim = {}'.format(list(plot.get_ylim())))

    # plot the peak S/N map
    _plot_map(plot, "Peak S/N map", peak_sn_map, dir_unit, kw)

    # display a colorbar for the image
    _display_colorbar(plot, colorbar, peak_sn_map)

    # Determine the transformation for the scatter plot marker based on the presence of a direction specification
    trans = plot.transAxes if has_dir_spec else None

    # Plot a scatter marker at the specified coordinates of the maximum peak S/N location.
    plot.scatter(scx, scy, s=300, marker="o", facecolors='none',
                 edgecolors='grey', linewidth=5, transform=trans)

    # dummy scatter to display a legend
    plot.scatter([], [], s=50, marker='o', facecolors='none',
                 edgecolors='grey', linewidth=3, label="Max")
    plot.legend(bbox_to_anchor=(1,1), loc='lower left', borderaxespad=0.5)


def _plot_mask_map(plot: 'Axes',
                   colorbar: 'Axes',
                   mask_map: 'sdtyping.NpArray2D',
                   peak_sn_threshold: float,
                   dir_unit: str,
                   kw: Dict[str, Union[float, Tuple[float, float]]]):
    """
    Plot the mask map with specified parameters.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        colorbar (Axes): The matplotlib Axes object to be used for colorbar.
        mask_map (NpArray2D): The data of the mask map.
        peak_sn_threshold (float): The threshold for the peak of signal-to-noise.
        dir_unit (str): The unit for the R.A. (Right Ascension) and Dec (Declination) axis labels.
        kw (Dict[str, Union[float, Tuple[float, float]]]): Additional keyword arguments for the imshow().
    """
    # plot the mask map
    _plot_map(plot, f"Mask map (1: S/N<{peak_sn_threshold})", mask_map, dir_unit, kw)

    # display the colorbar
    _display_colorbar(plot, colorbar, mask_map,
                      {'ticks': [0, 1],
                       'format': matplotlib.ticker.FixedFormatter(['Masked', 'Unmasked'])})


def _plot_map(plot: 'Axes',
              title: str,
              map: 'sdtyping.NpArray2D',
              dir_unit: str,
              kw: Dict[str, Union[float, Tuple[float, float]]]):
    """Plot map data.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        title (str): The title of the graph.
        map (NpArray2D): The data for plotting.
        dir_unit (str): The unit for the R.A. (Right Ascension) and Dec (Declination) axis labels.
        kw (Dict[str, Union[float, Tuple[float, float]]]): Additional keyword arguments for the imshow().
    """
    # Set the title and axis labels for the plot
    plot.set_title(title)
    plot.set_xlabel(f"R.A. [{dir_unit}]")
    plot.set_ylabel(f"Dec [{dir_unit}]")

    # Display the map as an image with the specified colormap and keyword arguments
    plot.imshow(np.flipud(map), cmap=DEFAULT_COLORMAP, **kw)


def _display_colorbar(plot: 'Axes',
                      colorbar: 'Axes',
                      map: 'sdtyping.NpArray2D',
                      options: Optional[Dict[str, Any]] = {}):
    """
    Display a colorbar for the plot.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        colorbar (Axes): The matplotlib Axes object to be used for colorbar.
        map (NpArray2D): 2D array for plotting.
        options (Optional[Dict[str, Any]], optional): Additional keyword arguments for
            matplotlib.colorbar.Colorbar. Defaults to {}.
    """
    # do not use offset notation
    colorbar.get_xaxis().get_major_formatter().set_useOffset(False)
    colorbar.get_yaxis().get_major_formatter().set_useOffset(False)

    # display the colorbar of DEFAULT_COLORMAP colored
    matplotlib.colorbar.Colorbar(ax=colorbar,
        mappable=matplotlib.cm.ScalarMappable(
            matplotlib.colors.Normalize(np.nanmin(map), np.nanmax(map)),
            DEFAULT_COLORMAP),
        **options)

    # set position of the colorbar
    ppos = plot.get_position()
    cpos = colorbar.get_position()
    colorbar.set_position([ppos.x1+0.005, ppos.y0, cpos.x1-cpos.x0, ppos.y1-ppos.y0])


def _plot_masked_averaged_spectrum(plot: 'Axes',
                                   rms_map: 'sdtyping.NpArray2D',
                                   masked_average_spectrum: 'sdtyping.NpArray1D',
                                   peak_sn_threshold: float,
                                   spectrum_at_peak: 'sdtyping.NpArray1D',
                                   freq_spec: Optional[FrequencySpec]=None):
    """
    Plot the masked-averaged spectrum with specified parameters.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        rms_map (NpArray2D): The data of the RMS map.
        masked_average_spectrum (NpArray1D): 1D array of the average spectrum of the masked regions.
        peak_sn_threshold (float): The threshold for the peak signal-to-noise.
        spectrum_at_peak (NpArray1D): The spectrum data at the peak.
        freq_spec (Optional[FrequencySpec]): Frequency specifications. Defaults to None.
    """
    # Calculate the standard deviation of the masked averaged spectrum
    stddev = np.nanstd(masked_average_spectrum)

    # Create a subplot for the masked-averaged spectrum
    plot.set_title("Masked-averaged spectrum")

    if freq_spec is not None:
        abc = freq_spec.data
        assert len(abc) == len(spectrum_at_peak)
        plot.set_xlabel(f'Frequency [{freq_spec.unit}]')
    else:
        abc = np.arange(len(spectrum_at_peak), dtype=int)
        plot.set_xlabel("Channel")

    # Get the width and the minimum value of the frequency or channel range
    w = np.abs(abc[-1] - abc[0])
    minabc = np.min(abc)

    # Set y-axis label and limits
    plot.set_ylabel("Intensity [K]")
    plot.set_ylim(stddev * LOWER_INTENSITY_LIMIT_FACTOR, stddev * UPPER_INTENSITY_LIMIT_FACTOR)

    # Plot the spectrum at the peak and the masked averaged spectrum
    plot.plot(abc, spectrum_at_peak, "-", color="grey", label="spectrum at peak", alpha=0.5)
    plot.plot(abc, masked_average_spectrum, "-", color="red", label="masked averaged")

    # Define the edges for horizontal lines
    _edge = [abc[0], abc[-1]]

    # Plot horizontal lines for reference
    plot.plot(_edge, [0, 0], "-", color="black")
    plot.plot(_edge, [STDDEV_THRESHOLD_FACTOR * stddev, STDDEV_THRESHOLD_FACTOR * stddev], "--", color="red")
    plot.plot(_edge, [np.nanmean(rms_map) * 1., np.nanmean(rms_map) * 1], "--", color="blue")
    plot.plot(_edge, [np.nanmean(rms_map) * (-1.), np.nanmean(rms_map) * (-1.)], "--", color="blue")

    # Plot additional lines and annotations if the standard deviation is above the threshold
    if stddev * UPPER_INTENSITY_LIMIT_FACTOR >= np.nanmean(rms_map) * peak_sn_threshold:
        plot.plot(_edge,
                  [np.nanmean(rms_map) * peak_sn_threshold, np.nanmean(rms_map) * peak_sn_threshold],
                  "--", color="green")
        plot.text(minabc + w * 0.5, np.nanmean(rms_map) * peak_sn_threshold,
                  "lower 10% level", fontsize=18, color="green")

    # Add text annotations for the plotted lines
    plot.text(minabc + w * 0.1, np.nanmean(rms_map) * 1., "1.0 x rms", fontsize=18, color="blue")
    plot.text(minabc + w * 0.1, np.nanmean(rms_map) * (-1.), "-1.0 x rms", fontsize=18, color="blue", va='top')
    plot.text(minabc + w * 0.6, STDDEV_THRESHOLD_FACTOR * stddev, f"{STDDEV_THRESHOLD_FACTOR} x std", fontsize=18, color="red")

    # Display the legend
    plot.legend()

    # Add a warning text if the minimum of the masked averaged spectrum is below the threshold
    if np.nanmin(masked_average_spectrum) <= STDDEV_THRESHOLD_FACTOR * stddev:
        plot.text(minabc + w * 2. / 5., -5. * stddev, "Warning!!", fontsize=25, color="Orange")


def _configure_plot_spec(plot: 'Axes',
                         dir_spec: DirectionSpec):
    """
    Configure the plot specifications based on the provided direction specifications.

    This function adjusts the x and y axis labels, ticks, and rotations based on the provided
    direction specifications. It uses the pointing module to determine the appropriate formatting
    and rotation for the R.A. and Dec labels.

    Args:
        plot (Axes): The plot object to be configured.
        dir_spec (DirectionSpec): The direction specifications containing details about R.A. and Dec.
    """
    # Calculate the span based on the maximum and minimum R.A. and Dec values, and the resolution
    _span = max(dir_spec.maxra - dir_spec.minra + dir_spec.resolution,
                dir_spec.maxdec - dir_spec.mindec + dir_spec.resolution)

    # Get the appropriate locators and formatters for R.A. and Dec based on the span and reference
    RAlocator, Declocator, RAformatter, Decformatter = pointing.XYlabel(_span, dir_spec.ref)

    # Configure the x-axis (R.A.) and y-axis (Dec) with the obtained formatters, locators, and rotations
    _configure_axis(plot.xaxis, RAformatter, RAlocator, pointing.RArotation)
    _configure_axis(plot.yaxis, Decformatter, Declocator, pointing.DECrotation)


def _configure_axis(axis: 'Axis',
                    formatter: 'Formatter',
                    locator: 'Locator',
                    rotation: Union['pointing.RArotation', 'pointing.DECrotation']):
    """
    Configure the given axis with the specified formatter, locator, and rotation.

    Args:
        axis (Axis): The axis to be configured.
        formatter (Formatter): The formatter to be set for the axis.
        locator (Locator): The locator to be set for the axis.
        rotation (Union[pointing.RArotation, pointing.DECrotation]): The rotation to be set for the axis labels.
    """
    axis.set_major_formatter(formatter)
    axis.set_major_locator(locator)
    axis.set_tick_params(rotation=rotation)


def _detect_deep_absorption_feature(masked_average_spectrum: 'sdtyping.NpArray1D') -> bool:
    """
    Check if a strong absorption feature exists in the spectrum.

    This function checks the masked average spectrum for any strong absorption features.

    Args:
        masked_average_spectrum (NpArray1D): 1D array of the average spectrum of the masked regions.

    Returns:
        True if contamination was detected, False otherwise.
    """
    # Calculate the standard deviation of the masked average spectrum
    std_value = np.nanstd(masked_average_spectrum)

    # Determine if the spectrum has a strong absorption feature
    _has_strong_absorption = np.nanmin(masked_average_spectrum) <= STDDEV_THRESHOLD_FACTOR * std_value

    return _has_strong_absorption


def _read_image(input: str) -> Tuple['sdtyping.NpArray3D', NAxis]:
    """
    Read image file (FITS or CASA image) and extract its header information using casatools.

    Args:
        input (str): Path to the image file.

    Returns:
        Tuple containing:
        - Data chunk extracted from the image.
        - Sizes of axes of 3D image cube.
    """
    LOG.info(f"FITS: {input}")

    # Extract data chunk and coordinate system from the image file
    with casa_tools.ImageReader(input) as ia:
        cube = ia.getchunk()

    # Reorder the axes of the cube for further processing
    cube_regrid = np.swapaxes(cube[:, :, 0, :], 2, 0)

    # Extract dimensions and increments from the cube and coordinate system
    naxis = NAxis(x=cube.shape[0], y=cube.shape[1], sp=cube.shape[3])

    return cube_regrid, naxis


def _get_direction_spec(image_obj: 'sd_display.SpectralImage') -> DirectionSpec:
    """
    Extract direction specifications from a given SpectralImage object.

    Args:
        image_obj: SpectralImage object containing image metadata.

    Returns:
        DirectionSpec object.
    """
    # Convert R.A. and Dec values from the image object to degrees
    minra, maxra = _convert_to_degrees(image_obj.ra_min, image_obj.ra_max)
    mindec, maxdec = _convert_to_degrees(image_obj.dec_min, image_obj.dec_max)

    # The grid size is obtained by dividing the beam size by 3.
    grid_size = image_obj.beam_size / 3

    return DirectionSpec(ref=image_obj.direction_reference,
                         minra=minra, maxra=maxra,
                         mindec=mindec, maxdec=maxdec,
                         resolution=grid_size)


def _convert_to_degrees(min_value: float,
                        max_value: float) -> Tuple[float, float]:
    """
    Convert given values to degrees using CASA quanta tool.

    Args:
        min_value (float): Minimum value to convert.
        max_value (float): Maximum value to convert.

    Returns:
        Tuple containing the converted minimum and maximum values in degrees.
    """
    # Initialize CASA quanta tool for unit conversion
    qa = casa_tools.quanta

    return qa.convert(min_value, 'deg')['value'], qa.convert(max_value, 'deg')['value']


def _get_frequency_spec(naxis: NAxis,
                        image_obj: 'sd_display.SpectralImage') -> FrequencySpec:
    """
    Extract frequency specifications from a given SpectralImage object.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        image_obj (SpectralImage) : SpectralImage object containing image metadata.

    Returns:
        FrequencySpec object.
    """
    # Retrieve spectral axis details in GHz from the image object
    refpix, refval, increment = image_obj.spectral_axis(unit='GHz')

    # Calculate the frequency values based on the spectral axis details
    frequency = np.array([refval + increment * (i - refpix) for i in range(naxis.sp)])

    return FrequencySpec(unit='GHz', data=frequency)


def _calculate_rms_and_peak_sn_map(cube_regrid: 'sdtyping.NpArray3D',
                                   naxis: NAxis,
                                   is_frequency_channel_reversed: bool) \
        -> Tuple['sdtyping.NpArray2D', 'sdtyping.NpArray2D', 'sdtyping.NpArray1D', np.int64, np.int64]:
    """Calculate the RMS (Root Mean Square) and Peak S/N maps for the given data cube.

    This function computes the RMS map for the data cube and then calculates the peak S/N for each pixel.
    It also identifies the location (idx, idy) of the maximum peak S/N in the image.

    Args:
        cube_regrid (NpArray3D): 3D data cube containing the image data.
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        is_frequency_channel_reversed (bool): Indicates if the frequency channels are in reversed order.

    Returns:
        Tuple[NpArray2D, NpArray2D, NpArray1D, np.int64, np.int64]:
            - rms_map: 2D array containing the RMS values for each pixel.
            - peak_sn_map: 2D array containing the peak S/N values for each pixel.
            - spectrum_at_peak: 1D array containing the spectrum at the location of the maximum peak S/N.
            - idx: x-coordinate of the maximum peak S/N location.
            - idy: y-coordinate of the maximum peak S/N location.
    """
    # Decide the lower RMS map of the data cube
    rms_map = _pick_quiet_slice(naxis, cube_regrid, is_frequency_channel_reversed)

    # Calculate the peak S/N for each pixel
    peak_sn_map = (np.nanmax(cube_regrid, axis=0)) / rms_map

    # Identify the location of the maximum peak S/N in the image
    idy, idx = np.unravel_index(np.nanargmax(peak_sn_map), peak_sn_map.shape)
    LOG.debug(f'idx {idx}, idy {idy}')

    # Extract the spectrum at the location of the maximum peak S/N
    spectrum_at_peak = cube_regrid[:, idy, idx]

    return rms_map, peak_sn_map, spectrum_at_peak, idx, idy


def _count_valid_pixels(cube_regrid: 'sdtyping.NpArray3D') -> int:
    """
    Count valid pixels for the given data cube.

    This function counts the pixels with a valid value (not NaN) in the data cube.

    Args:
        cube_regrid (NpArray3D): 3D data cube containing the image data.

    Returns:
        Number of pixels with a valid value.
    """
    total_pix = 0
    _, ny, nx = cube_regrid.shape

    # Iterate over the data cube and count valid pixels
    for i in range(nx):
        for j in range(ny):
            if not np.isnan(np.nanmax(cube_regrid[:, j, i])):
                total_pix += 1

    return total_pix


def _determine_peak_sn_threshold(cube_regrid: 'sdtyping.NpArray3D',
                                 rms_map: 'sdtyping.NpArray2D') -> float:
    """Determine the threshold of peak S/N.

    This function calculates the peak S/N for each pixel in the image and then determines a threshold
    based on a certain percentage of the total number of valid pixels.

    Args:
        cube_regrid (NpArray3D): 3D data cube containing the image data.
        rms_map (NpArray2D): 2D array containing the root mean square (RMS) values for each pixel.

    Returns:
        The calculated threshold of peak S/N.
    """
    # Calculate the peak S/N for each pixel
    peak_sn = (np.nanmax(cube_regrid, axis=0)) / rms_map

    # Flatten the 2D peak S/N array to a 1D array and sort it
    peak_sn_1d = np.ravel(peak_sn)
    peak_sn_1d.sort()

    # Define the percentage threshold
    percent_threshold = 10.  # %

    # Calculate the number of pixels corresponding to the percentage threshold
    total_pix = _count_valid_pixels(cube_regrid)
    pix_num_threshold = int(total_pix * percent_threshold / 100.)

    # Return the peak S/N value corresponding to the pixel number threshold
    if peak_sn_1d.shape[0] > 0:
        return peak_sn_1d[pix_num_threshold]
    else:
        return 0.
