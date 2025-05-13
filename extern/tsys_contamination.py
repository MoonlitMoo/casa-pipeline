# 19-04-2023. Version 1.0.
# 20-04-2023. Version 2.0. Different functions to determine contamination intervals.
# 30-04-2023. Version 2.1. Correct intent per scan for mosaics. Phasecal intents in Tsys.
# 02-05-2023. Version 2.2. Decrease puffing. Remove flag for empty spw field. Decrease SNR detection limit for large number of channels to 6 sigma.
# 03-05-2023. Version 2.3. Fix positive_line_intervals. difference_tolerance_ratio 0.5->0.67 in find_repeated_peaks. Fix filter atm_residual_intervals.
#                          1 channel tolerance for telluric identification.
# 08-05-2023. Version 2.4. Display science spws in Tsys plots and exclude line contamination flagging from regions outside the science spw. Several changes
#                          to the TsysData objects and other functions.
# 10-05-2023. Version 3.0. Fix the image sideband ATM model for HF. Fix common_peaks. Fix model_break.
# 20-05-2023. Version 3.1. Include dt/Tsys criterion (2% default) detection threshold. Change reason string. Improvements to plots and html log.
# 30-05-2023. Version 3.2. Convex hull on model_break. Fixing model_break.
# 11-07-2023. Version 3.3. Changes to find_repeated peaks. Filter on prominence over 5. Making the large contamination warning on full spw.
#                          Channel avoidance in puff.
# 25-09-2023. Version 3.4. Subtracting a cubic fitting to detect peaks in the bandpass.
from ..infrastructure.renderer import logger as pllogger

VERSION = "3.5"

import bz2
import glob
import itertools
import os
import pickle
import re
import subprocess
import warnings
from shutil import copy2, copytree

import matplotlib.pyplot as plt
import matplotlib.transforms as transforms
import numpy as np
import scipy.constants
from matplotlib.patches import Rectangle
from scipy.ndimage import label
from scipy.optimize import Bounds, minimize
from scipy.signal import find_peaks, peak_prominences, peak_widths, savgol_filter
from scipy.spatial import ConvexHull
from scipy.special import legendre

import pipeline.infrastructure.logging
import pipeline.infrastructure.pipelineqa as pipelineqa

from .TsysDataClassFile import TsysData

LOG = pipeline.infrastructure.logging.get_logger(__name__)

# QA score for alerts issues by this heuristic. The intent is that this score
# direct the EB to QA slow lane for further analysis, which currently applies
# to anything with a QA score of <0.66,
WARNINGS_QA_SCORE = 0.6


# PREQ 232 in JAOPOST
if True:
    # ALMA Bands  0       1     2    3    4     5     6     7     8    9    10
    usePWV = [
        99.0,
        5.186,
        5.186,
        2.748,
        2.748,
        1.796,
        1.262,
        0.913,
        0.658,
        0.472,
        0.472,
    ]
    CO_LINES_MHZ = {
        "C18O_1-0": 109782.17340,
        "13CO_1-0": 110201.35430,
        "CO_1-0": 115271.20180,
        "C18O_2-1": 219560.35410,
        "13CO_2-1": 220398.68420,
        "CO_2-1": 230538.00000,
        "C18O_3-2": 329330.55250,
        "13CO_3-2": 330587.96530,
        "CO_3-2": 345795.98990,
        "C18O_4-3": 439088.76580,
        "13CO_4-3": 440765.17350,
        "CO_4-3": 461040.76820,
        "C18O_6-5": 658553.27820,
        "13CO_6-5": 661067.27660,
        "CO_6-5": 691473.07630,
        "CO_7-6": 806651.80600,
        "C18O_8-7": 877921.95530,
        "13CO_8-7": 881272.80800,
        "CO_8-7": 921799.70000,
    }
    LINES_12C16O = np.array(
        [v for k, v in CO_LINES_MHZ.items() if re.match(r"CO_\d\-\d", k)]
    )
    MAX_VELOCITY_WIDTH = 520  # km/s, based on Dame's CO 1-0 Galaxy map
    WEATHER_DATA_GROUPING = 5  # 5 s grouping

    # preq232 @ jupyterif True:
    def gline_and_baseline(pars, x, deg=1):
        # peak, location, fwhm, a, b (a+bx baseline)
        if deg == 1:
            return (
                pars[0]
                * np.exp(
                    -4
                    * np.log(4)
                    * (x - pars[1]) ** 2
                    / (np.finfo(np.float64).eps + pars[2] ** 2)
                )
                + pars[3]
                + pars[4] * x
            )
        if deg == 2:
            return (
                pars[0]
                * np.exp(
                    -4
                    * np.log(4)
                    * (x - pars[1]) ** 2
                    / (np.finfo(np.float64).eps + pars[2] ** 2)
                )
                + pars[3]
                + pars[4] * x
                + pars[5] * x**2
            )

    def gaussian_line_fit(x, y, sigma=1):
        # (peak,x0,fwhm)=pars
        xi2 = lambda pars: np.sum(no_nans(y - gline_and_baseline(pars, x)) ** 2 / sigma)
        chan_max = np.argmax(y)
        pars0 = [
            y[chan_max] - np.min(y),
            chan_max + x[0],
            min(np.sum(y > (y[chan_max] + np.min(y)) / 2), len(x) / 2),
            np.min(y),
            0,
        ]  # 4 channels initial parameter fwhm

        # print(pars0)
        degree = 1
        lb = [-np.inf for _ in range(len(pars0))]
        ub = [np.inf for _ in range(len(pars0))]
        lb[0] = 0
        ub[0] = np.nanmax(y) - np.nanmin(y)
        lb[1] = np.min(x)
        ub[1] = np.max(x)
        res = minimize(xi2, pars0, bounds=Bounds(lb=lb, ub=ub))
        if res["fun"] / len(x) > 15:
            # print("trying more")
            for deg in [1, 2]:
                if deg == 2:
                    lb.append(-np.inf)
                    ub.append(np.inf)
                xi2 = lambda pars: np.sum(
                    no_nans(y - gline_and_baseline(pars, x, deg=deg)) ** 2
                )
                for fwhm_ratio in [3, 5, 7]:
                    pars0[2] = min(
                        np.sum(y > (y[chan_max] + np.min(y)) / 2), len(x) / fwhm_ratio
                    )
                    resaux = minimize(
                        xi2,
                        pars0 + ([] if deg == 1 else [0]),
                        bounds=Bounds(lb=lb, ub=ub),
                    )
                    # print(f"--{resaux['fun']}")
                    if resaux["fun"] / len(x) < 2 or resaux["fun"] <= res[
                        "fun"
                    ] - 0.5 * len(x):
                        res = resaux
                        degree = deg
        return res["x"], gline_and_baseline(res["x"], x, deg=degree)

    def width_fit_filter(data, peaks=None, fwhm_limit=None, window=7):
        if window < 3:
            window = 3
        if peaks is None:
            return None
        half_window_size = int((window - window % 2) / 2)
        d = np.r_[
            np.ones(half_window_size) * np.nan, data, np.ones(half_window_size) * np.nan
        ]
        # widths = []
        filtered_peaks = []
        for p in peaks + half_window_size:
            gpars, gline = gaussian_line_fit(
                x=np.arange(-half_window_size, half_window_size + 1),
                y=d[p - half_window_size : p + half_window_size + 1],
            )
            fwhm = gpars[2]
            if fwhm_limit is None:
                filtered_peaks.append(p - half_window_size)
            elif fwhm_limit > fwhm:
                filtered_peaks.append(p - half_window_size)
        return np.array(filtered_peaks)

    def linear_trend(data, weighting="abs"):
        x = np.arange(data.shape[0])
        if weighting == "abs":
            distance = lambda var: np.sum(no_nans(np.abs(var[0] * x + var[1] - data)))
        else:
            distance = lambda var: np.sum(no_nans((var[0] * x + var[1] - data) ** 2))
        var0 = [0, np.nanmean(data)]
        res = minimize(distance, var0)
        return res["x"][0] * x + res["x"][1]

    def find_repeated_peaks(
        data1,
        data2,
        prominence,
        prominence_limit=5,  # v3.3
        difference_tolerance_ratio=0.5,
        width=None,
        distance_from_peak=1,
    ):  # v2.5
        # assert False, prominence
        # tolerance_chan = int((np.log(len(data1))/np.log(2)-7)*3/5) # 0 tolerance for 128, 3 channels for 4096. v3.1

        data1 = data1.copy()
        data2 = data2.copy()
        x = np.arange(data1.shape[0]) - data1.shape[0] / 2.0
        distance = lambda var: np.sum(
            no_nans(
                (
                    var[0] * x**3
                    + var[1] * x**2
                    + var[2] * x
                    + var[3]
                    - ((data1 + data2) / 2)
                )
                ** 2
            )
        )
        var0 = [0, 0, 0, np.nanmean((data1 + data2) / 2)]
        res = minimize(distance, var0)
        mod3 = res["x"][0] * x**3 + res["x"][1] * x**2 + res["x"][2] * x + res["x"][3]

        # linear_approximation = linear_trend((data1+data2)/2)
        _mod3_1 = mod3 - np.nanmean(data1) / 2
        _mod3_2 = mod3 - np.nanmean(data2) / 2
        data1 -= _mod3_1
        data2 -= _mod3_2
        # plt.plot(data1);plt.plot(data2);plt.show(); input();

        p1, prominence_dictionary1 = find_peaks(data1, prominence=prominence)
        # print(f"find_repeated_peaks: p1={p1}, prominence_dictionary1={prominence_dictionary1}")
        p2, prominence_dictionary2 = find_peaks(data2, prominence=prominence)
        # print(p2, prominence_dictionary2); #assert False,""
        repeated_peak_channels1 = []
        repeated_peak_channels2 = []
        for _p1, _pp1 in zip(p1, prominence_dictionary1["prominences"]):
            tol = int(np.floor(np.sqrt(np.log(_pp1 / prominence) / np.log(2)) + 0.5))
            # print(_p1,_pp1, tol)
            _p2 = set(
                list(np.r_[[_p1 + delta for delta in range(-tol, tol + 1)]].flatten())
            ).intersection(set(list(p2)))
            if len(_p2) == 0:
                continue
            repeated_peak_channels1.append(_p1)
            if len(_p2) == 1:
                repeated_peak_channels2.append(next(iter(_p2)))
            else:
                _p2 = list(_p2)
                _pp2 = prominence_dictionary2["prominences"][
                    np.isin(p2, np.array(list(_p2)), assume_unique=True)
                ]
                # print(np.argmin(np.abs(_pp2-_pp1)))
                _selp2 = _p2[np.argmin(np.abs(_pp2 - _pp1))]
                repeated_peak_channels2.append(_selp2)
        # assert False, (np.array(repeated_peak_channels1),np.array(repeated_peak_channels2))
        auxi1 = []
        auxi2 = []
        dd1 = {"prominences": [], "left_bases": [], "right_bases": []}  # v3.4
        dd2 = {"prominences": [], "left_bases": [], "right_bases": []}  # v3.4
        for rp1, rp2 in zip(repeated_peak_channels1, repeated_peak_channels2):
            width_rp1 = peak_widths(
                data1,
                p1[p1 == rp1],
                rel_height=distance_from_peak
                / prominence_dictionary1["prominences"][p1 == rp1][0],
            )[0][0]
            width_rp2 = peak_widths(
                data2,
                p2[p2 == rp2],
                rel_height=distance_from_peak
                / prominence_dictionary2["prominences"][p2 == rp2][0],
            )[0][0]
            if not (width_rp1 <= width and width_rp2 <= width):
                continue
            if difference_tolerance_ratio < 1:
                _prom1 = prominence_dictionary1["prominences"][p1 == rp1]
                _prom2 = prominence_dictionary2["prominences"][p2 == rp2]

                comparison_ratio = np.abs(
                    (_prom1 - _prom2) / (_prom1 + _prom2)
                )  # change formula in v3.3

                if comparison_ratio >= difference_tolerance_ratio:
                    continue
                if _prom1 > prominence_limit and _prom2 > prominence_limit:
                    # print(rp1,_prom1)
                    auxi1.append(rp1)
                    dd1["prominences"].append(_prom1)  # v3.4
                    dd1["left_bases"].append(
                        prominence_dictionary1["left_bases"][p1 == rp1]
                    )  # v3.4
                    dd1["right_bases"].append(
                        prominence_dictionary1["right_bases"][p1 == rp1]
                    )
                    auxi2.append(rp2)
                    dd2["prominences"].append(_prom2)  # v3.4
                    dd2["left_bases"].append(
                        prominence_dictionary2["left_bases"][p2 == rp2]
                    )  # v3.4
                    dd2["right_bases"].append(
                        prominence_dictionary2["right_bases"][p2 == rp2]
                    )
        return np.array(auxi1), np.array(auxi2), [dd1, dd2]  # v3.4

    def exp_rescale(data, scale=1):
        if scale == 0:
            return data

        maximum = np.nanmax(data)
        assert maximum > 0, f"exp_rescale_1: maximum={maximum} <= 0 cannot rescale."
        nn = data / maximum
        # assert False, f"{nn} {scale}"
        return (np.exp(scale * nn) - 1) / (np.exp(scale) - 1) * maximum

    def model_break(model, peaks, window_length=5):  # v3.5
        peaks = [] if peaks is None else peaks
        nchan = model.shape[0]
        chans = np.arange(nchan)
        xp = np.array([[0, nchan - 1]])
        if len(peaks) <= 0:
            return xp, np.interp(chans, xp[0], model[xp[0]])
        window_length += 1 - window_length % 2
        (atm_1, atm_2) = (
            savgol_filter(model, window_length=window_length, deriv=dd, polyorder=3)
            for dd in (1, 2)
        )
        if (
            model[0] == np.max(model)
            and atm_1[0] == np.min(atm_1)
            and atm_2[0] == np.max(atm_2)
        ):
            peaks = np.r_[0, peaks]
        if (
            model[-1] == np.max(model)
            and atm_1[-1] == np.min(atm_1)
            and atm_2[-1] == np.max(atm_2)
        ):
            peaks = np.r_[peaks, nchan - 1]
        # assert False, peaks
        if len(peaks) <= 1:
            return xp, lower_hug(model)

        _ = find_peaks(model, prominence=0)  # v2.5
        _auxp, _auxpp = find_peaks(
            model,
            prominence=np.mean(
                np.sort(_[1]["prominences"])[-len(_[0]) : -len(_[0]) + 1]
            ),
        )  # v2.5 # v3.2
        crossings = np.array(
            list(set(list(np.r_[_auxpp["left_bases"], _auxpp["right_bases"]])))
        )  # v2.5
        # print(f'peaks={peaks} _auxp={_auxp}, crossings={crossings}')
        # crossings = np.where((atm_1[1:] > 0) * (atm_1[:-1] <= 0))[0]
        # assert np.all(atm_2[crossings]>0), "not all crossings correspond to local minima"
        # assert False, crossings
        peaks = np.sort(np.array(peaks).copy())
        newcross = []
        for i in range(peaks.shape[0] - 1):
            peak0, peak1 = peaks[i], peaks[i + 1]
            _ccs = crossings[(peak0 < crossings) * (crossings < peak1)]
            if len(_ccs) > 0:
                newcross.append(_ccs[model[_ccs] == np.min(model[_ccs])][0])
        crossings = np.array(newcross, dtype=int)
        xp = np.array(
            np.r_[
                [] if 0 in peaks else 0,
                crossings,
                int([] if nchan - 1 in peaks else nchan - 1),
            ],
            dtype=int,
        )
        # print(f'xp={xp}')
        baseline = np.interp(chans, xp, model[xp])
        xp_intervals = np.array([[xp[i], xp[i + 1]] for i in range(xp.shape[0] - 1)])
        for i in xp_intervals:
            baseline[i[0] : i[1] + 1] = lower_hug(model[np.arange(i[0], i[1] + 1)])
        return xp_intervals, baseline

    def lower_hug(y):
        x = np.arange(len(y))
        graph = np.c_[x, y]
        hull = ConvexHull(graph)
        # plt.plot(y);print(hull.simplices)
        nodes = dict()
        for v in set(list(hull.simplices.flatten())):
            nodes[v] = list(
                set(
                    list(
                        (
                            hull.simplices[
                                np.sum(hull.simplices == v, axis=1, dtype=bool)
                            ].flatten()
                        )
                    )
                )
                - set([v])
            )
        lower = [0]
        node0 = 0
        direction_slope = (y[nodes[0]] - y[0]) / (x[nodes[0]] - x[0])  ##
        node1 = nodes[0][np.argmin(direction_slope)]
        lower.append(node1)
        while node1 < len(y) - 1:
            _ = set(nodes[node1]) - set([node0])
            assert (
                len(_) == 1
            ), f"lower_hug: bad node construction.\nnodes={nodes}\nnode0={node0}\nnode1={node1}"
            node0 = node1
            node1 = next(iter(_))
            lower.append(node1)
        # assert False, lower
        lower = np.sort(np.array(list(set(lower))))
        # print(lower);plt.plot(y);plt.plot(lower,y[lower]); assert False,""
        # assert False, lower
        return np.interp(x, x[lower], y[lower])

    def modified_atmospheric_profile(
        model,
        baseline,
        degree_baseline,
        scales,
        coefficients,
        segments,
        x_coordinates=None,
        split=False,
    ):
        # plt.plot(model);plt.plot(baseline); assert False, segments
        nchan = model.shape[0]
        x_coordinates = np.linspace(-1, 1, nchan)
        segments = np.sort(segments)
        segments_resc = segments * 2 / (nchan - 1) - 1
        seg_lim = np.r_[segments[:, 0], segments[-1, -1]]
        seg_lim_resc = np.r_[segments_resc[:, 0], segments_resc[-1, -1]]
        # assert False, segments_resc
        # p_interp_baseline = baseline # np.polynomial.Polynomial.fit(seg_lim_resc,model[seg_lim],deg = len(seg_lim)-1)

        model_mod = model - baseline  # p_interp_baseline(x_coordinates) # v3.2

        n_scales = len(scales)

        t = coefficients[0]
        p = coefficients[1 : degree_baseline + 2]

        x_coordinatessum = p[0] * np.ones(x_coordinates.shape[0])
        for degree in range(1, len(p)):
            x_coordinatessum += p[degree] * legendre(degree)(x_coordinates)
        #
        modified = np.zeros(x_coordinates.shape[0])

        for nseg, seg_resc in enumerate(list(segments_resc)):
            a = coefficients[
                2 * nseg * n_scales
                + degree_baseline
                + 2 : 2 * nseg * n_scales
                + degree_baseline
                + 2
                + n_scales
            ]
            b = coefficients[
                2 * nseg * n_scales
                + degree_baseline
                + 2
                + n_scales : 2 * nseg * n_scales
                + degree_baseline
                + 2
                + 2 * n_scales
            ]
            # print(f"---{a}--{b}")
            if n_scales > 0:
                for ii, scale in enumerate(scales):
                    c0, c1 = segments[nseg, 0], segments[nseg, 1]
                    if np.sum(model[c0:c1] ** 2) > 0:
                        modified[c0:c1] += a[ii] * exp_rescale(
                            data=model_mod[c0:c1], scale=scale
                        ) + b[ii] * exp_rescale(data=model_mod[c0:c1], scale=-scale)
                modified = modified / np.sqrt(2 * len(a))
        #
        if split:
            return x_coordinatessum + baseline, t * (model) + t * modified
        return x_coordinatessum + baseline + t * (model + modified)

    def cross_model(data_a, data_b, verbose=False, asym_factor=1):
        model = lambda pp: pp[0] + pp[1] * (data_b - np.nanmean(data_b))
        dif2 = lambda pp: np.sum(
            no_nans(
                np.array([_ if _ >= 0 else asym_factor * _ for _ in data_a - model(pp)])
            )
            ** 2
        )
        # chi2 = lambda pp: np.sum(no_nans(pp[0]+pp[1]*data_b-data_a)**2)
        pp0 = [np.nanmean(data_a) - np.nanmean(data_b), 0]
        res = minimize(dif2, pp0)
        return res if verbose else res["x"]

    def max_without_outliers(x):
        outliers = np.repeat(True, len(x))
        _x = x.copy()
        while np.any(outliers):
            # print(_x)
            med = np.nanmedian(_x)
            sigma = median_abs_deviation(_x, scale="normal", nan_policy="omit")
            if sigma * len(_x) <= 0:
                break
            outliers = np.abs(_x - med) / sigma > np.abs(
                scipy.stats.norm.ppf(0.5 / len(no_nans(_x)))
            )
            # print(med,sigma,outliers)
            _x = _x[np.logical_not(outliers)]
        return np.nanmax(_x)

    def positive_line_intervals(
        normalized_data, detection_limit=7, base_detection=2, edge=0.025, verbose=False
    ):
        nchan = len(normalized_data)
        chans = np.arange(0, nchan)
        _norm = normalized_data.copy()
        _norm[np.abs(chans - nchan / 2) > (0.5 - edge) * nchan] = np.nan
        peaks0, peaks_dict0 = find_peaks(
            (_norm) * (_norm > base_detection),
            height=detection_limit,
            prominence=detection_limit,
        )
        peaks_dict0["peak_heights"] += base_detection

        if verbose:
            LOG.debug(
                "positive_line_intervals: peaks0, peaks_dict0 = %s, %s", peaks0, peaks_dict0
            )
            plt.close()
            plt.plot(normalized_data)
            _ = np.r_[peaks_dict0["left_bases"], peaks_dict0["right_bases"]]
            plt.plot(_, normalized_data[_], ".")
            plt.plot(peaks0, normalized_data[peaks0], "x")
            plt.plot(base_detection * np.ones(len(normalized_data)), linestyle="dotted")
            plt.plot(
                detection_limit * np.ones(len(normalized_data)), linestyle="dotted"
            )
            plt.title(f"{peaks0}")
            plt.show()
            input()

        lims = np.sort(
            np.array(
                list(
                    set(
                        list(
                            np.r_[peaks_dict0["left_bases"], peaks_dict0["right_bases"]]
                        )
                    )
                )
            )
        )  # v3.3
        intervals = np.array(
            [[np.max(lims[lims <= p]), np.min(lims[lims >= p])] for p in peaks0]
        )  # merge_intervals(_) # v3.3
        # print(f"positive_line_intervals:\n\tintervals={intervals}")
        # assert False, f'{peaks0} {peaks_dict0}'
        intervals = merge_intervals(intervals)  # v3.3
        # print(f"positive_line_intervals:\n\tmerged intervals={intervals}");input()
        peaks, peaks_intervals, heights = [], [], []
        for interval in intervals:
            _ = np.logical_and(peaks0 >= interval[0], peaks0 <= interval[1])
            inside_peaks = np.c_[
                peaks0[_], peaks_dict0["peak_heights"][_], peaks_dict0["prominences"][_]
            ]
            inside_peaks = inside_peaks[(-inside_peaks[:, 1]).argsort()]
            LOG.info("positive_line_intervals:\n\t\tinside_peaks=%s", inside_peaks)
            # input()
            peaks.append(inside_peaks[0, 0])
            peaks_intervals.append(interval)
            heights.append(_norm[np.array(inside_peaks[0, 0], dtype=int)])
        # if verbose: print(peaks_intervals)
        return (
            np.array(peaks, dtype=int),  # v2.3
            np.array(peaks_intervals, dtype=int).reshape(len(peaks), 2),
            np.array(heights),
        )

    def refitting_intervals_bp_to_source(
        source,
        bp,
        sigma,
        normalized_data,
        detection_limit,
        edge=0.025,
        base_detection=2,
    ):
        peaks, intervals, heights = positive_line_intervals(
            normalized_data=normalized_data,
            edge=edge,
            verbose=False,
            detection_limit=detection_limit,
            base_detection=base_detection,
        )

        LOG.debug("refitting_intervals_bp_to_source:\n\t--First version: %s", intervals)
        final_peaks, final_intervals = np.array([]), np.array([])
        for interval in intervals:
            shift = shift_bp_to_source(
                interval=interval, bp=bp, source=source, sigma=sigma
            )
            # print(intervals, shift)
            interval = np.array(interval, dtype=int)
            ch0, ch1 = interval[0], interval[1]
            peaks1, intervals1, heights1 = positive_line_intervals(
                normalized_data=((source - bp - shift) / sigma)[ch0 : ch1 + 1],
                edge=0,
                detection_limit=detection_limit,
                base_detection=base_detection,
                verbose=False,
            )
            # plt.plot(((source-bp-shift)/sigma)[ch0:ch1+1]); plt.show();input()

            peaks1 += ch0
            intervals1 += ch0
            LOG.debug(
                "refitting_intervals_bp_to_source:\n\t--Second version: %s", intervals1
            )
            final_peaks = np.r_[final_peaks, peaks1]
            final_intervals = (
                intervals1
                if final_intervals.shape[0] == 0
                else np.r_[final_intervals, intervals1]
            )

        # assert False, ''
        return final_peaks, final_intervals

    def below_fit(data, model, iterations, clip, start_asym_factor=2):
        meanmodel = np.nanmean(model)
        asym_factor = start_asym_factor
        for k in range(10):  # v3.5
            _data = data.copy()
            for i in range(iterations):

                # print(f"i={i}, model={model}\ndata={data}")
                dif = lambda pp: np.sum(
                    no_nans(
                        np.abs(
                            np.array(
                                [
                                    _ if _ >= 0 else asym_factor * _
                                    for _ in _data - model - pp
                                ]
                            )
                        )
                    )
                )

                meandata = np.nanmean(_data)
                pars0 = meandata - meanmodel
                res = minimize(dif, pars0)
                # print(f"clip={clip}");
                if False:
                    plt.plot(_data)
                    plt.plot(model + res["x"][0])
                    input()
                _data[np.abs(_data - model - res["x"][0]) > clip] = np.nan
            if not (np.any(_data == _data) and res["x"][0] == res["x"][0]):
                # assert False, f"_data={_data}, mu={res['x'][0]}"
                asym_factor *= 2  # aca explota, hay que arreglarlo
            else:
                break
        return res["x"][0]

    def shift_bp_to_source(
        interval,
        source,
        bp,
        sigma,
        clip_snr=4,
        iterations=2,
        asym_factor=2,
    ):  # v3.5
        nchan = len(source)
        chans = np.arange(0, nchan)
        interval = np.array(interval, dtype=int)
        delta = search_expanded_interval(
            data=bp,
            interval=interval,
            sigma=np.nanmedian(sigma),
            limit_sigma=1,
        )
        if delta == 0:
            delta = (interval[1] - interval[0]) / 2
        ch0, ch1 = max(0, int(interval[0] - delta / 2)), int(
            min(interval[1] + delta / 2, nchan - 1)
        )
        LOG.debug("shift_bp_to_source %s,%s", ch0, ch1)

        shift = below_fit(
            data=(source)[ch0 : ch1 + 1],
            model=(bp)[ch0 : ch1 + 1],
            iterations=iterations,
            clip=clip_snr * np.nanmedian(sigma),
            start_asym_factor=1,
        )  # asym_factor)
        if False:
            plt.plot(source[ch0 : ch1 + 1])
            plt.plot(shift + bp[ch0 : ch1 + 1])
            input()
        LOG.debug("shift=%s", shift)
        return shift

    def search_expanded_interval(data, interval, sigma, limit_sigma=2):
        LOG.debug("---initial interval=%s", interval)
        ch0, ch1 = int(interval[0]), int(interval[1])
        delta0 = ch1 - ch0
        nchan = data.shape[0]
        delta_return = 0
        for delta in range(
            int(min(2 * interval[0] / 3, 2 * (nchan - interval[1]) / 3)) + 1
        ):
            _ch0, _ch1 = int(ch0 - delta / 2), int(ch1 + delta / 2)
            x = np.linspace(_ch0, _ch1, _ch1 - _ch0 + 1)
            y = data[_ch0 : _ch1 + 1]
            # print(x.shape,y.shape)
            m, n = np.polyfit(y=y, x=x, deg=1)
            reduced_chi_square = (
                np.sum(no_nans(m * x + n - y) ** 2) / sigma**2 / (_ch1 - _ch0 + 1)
            )
            if reduced_chi_square > 1 + limit_sigma * np.sqrt(2 / (_ch1 - _ch0 + 1)):
                LOG.debug(
                    "---redchi-sq = %s, delta_return = %s",
                    reduced_chi_square,
                    delta_return,
                )
                return delta_return
            else:
                delta_return = delta
        LOG.debug(
            "---redchi-sq = %s, delta_return = %s", reduced_chi_square, delta_return
        )
        return delta_return

    def basic_atm_asymmetric_fit(data, transmission, iterations, clip, asym_factor=2):
        xvec = np.linspace(-1, 1, transmission.shape[0])
        model = (
            lambda pp: pp[0] * (1 - transmission) + pp[1] * xvec + pp[2]
        )  # include base in v3.3
        _data = data.copy()
        for i in range(iterations):
            dif2 = lambda pp: np.sum(
                no_nans(
                    np.abs(
                        np.array(
                            [
                                _ if _ >= 0 else asym_factor * _
                                for _ in _data - model(pp)
                            ]
                        )
                    )
                )
            )
            meantsys = np.nanmean(data)
            pars0 = [meantsys / np.nanmean(1 - transmission), 0, 0]
            _ = np.abs(meantsys / np.nanmean(1 - transmission))
            # print(pars0)
            res = minimize(
                dif2, pars0, bounds=[(-10 * _, 10 * _), (None, None), (None, None)]
            )
            _data[
                np.abs(_data - model(res["x"]))
                > clip * np.nanmedian(np.abs(_data - model(res["x"])))
            ]
        return res["x"]

    def smoother_sigma(v, iterations=2, clip_level=7, stype="sg"):
        vv = v.copy()
        for i in range(iterations):
            sub, ss = smoother(vv, stype=stype)
            sigma = median_abs_deviation(sub, scale="normal", nan_policy="omit")
            LOG.debug("smoother_sigma:\n\tsub=%s\n\tsigma=%s", sub, sigma)
            selection = np.abs(sub) > clip_level * sigma
            if np.any(selection):
                vv[selection] = np.nan
            else:
                break
        return sigma

    def no_nans(x):
        return x[~np.isnan(x)]

    def smoother(
        vector_for_smoothing,
        vector_to_subtract_from=None,
        order=3,
        window_size=None,
        stype="sg",
    ):
        nchan = vector_for_smoothing.shape[0]
        window_size = (
            int(128 / max(1, 128 * 4 / nchan)) if window_size is None else window_size
        )
        window_size += 1 - window_size % 2
        stype = "constant" if stype != "sg" else "sg"
        if stype == "sg":
            s = savgol_filter(vector_for_smoothing, window_size, order)
        elif stype == "constant":
            s = np.nanmean(vector_for_smoothing) * np.ones(len(vector_for_smoothing))
        if vector_to_subtract_from is None:
            vector_to_subtract_from = vector_for_smoothing.copy()
        subtracted = vector_to_subtract_from.copy() - s
        return (subtracted, s)

    def _contains_nan(a, nan_policy="propagate", use_sumodelation=True):
        policies = ["propagate", "raise", "omit", "ignore", "skip"]
        if nan_policy not in policies:
            raise ValueError(
                "nan_policy must be one of {%s}"
                % ", ".join("'%s'" % s for s in policies)
            )
        try:
            # The sumodelation method avoids creating a (potentially huge) array.
            # But, it will set contains_nan to True for (e.g.) [-inf, ..., +inf].
            # If this is undesirable, set use_sumodelation to False instead.
            if use_sumodelation:
                with np.errstate(invalid="ignore", over="ignore"):
                    contains_nan = np.isnan(np.sum(a))
            else:
                contains_nan = np.isnan(a).any()
        except TypeError:
            # This can happen when attempting to sum things which are not
            # numbers (e.g. as in the function `mode`). Try an alternative method:
            try:
                contains_nan = np.any(np.isnan(a))
            except TypeError:
                # Don't know what to do. Fall back to omitting nan values and
                # issue a warning.
                contains_nan = False
                nan_policy = "omit"
                warnings.warn(
                    "The input array could not be properly "
                    "checked for nan values. nan values "
                    "will be ignored.",
                    RuntimeWarning,
                )

        if contains_nan and nan_policy == "raise":
            raise ValueError("The input contains nan values")

        return contains_nan, nan_policy

    def median_abs_deviation(
        x, axis=0, center=np.median, scale=1.0, nan_policy="propagate"
    ):
        r"""
        Compute the median absolute deviation of the data along the given axis.
        The median absolute deviation (MAD, [1]_) computes the median over the
        absolute deviations from the median. It is a measure of dispersion
        similar to the standard deviation but more robust to outliers [2]_.
        The MAD of an empty array is ``np.nan``.
        .. versionadded:: 1.5.0
        Parameters
        ----------
        x : array_like
            Input array or object that can be converted to an array.
        axis : int or None, optional
            Axis along which the range is computed. Default is 0. If None, compute
            the MAD over the entire array.
        center : callable, optional
            A function that will return the central value. The default is to use
            np.median. Any user defined function used will need to have the
            function signature ``func(arr, axis)``.
        scale : scalar or str, optional
            The numerical value of scale will be divided out of the final
            result. The default is 1.0. The string "normal" is also accepted,
            and results in `scale` being the inverse of the standard normal
            quantile function at 0.75, which is approximately 0.67449.
            Array-like scale is also allowed, as long as it broadcasts correctly
            to the output such that ``out / scale`` is a valid operation. The
            output dimensions depend on the input array, `x`, and the `axis`
            argument.
        nan_policy : {'propagate', 'raise', 'omit'}, optional
            Defines how to handle when input contains nan.
            The following options are available (default is 'propagate'):
            * 'propagate': returns nan
            * 'raise': throws an error
            * 'omit': performs the calculations ignoring nan values
        Returns
        -------
        mad : scalar or ndarray
            If ``axis=None``, a scalar is returned. If the input contains
            integers or floats of smaller precision than ``np.float64``, then the
            output data-type is ``np.float64``. Otherwise, the output data-type is
            the same as that of the input.
        See Also
        --------
        numpy.std, numpy.var, numpy.median, scipy.stats.iqr, scipy.stats.tmean,
        scipy.stats.tstd, scipy.stats.tvar
        Notes
        -----
        The `center` argument only affects the calculation of the central value
        around which the MAD is calculated. That is, passing in ``center=np.mean``
        will calculate the MAD around the mean - it will not calculate the *mean*
        absolute deviation.
        The input array may contain `inf`, but if `center` returns `inf`, the
        corresponding MAD for that data will be `nan`.
        References
        ----------
        .. [1] "Median absolute deviation",
               https://en.wikipedia.org/wiki/Median_absolute_deviation
        """
        if not callable(center):
            raise TypeError(
                "The argument 'center' must be callable. The given "
                f"value {repr(center)} is not callable."
            )

        # An error may be raised here, so fail-fast, before doing lengthy
        # computations, even though `scale` is not used until later
        if isinstance(scale, str):
            if scale.lower() == "normal":
                scale = 0.6744897501960817  # special.ndtri(0.75)
            else:
                raise ValueError(f"{scale} is not a valid scale value.")

        x = np.asarray(x)

        # Consistent with `np.var` and `np.std`.
        if not x.size:
            if axis is None:
                return np.nan
            nan_shape = tuple(item for i, item in enumerate(x.shape) if i != axis)
            if nan_shape == ():
                # Return nan, not array(nan)
                return np.nan
            return np.full(nan_shape, np.nan)
        # print("ccc",x)
        contains_nan, nan_policy = _contains_nan(x, nan_policy)

        if contains_nan:
            if axis is None:
                mad = _mad_1d(x.ravel(), center, nan_policy)
            else:
                mad = np.apply_along_axis(_mad_1d, axis, x, center, nan_policy)
        else:
            if axis is None:
                med = center(x, axis=None)
                mad = np.median(np.abs(x - med))
            else:
                # Wrap the call to center() in expand_dims() so it acts like
                # keepdims=True was used.
                med = np.expand_dims(center(x, axis=axis), axis)
                mad = np.median(np.abs(x - med), axis=axis)

        return mad / scale

    def _mad_1d(x, center, nan_policy):
        # Median absolute deviation for 1-d array x.
        # This is a helper function for `median_abs_deviation`; it assumes its
        # arguments have been validated already.  In particular,  x must be a
        # 1-d numpy array, center must be callable, and if nan_policy is not
        # 'propagate', it is assumed to be 'omit', because 'raise' is handled
        # in `median_abs_deviation`.
        # No warning is generated if x is empty or all nan.
        isnan = np.isnan(x)
        if isnan.any():
            if nan_policy == "propagate":
                return np.nan
            x = x[~isnan]
        if x.size == 0:
            # MAD of an empty array is nan.
            return np.nan
        # Edge cases have been handled, so do the basic MAD calculation.
        med = center(x)
        mad = np.median(np.abs(x - med))
        return mad

    def plot_source_bp_normalized_residuals(
        source_tsys,
        bandpass_tsys,
        model,
        frequencies,
        normalized_residuals,
        sigma,
        tsys_contamination_intervals,
        label_source="science",
        default_label="Tsys contamination",
        default_color="red",
        first_plot_title="",
        level=None,
        background=None,
        other_emphasized_intervals=None,
        savefigfile=None,
        sci_spw_intervals=None,
    ):

        nchan = frequencies.shape[0]
        chans = np.arange(nchan)
        _nu_function = (
            lambda ch: frequencies[0]
            + (frequencies[-1] - frequencies[0]) / (nchan - 1) * ch
        )
        _channel_function = (
            lambda nu: (nchan - 1)
            / (frequencies[-1] - frequencies[0])
            * (nu - frequencies[0])
        )
        fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(15, 6))

        if background is not None:  # v3.3
            [x.set_facecolor(background) for x in (ax0, ax1)]  # v3.3

        _ = source_tsys.copy()
        for i in list(tsys_contamination_intervals):
            _chsel = list(np.arange(int(i[0]), int(i[1]) + 1))
            # print(_chsel)
            _[_chsel] = np.nan
            _chsel = [max(0, _chsel[0] - 1)] + _chsel + [min(nchan - 1, _chsel[-1] + 1)]
            # print(_chsel)
            ax0.plot(_chsel, source_tsys[_chsel], ".-", color="red")
            ax0.plot(
                [_chsel[0], _chsel[-1]],
                source_tsys[[_chsel[0], _chsel[-1]]],
                "--",
                color="#1f77b4",
            )
        ax0.plot(_, "-", label=label_source)
        dif = source_tsys - bandpass_tsys
        _ = -np.nanmean(dif) + np.nanmean(np.r_[source_tsys, bandpass_tsys])
        ax0.plot(bandpass_tsys, label="bandpass")
        ax0.plot(dif + _, label="difference", linewidth=3)
        # v2.5
        ax0.plot(model + _, "--", label="model")
        _min, _max = np.nanmin(np.r_[source_tsys, bandpass_tsys, dif + _]), np.nanmax(
            np.abs(np.r_[source_tsys, bandpass_tsys])
        )

        # _t = _min*.995-(trans_b)*(_min*.995-_max*1.005)
        # ax0.plot(chans,_t, label='ATM')
        ax0.set_ylim(_min - (_max - _min) / 15, _max + (_max - _min) / 15)
        ax0.set_title(first_plot_title)
        # ax0.set_xlabel('Frequency (MHz)');
        ax0.set_ylabel("Tsys (K)")

        put_legend_in_second_plot = False
        for i, interval in enumerate(list(tsys_contamination_intervals)):
            ax0.fill_betweenx(
                np.array([-2 * _max, 2 * _max]),
                interval[0],
                interval[1],
                alpha=0.2,
                facecolor="red",
            )
            ax1.fill_betweenx(
                np.array([-_max, 2 * np.nanmax(np.abs(normalized_residuals))]),
                interval[0],
                interval[1],
                alpha=0.2,
                facecolor="red",
                label=default_label if i == 0 else None,
            )
            put_legend_in_second_plot = True

        sci_spw_intervals = dict() if sci_spw_intervals is None else sci_spw_intervals
        for spw, spw_interval in sci_spw_intervals.items():
            trans = transforms.blended_transform_factory(ax0.transData, ax0.transAxes)
            ax0.add_patch(
                Rectangle(
                    (spw_interval[0], 0.04),
                    spw_interval[1] - spw_interval[0],
                    0.01,
                    fill=False,
                    color="black",
                    transform=trans,
                )
            )
            ax0.text(spw_interval[0], 0.015, str(spw), ma="left", transform=trans)
            # print(f'ax0.add_patch(Rectangle(({spw_interval[0]},{_min}),{spw_interval[1]-spw_interval[0]},1))')
        for label, info in other_emphasized_intervals.items():
            alpha = info["alpha"] if "alpha" in info else 0.2
            hatch = info["hatch"] if "hatch" in info else None
            if len(info["intervals"].flatten()) == 0:
                continue
            for i, interval in enumerate(list(info["intervals"])):
                color = info["color"]
                color = "grey" if color is None else color
                ax0.add_patch(
                    Rectangle(
                        (interval[0], -0.1),
                        interval[1] - interval[0],
                        1.1,
                        fill=True,
                        color=color,
                        alpha=alpha,
                        transform=trans,
                        linewidth=0,
                        hatch=hatch,
                    )
                )

                trans1 = transforms.blended_transform_factory(
                    ax1.transData, ax1.transAxes
                )
                ax1.add_patch(
                    Rectangle(
                        (interval[0], -0.1),
                        interval[1] - interval[0],
                        1.1,
                        fill=True,
                        color=color,
                        alpha=alpha,
                        transform=trans1,
                        linewidth=0,
                        label=label if i == 0 else None,
                        hatch=hatch,
                    )
                )
                put_legend_in_second_plot = True

        _min, _max = np.nanmin(normalized_residuals), np.nanmax(normalized_residuals)
        ax1.set_ylim(_min - (_max - _min) / 15, _max + (_max - _min) / 15)

        ax0.legend(loc="upper right")
        ax0.grid()

        if put_legend_in_second_plot:
            ax1.legend()

        [
            x.secondary_xaxis("top", functions=(_nu_function, _channel_function))
            for x in (ax0, ax1)
        ]
        ax1_kelvin = ax1.secondary_yaxis(
            "right",
            functions=(lambda nt: sigma * nt, lambda t: t / sigma),
        )
        ax1.set_ylabel("σ")
        ax1_kelvin.set_ylabel("[K]")
        ax1.plot(chans, normalized_residuals, ".-")
        ax1.grid()
        if level is not None:
            try:
                l0, l1 = level
            except TypeError:
                l0 = level
                l1 = None
            except ValueError:
                l0 = level[0]
                l1 = None
            if l0 is not None:
                ax0.plot(chans, np.ones(len(chans)) * l0, ":", color="black")

            if l1 is not None:
                ax1.plot(chans, np.ones(len(chans)) * l1, ":", color="black")

        if savefigfile is None:
            plt.show()
            input()
        else:
            plt.savefig(savefigfile, bbox_inches="tight")
            plt.close()

    ## Integer Intervals manipulation
    def puff(intervals, delta=0, nchan=np.inf, chan_avoidance=None):
        puffed = []

        for i in intervals:
            pufflim0, pufflim1 = max(0, i[0] - delta), min(nchan, i[1] + delta)
            if chan_avoidance is not None:
                if np.any(
                    [p in range(int(pufflim0), int(i[0]) + 1) for p in chan_avoidance]
                ):
                    pufflim0 = i[0]
                if np.any(
                    [p in range(int(i[1]), int(pufflim1 + 1)) for p in chan_avoidance]
                ):
                    pufflim1 = i[1]
            puffed.append([pufflim0, pufflim1])
        return np.array(puffed)

    def merge_intervals(intervals):
        # Normalize to sorted non-overlaping intervals. Aimilar idea as in
        # https://www.geeksforgeeks.org/merging-intervals/
        if len(intervals.flatten()) == 0:
            return intervals
        # print(f"merge_intervals: {intervals} {len(intervals)}")
        assert np.all(
            intervals[:, 0] <= intervals[:, 1]
        ), f"merge_intervals: intervals not well defined. intervals={intervals}"
        if len(intervals) == 1:
            return intervals
        intervals = np.sort(intervals.copy(), axis=0)
        stack = []
        # insert first interval into stack
        stack.append(intervals[0])
        for i in intervals[1:]:
            # Check for overlapping interval,
            # if interval overlap
            if i[0] > stack[-1][1] + 1:
                stack.append(i)
            else:
                stack[-1][1] = max(stack[-1][1], i[1])
        return np.array(stack)

    def union_intervals(a, b):
        return merge_intervals(np.r_[a, b])

    def complement_intervals(
        a,
    ):  # This is the key function. Needs to work with infinities and empty sets.
        if len(a) == 0:
            return np.array([[-np.inf, np.inf]])
        a_normalized = merge_intervals(a)
        result0 = np.r_[-np.inf, a_normalized[:, 1] + 1]
        result1 = np.r_[a_normalized[:, 0] - 1, np.inf]
        non_empty = np.logical_and(result0 < np.inf, result1 > -np.inf)
        result = np.c_[result0[non_empty], result1[non_empty]]
        if np.array_equal(result, np.array([[np.inf, -np.inf]])):
            result = np.array([])
        return merge_intervals(result)

    def intersection_intervals(a, b):
        union_of_complements = union_intervals(
            complement_intervals(a), complement_intervals(b)
        )
        return complement_intervals(union_of_complements)

    def difference_intervals(a, b):
        return intersection_intervals(a, complement_intervals(b))


def save_tsysdata(tsysdata):
    file = f"{tsysdata.tsystable}.tsysdata.pbz2"
    data_dict = {
        "VERSION": float(VERSION[0:3]),
        "tsystable": tsysdata.tsystable,
        "msfile": tsysdata.msfile,
        "tsysfields": tsysdata.tsysfields,
        "tsysdata": tsysdata.tsysdata,
        "specfields": tsysdata.specfields,
        "specdata": tsysdata.specdata,
        "absorptionfields": tsysdata.absorptionfields,
        "absorptiondata": tsysdata.absorptiondata,
        "inversetsysmap": tsysdata.inversetsysmap,
        "ch_frq": tsysdata.ch_frq,
    }
    with bz2.BZ2File(file, "wb") as w:
        pickle.dump(data_dict, w)


def find_prominent_peaks_in_atm(
    n_sigma_atm, atm_model, temperature, sigma, frequencies=None
):
    # Identify prominent peaks in the atmospheric profile. Ignore CO as a prominent peak.
    # n_sigma_atm = n_sigma_atm_prominent_peak
    frequencies = np.arange(atm_model.shape[0]) if frequencies is None else frequencies
    atm_peak_centers, _ = find_peaks(atm_model)
    newpeaks = []

    for p in atm_peak_centers:
        if np.all(
            np.abs(LINES_12C16O - frequencies[p])
            > np.abs(frequencies[0] - frequencies[1])
        ):
            newpeaks.append(p)
    atm_peak_centers = np.array(newpeaks, dtype=int)
    atm_peak_centers_freq = frequencies[atm_peak_centers]
    prominent_peaks_in_atm = None
    _ = peak_prominences(atm_model, atm_peak_centers)[0]
    if np.any(_ > n_sigma_atm * sigma / np.abs(temperature)):
        prominent_peaks_in_atm = atm_peak_centers[
            _ > n_sigma_atm * sigma / np.abs(temperature)
        ]
        LOG.info("Prominent peak at chans %s", prominent_peaks_in_atm)
    else:
        LOG.info("No prominent peaks in the ATM profile.")
    return prominent_peaks_in_atm


def detect_tsys_contamination(
    bandpass, source, atm, n_sigma_atm_prominent_peak=7, n_sigma_detection_limit=7
):

    source = source.copy()  # avoid mutability.
    bandpass = bandpass.copy()
    # subtract_linear_trend
    linear_approximation = linear_trend((source + bandpass) / 2)
    _lin_delta_s = linear_approximation - np.nanmean(source) / 2
    _lin_delta_b = linear_approximation - np.nanmean(bandpass) / 2
    source -= _lin_delta_s
    bandpass -= _lin_delta_b

    # Estimate some simple noise figures
    dif = source - bandpass
    sigma_smooth = smoother_sigma(dif)
    # print(f"sigma_smooth={sigma_smooth}");assert False, ""
    # Simple ATM model to start with. Obtain t0 and a slope for the dif
    t0, slope, base = basic_atm_asymmetric_fit(
        dif - np.nanmin(dif), 1 - atm, clip=4, iterations=3
    )  # v3.3 set min of dif to zero. Include base.
    LOG.info("t0 = %s, slope=%s", t0, slope)
    nchan = len(dif)
    simplemm = mm = (
        t0 * atm + slope * np.linspace(-1, 1, nchan) + base + np.nanmin(dif)
    )  # v3.3
    channel_percentage_within_simple_model = np.array(
        [
            (100 * np.sum(np.abs(dif - simplemm) <= i * sigma_smooth) / nchan)
            for i in range(1, 6, 2)
        ]
    )
    LOG.info(
        "channel percentage within n-sigmas: %s%%", channel_percentage_within_simple_model
    )

    # Identify prominent peaks in the atmospheric profile. Ignore CO as a prominent peak.
    # plt.plot(source,'.-');#plt.plot(dif);print(sigma_smooth);input()
    prominent_peaks_in_atm = find_prominent_peaks_in_atm(
        atm_model=atm,  # frequencies = freqs_b,
        temperature=t0,
        sigma=sigma_smooth,
        n_sigma_atm=n_sigma_atm_prominent_peak / 2,
    )

    # First version of residuals
    sigma = sigma_smooth
    normres = (dif - mm) / sigma_smooth

    for k in range(2):
        if prominent_peaks_in_atm is not None or np.any(
            channel_percentage_within_simple_model < np.array([20, 40, 50])
        ):
            LOG.info("Not simple")
            dega = 3 if prominent_peaks_in_atm is not None else 0  # v3.5
            dega += k
            deg_baseline = 3
            xp_mod, baseline_mod = model_break(
                atm, prominent_peaks_in_atm, window_length=5 * np.ceil(nchan / 128).astype('int')
            )

            _dif = dif.copy()
            for i in range(2):
                chi2 = lambda var: np.sum(
                    no_nans(
                        (
                            _dif
                            - modified_atmospheric_profile(
                                model=atm,
                                degree_baseline=deg_baseline,
                                baseline=baseline_mod,
                                scales=list(range(1, dega)),
                                coefficients=var,
                                segments=xp_mod,
                            )
                        )
                    )
                    ** 2
                    / sigma_smooth**2
                )
                var0 = list(
                    np.r_[
                        t0,
                        np.zeros(deg_baseline + 1),
                        np.zeros(2 * dega * xp_mod.shape[0]),
                    ]
                )
                var0[2] = slope
                lb = [-np.inf for _ in var0]
                ub = [np.inf for _ in var0]
                if np.prod(np.sign(np.r_[np.nanmedian(dif), np.nanmean(dif)])) == 1:
                    # lb[1] = 0 if np.sign(np.nanmean(dif))>0 else -np.inf
                    # ub[1] = 0 if np.sign(np.nanmean(dif))<0 else np.inf
                    for i in range(deg_baseline + 1, len(var0)):
                        lb[i] = -np.inf  # if np.sign(np.nanmean(dif))>0 else -np.inf
                        # ub[i] = 0 if np.sign(np.nanmean(dif))<0 else np.inf
                if prominent_peaks_in_atm is None:
                    lb[0], ub[0] = min(0.9 * t0, 1.1 * t0), max(0.9 * t0, 1.1 * t0)
                res = minimize(chi2, var0, bounds=Bounds(lb=lb, ub=ub))
                baseline, modline = modified_atmospheric_profile(
                    model=atm,
                    degree_baseline=deg_baseline,
                    baseline=baseline_mod,
                    scales=list(range(1, dega)),
                    coefficients=res["x"],
                    segments=xp_mod[0:],
                    split=True,
                )
                mm = baseline + modline
                sigma = np.sqrt(sigma_smooth**2 + (dif * 0.01) ** 2)
                normres = (dif - mm) / sigma
                if np.sum(normres > 10) <= 0:
                    break
                _dif[normres > 10] = np.nan

        _normres = normres.copy()
        _normres[normres >= 7] = np.nan
        rescale_smooth_sigma_factor = max(
            smoother_sigma(_normres, stype="constant"), 0.5
        )
        if np.any(normres[prominent_peaks_in_atm] >= n_sigma_detection_limit):
            # print("**",normres[prominent_peaks_in_atm])
            LOG.info(
                "detect_tsys_contamination: re-iterating ATM fitting with higher order."
            )
        else:
            break

    # print(np.nanmedian(np.abs(dif-mm-np.nanmedian(dif-mm))))

    prominent_peaks_in_atm = (
        [] if prominent_peaks_in_atm is None else prominent_peaks_in_atm
    )

    # plt.plot(dif-mm-np.nanmedian(dif-mm)); input();
    normres /= rescale_smooth_sigma_factor
    sigma *= rescale_smooth_sigma_factor

    if False:
        plt.plot(normres)
        input()
    # print(f"spw={spw} medianMin={np.nanmedian(min_science[key])} sigma={sigma} {relative_detection_factor*np.nanmedian(min_science[key]/sigma)}")
    # assert False, ""

    # n_ave = np.sum(intents==intent_dict[field])/len(set(list(spws))) - 2*remove_n_extreme
    # n_sigma_detection_limit = n_sigma_detection_limit # max(n_sigma_detection_limit, np.nanmedian(delta_tsys_detection_level/sigma)) # v3.1
    pcline_peak_chans, pcline_intervals = refitting_intervals_bp_to_source(
        source=source,
        bp=bandpass,
        sigma=sigma,
        normalized_data=normres,
        detection_limit=n_sigma_detection_limit,
        base_detection=(1 - 2) / (4096 - 128) * (nchan - 128) + 2,
    )

    # plt.plot(dif);plt.plot(mm);input()
    return {
        "contamination_peaks": pcline_peak_chans,
        "contamination_intervals": pcline_intervals,
        "normalized_residuals": normres,
        "sigma": sigma,
        "prominent_atmospheric_peaks_location": prominent_peaks_in_atm,
        "model": mm - _lin_delta_b + _lin_delta_s,
    }


def co_telluric_intervals(normalized_spectrum, frequencies_mhz):
    results = dict()
    nchan = len(normalized_spectrum)
    nu0, nu1, dnu = (
        np.min(frequencies_mhz),
        np.max(frequencies_mhz),
        np.mean(np.abs(frequencies_mhz[1:] - frequencies_mhz[0:-1])),
    )
    line_names = []
    line_frequencies = []
    for k, v in CO_LINES_MHZ.items():
        if nu0 < v < nu1:
            line_names.append(k)
            line_frequencies.append(v)
    if len(line_names) <= 0:
        return results
    line_frequencies = np.array(line_frequencies)
    peaks, prominence_dictionary = find_peaks(normalized_spectrum, prominence=5)
    selection = np.array(
        [np.min(np.abs(frequencies_mhz[p] - line_frequencies)) < 2 * dnu for p in peaks]
    )
    if len(selection) <= 0:
        return results
    # assert False, [selection, line_frequencies, np.min(np.abs(frequencies_mhz[np.array(peaks[1])]-line_frequencies)),dnu]
    peaks = peaks[selection]
    if len(peaks) <= 0:
        return results
    widths, heights, left_ipss, right_ipss = peak_widths(
        normalized_spectrum, peaks, rel_height=0.5
    )
    # assert False, peaks
    results = {"co_lines": [], "co_line_frequencies": [], "ranges": []}
    for p, width, height, left_ips, right_ips in zip(
        peaks, widths, heights, left_ipss, right_ipss
    ):
        frequency_of_peak = frequencies_mhz[p]
        for k, v in CO_LINES_MHZ.items():
            if np.abs(frequency_of_peak - v) < 2 * dnu:
                results["co_lines"].append(k)
                results["co_line_frequencies"].append(v)
                results["ranges"].append(
                    [max(0, p - width / 2.35 * 3), min(nchan - 1, p + width / 2.35 * 3)]
                )
                break
    results["ranges"] = np.array(results["ranges"], dtype=int)
    return results


def get_tsys_contaminated_intervals(
    tsysdata,
    spwlist=None,
    fieldlist=None,
    large_interval_warning_limit=0.25,  # % of spectral window covered by line for warning
    n_sigma_atm_prominent_peak=7,
    plot=False,
    savefigfile=None,
    remove_n_extreme=1,
    relative_detection_factor=0.05,  # v3.1
    n_sigma_detection_limit=7,
):
    # START - new function variables introduced for PIPE-2009
    plot_wrappers = []  # list to hold pipeline plot wrappers.
    qascores = []       # list to hold QAScores created alongside warning messages
    vis = tsysdata.msfile  # short alias to MS name. new variable added in PIPE-2009.
    # END - new function variables introduced for PIPE-2009

    warnings_list = []
    tsys = tsysdata  ###

    _ = set(list(tsys.specdata[tsys.specfields.index("spw")]))
    if spwlist is None:
        spwlist = list(_)
    spwlist = list(_.intersection(set(spwlist)))
    if len(spwlist) == 0:
        LOG.info("get_tsys_contaminated_intervals. Warning: no spwlist -> no output.")
    _ = set(list(tsys.tsysdata[tsys.tsysfields.index("field")]))
    if fieldlist is None:
        fieldlist = list(_)
    fieldlist = list(_.intersection(set(fieldlist)))
    if len(fieldlist) == 0:
        LOG.info("get_tsys_contaminated_intervals. Warning: no fieldlist -> no output.")
    with warnings.catch_warnings():
        warnings.filterwarnings(action="ignore", message="Mean of empty slice")
        average_difference, average_science, average_bp, average_antenna = (
            tsys.filter_statistics_and_dif(
                statistics=np.nanmean, remove_n_extreme=remove_n_extreme
            )
        )
        min_difference, min_science, min_bp, _ = tsys.filter_statistics_and_dif(
            statistics=np.nanmin, remove_n_extreme=remove_n_extreme
        )

    spws, intents, scans, fields = (
        tsys.tsysdata[tsys.tsysfields.index(f)]
        for f in ["spw", "intent", "scan", "field"]
    )
    _ = []
    for i, f in zip(intents, fields):
        _.append((f, i))
    intent_dict = dict(set(_))

    all_freqs_mhz = tsys.specdata[tsys.specfields.index("freq_mhz")]
    all_absorption_curves = tsys.absorptiondata[
        tsys.absorptionfields.index("absorption")
    ]

    result = dict()
    large_residual = []
    # assert False, average_science.values()
    corr = np.arange((list(average_science.values())[0]).shape[0])
    for spw, field in itertools.product(spwlist, fieldlist):
        if intent_dict[field] == "bandpass":
            continue
        key = f"{spw}_{field}"
        LOG.info("-- %s", key)
        _ = set(scans[(intents == "bandpass") * (spws == spw)].tolist())
        assert (
            len(_) == 1
        ), "get_tsys_contaminated_intervals: more than 1 BP scan per spw in EB!"
        scan_b = _.pop()

        freqs_mhz = all_freqs_mhz[
            np.nonzero(tsys.specdata[tsys.specfields.index("spw")] == spw)[0][0]
        ]
        freqs_b = freqs_mhz  ###

        _ = (tsys.absorptiondata[tsys.absorptionfields.index("scan")] == scan_b) * (
            tsys.absorptiondata[tsys.absorptionfields.index("spw")] == spw
        )
        trans_b = np.mean(all_absorption_curves[_], axis=0)  # removes 1 dimension

        nchan = freqs_b.shape[0]
        chansize_mhz = np.abs(freqs_b[1] - freqs_b[0])
        n_sigma_detection_limit = (6 - 5) / (4096 - 128) * (nchan - 128) + 5  #### v2.2
        chans = np.arange(nchan)

        spec_b = np.nanmean(average_bp[key][corr], axis=0)
        spec_s = np.nanmean(average_science[key][corr], axis=0)

        _ = detect_tsys_contamination(
            bandpass=spec_b,
            source=spec_s,
            atm=1 - trans_b,
            n_sigma_detection_limit=n_sigma_detection_limit,
        )
        pcline_intervals = _["contamination_intervals"]
        pcline_peak_chans = _["contamination_peaks"]
        normres = _["normalized_residuals"]
        sigma = _["sigma"]
        prominent_peaks_in_atm = _["prominent_atmospheric_peaks_location"]
        mm = _["model"]
        dif = spec_s - spec_b

        # plt.plot(dif);plt.plot(mm);input()

        large_baseline_residual = False
        sigma_smooth_bp, sigma_smooth_s = smoother_sigma(spec_b), smoother_sigma(spec_s)
        sigma_mad_dif = median_abs_deviation(
            dif - mm, scale="normal", nan_policy="ignore"
        )
        LOG.info(
            "large_baseline_residual *** \n\tsigma_mad_dif,sigma_smooth_bp,sigma_smooth_s,prominent_peaks_in_atm = %.2g,%.2g,%.2g,%s",
            sigma_mad_dif,
            sigma_smooth_bp,
            sigma_smooth_s,
            prominent_peaks_in_atm,
        )
        LOG.info(
            "\tchans over 5sigma = %s, nchan=%s, nchan/4=%s", np.sum((dif-mm)>5*sigma_smooth_bp), nchan, nchan/4
        )
        if False:
            plt.close()
            plt.plot((dif - mm) / sigma_smooth_bp)
            input()
        if (
            sigma_mad_dif > 3 * sigma_smooth_bp
            and np.sum((dif - mm) > 5 * sigma_smooth_bp, dtype=int) > nchan / 4
            and len(prominent_peaks_in_atm) == 0
        ):  # v3.4
            large_baseline_residual = True
            large_residual.append(key)  # v3.4
            warning = f"Large residuals"
            msg_spw, msg_field = key.split("_")
            message = f"Large residuals detected in {vis}, spw {msg_spw}, field {msg_field}."
            warnings_list.append([warning, message])
            qascores.append(
                pipelineqa.QAScore(
                    score=WARNINGS_QA_SCORE,
                    shortmsg=warning,
                    longmsg=f'{message}',
                    applies_to=pipelineqa.TargetDataSelection(vis={vis}, spw={msg_spw}, field={msg_field})
                )
            )
            LOG.info("Large residual detected in %s spw %s field %s", vis, msg_spw, msg_field)
        # print(f" sigma_mad_dif={sigma_mad_dif} sigma_smooth_bp = {sigma_smooth_bp} sigma_quot={sigma_smooth_s/sigma_smooth_bp}")

        co_telluric = co_telluric_intervals(spec_b / sigma_smooth_bp, freqs_b)

        speaks, prominence_dictionary_source_peaks = find_peaks(
            spec_s / sigma_smooth_s,
            prominence=n_sigma_detection_limit
            * ((1 - 0.0) / 1920 * (nchan - 128) + 0.0),
        )
        speaks = width_fit_filter(
            data=spec_s / sigma_smooth_s,
            peaks=speaks,
            fwhm_limit=(150 - 5) / 1920 * (nchan - 128) + 5,
            window=int(9 + (200 - 9) / 1920 * (nchan - 128)),
        )
        common_peaks1, common_peaks2, more_info_common_peaks = find_repeated_peaks(
            data1=spec_s / sigma_smooth_s,
            data2=spec_b / sigma_smooth_bp,
            prominence=n_sigma_detection_limit / 1.5,
            width=int(nchan / 64),
        )  #
        # assert False, more_info_common_peaks
        # change above difference_tolerance_ratio treatment in v3.3
        delta_tsys_detection_level = relative_detection_factor * np.nanmedian(
            min_science[key][corr]
        )  # v3.1
        LOG.info(
            "common_peaks1=%s, common_peaks2=%s, pcline_peak_chans=%s, pcline_intervals=%s",
            common_peaks1,
            common_peaks2,
            pcline_peak_chans,
            pcline_intervals,
        )
        # print(f"speaks={speaks}, prominence_dictionary_source_peaks={prominence_dictionary_source_peaks}")
        # plt.plot(spec_s/sigma_smooth_s,'-');plt.plot(speaks,(spec_s/sigma_smooth_s)[speaks],'.');input()

        line_intervals = []
        common_feature_intervals = []
        possible_line_intervals = []
        telluric_intervals = []
        atm_residual_intervals = []
        low_tsys_contamination = []
        interval_range_limit = 0.4
        concommitant_peak_max_distance = (4 - 2) / 2048 * (nchan - 128) + 2.0
        for pcli, pclp in zip(list(pcline_intervals), list(pcline_peak_chans)):
            LOG.info(f"pcli=%s, pclp=%s", pcli, pclp)
            delta_s = np.ptp(spec_s[pcli[0] : pcli[1] + 1])  # v3.1
            delta_b = np.ptp(spec_b[pcli[0] : pcli[1] + 1])  # v3.1
            delta_dif = np.ptp(
                spec_s[pcli[0] : pcli[1] + 1] - spec_b[pcli[0] : pcli[1] + 1]
            )

            interval_comparison = min(delta_s, delta_b) / max(delta_s, delta_b)  # v3.1
            # assert False, interval_comparison, delta_dif ,delta_tsys_detection_level
            # v2.3 1 channel tolerance telluric. # v2.5 more tolerance, nchan dependance
            telluric_channel_tolerance = int(
                (32 - 1) / (4096 - 128) * (nchan - 128) + 1
            )
            telluric_channels = np.r_[
                [
                    common_peaks1 + _
                    for _ in range(
                        -telluric_channel_tolerance, telluric_channel_tolerance + 1
                    )
                ]
            ].flatten()
            # assert False, (pclp in telluric_channels, pclp, telluric_channels, interval_comparison)
            # concommitant_peak =
            if (
                pclp in telluric_channels and interval_comparison > interval_range_limit
            ):  # v3.1
                if np.any(np.abs(freqs_b[int(pclp)] - LINES_12C16O) < 2 * chansize_mhz):
                    if interval_comparison < 0.7:
                        warning = "Telluric residuals"
                        message = f"Large difference between CO Line {LINES_12C16O[np.abs(freqs_b[int(pclp)] - LINES_12C16O) < chansize_mhz]} MHz in {vis} spw {spw}, comparing field {field} and bandpass."
                        LOG.info(message)
                        warnings_list.append([warning, message])
                        qascores.append(
                            pipelineqa.QAScore(
                                    score=WARNINGS_QA_SCORE,
                                    shortmsg=warning,
                                    longmsg=f'Large difference between the bandpass telluric line and field {field} in {vis} spw {spw}',
                                    applies_to=pipelineqa.TargetDataSelection(vis={vis}, spw={spw}, field={field})
                            )
                        )
                    telluric_intervals.append(pcli)
                elif np.any(
                    np.array([np.prod(pcli - _) for _ in prominent_peaks_in_atm]) <= 0
                ):
                    atm_residual_intervals.append(pcli)
                else:
                    common_feature_intervals.append(pcli)
            elif np.any(
                np.array([np.prod(pcli - _) for _ in prominent_peaks_in_atm]) <= 0
            ):  # v2.3
                atm_residual_intervals.append(pcli)
            elif delta_dif < delta_tsys_detection_level:
                low_tsys_contamination.append(pcli)
            elif np.all([(sp <= pcli[0] or sp >= pcli[1]) for sp in speaks]):
                gaussian_check, gaussian_plus_baseline_model = gaussian_line_fit(
                    x=np.arange(pcli[0], pcli[1] + 1),
                    y=(spec_s / sigma_smooth_s)[int(pcli[0]) : int(pcli[1]) + 1],
                )
                _auxipeak, _ = find_peaks(gaussian_plus_baseline_model)
                # print("##",gaussian_check[0]< n_sigma_detection_limit*((1-0.5)/1920*(nchan-128)+0.5) , not np.any((pcli[1]>_auxipeak)*(_auxipeak > pcli[0])))
                if gaussian_check[0] < n_sigma_detection_limit * (
                    (1 - 0.5) / (4096 - 128) * (nchan - 128) + 0.5
                ) or not np.any((pcli[1] - pcli[0] > _auxipeak) * (_auxipeak > 0)):
                    LOG.info(
                        "%s -> possible (failed concommitant peak gaussian check)", pcli
                    )
                    possible_line_intervals.append(pcli)
                else:
                    LOG.info("%s -> recovered peak in gaussian check", pcli)
                    line_intervals.append(pcli)
            else:
                LOG.info("-- pcli=%s", pcli)
                line_intervals.append(pcli)

        line_intervals = np.array(line_intervals)

        possible_line_intervals = difference_intervals(
            np.array(possible_line_intervals), line_intervals
        )
        # telluric_intervals = difference_intervals(np.array(telluric_intervals),line_intervals) # commented in v3.3
        telluric_intervals = (
            np.stack(telluric_intervals, axis=0)
            if len(telluric_intervals) > 0
            else np.array([])
        )  # v3.3
        atm_residual_intervals = difference_intervals(
            np.array(atm_residual_intervals), line_intervals
        )
        common_feature_intervals = difference_intervals(
            np.array(common_feature_intervals), line_intervals
        )
        low_tsys_contamination = difference_intervals(
            np.array(low_tsys_contamination), line_intervals
        )

        line_intervals = puff(
            line_intervals,
            delta=int((16 - 0) / (4096 - 128) * (nchan - 128) + 0),
            nchan=nchan,  ### v2.2
            chan_avoidance=common_peaks1[
                peak_prominences(spec_b, common_peaks1)[0]
                > n_sigma_detection_limit * sigma_smooth_bp
            ],
        )  ### v3.3

        labeling, number_of_intervals = label(normres > n_sigma_detection_limit)
        if number_of_intervals > 0:
            auxiliary_intervals = np.array(
                [
                    [min(chans[labeling == l]), max(chans[labeling == l]) + 1]
                    for l in range(1, number_of_intervals + 1)
                ]
            )
            auxiliary_intervals = np.array(
                [
                    interval
                    for interval in auxiliary_intervals
                    if len(intersection_intervals(np.array([interval]), line_intervals))
                    > 0
                ]
            )
            line_intervals = union_intervals(line_intervals, auxiliary_intervals)
        line_intervals = difference_intervals(
            line_intervals, np.array(telluric_intervals)
        )  # v3.3
        if len(co_telluric) > 0:
            line_intervals = difference_intervals(
                line_intervals, co_telluric["ranges"]
            )  # v3.5
        LOG.info("telluric_intervals: %s", telluric_intervals)
        LOG.info("line_intervals: %s", list(line_intervals))
        LOG.info("possible_line_intervals: %s", possible_line_intervals)
        LOG.info("low_tsys_contamination: %s", low_tsys_contamination)
        # puff the intervals by 1 channel for 12M and several if the thing has more channels
        off_science_line_intervals = np.array([[]])
        interval_dict = {
            "telluric_intervals": telluric_intervals,
            "possible_line_intervals": possible_line_intervals,
            "low_tsys_contamination": low_tsys_contamination,
            "line_intervals": line_intervals,
            "atm_residual_intervals": atm_residual_intervals,
            "common_feature_intervals": common_feature_intervals,
        }
        for k, interval in interval_dict.items():
            _ = separate_non_intersecting_sci_spw(tsys, interval, tsysspw=spw)
            interval_dict[k] = _["in"].copy()
            # print(f"{off_science_line_intervals} {_['off']}")
            if len(off_science_line_intervals.flatten()) * len(_["off"]) > 0:
                off_science_line_intervals = np.r_[off_science_line_intervals, _["off"]]
            elif len(_["off"]) > 0:
                off_science_line_intervals = _["off"]

                off_science_line_intervals = _["off"].copy()
                # assert False, off_science_line_intervals
        sci_spw_intervals_in_tsys_channels = _["sci_intervals"]
        telluric_intervals = interval_dict["telluric_intervals"]
        possible_line_intervals = interval_dict["possible_line_intervals"]
        low_tsys_contamination = interval_dict["low_tsys_contamination"]
        line_intervals = interval_dict["line_intervals"]
        atm_residual_intervals = interval_dict["atm_residual_intervals"]
        common_feature_intervals = interval_dict["common_feature_intervals"]

        # print(f"*#* {line_intervals} {line_intervals.shape} {possible_line_intervals} {possible_line_intervals.shape}")
        # Large intervals are only "possible"
        _ = []

        for i in line_intervals:
            relative_channel_width = 1 - freqs_b[1] / freqs_b[0]
            wide = False
            velocity_width_kms = (
                abs(i[1] - i[0]) * relative_channel_width * scipy.constants.c / 1e3
            )
            dv_criterion = velocity_width_kms >= MAX_VELOCITY_WIDTH
            spw_percentage_criterion = (
                abs(i[1] - i[0]) > nchan * large_interval_warning_limit
            )
            if spw_percentage_criterion or dv_criterion:  # based on Dame's CO 1-0 map.
                if len(possible_line_intervals) == 0:
                    possible_line_intervals = np.array([i])
                else:
                    possible_line_intervals = np.append(possible_line_intervals, i)
                wide = True
                warning = f"Wide line contamination"
                message = " ".join(
                    [
                        f"Channel range {int(i[0])}~{int(i[1])} equivalent to",
                        (
                            f"{velocity_width_kms:.3g} km/s > {MAX_VELOCITY_WIDTH} km/s"
                            if dv_criterion
                            else f"{int(abs(i[1]-i[0])/nchan*100)}% of the spw"
                        ),
                        f"in {vis}, spw {spw}, field {field}.",
                    ]
                )

                warnings_list.append([warning, message])
                qascores.append(
                    pipelineqa.QAScore(
                        score=WARNINGS_QA_SCORE,
                        shortmsg=warning,
                        longmsg=f'Astrophysical line contamination covering a wide frequency range in {vis}, spw {spw}, field {field}.',
                        applies_to=pipelineqa.TargetDataSelection(vis={vis}, spw={spw}, field={field})
                    )
                )
                LOG.info("Astronomical contamination covering a wide frequency range. %s", message)
            LOG.info(
                "Range %s equivalent to %s km/s or %d%% of the SpW", i, velocity_width_kms, abs(i[1]-i[0])/nchan*100
            )
            if not wide:
                _.append(i)
        line_intervals = np.array(_)
        # possible_line_intervals = merge(possible_line_intervals
        # assert False, f"{line_intervals} {line_intervals.shape} {possible_line_intervals} {possible_line_intervals.shape}"

        if plot:
            if large_baseline_residual:
                LOG.info("%s large_baseline_residual: %s", key, large_baseline_residual)
            plot_figfile = f"{savefigfile}_spw{spw}_field{field}.png" if savefigfile is not None else None
            plot_source_bp_normalized_residuals(
                source_tsys=spec_s,
                bandpass_tsys=spec_b,
                model=mm,
                frequencies=freqs_b / 1e3,
                sigma=np.nanmedian(sigma),  # freqs in GHz. sigma. v3.1
                # level=(None,n_sigma_detection_limit),
                normalized_residuals=normres,
                tsys_contamination_intervals=merge_intervals(line_intervals),
                first_plot_title=f'{tsys.msfile.replace(".ms","")}: spw{spw} field={field} intent={intent_dict[field]}',
                background="mistyrose" if large_baseline_residual else None,  # v3.3
                savefigfile=plot_figfile,
                label_source=intent_dict[field],
                other_emphasized_intervals={
                    "Tsys contamination?": {
                        "color": "grey",
                        "intervals": merge_intervals(possible_line_intervals),
                    },
                    "Telluric": {
                        "color": "blue",
                        "intervals": merge_intervals(telluric_intervals),
                    },
                    "Off science": {
                        "color": "aquamarine",
                        "alpha": 0.4,
                        "intervals": merge_intervals(off_science_line_intervals),
                    },
                    "ATM residual": {
                        "color": "#6a4530",
                        "intervals": merge_intervals(atm_residual_intervals),
                    },
                    "Common BP feature": {
                        "color": "#6a4530",
                        "intervals": merge_intervals(common_feature_intervals),
                    },
                    f"dT/T<{relative_detection_factor*100:.1f}%": {
                        "color": "purple",
                        "hatch": "/",
                        "intervals": low_tsys_contamination,
                    },
                },
                sci_spw_intervals=sci_spw_intervals_in_tsys_channels,
            )
            if plot_figfile is not None and os.path.exists(plot_figfile):
                wrapper = pllogger.Plot(
                    plot_figfile,
                    x_axis='channel',
                    y_axis='tsys',
                    parameters={
                        'vis': os.path.basename(tsysdata.msfile),
                        'field': field,
                        'tsys_spw': spw,
                        'intent': intent_dict[field],
                    }
                )
                plot_wrappers.append(wrapper)

        result[key] = {
            "possible_tsys_contamination": merge_intervals(possible_line_intervals),
            "telluric": merge_intervals(telluric_intervals),
            "atm_residual": merge_intervals(atm_residual_intervals),
            "tsys_contamination": merge_intervals(line_intervals),
            "tsys_contamination_offscience": merge_intervals(
                off_science_line_intervals
            ),
        }

    return result, warnings_list, plot_wrappers, qascores


def separate_non_intersecting_sci_spw(tsysdata, chan_tsys_intervals, tsysspw):  # v2.4
    intervals = chan_tsys_intervals.copy()
    scispws = tsysdata.inversetsysmap[tsysspw]
    scispws_intervals = np.array(
        [
            np.sort(
                tsysdata.msspec[spw][0](np.array([0, tsysdata.msspec[spw][-1] - 1]))
            )
            for spw in scispws
        ]
    )
    scispws_intervals = np.sort(tsysdata.msspec[tsysspw][1](scispws_intervals))
    puffedscispws_intervals = scispws_intervals.copy()
    puffedscispws_intervals[:, 0] = np.floor(puffedscispws_intervals[:, 0])
    puffedscispws_intervals[:, 1] = np.ceil(puffedscispws_intervals[:, 1])
    intersect, non_intersect = [], []
    for i in intervals:
        if (
            np.prod(
                intersection_intervals(i[np.newaxis, :], puffedscispws_intervals).shape
            )
            <= 0
        ):
            non_intersect.append(i)
        else:
            intersect.append(i)
    return {
        "in": np.array(intersect),
        "off": np.array(non_intersect),
        "sci_intervals": dict(
            ((spw, scispws_intervals[i]) for i, spw in enumerate(scispws))
        ),
    }


def intervals_to_casa_string(
    intervals, scaled_array=None, unit="", format=".0f"
) -> str:
    if np.prod(np.array(intervals.shape)) == 0:
        return ""
    if scaled_array is None:
        scaled_array = range(int(np.max(intervals.flatten())) + 1)
    rs = ""
    i = 0
    for k in range(len(np.sort(intervals, axis=0))):
        l0, l1 = intervals[k, 0], intervals[k, 1]
        assert l1 >= l0, f"Inconsistency in integer_set_to_casa_string. {l0} {l1}"
        rs += ";" if i > 0 else ""
        if scaled_array[int(l0)] > scaled_array[int(l1)]:
            l0, l1 = l1, l0  # v3.4
        rs += (
            f"{scaled_array[int(l0)]:{format}}~{scaled_array[int(l1)]:{format}}{unit}"
            if l0 != l1
            else f"{scaled_array[int(l0)]:{format}}{unit}"
        )  # l0!=l1 v3.4
        i += 1
    return rs


def make_tsyscontamination_html_log(title="TsysCont"):

    txtfiles = glob.glob("uid___*.ms_tsys_contamination.txt")
    if len(txtfiles) == 0:
        return False
    logdirname = "tsys_contamination_log"
    if os.path.exists(logdirname):
        os.system(f"rm -rf {logdirname}")
    _ = os.makedirs(logdirname)

    with open(os.path.join(logdirname, "txtstyle.css"), "w") as f:
        f.write(
            """html, body { font-family: Helvetica, Arial, sans-serif; white-space: pre-wrap; font-size:15}
hr.thick {border: 1px solid;}
hr.double{border: 2px double;}
"""
        )
    my_html = open(os.path.join(logdirname, "index.html"), "w")
    my_html.write(
        f'<title>{title}</title>\n<link href="txtstyle.css" rel="stylesheet" type="text/css" />\n'
    )
    version_written = False
    for txtfile in txtfiles:
        m = re.match(r"(uid\S+(X\S+).ms)_tsys_contamination.txt", txtfile)
        assert m, "tsys cont file with bad name?"
        # if not m: continue
        msfile, lid = m.group(1, 2)
        with open(txtfile) as rr:
            for l in rr:
                # print(l,'version' in l, version_written, not('version' in l and version_written))
                if not ("version" in l and version_written):
                    my_html.write(l)
                version_written = version_written or "version" in l

    my_html.write('<hr class="double">\n')
    for txtfile in txtfiles:
        m = re.match(r"(uid\S+(X\S+).ms)_tsys_contamination.txt", txtfile)
        assert m, "x"
        # if not m: continue
        msfile, lid = m.group(1, 2)
        with open(txtfile) as rr:
            txtfile_content = rr.read()
        # print(f" txtfile:{txtfile_content}")
        my_html.write(txtfile_content)
        pngs = glob.glob(f"{msfile}_tsys_contamination_spw*_field*.png")
        pngs.sort()
        for pngfile in pngs:
            copy2(pngfile, logdirname)
            # print(f"{os.path.join('..',pngfile)}")
            try:
                os.remove(pngfile)  # v3.3. Cleaner working folder.
            except FileNotFoundError:
                pass
            _ = os.path.split(pngfile)[1]
            my_html.write(f'<img src="{_}" alt="{_}">\n')
        my_html.write('<hr class="thick">\n')
    my_html.close()

    return True


def main():
    testtables = [  #'uid___A002_Xfaf3de_X1ede.ms.h_tsyscal.s6_1.tsyscal.tbl', #not ok needs pipeline run
        "uid___A002_Xf6b678_X617.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xf73568_X1c47.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. wide line
        "uid___A002_Xf73ead_X1d0f.ms.h_tsyscal.s6_3.tsyscal.tbl",  # ok. Wide line.
        "uid___A002_Xf0fd41_Xe61c.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X1018459_X1249c.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xfc69ac_X711a.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xf87688_Xc13.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. Small features and off-science spws. Off science before
        "uid___A002_Xf8b429_X4bf7.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X103beb6_X293.ms.h_tsyscal.s6_2.tsyscal.tbl",  # ok
        "uid___A002_Xf138ff_X19f7.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xf624a9_X5a2.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X1015532_X2103d.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xf99bb0_X14d02.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X104d6df_X7e2d.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. TM spectra.
        "uid___A002_X10239e1_X6f31.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X1020da2_X452f.ms.h_tsyscal.s6_3.tsyscal.tbl",  # ok
        "uid___A002_X10239e1_X44ce.ms.h_tsyscal.s6_11.tsyscal.tbl",  # ok
        "uid___A002_Xfc434c_X2394.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. B9
        "uid___A002_X1060df2_X706b.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xf4073d_X2613.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_Xfca6fd_X3be8.ms.h_tsyscal.s6_3.tsyscal.tbl",  # ok
        "uid___A002_X1035744_X12b8.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok . ATM residual spw32
        "uid___A002_X1066403_X30ae.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. Large baseline residual. Very wide contamination.
        "uid___A002_X107649f_X10cc.ms.h_tsyscal.s6_11.tsyscal.tbl",  # ok ATM residual spw38. Large baseline residual spw28. #'uid___A002_X106e53c_X2c99.ms.h_tsyscal.s6_9.tsyscal.tbl',#ok
        "uid___A002_X10bc7d4_X396b.ms.h_tsyscal.s6_15.tsyscal.tbl",  #'uid___A002_X10a1d0b_X3c23.ms.h_tsyscal.s6_2.tsyscal.tbl',   # ok. Large telluric residual
        "uid___A002_X10ac6bc_Xc408.ms.h_tsyscal.s6_1.tsyscal.tbl",  # single pol . ok.
        "uid___A002_X10bc7d4_Xadb8.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok. Common BP feature.
        "uid___A002_X10b6f7c_X1c306.ms.h_tsyscal.s6_3.tsyscal.tbl",  # ok
        "uid___A002_X10cc13c_Xcd9b.ms.h_tsyscal.s6_1.tsyscal.tbl",  # ok
        "uid___A002_X10ded83_X9793.ms.h_tsyscal.s6_3.tsyscal.tbl",  #
        "uid___A002_X1096e27_X3b01.ms.h_tsyscal.s6_1.tsyscal.tbl",  # telluric near range
        "uid___A002_X10eb7e9_X7c12.ms.h_tsyscal.s6_1.tsyscal.tbl",  # telluric on top of contaminated range in spw26
        "uid___A002_X10ed869_Xde7.ms.h_tsyscal.s6_1.tsyscal.tbl",  # 'uid___A002_X10ed869_Xde7_flagged.ms.h_tsyscal.s6_1.tsyscal.tbl',
    ]
    tsystables = (
        testtables
        if "testing" in VERSION
        else glob.glob("uid___*.ms.h_tsyscal.*.tsyscal.tbl")
    )
    # random.shuffle(tsystables)
    for tsystable in tsystables[0:]:
        tsys = TsysData(
            tsystable=tsystable,
            load_pickle=True,
            single_polarization=(
                tsystable == "uid___A002_X10ac6bc_Xc408.ms.h_tsyscal.s6_1.tsyscal.tbl"
            ),
        )

        [spws, intents, scans, fields] = [
            tsys.tsysdata[tsys.tsysfields.index(f)]
            for f in ["spw", "intent", "scan", "field"]
        ]
        field_intent_dict = []
        scan_field_dict = []

        for i, f, s in zip(intents, fields, scans):
            field_intent_dict.append((f, i))
            scan_field_dict.append((s, f))
        field_intent_dict = dict(set(field_intent_dict))
        scan_field_dict = dict(set(scan_field_dict))
        field_scanlist_dict = dict()
        for k, v in scan_field_dict.items():
            field_scanlist_dict.setdefault(v, []).append(k)

        file = f"{tsys.tsystable}.tsysdata.pbz2"
        if not os.path.exists(file):
            save_tsysdata(tsys)
        baktbl = f"{tsystable}.bak"
        if not os.path.exists(baktbl):
            # rmtree(baktbl)
            copytree(tsystable, baktbl)
        LOG.info("%s\n%s\n%s", ("-" * 40), tsys.msfile, ("-" * 40))  # v2.3
        line_contamination_intervals, warnings_list, _ = get_tsys_contaminated_intervals(
            tsys,
            plot=True,  # large baseline residual v3.3
            # spwlist=[np.int(30)],fieldlist=[np.int64(2)],# this selection does not work with the saved spool sample
            remove_n_extreme=2,
            relative_detection_factor=0.5 / 100,
            savefigfile=f"{tsys.msfile}_tsys_contamination",
        )
        # assert False, ""
        for k, v in line_contamination_intervals.copy().items():
            if np.sum(np.array([len(vv) for kk, vv in v.items()])) == 0:
                del line_contamination_intervals[k]

        all_freqs_mhz = tsys.specdata[tsys.specfields.index("freq_mhz")]

        output_file = tsys.msfile.replace(".ms", ".ms_tsys_contamination.txt")
        f = open(output_file, "w")

        flagtsystemplate_file = tsys.msfile.replace(".ms", ".flagtsystemplate.txt")
        ft = open(flagtsystemplate_file, "a")

        pl_run_dir = ""
        m = re.match(
            r".*([0-9]{4}\..\.[0-9]{5}\.[^_]+_[0-9]{4}_[0-9]{2}_[0-9]{2}T[0-9]{2}_[0-9]{2}_[0-9]{2}\.[0-9]+).*",
            os.getcwd(),
        )
        if m:
            pl_run_dir = m.group(1)
            LOG.info("In pipeline working dir: saving timestamp code %s", pl_run_dir)
        f.write(f"\n# script version {VERSION} {pl_run_dir}\n# {tsystable}\n")

        field_contamination = dict()
        for k in line_contamination_intervals:
            m = re.match(r"(?P<spw>[0-9]+)_(?P<field>[0-9]+)", k)
            spw, field = m.group(1, 2)
            field_contamination.setdefault(np.int64(field), []).append(np.int64(spw))

        if len(field_contamination) == 0:
            _ = f"## No tsys contamination identified.\n"
            f.write(_)
            LOG.info(_)

        # v3.3 large baseline residual
        for w in warnings_list:
            _ = " ".join(w)
            f.write(f"# {_}\n")

        for field in field_contamination:
            field = np.int64(field)
            spw_ranges = []
            spw_ranges_freq = []

            for spw in field_contamination[field]:
                if field_intent_dict[field] == "bandpass":
                    continue
                key = f"{spw}_{field}"
                spw = np.int64(spw)
                freqs_ghz = (
                    all_freqs_mhz[
                        np.nonzero(tsys.specdata[tsys.specfields.index("spw")] == spw)[
                            0
                        ][0]
                    ]
                    / 1000
                )
                rs = intervals_to_casa_string(
                    line_contamination_intervals[key]["tsys_contamination"]
                )
                rsf = intervals_to_casa_string(
                    line_contamination_intervals[key]["tsys_contamination"],
                    scaled_array=freqs_ghz,
                    unit="GHz",
                    format=".3f",
                )
                if rs != "":
                    spw_ranges.append(f"{spw}:{rs}")
                    spw_ranges_freq.append(f"{spw}:{rsf}")

            if len(spw_ranges) == 0:
                continue  # v2.2
            spw_ranges = ",".join(spw_ranges)
            spw_ranges_freq = ",".join(spw_ranges_freq)
            contamination_scans = field_scanlist_dict[field]
            contamination_scans.sort()
            _ = f"## {tsystable}: field={field}, intent={field_intent_dict[field]}\n"
            # for warn_ in warnings_list:
            #     _ += f"#{warn_}\n"
            _ += f"# Frequency ranges: '{spw_ranges_freq}' \n"
            flagline = f"# mode='manual' scan='{','.join([str(sc) for sc in contamination_scans])}' spw='{spw_ranges}' reason='Tsys:tsysflag_tsys_channel'\n"
            _ += flagline
            f.write(_)
            LOG.info(_)
            ft.write(flagline)

        f.close()
        ft.close()

    if make_tsyscontamination_html_log():
        LOG.info("Log folder created.")
        if "testing" in VERSION or False:
            subprocess.call(
                [
                    "rsync",
                    "-uva",
                    "tsys_contamination_log",
                    os.path.join(os.environ["SPOOLAREA"], "PIPEREQ-49", "PIPEREQ-232/"),
                ],
                shell=False,
            )


with warnings.catch_warnings():  # v2.3
    if "testing" in VERSION:
        warnings.filterwarnings(
            action="ignore", category=RuntimeWarning
        )  # ignore these warnings which are not very useful
    # main()
