import pytest
from .. import casa_tools

from .imaging import _get_cube_freq_axis, chan_selection_to_frequencies, \
                     freq_selection_to_channels, spw_intersect, \
                     update_sens_dict, update_beams_dict, set_nested_dict, \
                     intersect_ranges, intersect_ranges_by_weight, merge_ranges, \
                     equal_to_n_digits, velocity_to_frequency, frequency_to_velocity, \
                     predict_kernel

from .. import casa_tools


_get_cube_freq_axis_test_params = ((casa_tools.utils.resolve('pl-unittest/helms30_sci.spw16.cube'), (214450186328.0, 15624760.100036621, 'Hz', 0.0, 117)),)

@pytest.mark.parametrize("img, freq_axis", _get_cube_freq_axis_test_params)
def test__get_cube_freq_axis(img, freq_axis):
    """
    Test _get_cube_freq_axis()
    """

    assert _get_cube_freq_axis(img) == freq_axis


chan_selection_to_frequencies_test_params = ((casa_tools.utils.resolve('pl-unittest/helms30_sci.spw16.cube'), '10~20', 'GHz', [(214.59862154895035, 214.77049391005076)]),
                                             (casa_tools.utils.resolve('pl-unittest/helms30_sci.spw16.cube'), '40~50;60~80', 'GHz', [(215.06736435195145, 215.23923671305187), (215.3798595539522, 215.70797951605297)]))

@pytest.mark.parametrize("img, selection, unit, frequency_ranges", chan_selection_to_frequencies_test_params)
def test_chan_selection_to_frequencies(img, selection, unit, frequency_ranges):
    """
    Test chan_selection_to_frequencies()
    """

    assert chan_selection_to_frequencies(img, selection, unit) == frequency_ranges


freq_selection_to_channels_test_params = ((casa_tools.utils.resolve('pl-unittest/helms30_sci.spw16.cube'), '214.5~214.9GHz', [(4, 28)]),
                                          (casa_tools.utils.resolve('pl-unittest/helms30_sci.spw16.cube'), '214.5~214.9GHz;215123.4~215567.8MHz', [(4, 28), (44, 71)]))

@pytest.mark.parametrize("img, selection, channel_ranges", freq_selection_to_channels_test_params)
def test_freq_selection_to_channels(img, selection, channel_ranges):
    """
    Test freq_selection_to_channels()
    """

    assert freq_selection_to_channels(img, selection) == channel_ranges


spw_intersect_test_params = (([4, 12],
                              [[7, 9]],
                              [[4, 7], [9, 12]]),
                             ([4, 12],
                              [[4, 5]],
                              [[5, 12]]),
                             ([4, 12],
                              [[11, 12]],
                              [[4, 11]]),
                             ([4, 12],
                              [[4, 5], [11, 12]],
                              [[5, 11]]),
                             ([4, 12],
                              [[5, 6], [10, 11]],
                              [[4, 5], [6, 10], [11, 12]]),
                             ([228.0, 232.0],
                              [[229.7, 229.9], [230.4, 230.6], [231.0, 231.5]],
                              [[228.0, 229.7], [229.9, 230.4], [230.6, 231.0], [231.5, 232.0]]))


@pytest.mark.parametrize("spw_range, line_regions, expected", spw_intersect_test_params)
def test_spw_intersect(spw_range, line_regions, expected):
    """
    Test spw_intersect()

    This utility function takes a frequency range (as unitless integers
    or doubles) and computes the intersection with a list of frequency
    ranges denoting the regions of spectral lines. It returns the remaining
    ranges excluding the line frequency ranges.

    """
    assert spw_intersect(spw_range, line_regions) == expected


update_sens_dict_test_params = (({'robust': 0.5,
                                  'uvtaper': [],
                                  'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {16:
                                         {'sensitivityAllChans': '0.000261 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 },
                                 {'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {18:
                                         {'sensitivityAllChans': '0.000248 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        20:
                                         {'sensitivityAllChans': '0.000281 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 },
                                 {'robust': 0.5,
                                  'uvtaper': [],
                                  'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {16:
                                         {'sensitivityAllChans': '0.000261 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        18:
                                         {'sensitivityAllChans': '0.000248 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        20:
                                         {'sensitivityAllChans': '0.000281 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 }),)

@pytest.mark.parametrize("dct, udct, rdct", update_sens_dict_test_params)
def test_update_sens_dict(dct, udct, rdct):
    """
    Test update_sens_dict()
    """

    sens_dct = dct
    update_sens_dict(sens_dct, udct)
    assert sens_dct == rdct

update_beams_dict_test_params = (({'robust': 0.5,
                                   'uvtaper': [],
                                   'J0006-0623':
                                    {'BANDPASS':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.52, 'unit': 'arcsec'},
                                           'minor': {'value': 4.1, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': -85.49505615234375}
                                          }
                                        }
                                      }
                                    }
                                  },
                                  {'Uranus':
                                    {'AMPLITUDE':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.49, 'unit': 'arcsec'},
                                           'minor': {'value': 4.56, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': 79.95410919189453}
                                          }
                                        }
                                      }
                                    }
                                  },
                                  {'robust': 0.5,
                                   'uvtaper': [],
                                   'J0006-0623':
                                    {'BANDPASS':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.52, 'unit': 'arcsec'},
                                           'minor': {'value': 4.1, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': -85.49505615234375}
                                          }
                                        }
                                      }
                                    },
                                   'Uranus':
                                    {'AMPLITUDE':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.49, 'unit': 'arcsec'},
                                           'minor': {'value': 4.56, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': 79.95410919189453}
                                          }
                                        }
                                      }
                                    }
                                  }),)

@pytest.mark.parametrize("dct, udct, rdct", update_beams_dict_test_params)
def test_update_beams_dict(dct, udct, rdct):
    """
    Test update_beams_dict()
    """

    beams_dct = dct
    update_beams_dict(beams_dct, udct)
    assert beams_dct == rdct


set_nested_dict_test_params = (({}, ('key1', 'key2', 'key3'), 1, {'key1': {'key2': {'key3': 1}}}),)

@pytest.mark.parametrize("dct, keys, value, rdct", set_nested_dict_test_params)
def test_set_nested_dict(dct, keys, value, rdct):
    """
    Test set_nested_dict()
    """

    test_dct = dct
    set_nested_dict(test_dct, keys, value)
    assert test_dct == rdct


intersect_ranges_test_params = (([(10, 20), (15, 25)], (15, 20)),
                                ([(230.3, 231.5), (230.9, 231.2)], (230.9, 231.2)),
                                ([(230.3, 231.5), (232.9, 233.2)], ()),
                                ([], ()))

@pytest.mark.parametrize("ranges, intersect_range", intersect_ranges_test_params)
def test_intersect_ranges(ranges, intersect_range):
    """
    Test intersect_ranges()
    """

    assert intersect_ranges(ranges) == intersect_range


intersect_ranges_by_weight_test_params = (([(230.3, 231.5), (230.9, 231.2)], 0.01, 0.5, (230.3, 231.49999999999892)),
                                          ([(230.3, 231.5), (230.9, 231.2)], 0.01, 1.0, (230.90999999999946, 231.1999999999992)),
                                          ([(230.0, 231.0), (230.05, 230.95), (230.07, 230.93), (230.1, 230.9)], 0.0001, 0.7, (230.07000000000232, 230.92990000003087)),
                                          ([(230.1, 230.2), (230.3, 230.4)], 0.001, 1.0, ()),
                                          ([], 0.01, 1.0, ()))

@pytest.mark.parametrize("ranges, delta, threshold, intersect_range", intersect_ranges_by_weight_test_params)
def test_intersect_ranges_by_weight(ranges, delta, threshold, intersect_range):
    """
    Test intersect_ranges_by_weight()
    """

    assert intersect_ranges_by_weight(ranges, delta, threshold) == intersect_range


merge_ranges_test_params = (([(5,7), (3,5), (-1,3)], [(-1, 7)]),
                            ([(5,6), (3,4), (1,2)], [(1, 2), (3, 4), (5, 6)]),
                            ([], []))

@pytest.mark.parametrize("ranges, merged_ranges", merge_ranges_test_params)
def test_merge_ranges(ranges, merged_ranges):
    """
    Test merge_ranges()
    """

    assert list(merge_ranges(ranges)) == merged_ranges


equal_to_n_digits_test_params = ((1.234, 1.235, 3, True), (1.234, 1.235, 4, False))

@pytest.mark.parametrize("x, y, numdigits, result", equal_to_n_digits_test_params)
def test_equal_to_n_digits(x, y, numdigits, result):
    """
    Test equal_to_n_digits()
    """

    assert equal_to_n_digits(x, y, numdigits) == result


velocity_to_frequency_test_params = (('29.976248175km/s', '100.01GHz', '100.0GHz'),)

@pytest.mark.parametrize("velocity, restfreq, result", velocity_to_frequency_test_params)
def test_velocity_to_frequency(velocity, restfreq, result):
    """
    Test velocity_to_frequency()
    """

    cqa = casa_tools.quanta

    assert cqa.eq(velocity_to_frequency(velocity, restfreq), result)


frequency_to_velocity_test_params = (('100.0GHz', '100.01GHz', '29.976248175km/s'),)


@pytest.mark.parametrize("frequency, restfreq, result", frequency_to_velocity_test_params)
def test_frequency_to_velocity(frequency, restfreq, result):
    """
    Test frequency_to_velocity()
    """

    cqa = casa_tools.quanta

    assert cqa.eq(frequency_to_velocity(frequency, restfreq), result)


predict_kernel_test_params = (({'major': {'unit': 'arcsec', 'value': 1.3},
                                'minor': {'unit': 'arcsec', 'value': 1.2},
                                'positionangle': {'unit': 'deg', 'value': 50.0}},
                               {'major': {'unit': 'arcsec', 'value': 1.3},
                                'minor': {'unit': 'arcsec', 'value': 1.2},
                                'positionangle': {'unit': 'deg', 'value': 20.0}},
                               1e-6, 1e-3,
                               {'major': {'unit': 'arcsec', 'value': 0.0},
                                'minor': {'unit': 'arcsec', 'value': 0.0},
                                'pa': {'unit': 'deg', 'value': 0.0}},
                               2),
                              ({'major': {'unit': 'arcsec', 'value': 1.3},
                                'minor': {'unit': 'arcsec', 'value': 1.2},
                                'positionangle': {'unit': 'deg', 'value': 50.0}},
                               {'major': {'unit': 'arcsec', 'value': 1.3},
                                'minor': {'unit': 'arcsec', 'value': 1.2},
                                'positionangle': {'unit': 'deg', 'value': 50.0}},
                               1e-6, 1e-3,
                               {'major': {'unit': 'arcsec', 'value': 0.0},
                                'minor': {'unit': 'arcsec', 'value': 0.0},
                                'pa': {'unit': 'deg', 'value': 0.0}},
                               1),
                              ({'major': {'unit': 'arcsec', 'value': 1.0},
                                'minor': {'unit': 'arcsec', 'value': 1.0},
                                  'positionangle': {'unit': 'deg', 'value': 50.0}},
                               {'major': {'unit': 'arcsec', 'value': 1.3},
                                'minor': {'unit': 'arcsec', 'value': 1.2},
                                'positionangle': {'unit': 'deg', 'value': 20.0}},
                               1e-6, 1e-3,
                              {'major': {'unit': 'arcsec', 'value': 0.8306623862918077},
                               'minor': {'unit': 'arcsec', 'value': 0.6633249580710798},
                               'pa': {'unit': 'deg', 'value': 19.999999999999982}},
                               0),
                              )


@pytest.mark.parametrize("beam, target_beam, pstol, patol, kn, kn_code", predict_kernel_test_params)
def test_predict_kernel(beam, target_beam, pstol, patol, kn, kn_code):
    """
    Test predict_kernel()
    """

    assert predict_kernel(beam, target_beam, pstol=pstol, patol=patol) == (kn, kn_code)
