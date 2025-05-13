from typing import Union, List, Dict, Tuple
from unittest.mock import Mock

import numpy as np
import pytest
import copy
import os

from pipeline import domain
from pipeline.infrastructure import casa_tools, casa_tasks

from .utils import find_ranges, dict_merge, are_equal, approx_equal, flagged_intervals, \
    get_casa_quantity, get_num_caltable_polarizations, fieldname_for_casa, fieldname_clean, \
    get_field_accessor, get_field_identifiers, get_receiver_type_for_spws, place_repr_source_first, \
    get_taskhistory_fromimage

params_find_ranges = [('', ''), ([], ''), ('1:2', '1:2'), ([1, 2, 3], '1~3'),
                      (['5~12', '14', '16:17'], '5~12,14,16:17'),
                      ([1, 2, 3, 6, 7], '1~3,6~7'),
                      ([1, 2, 3, '6', '7'], '1~3,6~7')]


@pytest.mark.parametrize('data, expected', params_find_ranges)
def test_find_ranges(data: Union[str, list], expected: str):
    """Test find_ranges()

    This utility function takes a string or a list of integers (e.g. spectral
    window lists) and returns a string containing identified ranges.
    E.g. [1,2,3] -> '1~3'
    """
    assert find_ranges(data) == expected


params_dict_merge = [({}, {}, {}), ({}, 1, 1), ({'a': 1}, {}, {'a': 1}),
                     ({'a': {'b': 1}}, {'c': 2}, {'a': {'b': 1}, 'c': 2}),
                     ({'a': {'b': 1}}, {'a': {'b': 2}}, {'a': {'b': 2}}),
                     ({'a': {'b': 1}}, {'a': 2}, {'a': 2}),
                     ({'a': {'b': {'c': 1}}}, {'a': {'b': {'c': 2}}},
                      {'a': {'b': {'c': 2}}})]


@pytest.mark.parametrize('a, b, expected', params_dict_merge)
def test_dict_merge(a: Dict, b: Dict, expected: Dict):
    """Test dict_merge()

    This utility function recursively merges dictionaries. If second argument
    (b) is a dictionary, then a copy of first argument (dictionary a) is created
    and the elements of b are merged into the new dictionary. Otherwise return
    argument b.

    In case of matching non-dictionary value keywords, content of dictionary b
    overwrites that of dictionary a. If the matching keyword value is a dictionary
    then continue merging recursively.
    """
    assert dict_merge(a, b) == expected


params_are_equal = [([1, 2, 3], [1, 2, 3], True), ([1, 2.5, 3], [1, 2, 3], False),
                    (np.ones(2), np.zeros(2), False), (np.ones(2), np.ones(2), True),
                    (np.ones(2), np.ones(3), False)]


@pytest.mark.parametrize('a, b, expected', params_are_equal)
def test_are_equal(a: Union[List, np.ndarray], b: Union[List, np.ndarray], expected: bool):
    """Test are_equal()

    This utility function check the equivalence of array like objects. Two arrays
    are equal if they have the same number of elements and elements of the same
    index are equal.
    """
    assert are_equal(a, b) == expected


params_approx_eqaul = [(1.0e-2, 1.2e-2, 1e-2, True),
                       (1.0e-2, 1.2e-2, 1e-3, False),
                       (1.0, 2.0, 0.1, False), (1, 2, 10, True)]


@pytest.mark.parametrize('x, y, tol, expected', params_approx_eqaul)
def test_approx_equal(x: float, y: float, tol: float, expected: bool):
    """Test approx_equal()

    This utility function returns True if two numbers are equal within the
    given tolerance.
    """
    assert approx_equal(x, y, tol=tol) == expected


params_test_get_num_calltable_pol = [('uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms.hifa_'
                                      'timegaincal.s17_7.spw0.solintinf.gacal.tbl', 1),
                                     ('uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms.hifa_'
                                      'timegaincal.s17_2.spw0.solintinf.gpcal.tbl', 2)]


@pytest.mark.parametrize('caltable, expected', params_test_get_num_calltable_pol)
def test_get_num_caltable_polarizations(caltable: str, expected: int):
    """Test get_num_caltable_polarizations()
    """
    assert get_num_caltable_polarizations(caltable=casa_tools.utils.resolve('pl-unittest/'+caltable)) == expected


params_flagged_intervals = [([], []), ([1, 2], [(0, 0)]),
                            ([0, 1, 0, 1, 1], [(1, 1), (3, 4)]),
                            ([0, 1, 0, 1, 2], [(1, 1), (3, 3)])]


@pytest.mark.parametrize('vec, expected', params_flagged_intervals)
def test_flagged_intervals(vec: Union[List[int], np.ndarray], expected: List[Tuple[int]]):
    """Test flagged_intervals()

    This utility function finds islands of ones in vector provided in argument.
    Used to find contiguous flagged channels in a given spw.  Returns a list of
    tuples with the start and end channels.
    """
    assert flagged_intervals(vec=vec) == expected


params_fieldname_for_casa = [('', ''), ('helm30', 'helm30'),
                             ('helm=30', '"helm=30"'), ('1', '"1"')]


@pytest.mark.parametrize('field, expected', params_fieldname_for_casa)
def test_fieldname_for_casa(field: str, expected: str):
    """Test fieldname_for_casa()

    This utility function ensures that field string can be used as CASA argument.

    If field contains special characters, then return field string enclose in
    quotation marks, otherwise return unchanged string.
    """
    assert fieldname_for_casa(field=field) == expected


params_fieldname_clean = [('', ''), ('helm30', 'helm30'), ('helm=30', 'helm_30'),
                          ('1', '1')]


@pytest.mark.parametrize('field, expected', params_fieldname_clean)
def test_fieldname_clean(field: str, expected: str):
    """Test fieldname_clean()

    This utility function replaces special characters in string with underscore.
    """
    assert fieldname_clean(field=field) == expected


# Create mock Fields and MeasurementSets for testing get_field_accessor() and get_field_identifiers()
# The Field name and id attributes, and MeasurementSet fields attribute and get_fields() methods are
# accessed.
fields = []
for i, fn in enumerate(['Mars', 'Jupiter', 'Mars']):
    m = Mock(spec=domain.Field, **{'id': i + 1})
    m.name = fn  # Mock name and name attribute interfere, set attribute explicitly
    fields.append(m)

# get_fields() is called only once in this test, therefore set return_value.
params_get_field_accessor = [
    (Mock(spec=domain.MeasurementSet, **{
         'get_fields.return_value': [fields[1]]
    }), fields[1], 'Jupiter'),  # All fields names are unique
    (Mock(spec=domain.MeasurementSet, **{
        'get_fields.return_value': [fields[0], fields[2]]
    }), fields[2], '3')]  # Field name 'Mars' repeats


@pytest.mark.parametrize('ms, field, expected', params_get_field_accessor)
def test_get_field_accessor(ms, field, expected):
    """Test get_field_accessor()

    This utility function returns an attribute getter. If the field specified
    in the argument is unique in the MeasurementSet, then the getter will access
    the field name (name attribute), otherwise the getter will access the field
    id (id attribute).
    """
    assert get_field_accessor(ms, field)(field) == expected


# get_fields() returns all fields with the name given in argument, mock this behaviour
# The method is called multiple times, therefore set side_effect.
params_get_field_ids = [
    (Mock(spec=domain.MeasurementSet, **{
        'fields': fields[0:2],
        'get_fields.side_effect': [[f] for f in fields[0:2]]
    }), {1: 'Mars', 2: 'Jupiter'}),  # All fields names are unique
    (Mock(spec=domain.MeasurementSet, **{
        'fields': fields,
        'get_fields.side_effect': [[fields[0], fields[2]],
                                   [fields[1]],
                                   [fields[0], fields[2]]]
    }), {1: '1', 2: 'Jupiter', 3: '3'})]  # Field name 'Mars' repeats


@pytest.mark.parametrize('ms, expected', params_get_field_ids)
def test_get_field_identifiers(ms, expected):
    """Test get_field_identifiers()

    This utility function returns a dictionary with field ID keys and either
    field name or str(field ID) values. The latter happens when a field name
    occurs more than once.
    """
    assert get_field_identifiers(ms=ms) == expected


params_get_receiver_type_for_spws = [
    (Mock(spec=domain.MeasurementSet, **{
        'get_spectral_windows.side_effect': [None,
                                             [Mock(**{'receiver': 'fake'})]]
    }), [1, 2], {1: 'N/A', 2: 'fake'})]


@pytest.mark.parametrize('ms, spwids, expected', params_get_receiver_type_for_spws)
def test_get_receiver_type_for_spws(ms, spwids, expected):
    """Test get_receiver_type_for_spws()

    This utility function returns a dictionary with spectral window IDs (spwids
    arguemnt) as keys and the associated receiver strings in the MeasurementSet
    as values. If spectral window ID is not found in the MeasurementSet, then
    the associated values is set to 'N/A'.
    """
    assert get_receiver_type_for_spws(ms=ms, spwids=spwids) == expected


params_get_casa_quantity = [(None, {'unit': '', 'value': 0.0}),
                            ('10klambda', {'unit': 'klambda', 'value': 10.0}),
                            (10.0, {'unit': '', 'value': 10.0})]


@pytest.mark.parametrize('value, expected', params_get_casa_quantity)
def test_get_casa_quantity(value: Union[str, float, Dict, None], expected: Dict):
    """Test get_casa_quantity()

    This utility function handles None values when calling CASA quanta.quantity()
    tool method.
    """
    assert get_casa_quantity(value) == expected


params_place_repr_source_first = [(['f0', 'f1', 'f2', 'f3'], 'f2', ['f2', 'f0', 'f1', 'f3']),
                                  ([('f3', 'a'), ('f2', 'b'), ('f0', 'p'), ('f1', 't')], 'f2', [('f2', 'b'), ('f3', 'a'), ('f0', 'p'), ('f1', 't')])]


@pytest.mark.parametrize('itemlist, repr_source, expected', params_place_repr_source_first)
def test_place_repr_source_first(itemlist: Union[List[str], List[Tuple]], repr_source: str, expected: Union[List[str], List[Tuple]]):
    """
    Test place_repr_source_first()
    """
    assert place_repr_source_first(itemlist, repr_source) == expected


def test_get_taskhistory_fromimage(tmpdir):
    """Test get_taskhistory_fromimage()."""

    tclean_job_base = {'vis': 'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms',
                       'field': 'Uranus', 'spw': ['0'], 'antenna': ['0,1,2,3,4,5,6,7,8,9,10&'],
                       'scan': ['6'], 'intent': 'CALIBRATE_FLUX#ON_SOURCE', 'datacolumn': 'data',
                       'imagename': 'oussid.s21_0.Uranus_flux.spw0.mfs.I.iter0', 'imsize': [90, 90],
                       'cell': ['0.9arcsec'], 'phasecenter': 'TRACKFIELD', 'stokes': 'I', 'specmode': 'mfs',
                       'pblimit': 0.2, 'deconvolver': 'hogbom', 'restoringbeam': 'common', 'pbcor': False,
                       'weighting': 'briggs', 'robust': 0.5, 'npixels': 0, 'niter': 0, 'savemodel': 'none', 'parallel': False}

    tclean_job_list = [copy.deepcopy(tclean_job_base) for i in range(2)]
    tclean_job_list[1]['niter'] = 1

    for tclean_job_parameters in tclean_job_list:
        vis = casa_tools.utils.resolve(f"pl-unittest/{tclean_job_parameters['vis']}")
        tclean_job_parameters['vis'] = vis
        tclean_job_parameters['imagename'] = os.path.join(str(tmpdir), tclean_job_parameters['imagename'])
        job = casa_tasks.tclean(**tclean_job_parameters)
        job.execute()

    task_history_list = get_taskhistory_fromimage(tclean_job_parameters['imagename']+'.model')

    assert len(task_history_list) == len(tclean_job_list)
    for idx, task_job in enumerate(tclean_job_list):
        for k, v in task_job.items():
            assert task_history_list[idx][k] == v
