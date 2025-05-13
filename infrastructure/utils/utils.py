"""
The utils module contains general-purpose uncategorised utility functions and
classes.
"""
import ast
import bisect
import collections
import contextlib
import copy
import errno
import fcntl
import glob
import itertools
import operator
import os
import pickle
import re
import shutil
import string
import tarfile
from typing import Collection, Dict, List, Optional, Sequence, Tuple, Union

import casaplotms
import numpy as np

from .. import casa_tools, logging, mpihelpers
from .conversion import dequote, range_to_list

LOG = logging.get_logger(__name__)

__all__ = ['find_ranges', 'dict_merge', 'are_equal', 'approx_equal', 'get_num_caltable_polarizations', 'fieldname_for_casa',
           'flagged_intervals', 'get_field_identifiers', 'get_receiver_type_for_spws', 'get_spectralspec_to_spwid_map',
           'imstat_items', 'get_stokes', 'get_taskhistory_fromimage', 'glob_ordered', 'deduplicate',
           'get_casa_quantity', 'get_si_prefix', 'absolute_path', 'relative_path', 'get_task_result_count',
           'place_repr_source_first', 'shutdown_plotms', 'get_casa_session_details', 'get_obj_size', 'get_products_dir',
           'export_weblog_as_tar', 'ensure_products_dir_exists', 'ignore_pointing', 'request_omp_threading',
           'open_with_lock', 'nested_dict', 'string_to_val', 'remove_trailing_string']


def find_ranges(data: Union[str, List[int]]) -> str:
    """Identify numeric ranges in string or list.

    This utility function takes a string or a list of integers (e.g. spectral
    window lists) and returns a string containing identified ranges.

    Examples:
    >>> find_ranges([1,2,3])
    '1~3'
    >>> find_ranges('1,2,3,5~12')
    '1~3,5~12'
    """
    if isinstance(data, str):
        # barf if channel ranges are also in data, eg. 23:1~10,24
        if ':' in data:
            return data

        data = range_to_list(data)
        if len(data) == 0:
            return ''

    try:
        integers = [int(d) for d in data]
    except ValueError:
        return ','.join(data)

    s = sorted(integers)
    ranges = []
    for _, g in itertools.groupby(enumerate(s), lambda i_x: i_x[0] - i_x[1]):
        rng = list(map(operator.itemgetter(1), g))
        if len(rng) == 1:
            ranges.append('%s' % rng[0])
        else:
            ranges.append('%s~%s' % (rng[0], rng[-1]))
    return ','.join(ranges)


def dict_merge(a: Dict, b: Union[Dict, any]) -> Dict:
    """Recursively merge dictionaries.

    This utility function recursively merges dictionaries. If second argument
    (b) is a dictionary, then a copy of first argument (dictionary a) is created
    and the elements of b are merged into the new dictionary. Otherwise return
    argument b.

    Examples:
    >>> dict_merge({'a': {'b': 1}}, {'c': 2})
    {'a': {'b': 1}, 'c': 2}
    """
    if not isinstance(b, dict):
        return b
    result = copy.deepcopy(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict):
            result[k] = dict_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def are_equal(a: Union[List, np.ndarray], b: Union[List, np.ndarray]) -> bool:
    """Return True if the contents of the given arrays are equal.

    This utility function check the equivalence of array like objects. Two arrays
    are equal if they have the same number of elements and elements of the same
    index are equal.

    Examples:
    >>> are_equal([1, 2, 3], [1, 2, 3])
    True
    >>> are_equal([1, 2, 3], [1, 2, 3, 4])
    False
    """
    return len(a) == len(b) and len(a) == sum([1 for i, j in zip(a, b) if i == j])


def approx_equal(x: float, y: float, tol: float = 1e-15) -> bool:
    """Return True if two numbers are equal within the given tolerance.

    This utility function returns True if two numbers are equal within the
    given tolerance.

    Examples:
    >>> approx_equal(1.0e-2, 1.2e-2, 1e-2)
    True
    >>> approx_equal(1.0e-2, 1.2e-2, 1e-3)
    False
    """
    lo = min(x, y)
    hi = max(x, y)
    return (lo + 0.5 * tol) >= (hi - 0.5 * tol)


def get_num_caltable_polarizations(caltable: str) -> int:
    """Obtain number of polarisations from calibration table.

    Seemingly the number of QA ID does not map directly to the number of
    polarisations for the spw in the MS, but the number of polarisations for
    the spw as held in the caltable.
    """
    with casa_tools.TableReader(caltable) as tb:
        col_shapes = set(tb.getcolshapestring('CPARAM'))

    # get the number of pols stored in the caltable, checking that this
    # is consistent across all rows
    fmt = re.compile(r'\[(?P<num_pols>\d+), (?P<num_rows>\d+)\]')
    col_pols = set()
    for shape in col_shapes:
        m = fmt.match(shape)
        if m:
            col_pols.add(int(m.group('num_pols')))
        else:
            raise ValueError('Could not find shape of polarisation from %s' % shape)

    if len(col_pols) != 1:
        raise ValueError('Got %s polarisations from %s' % (len(col_pols), col_shapes))

    return int(col_pols.pop())


def flagged_intervals(vec: Union[List, np.ndarray]) -> List:
    """Idendity isnads of ones in input array or list.

    This utility function finds islands of ones in array or list provided in argument.
    Used to find contiguous flagged channels in a given spw.  Returns a list of
    tuples with the start and end channels.

    Examples:
    >>> flagged_intervals([0, 1, 0, 1, 1])
    [(1, 1), (3, 4)]
    """
    if len(vec) == 0:
        return []
    elif not isinstance(vec, np.ndarray):
        vec = np.array(vec)

    edges, = np.nonzero(np.diff((vec == True) * 1))
    edge_vec = [edges + 1]
    if vec[0] != 0:
        edge_vec.insert(0, [0])
    if vec[-1] != 0:
        edge_vec.append([len(vec)])
    edges = np.concatenate(edge_vec)
    return list(zip(edges[::2], edges[1::2] - 1))


def fieldname_for_casa(field: str) -> str:
    """Prepare field string to be used as CASA argument.

    This utility function ensures that field string can be used as CASA argument.

    If field contains special characters, then return field string enclose in
    quotation marks, otherwise return unchanged string.

    Examples:
    >>> fieldname_for_casa('helm=30')
    '"helm=30"'
    """
    if field.isdigit() or field != fieldname_clean(field):
        return '"{0}"'.format(field)
    return field


def fieldname_clean(field: str) -> str:
    """Indicate if the fieldname is allowed as-is.

    This utility function replaces special characters in string with underscore.
    The return string is used in fieldname_for_casa() function to determine
    whether the field name, when given as a CASA argument, should be enclosed
    in quotes.

    Examples:
    >>> fieldname_clean('helm=30')
    'helm_30'
    """
    allowed = string.ascii_letters + string.digits + '+-'
    return ''.join([c if c in allowed else '_' for c in field])


def get_field_accessor(ms, field):
    """Returns accessor to field name or field ID, if field name is ambiguous.
    """
    fields = ms.get_fields(name=field.name)
    if len(fields) == 1:
        return operator.attrgetter('name')

    def accessor(x):
        return str(operator.attrgetter('id')(x))
    return accessor


def get_field_identifiers(ms) -> Dict:
    """Maps numeric field IDs to field names.

    Get a dict of numeric field ID to unambiguous field identifier, using the
    field name where possible and falling back to numeric field ID where the
    name is duplicated, for instance in mosaic pointings.
    """
    field_name_accessors = {field.id: get_field_accessor(ms, field) for field in ms.fields}
    return {field.id: field_name_accessors[field.id](field) for field in ms.fields}


def get_receiver_type_for_spws(ms, spwids: Sequence) -> Dict:
    """Return dictionary of receiver types for requested spectral window IDs.

    If spwid is not found in MeasurementSet instance, then detector type is
    set to "N/A".

    Args:
        ms: MeasurementSet to query for receiver types.
        spwids: list of spw ids (integers) to query for.

    Returns:
        A dictionary assigning receiver types as values to spwid keys.
    """
    rxmap = {}
    for spwid in spwids:
        spw = ms.get_spectral_windows(spwid, science_windows_only=False)
        if not spw:
            rxmap[spwid] = "N/A"
        else:
            rxmap[spwid] = spw[0].receiver
    return rxmap


def get_spectralspec_to_spwid_map(spws: Collection) -> Dict:
    """
    Returns a dictionary of spectral specs mapped to corresponding spectral
    window IDs for requested list of spectral window objects.

    :param spws: list of spectral window objects
    :return: dictionary with spectral spec as keys, and corresponding
    list of spectral window IDs as values.
    """
    spwmap = collections.defaultdict(list)
    for spw in sorted(spws, key=lambda s: s.id):
        spwmap[spw.spectralspec].append(spw.id)
    return spwmap


def imstat_items(image, items=['min', 'max'], mask=None):
    """Extract desired stats properties (per Stokes) using ia.statistics().

    Beside the standard output, some additional stats property keys are supported.
    Note: 'image' is expected to an instance of CASA ia tool.
    """

    imstats = image.statistics(robust=True, axes=[0, 1, 3], mask=mask)
    stats = collections.OrderedDict()

    for item in items:
        if item.lower() == 'madrms':
            stats['madrms'] = imstats['medabsdevmed']*1.4826  # see CAS-9631
        elif item.lower() == 'max/madrms':
            stats['max/madrms'] = imstats['max']/imstats['medabsdevmed']*1.4826  # see CAS-9631
        elif item.lower() == 'maxabs':
            stats['maxabs'] = np.maximum(np.abs(imstats['max']), np.abs(imstats['min']))
        elif 'pct<' in item:
            threshold = float(item.replace('pct<', ''))
            imstats_threshold = image.statistics(robust=True, axes=[0, 1, 3], includepix=[0, threshold], mask=mask)
            if len(imstats_threshold['npts']) == 0:
                # if no pixel is selected from the restricted pixel value range, the return of ia.statitics() would be empty.
                imstats_threshold['npts'] = np.zeros(4)
            stats[item] = imstats_threshold['npts']/imstats['npts']
        elif item.lower() == 'pct_masked':
            im_shape = (imstats['trc']-imstats['blc'])+1
            stats[item] = 1.-imstats['npts']/im_shape[0]/im_shape[1]
        elif item.lower() == 'peak':  # Here 'peak' means the pixel value with largest deviation from zero.
            stats[item] = np.where(np.abs(imstats['max']) > np.abs(imstats['min']), imstats['max'], imstats['min'])
        elif item.lower() == 'peak/madrms':
            peak = np.where(np.abs(imstats['max']) > np.abs(imstats['min']), imstats['max'], imstats['min'])
            madrms = imstats['medabsdevmed']*1.4826  # see CAS-9631
            stats['peak/madrms'] = peak/madrms
        else:
            stats[item] = imstats[item.lower()]

    return stats


def get_stokes(imagename):
    """Get the labels of all stokes planes present in a CASA image."""

    with casa_tools.ImageReader(imagename) as image:
        cs = image.coordsys()
        stokes_labels = cs.stokes()
        stokes_present = [stokes_labels[idx] for idx in range(image.shape()[2])]
        cs.done()

    return stokes_present


def get_casa_quantity(value: Union[None, Dict, str, float, int]) -> Dict:
    """Wrapper around quanta.quantity() that handles None input.

    Starting with CASA 6, quanta.quantity() no longer accepts None as input. This
    utility function handles None values when calling CASA quanta.quantity() tool
    method.

    Returns:
        A CASA quanta.quantity (dictionary)

    Examples:
    >>> get_casa_quantity(None)
    {'unit': '', 'value': 0.0}
    >>> get_casa_quantity('10klambda')
    {'unit': 'klambda', 'value': 10.0}
    """
    if value is not None:
        return casa_tools.quanta.quantity(value)
    else:
        return casa_tools.quanta.quantity(0.0)


def get_si_prefix(value: float, select: str = 'mu', lztol: int = 0) -> tuple:
    """Obtain the best SI unit prefix option for a numeric value.

    A "best" SI prefix from a specified prefix collection is defined by minimizing :
        * leading zeros (possibly to a specified tolerance limit, see `lztol`)
        * significant digits before the decimal point
    , after the prefix is applied.

    Args:
        value (float): the numerical value for picking the prefix.
        select (str, optional): SI prefix candidates, a substring of "yzafpnum kMGTPEZY").
            Defaults to 'mu'.
        lztol (int, optional): leading zeros tolerance.
            Defaults to 0 (avoid any leading zeros when possible).

    Returns:
        tuple: (prefix_string, prefix_scale)

    Examples:

    e.g. for frequency value in Hz
    >>> get_si_prefix(10**7,select='kMGT')
    ('M', 1000000.0)

    e.g. for flux value in Jy
    >>> get_si_prefix(1.0,select='um')
    ('', 1.0)
    >>> get_si_prefix(0.0,select='um')
    ('', 1.0)
    >>> get_si_prefix(-0.9,select='um')
    ('m', 0.001)
    >>> get_si_prefix(0.9,select='um',lztol=1)
    ('', 1.0)
    >>> get_si_prefix(1e-7,select='um')
    ('u', 1e-06)
    >>> get_si_prefix(1e3,select='um')
    ('', 1.0)

    """
    if value == 0:
        return '', 1.0
    else:
        sp_tab = "yzafpnum kMGTPEZY"
        sp_list, sp_pow = zip(*[(p, (idx-8)*3.0)
                                for idx, p in enumerate(sp_tab) if p in select+' '])
        idx = bisect.bisect(sp_pow, np.log10(abs(value))+lztol)
        idx = max(idx-1, 0)

        return sp_list[idx].strip(), 10.**sp_pow[idx]


def absolute_path(name: str) -> str:
    """Return an absolute path of a given file."""
    return os.path.abspath(os.path.expanduser(os.path.expandvars(name)))


def relative_path(name: str, start: Optional[str]=None) -> str:
    """
    Retun a relative path of a given file with respect a given origin.

    Args:
        name: A path to file.
        start: An origin of relative path. If the start is not given, the
            current directory is used as the origin of relative path.

    Examples:
    >>> relative_path('/root/a/b.txt', '/root/c')
    '../a/b.txt'
    >>> relative_path('../a/b.txt', './c')
    '../../a/b.txt'
    """
    if start is not None:
        start = absolute_path(start)
    return os.path.relpath(absolute_path(name), start)


def get_task_result_count(context, taskname: str = 'hif_makeimages') -> int:
    """Count occurrences of a task result in the context.results list.

    Loop over the content of the context.results list and compare taskname to the pipeline_casa_task
    attribute of each result object. Increase counter if taskname substring is found in the attribute.

    The order number is determined by counting the number of previous execution of
    the task, based on the content of the context.results list. The introduction
    of this method is necessary because VLASS-SE-CONT imaging happens in multiple
    stages (hif_makeimages calls). Imaging parameters change from stage to stage,
    therefore it is necessary to know what is the current stage ordinal number.
    """
    count = 0
    for r in context.results:
        # Work around the fact that r has read() method in some cases (e.g. editimlist)
        # but not in others (e.g. in tclean renderer)
        try:
            if taskname in r.read().pipeline_casa_task:
                count += 1
        except AttributeError:
            if taskname in r.pipeline_casa_task:
                count += 1
    return count


def place_repr_source_first(itemlist: Union[List[str], List[Tuple]], repr_source: str) -> Union[List[str], List[Tuple]]:
    """
    Place representative source first in a list of source names
    or tuples with source name as first tuple element.
    """
    try:
        itemtype = type(itemlist[0])
        if itemtype is str:
            repr_source_index = [dequote(item) for item in itemlist].index(dequote(repr_source))
        elif itemtype is tuple or itemtype is list:
            repr_source_index = [dequote(item[0]) for item in itemlist].index(dequote(repr_source))
        else:
            raise Exception('Cannot handle items of type {}'.format(itemtype))
        repr_source_entry = itemlist.pop(repr_source_index)
        itemlist = [repr_source_entry] + itemlist
    except ValueError:
        LOG.warning('Could not reorder field list to place representative source first')

    return itemlist


def shutdown_plotms():
    """Shutdown the existing plotms process in the current CASA session.

    This utility function shuts down the persist plotms process in the current CASA session, so the next plotms call
    can start from a new proc. It's implemented as a short-term workaround for two plotms behaviors due to the persistent
    state of plotms once it's called in a CASA session.
        1. a plotms process always uses the initial working directory to construct the output plot path when the
           figure name is specified as a relative path (see CAS-13626), even after the working directory has changed
           in the Python perspective.
        2. a plotms process always uses the same casa logfile when it was started for its logging, even after
           the casa log file location has been altered.

    Note: This function follows the practice illustrated inside casaplotms.private.plotmstool.__stub_check()
    """
    plotmstool = casaplotms.plotmstool

    if plotmstool.__proc is not None:
        plotmstool.__proc.kill()
        _, _ = plotmstool.__proc.communicate()
        plotmstool.__proc = None
        plotmstool.__stub = None
        plotmstool.__uri = None


def get_casa_session_details():
    """Get the current CASA session details.

    return a dictionary including the following keys:
        casa_dir: the root directory of the monolithic CASA distribution.
        omp_num_threads: the number of OpenMP threads in the current parallel region.
        data_path: CASA data paths in-use.
        numa_mem: memory properties from the NUMA software perspective.
        numa_cpu: cpu properties from the NUMA software perspective.
            The above CPU/mem properties might be different from the hardware specs obtained from
            standard Python functions (e.g. os.cpu_count()) or pipeline.environment.
            On the difference between the "software" and hardware nodes, see
                https://www.kernel.org/doc/html/latest/vm/numa.html
    """
    casa_session_details = casa_tools.utils.hostinfo()
    casa_session_details['casa_dir'] = casa_tools.utils.getrc()
    casa_session_details['omp_num_threads'] = casa_tools.casalog.ompGetNumThreads()
    casa_session_details['data_path'] = casa_tools.utils.defaultpath()
    casa_session_details['numa_cpu'] = casa_session_details.pop('cpus')
    casa_session_details['numa_mem'] = casa_session_details.pop('memory')

    return casa_session_details


def get_taskhistory_fromimage(imagename: str):
    """Retrieve past CASA/tclean() call parameters from the image history.

    Note: the tclean history is only added to images/logtable in CASA ver>=6.2 (see CAS-13247)
    For tclean products generated by earlier CASA versions, an empty list will be returned.
    """
    taskhistory_list = []

    with casa_tools.ImageReader(imagename) as image:
        history_list = image.history(list=False)
        is_fromtask = False
        for line in history_list:
            if 'taskname' in line and '=' in line:
                is_fromtask = True
                k, v = line.partition('=')[::2]
                k = k.strip()
                v = v.strip()
                taskhistory_list.append(collections.OrderedDict([('taskname', v), ('taskversion', 'unkown')]))
                continue
            if 'version:' in line and 'CASAtools:' in line and is_fromtask:
                taskhistory_list[-1]['taskversion'] = line
                continue
            if '=' in line and is_fromtask:
                k, v = line.partition('=')[::2]
                k = k.strip()
                v = v.strip()
                taskhistory_list[-1][k] = ast.literal_eval(v)
            else:
                is_fromtask = False

    LOG.info(f'Found {len(taskhistory_list)} task history entry/entries from {imagename}')

    return taskhistory_list


def get_obj_size(obj, serialize=True):
    """Estimate the size of a Python object.

    If serialize=True, the size of a serialized object is returned. Note that this is NOT the
    same as the object size in memory.

    When serialize=False, the memory consumption of the object is returned via
    the asizeof method of Pympler.:
        pympler.asizeof.asizeof(obj) # https://pypi.org/project/Pympler
    An alternative is the get_deep_size() function from objsize.
        objsize.get_deep_size(obj)   # https://pypi.org/project/objsize
    """

    if serialize:
        return len(pickle.dumps(obj, protocol=-1))
    else:
        try:
            from pympler.asizeof import asizeof
            # PIPE-1698: a workaround for NumPy-related issues with the recent Pympler/asizeof versions
            # see https://github.com/pympler/pympler/issues/155
            _ = asizeof(np.str_())
        except ImportError as err:
            LOG.debug('Import error: {!s}'.format(err))
            raise Exception(
                "Pympler/asizeof is not installed, which is required to run get_obj_size(obj, serialize=False).")
        return asizeof(obj)


def glob_ordered(pattern: str, *args, order: Optional[str] = None, **kwargs) -> List[str]:
    """Return a sorted list of paths matching a pathname pattern."""

    path_list = glob.glob(pattern, *args, **kwargs)

    if order == 'mtime':
        path_list.sort(key=os.path.getmtime)
    elif order == 'ctime':
        path_list.sort(key=os.path.getctime)
    else:
        if order is not None:
            LOG.warning("Unknown sorting order requested: order=%r. Only 'mtime', 'ctime', or None is allowed.", order)
            LOG.warning("We will use the default alphabetically/numerically ascending order (order=None) instead.")
        path_list = sorted(path_list)

    return path_list


def deduplicate(items):
    """Remove duplicate entries from a list, but preserve the order.

    Note that the use of list(set(x)) can cause random order in the output.
    The return of this function is guaranteed to be in the order that unique items show up in the input, unlike
    a deduplicate-resorting solution like sorted(set(x).
    Ref: https://stackoverflow.com/questions/480214/how-do-i-remove-duplicates-from-a-list-while-preserving-order
    This solution only works for Python 3.7+.
    """
    deduplicated_items = list(dict.fromkeys(items))

    return deduplicated_items


@contextlib.contextmanager
def ignore_pointing(vis):
    """A context manager to ignore pointing tables of MSes during I/O operations.

    The original pointing table will be temperarily renamed to POINTING_ORIGIN, and a new empty pointing table
    is created. When the context manager exits, the original table is restored.

    For example, to ignore the pointing table of a MS during mstransform() calls, use:

        with ignore_pointing('test.ms'):
            casatasks.mstransform(vis='test.ms',outputvis='test_output.ms',scan='16',datacolumn='data')

    The pointing table of the output MS should be empty.

    On the other hand, if the pointing table is needed in the output vis, e.g. for imaging with tclean(usepointing=True),
    we can manually create hardlinks of pointing table afterwards while minimizing the disk space usage:

        import shutil, os
        shutil.rmtree('test_small.ms/POINTING')
        shutil.copytree('test.ms/POINTING', 'test_output.ms/POINTING', copy_function=os.link)

    One can verify the inodes of the pointing table files, which should be the same:

        ls -lih test.ms/POINTING
        ls -lih test_small.ms/POINTING

    """
    if isinstance(vis, (list, set)):
        vis_list = vis
    else:
        vis_list = [vis]

    vis_list_ignore = []
    try:
        for ms in vis_list:
            if not os.path.isdir(ms+'/POINTING') and not os.path.isdir(ms+'/POINTING_ORIGIN'):
                LOG.warning(f'No pointing table found in {ms}.')
                continue
            vis_list_ignore.append(ms)
            if not os.path.isdir(ms+'/POINTING_ORIGIN'):
                LOG.info(f'backup the pointing table for {ms}')
                shutil.move(ms+'/POINTING', ms+'/POINTING_ORIGIN')
            with casa_tools.TableReader(ms+'/POINTING_ORIGIN', nomodify=True) as table:
                tabdesc = table.getdesc()
                dminfo = table.getdminfo()
            if os.path.isdir(ms+'/POINTING'):
                shutil.rmtree(ms+'/POINTING')
            LOG.info(f'empty the pointing table for {ms}')
            tb = casa_tools.table
            tb.create(ms+'/POINTING', tabdesc, dminfo=dminfo)
            tb.close()
        yield
    finally:
        for ms in vis_list_ignore:
            if os.path.isdir(ms+'/POINTING_ORIGIN'):
                if os.path.isdir(ms+'/POINTING'):
                    shutil.rmtree(ms+'/POINTING')
                LOG.info(f'restore the pointing table for {ms}')
                shutil.move(ms+'/POINTING_ORIGIN', ms+'/POINTING')


@contextlib.contextmanager
def request_omp_threading(num_threads=None):
    """A context manager to override the session-wise OMP threading setting on CASA MPI client.

    This function is intended to improve certain CASAtask/tool call performance on the MPI client by
    temporarily turning on OpenMP threading while the MPI servers are idle. This feature will only
    take effect under restricted circumstances to avoid competing with the MPI server processes from
    the tier0 or tier1 parallelization.

    This function can be used as both a decorator and context manager. For example:

        @request_omp_threading(4)
        def do_something():
            ...
        or,
        with request_omp_threading(4):
            immoments(..)

    Note: please use it with caution and examine the computing resource allocation circumstance
    carefully at the execution point.
    """

    session_num_threads = casa_tools.casalog.ompGetNumThreads()
    LOG.debug('session_num_threads = {!s}'.format(session_num_threads))
    is_mpi_ready = mpihelpers.is_mpi_ready()  # return True if MPI is ready and we are on the MPI client.

    num_threads_limits = []

    # this is generally inherited from cgroup, but might be sub-optimal (too large) for high core-count
    # workstations when cgroup limit is not applied.
    casa_num_cpus = casa_tools.casalog.getNumCPUs()
    LOG.info('casalog.getNumCPUs() = {!s}'.format(casa_num_cpus))
    num_threads_limits.append(casa_num_cpus)

    # check against MPI.UNIVERSE_SIZE, which is another way to limit the number of threads.
    # see https://www.mpi-forum.org/docs/mpi-4.0/mpi40-report.pdf (Sec. 11.10.1, Universe Size)
    try:
        from mpi4py import MPI
        if MPI.UNIVERSE_SIZE != MPI.KEYVAL_INVALID:
            universe_size = MPI.COMM_WORLD.Get_attr(MPI.UNIVERSE_SIZE)
            LOG.info('MPI.UNIVERSE_SIZE = {!s}'.format(universe_size))
            if isinstance(universe_size, int) and universe_size > 1:
                num_threads_limits.append(universe_size)
            world_size = MPI.COMM_WORLD.Get_size()
            LOG.info('MPI.COMM_WORLD.Get_size() = {!s}'.format(world_size))
            if isinstance(world_size, int) and world_size > 1:
                num_threads_limits.append(world_size)
    except ImportError as ex:
        pass

    max_num_threads = min(num_threads_limits)

    context_num_threads = None
    if is_mpi_ready and session_num_threads == 1 and max_num_threads > 1:
        if num_threads is not None:
            if 0 < num_threads <= max_num_threads:
                max_num_threads = num_threads
            else:
                LOG.warning(
                    f'The requested num_threads ({num_threads}) is larger than the optimal number of logical CPUs ({max_num_threads}) assigned for this CASA session. ')

        context_num_threads = max_num_threads
    try:
        if context_num_threads is not None:
            casa_tools.casalog.ompSetNumThreads(context_num_threads)
            LOG.info('adjust openmp threads to {}'.format(context_num_threads))
        yield
    finally:
        if context_num_threads is not None:
            casa_tools.casalog.ompSetNumThreads(session_num_threads)
            LOG.info('restore openmp threads to {}'.format(session_num_threads))


@contextlib.contextmanager
def open_with_lock(filename, mode='r'):
    """Open file with a lock.

    The file open context manager function will try to lock the file upon opening 
    so other processes using the same "opener" will wait for the file to unlock. 
    The applied lock will be released when leaving the context.

    Notes: 
    
    all file open calls with `open_with_lock` will block other `open_with_lock` usages 
    (even from different processes/lustre-clients) from read/write until the lock is released.
    However, this locking/blocking mechanism will not prevent the file access (potentially 
    causing IO error) from other file openers (e.g. the default open() function).

    Because the Python/fcntl API depends on the underlying OS/storage implementation: 
        https://docs.python.org/3/library/fcntl.html
    Not all OS/fsys is fully supported. For Lustre, it seems to depend on fsys mount `-o flock` 
    options, which is indeed used for cv/naasc/aoc lustre systems:
        $ mount -l | grep lustre
            192.168.1.30@o2ib:/aoclst03 on /.lustre/aoc type lustre (rw,flock,user_xattr,lazystatfs)
    For NFS, please see additional notes in PIPE-2051.

    For a practical test, try the code snippet below from different clients/processes to see 
    if the fnctl blocking mechanism can prevent non-exclusive access to a file.
        import fcntl
        fd=open(filename,'w')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    Args:
        filename (str): the file name
        mode (str, optional): open mode. Defaults to 'r'.

    """

    with open(filename, mode) as fd:

        LOG.debug('acquiring lock on %s', filename)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            LOG.debug('acquired lock on %s', filename)
        except OSError as e:
            LOG.warning('failed to acquire file lock due to the filesystem limitation,'
                        'which might cause racing conditions if multiple processes access %s simultaneously.',
                        filename)

        yield fd
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
            LOG.debug('released lock on %s', filename)
        except OSError as e:
            LOG.warning('failed to acquire file lock due to the filesystem limitation,'
                        'which might cause racing conditions if multiple processes access %s simultaneously.',
                        filename)

def ensure_products_dir_exists(products_dir):
    try:
        LOG.trace(f"Creating products directory: {products_dir}")
        os.makedirs(products_dir)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def export_weblog_as_tar(context, products_dir, name_builder):
    # Construct filename prefix from oussid and recipe name if available.
    prefix = context.get_oussid()
    recipe_name = context.get_recipe_name()
    if recipe_name:
        prefix = prefix + '.' + recipe_name

    # Construct filename for weblog output tar archive.
    tarfilename = name_builder.weblog(project_structure=context.project_structure,
                                      ousstatus_entity_id=prefix)

    # Save weblog directory to tar archive.
    LOG.info(f"Saving weblog in {tarfilename}")
    tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")
    tar.add(os.path.join(os.path.basename(os.path.dirname(context.report_dir)), 'html'))
    tar.close()
    return tarfilename


def get_products_dir(context):
    if context.products_dir is None:
        return os.path.abspath('./')
    else:
        return context.products_dir


class pl_defaultdict(collections.defaultdict):
    def __repr__(self):
        return str(dict(self))

    def as_plain_dict(self):
        return to_plain_dict(self)


def nested_dict():
    return pl_defaultdict(nested_dict)


def to_plain_dict(default_dict):
    plain_dict = dict()
    for k, v in default_dict.items():
        if isinstance(v, collections.defaultdict):
            plain_dict[k] = to_plain_dict(v)
        else:
            plain_dict[k] = v

    return plain_dict


def string_to_val(s):
    """
    Convert a string to a Python data type.
    """
    try:
        pyobj = ast.literal_eval(s)
        # PIPE-1030: prevent a string like "1,2,3" from being unexpectedly translated into tuple
        if type(pyobj) is tuple and s.strip()[0] != '(':
            pyobj = s
        return pyobj
    except ValueError:
        return s
    except SyntaxError:
        return s


def remove_trailing_string(s, t):
    """
    Remove a trailing string if it exists.
    """
    if s.endswith(t):
        return s[:-len(t)]
    else:
        return s
