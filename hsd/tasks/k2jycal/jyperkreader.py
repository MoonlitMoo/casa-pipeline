"""Module to read Jy/K factor file."""
import contextlib
import csv
import io

from typing import TYPE_CHECKING, Generator, List, Optional, TextIO, Tuple, Union

import numpy

import pipeline.infrastructure as infrastructure
from pipeline.domain import DataType

if TYPE_CHECKING:
    from numbers import Number

    from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


def read(context: 'Context', filename: str) -> List[List[str]]:
    """Read jyperk factors from a file.

    Args:
        context: Pipeline context
        filename: Name of the file to read

    Returns:
        String list of [['MS','ant','spwid','polid','factor'], ...]
    """
    filetype = inspect_type(filename)
    if filetype == 'MS-Based':
        LOG.debug('MS-Based Jy/K factors file is specified')
        return read_ms_based(filename)
    else:
        LOG.debug('Session-Based Jy/K factors file is specified')
        return read_session_based(context, filename)


def inspect_type(filename: str) -> str:
    """Inspect file type.

    Read the first line and check its contents.

    Args:
        filename: Name of the file to read

    Returns:
        'Session-Based' or 'MS-Based'
    """
    with open(filename, 'r') as f:
        line = f.readline()
    if len(line) > 0 and line[0] == '#':
        return 'Session-Based'
    else:
        return 'MS-Based'


def read_ms_based(reffile: str) -> List[List[str]]:
    """Read "MS-Based" jyperk factor file.

    Args:
        reffile: Name of the file to read

    Returns:
        String list of [['MS','ant','spwid','polid','factor'], ...]
    """
    #factor_list = []
    with open(reffile, 'r') as f:
        return list(_read_stream(f))


def read_session_based(context: 'Context', reffile: str) -> List[List[str]]:
    """Read "Session-Based" jyperk factor file.

    Args:
        context: Pipeline context
        reffile: Name of the file to read

    Returns:
        String list of [['MS','ant','spwid','polid','factor'], ...]
    """
    parser = JyPerKDataParser
    storage = JyPerK()
    with open(reffile, 'r') as f:
        for line in f:
            if line[0] == '#':
                # header
                meta = parser.parse_header(line)
                storage.register_meta(meta)
            else:
                # data
                data = parser.parse_data(line)
                storage.register_data(data)

    with associate(context, storage) as f:
        return list(_read_stream(f))


def _read_stream(stream: TextIO) -> Generator[List[str], None, None]:
    """Read CSV data.

    Args:
        stream (): I/O stream returned by open()

    Yields:
        Single line (CSV entry) of the file as a list of strings
    """
    reader = csv.reader(stream)
    # Check if first line is header or not
    line = next(reader)
    if len(line) == 0 or line[0].strip().upper() == 'MS' or line[0].strip()[0] == '#':
        # must be a header, commented line, or empty line
        pass
    elif len(line) == 5:
        # may be a data record
        #factor_list.append(line)
        yield line
    else:
        LOG.error('Jy/K factor file is invalid format')
    for line in reader:
        if len(line) == 0 or len(line[0]) == 0 or line[0][0] == '#':
            continue
        elif len(line) == 5:
            yield line
        else:
            LOG.error('Jy/K factor file is invalid format')


# Utility classes/functions to convert session based factors file
# to MS based one are defined below
class JyPerKDataParser(object):
    """Utility class to convert session based factors file into MS based one."""

    @classmethod
    def get_content(cls, line: str) -> str:
        """Sanitize the string.

        Strip some special characters from the string.

        Args:
            line: String to be sanitized

        Returns:
            Sanitized string
        """
        return line.strip('# \n\t')

    @classmethod
    def parse_header(cls, line: str) -> Optional[Union[Tuple[str, str], List[str]]]:
        """Parse single header string.

        Header string may contain,

            - meta data in "key=value" format
            - data header as a comma separated list of column names

        Args:
            line: Single line of header

        Returns:
            Contents of the header string. If given string is not a header,
            None is returned.
        """
        content = cls.get_content(line)
        if content.find('=') != -1:
            # this must be a meta data
            return tuple(content.split('='))
        elif content.find(',') != -1 and not content[0].isdigit():
            # this must be a data header
            return content.split(',')
        else:
            # empty line or commented data, ignored
            return None

    @classmethod
    def parse_data(cls, line: str) -> Optional[List[str]]:
        """Parse comma-separated data string.

        Args:
            line: Comma-separated data string

        Returns:
            List of data or None
        """
        content = cls.get_content(line)
        if content.find(',') != -1:
            # data
            return content.split(',')
        else:
            # invalid or empty line, ignored
            return None


class JyPerK(object):
    """Parse session based jyperk csv and store.

    * meta stores meta data information from the lines in the form, '#name=value',
        as a dictionary, meta[name]=value.
    * header stores column label from the line in the form '#header0, header1, ...'
        as a list, header = ['header0', 'header1', ...]
    * data stores values in csv file as a dictionary,
        data['header0'] = [data00, data01, ...]
    """

    def __init__(self) -> None:
        """Initialize JyPerK class instance."""
        self.meta = dict()
        self.header = []
        self.data = []

    def register_meta(self, content: Union[List[str], Tuple[str, str]]) -> None:
        """Register meta data for the contents.

        Args:
            content: Contents of the file as a list or a tuple of string.
                     Should be either tuple of (key, value) format, or
                     a list of column names.
        """
        if isinstance(content, list):
            # this should be data header
            self.header = content
            self.data = dict(((k, []) for k in self.header))
        elif isinstance(content, tuple):
            self.meta[content[0]] = content[1]

    def register_data(self, content: List[str]) -> None:
        """Register data.

        Args:
            content: Contents of the file as a list of string.
                     Should be list of data as strings.
        """
        assert len(self.header) > 0
        assert len(self.header) == len(content)
        for (k, v) in zip(self.header, content):
            self.data[k].append(v)


@contextlib.contextmanager
def associate(context: 'Context', factors: JyPerK) -> Generator[TextIO, None, None]:
    """Provide an interface to access "Session-Based" data like "MS-Based" one.

    Convert data collected from session based jyperk csv as JyPerK object
    to MS-beased csv, i.e., a string list of ['MS,ant,spwid,polid,factor', ...].

    This is intended to be used in combination with "with" statement.

    Args:
        context: Pipeline context
        factors: JyPerK instance

    Yields:
        StringIO instance that caller of this function can handle the return value
        as if it accessed to the file.
    """
    stream = io.StringIO()
    try:
        data = factors.data
        for ms in context.observing_run.get_measurement_sets_of_type(DataType.RAW):
            session_name = ms.session
            if session_name == 'Session_default':
                # Session_default is not supported, use Session_1 instead
                LOG.warning('Session for %s is \'Session_default\'. Use \'Session_1\' for application of Jy/K factor. ' %
                            ms.basename)
                session_id = 1
            else:
                # session_name should be 'Session_X' where X is an integer
                session_id = int(session_name.split('_')[-1])
            session_list = numpy.array([int(x) for x in data['sessionID']])
            idx = numpy.where(session_list == session_id)

            antennas = [x.name for x in ms.antennas]
            antenna_list = data['Antenna']

            factor_list = numpy.array([float(x) for x in data['Factor']])

            spws = ms.get_spectral_windows()
            bandcenter = numpy.array([float(x) * 1.0e6 for x in data['BandCenter(MHz)']])
            bandwidth = numpy.array([float(x) * 1.0e6 for x in data['BandWidth(MHz)']])
            range_min = bandcenter - 0.5 * bandwidth
            range_max = bandcenter + 0.5 * bandwidth
            for spw in spws:
                max_freq = float(spw.max_frequency.value)
                min_freq = float(spw.min_frequency.value)
                tot_bandwidth = float(spw.bandwidth.value)

                spwid = spw.id
                d = {}
                for i in idx[0]:
                    #coverage = inspect_coverage(min_freq, max_freq, range_min[i], range_max[i])
                    antenna = antenna_list[i]
                    if antenna in d:
                        #d[antenna].append([i, coverage, bandwidth[i]])
                        d[antenna].append(i)
                    else:
                        #d[antenna] = [[i, coverage, bandwidth[i]]]
                        d[antenna] = [i]

                for ant in antennas:
                    if ant in d:
                        f = d[ant]
                    else:
                        LOG.info('%s: No factors available for spw %s antenna %s use ANONYMOUS' %
                                 (session_name, spwid, ant))
                        f = d['ANONYMOUS']
                    coverage_list = [inspect_coverage(min_freq, max_freq, range_min[x], range_max[x]) for x in f]
                    #_best_index = numpy.argmax(coverage_list)
                    best_index = f[0]
                    _best_score = inspect_coverage(min_freq, max_freq, range_min[f[0]], range_max[f[0]])
                    for _i in f[1:]:
                        coverage = inspect_coverage(min_freq, max_freq, range_min[_i], range_max[_i])
                        if coverage > _best_score:
                            best_index = _i
                            _best_score = coverage
                    line = '%s,%s,%s,%s,%s' % (ms.basename, ant, spwid, data['POL'][best_index],
                                               factor_list[best_index])
                    LOG.debug(line)
                    stream.write(line + '\n')

        stream.seek(0, 0)
        yield stream

    finally:
        stream.close()


def inspect_coverage(minval: 'Number', maxval: 'Number', minref: 'Number', maxref: 'Number') -> 'Number':
    """Inspect overlapped region of given two ranges.

    Compute a fraction of the overlapped region of given two ranges
    specified by (minval, maxval) and (minref, maxref). If reference
    range (minref, maxref) is too broad (> 110%) compared with
    (minval, maxval), returned value will be 0 regardless of actual
    fraction.

    Args:
        minval: minimum value of the range
        maxval: maximum value of the range
        minref: minimum value of the reference range
        maxref: maximum value of the reference range

    Returns:
        Fraction of the overlapped region. Ranges from 0 to 1.
    """
    if minval > maxval or minref > maxref:
        return 0.0

    coverage = (min(maxval, maxref) - max(minval, minref)) / (maxval - minval)

    bandwidth_ratio = (maxref - minref) / (maxval - minval)

    if coverage < 0.0 or coverage > 1.0 or bandwidth_ratio > 1.1:
        return 0.0

    return coverage
