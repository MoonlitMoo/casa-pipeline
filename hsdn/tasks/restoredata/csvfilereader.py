import numpy
import csv
import contextlib
import itertools
import os
import pipeline.infrastructure as infrastructure

LOG = infrastructure.get_logger(__name__)

def read(context, filename):
    """
    Original is pipeline.hsd.tasks.k2jycal.jyperkreader.py. 
    This csvfilereader.py is simplified specially for NRO data 
    to read reference file (reffile=nroscalefile.csv). 
    Reads factors from a file and returns a string list
    of [['MS','ant','spwid','polid','factor'], ...]
    """
    return read_ms_based(filename)

def read_ms_based(reffile):
    with open(reffile, 'r') as f:
        return list(_read_stream(f))

def _read_stream(stream):
    reader = csv.reader(stream)
    # Check if first line is header or not
    filename = os.path.basename(stream.name)
    line = next(reader)
    LOG.debug('first line: {0}'.format(line))
    if len(line) == 0 or line[0].strip().upper() == 'MS' or '#' in line[0]:
        # must be a header, commented line, or empty line
        pass
    elif len(line) == 5:
        # may be a data record
        yield line
    else:
        pass
        #LOG.warning('The line {0} is invalid format'.format(line))
    for line in reader:
        LOG.debug('{0}'.format(line))
        if len(line) == 0 or len(line[0]) == 0 or line[0][0] == '#':
            continue
        elif len(line) == 5:
            yield line
        else:
            pass
            #LOG.warning('The line {0} is invalid format'.format(line))
