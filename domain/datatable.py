#
# ALMA - Atacama Large Milliiter Array (c) European Southern Observatory, 2002
# Copyright by ESO (in the framework of the ALMA collaboration), All rights
# reserved
#
# This library is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 2.1 of the License, or (at your option)
# any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

#
# $Revision: 1.1.2.6 $
# $Date: 2013/03/01 05:07:45 $
# $Author: tnakazat $
#
import bisect
import collections
import os
import re
import time
from typing import Tuple

# import memory_profiler
import numpy

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import absolute_path

LOG = infrastructure.get_logger(__name__)


def __coldesc(vtype, option, maxlen, ndim, comment, unit=None):
    d = {'dataManagerGroup': 'StandardStMan',
         'dataManagerType': 'StandardStMan',
         'valueType': vtype,
         'option': option,
         'maxlen': maxlen,
         'comment': comment}
    if ndim > 0:
        d['ndim'] = ndim
    if unit is not None:
        d['keywords'] = {'UNIT': unit}
    return d


# Description for data table columns as dictionary.
# Each value is a tuple containing:
#
#    (valueType,option,maxlen,ndim,comment[,unit])
#
# dataManagerGroup and dataManagerType is always 'StandardStMan'.
#
# 2018/07/31 HE : added SHIFT_RA, SHIFT_DEC for CAS-11674
# 2019/05/23 HE : added OFS_RA, OFS_DEC for PIPE-220
def __tabledescro():
    TD_DESC_RO = [
        #    __coldesc('integer', 0, 0, -1, 'Primary key'),
        __coldesc('integer', 0, 0, -1, 'Row number'),
        __coldesc('integer', 0, 0, -1, 'Scan number'),
        __coldesc('integer', 0, 0, -1, 'IF number'),
        __coldesc('integer', 0, 0, -1, 'Number of Polarizations'),
        __coldesc('integer', 0, 0, -1, 'Beam number'),
        __coldesc('string', 0, 0, -1, 'Date'),
        __coldesc('double', 0, 0, -1, 'Time in MJD', 'd'),
        __coldesc('double', 0, 0, -1, 'Elapsed time since first scan' 'd'),
        __coldesc('double', 0, 0, -1, 'Exposure time', 's'),
        __coldesc('double', 0, 0, -1, 'Right Ascension', 'deg'),
        __coldesc('double', 0, 0, -1, 'Declination', 'deg'),
        __coldesc('double', 0, 0, -1, 'Shifted Right Ascension', 'deg'),
        __coldesc('double', 0, 0, -1, 'Shifted Declination', 'deg'),
        __coldesc('double', 0, 0, -1, 'Offset Right Ascension', 'deg'),
        __coldesc('double', 0, 0, -1, 'Offset Declination', 'deg'),
        __coldesc('double', 0, 0, -1, 'Azimuth', 'deg'),
        __coldesc('double', 0, 0, -1, 'Elevation', 'deg'),
        __coldesc('integer', 0, 0, -1, 'Number of channels'),
        __coldesc('double', 0, 0, 1, 'Tsys', 'K'),
        __coldesc('string', 0, 0, -1, 'Target name'),
        __coldesc('integer', 0, 0, -1, 'Antenna index'),
        __coldesc('integer', 0, 0, -1, 'Source type enum'),
        __coldesc('integer', 0, 0, -1, 'Field ID')
    ]

    name = [
        'ROW', 'SCAN', 'IF', 'NPOL', 'BEAM', 'DATE',
        'TIME', 'ELAPSED', 'EXPOSURE', 'RA', 'DEC',
        'SHIFT_RA', 'SHIFT_DEC', 'OFS_RA', 'OFS_DEC',
        'AZ', 'EL', 'NCHAN', 'TSYS', 'TARGET', 'ANTENNA',
        'SRCTYPE', 'FIELD_ID'
    ]
    return dict(zip(name, TD_DESC_RO))


def __tabledescrw():
    TD_DESC_RW = [
        __coldesc('double', 0, 0, 2, 'Statistics'),
        __coldesc('integer', 0, 0, 2, 'Flgas'),
        __coldesc('integer', 0, 0, 2, 'Permanent flags'),
        __coldesc('integer', 0, 0, 1, 'Actual flag'),
        __coldesc('integer', 0, 0, -1, 'Number of mask regions'),
        __coldesc('integer', 0, 0, 2, 'List of mask ranges'),
        __coldesc('integer', 0, 0, -1, 'Unchanged row or not'),
        __coldesc('integer', 0, 0, -1, 'Position group id')
    ]

    name = [
        'STATISTICS', 'FLAG', 'FLAG_PERMANENT',
        'FLAG_SUMMARY', 'NMASK', 'MASKLIST', 'NOCHANGE',
        'POSGRP']
    return dict(zip(name, TD_DESC_RW))


TABLEDESC_RO = __tabledescro()
TABLEDESC_RW = __tabledescrw()


def create_table(table, name, desc, memtype='plain', nrow=0):
    ret = table.create(name, desc, memtype=memtype, nrow=nrow)
    assert ret == True
    for _colname, _coldesc in desc.items():
        if 'keywords' in _coldesc:
            table.putcolkeywords(_colname, _coldesc['keywords'])


# FLAG_PERMANENT Layout
WeatherFlagIndex = 0
TsysFlagIndex = 1
UserFlagIndex = 2
OnlineFlagIndex = 3


def timetable_key(table_type, antenna, spw, polarization=None, ms=None, field_id=None):
    key = 'TIMETABLE_%s' % table_type
    if ms is not None:
        key = key + '_%s' % (ms.replace('.', '_'))
    if field_id is not None:
        key = key + '_FIELD%s' % field_id
    key = key + '_ANT%s_SPW%s' % (antenna, spw)
    if polarization is not None:
        key = key + '_POL%s' % polarization
    return key


class DataTableIndexer(object):
    """
    Map between serial indices and per-MS row indices.

    DataTableIndexer is responsible for mapping between classical
    (serial) row indices (unique row IDs throughout all origin MSes)
    and per-MS row indices.
    """
    @property
    def origin_mses(self):
        return self.__origin_mses

    @origin_mses.setter
    def origin_mses(self, value):
        """Set an attribute, origin_ms."""
        self.__origin_mses = value

    def __init__(self, context):
        """
        Initialize DataTable Indexer class.

        Args:
            context: Pipeline context
        """
        self.context = context
        self.origin_mses = [ms for ms in self.context.observing_run.measurement_sets
                            if ms.name == ms.origin_ms]
        self.nrow_per_ms = []
        for origin_ms in self.origin_mses:
            ro_table_name = os.path.join(context.observing_run.ms_datatable_name, origin_ms.basename, 'RO')
            with casa_tools.TableReader(ro_table_name) as tb:
                self.nrow_per_ms.append(tb.nrows())
        self.num_mses = len(self.nrow_per_ms)

    def serial2perms(self, i: int) -> Tuple[str, int]:
        """
        Return basename of origin MS and per-MS row id of a given serial index.

        Args:
            i: serial index

        Returns:
            The basename of origin MS and the row index of the datatable for
            that MS corresponding to a given serial index.
        """
        base = 0
        for j in range(self.num_mses):
            past_base = base
            base += self.nrow_per_ms[j]
            if i < base:
                return self.origin_mses[j].basename, i - past_base

        raise RuntimeError('Internal Consistency Error. ')

    def perms2serial(self, vis: str, i: int) -> int:
        """
        Return serial index.

        Args:
            vis: Name of an MS
            i: per-MS DataTable row index

        Returns:
            A specified index in serial mode
        """
        ms = self.context.observing_run.get_ms(vis)
        j = self.origin_mses.index(self.context.observing_run.get_ms(ms.origin_ms))
        assert j < self.num_mses
        assert i < self.nrow_per_ms[j]

        base = sum(self.nrow_per_ms[:j])
        return base + i

    def per_ms_index_list(self, ms, index_list):
        origin_ms = self.context.observing_run.get_ms(ms.origin_ms)
        j = self.origin_mses.index(origin_ms)
        base = sum(self.nrow_per_ms[:j])
        length = self.nrow_per_ms[j]
        perms_list = numpy.where(numpy.logical_and(index_list >= base,
                                                   index_list < base + length), index_list)
        return perms_list - base


class DataTableImpl(object):
    """
    DataTable is an object to hold meta data of scantable on memory.

    row layout: [Row, Scan, IF, Pol, Beam, Date, Time, ElapsedTime,
                   0,    1,  2,   3,    4,    5,    6,            7,
                 Exptime, RA, DEC, Az, El, nchan, Tsys, TargetName,
                       8,  9,  10, 11, 12,    13,   14,         15,
                 Statistics, Flags, PermanentFlags, SummaryFlag, Nmask, MaskList, NoChange, Ant]
                         16,    17,             18,          19,    20,       21,       22,  23
    Statistics: DataTable[ID][16] =
                [LowFreqRMS, NewRMS, OldRMS, NewRMSdiff, OldRMSdiff, ExpectedRMS, ExpectedRMS]
                          0,      1,      2,          3,          4,           5,           6
    Flags: DataTable[ID][17] =
                [LowFrRMSFlag, PostFitRMSFlag, PreFitRMSFlag, PostFitRMSdiff, PreFitRMSdiff, PostFitExpRMSFlag, PreFitExpRMSFlag]
                            0,              1,             2,              3,             4,                 5,                6
    PermanentFlags: DataTable[ID][18] =
                [WeatherFlag, TsysFlag, UserFlag]
                           0,        1,        2
    Note for Flags: 1 is valid, 0 is invalid
    """

    @classmethod
    def get_rotable_name(cls, datatable_name):
        return os.path.join(datatable_name, 'RO')

    @classmethod
    def get_rwtable_name(cls, datatable_name):
        return os.path.join(datatable_name, 'RW')

    REFKEY = 'DIRECTION_REF'

    def __init__(self, name=None, readonly=None):
        """
        name (optional) -- name of DataTable
        """
        # unique memory table name
        timestamp = ('%f' % (time.time())).replace('.', '')
        self.memtable1 = 'DataTableImplRO%s.MemoryTable' % timestamp
        self.memtable2 = 'DataTableImplRW%s.MemoryTable' % timestamp
        self.plaintable = ''
        self.cols = {}
        # New table class instances are required to avoid accidental closure of
        # the global table tool instance, casa_tools.table
        self.tb1 = casa_tools._logging_table_cls()
        self.tb2 = casa_tools._logging_table_cls()
        self.isopened = False
        if name is None or len(name) == 0:
            if readonly is None:
                readonly = False
            self._create(readonly=readonly)
        elif not os.path.exists(name):
            if readonly is None:
                readonly = False
            self._create(readonly=readonly)
            self.plaintable = absolute_path(name)
        else:
            if readonly is None:
                readonly = True
            self.importdata2(name=name, minimal=False, readonly=readonly)

    def __del__(self):
        # make sure that table is closed
        # LOG.debug('__del__ close CASA table...')
        self.cols.clear()
        self._close()

    def __len__(self):
        return self.nrow

    @property
    def nrow(self):
        if self.isopened:
            return self.tb1.nrows()
        else:
            return 0

    @property
    def name(self):
        return self.plaintable

    @property
    def position_group_id(self):
        key = 'POSGRP_REP'
        if self.haskeyword(key):
            return numpy.max(numpy.fromiter((int(x) for x in self.getkeyword(key)), dtype=numpy.int32)) + 1
        else:
            return 0

    @property
    def time_group_id_small(self):
        return self.__get_time_group_id(True)

    @property
    def time_group_id_large(self):
        return self.__get_time_group_id(False)

    @property
    def direction_ref(self):
        if self.REFKEY in self.tb1.keywordnames():
            return self.tb1.getkeyword(self.REFKEY)
        else:
            return None

    @direction_ref.setter
    def direction_ref(self, value):
        # value must be string
        if not isinstance(value, str):
            return

        # set value only if it is not yet registered to table
        if self.REFKEY not in self.tb1.keywordnames():
            self.tb1.putkeyword(self.REFKEY, value)

    def __get_time_group_id(self, small=True):
        if small:
            subkey = 'SMALL'
        else:
            subkey = 'LARGE'
        pattern = r'^TIMETABLE_%s_.*' % subkey
        if numpy.any(numpy.fromiter((re.match(pattern, x) is not None for x in self.keywordnames()), dtype=bool)):
            group_id = 0
            for key in self.tb2.keywordnames():
                if re.match(pattern, key) is not None:
                    max_id = numpy.max(
                        numpy.fromiter((int(x) for x in self.getkeyword(key)), dtype=numpy.int32)) + 1
                    group_id = max(group_id, max_id)
            return group_id
        else:
            return 0

    def haskeyword(self, name):
        return name in self.tb2.keywordnames()

    def addrows(self, nrow):
        self.tb1.addrows(nrow)
        self.tb2.addrows(nrow)

        # reset self.plaintable since memory table and corresponding
        # plain table have different nrows
        self.plaintable == ''

    def colnames(self):
        return list(self.cols.keys())

    def getcol(self, name, startrow=0, nrow=-1, rowincr=1):
        return self.cols[name].getcol(startrow, nrow, rowincr)

    def putcol(self, name, val, startrow=0, nrow=-1, rowincr=1):
        self.cols[name].putcol(val, startrow, nrow, rowincr)

    def getcell(self, name, idx):
        return self.cols[name].getcell(idx)

    def putcell(self, name, idx, val):
        """
        name -- column name
        idx -- row index
        val -- value to be put
        """
        self.cols[name].putcell(idx, val)

    def getcolslice(self, name, blc, trc, incr, startrow=0, nrow=-1, rowincr=1):
        return self.cols[name].getcolslice(blc, trc, incr, startrow, nrow, rowincr)

    def putcolslice(self, name, value, blc, trc, incr, startrow=0, nrow=-1, rowincr=1):
        self.cols[name].putcolslice(value, blc, trc, incr, startrow, nrow, rowincr)

    def getcellslice(self, name, rownr, blc, trc, incr):
        return self.cols[name].getcellslice(rownr, blc, trc, incr)

    def putcellslice(self, name, rownr, value, blc, trc, incr):
        self.cols[name].putcellslice(rownr, value, blc, trc, incr)

    def getcolkeyword(self, columnname, keyword):
        if columnname in TABLEDESC_RO:
            return self.tb1.getcolkeyword(columnname, keyword)
        else:
            return self.tb2.getcolkeyword(columnname, keyword)

    def putkeyword(self, name, val):
        """
        name -- keyword name
        val -- keyword value
        """
        if isinstance(val, str):
            _val = '"{}"'.format(val)
        else:
            _val = str(val)
        self.tb2.putkeyword(name, _val)

    def getkeyword(self, name):
        """
        name -- keyword name
        """
        _val = self.tb2.getkeyword(name)
        val = eval(_val)
        return val

    def keywordnames(self):
        """
        return table keyword names
        """
        return self.tb2.keywordnames()

    def importdata(self, name, minimal=True, readonly=True):
        """
        name -- name of DataTable to be imported
        """
        LOG.debug('Importing DataTable from %s...' % name)

        # copy input table to memory
        self._copyfrom(name, minimal)
        self.plaintable = absolute_path(name)
        self.__init_cols(readonly=readonly)

    def importdata2(self, name, minimal=True, readonly=True):
        """
        name -- name of DataTable to be imported
        """
        LOG.debug('Importing DataTable from %s...' % name)

        # copy input table to memory
        self._copyfrom2(name, minimal)
        self.plaintable = absolute_path(name)
        self.__init_cols(readonly=readonly)

    def sync(self, minimal=True):
        """
        Sync with DataTable on disk.
        """
        self.importdata(name=self.plaintable, minimal=minimal)

    def exportdata(self, name=None, minimal=True, overwrite=False):
        """
        name -- name of exported DataTable
        overwrite -- overwrite existing DataTable
        """
        if name is None or len(name) == 0:
            if len(self.plaintable) == 0:
                raise IOError('You have to specify name of export table')
            else:
                name = self.plaintable
                overwrite = True

        LOG.debug('Exporting DataTable to %s...' % name)
        # overwrite check
        abspath = absolute_path(name)
        if not os.path.exists(abspath):
            os.makedirs(abspath)
        elif overwrite:
            LOG.debug('Overwrite existing DataTable %s...' % name)
            # os.system( 'rm -rf %s/*'%(abspath) )
        else:
            raise IOError('The file %s exists.' % name)

        # save
        if not minimal or not os.path.exists(os.path.join(abspath, 'RO')):
            # LOG.trace('Exporting RO table')
            if os.path.exists(self.tb1.name()):
                # self.tb1 seems to be plain table, nothing to be done
                pass
            else:
                # tb1 is memory table
                tbloc = self.tb1.copy(os.path.join(abspath, 'RO'), deep=True,
                                      valuecopy=True, returnobject=True)
                tbloc.close()
        # LOG.trace('Exporting RW table')
        tbloc = self.tb2.copy(os.path.join(abspath, 'RW'), deep=True,
                              valuecopy=True, returnobject=True)
        tbloc.close()
        self.plaintable = abspath

    # FIXME: this unused method contains bugs to fix.
    def export_rwtable_exclusive(self, dirty_rows=None, cols=None):
        """
        Export "on-memory" RW table to the one on disk.

        To support parallel operation, the method will acquire a lock for RW table
        to ensure the operation in one process doesn't overwrite the changes made by
        other processes.

        dirty_rows -- list of row numbers that are updated. If None, everything
                      including unchanged rows will be flushed. Default is None.
        cols -- list of columns that are updated. If None, all rows will be flushed.
                default is None.
        """
        # RW table name
        rwtable = self.get_rwtable_name(self.plaintable)

        # list of ordinary columns
        ordinary_cols = {'STATISTICS', 'FLAG', 'FLAG_PERMANENT', 'FLAG_SUMMARY', 'NMASK', 'POSGRP'}
        intersects = ordinary_cols.intersection(cols)

        # columns with special care
        with_masklist = 'MASKLIST' in cols
        with_nochange = 'NOCHANGE' in cols

        # open table
        with casa_tools.TableReader(rwtable, nomodify=False, lockoptions={'option': 'user'}) as tb:
            # lock table
            tb.lock()
            LOG.info('Process {0} have acquired a lock for RW table'.format(os.getpid()))

            if dirty_rows is None:
                # process all rows
                # FIXME: built-in range (or list) does not support .min(), .max(); should this be a numpy range?
                dirty_rows = range(tb.nrows())

            try:
                nrow_chunk = 2000
                # compute number of chunks
                nrow = dirty_rows.max() - dirty_rows.min() + 1
                nchunk = nrow // nrow_chunk
                mod = nrow % nrow_chunk
                chunks = [nrow_chunk] * nchunk + [mod]
                # LOG.info('chunks={0} (nrow {1})'.format(chunks, nrow))
                # for each column
                for col in intersects:
                    start_row = dirty_rows.min()
                    for size_chunk in chunks:
                        # LOG.info('start_row {0}, size_chunk {1}'.format(start_row, size_chunk))
                        # read chunk
                        chunk_src = self.tb2.getcol(col, startrow=start_row, nrow=size_chunk)
                        chunk_dst = tb.getcol(col, startrow=start_row, nrow=size_chunk)

                        # update chunk
                        chunk_min = start_row
                        chunk_max = start_row + size_chunk - 1
                        target_rows = dirty_rows[numpy.logical_and(chunk_min <= dirty_rows,
                                                                   dirty_rows <= chunk_max)]
                        for row in target_rows:
                            chunk_index = row - start_row
                            # LOG.info('row {0}, chunk_index {1}'.format(row, chunk_index))
                            chunk_dst[..., chunk_index] = chunk_src[..., chunk_index]

                        # flush chunk
                        tb.putcol(col, chunk_dst, startrow=start_row, nrow=size_chunk)

                        # increment start_row
                        start_row += size_chunk

                # merge MASKLIST if necessary
                if with_masklist is True:
                    src = self.cols['MASKLIST']
                    dst = DataTableColumnMaskList(tb)
                    for index, row in enumerate(dirty_rows):
                        data = src.getcell(index)
                        dst.putcell(row, data)

                if with_nochange is True:
                    src = self.cols['NOCHANGE']
                    dst = DataTableColumnNoChange(tb)
                    for index, row in enumerate(dirty_rows):
                        data = src.getcell(index)
                        dst.putcell(row, data)

            finally:
                # the table lock must eventually be released
                LOG.info('Process {0} is going to release a lock for RW table'.format(os.getpid()))
                tb.unlock()

    def _create(self, readonly=False):
        self._close()
        create_table(self.tb1, self.memtable1, TABLEDESC_RO, 'memory', self.nrow)
        create_table(self.tb2, self.memtable2, TABLEDESC_RW, 'memory', self.nrow)
        self.isopened = True
        self.__init_cols(readonly=readonly)

    def __init_cols(self, readonly=True):
        self.cols.clear()
        if readonly:
            RO_COLUMN = RODataTableColumn
            RW_COLUMN = RWDataTableColumn
        else:
            RO_COLUMN = RWDataTableColumn
            RW_COLUMN = RWDataTableColumn
        type_map = {'integer': int,
                    'double': float,
                    'string': str}
        datatype = lambda desc: list if 'ndim' in desc and desc['ndim'] > 0 else type_map[desc['valueType']]
        for k, v in TABLEDESC_RO.items():
            self.cols[k] = RO_COLUMN(self.tb1, k, datatype(v))
        for k, v in TABLEDESC_RW.items():
            if k == 'MASKLIST':
                self.cols[k] = DataTableColumnMaskList(self.tb2)
            elif k == 'NOCHANGE':
                self.cols[k] = DataTableColumnNoChange(self.tb2)
            else:
                self.cols[k] = RW_COLUMN(self.tb2, k, datatype(v))

    def _close(self):
        if self.isopened:
            self.tb1.close()
            self.tb2.close()
            self.isopened = False

    def _copyfrom(self, name, minimal=True):
        self._close()
        abspath = absolute_path(name)
        if not minimal or abspath != self.plaintable:
            with casa_tools.TableReader(os.path.join(name, 'RO')) as tb:
                self.tb1 = tb.copy(self.memtable1, deep=True,
                                   valuecopy=True, memorytable=True,
                                   returnobject=True)
        with casa_tools.TableReader(os.path.join(name, 'RW')) as tb:
            self.tb2 = tb.copy(self.memtable2, deep=True,
                               valuecopy=True, memorytable=True,
                               returnobject=True)
        self.isopened = True

    def _copyfrom2(self, name, minimal=True):
        self._close()
        abspath = absolute_path(name)
        if not minimal or abspath != self.plaintable:
            with casa_tools.TableReader(os.path.join(name, 'RO')) as tb:
                self.tb1 = tb.copy(self.memtable1, deep=True,
                                   valuecopy=True, memorytable=True,
                                   returnobject=True)
        with casa_tools.TableReader(os.path.join(name, 'RW')) as tb:
            self.tb2 = tb.copy(self.memtable2, deep=True,
                               valuecopy=True, memorytable=True,
                               returnobject=True)
        self.isopened = True

    def get_posdict(self, ant, spw, pol):
        posgrp_list = self.getkeyword('POSGRP_LIST')
        try:
            mygrp = posgrp_list[str(ant)][str(spw)][str(pol)]
        except KeyError:
            raise KeyError('ant %s spw %s pol %s not in reduction group list' % (ant, spw, pol))
        except Exception as e:
            raise e

        posgrp_rep = self.getkeyword('POSGRP_REP')
        rows = self.getcol('ROW')
        posgrp = self.getcol('POSGRP')
        posdict = {}
        for k, v in posgrp_rep.items():
            if int(k) not in mygrp:
                continue
            key = rows[v]
            posdict[key] = [[], []]

        for idx in range(len(posgrp)):
            grp = posgrp[idx]
            if grp not in mygrp:
                continue
            row = rows[idx]
            rep = posgrp_rep[str(grp)]
            key = rows[rep]
            posdict[key][0].append(row)
            posdict[key][1].append(idx)
            if row != key:
                posdict[row] = [[-1, key], [rep]]

        return posdict

    def set_timetable(self, ant, spw, pol, mygrp, timegrp_s, timegrp_l, ms=None, field_id=None):
        # time table format
        # TimeTable: [TimeTableSmallGap, TimeTableLargeGap]
        # TimeTableXXXGap: [[[row0, row1, ...], [idx0, idx1, ...]], ...]
        LOG.info('set_timetable start')
        start_time = time.time()

        mygrp_s = mygrp['small']
        mygrp_l = mygrp['large']
        rows = self.getcol('ROW')

        timedic_s = construct_timegroup(rows, set(mygrp_s), timegrp_s)
        timedic_l = construct_timegroup(rows, set(mygrp_l), timegrp_l)
        timetable_s = [timedic_s[idx] for idx in mygrp_s]
        timetable_l = [timedic_l[idx] for idx in mygrp_l]
        timetable = [timetable_s, timetable_l]
        end_time = time.time()
        LOG.info('construct timetable: Elapsed time %s sec' % (end_time - start_time))

        LOG.debug('timetable=%s' % timetable)

        # put time table to table keyword
        start_time2 = time.time()
        key_small = timetable_key('SMALL', ant, spw, pol, ms, field_id)
        key_large = timetable_key('LARGE', ant, spw, pol, ms, field_id)
        keys = self.tb2.keywordnames()
        LOG.debug('add time table: keys for small gap \'%s\' large gap \'%s\'' % (key_small, key_large))
        dictify = lambda x: dict([(str(i), t) for (i, t) in enumerate(x)])
        if key_small not in keys or key_large not in keys:
            self.putkeyword(key_small, dictify(timetable[0]))
            self.putkeyword(key_large, dictify(timetable[1]))

        end_time = time.time()
        LOG.info('put timetable: Elapsed time %s sec' % (end_time - start_time2))
        LOG.info('set get_timetable end: Elapsed time %s sec' % (end_time - start_time))

    def get_timetable(self, ant, spw, pol, ms=None, field_id=None):
        LOG.trace('new get_timetable start')
        start_time = time.time()
        key_small = timetable_key('SMALL', ant, spw, pol, ms, field_id)
        key_large = timetable_key('LARGE', ant, spw, pol, ms, field_id)
        keys = self.tb2.keywordnames()
        LOG.debug('get time table: keys for small gap \'%s\' large gap \'%s\'' % (key_small, key_large))
        if key_small in keys and key_large in keys:
            ttdict_small = self.getkeyword(key_small)
            ttdict_large = self.getkeyword(key_large)
            timetable_small = [ttdict_small[str(i)] for i in range(len(ttdict_small))]
            timetable_large = [ttdict_large[str(i)] for i in range(len(ttdict_large))]
            timetable = [timetable_small, timetable_large]
        else:
            raise RuntimeError('time table for Antenna %s spw %s pol %s is not configured properly' % (ant, spw, pol))
        end_time = time.time()
        LOG.trace('new get_timetable end: Elapsed time %s sec' % (end_time - start_time))

        return timetable

    def get_timegap(self, ant, spw, pol, asrow=True, ms=None, field_id=None):
        timegap_s = self.getkeyword('TIMEGAP_S')
        timegap_l = self.getkeyword('TIMEGAP_L')
        if ms is None:
            try:
                mygap_s = timegap_s[ant][spw][pol]
                mygap_l = timegap_l[ant][spw][pol]
            except KeyError:
                raise KeyError('ant %s spw %s pol %s not in reduction group list' % (ant, spw, pol))
            except Exception as e:
                raise e
        else:
            try:
                mygap_s = timegap_s[ms.basename.replace('.', '_')][ant][spw][field_id]
                mygap_l = timegap_l[ms.basename.replace('.', '_')][ant][spw][field_id]
            except KeyError:
                raise KeyError(
                    'ms %s field %s ant %s spw %s not in reduction group list' % (ms.basename, field_id, ant, spw))
            except Exception as e:
                raise e

        if asrow:
            rows = self.getcol('ROW')
            timegap = [[], []]
            for idx in mygap_s:
                timegap[0].append(rows[idx])
            for idx in mygap_l:
                timegap[1].append(rows[idx])
        else:
            timegap = [mygap_s, mygap_l]
        return timegap

    def _update_tsys(self, context, infile, tsystable, spwmap, to_fieldid, gainfield):
        """
        Transfer Tsys values in a Tsys calibration table and fill Tsys
        values in DataTable.
        Tsys in cal table are averaged by channels taking into account
        of FLAG and linearly interpolated in time to derive values which
        corresponds to TIME in DataTable.

        Arguments
            context: pipeline context
            infile: the name of input MS
            tsystable: the name of Tsys calibration table
            spwmap: the list of SPW mapping
            to_fieldid: FIELD_ID of data table to which Tsys is transferred
            gainfield: how to find FIELD form which Tsys is extracted in cal table.
        """
        start_time = time.time()

        msobj = context.observing_run.get_ms(infile)
        to_antids = [a.id for a in msobj.antennas]
        from_fields = []
        if gainfield.upper() == 'NEAREST':
            LOG.info('to_fieldid={}'.format(to_fieldid))
            to_field = msobj.get_fields(field_id=to_fieldid)[0]
            if 'ATMOSPHERE' in to_field.intents:
                # if target field has ATMOSPHERE intent, use it
                from_fields = [to_fieldid]
            else:
                atm_fields = msobj.get_fields(intent='ATMOSPHERE')
                # absolute OFF
                test_prefix = '{}_OFF_'.format(to_field.clean_name)
                #LOG.info('test_prefix {}, atm_fields {}'.format(test_prefix, [a.clean_name for a in atm_fields]))
                nearest_id = numpy.where([a.clean_name.startswith(test_prefix) for a in atm_fields])[0]
                #LOG.info('nearest_id = {}'.format(nearest_id))
                if len(nearest_id) > 0:
                    from_fields = [atm_fields[i].id for i in nearest_id]
                else:
                    # more generic case that requires to search nearest field by separation
                    rmin = casa_tools.quanta.quantity(180.0, 'deg')
                    origin = to_field.mdirection
                    nearest_id = -1
                    for f in atm_fields:
                        r = casa_tools.measures.separation(origin, f.mdirection)
                        #LOG.info('before test: rmin {} r {} nearest_id {}'.format(rmin['value'], r['value'], nearest_id))
                        # quanta.le is equivalent to <=
                        if casa_tools.quanta.le(r, rmin):
                            rmin = r
                            nearest_id = f.id
                        #LOG.info('after test: rmin {} r {} nearest_id {}'.format(rmin['value'], r['value'], nearest_id))
                    if nearest_id != -1:
                        from_fields = [nearest_id]
                    else:
                        raise RuntimeError('No nearest field for Tsys update.')
        else:
            from_fields = [fld.id for fld in msobj.get_fields(gainfield)]
        LOG.info('from_fields = {}'.format(from_fields))

        with casa_tools.TableReader(tsystable) as tb:
            tsel = tb.query('FIELD_ID IN {}'.format(list(from_fields)))
            spws = tsel.getcol('SPECTRAL_WINDOW_ID')
            times = tsel.getcol('TIME')
            #fieldids = tsel.getcol('FIELD_ID')
            antids = tsel.getcol('ANTENNA1')
            tsys_masked = {}
            for i in range(tsel.nrows()):
                tsys = tsel.getcell('FPARAM', i)
                flag = tsel.getcell('FLAG', i)
                tsys_masked[i] = numpy.ma.masked_array(tsys, mask=(flag == True))
            tsel.close()

        #LOG.info('tsys={}'.format(tsys_masked))

        def map_spwchans(atm_spw, science_spw):
            """
            Map the channel ID ranges of ATMCal spw that covers frequency range of a science spw
            Arguments: spw object of ATMCal and science spws
            """
            atm_freqs = numpy.array(atm_spw.channels.chan_freqs)
            min_chan = numpy.where(abs(atm_freqs - float(science_spw.min_frequency.value)) == min(
                abs(atm_freqs - float(science_spw.min_frequency.value))))[0][0]
            max_chan = numpy.where(abs(atm_freqs - float(science_spw.max_frequency.value)) == min(
                abs(atm_freqs - float(science_spw.max_frequency.value))))[0][-1]
            start_atmchan = min(min_chan, max_chan)
            end_atmchan = max(min_chan, max_chan)
            # LOG.trace('calculate_average_tsys:   satrt_atmchan == %d' % start_atmchan)
            # LOG.trace('calculate_average_tsys:   end_atmchan == %d' % end_atmchan)
            if end_atmchan == start_atmchan:
                end_atmchan = start_atmchan + 1
            return start_atmchan, end_atmchan

        _dt_antenna = self.getcol('ANTENNA')
        _dt_spw = self.getcol('IF')
        dt_field = self.getcol('FIELD_ID')
        field_sel = numpy.where(dt_field == to_fieldid)[0]
        dt_antenna = _dt_antenna[field_sel]
        dt_spw = _dt_spw[field_sel]
        atm_spws = set(spws)
        science_spws = [x.id for x in msobj.get_spectral_windows(science_windows_only=True)]
        for spw_to, spw_from in enumerate(spwmap):
            # only process atm spws
            if spw_from not in atm_spws:
                continue

            # only process science spws
            if spw_to not in science_spws:
                continue

            atm_spw = msobj.get_spectral_window(spw_from)
            science_spw = msobj.get_spectral_window(spw_to)
            science_dd = msobj.get_data_description(spw=science_spw)
            corr_index = [science_dd.get_polarization_id(corr) for corr in science_dd.corr_axis]
            start_atmchan, end_atmchan = map_spwchans(atm_spw, science_spw)
            LOG.info('Transfer Tsys from spw {} (chans: {}~{}) to {}'.format(spw_from, start_atmchan, end_atmchan, spw_to))
            for ant_to in to_antids:
                # select caltable row id by SPW and ANT
                cal_idxs = numpy.where(numpy.logical_and(spws == spw_from, antids == ant_to))[0]
                if len(cal_idxs) == 0:
                    continue
                # atsys.shape = (nrow, npol)
                atsys = numpy.asarray([tsys_masked[i].take(corr_index, axis=0)[:, start_atmchan:end_atmchan+1].mean(axis=1).data
                                           for i in cal_idxs])
                dtrows = field_sel[numpy.where(numpy.logical_and(dt_antenna == ant_to, dt_spw == spw_to))[0]]
                #LOG.info('ant {} spw {} dtrows {}'.format(ant_to, spw_to, len(dtrows)))
                time_sel = times.take(cal_idxs)  # in sec
                for dt_id in dtrows:
                    #LOG.info('ant {} spw {} field {}'.format(self.getcell('ANTENNA', dt_id),
                    #                                         self.getcell('IF', dt_id),
                    #                                         self.getcell('FIELD_ID', dt_id)))
                    tref = self.getcell('TIME', dt_id) * 86400  # day->sec
                    # LOG.trace("cal_field_ids=%s" % cal_field_idxs)
                    # LOG.trace('atsys = %s' % str(atsys))
                    if atsys.shape[0] == 1:  # only one Tsys measurement selected
                        self.putcell('TSYS', dt_id, atsys[0, :])
                    else:
                        itsys = _interpolate(atsys, time_sel, tref)
                        self.putcell('TSYS', dt_id, itsys)
        end_time = time.time()
        LOG.info('_update_tsys: elapsed {} sec'.format(end_time - start_time))

    # @memory_profiler.profile
    def _update_flag(self, infile):
        """
        Read MS and update online flag status of DataTable.
        Arguments:
            context: pipeline context instance
            infile: the name of MS to transfer flag from
        NOTE this method should be called before applying the other flags.
        """
        LOG.info('Updating online flag for %s' % (os.path.basename(infile)))
        filename = self.getkeyword('FILENAME')
        assert os.path.basename(infile) == os.path.basename(filename)

        # back to previous impl. with reduced memory usage
        # (performance degraded)
        ms_rows = self.getcol('ROW')
        tmp_array = numpy.empty((4, 1,), dtype=numpy.int32)
        with casa_tools.TableReader(infile) as tb:
            # for dt_row in index[0]:
            for dt_row, ms_row in enumerate(ms_rows):
                # ms_row = rows[dt_row]
                flag = tb.getcell('FLAG', ms_row)
                rowflag = tb.getcell('FLAG_ROW', ms_row)
                # irow += 1
                npol = flag.shape[0]
                #online_flag = numpy.empty((npol, 1,), dtype=numpy.int32)
                online_flag = tmp_array[:npol, :]
                if rowflag == True:
                    online_flag[:] = 0
                else:
                    for ipol in range(npol):
                        online_flag[ipol, 0] = 0 if flag[ipol].all() else 1
                self.putcellslice('FLAG_PERMANENT', int(dt_row), online_flag,
                                  blc=[0, OnlineFlagIndex], trc=[npol - 1, OnlineFlagIndex],
                                  incr=[1, 1])


class RODataTableColumn(object):
    def __init__(self, table, name, dtype):
        self.tb = table
        self.name = name
        self.caster_get = dtype

    def __repr__(self):
        return '%s("%s","%s")' % (self.__class__.__name__, self.name, self.caster_get)

    def getcell(self, idx):
        return self.tb.getcell(self.name, idx)

    def getcol(self, startrow=0, nrow=-1, rowincr=1):
        return self.tb.getcol(self.name, startrow, nrow, rowincr)

    def getcellslice(self, rownr, blc, trc, incr):
        return self.tb.getcellslice(self.name, rownr, blc, trc, incr)

    def getcolslice(self, blc, trc, incr, startrow=0, nrow=-1, rowincr=1):
        return self.tb.getcolslice(self.name, blc, trc, incr, startrow, nrow, rowincr)

    def putcell(self, idx, val):
        self.__raise()

    def putcol(self, val, startrow=0, nrow=-1, rowincr=1):
        self.__raise()

    def putcellslice(self, rownr, value, blc, trc, incr):
        self.__raise()

    def putcolslice(self, value, blc, trc, incr, startrow=0, nrow=-1, rowincr=1):
        self.__raise()

    def __raise(self):
        raise NotImplementedError('column %s is read-only' % self.name)


class RWDataTableColumn(RODataTableColumn):
    def __init__(self, table, name, dtype):
        super(RWDataTableColumn, self).__init__(table, name, dtype)
        if dtype == list:
            self.caster_put = numpy.asarray
        else:
            self.caster_put = dtype

    def putcell(self, idx, val):
        self.tb.putcell(self.name, int(idx), self.caster_put(val))

    def putcol(self, val, startrow=0, nrow=-1, rowincr=1):
        self.tb.putcol(self.name, numpy.asarray(val), int(startrow), int(nrow), int(rowincr))

    def putcellslice(self, rownr, value, blc, trc, incr):
        return self.tb.putcellslice(self.name, rownr, value, blc, trc, incr)

    def putcolslice(self, value, blc, trc, incr, startrow=0, nrow=-1, rowincr=1):
        return self.tb.putcolslice(self.name, value, blc, trc, incr, startrow, nrow, rowincr)


class DataTableColumnNoChange(RWDataTableColumn):
    def __init__(self, table):
        super(RWDataTableColumn, self).__init__(table, "NOCHANGE", int)

    def putcell(self, idx, val):
        if isinstance(val, bool):
            v = -1
        else:
            v = val
        self.tb.putcell(self.name, int(idx), int(v))


class DataTableColumnMaskList(RWDataTableColumn):
    NoMask = numpy.zeros((1, 2), dtype=numpy.int32) - 1  # [[-1,-1]]

    def __init__(self, table):
        super(RWDataTableColumn, self).__init__(table, "MASKLIST", list)

    def getcell(self, idx):
        v = self.tb.getcell(self.name, int(idx))
        if sum(v[0]) < 0:
            return numpy.zeros(0, dtype=numpy.int32)
        else:
            return v

    def getcol(self, startrow=0, nrow=-1, rowincr=1):
        """
        Note: returned array has shape (nrow,nmask), in
              contrast to (nmask,nrow) for return value of
              tb.getcol().
        """
        if nrow < 0:
            nrow = self.tb.nrows()
        ret = collections.defaultdict(list)
        idx = 0
        for i in range(startrow, nrow, rowincr):
            tMASKLIST = self.getcell(i)
            if len(tMASKLIST) == 1 and tMASKLIST[0][0] == 0 and \
                    tMASKLIST[0][1] == 0:
                ret[idx] = tMASKLIST
            idx += 1
        return ret

    def putcell(self, idx, val):
        if len(val) == 0:
            v = self.NoMask
        else:
            v = val
        self.tb.putcell(self.name, int(idx), numpy.asarray(v))

    def putcol(self, val, startrow=0, nrow=-1, rowincr=1):
        """
        Note: input array should have shape (nrow,nmask), in
              contrast to (nmask,nrow) for tb.putcol()
        """
        if nrow < 0:
            nrow = min(startrow + len(val) * rowincr, self.tb.nrows())
        idx = 0
        for i in range(startrow, nrow, rowincr):
            self.putcell(i, numpy.asarray(val[idx]))
            idx += 1


def _interpolate(v, t, tref):
    # bisect.bisect_left(a, x)
    # bisect_left returns an insertion point of x in a.
    # if x matches any value in a, bisect_left returns its index.
    # (bisect_right and bisect returns index next to the matched value)
    idx = bisect.bisect_left(t, tref)
    #LOG.info('len(t) = {}, idx = {}'.format(len(t), idx))
    if idx == 0:
        return v[0]
    elif idx == len(t):
        return v[-1]
    else:
        t1 = t[idx] - tref
        t0 = tref - t[idx - 1]
        return (v[idx] * t0 + v[idx-1] * t1) / (t[idx] - t[idx-1])


def construct_timegroup(rows, group_id_list, group_association_list):
    timetable_dict = {x: [[], []] for x in group_id_list}
    for (idx, group_id) in enumerate(group_association_list):
        if group_id not in group_id_list:
            continue
        timetable_dict[group_id][0].append(rows[idx])
        timetable_dict[group_id][1].append(idx)
    return timetable_dict
