import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataTable, DataType
from pipeline.infrastructure import casa_tools
from .. import common

LOG = infrastructure.get_logger(__name__)


class WeightMSInputs(vdp.StandardInputs):
    """
    Inputs for exporting data to MS
    NOTE: infile should be a complete list of MSes
    """
    # Search order of input vis
    processing_data_type = [DataType.BASELINED, DataType.ATMCORR,
                            DataType.REGCAL_CONTLINE_ALL, DataType.RAW ]

    infiles = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    outfiles = vdp.VisDependentProperty(default='')
    antenna = vdp.VisDependentProperty(default='')
    spwid = vdp.VisDependentProperty(default=-1)
    fieldid = vdp.VisDependentProperty(default=-1)
    spwtype = vdp.VisDependentProperty(default=None)

    @vdp.VisDependentProperty(readonly=True)
    def spwtype(self):
        msobj = self.context.observing_run.get_ms(self.vis)
        spwobj = msobj.spectral_windows[self.spwid]
        return spwobj.type

    # Synchronization between infiles and vis is still necessary
    @vdp.VisDependentProperty
    def vis(self):
        return self.infiles

    def __init__(self, context, infiles=None, outfiles=None,
                 antenna=None, spwid=None, fieldid=None):
        super(WeightMSInputs, self).__init__()

        self.context = context
        self.infiles = infiles
        self.outfiles = outfiles
        self.antenna = antenna
        self.fieldid = fieldid
        self.spwid = spwid


class WeightMSResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(WeightMSResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(WeightMSResults, self).merge_with_context(context)

    def _outcome_name(self):
        # return [image.imagename for image in self.outcome]
        return self.outcome


class WeightMS(basetask.StandardTaskTemplate):
    Inputs = WeightMSInputs

    Rule = {'WeightDistance': 'Gauss',
            'Clipping': 'MinMaxReject',
            'WeightRMS': True,
            'WeightTsysExpTime': False}

    def prepare(self, datatable_dict=None):
        # for each data
        outfile = self.inputs.outfiles
        is_full_resolution = (self.inputs.spwtype.upper() in ["TDM", "FDM"])
        LOG.info('Setting weight for %s Antenna %s Spw %s Field %s' % \
                 (os.path.basename(outfile), self.inputs.ms.antennas[self.inputs.antenna].name,
                  self.inputs.spwid, self.inputs.ms.fields[self.inputs.fieldid].name))

        # make row mapping table between scantable and ms
        row_map = self._make_row_map()

        # set weight
        if is_full_resolution:
            minmaxclip = (WeightMS.Rule['Clipping'].upper() == 'MINMAXREJECT')
            weight_rms = WeightMS.Rule['WeightRMS']
            weight_tintsys = WeightMS.Rule['WeightTsysExpTime']
        else:
            minmaxclip = False
            weight_rms = False
            weight_tintsys = True
        datatable = None
        if datatable_dict is not None:
            datatable = datatable_dict[os.path.basename(outfile)]
        self._set_weight(row_map, minmaxclip=minmaxclip, weight_rms=weight_rms,
                         weight_tintsys=weight_tintsys, try_fallback=is_full_resolution,
                         default_datatable=datatable)

        result = WeightMSResults(task=self.__class__,
                                 success=True,
                                 outcome=outfile)

        return result

    def analyse(self, result):
        return result

    def _make_row_map(self):
        """
        Returns a dictionary which maps input and output MS row IDs
        The dictionary has
            key: input MS row IDs
            value: output MS row IDs
        It has row IDs of all intents of selected field, spw, and antenna.
        """
        infile = self.inputs.infiles
        outfile = self.inputs.outfiles
        spwid = self.inputs.spwid
        antid = self.inputs.antenna
        fieldid = self.inputs.fieldid
        in_rows = []
        out_rows = []
        with casa_tools.TableReader(os.path.join(outfile, 'DATA_DESCRIPTION')) as tb:
            spwids = tb.getcol('SPECTRAL_WINDOW_ID')
            data_desc_id = numpy.where(spwids == spwid)[0][0]

        with casa_tools.TableReader(outfile) as tb:
            tsel = tb.query('DATA_DESC_ID==%s && FIELD_ID==%d && ANTENNA1==%d && ANTENNA2==%d' %
                            (data_desc_id, fieldid, antid, antid),
                            sortlist='TIME')
            out_rows = tsel.rownumbers()
            stateids = numpy.unique(tsel.getcol('STATE_ID'))
            tsel.close()

        with casa_tools.TableReader(os.path.join(infile, 'DATA_DESCRIPTION')) as tb:
            spwids = tb.getcol('SPECTRAL_WINDOW_ID')
            data_desc_id = numpy.where(spwids == spwid)[0][0]

        with casa_tools.TableReader(infile) as tb:
            # Tentative adding state id selection for sdatmcor data selection issue.
            tsel = tb.query('DATA_DESC_ID==%d && FIELD_ID==%d && ANTENNA1==%d && ANTENNA2==%d && STATE_ID IN %s' %
                            (data_desc_id, fieldid, antid, antid, list(stateids)),
                            sortlist='TIME', style='python')
            if tsel.nrows() > 0:
                in_rows = tsel.rownumbers()
            tsel.close()

        assert len(in_rows) == len(out_rows)

        row_map = {}
        # in_row: out_row
        for index in range(len(in_rows)):
            row_map[in_rows[index]] = out_rows[index]

        return row_map

    def _set_weight(self, row_map, minmaxclip, weight_rms, weight_tintsys, try_fallback=False, default_datatable=None):
        inputs = self.inputs
        infile = inputs.infiles
        outfile = inputs.outfiles
        spwid = inputs.spwid
        antid = inputs.antenna
        fieldid = self.inputs.fieldid

        context = inputs.context
        datatable = None
        if default_datatable is not None:
            filename = default_datatable.getkeyword('FILENAME')
            if os.path.basename(filename) == os.path.basename(infile):
                datatable = default_datatable
        if datatable is None:
            datatable_name = os.path.join(context.observing_run.ms_datatable_name, os.path.basename(infile))
            datatable = DataTable(name=datatable_name, readonly=True)

        # get corresponding datatable rows (only IDs of target scans will be retruned)
        index_list = common.get_index_list_for_ms(datatable, [infile], [antid], [fieldid], [spwid])

        in_rows = datatable.getcol('ROW').take(index_list)
#         # row map filtered by target scans (key: target input
#         target_row_map = {}
#         for idx in in_rows:
#             target_row_map[idx] = row_map.get(idx, -1)

        weight = {}
#         for row in in_rows:
#             weight[row] = 1.0

        # PIPE-1523
        net_flags = datatable.getcol('FLAG_SUMMARY').take(index_list, axis=1)

        # set weight (key: input MS row ID, value: weight)
        # TODO: proper handling of pols
        if weight_rms:
            stats = datatable.getcol('STATISTICS').take(index_list, axis=2)
            for index in range(len(in_rows)):
                row = in_rows[index]
                cell_stat = stats[:, :, index]
                flags = net_flags[:, index]
                weight[row] = numpy.ones(cell_stat.shape[0])
                for ipol in range(weight[row].shape[0]):
                    stat = cell_stat[ipol, 1]  # get post-fitting RMS
                    if flags[ipol] == 1:
                        # treat unflagged data
                        if stat > 0.0:
                            weight[row][ipol] /= (stat * stat)
                        elif stat < 0.0 and cell_stat[ipol, 2] > 0.0:
                            stat = cell_stat[ipol, 2]  # get pre-fitting RMS
                            weight[row][ipol] /= (stat * stat)
                        elif try_fallback:
                            weight_tintsys = True
                        else:
                            weight[row][ipol] = 0.0
                    else:
                        # treat flagged data
                        weight[row][ipol] = 0.0

        if weight_tintsys:
            exposures = datatable.getcol('EXPOSURE').take(index_list)
            tsyss = datatable.getcol('TSYS').take(index_list, axis=1)
            for index in range(len(in_rows)):
                row = in_rows[index]
                exposure = exposures[index]
                tsys = tsyss[:, index]
                if row not in weight:
                    weight[row] = numpy.ones(tsys.shape[0])
                for ipol in range(weight[row].shape[0]):
                    if tsys[ipol] > 0.5:
                        weight[row][ipol] *= (exposure / (tsys[ipol] * tsys[ipol]))
                    else:
                        weight[row][ipol] = 0.0

        # put weight
        with casa_tools.TableReader(outfile, nomodify=False) as tb:
            # The selection, tsel, contains all intents.
            # Need to match output element in selected table by rownumbers().
            tsel = tb.query('ROWNUMBER() IN %s' % (list(row_map.values())), style='python')
            ms_weights = tsel.getcol('WEIGHT')
            rownumbers = tsel.rownumbers().tolist()
            for idx in range(len(in_rows)):
                in_row = in_rows[idx]
                out_row = row_map[in_row]
                ms_weight = weight[in_row]
                # search for the place of selected MS col to put back the value.
                ms_index = rownumbers.index(out_row)
                ms_weights[:, ms_index] = ms_weight
            tsel.putcol('WEIGHT', ms_weights)
            tsel.close()

        # set channel flag for min/max in each channel
        minmaxclip = False
        if minmaxclip:
            with casa_tools.TableReader(outfile, nomodify=False) as tb:
                tsel = tb.query('ROWNUMBER() IN %s' % (list(row_map.values())),
                                style='python')
                if 'FLOAT_DATA' in tsel.colnames():
                    data_column = 'FLOAT_DATA'
                else:
                    data_column = 'DATA'
                data = tsel.getcol(data_column)  # (npol, nchan, nrow)

                (npol, nchan, nrow) = data.shape
                if nrow > 2:
                    argmax = data.argmax(axis=2)
                    argmin = data.argmin(axis=2)
                    print(argmax.tolist())
                    print(argmin.tolist())
                    flag = tsel.getcol('FLAG')
                    for ipol in range(npol):
                        maxrow = argmax[ipol]
                        minrow = argmin[ipol]
                        for ichan in range(nchan):
                            flag[ipol, ichan, maxrow[ichan]] = True
                            flag[ipol, ichan, minrow[ichan]] = True
                    tsel.putcol('FLAG', flag)
                tsel.close()
