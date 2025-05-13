#!/usr/bin/env python
"""
Class to handle continuum frequency range files.

The text files contain ranges per source and spw using CASA syntax.
The keyword "NONE" can be written in case of non-detection of a continuum
frequency range.
"""

import collections
import re
from typing import Any, Dict, List, Tuple, Union

import numpy as np

from . import casa_tools, logging, utils

LOG = logging.get_logger(__name__)


class ContFileHandler(object):

    def __init__(self, filename, warn_nonexist=False):
        self.filename = filename
        self.p = re.compile(r'([\d.]*)(~)([\d.]*)(\D*)')
        self.cont_ranges = self.read(warn_nonexist=warn_nonexist)

    def read(self, warn_nonexist=False):

        if self.filename is None:
            return {}

        cont_ranges = {'fields': {}, 'version': 3}

        try:
            cont_region_data = [item.strip() for item in open(self.filename, 'r').readlines() if item.strip()]
        except:
            cont_region_data = []
            if warn_nonexist:
                LOG.warning('Could not read file %s. Using empty selection.' % self.filename)

        for item in cont_region_data:
            try:
                if ((item.find('SpectralWindow:') == -1) and
                    (item.find('SPW') == -1 or item.find('SPW') > 0) and
                    (item.find('Flags') == -1) and
                    (item.find('~') == -1) and
                    (item not in ('NONE', 'ALL', 'ALLCONT'))):
                    if item.find('Field:') == 0:
                        field_name = item.split('Field:')[1].strip()
                    else:
                        field_name = item
                    field_name = utils.dequote(field_name).strip()
                    if field_name != '':
                        cont_ranges['fields'][field_name] = {}
                elif item.find('SPW') == 0:
                    cont_ranges['version'] = 1
                    virt_spw_id = item.split('SPW')[1].strip()
                    cont_ranges['fields'][field_name][virt_spw_id] = []
                elif item.find('SpectralWindow:') == 0:
                    spw_items = item.split()
                    if len(spw_items) == 2:
                        cont_ranges['version'] = 2
                        virt_spw_id = spw_items[1]
                        spw_name = f'spw{spw_items[1]}'
                    elif len(spw_items) == 3:
                        cont_ranges['version'] = 3
                        virt_spw_id = spw_items[1]
                        spw_name = spw_items[2]
                    cont_ranges['fields'][field_name][virt_spw_id] = {'spwname': spw_name, 'flags': [], 'ranges': []}
                elif item.find('Flags:') == 0:
                    flags = item.split()
                    cont_ranges['fields'][field_name][virt_spw_id]['flags'].extend(flags[1:])
                    cont_ranges['version'] = 3
                else:
                    cont_regions = self.p.findall(item.replace(';', ''))
                    for cont_region in cont_regions:
                        if cont_ranges['version'] == 1:
                            unit = cont_region[3]
                            refer = 'TOPO'
                        elif cont_ranges['version'] in (2, 3):
                            unit, refer = cont_region[3].split()
                        fLow = casa_tools.quanta.convert('%s%s' % (cont_region[0], unit), 'GHz')['value']
                        fHigh = casa_tools.quanta.convert('%s%s' % (cont_region[2], unit), 'GHz')['value']
                        cont_ranges['fields'][field_name][virt_spw_id]['ranges'].append({'range': (fLow, fHigh), 'refer': refer})
            except Exception as e:
                LOG.error(f'Could not read cont file {self.filename}: {e}')
                raise e

        for fkey in cont_ranges['fields']:
            for skey in cont_ranges['fields'][fkey]:
                if cont_ranges['fields'][fkey][skey]['ranges'] == []:
                    cont_ranges['fields'][fkey][skey]['ranges'] = ['NONE']

        return cont_ranges

    def write(self, cont_ranges=None):

        if self.filename is None:
            return

        if cont_ranges is None:
            cont_ranges = self.cont_ranges

        with open(self.filename, 'w+') as fd:
            if cont_ranges != {}:
                for field_name in cont_ranges['fields']:
                    if cont_ranges['version'] == 1:
                        fd.write('%s\n\n' % (field_name.replace('"', '')))
                    elif cont_ranges['version'] in (2, 3):
                        fd.write('Field: %s\n\n' % (field_name.replace('"', '')))
                    for virt_spw_id in cont_ranges['fields'][field_name]:
                        if cont_ranges['version'] == 1:
                            fd.write('SPW%s\n' % virt_spw_id)
                        elif cont_ranges['version'] in (2, 3):
                            fd.write('SpectralWindow: %s' % virt_spw_id)
                            if cont_ranges['version'] == 3:
                                fd.write(f" {cont_ranges['fields'][field_name][virt_spw_id]['spwname']}\n")
                            else:
                                fd.write('\n')

                        if cont_ranges['fields'][field_name][virt_spw_id]['flags'] != [] and cont_ranges['version'] == 3:
                            fd.write(f"Flags: {' '.join(cont_ranges['fields'][field_name][virt_spw_id]['flags'])}\n")

                        if cont_ranges['fields'][field_name][virt_spw_id]['ranges'] in ([], ['NONE']):
                            if cont_ranges['version'] == 2:
                                fd.write('NONE\n')
                        elif cont_ranges['fields'][field_name][virt_spw_id]['ranges'] == ['ALL']:
                            if cont_ranges['version'] == 2:
                                fd.write('ALL\n')
                        else:
                            for freq_range in cont_ranges['fields'][field_name][virt_spw_id]['ranges']:
                                if freq_range in ('ALL', 'NONE') and cont_ranges['version'] == 2:
                                    fd.write(f'{freq_range}\n')
                                else:
                                    if cont_ranges['version'] == 1:
                                        fd.write('%.10f~%.10fGHz\n' % (float(freq_range['range'][0]), float(freq_range['range'][1])))
                                    elif cont_ranges['version'] in (2, 3):
                                        fd.write('%.10f~%.10fGHz %s\n' % (float(freq_range['range'][0]), float(freq_range['range'][1]),
                                                                    freq_range['refer']))
                        fd.write('\n')

    def get_merged_selection(self, field_name:str, spw_id:str, spw_name:Union[str, None]=None, cont_ranges:Union[Dict, None]=None):
        """
        Inputs:

        field_name: field name
        spw_def: spw ID (digits) or spw name
        cont_ranges: Optional user supplied continuum ranges lookup dictionary

        Returns:
        merged continuum selection: string
        all continuum flag: boolean
        low bandwidth flag: boolean
        low spread flag: boolean
        """

        field_name = str(field_name)
        spw_id = str(spw_id)
        if spw_name is not None:
            spw_name = str(spw_name)

        if cont_ranges is None:
            cont_ranges = self.cont_ranges

        all_continuum = False
        low_bandwidth = False
        low_spread = False
        if field_name in cont_ranges['fields']:
            # Internally the lookup dictionary still relies on virtual spw IDs.
            # But with PIPE-2128 we introduced writing spw names to cont.dat.
            # If the spw name is given, it is preferred for the lookup.
            virt_spw_id = self.get_cont_dat_virt_spw_id(spw_id, spw_name)

            if virt_spw_id in cont_ranges['fields'][field_name]:
                if cont_ranges['fields'][field_name][virt_spw_id]['ranges'] not in (['ALL'], [], ['NONE']):
                    merged_cont_ranges = utils.merge_ranges(
                        [cont_range['range'] for cont_range in cont_ranges['fields'][field_name][virt_spw_id]['ranges'] if isinstance(cont_range, dict)])
                    cont_ranges_spwsel = ';'.join(['%.10f~%.10fGHz' % (float(spw_sel_interval[0]), float(spw_sel_interval[1]))
                                                   for spw_sel_interval in merged_cont_ranges])
                    refers = np.array([cont_range['refer']
                                       for cont_range in cont_ranges['fields'][field_name][virt_spw_id]['ranges'] if isinstance(cont_range, dict)])
                    if (refers == 'TOPO').all():
                        refer = 'TOPO'
                    elif (refers == 'LSRK').all():
                        refer = 'LSRK'
                    elif (refers == 'SOURCE').all():
                        refer = 'SOURCE'
                    else:
                        refer = 'UNDEFINED'
                    cont_ranges_spwsel = '%s %s' % (cont_ranges_spwsel, refer)
                    if 'ALL' in cont_ranges['fields'][field_name][virt_spw_id]['ranges'] or 'ALLCONT' in cont_ranges['fields'][field_name][virt_spw_id]['flags']:
                        all_continuum = True
                elif cont_ranges['fields'][field_name][virt_spw_id]['ranges'] == ['ALL'] or 'ALLCONT' in cont_ranges['fields'][field_name][virt_spw_id]['flags']:
                    cont_ranges_spwsel = 'ALLCONT'
                    all_continuum = True
                else:
                    cont_ranges_spwsel = 'NONE'
                low_bandwidth = 'LOWBANDWIDTH' in cont_ranges['fields'][field_name][virt_spw_id]['flags']
                low_spread = 'LOWSPREAD' in cont_ranges['fields'][field_name][virt_spw_id]['flags']
            else:
                LOG.info(f'spw ID {virt_spw_id} not found in cont file.')
                cont_ranges_spwsel = ''
        else:
            LOG.info(f'Field {field_name} not found in cont file.')
            cont_ranges_spwsel = ''

        return cont_ranges_spwsel, all_continuum, low_bandwidth, low_spread

    def to_topo(self, selection:str, msnames:List[str], fields:List[Union[int, str]], spw_id:Union[int, str], observing_run:Any, spw_name:Union[str, None]=None, ctrim:int=0, ctrim_nchan:int=-1) -> Tuple[List[str], List[str], Dict]:

        frame_freq_selection, refer = selection.split()
        if refer not in ('LSRK', 'SOURCE'):
            msg = f'Original reference frame must be LSRK or SOURCE, not {refer}.'
            LOG.error(msg)
            raise Exception(msg)

        if len(msnames) != len(fields):
            msg = 'MS names and fields lists must match in length.'
            LOG.error(msg)
            raise Exception(msg)

        virt_spw_id = int(self.get_cont_dat_virt_spw_id(str(spw_id), spw_name))

        qaTool = casa_tools.quanta
        suTool = casa_tools.synthesisutils

        freq_ranges = []
        aggregate_frame_bw = '0.0GHz'
        cont_regions = self.p.findall(frame_freq_selection.replace(';', ''))
        for cont_region in cont_regions:
            fLow = qaTool.convert('%s%s' % (cont_region[0], cont_region[3]), 'Hz')['value']
            fHigh = qaTool.convert('%s%s' % (cont_region[2], cont_region[3]), 'Hz')['value']
            freq_ranges.append((fLow, fHigh))
            delta_f = qaTool.sub('%sHz' % fHigh, '%sHz' % fLow)
            aggregate_frame_bw = qaTool.add(aggregate_frame_bw, delta_f)

        topo_chan_selections = []
        topo_freq_selections = []
        for i in range(len(msnames)):
            msname = msnames[i]
            real_spw_id = observing_run.virtual2real_spw_id(virt_spw_id, observing_run.get_ms(msname))
            field = int(fields[i])
            topo_chan_selection = []
            topo_freq_selection = []
            try:
                if field != -1:
                    for freq_range in freq_ranges:
                        if refer == 'LSRK':
                            result = suTool.advisechansel(msname=msname, fieldid=field, spwselection=str(real_spw_id),
                                                          freqstart=freq_range[0], freqend=freq_range[1], freqstep=100.,
                                                          freqframe='LSRK')
                        else:
                            result = suTool.advisechansel(msname=msname, fieldid=field, spwselection=str(real_spw_id),
                                                          freqstart=freq_range[0], freqend=freq_range[1], freqstep=100.,
                                                          freqframe='SOURCE', ephemtable='TRACKFIELD')
                        suTool.done()
                        spw_index = result['spw'].tolist().index(real_spw_id)
                        start = result['start'][spw_index]
                        stop = start + result['nchan'][spw_index] - 1
                        # Optionally skip edge channels
                        if ctrim_nchan != -1:
                            start = start if (start >= ctrim) else ctrim
                            stop = stop if (stop < (ctrim_nchan-ctrim)) else ctrim_nchan-ctrim-1
                        if stop >= start:
                            topo_chan_selection.append((start, stop))
                            result = suTool.advisechansel(msname=msname, fieldid=field,
                                                          spwselection='%d:%d~%d' % (real_spw_id, start, stop),
                                                          freqframe='TOPO', getfreqrange=True)
                            fLow = float(qaTool.getvalue(qaTool.convert(result['freqstart'], 'GHz')))
                            fHigh = float(qaTool.getvalue(qaTool.convert(result['freqend'], 'GHz')))
                            topo_freq_selection.append((fLow, fHigh))
            except Exception as e:
                LOG.info('Cannot calculate TOPO range for MS %s Field %s SPW %s. Exception: %s' % (msname, field, real_spw_id, e))

            topo_chan_selections.append(';'.join('%d~%d' % (item[0], item[1]) for item in topo_chan_selection))
            topo_freq_selections.append('%s TOPO' % (';'.join('%.10f~%.10fGHz' %
                                                              (float(item[0]), float(item[1])) for item in topo_freq_selection)))

        return topo_freq_selections, topo_chan_selections, aggregate_frame_bw

    def get_cont_dat_virt_spw_id(self, spw_id:str, spw_name:Union[str, None]):

        virt_spw_id = None
        if spw_name is not None:
            for f in self.cont_ranges['fields']:
                for s in self.cont_ranges['fields'][f]:
                    if self.cont_ranges['fields'][f][s]['spwname'] == spw_name:
                        virt_spw_id = s
                        break
                if virt_spw_id is None:
                    LOG.info(f'SPW name: {spw_name} not found. Falling back to SPW ID lookup.')

        if virt_spw_id is None:
            if not spw_id.isdigit():
                msg = f'SPW ID string must be an integer, not {spw_id}.'
                LOG.error(msg)
                raise Exception(msg)
            else:
                # "Old" style SPW ID lookup
                virt_spw_id = spw_id

        return virt_spw_id


def contfile_to_spwsel(vis, context, contfile='cont.dat', use_realspw=True):
    """Translate continuum ranges specified in contfile to frequency selection string.

    The return is a dictionary with field names with keys and spwsel as values, e.g.,
        {'04287+1801': '20:327.464~328.183GHz;328.402~329.136GHz,26:340.207~340.239GHz;340.280~340.313GHz'}
    By default (use_realspw=True), the frequency selection string is in real SPWs of input vis, even though
    contfile is in virtual SPWs. If use_realspw=False, the output frequency selection string is in virtual SPWs.
    If the frequencies specified in the contfile are in LSRK, they will be converted to TOPO.
    """

    contfile_handler = ContFileHandler(contfile)
    contdict = contfile_handler.read(warn_nonexist=False)
    ms = context.observing_run.get_ms(vis)
    fielddict = {}

    for field in contdict['fields']:

        # Note that ContFileHandler and cont.dat use original field names from CASA tables, rather than 
        # the "CASA-safe" names (see PIPE-1887 and heurtsics/field_parameter.md). So we need the dequotation
        # and then match.
        fieldid_list = [fieldobj.id for fieldobj in ms.fields if utils.dequote(fieldobj.name) == field]

        # If no field is found, skip it.
        if not fieldid_list:
            continue

        spwstring = ''
        for virt_spw_id in contdict['fields'][field]:
            spw_name = context.observing_run.virtual_science_spw_ids[int(virt_spw_id)]
            crange_list = [crange for crange in contdict['fields'][field]
                           [virt_spw_id]['ranges'] if crange not in ('NONE', 'ALL', 'ALLCONT')]
            if crange_list[0]['refer'] in ('LSRK', 'SOURCE'):
                LOG.info("Converting from %s to TOPO...", crange_list[0]['refer'])
                sname = field
                field_id = str(fieldid_list[0])

                cranges_spwsel = collections.OrderedDict()
                cranges_spwsel[sname] = collections.OrderedDict()
                cranges_spwsel[sname][virt_spw_id], _, _, _ = contfile_handler.get_merged_selection(sname, virt_spw_id, spw_name)

                freq_ranges, _, _ = contfile_handler.to_topo(
                    cranges_spwsel[sname][virt_spw_id], [vis], [field_id], int(virt_spw_id),
                    context.observing_run, spw_name)
                freq_ranges_list = freq_ranges[0].split(';')
                spwstring = spwstring + virt_spw_id + ':'
                for freqrange in freq_ranges_list:
                    spwstring = spwstring + freqrange.replace(' TOPO', '') + ';'
                spwstring = spwstring[:-1]
                spwstring = spwstring + ','

            if crange_list[0]['refer'] == 'TOPO':
                LOG.info("Using TOPO frequency specified in {!s}".format(contfile))
                spwstring = spwstring + virt_spw_id + ':'
                for freqrange in crange_list:
                    spwstring = spwstring + str(freqrange['range'][0]) + '~' + str(freqrange['range'][1]) + 'GHz;'
                spwstring = spwstring[:-1]
                spwstring = spwstring + ','

        # remove appending semicolon
        spwstring = spwstring[:-1]

        if use_realspw:
            spwstring = context.observing_run.get_real_spwsel([spwstring], [vis])
        fielddict[field] = spwstring[0]

    LOG.info("Using frequencies in TOPO reference frame:")
    for field, spwsel in fielddict.items():
        LOG.info("    Field: {!s}   SPW: {!s}".format(field, spwsel))

    return fielddict


def contfile_to_chansel(vis, context, contfile='cont.dat', excludechans=False):
    """Translate continuum ranges specified in contfile to channel selection string.

    The return is a dictionary with field names with keys and chansel as values, e.g.,
        {'04287+1801': '20:327~328,26:340~341'}
    The channel selection string is in real SPWs of input vis.
    If excludechans=True, the returned string will select channels outside the continuum ranges instead.
    """

    spwsel_dict = contfile_to_spwsel(vis, context, contfile, use_realspw=True)
    chansel_dict = collections.OrderedDict()
    for field, spwsel in spwsel_dict.items():
        chansel_dict[field] = spwsel2chansel(vis, utils.fieldname_for_casa(field), spwsel, excludechans)

    return chansel_dict


def spwsel2chansel(vis, field, spwsel, excludechans):
    """Convert selections of frequecy ranges to channel indexes.

    This function can convert selections of spws/chans in to channel indexes.
    If excludechans=True, it will select channels outside of the input selection.

    This function starts as a copy of a private helper function (_quantityRangesToChannels) from
        casatasks.private.task_uvcontsub_old._quantityRangesToChannels (ver6.5.2/6.5.3)
        casatasks.private.task_uvcontsub._quantityRangesToChannels (ver6.5.1)
    https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatasks/src/private/task_uvcontsub_old.py?at=refs%2Ftags%2F6.5.3.28#316
    https://casadocs.readthedocs.io/en/v6.5.3/api/tt/casatasks.manipulation.uvcontsub_old.html (see the "fitspw" parameter)

    A refactoring work was later done via PIPE-1815.

    Also see the relevant disscussion on this action in JIRA:
    https://open-jira.nrao.edu/browse/CAS-13631?focusedCommentId=203983&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-203983

    Examples:

    CASA <17>: from pipeline.infrastructure.contfilehandler import spwsel2chansel
    CASA <18>: spwsel2chansel('uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms','helms30','0:215369.8696MHz~215490.8696MHz',False)
    Out[18]: '0:56~63'

    CASA <19>: spwsel2chansel('uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms','helms30','0:215369.8696MHz~215490.8696MHz',True)
    Out[19]: '0:0~55,0:64~127'

    Note: the field input value is a string in CASA/field selection syntax, e.g. a "CASA-safe" field name
    also see https://casacore.github.io/casacore-notes/263.html#x1-190004
    """

    with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as tb:
        nspw = tb.nrows()

    fullspwids = str(list(range(nspw))).strip('[,]')
    tql = {'field': field, 'spw': fullspwids}

    with casa_tools.MSReader(vis) as ms:

        ms.msselect(tql, True)
        allsels = ms.msselectedindices()
        ms.reset()

        # input fitspw selection
        tql['spw'] = spwsel
        ms.msselect(tql, True)
        usersels = ms.msselectedindices()['channel']

    # sort the arrays so that chan ranges are in order
    usersels = usersels[np.lexsort((usersels[:, 1], usersels[:, 0]))]
    spwid = -1
    prevspwid = None
    newchanlist = []
    nsels = len(usersels)
    # casalog.post("Usersels=",usersels)
    if excludechans:
        for isel in range(nsels):
            prevspwid = spwid
            spwid = usersels[isel][0]
            lochan = usersels[isel][1]
            hichan = usersels[isel][2]
            stp = usersels[isel][3]
            maxchanid = allsels['channel'][spwid][2]
            # find left and right side ranges of the selected range
            if spwid != prevspwid:
                # first line in the selected spw
                if lochan > 0:
                    outloL = 0
                    outhiL = lochan-1
                    outloR = (0 if hichan+1 >= maxchanid else hichan+1)
                    if outloR:
                        if isel < nsels-1 and usersels[isel+1][0] == spwid:
                            outhiR = usersels[isel+1][1]-1
                        else:
                            outhiR = maxchanid
                    else:
                        outhiR = 0  # higher end of the user selected range reaches maxchanid
                        # so no right hand side range
                    # casalog.post("outloL,outhiL,outloR,outhiR==", outloL,outhiL,outloR,outhiR)
                else:
                    # no left hand side range
                    outloL = 0
                    outhiL = 0
                    outloR = hichan+1
                    if isel < nsels-1 and usersels[isel+1][0] == spwid:
                        outhiR = usersels[isel+1][1]-1
                    else:
                        outhiR = maxchanid
            else:
                #expect the left side range is already taken care of
                outloL = 0
                outhiL = 0
                outloR = hichan+1
                if outloR >= maxchanid:
                    #No more boundaries to consider
                    outloR = 0
                    outhiR = 0
                else:
                    if isel < nsels-1 and usersels[isel+1][0] == spwid:
                        outhiR = min(usersels[isel+1][1]-1, maxchanid)
                    else:
                        outhiR = maxchanid
                    if outloR > outhiR:
                        outloR = 0
                        outhiR = 0
            if (not(outloL == 0 and outhiL == 0)) and outloL <= outhiL:
                newchanlist.append([spwid, outloL, outhiL, stp])
            if (not(outloR == 0 and outhiR == 0)) and outloR <= outhiR:
                newchanlist.append([spwid, outloR, outhiR, stp])
        # casalog.post("newchanlist=",newchanlist)
    else:
        # excludechans=False
        newchanlist = usersels

    # return newchanlist
    # create spw selection string from newchanlist
    spwstr = ''
    for irange, chanrange in enumerate(newchanlist):
        spwstr += str(chanrange[0])+':'+str(chanrange[1])+'~'+str(chanrange[2])
        if irange != len(newchanlist)-1:
            spwstr += ','

    return spwstr
