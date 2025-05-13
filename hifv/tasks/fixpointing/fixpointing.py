import shutil
import os
import numpy as np
import math
from datetime import datetime

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tools, task_registry


LOG = infrastructure.get_logger(__name__)


def fixpointing_offset_vlass(vis, intable='POINTING', antlist=[], timeoffset=[0.45, 0.95], dofilter=False,
                             usetarget=True, dointerval=True, dolookahead=True, dodirectiononly=False):
    """
    Version 0.0  STM 2019-02-05 from KG fixpointing
    Version 1.0  STM 2019-02-06 extrapolate using single-diff derivative on TARGET
    Version 1.01 STM 2019-02-12 usetarget
    Version 1.1  STM 2019-02-21 include filtering on DIRECTION-TARGET, intable
    Version 1.2  STM 2019-04-10 option to correct only DIRECTION (based on TARGET)
    Version 1.2  STM 2019-05-03 inster st.close commands
    this function will apply time offsets to the pointing in the table for the selected antennas

    Example to correct the pointing on a subset of antennas, and to filter the encoder data:
    fixpointing_offset_vlass(vis='VLASS1.1.sb34523741.eb34557765.58027.86045872685.ms',antlist=[2,3,4,5,6,8,9,10,11,13,14,16,17,18,20,21,22,23,24,25])

    To also copy the TARGET column (after fixing) to DIRECTION:
    fixpointing_offset_vlass(vis='VLASS1.1.sb34523741.eb34557765.58027.86045872685.ms',antlist=[2,3,4,5,6,8,9,10,11,13,14,16,17,18,20,21,22,23,24,25],usetarget=True)

    To only copy the TARGET column (after fixing) to DIRECTION:
    fixpointing_offset_vlass(vis='VLASS1.1.sb34523741.eb34557765.58027.86045872685.ms',antlist=[],dofilter=False,dointerval=False,usetarget=True)

    Another example (different dataset)
    fixpointing_offset_vlass(vis='VLASS1.1.sb34901451.eb34995355.58154.327259618054.ms',antlist=[2,3,4,5,7,8,9,10,11,13,14,16,17,18,20,21,22,23,24])

    For data taken in VLASS1.2 onward without the timing offset to pointing, but for which you
    want to construct a valid POINTING table (using the TARGET column):
    fixpointing_offset_vlass(vis='TSKY0001.sb36243279.eb36252233.58521.307180902775_OTFM_TARGET.ms',dofilter=False,usetarget=True,dointerval=True)

    Note that backup versions of the existing POINTING table will be created when run.

    For VLASS1.1 the dates with sets of old ACUs are (from VLASS Memo 12):

    9/7/17-10/23/17
    ea03, ea04, ea05, ea06, ea07, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27, ea28

    10/23/17-2/7/18
    ea03, ea04, ea05, ea06, ea07, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27

    2/7/18-2/20/18
    ea03, ea04, ea05, ea06, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27

    """
    r2d = 180.0 / np.pi
    datestring = datetime.isoformat(datetime.today())
    if not os.path.exists(vis + '/POINTING_ORIG'):
        shutil.copytree(vis + '/POINTING', vis + '/POINTING_ORIG')
        LOG.info('Copying POINTING to POINTING_ORIG')
    else:
        backupname = 'POINTING_BACKUP_' + datestring
        shutil.copytree(vis + '/POINTING', vis + '/' + backupname)
        LOG.info('Copying POINTING to ' + backupname)
    if intable != 'POINTING':
        if os.path.exists(vis + '/' + intable):
            shutil.copytree(vis + '/' + intable, vis + '/POINTING')
            LOG.info('Copying ' + intable + ' to POINTING')
        else:
            LOG.error('ERROR: could not find intable = ' + vis + '/' + intable)
            raise NameError('InTable not found')

    with casa_tools.TableReader(vis + '/POINTING', nomodify=False) as tb:
        # tb.open(vis + '/POINTING', nomodify=False)
        ants = tb.getcol('ANTENNA_ID')
        ants = np.unique(ants)
        dref = 'J2000'
        eref = tb.getcolkeyword('TIME', 'MEASINFO')['Ref']
        #
        # Need to get the columns
        col = 'DIRECTION'
        colkeyw = tb.getcolkeyword(col, 'MEASINFO')
        pref = tb.getcolkeyword(col, 'MEASINFO')['Ref']
        tcol = 'TARGET'
        tcolkeyw = tb.getcolkeyword(tcol, 'MEASINFO')
        tref = tb.getcolkeyword(tcol, 'MEASINFO')['Ref']
        # colkeyw['Ref']=dref
        # tb.putcolkeyword(col, 'MEASINFO', colkeyw)
        casa_tools.measures.doframe(casa_tools.measures.observatory('VLA'))
        alltimes = tb.getcol('TIME')
        firsttime = alltimes[0]
        del (alltimes)
        #
        delta_time = 0.1  # expected DT in seconds
        integ_offset_x = int(timeoffset[0] / delta_time)
        integ_offset_y = int(timeoffset[1] / delta_time)
        if dodirectiononly:
            LOG.info('Will correct only the DIRECTION column (based on TARGET motions)')
        else:
            LOG.info('Will correct both TARGET and DIRECTION columns (based on TARGET motions)')
        if dolookahead:
            LOG.info('Will lookahead (%i,%i) integrations in (AZ,EL)' % (integ_offset_x, integ_offset_y))
        #
        if len(antlist) > 0:
            # Correct for timing offsets in pointing for these antennas
            for ant in antlist:
                st = tb.query('ANTENNA_ID==' + str(ant))
                nrows = st.nrows()
                LOG.info('Ant %i processing nrows = %i' % (ant, nrows))
                # print(st.nrows())
                ptimes = st.getcol('TIME')
                dt = np.diff(ptimes, 1)
                ntimes = len(ptimes)

                targ_orig = st.getcol(tcol)
                dirv_orig = st.getcol(col)

                sh = dirv_orig.shape
                targ = targ_orig.reshape((sh[0], sh[len(sh) - 1]))
                dirv = dirv_orig.reshape((sh[0], sh[len(sh) - 1]))

                x = targ[0, :]
                y = targ[1, :]
                dirx = dirv[0, :]
                diry = dirv[1, :]

                if not dolookahead:
                    dx = np.diff(x, 1)
                    dy = np.diff(y, 1)
                    dxdt = dx / dt
                    dydt = dy / dt

                xoff = np.zeros(ntimes)
                yoff = np.zeros(ntimes)
                xoff_good = 0.0
                yoff_good = 0.0
                # extrapolate derivatives, check for gaps
                i_lo = 0
                for i in range(ntimes - 1):
                    #
                    # Get derivative and extrapolate (original scheme)
                    i_up = i
                    if not dolookahead:
                        if dx[i] >= np.pi:
                            dx[i] = dx[i] - 2.0 * np.pi
                            dxdt[i] = dx[i] / dt[i]
                        elif dx[i] < -np.pi:
                            dx[i] = dx[i] + 2.0 * np.pi
                            dxdt[i] = dx[i] / dt[i]
                        if dy[i] >= np.pi:
                            dy[i] = dy[i] - 2.0 * np.pi
                            dydt[i] = dy[i] / dt[i]
                        elif dy[i] < -np.pi:
                            dy[i] = dy[i] + 2.0 * np.pi
                            dydt[i] = dy[i] / dt[i]
                        if dt[i] <= 1.0:
                            # 1sec or less gap to next time, use derivative
                            xoff[i] = dxdt[i] * timeoffset[0]
                            yoff[i] = dydt[i] * timeoffset[1]
                            xoff_good = xoff[i]
                            yoff_good = yoff[i]
                        else:
                            # more than 1sec gap to the next time, found end of a scan
                            # use best previous offset
                            xoff[i] = xoff_good
                            yoff[i] = yoff_good
                            i_lo = i + 1
                    else:
                        # lookahead a number of samples closer to offset (new scheme)
                        if dt[i] > 1.0 or i == (ntimes - 2):
                            # more than 1sec gap to the next time, found end of a scan
                            for j in range(i_lo, i):
                                j_offset_x = min(i, j + integ_offset_x)
                                del_t_x = ptimes[j_offset_x] - ptimes[j]
                                del_x = x[j_offset_x] - x[j]
                                del_x_del_t = del_x / del_t_x
                                xoff[j] = del_x_del_t * timeoffset[0]
                                #
                                j_offset_y = min(i, j + integ_offset_y)
                                del_t_y = ptimes[j_offset_y] - ptimes[j]
                                del_y = y[j_offset_y] - y[j]
                                del_y_del_t = del_y / del_t_y
                                yoff[j] = del_y_del_t * timeoffset[1]
                                #
                                xoff_good = xoff[j]
                                yoff_good = yoff[j]
                            # final point in scan
                            xoff[i] = xoff_good
                            yoff[i] = yoff_good
                            #
                            i_lo = i + 1
                # final entry
                xoff[ntimes - 1] = xoff_good
                yoff[ntimes - 1] = yoff_good
                ###
                if (len(dirv_orig.shape) == 3):
                    targ_orig[0, 0, :] += xoff
                    targ_orig[1, 0, :] += yoff
                    dirv_orig[0, 0, :] += xoff
                    dirv_orig[1, 0, :] += yoff
                else:
                    targ_orig[0, :] += xoff
                    targ_orig[1, :] += yoff
                    dirv_orig[0, :] += xoff
                    dirv_orig[1, :] += yoff
                st.putcol(col, dirv_orig)
                if not dodirectiononly:
                    st.putcol(tcol, targ_orig)
                #
                mad_xoff = np.median(np.absolute(xoff))
                mad_yoff = np.median(np.absolute(yoff))
                logstr = 'AntID %i : MAD CORRECTIONS AZ=%.3f EL=%.3f arcmin' % (
                ant, mad_xoff * r2d * 60.0, mad_yoff * r2d * 60.0)
                LOG.info(logstr)
                st.close()
        #
        if dofilter:
            n_sigma_thresh = 5.0
            n_sigma_thresh_ddiff = math.sqrt(1.5) * n_sigma_thresh
            n_sigma_thresh_diff = math.sqrt(2.0) * 2.0 * n_sigma_thresh
            max_rate_degpersec = 5.0
            max_rate = max_rate_degpersec * np.pi / 180.0
            # for all ants filter glitches in DIRECTION-TARGET
            for ant in ants:
                st = tb.query('ANTENNA_ID==' + str(ant))
                nrows = st.nrows()
                LOG.info('Ant %i glitch filtering nrows = %i' % (ant, nrows))
                # print(st.nrows())
                ptimes = st.getcol('TIME')
                dt = np.diff(ptimes, 1)
                ntimes = len(ptimes)

                targ_orig = st.getcol(tcol)
                dirv_orig = st.getcol(col)
                sh = dirv_orig.shape
                targ = targ_orig.reshape((sh[0], sh[len(sh) - 1]))
                dirv = dirv_orig.reshape((sh[0], sh[len(sh) - 1]))
                offs = dirv - targ

                offx_raw = offs[0, :]
                if offx_raw[0] >= np.pi:
                    offx_raw -= 2.0 * np.pi
                elif offx_raw[0] < -np.pi:
                    offx_raw += 2.0 * np.pi
                offx = np.unwrap(offx_raw)  # there seems to be a wrap issue in AZ
                offy = offs[1, :]

                mad_offx = np.median(np.absolute(offx))
                mad_offy = np.median(np.absolute(offy))
                logstr = 'AntID %i : MAD DIR-TARG AZ=%.3f EL=%.3f arcmin' % (
                ant, mad_offx * r2d * 60.0, mad_offy * r2d * 60.0)
                LOG.info(logstr)
                madrms_offx = 1.4826 * mad_offx
                madrms_offy = 1.4826 * mad_offy
                #
                doffx = np.diff(offx, 1)
                doffy = np.diff(offy, 1)
                logstr = 'AntID %i : will correct differences > AZ=%.3f EL=%.3f arcmin' % (
                ant, mad_offx * n_sigma_thresh * r2d * 60.0, mad_offy * n_sigma_thresh * r2d * 60.0)
                LOG.info(logstr)

                nfilter = 0
                nxfilter = 0
                nyfilter = 0
                # find scan breaks
                i_lo = 0
                for i in range(ntimes - 1):
                    # Filter
                    ixfilter = 0
                    iyfilter = 0
                    if dt[i] <= 1.0:
                        # 1sec or less gap to next time, use derivative
                        if i > i_lo:
                            # check if value discrepant from average of neighbors
                            # f_i - 0.5(f_i-1 + f_i+1) = 0.5*( off[i-1]-off[i])
                            ddiffx = 0.5 * abs(doffx[i - 1] - doffx[i])
                            if ddiffx >= n_sigma_thresh_ddiff * madrms_offx:
                                # deemed a "glitch", use average
                                offx[i] = 0.5 * (offx[i - 1] + offx[i + 1])
                                doffx[i - 1] = offx[i] + offx[i - 1]
                                doffx[i] = offx[i + 1] + offx[i]
                                ixfilter += 1
                            ddiffy = 0.5 * abs(doffy[i - 1] - doffy[i])
                            if ddiffy >= n_sigma_thresh_ddiff * madrms_offy:
                                # deemed a "glitch", use average
                                offy[i] = 0.5 * (offy[i - 1] + offy[i + 1])
                                doffy[i - 1] = offy[i] + offy[i - 1]
                                doffy[i] = offy[i + 1] + offy[i]
                                iyfilter += 1
                            max_rate_x = max_rate * dt[i - 1]  # prob shouldnt modify by cos(el)
                            if abs(doffx[i - 1]) > min(max_rate_x, n_sigma_thresh_diff * madrms_offx):
                                # check for unphysical changes in single diff
                                # choose the least discrepant
                                if abs(offx[i - 1]) < abs(offx[i]):
                                    offx[i] = offx[i - 1]
                                    doffx[i - 1] = 0.0
                                    ixfilter += 1
                                elif abs(offx[i]) < abs(offx[i - 1]):
                                    offx[i - 1] = offx[i]
                                    doffx[i - 1] = 0.0
                                    ixfilter += 1
                            max_rate_y = max_rate * dt[i - 1]
                            if abs(doffy[i - 1]) > min(max_rate_y, n_sigma_thresh_diff * madrms_offy):
                                # check for unphysical changes in single diff
                                # choose the least discrepant
                                if abs(offy[i - 1]) < abs(offy[i]):
                                    offy[i] = offy[i - 1]
                                    doffy[i - 1] = 0.0
                                    iyfilter += 1
                                elif abs(offy[i]) < abs(offy[i - 1]):
                                    offy[i - 1] = offy[i]
                                    doffy[i - 1] = 0.0
                                    iyfilter += 1
                                    #
                    else:
                        # more than 1sec gap to the next time, found end of a scan
                        if i > i_lo:
                            max_rate_x = max_rate * dt[i - 1]  # prob shouldnt modify by cos(el)
                            if abs(doffx[i - 1]) > min(max_rate_x, n_sigma_thresh_diff * madrms_offx):
                                # check for unphysical changes in single diff
                                # choose the least discrepant
                                if abs(offx[i - 1]) < abs(offx[i]):
                                    offx[i] = offx[i - 1]
                                    doffx[i - 1] = 0.0
                                    ixfilter += 1
                                elif abs(offx[i]) < abs(offx[i - 1]):
                                    offx[i - 1] = offx[i]
                                    doffx[i - 1] = 0.0
                                    ixfilter += 1
                            max_rate_y = max_rate * dt[i - 1]
                            if abs(doffy[i - 1]) > min(max_rate_y, n_sigma_thresh_diff * madrms_offy):
                                # check for unphysical changes in single diff
                                # choose the least discrepant
                                if abs(offy[i - 1]) < abs(offy[i]):
                                    offy[i] = offy[i - 1]
                                    doffy[i - 1] = 0.0
                                    iyfilter += 1
                                elif abs(offy[i]) < abs(offy[i - 1]):
                                    offy[i - 1] = offy[i]
                                    doffy[i - 1] = 0.0
                                    iyfilter += 1
                        i_lo = i + 1
                    #
                    # update counters for this i
                    if ixfilter > 0:
                        nxfilter += 1
                    if iyfilter > 0:
                        nyfilter += 1
                    if ixfilter > 0 or iyfilter > 0:
                        nfilter += 1

                # Final entry
                ixfilter = 0
                iyfilter = 0
                i = ntimes - 1
                if i > i_lo:
                    max_rate_x = max_rate * dt[i - 1]  # prob shouldnt modify by cos(el)
                    if abs(doffx[i - 1]) > min(max_rate_x, n_sigma_thresh_diff * madrms_offx):
                        # check for unphysical changes in single diff
                        # choose the least discrepant
                        if abs(offx[i - 1]) < abs(offx[i]):
                            offx[i] = offx[i - 1]
                            doffx[i - 1] = 0.0
                            ixfilter += 1
                        elif abs(offx[i]) < abs(offx[i - 1]):
                            offx[i - 1] = offx[i]
                            doffx[i - 1] = 0.0
                            ixfilter += 1
                    max_rate_y = max_rate * dt[i - 1]
                    if abs(doffy[i - 1]) > min(max_rate_y, n_sigma_thresh_diff * madrms_offy):
                        # check for unphysical changes in single diff
                        # choose the least discrepant
                        if abs(offy[i - 1]) < abs(offy[i]):
                            offy[i] = offy[i - 1]
                            doffy[i - 1] = 0.0
                            iyfilter += 1
                        elif abs(offy[i]) < abs(offy[i - 1]):
                            offy[i - 1] = offy[i]
                            doffy[i - 1] = 0.0
                            iyfilter += 1
                if ixfilter > 0:
                    nxfilter += 1
                if iyfilter > 0:
                    nyfilter += 1
                if ixfilter > 0 or iyfilter > 0:
                    nfilter += 1
                #
                LOG.info('Ant %i glitch filtering modified N events: tot=%i x=%i y=%i' % (ant, nfilter, nxfilter, nyfilter))
                ###
                # Put filtered DIRECTION back in POINTING table
                dirvx = targ[0, :] + offx
                dirvy = targ[1, :] + offy
                if (len(dirv_orig.shape) == 3):
                    dirv_orig[0, 0, :] = dirvx
                    dirv_orig[1, 0, :] = dirvy
                else:
                    dirv_orig[0, :] = dirvx
                    dirv_orig[1, :] = dirvy
                st.putcol(col, dirv_orig)
                #
                # Done with this ant
                st.close()
                #
                # Done with all ants

        if usetarget:
            # for all ants replace DIRECTION with TARGET
            for ant in ants:
                st = tb.query('ANTENNA_ID==' + str(ant))
                nrows = st.nrows()
                LOG.info('Ant %i replacing DIRECTION with TARGET for nrows = %i' % (ant, nrows))
                targ_orig = st.getcol(tcol)
                dirv_orig = st.getcol(col)
                st.putcol(col, targ_orig)
                #
                if dointerval:
                    LOG.info('Ant %i setting interval to -1 for nrows = %i' % (ant, nrows))
                    inter = st.getcol('INTERVAL')
                    inter[:] = -1.0
                    st.putcol('INTERVAL', inter)
                st.close()
        elif dointerval:
            for ant in ants:
                st = tb.query('ANTENNA_ID==' + str(ant))
                nrows = st.nrows()
                LOG.info('Ant %i setting interval to -1 for nrows = %i' % (ant, nrows))
                inter = st.getcol('INTERVAL')
                inter[:] = -1.0
                st.putcol('INTERVAL', inter)
                st.close()

        # tb.done()


class FixpointingResults(basetask.Results):
    def __init__(self):
        super(FixpointingResults, self).__init__()
        self.pipeline_casa_task = 'Fixpointing'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'FixpointingResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'FixpointingResults:'


class FixpointingInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hifv_fixpointing')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Fixpointing(basetask.StandardTaskTemplate):
    Inputs = FixpointingInputs

    def prepare(self):
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        obs_start = self.inputs.context.observing_run.start_datetime
        obs_end = self.inputs.context.observing_run.end_datetime

        # For VLASS1.1 the dates with sets of old ACUs are (from VLASS Memo 12):

        # 9/7/17 - 10/23/17
        # ea03, ea04, ea05, ea06, ea07, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27, ea28

        # 10/23/17 - 2/7/18
        # ea03, ea04, ea05, ea06, ea07, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27

        # 2/7/18 - 2/20/18
        # ea03, ea04, ea05, ea06, ea09, ea10, ea11, ea12, ea13, ea15, ea16, ea18, ea19, ea20, ea22, ea23, ea24, ea25, ea26, ea27

        antnames = []
        antlist = []
        if datetime(2017, 9, 7, 0, 0, 0) < obs_start <= datetime(2017, 10, 23, 0, 0, 0):
            antnames = ['ea03', 'ea04', 'ea05', 'ea06', 'ea07', 'ea09', 'ea10', 'ea11', 'ea12', 'ea13', 'ea15', 'ea16',
                        'ea18', 'ea19', 'ea20', 'ea22', 'ea23', 'ea24', 'ea25', 'ea26', 'ea27', 'ea28']

        if datetime(2017, 10, 23, 0, 0, 0) < obs_start <= datetime(2018, 2, 7, 0, 0, 0):
            antnames = ['ea03', 'ea04', 'ea05', 'ea06', 'ea07', 'ea09', 'ea10', 'ea11', 'ea12', 'ea13', 'ea15', 'ea16',
                        'ea18', 'ea19', 'ea20', 'ea22', 'ea23', 'ea24', 'ea25', 'ea26', 'ea27']

        if datetime(2018, 2, 7, 0, 0, 0) < obs_start <= datetime(2018, 2, 20, 0, 0, 0):
            antnames = ['ea03', 'ea04', 'ea05', 'ea06', 'ea09', 'ea10', 'ea11', 'ea12', 'ea13', 'ea15', 'ea16', 'ea18',
                        'ea19', 'ea20', 'ea22', 'ea23', 'ea24', 'ea25', 'ea26', 'ea27']

        if antnames != []:
            antobjectslist = m.get_antenna()

            # index identifiers for VLA antennas
            antlist = [antenna.id for antenna in antobjectslist if antenna.name in antnames]
        # Example translation of antenna names to index numbers
        # antnames = ['ea03', 'ea04', 'ea05', 'ea06', 'ea09', 'ea10', 'ea11', 'ea12', 'ea13', 'ea15', 'ea16', 'ea18',
        #                'ea19', 'ea20', 'ea22', 'ea23', 'ea24', 'ea25', 'ea26', 'ea27']
        # antlist = [2,3,4,5,6,8,9,10,11,13,14,16,17,18,20,21,22,23,24,25]
        fixpointing_offset_vlass(self.inputs.vis, antlist=antlist)

        return FixpointingResults()

    def analyse(self, results):
        return results



