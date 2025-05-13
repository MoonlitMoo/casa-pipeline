
import math

import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.h.tasks.common import commonresultobjects
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


def calculate_qa_numbers(result, result_wvrinfos, PHnoisy, BPnoisy):
    """Calculate a single number from the qa views stored in result.

    result -- The Qa2Result object containing the Qa2 views.
    """
    qa_per_view = {}

    for description in result.descriptions():
        qa_result = result.last(description)

        qa_data = qa_result.data
        qa_flag = qa_result.flag

        # qa score is no-wvr rms / with-wvr rms
        qa_per_view[description] = 1.0 / np.median(qa_data[qa_flag == False])

        # PIPE-1509
        # as we are looping the descriptions (i.e. BANDPASS and PHASE intents)
        # we can already assign the overall score for the
        # new clause, if PHnoisy==True
        if PHnoisy and 'BANDPASS' in description:
            result.overall_score = qa_per_view[description]

    result.view_score = qa_per_view

    # if the PHnoisy was not triggered 
    # we do the median as was done pre-PIPE-1509
    if not PHnoisy or result.overall_score == {}:
        result.overall_score = np.median(list(qa_per_view.values()))

    # Optional for PIPE-1509
    # check if disc and rms values from the WVR code are 
    # particularly high overall 

    # hard coded limits - could be inputs - but not exposed
    # PLWG to tweak later
    disc_max = 500.
    rms_max = 500.

    disc_list=[]
    rms_list=[]
    for WVRinfo in result_wvrinfos:
        disc_list.append(WVRinfo.disc.value)
        rms_list.append(WVRinfo.rms.value)

    # now just a remcloud trigger boolean - for tracking
    # read and warn in scorecalcualtor 
    suggest_remcloud = False
    if result.overall_score < 1.0:
        if np.median(disc_list) > disc_max or np.median(rms_list) > rms_max:
            suggest_remcloud = True 

    return suggest_remcloud


def calculate_view(context, nowvrtable, withwvrtable, result, qa_intent):

    # get phase rms results for no-wvr case and with-wvr case
    nowvr_results = calculate_phase_rms(context, nowvrtable, qa_intent)
    wvr_results = calculate_phase_rms(context, withwvrtable, qa_intent)

    # Limits hard coded to 90. - could be flexible stage inputs?
    # but probably not - i.e. people shouldn't mess with this heuristic 
    # - later PLWG might adjust separatly to tweak scores
    PHlim = 90.
    BPlim = 90.

    # Initial flag settings
    PHnoisy = False
    BPnoisy = False
    BPgood = False
    PHgood = False
    
    for k, v in wvr_results.items():
        result.vis = v.filename

        # the ratio withwvr/nowvr is the view we want
        nowvr_data = nowvr_results[k].data
        nowvr_flag = nowvr_results[k].flag
        wvr_data = wvr_results[k].data
        wvr_flag = wvr_results[k].flag

        oldseterr = np.seterr(divide='ignore', invalid='ignore') 
        data = wvr_data / nowvr_data
        data_flag = (nowvr_flag | wvr_flag | (nowvr_data == 0))
        data_flag[np.isinf(data)] = True
        data_flag[np.isnan(data)] = True
        np.seterr(**oldseterr) 

        # PIPE-1509
        ## as above code checked for flag when phase RMS was zero/nan etc
        ## use that to now make the phase RMS assessment 
        ## for only unflagged data and pass the 'noisy' parameters to result
        
        # use the 80th percentile - why:
        # 1) if the phase has pure noise all ants and scans will be > 100deg
        #    - this is designed to catch low SNR, not variable atmosphere
        #    -- this is used to decide if we use a combined PH and BP score or BP only
        # 2) for the bandpass, this will select the 'worse' antennas which
        #    will typically relate to longer baselines and we
        #    and the bandpass should have v.high SNR in phase soln.
        #    these are the 'longer' ones and hence 80th Percentile is good.
        #    This metric really tracks the varying atmopshere
        #    - this is used only in final score to say phase RMS is high
        #      due to real variability not low SNR
        # Due to one particualr problem dataset MOUS
        # the max BP RMS is 93, 80th percentile is 84.9
        # thus set limit to trigger if max > 90 and 80th % over 85
        # few other LB data tested have phase RMS < 50 but
        # wider data required - after tarball??

        perhi = np.percentile(wvr_data[data_flag==False], 80)
        perhi_nowvr = np.percentile(nowvr_data[data_flag==False], 80)
        bpmax = np.max(wvr_data[data_flag==False])
        if v.intent == 'PHASE' and perhi > PHlim:
             PHnoisy = True
        elif v.intent == 'BANDPASS' and perhi > (BPlim-5.) and bpmax > BPlim:
             BPnoisy = True

        # PIPE-1837
        # simply add a bool again for 'good' phase rms
        # define this as <1 radian over all the data averaged
        # here just return the values for both, do logic for score elsewhere
        LOG.info('phase rms is : '+str(perhi))
        if v.intent == 'PHASE' and perhi_nowvr < 57.1:
            PHgood = True
        elif v.intent == 'BANDPASS' and perhi_nowvr < 57.1:
            BPgood = True

        ############################

        axes = v.axes
        improvement_result = commonresultobjects.ImageResult(
          v.filename, data=data,
          datatype='with-wvr phase rms / no-wvr phase rms',
          axes=axes, flag=data_flag, intent=v.intent, spw=v.spw)

        result.addview(improvement_result.description, improvement_result)

    return PHnoisy, BPnoisy, PHgood, BPgood


def calculate_phase_rms(context, gaintable, qa_intent):
    """
    tsystable -- CalibrationTableData object giving access to the wvrg
                 caltable.
    """
    # raise an exception when np encounters any error
    olderr = np.seterr(all='raise')

    try:
        phase_rms_results = {}
        with casa_tools.TableReader(gaintable) as table:
            vis = table.getkeyword('MSName')

            # access field names via domain object
            ms = context.observing_run.get_ms(name=vis)

            # for each intent in the QA intent list, create a phase RMS result
            intent_list = set(qa_intent.split(','))
            for intent in intent_list:                
                # identify scans corresponding to this intent
                intent_scans = [scan.id for scan in ms.scans if intent in scan.intents]
                if len(intent_scans) <= 0:
                    continue

                sb = table.query('SCAN_NUMBER in %s' % str(intent_scans))
                spectral_window_id = sb.getcol('SPECTRAL_WINDOW_ID')
                spwids = sorted(set(spectral_window_id))
                antenna1 = sb.getcol('ANTENNA1')
                antennas = set(antenna1)
                timecol = sb.getcol('TIME')

                # Are there any times.
                times = sorted(set(timecol))
                if len(times) <= 0:
                    sb.done()
                    continue
                times = np.array(times)

                # look for continuously sampled chunks of data. Sometimes
                # there are small gaps of 10-15 sec within lengths of data
                # that we want to regard as continuous, so make the minimum
                # 'gap' larger than this.
                time_chunks = findchunks(times, gap_time=30.0)

                cparam = sb.getcol('CPARAM')
                flag = sb.getcol('FLAG')
                rows = np.arange(sb.nrows())

                # to eliminate the effect of the refant on the results 
                # (i.e. phase rms of refant is always 0) we first
                # construct 8 versions of the gain table with the entries
                # corresponding to 8 different values for the refant.
                cparam_refant = {}
                flag_refant = {}
                refants = list(antennas)[:8]
                for refant in refants:
                    cparam_refant[refant] = np.array(cparam)
                    flag_refant[refant] = np.array(flag)

                    for spwid in spwids:
                        for t in times:
                            # gains for all antennas at time t
                            selected_rows = rows[
                                (spectral_window_id == spwid) & (timecol == t)]
                            gain = cparam_refant[refant][0, 0, selected_rows]
                            # gain_flag = flag_refant[refant][0, 0, selected_rows]

                            # gain for refant at time t
                            refant_row = rows[(spectral_window_id == spwid)
                                              & (timecol == t)
                                              & (antenna1 == refant)]
                            if refant_row:
                                refant_gain = \
                                    cparam_refant[refant][0, 0, refant_row][0]
                                refant_gain_flag = \
                                    flag_refant[refant][0, 0, refant_row][0]
                            else:
                                refant_gain_flag = True

                            # now modify entries to make refant the reference
                            if not refant_gain_flag:
                                # use a.b = ab cos(theta) axb = ab sin(theta) to 
                                # calculate theta
                                dot_product = \
                                    gain.real * refant_gain.real \
                                    + gain.imag * refant_gain.imag
                                cross_product = \
                                    gain.real * refant_gain.imag \
                                    - gain.imag * refant_gain.real

                                complex_rel_phase = np.zeros([len(dot_product)], complex)
                                complex_rel_phase.imag = np.arctan2(
                                    cross_product, dot_product)

                                cparam_refant[refant][0, 0, selected_rows] = \
                                    np.abs(gain) * np.exp(-complex_rel_phase)
                            else:
                                flag_refant[refant][0, 0, selected_rows] = True

                # now calculate the phase rms views
                for spwid in spwids:
                    data = np.zeros([max(antennas)+1, len(time_chunks)])
                    data_flag = np.ones([max(antennas)+1, len(time_chunks)], bool)
                    chunk_base_times = np.zeros([len(time_chunks)])

                    for antenna in antennas:
                        selected_rows = rows[(spectral_window_id == spwid)
                                             & (antenna1 == antenna)]

                        gain_times = timecol[selected_rows]

                        for i, chunk in enumerate(time_chunks):
                            chunk_start = times[chunk[0]] - 0.1
                            chunk_end = times[chunk[-1]] + 0.1
                            chunk_select = \
                                (gain_times >= chunk_start)\
                                & (gain_times <= chunk_end)
                            if not chunk_select.any():
                                continue

                            gain_times_chunk = gain_times[chunk_select]
                            chunk_base_times[i] = gain_times_chunk[0]

                            rms_refant = np.zeros([8])
                            rms_refant_flag = np.ones([8], bool)

                            for refant in refants:
                                gain = cparam_refant[refant][0, 0, selected_rows]
                                gain_chunk = gain[chunk_select]
                                gain_flag = flag_refant[refant][0, 0, selected_rows]
                                gain_flag_chunk = gain_flag[chunk_select]

                                try:
                                    valid = np.logical_not(gain_flag_chunk)
                                    if not np.any(valid):
                                        continue

                                    # complex median: median(reals) + 
                                    # i*median(imags)
                                    cmedian = complex(
                                        np.median(gain_chunk.real[valid]),
                                        np.median(gain_chunk.imag[valid]))

                                    # use a.b = ab cos(theta) and 
                                    # axb = ab sin(theta) to calculate theta
                                    dot_product = \
                                        gain_chunk.real[valid] * cmedian.real\
                                        + gain_chunk.imag[valid] * cmedian.imag
                                    cross_product = \
                                        gain_chunk.real[valid] * cmedian.imag\
                                        - gain_chunk.imag[valid] * cmedian.real

                                    phases = \
                                        np.arctan2(cross_product, dot_product)\
                                        * 180.0 / math.pi

                                    # calculate phase rms
                                    phases *= phases
                                    phase_rms = np.sum(phases) / float(len(phases))
                                    phase_rms = math.sqrt(phase_rms)
                                    rms_refant[refant] = phase_rms
                                    rms_refant_flag[refant] = False
                                except:
                                    pass

                            # set view
                            valid_data = rms_refant[
                              np.logical_not(rms_refant_flag)]
                            if len(valid_data) > 0:
                                data[antenna, i] = np.median(valid_data)
                                data_flag[antenna, i] = False

                    axes = [
                        commonresultobjects.ResultAxis(
                            name='Antenna', units='id',
                            data=np.arange(max(antennas)+1)),
                        commonresultobjects.ResultAxis(
                            name='Time', units='', data=chunk_base_times)]

                    phase_rms_result = commonresultobjects.ImageResult(
                        filename=vis, data=data, datatype='r.m.s. phase',
                        axes=axes, flag=data_flag, intent=intent, spw=spwid,
                        units='degrees')
                    phase_rms_results[phase_rms_result.description] = \
                        phase_rms_result

                # free sub table and its resources
                sb.done()

            return phase_rms_results

    finally:
        np.seterr(**olderr)


def findchunks(times, gap_time):
    """Return a list of arrays, each containing the indices of a chunk
    of data i.e. a sequence of equally spaced measurements separated
    from other chunks by larger time gaps.

    Keyword arguments:
    times    -- Numeric array of times at which the measurements
                were taken.
    gap_time -- Minimum gap that signifies a 'gap'.
    """
    difference = times[1:] - times[:-1]
    median_diff = np.median(difference)
    gap_diff = max(1.5 * median_diff, gap_time)

    chunks = []
    chunk = [0]
    for i in np.arange(len(difference)):
        if difference[i] < gap_diff:
            chunk.append(i+1)
        else:
            chunks.append(np.array(chunk))
            chunk = [i+1]
    chunks.append(np.array(chunk))
    return chunks
