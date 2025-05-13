import bz2
import itertools
import math
import pickle
import re

import numpy as np
from casarecipes.almahelpers import tsysspwmap
from casatools import atmosphere as attool
from casatools import measures as metool
from casatools import ms as mstool
from casatools import msmetadata as msmdtool
from casatools import quanta as qatool
from casatools import table as tbtool

import pipeline.infrastructure.logging

LOG = pipeline.infrastructure.logging.get_logger(__name__)


ms = mstool()
msmd = msmdtool()
tb = tbtool()

# ALMA Bands  0       1     2    3    4     5     6     7     8    9    10
usePWV = [99.0, 5.186, 5.186, 2.748, 2.748, 1.796, 1.262, 0.913, 0.658, 0.472, 0.472]
WEATHER_DATA_GROUPING = 5  # 5 s grouping

VERSION = 3.4


def getband(freq):
    """Identify the Band for specific frequency (in GHz)"""
    lo = np.array([35, 67, 84, 125, 157, 211, 275, 385, 602, 787]) * 1e9
    hi = np.array([50, 84, 116, 163, 212, 275, 373, 500, 720, 950]) * 1e9

    return np.arange(1, len(lo) + 1)[(freq > lo) & (freq < hi)][0]


def ATMtrans(msfile, scans, spws, ifld=None):
    results = dict()
    assert ms.open(msfile), f"Could not open ms file {msfile}"
    assert msmd.open(msfile), f"Could not open metadata from {msfile}"
    # print(msmd.phasecenter(0))
    # return None
    # results = dict()
    # pwvf = pwv_function(msfile)
    fields = []
    for iscan in scans:
        # Find the field if none supplied
        # print(f'iscan={iscan}')
        if ifld is None:
            # Find all fields associated with this scan
            fld = msmd.fieldsforscan(iscan)
            # print(f'### {ifld}')
            # If there is more than 1 field, it is a mosaic. Take the central field value, hoping
            # that it is near the center...
            if len(fld) > 1:
                fld = int(math.floor(np.median(fld)))
            else:
                fld = int(fld[0])
            fields.append(fld)

    fields_set = set(fields)
    mydirections_dict = dict((fld, msmd.phasecenter(fld)) for fld in fields_set)
    mydirections_ra = [mydirections_dict[ifld]["m0"] for ifld in fields]
    mydirections_dec = [mydirections_dict[ifld]["m1"] for ifld in fields]
    myscantimes = [
        np.median(msmd.timesforscan(iscan)) for iscan in scans
    ]  # MJD in seconds

    myme = metool()
    observatory = "ALMA"
    myme.doframe(
        myme.observatory(observatory)
    )  # will not throw an exception if observatory not recognized
    myqa = qatool()
    # mydirections_q = []
    frame = "AZEL"
    myazels = []
    for ra, dec, mjd in zip(mydirections_ra, mydirections_dec, myscantimes):
        raQuantity = myqa.quantity(ra["value"], ra["unit"])
        decQuantity = myqa.quantity(dec["value"], dec["unit"])
        mydir = myme.direction("ICRS", raQuantity, decQuantity)
        myme.doframe(myme.epoch("mjd", myqa.quantity(mjd, "s")))
        myazel = myme.measure(mydir, frame)
        myaz = myazel["m0"]["value"]  # *180/np.pi
        myel = myazel["m1"]["value"]  # *180/np.pi
        myazels.append([myaz, myel])

    myme.done()
    # myqa.done()
    airmasses = [1.0 / np.cos(np.pi / 2 - azel[1]) for azel in myazels]

    # Fixed parameters adopted from Todd's aU and plotbandpass3 code
    # - could make options later but probably not required for what we need this function to do
    dP = 5.0
    dPm = 1.1
    maxAltitude = 60.0
    h0 = 1.0
    atmType = 1
    nbands = 1
    telescopeName = "ALMA"

    _st = np.array(myscantimes)
    wf, _wfdata = weather_function(msfile)
    # myqa = qatool()
    myat = attool()
    spwInfo = ms.getspectralwindowinfo()
    bandFreq = spwInfo[str(next(iter(spws)))]["Chan1Freq"]  # v2.5
    band = int(getband(bandFreq))
    # assert False, f'{band} {bandFreq}'

    for iscan, pwv, P, H, T, airmass in zip(
        scans,
        wf["pwv"](_st),
        wf["pressure"](_st),
        wf["humidity"](_st),
        wf["temperature"](_st),
        airmasses,
    ):
        T += 273.15  # Kelvin
        # Sometimes weather values passed are zeros, even if going through the full weather code.
        # In those cases, set to the default values.
        pwv = pwv if pwv > 0 else usePWV[band]
        P = P if P > 0 else 563.0
        T = T if T > 0 else 273.15
        H = H if H > 0 else 20

        # if P == 0:
        #     P = 563.0
        # if H == 0:
        #     H = 20.0
        # if T == 0:
        #     T = 273.15
        # if iscan!= 15: continue
        # print(pwv, P, H, T, airmass)

        ATMresult = myat.initAtmProfile(
            humidity=H,
            temperature=myqa.quantity(T, "K"),
            altitude=myqa.quantity(5059, "m"),
            pressure=myqa.quantity(P, "mbar"),
            atmType=atmType,
            h0=myqa.quantity(h0, "km"),
            maxAltitude=myqa.quantity(maxAltitude, "km"),
            dP=myqa.quantity(dP, "mbar"),
            dPm=dPm,
        )

        # CASA 5 vs 6 check
        ATMresult = ATMresult[0] if type(ATMresult) == tuple else ATMresult

        for ispw in spws:
            freqs = msmd.chanfreqs(ispw, "GHz")
            numchan = msmd.nchan(ispw)
            reffreq = 0.5 * (freqs[int(numchan / 2) - 1] + freqs[int(numchan / 2)])

            # Gather frequency information into the form we'll need to make the model.
            # For Bands 9 and 10, the spectral window also has atmospheric contributions from the
            # image sideband which we must also calculate.
            if band in [9, 10]:
                nbands = 2
                freqs_SB, chansep_SB, center_SB, width_SB = getImageSBFreqs(
                    msfile, ispw
                )
                fCenter = myqa.quantity([reffreq, center_SB], "GHz")
                chansep = (freqs[-1] - freqs[0]) / (numchan - 1)
                fResolution = myqa.quantity([chansep, -chansep], "GHz")
                fWidth = myqa.quantity([numchan * chansep, numchan * -chansep], "GHz")
            else:
                fCenter = myqa.quantity(reffreq, "GHz")
                chansep = (freqs[-1] - freqs[0]) / (numchan - 1)
                fResolution = myqa.quantity(chansep, "GHz")
                fWidth = myqa.quantity(numchan * chansep, "GHz")

            # Setup the CASA atmosphere tool and generate the model.
            # Note this is more or less from inside Todd's aU of CalcAtmTransmission
            # from Plotbandpass3.py code

            # This sets the frequency information and the PWV measurement
            myat.initSpectralWindow(nbands, fCenter, fWidth, fResolution)
            myat.setUserWH2O(myqa.quantity(pwv, "m"))  # in meters yet
            # Now calculate the model based on inputs provided above and get the transmission out
            dry = np.array(myat.getDryOpacitySpec(0)[1])  # O3, etc.
            wet = np.array(myat.getWetOpacitySpec(0)[1]["value"])  # water absorption
            # assert False, dry[100]
            transmission = np.exp(-airmass * (wet + dry))  # e^-tau;
            if band in [9, 10]:
                dry_SB = np.array(myat.getDryOpacitySpec(1)[1])
                wet_SB = np.array(myat.getWetOpacitySpec(1)[1]["value"])
                transmission_SB = np.exp(-airmass * (wet_SB + dry_SB))

            # Close the tools

            if band in [9, 10]:
                # results[f'{ispw}_{iscan}'] = np.c_[np.array(transmission), np.array(transmission_SB)]
                results[f"{ispw}_{iscan}"] = (
                    np.array(transmission) + np.array(transmission_SB)
                ) / 2  # v2.5
            else:
                results[f"{ispw}_{iscan}"] = np.array(transmission)
        # break

    myat.close()
    myqa.done()
    ms.close()
    msmd.close()

    return results


def weather_function(msfile):
    # Use a standard preferred weather station (as noted in AU task).
    preferredStation = "TB2"
    conditions = dict()
    # conditions['pwv'] = _pwv_function(msfile)
    # PWV data
    pwv = []
    try:
        tb.open(msfile + "/ASDM_CALWVR")
        pwvtime = tb.getcol("startValidTime")  # mjdsec
        assert len(pwvtime) > 0, "no valid data in table ASDM_CALWVR"
        antenna = tb.getcol("antennaName")
        nant = len(set(list(antenna)))
        assert nant > 0, "no valid data in table ASDM_CALWVR"
        pwv = tb.getcol("water")
        tb.close()
        assert len(pwv) % nant == 0, "weather_function: incompatible dimensions"
        ntimes = int(len(pwv) / nant)
        pwvtime = np.median(pwvtime.reshape(ntimes, nant), axis=1)
        if np.all(pwv == 1.0):
            LOG.info("weather_function: PWV all ones, no valid data")
            pwv *= 0
        pwv = np.median(pwv.reshape(ntimes, nant), axis=1)
        conditions["pwv"] = lambda tt: np.interp(tt, pwvtime, pwv)
    except RuntimeError:
        LOG.warning(
            "This measurement set does not have or has problems with ASDM_CALWVR table. Try the option asis='SBSummary ExecBlock Annotation Antenna Station Receiver Source CalAtmosphere CalWVR CalPointing' in importasdm"
        )
        raise RuntimeError()
    except AssertionError:
        LOG.info("no valid data in table ASDM_CALWVR. Trying ASDM_CALATMOSPHERE.")

    if len(pwv) <= 0:
        try:
            tb.open(msfile + "/ASDM_CALATMOSPHERE")
            pwvtime = tb.getcol("startValidTime")  # mjdsec
            antenna = tb.getcol("antennaName")
            pwv = tb.getcol("water")[
                0
            ]  # There seem to be 2 identical entries per row, so take first one.
            tb.close()
            conditions["pwv"] = lambda tt: np.interp(tt, pwvtime, pwv)
        except RuntimeError:
            LOG.warning(
                "This measurement set does not have an ASDM_CALATMOSPHERE table. Try the option asis='SBSummary ExecBlock Annotation Antenna Station Receiver Source CalAtmosphere CalWVR CalPointing' in importasdm"
            )
            raise RuntimeError()

    # Get the weather table.
    try:
        tb.open(msfile + "/WEATHER")
    except RuntimeError:
        LOG.info("Could not open the WEATHER table for this ms, default returned.")
        conditions["pressure"] = lambda tt: 563.0
        conditions["temperature"] = lambda tt: 0.0  # in deg C
        conditions["humidity"] = lambda tt: 20.0
        # return(conditions)

    # Get all weather information.
    mjdsec = tb.getcol("TIME")
    indices = np.argsort(mjdsec)  # sometimes slightly out of order, fix.
    pressure = tb.getcol("PRESSURE")
    relativeHumidity = tb.getcol("REL_HUMIDITY")
    temperature = tb.getcol("TEMPERATURE")
    # If in units of Kelvin, convert to C
    if np.mean(temperature) > 100:
        temperature = temperature - 273.15

    # Apply correct ordering
    mjdsec = np.array(mjdsec)[indices]
    pressure = np.array(pressure)[indices]
    # assert False, pressure
    relativeHumidity = np.array(relativeHumidity)[indices]
    temperature = np.array(temperature)[indices]
    # Grab weather station IDs.
    if "NS_WX_STATION_ID" in tb.colnames():
        stations = tb.getcol("NS_WX_STATION_ID")
    else:
        stations = None
    tb.close()

    prefix = ["WSTB", "Meteo", "OSF"]  # Do not know why
    asdmStation = msfile + "/ASDM_STATION"
    wsdict = {}
    try:
        assert tb.open(asdmStation), "Could not open asdmStation. "
        station_names = tb.getcol("name")

        for i, name in enumerate(station_names):
            for p in prefix:
                #            print "Checking if %s contains %s" % (name.lower(),p.lower())
                if name.lower().find(p.lower()) >= 0:
                    wsdict[i] = name
        tb.close()
    except RuntimeError:
        LOG.warning(
            "This measurement set does not have an ASDM_STATION table. Try the option asis='SBSummary ExecBlock Annotation Antenna Station Receiver Source CalAtmosphere CalWVR CalPointing' in importasdm"
        )
        raise RuntimeError()

    # Get the weather station names.

    preferredStationID = None
    # Loop over weather stations, searching for the preferred.
    for w in list(wsdict.keys()):
        if wsdict[w].find(preferredStation) >= 0:
            preferredStationID = w
    # If preferred found, use only data from that one, otherwise use all.
    # assert False, preferredStationID
    if preferredStationID is not None:
        indices = np.where(stations == preferredStationID)
        mjdsec = np.array(mjdsec)[indices]
        pressure = np.array(pressure)[indices]
        relativeHumidity = np.array(relativeHumidity)[indices]
        temperature = np.array(temperature)[indices]
        stations = np.array(stations)[indices]

    # clean stations with invalid data
    # assert False, stations
    # stations  = stations[pressure>0]
    pgt0 = pressure > 0
    try:
        assert np.any(
            pgt0
        ), "weather_function: No valid weather data! Returning default conditions."
    except AssertionError:
        conditions["pressure"] = lambda tt: 563.0
        conditions["temperature"] = lambda tt: 0.0  # in deg C
        conditions["humidity"] = lambda tt: 20.0
        # return conditions

    mjdsec = mjdsec[pgt0]
    temperature = temperature[pgt0]
    relativeHumidity = relativeHumidity[pgt0]
    pressure = pressure[pgt0]
    stations = stations[pgt0]
    # assert False, pressure
    # sys.exit(0)
    nstations = len(set(list(stations)))
    # delta_t = np.quantile(np.diff(mjdsec),(1-1/len(mjdsec))/nstations)
    data_weather = {
        "pressure": pressure,
        "humidity": relativeHumidity,
        "temperature": temperature,
        "mjdsec": mjdsec,
    }
    # quantile = np.quantile(np.diff(mjdsec),(1-1/len(mjdsec))/nstations)

    split_idx = np.split(
        np.arange(mjdsec.shape[0]),
        np.where(np.diff(mjdsec) > WEATHER_DATA_GROUPING)[0] + 1,
    )

    for k, v in data_weather.items():
        data_weather[k] = np.array([np.nanmean(v[_]) for _ in split_idx])

    for k in set(data_weather.keys()) - set(["mjdsec"]):
        # One needs to be careful with lambda functions. They re-evaluate calls to the variables used
        # one way to avoid this is by using the default valua as below.
        conditions[k] = lambda tt, times=data_weather["mjdsec"], vals=data_weather[
            k
        ]: np.interp(tt, times, vals)

    return conditions, data_weather


# # AL added - PIPE 1168 (2)


def getImageSBFreqs(msfile, spwin):
    """
    Purpose:
        Given the spectral window, find the LO frequency and return the frequencies of
        the image sideband. Specifically, return the inputs needed for ATMtrans() in
        order to properly show the atmospheric transmission for bands 9 and 10 where the
        image sideband atomospheric window also contributes to the spectral window.

    Inputs:
        spwin : int
            The spectral window for which the image sideband frequencies are needed.

    Outputs:
        fSB : numpy.array
            The frequencies of the image sideband. These will be properly arranged
            (i.e. they will be opposite in direction to the input spectral window
            frequencies).

        chansepSB : float
            The separation between channels (channel width).

        fCenterSB : float
            The mean frequency (center) of the image sideband.

        fWidthSB : float
            The total bandwidth of the image sideband (will be equal to the input
            spectral window).

    Note: This has been taken from Todd Hunter's AU tools, specifically au.interpretLOs,
          au.getLOs, and plotbandpass3.py - CalcAtmTranmission.
    """
    # Get the information we need from the MS table ASDM_RECEIVER which will give us
    # the spw numbers, the LOs, and the "names" (more like an intent) of the spws.

    tb.open(msfile + "/ASDM_RECEIVER")
    numLO = tb.getcol("numLO")
    freqLO = []
    spws = []
    names = []
    for i in range(len(numLO)):
        spw = int((tb.getcell("spectralWindowId", i).split("_")[1]))
        if spw not in spws:
            spws.append(spw)
            freqLO.append(tb.getcell("freqLO", i))
            names.append(tb.getcell("name", i))
    tb.close()

    # We want to ignore the superfluous WVR windows and find the right index for our
    # input spectral window.
    sawWVR = False
    indices = []
    for i in range(len(spws)):
        if names[i].find("WVR") >= 0:
            if not sawWVR:
                indices.append(i)
                sawWVR = True
        else:
            indices.append(i)

    # This is quite clever (taken from Todd's bandpass3), the LO is the frequency that
    # is exactly between the spw and the image sideband. Therefore,
    #   2*(spw_freq - LO_freq) - spw_freq
    #   2 * spw_freq - 2 * LO_freq - spw_freq
    #   spw_freq - 2 * LO_freq
    # this results in each channel getting the correctly matched image sideband frequency
    # such that the array counts in the right direction which is opposite the input spectral
    # window.
    mymsmd = msmdtool()
    mymsmd.open(msfile)
    fSB = np.array(2 * freqLO[indices[spwin]][0] - mymsmd.chanfreqs(spwin)) * 1e-9
    mymsmd.close()
    fCenterSB = np.mean(fSB)
    chansepSB = (fSB[-1] - fSB[0]) / (len(fSB) - 1)
    fWidthSB = chansepSB * len(fSB)

    return fSB, chansepSB, fCenterSB, fWidthSB


def chanfreq_records_to_functions(chan_records):
    ch_frq = chan_records
    freqMHz = dict()
    chan = dict()
    numchannels = dict()
    for recn in ch_frq.keys():
        chf = ch_frq[recn][:, 0]
        spw = int(recn.replace("r", "")) - 1
        f0, f1, nchan = [chf[0], chf[-1], chf.shape[0]]
        freqMHz[spw] = (
            lambda ch, f0=f0, f1=f1, nchan=nchan: (f0 + ch * (f1 - f0) / nchan) / 1e6
        )
        chan[spw] = (
            lambda fMHz, f0=f0, f1=f1, nchan=nchan: (fMHz * 1e6 - f0)
            / (f1 - f0)
            * nchan
        )
        # chansizeMHz[spw] = ((f1-f0)/1.0e6/nchan)
        numchannels[spw] = nchan
    return freqMHz, chan, numchannels


class TsysData(object):
    def __init__(
        self,
        tsystable=None,
        oldobject=None,
        load_pickle=False,
        single_polarization=False,
    ):
        self.tsystable = tsystable
        if tsystable is None:
            return None
        if oldobject is not None and tsystable == oldobject.tsystable:
            self.msfile = oldobject.msfile
            self.tsysfields = oldobject.tsysfields
            self.tsysdata = oldobject.tsysdata
            self.specfields = oldobject.specfields
            self.specdata = oldobject.specdata
            self.absorptionfields = oldobject.absorptionfields
            self.absorptiondata = oldobject.absorptiondata
            self.inversetsysmap = oldobject.inversetsysmap
            self.ch_frq = oldobject.ch_frq
            self.msspec = oldobject.msspec
            return None
        if load_pickle:
            file = f"{self.tsystable}.tsysdata.pbz2"
            try:
                with bz2.BZ2File(file, "rb") as reader:
                    data_dict = pickle.load(reader)
                    assert (
                        "VERSION" in data_dict
                    ), "No version information in the picke file!"
                    assert (
                        data_dict["VERSION"] >= VERSION
                    ), f"Version of pickle is {data_dict['VERSION']} < {VERSION}, which is the version of the script."
                self.msfile = data_dict["msfile"]
                self.tsysfields = data_dict["tsysfields"]
                self.tsysdata = data_dict["tsysdata"]
                self.specfields = data_dict["specfields"]
                self.specdata = data_dict["specdata"]
                self.absorptionfields = data_dict["absorptionfields"]
                self.absorptiondata = data_dict["absorptiondata"]
                self.inversetsysmap = data_dict["inversetsysmap"]
                self.ch_frq = data_dict["ch_frq"]
                freqMHz, chan, numchannels = chanfreq_records_to_functions(
                    data_dict["ch_frq"]
                )
                self.msspec = dict(
                    (
                        (spw, (freqMHz[spw], chan[spw], numchannels[spw]))
                        for spw in freqMHz.keys()
                    )
                )  # v2.4
                return
            except OSError:
                LOG.info(f"Could not load {file}.")

        m = re.match(r"(uid.+)\.ms\S+", tsystable)
        assert m, "Could not extract ms file name from tsystable:{tsystable}"
        msfile, asdm = m.group(1) + ".ms", f"../rawdata/{m.group(1)}"
        self.msfile = msfile

        assert tb.open(f"{tsystable}/ANTENNA"), f"Could not open {tsystable}/ANTENNA"
        ant_names = tb.getcol("NAME")
        self.antnames = ant_names  # v2.4
        assert tb.close(), f"Could not close {tsystable}/ANTENNA."
        freqMHz = dict()
        # chansizeMHz = dict()
        chan = dict()
        numchannels = dict()
        assert tb.open(
            f"{tsystable}/SPECTRAL_WINDOW"
        ), f"Could not open {tsystable}/SPECTRAL_WINDOW"
        ch_frq = tb.getvarcol("CHAN_FREQ")
        tb.close()

        self.ch_frq = ch_frq
        freqMHz, chan, numchannels = chanfreq_records_to_functions(ch_frq)
        self.msspec = dict(
            (
                (spw, (freqMHz[spw], chan[spw], numchannels[spw]))
                for spw in freqMHz.keys()
            )
        )  # v2.4

        assert tb.open(tsystable), f"Could not open {tsystable}"
        ant1, scans, spws, fields = (
            tb.getcol("ANTENNA1"),
            tb.getcol("SCAN_NUMBER"),
            tb.getcol("SPECTRAL_WINDOW_ID"),
            tb.getcol("FIELD_ID"),
        )
        fparam = tb.getvarcol("FPARAM")
        tsysflag = tb.getvarcol("FLAG")
        nrows = tb.nrows()
        assert tb.close(), f"Could not close {tsystable}"
        if single_polarization:
            fparam = dict(((k, v[0:1]) for k, v in fparam.items()))
            tsysflag = dict(((k, v[0:1]) for k, v in tsysflag.items()))

        assert msmd.open(msfile), f"Could not open metadata for {msfile}"
        self.tsysmap = TsysData.tsysmap(
            vis=msfile, tsystable=tsystable, msmdtool_instance=msmd
        )  # v2.4
        auxi = dict()
        for k, v in self.tsysmap.items():
            auxi.setdefault(v, []).append(k)
        self.inversetsysmap = auxi.copy()

        intent = dict()
        for scan in set(list(scans)):
            # This is a bit tricky. We need all intents on the source _name_ associated with the scan. Necessary for mosaics.
            fieldnumbers_for_fieldname = msmd.fieldsforname(
                msmd.fieldnames()[msmd.fieldsforscan(scan)[0]]
            )
            all_intents = []
            for f in fieldnumbers_for_fieldname:
                all_intents += msmd.intentsforfield(f)
            _ii = []
            # print(all_intents)
            if "CALIBRATE_BANDPASS#ON_SOURCE" in all_intents:
                _ii.append("bandpass")
            if "CALIBRATE_PHASE#ON_SOURCE" in all_intents:
                _ii.append("phasecal")
            if "OBSERVE_TARGET#ON_SOURCE" in all_intents:
                _ii.append("science")
            if len(_ii) == 0:
                _ii = ["other"]
            intent[scan] = ",".join(_ii)
        msmd.done()
        # break

        atmos_transmission = ATMtrans(
            msfile=msfile, scans=set(list(scans)), spws=set(list(spws))
        )
        assert np.all(
            np.array([np.all(_ <= 1) for _ in atmos_transmission.values()])
        ), "Not all transmission curves <=1! Something is wrong!"

        flags = []
        tsys = []
        intents = []

        for recn, antenna, scan, spw in zip(range(1, nrows + 1), ant1, scans, spws):
            flagspec = np.sum(tsysflag[f"r{recn}"][:, :, 0], axis=0) > 0
            spec = fparam[f"r{recn}"][
                :, :, 0
            ]  # np.mean(fparam[f'r{recn}'][:,:,0], axis=0) # here the polarization average occurs.
            spec[:, flagspec] = np.nan  # same flags per polarization are ensured.
            flags.append(flagspec)
            tsys.append(spec)
            intents.append(intent[scan])

        self.tsysfields = ["spw", "scan", "intent", "field", "antenna", "flags", "tsys"]
        self.tsysdata = np.array(
            [
                list(spws),
                list(scans),
                intents,
                list(fields),
                list(ant_names[ant1]),
                flags,
                tsys,
            ],
            dtype=object,
        )

        spws_list = list(set(list(spws)))
        scans_list = list(set(list(scans)))

        freq_mhz = []
        nchans = []
        for spw in spws_list:
            chans = np.arange(numchannels[spw])
            freq_mhz.append(freqMHz[spw](chans))
            nchans.append(numchannels[spw])

        self.specfields = ["spw", "freq_mhz", "nchan"]
        self.specdata = np.array(
            [spws_list, freq_mhz, nchans], dtype=object
        )  # v2.4 nchan -> nchans seems. like a bug.

        absorptions = []
        scans = []
        spws = []
        for spw in spws_list:
            for scan in scans_list:
                scans.append(scan)
                spws.append(spw)
                absorptions.append(atmos_transmission[f"{spw}_{scan}"])

        self.absorptionfields = ["spw", "scan", "absorption"]
        self.absorptiondata = np.array([spws, scans, absorptions], dtype=object)

    def tsysmap(
        vis, tsystable, msmdtool_instance, intent="OBSERVE_TARGET#ON_SOURCE"
    ):  # v2.4, this function
        _tsysmap = np.array(tsysspwmap(vis=vis, tsystable=tsystable))
        mymsmd = msmdtool_instance
        # mymsmd.open(vis)
        allIntents = mymsmd.intents()
        if intent not in allIntents and intent != "":
            for i in allIntents:
                if i.find(intent) >= 0:
                    intent = i
                    LOG.info("Translated intent to ", i)
                    break

        value = [i.find(intent.replace("*", "")) for i in allIntents]
        # If any intent gives a match, the mean value of the location list will be > -1
        if np.mean(value) > -1:
            spws = mymsmd.spwsforintent(intent)
            almaspws = mymsmd.almaspws(tdm=True, fdm=True, sqld=False)
            # mymsmd.close()
            if len(spws) == 0 or len(almaspws) == 0:
                scienceSpws = []
            else:
                scienceSpws = np.intersect1d(spws, almaspws)
            return dict(
                (
                    (sspw, tspw)
                    for sspw, tspw in zip(
                        list(scienceSpws), list(_tsysmap[scienceSpws])
                    )
                )
            )
            # return(scienceSpws, _tsysmap)
        else:
            # mymsmd.close()
            raise IntentException(
                "%s not found in this dataset. Available intents: " % (intent),
                allIntents,
            )

    class IntentException(Exception):  # v2.4
        pass

    def get_valid_antennae(self):
        valid_dict = dict()
        exclude = set()
        ##
        [antennas, spws, scans, fields, flags, intents, tsyss] = [
            self.tsysdata[self.tsysfields.index(k)]
            for k in ("antenna", "spw", "scan", "field", "flags", "intent", "tsys")
        ]
        valid = set(
            itertools.product(set(antennas), set(spws), set(scans), set(fields))
        )
        for antenna, spw, scan, field in valid:
            selection = np.nonzero(
                (antennas == antenna)
                * (spws == spw)
                * (scans == scan)
                * (fields == field)
            )[0]
            if len(selection) <= 0:
                continue
            assert (
                len(selection) == 1
            ), f"Selection antenna={antenna},spw={spw},scan={scan},field={field} has more than 1 match!"
            selection = selection[0]

            flag = flags[selection]
            intent = intents[selection]
            key = f"{spw}_{field}_{scan}_{antenna}_{intent}"

            if np.all(flag):
                exclude = exclude.union(set([key]))
            else:
                valid_dict[key] = tsyss[selection]
        # valid = valid - exclude
        return valid_dict, exclude

    def _partition_by_equivalence_class(a, equiv):
        partitions = []  # Found partitions
        for e in a:  # Loop over each element
            found = False  # Note it is not yet part of a know partition
            for p in partitions:
                if equiv(e, next(iter(p))):  # Found a partition for it!
                    p.add(e)
                    found = True
                    break
            if not found:  # Make a new partition for it.
                partitions.append(set([e]))
        return partitions

    def _equiv(x, y, numeric_fields=3):
        mx = re.match(r"^(" + "[0-9]+_" * numeric_fields + ").*", x)
        my = re.match(r"^(" + "[0-9]+_" * numeric_fields + ").*", y)
        if not (mx and my):
            return False
        return mx.group(1) == my.group(1)

    def _combine_difference(minuend_dict, subtrahend_dict):
        minuend_keys = list(minuend_dict.keys())
        subtrahend_keys = list(subtrahend_dict.keys())
        k = minuend_keys[0]
        m = re.match(
            r"(?P<spw>\d+)_(?P<field>\d+)_(?P<scan>\d+)_(?P<antenna>.+)_(?P<intent>.+)",
            k,
        )
        assert m, f"_combine_difference: no match for key k={k}"
        spw, field = m.group("spw", "field")
        differences = dict()
        # antennae = []
        for mi, su in itertools.product(minuend_keys, subtrahend_keys):
            m = re.match(
                rf"{spw}_{field}_(?P<scan>\d+)_(?P<antenna>.+)_(?P<intent>.+)", mi
            )
            assert m, f"_combine_difference: no match for key mi={mi}"
            scan_mi, antenna_mi = m.group("scan", "antenna")
            m = re.match(rf"{spw}_\d+_(?P<scan>\d+)_(?P<antenna>.+)_(?P<intent>.+)", su)
            if not m:
                continue
            # assert m, f"_combine_difference: no match for key su={su}"
            scan_su, antenna_su = m.group("scan", "antenna")
            if not antenna_mi == antenna_su:
                continue
            differences[f"{spw}_{scan_mi}-{scan_su}_{antenna_mi}"] = (
                minuend_dict[mi] - subtrahend_dict[su]
            )
            # assert not antenna_mi in antennae, f"_combine_difference: Repeated antenna in difference calculation.\n--{minuend_keys}\n--{subtrahend_keys}"
            # antennae.append(antenna_mi)

        return differences  # , antennae

    def filter_statistics_and_dif(
        self, statistics, remove_n_extreme=1, polarizations=None
    ):
        valid, invalid = self.get_valid_antennae()
        valid_keys = set(list(valid.keys()))
        fequiv = lambda x, y: TsysData._equiv(x, y, numeric_fields=2)
        partitioned_keys_by_field = TsysData._partition_by_equivalence_class(
            valid_keys, fequiv
        )

        intents = set([re.match(r".*_([^_]+)$", vk).group(1) for vk in valid_keys])
        assert "bandpass" in intents, "filter_average_dif: No 'bandpass' in intents"
        intents = intents - {"bandpass"}
        partitioned_keys_source, partitioned_keys_bp = [], []
        bpdict = dict()
        while partitioned_keys_by_field:
            keyset = partitioned_keys_by_field.pop()
            if "bandpass" in next(iter(keyset)):
                partitioned_keys_bp.append(keyset)
                bpdict.update(dict([(k, valid[k]) for k in keyset]))
            else:
                partitioned_keys_source.append(keyset)

        stats_dif = dict()
        stats_sou = dict()
        stats_bp = dict()
        stats_antenna = dict()
        # assert False, partitioned_keys_source
        for keyset_s in partitioned_keys_source:
            # print(keyset_s)
            m = re.match(
                r"(?P<spw>\d+)_(?P<field>\d+)_(?P<scan>\d+)_(?P<antenna>.+)_(?P<intent>.+)",
                next(iter(keyset_s)),
            )
            spw, field, intent_sou = m.group("spw", "field", "intent")
            sourcedict = dict([(k, valid[k]) for k in keyset_s])
            bpdict_spw = dict(
                [(k, v) for k, v in bpdict.items() if re.match(rf"^{spw}_.*", k)]
            )

            diff = TsysData._combine_difference(
                minuend_dict=sourcedict, subtrahend_dict=bpdict_spw
            )

            m = re.match(
                r"(?P<spw>\d+)_(?P<field>\d+)_(?P<scan>\d+)_(?P<antenna>.+)_(?P<intent>.+)",
                next(iter(bpdict_spw.keys())),
            )
            field_bp, scan_bp = m.group("field", "scan")
            diffkeyslist = list(diff.keys()).copy()
            # print(spw,field,intent_sou, diffkeyslist)

            m = re.match(
                r"(?P<spw>\d+)_(?P<scan_sou>\d+)-(?P<scan_bp>\d+)_(?P<antenna>.+)",
                diffkeyslist[0],
            )
            assert m, f"{diffkeyslist[0]}"

            key = f"{spw}_{field}"
            # if key != '24_3': continue
            auxi_dif = np.c_[[diff[k] for k in diffkeyslist]]
            if len(auxi_dif) <= 2 * remove_n_extreme:
                stats_dif[key] = None
                stats_sou[key] = None
                stats_bp[key] = None
                stats_antenna[key] = None
                continue
            # print(diffkeyslist)
            # print(f'{np.nanmax(auxi_dif)}'); assert False, ""
            auxiliar = (
                auxi_dif - np.nanmean(auxi_dif, axis=(1, 2))[:, np.newaxis, np.newaxis]
            )
            order_max = np.argsort(
                np.nanmax(auxiliar, axis=(1, 2))
            )  # np.argsort(np.nanmax(auxi_dif-np.nanmean(auxi_dif,axis=1)[:,np.newaxis],axis=1))
            order_min = np.argsort(
                np.nanmin(auxiliar, axis=(1, 2))
            )  # np.argsort(np.nanmin(auxi_dif-np.nanmean(auxi_dif,axis=1)[:,np.newaxis],axis=1))
            selection = np.array(
                list(
                    set(range(auxi_dif.shape[0]))
                    - set(list(order_max[-remove_n_extreme:]))
                    - set(list(order_min[0:remove_n_extreme]))
                )
            )
            # selection = np.array(list(set(list(selection)) - set(list(selection[-remove_n_extreme:]))))
            auxi_dif_antenna = np.r_[
                [
                    re.match(rf"^{spw}_.*_(?P<antenna>.+)$", k).group("antenna")
                    for k in diffkeyslist
                ]
            ]

            # selection = np.nonzero((order_negative<len(order_negative)-remove_n_extreme) * (order_positive <len(order)-remove_n_extreme))[0] # a bit of robustness against bad data.
            # print(set(list(selection)))
            antenna_selection = auxi_dif_antenna[selection]
            auxi_sou = np.c_[
                [
                    valid[
                        re.sub(
                            rf"^{spw}_(\d+)-{scan_bp}_([^_]+)$",
                            rf"{spw}_{field}_\1_\2_{intent_sou}",
                            k,
                        )
                    ]
                    for k in diffkeyslist
                ]
            ]
            auxi_bp = np.c_[
                [
                    bpdict[
                        re.sub(
                            rf"^{spw}_(\d+)-{scan_bp}_([^_]+)$",
                            rf"{spw}_{field_bp}_{scan_bp}_\2_bandpass",
                            k,
                        )
                    ]
                    for k in diffkeyslist
                ]
            ]
            auxi_dif = auxi_dif[selection]
            auxi_sou = auxi_sou[selection]
            auxi_bp = auxi_bp[selection]

            stats_dif[key] = statistics(auxi_dif, axis=0)
            stats_sou[key] = statistics(auxi_sou, axis=0)
            stats_bp[key] = statistics(auxi_bp, axis=0)
            stats_antenna[key] = antenna_selection
        # print(f'*** {np.sort(stats_antenna["24_3"])} {np.nanmax(stats_sou["24_3"])}');assert False,""
        return stats_dif, stats_sou, stats_bp, stats_antenna
