import certifi
import collections
import datetime
import decimal
import os
import ssl
import urllib
from xml.dom import minidom
from xml.parsers.expat import ExpatError

import pipeline.domain as domain
import pipeline.domain.measures as measures
import pipeline.h.tasks.common.commonfluxresults as commonfluxresults
import pipeline.h.tasks.importdata.fluxes as fluxes
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)

try:
    FLUX_SERVICE_URL = os.environ['FLUX_SERVICE_URL']
    if FLUX_SERVICE_URL == '':
        LOG.info('Environment variable FLUX_SERVICE_URL not defined.  Switching to backup url.')
    else:
        LOG.info('Using ALMA flux service URL: {!s}'.format(FLUX_SERVICE_URL))
except Exception as e:
    LOG.info('Environment variable FLUX_SERVICE_URL not defined.  Switching to backup url.')
    FLUX_SERVICE_URL = ''
    # FLUX_SERVICE_URL = 'https://almascience.eso.org/sc/flux'
    # FLUX_SERVICE_URL = 'https://osf-sourcecat-2019jul.asa-test.alma.cl/sc/'

try:
    FLUX_SERVICE_URL_BACKUP = os.environ['FLUX_SERVICE_URL_BACKUP']
    if FLUX_SERVICE_URL_BACKUP == '':
        LOG.info('Environment variable FLUX_SERVICE_URL_BACKUP not defined.')
    else:
        LOG.info('Backup URL defined at: {!s}'.format(FLUX_SERVICE_URL_BACKUP))
    # 'https://2019jul.asa-test.alma.cl/sc/flux'
except Exception as e:
    LOG.info('Environment variable FLUX_SERVICE_URL_BACKUP not defined.')
    FLUX_SERVICE_URL_BACKUP = ''

ORIGIN_DB = 'DB'


def get_setjy_results(mses):
    """
    Get flux values from the database service reverting to the Source
    tables XML for backup values and store the values in the context
    """
    results = []
    qastatus = []
    for ms in mses:
        result = commonfluxresults.FluxCalibrationResults(ms.name)
        science_spw_ids = {spw.id for spw in ms.get_spectral_windows()}

        fluxdb_results, qacodes = read_fluxes_db(ms)

        for source, measurements in fluxdb_results.items():
            m = [m for m in measurements if int(m.spw_id) in science_spw_ids]

            # import flux values for all fields and intents so that we can
            # compare them to the fluxscale-derived values later in the run
            #            for field in [f for f in source.fields if 'AMPLITUDE' in f.intents]:
            for field in source.fields:
                result.measurements[field.id].extend(m)

        results.append(result)
        if qacodes:
            qastatus.extend(qacodes)

    return results, qastatus


def read_fluxes_db(ms):
    """
    Read fluxes from the database server, defaulting to the Source XML table
    if no fluxes can be found
    """
    xml_measurements = fluxes.read_fluxes_nodb(ms)

    if not xml_measurements:
        # Source.xml could not be read or parsed. Fall back to catalogue query
        return flux_nosourcexml(ms), None

    results, qacodes = add_catalogue_fluxes(xml_measurements, ms)

    return results, qacodes


def flux_nosourcexml(ms):
    """
    Call the flux service and get the frequencies from the ms if no Source.xml is available
    """
    result = collections.defaultdict(list)

    for source in ms.sources:
        for spw in ms.get_spectral_windows(science_windows_only=True):
            m = query_online_catalogue(ms, spw, source)
            if m:
                result[source].append(m)
                log_result(source, spw, 'N/A', m.I, m.spix, m.age)

    return result


def buildurl(service_url, obs_time, frequency, sourcename):
    # Example:
    # https://almascience.eso.org/sc/flux?DATE=10-August-2017&FREQUENCY=232101563000.0&NAME=J1924-2914&WEIGHTED=true&RESULT=1&CATALOGUE=5
    # New Example May 2019:
    # https://osf-sourcecat-2019apr.asa-test.alma.cl/sc/flux?DATE=27-March-2013&FREQUENCY=86837309056.169219970703125&WEIGHTED=true&RESULT=0&NAME=J1427-4206
    date = '{!s}-{!s}-{!s}'.format(str(obs_time.day).zfill(2), obs_time.strftime('%B'), obs_time.year)
    sourcename = sanitize_string(sourcename)
    urlparams = buildparams(sourcename, date, frequency)

    url = '{!s}?{!s}'.format(service_url, urlparams)
    LOG.debug("SC URL: {}".format(url))

    return url


def fluxservice(service_url, obs_time, frequency, sourcename):
    """
    Usage of this online service requires:
        - service_url - url for the db service
        - obs_time - for getting the date
        - frequency_text - we will get the frequency out of this in Hz
        - source - we will get source.name from this object
    """

    url = buildurl(service_url, obs_time, frequency, sourcename)
    LOG.info('Attempting query {!s}'.format(url))

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    try:
        response = urllib.request.urlopen(url, context=ssl_context, timeout=60.0)
    except IOError:
        LOG.warning('Problem contacting flux service at: <a href="{!s}">{!s}</a>'.format(url, url))
        raise

    try:
        dom = minidom.parse(response)
    except ExpatError:
        LOG.warning('Could not parse source catalogue response')
        raise

    rowdict = {}
    for node in dom.getElementsByTagName('TR'):
        row = node.getElementsByTagName('TD')
        rl = len(row)
        rowdict['statuscode'] = row[0].childNodes[0].nodeValue
        if rl == 13:
            try:
                rowdict['clarification'] = row[1].childNodes[0].nodeValue
            except IndexError:
                rowdict['clarification'] = None
        else:
            rowdict['clarification'] = None
        rowdict['sourcename'] = row[rl-11].childNodes[0].nodeValue
        rowdict['dbfrequency'] = row[rl-10].childNodes[0].nodeValue
        rowdict['date'] = row[rl-9].childNodes[0].nodeValue
        rowdict['fluxdensity'] = row[rl-8].childNodes[0].nodeValue
        rowdict['fluxdensityerror'] = row[rl-7].childNodes[0].nodeValue
        rowdict['spectralindex'] = row[rl-6].childNodes[0].nodeValue
        rowdict['spectralindexerror'] = row[rl-5].childNodes[0].nodeValue
        rowdict['dataconditions'] = row[rl-4].childNodes[0].nodeValue
        rowdict['ageOfNearestMonitorPoint'] = row[rl-3].childNodes[0].nodeValue
        # rowdict['verbose'] = row[rl-2].childNodes[0].nodeValue
        rowdict['version'] = row[rl-1].childNodes[0].nodeValue
        rowdict['url'] = url

    return rowdict


def buildparams(name, date, frequency):
    """
    Inputs are all strings with the format:
    NAME=3c279&DATE=04-Apr-2014&FREQUENCY=231.435E9&WEIGHTED=true&RESULT=1&CATALOGUE=5
    """
    params = dict(NAME=name, DATE=date, FREQUENCY=frequency, WEIGHTED='true', RESULT=1, VERBOSE=1, CATALOGUE=5)
    return urllib.parse.urlencode(params)


def sanitize_string(name):
    """
    Sanitize source name if needed, taking first alias.
    """
    return name.split(';')[0]


def query_online_catalogue(flux_url, ms, spw, source):
    # At this point we take:
    #  - the source name string
    #  - the frequency of the spw_id in Hz
    #  - The observation date
    #  and attempt to call the online flux catalog web service, and use the flux result
    #  and spectral index
    source_name = source.name
    freq_hz = str(spw.centre_frequency.to_units(measures.FrequencyUnits.HERTZ))
    obs_time = utils.get_epoch_as_datetime(ms.start_time)

    LOG.info("Input source name: "+str(source_name)+"    Input SPW: "+str(spw.id))

    utcnow = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        fluxdict = fluxservice(flux_url, obs_time, freq_hz, source_name)
    except Exception as e:
        # error contacting service
        return flux_url, '0.0', '0', None, None, None

    try:
        cat_fd = float(fluxdict['fluxdensity'])
        cat_spix = float(fluxdict['spectralindex'])
    except Exception as e:
        # could not convert 'null' to number. Bad catalogue value.
        return flux_url, '0.0', fluxdict['statuscode'], None, fluxdict['clarification'], None

    valid_catalogue_val = cat_fd > 0.0 and cat_spix != -1000
    if not valid_catalogue_val:
        return flux_url, '0.0', fluxdict['statuscode'], None, fluxdict['clarification'], None

    final_I = measures.FluxDensity(cat_fd, measures.FluxDensityUnits.JANSKY)
    final_spix = decimal.Decimal('%0.3f' % cat_spix)
    age_n_m_p = fluxdict['ageOfNearestMonitorPoint']

    return fluxdict['url'], fluxdict['version'], fluxdict['statuscode'], fluxdict['dataconditions'], fluxdict['clarification'], \
           domain.FluxMeasurement(spw.id, final_I, spix=final_spix, origin=ORIGIN_DB, queried_at=utcnow, age=age_n_m_p)


def add_catalogue_fluxes(measurements, ms):
    results = collections.defaultdict(list)
    science_windows = ms.get_spectral_windows(science_windows_only=True)

    # Test query to see if we need to switch to the backup URL
    obs_time = datetime.datetime(2013, 3, 27, 7, 53, 3, 168000)
    freq_hz = '86837309056.169219970703125'
    source_name = 'J1427-4206'
    contact_fail = False
    backup_url = FLUX_SERVICE_URL_BACKUP
    # 'https://2019jul.asa-test.alma.cl/sc/flux'
    flux_url = FLUX_SERVICE_URL
    try:
        LOG.info("Test query...")
        fluxdict = fluxservice(flux_url, obs_time, freq_hz, source_name)
    except IOError:
        # error contacting service
        # LOG.warning("Could not contact the primary flux service at {!s}".format(flux_url))
        flux_url = backup_url
        contact_fail = True
    except ExpatError:
        # error parsing the XML table
        LOG.warning("Table parsing issue.")
        LOG.warning("Could not contact the primary flux service at {!s}".format(flux_url))
        flux_url = backup_url
        contact_fail = True

    if contact_fail:
        try:
            # Try the backup URL at JAO
            LOG.warning("Switching to backup url at: {!s}".format(flux_url))
            LOG.info("Test query...")
            fluxdict = fluxservice(flux_url, obs_time, freq_hz, source_name)
        except IOError:
            # LOG.error("Could not contact the backup flux service URL.")
            return results

    qacodes = []   # Dictionaries will be added here for codes and warning messages from the sources catalog

    # Continue with required queries
    for source, xml_measurements in measurements.items():
        for xml_measurement in xml_measurements:
            spw = ms.get_spectral_window(xml_measurement.spw_id)
            # LOG.info("SPW ID: "+str(spw.id))

            # only query database for science windows
            if spw not in science_windows:
                continue

            url, version, status_code, data_conditions, clarification, catalogue_measurement = query_online_catalogue(flux_url, ms, spw, source)

            if catalogue_measurement:
                # Catalogue doesn't return Q,U,V so adopt Q,U,V from XML
                catalogue_measurement.Q = xml_measurement.Q
                catalogue_measurement.U = xml_measurement.U
                catalogue_measurement.V = xml_measurement.V

                results[source].append(catalogue_measurement)

                # set text for logging statements
                catalogue_I = catalogue_measurement.I
                spix = catalogue_measurement.spix
                age = catalogue_measurement.age

            else:
                # No/invalid catalogue entry, so use Source.XML measurement

                results[source].append(xml_measurement)

                # set text for logging statements
                catalogue_I = 'N/A'
                spix = 'N/A'
                age = 'N/A'

            if clarification or int(status_code) > 1:
                qacodes.append({'source': source, 'status_code': status_code, 'clarification': clarification})

            log_result(source, spw, xml_measurement.I, catalogue_I, spix, age, url, version,
                       status_code, data_conditions, clarification)

    return results, qacodes


def log_result(source, spw, asdm_I, catalogue_I, spix, age, url, version, status_code, data_conditions, clarification):

    codedict = {}
    codedict[0] = "Grid cal flux estimation heuristic used"
    codedict[1] = "Low-cadence flux estimation heuristic used"
    codedict[2] = "Flux densities outside of the window were required to calculate an answer"
    codedict[3] = "Fallback algorithm used, went outside the window"
    codedict[4] = "No valid flux density could be calculated"

    # "dual-band data? " yes/no; "measurements bracketed in time? " yes/no.
    decision = {'0': 'No', '1': 'Yes'}

    LOG.info('Source: {!s} spw: {!s}    ASDM flux: {!s}    Catalogue flux: {!s}'.format(source.name, spw.id,
                                                                                        asdm_I, catalogue_I))
    LOG.info('         Online catalog Spectral Index: {!s}'.format(spix))
    LOG.info('         ageOfNearestMonitorPoint: {!s}'.format(age))
    LOG.info('         {!s}'.format(codedict[int(status_code)]))
    if data_conditions:
        LOG.info('         Number of measurements = {!s}'.format(str(data_conditions)[0]))
        LOG.info('         Dual-band data? {!s}'.format(decision[str(data_conditions)[1]]))
        LOG.info('         Measurements bracketed in time? {!s}'.format(decision[str(data_conditions)[2]]))
    else:
        LOG.info('         Number of measurements = {!s}'.format('N/A'))
        LOG.info('         Dual-band data? {!s}'.format('N/A'))
        LOG.info('         Measurements bracketed in time? {!s}'.format('N/A'))

    LOG.info('         URL: {!s}'.format(url))
    LOG.info('         Version: {!s}'.format(version))
    if clarification:
        LOG.info('         WARNING message returned: {!s}'.format(clarification))
    if catalogue_I == 'N/A':
        LOG.warning('         **No flux returned from the flux catalogue service.**')
    LOG.info("---------------------------------------------")
