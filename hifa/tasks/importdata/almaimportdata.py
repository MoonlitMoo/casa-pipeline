import os
import ssl
import urllib

import certifi

import pipeline.h.tasks.importdata.fluxes as fluxes
import pipeline.h.tasks.importdata.importdata as importdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

from . import dbfluxes

__all__ = [
    'ALMAImportData',
    'SerialALMAImportData',
    'ALMAImportDataInputs',
    'ALMAImportDataResults'
]

LOG = infrastructure.get_logger(__name__)

try:
    FLUX_SERVICE_URL = os.environ['FLUX_SERVICE_URL']
except Exception as e:
    FLUX_SERVICE_URL = ''
    # FLUX_SERVICE_URL = 'https://2019jul.asa-test.alma.cl/sc/flux'

try:
    FLUX_SERVICE_URL_BACKUP = os.environ['FLUX_SERVICE_URL_BACKUP']
    # 'https://2019jul.asa-test.alma.cl/sc/flux'
except Exception as e:
    FLUX_SERVICE_URL_BACKUP = ''


class ALMAImportDataInputs(importdata.ImportDataInputs):
    asis = vdp.VisDependentProperty(default='Annotation Antenna CalAtmosphere CalPointing CalWVR ExecBlock Receiver SBSummary Source Station')
    dbservice = vdp.VisDependentProperty(default=False)
    createmms = vdp.VisDependentProperty(default='false')
    # sets threshold for polcal parallactic angle coverage. See PIPE-597
    minparang = vdp.VisDependentProperty(default=0.0)
    parallel = sessionutils.parallel_inputs_impl(default=False)

    def __init__(self, context, vis=None, output_dir=None, asis=None, process_caldevice=None, session=None,
                 overwrite=None, nocopy=None, bdfflags=None, lazy=None, save_flagonline=None, dbservice=None,
                 createmms=None, ocorr_mode=None, datacolumns=None, minparang=None, parallel=None):
        super().__init__(context, vis=vis, output_dir=output_dir, asis=asis,
                         process_caldevice=process_caldevice, session=session,
                         overwrite=overwrite, nocopy=nocopy, bdfflags=bdfflags, lazy=lazy,
                         save_flagonline=save_flagonline, createmms=createmms,
                         ocorr_mode=ocorr_mode, datacolumns=datacolumns)
        self.dbservice = dbservice
        self.minparang = minparang
        self.parallel = parallel


class ALMAImportDataResults(importdata.ImportDataResults):
    def __init__(self, mses=None, setjy_results=None):
        super().__init__(mses=mses, setjy_results=setjy_results)
        self.parang_ranges = {}

    def __repr__(self):
        return 'ALMAImportDataResults:\n\t{0}'.format(
            '\n\t'.join([ms.name for ms in self.mses]))


class SerialALMAImportData(importdata.ImportData):
    Inputs = ALMAImportDataInputs
    Results = ALMAImportDataResults

    def _get_fluxes(self, context, observing_run):
        # get the flux measurements from Source.xml for each MS

        if self.inputs.dbservice:
            testquery = '?DATE=27-March-2013&FREQUENCY=86837309056.169219970703125&WEIGHTED=true&RESULT=1&NAME=J1427-4206&VERBOSE=1'
            # Test for service response
            baseurl = FLUX_SERVICE_URL
            url = baseurl + testquery
            if baseurl == '':
                url = ''

            try:
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                LOG.info('Attempting test query at: {!s}'.format(url))
                response = urllib.request.urlopen(url, context=ssl_context, timeout=60.0)
                xml_results, qastatus = dbfluxes.get_setjy_results(observing_run.measurement_sets)
                fluxservice = 'FIRSTURL'
            except Exception as e:
                try:
                    LOG.warning('Unable to execute initial test query with primary flux service.')
                    ssl_context = ssl.create_default_context(cafile=certifi.where())
                    baseurl = FLUX_SERVICE_URL_BACKUP
                    url = baseurl + testquery
                    if baseurl == '':
                        url = ''
                    LOG.info('Attempting test query at backup: {!s}'.format(url))
                    response = urllib.request.urlopen(url, context=ssl_context, timeout=60.0)
                    xml_results, qastatus = dbfluxes.get_setjy_results(observing_run.measurement_sets)
                    fluxservice='BACKUPURL'
                except Exception as e2:
                    if url == '':
                        msg = 'Backup URL not defined for test query...'
                    else:
                        msg = 'Unable to execute backup test query with flux service.'
                    LOG.warning(msg+'\nProceeding without using the online flux catalog service.')
                    xml_results = fluxes.get_setjy_results(observing_run.measurement_sets)
                    fluxservice = 'FAIL'
                    qastatus = None
        else:
            xml_results = fluxes.get_setjy_results(observing_run.measurement_sets)
            fluxservice = None
            qastatus = None
        # write/append them to flux.csv

        # Cycle 1 hack for exporting the field intents to the CSV file:
        # export_flux_from_result queries the context, so we pseudo-register
        # the mses with the context by replacing the original observing run
        orig_observing_run = context.observing_run
        context.observing_run = observing_run
        try:
            fluxes.export_flux_from_result(xml_results, context)
        finally:
            context.observing_run = orig_observing_run

        # re-read from flux.csv, which will include any user-coded values
        combined_results = fluxes.import_flux(context.output_dir, observing_run)

        return fluxservice, combined_results, qastatus


@task_registry.set_equivalent_casa_task('hifa_importdata')
@task_registry.set_casa_commands_comment('If required, ASDMs are converted to MeasurementSets.')
class ALMAImportData(sessionutils.ParallelTemplate):
    """ALMAImportData class for parallelization."""

    Inputs = ALMAImportDataInputs
    Task = SerialALMAImportData
