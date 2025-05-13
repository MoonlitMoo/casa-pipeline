import contextlib
import os
import shutil
import tarfile
import collections
from typing import List, Optional, Set

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain.datatype import DataType
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline import environment
from . import fluxes

__all__ = [
    'ImportData',
    'ImportDataInputs',
    'ImportDataResults'
]

LOG = infrastructure.get_logger(__name__)


class ImportDataInputs(vdp.StandardInputs):
    asis = vdp.VisDependentProperty(default='')
    bdfflags = vdp.VisDependentProperty(default=True)
    createmms = vdp.VisDependentProperty(default='automatic')
    datacolumns = vdp.VisDependentProperty(default={})
    lazy = vdp.VisDependentProperty(default=False)
    nocopy = vdp.VisDependentProperty(default=False)
    ocorr_mode = vdp.VisDependentProperty(default='ca')
    overwrite = vdp.VisDependentProperty(default=False)
    process_caldevice = vdp.VisDependentProperty(default=False)
    save_flagonline = vdp.VisDependentProperty(default=True)
    session = vdp.VisDependentProperty(default='session_1')

    def __init__(self, context, vis=None, output_dir=None, asis=None, process_caldevice=None, session=None,
                 overwrite=None, nocopy=None, save_flagonline=None, bdfflags=None, lazy=None, createmms=None,
                 ocorr_mode=None, datacolumns=None):
        super().__init__()

        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.asis = asis
        self.bdfflags = bdfflags
        self.createmms = createmms
        self.datacolumns = datacolumns
        self.lazy = lazy
        self.nocopy = nocopy
        self.ocorr_mode = ocorr_mode
        self.overwrite = overwrite
        self.process_caldevice = process_caldevice
        self.save_flagonline = save_flagonline
        self.session = session

    def to_casa_args(self):
        raise NotImplementedError


class ImportDataResults(basetask.Results):
    """
    ImportDataResults holds the results of the ImportData task. It contains
    the resulting MeasurementSet domain objects and optionally the additional
    SetJy results generated from flux entries in Source.xml.
    """

    def __init__(self, mses=None, setjy_results=None):
        super(ImportDataResults, self).__init__()
        self.mses = [] if mses is None else mses
        self.setjy_results = setjy_results
        self.origin = {}

        # Flux service query is None (dbservice=False), FIRSTURL, BACKUPURL, or FAIL
        self.fluxservice = None

    def merge_with_context(self, context):
        target = context.observing_run
        for ms in self.mses:
            LOG.info('Adding {0} to context'.format(ms.name))
            target.add_measurement_set(ms)

        if self.setjy_results:
            for result in self.setjy_results:
                result.merge_with_context(context)

    def __repr__(self):
        return 'ImportDataResults:\n\t{0}'.format(
            '\n\t'.join([ms.name for ms in self.mses]))


@task_registry.set_equivalent_casa_task('h_importdata')
@task_registry.set_casa_commands_comment('If required, ASDMs are converted to MeasurementSets.')
class ImportData(basetask.StandardTaskTemplate):
    Inputs = ImportDataInputs
    Results = ImportDataResults

    @staticmethod
    def _ms_directories(names):
        """
        Inspect a list of file entries, finding the root directory of any
        measurement sets present via a set of characteristic files and
        directories.
        """
        identifiers = ('SOURCE', 'FIELD', 'ANTENNA', 'DATA_DESCRIPTION')

        matching = [os.path.dirname(n) for n in names
                    if os.path.basename(n) in identifiers]

        return {m for m in matching if matching.count(m) == len(identifiers)}

    @staticmethod
    def _asdm_directories(members):
        """
        Inspect a list of file entries, finding the root directory of any
        ASDMs present via a set of characteristic files and directories.
        """
        identifiers = ('ASDMBinary', 'Main.xml', 'ASDM.xml', 'Antenna.xml')

        matching = [os.path.dirname(m) for m in members
                    if os.path.basename(m) in identifiers]

        return {m for m in matching if matching.count(m) == len(identifiers)}

    def prepare(self, **parameters):
        inputs = self.inputs
        abs_output_dir = os.path.abspath(inputs.output_dir)

        vis = inputs.vis

        if vis is None:
            msg = 'Empty input data set list'
            LOG.warning(msg)
            raise ValueError(msg)

        if not os.path.exists(vis):
            msg = 'Input data set \'{0}\' not found'.format(vis)
            LOG.error(msg)
            raise IOError(msg)

        results = self.Results()

        # if this is a tar, get the names of the files and directories inside
        # the tar and calculate which can be directly imported (filenames with
        # a measurement set fingerprint) and which must be converted (files
        # with an ASDM fingerprint).
        if os.path.isfile(vis) and tarfile.is_tarfile(vis):
            with contextlib.closing(tarfile.open(vis)) as tar:
                filenames = tar.getnames()

                (to_import, to_convert) = self._analyse_filenames(filenames, vis)

                to_convert = [os.path.join(abs_output_dir, asdm) for asdm in to_convert]
                to_import = [os.path.join(abs_output_dir, ms) for ms in to_import]

                LOG.info('Extracting %s to %s', vis, abs_output_dir)
                tar.extractall(path=abs_output_dir)

        # Assume that if vis is not a tar, it's a directory ready to be
        # imported, or in the case of an ASDM, converted then imported.
        else:
            # get a list of all the files in the given directory
            filenames = [os.path.join(vis, f) for f in os.listdir(vis)]

            (to_import, to_convert) = self._analyse_filenames(filenames, vis)

            if not to_import and not to_convert:
                raise TypeError('{!s} is of unhandled type'.format(vis))

            # convert all paths to absolute paths for the next sequence
            to_import = [os.path.abspath(f) for f in to_import]

            # if the file is not in the working directory, copy it across,
            # replacing the filename with the relocated filename
            to_copy = {f for f in to_import
                       if f.find(abs_output_dir) != 0
                       and inputs.nocopy is False}
            for src in to_copy:
                dst = os.path.join(abs_output_dir, os.path.basename(src))
                to_import.remove(src)
                to_import.append(dst)

                if os.path.exists(dst):
                    LOG.warning('{} already in {}. Will import existing data.'.format(os.path.basename(src), abs_output_dir))
                    continue

                LOG.info('Copying %s to %s', src, inputs.output_dir)
                shutil.copytree(src, dst)

        # launch an import job for each ASDM we need to convert
        for asdm in to_convert:
            self._do_importasdm(asdm)

        # calculate the filenames of the resultant measurement sets
        asdms = [os.path.join(abs_output_dir, f) for f in to_convert]

        # Now everything is in MS format, create a list of the MSes to import
        converted_asdms = [self._asdm_to_vis_filename(asdm) for asdm in asdms]
        to_import.extend(converted_asdms)

        # get the path to the MS for the converted ASDMs, which we'll later
        # compare to ms.name in order to calculate the origin of each MS
        converted_asdm_abspaths = [os.path.abspath(f) for f in converted_asdms]

        # PIPE-2006: for ALMA data in cycles 0-2, replace the [incorrect] coordinate
        # system name 'J2000' by 'ICRS' (supersedes PIPE-1575)
        for ms_path in to_import:
            self._rename_J2000_to_ICRS(ms_path)

        LOG.info('Creating pipeline objects for measurement set(s) {0}'
                 ''.format(', '.join(to_import)))

        ms_reader = tablereader.ObservingRunReader

        rel_to_import = [os.path.relpath(f, abs_output_dir) for f in to_import]

        observing_run = ms_reader.get_observing_run(rel_to_import)
        available_data_types = [v.name for v in DataType]
        short_data_types = list(set([v.replace('_ALL', '').replace('_SCIENCE', '')
                                     for v in available_data_types
                                     if v.endswith('_ALL') or v.endswith('_SCIENCE')]))
        data_type_entry = collections.namedtuple('DataTypeEntry', ('str_data_type enum_data_type'))
        for ms in observing_run.measurement_sets:
            LOG.debug(f'Setting session to {inputs.session} for {ms.basename}')

            ms_origin = 'ASDM' if ms.name in converted_asdm_abspaths else 'MS'

            datacolumn_name = get_datacolumn_name(ms.name)
            if datacolumn_name is None:
                msg = 'No data column found in {}'.format(ms.basename)
                LOG.error(msg)
                raise IOError(msg)

            correcteddatacolumn_name = get_correcteddatacolumn_name(ms.name)

            if inputs.datacolumns in (None, {}):
                data_types = {'DATA': data_type_entry('RAW', DataType.RAW)}
                if correcteddatacolumn_name is not None:
                    # Default to standard calibrated IF MS if the corrected data column is present
                    data_types['CORRECTED'] = data_type_entry('REGCAL_CONTLINE_ALL', DataType.REGCAL_CONTLINE_ALL)
            else:
                data_types = {}

                # Check inputs and parse any short data types
                if 'DATA' not in [k.upper() for k in inputs.datacolumns.keys()]:
                    msg = 'Must specify at least the data type for the DATA column'
                    LOG.error(msg)
                    raise ValueError(msg)

                for k, v in inputs.datacolumns.items():
                    if k.upper() not in ('DATA', 'CORRECTED'):
                        msg = f'Column name {k.upper()} is unknown. Only DATA and CORRECTED are supported.'
                        LOG.error(msg)
                        raise ValueError(msg)

                    if v.upper() in short_data_types:
                        if ms.intents == {'TARGET'}:
                            data_types[k.upper()] = data_type_entry(f'{v.upper()}_SCIENCE', DataType[f'{v.upper()}_SCIENCE'])
                        else:
                            data_types[k.upper()] = data_type_entry(f'{v.upper()}_ALL', DataType[f'{v.upper()}_ALL'])
                    elif v.upper() in available_data_types:
                        data_types[k.upper()] = data_type_entry(f'{v.upper()}', DataType[f'{v.upper()}'])
                    else:
                        msg = f'No such data type {v.upper()}'
                        LOG.error(msg)
                        raise ValueError(msg)

                if len(data_types) == 0:
                    msg = 'Must specify data type for at least one column'
                    LOG.error(msg)
                    raise ValueError(msg)
                if len(data_types) == 1:
                    if ms_origin == 'ASDM' and 'DATA' in data_types and data_types['DATA'].str_data_type != 'RAW':
                        msg = 'Data type for ASDMs can only be "RAW"'
                        LOG.error(msg)
                        raise ValueError(msg)
                elif len(data_types) == 2:
                    if ms_origin == 'ASDM':
                        msg = 'ASDMs only have a single raw data column'
                        LOG.error(msg)
                        raise ValueError(msg)
                    if correcteddatacolumn_name is None:
                        msg = 'Only one data column detected'
                        LOG.error(msg)
                        raise ValueError(msg)
                else:
                    msg = 'Maximum number of configurable data types is 2 (DATA and CORRECTED columns)'
                    LOG.error(msg)
                    raise ValueError(msg)

            # Set data_type for DATA and CORRECTED_DATA columns if specified
            if 'DATA' in data_types:
                LOG.info(f'Setting data type for data column of {ms.basename} to {data_types["DATA"].str_data_type}')
                ms.set_data_column(data_types['DATA'].enum_data_type, datacolumn_name)

            if 'CORRECTED' in data_types:
                ms.set_data_column(data_types['CORRECTED'].enum_data_type, correcteddatacolumn_name)
                LOG.info(f'Setting data type for corrected data column of {ms.basename} to {data_types["CORRECTED"].str_data_type}')

            ms.session = inputs.session
            results.origin[ms.basename] = ms_origin

        # Log IERS tables information (PIPE-734)
        LOG.info(environment.iers_info)

        fluxservice, combined_results, qastatus = self._get_fluxes(inputs.context, observing_run)

        results.mses.extend(observing_run.measurement_sets)
        results.setjy_results = combined_results
        results.fluxservice = fluxservice
        results.qastatus = qastatus

        return results

    def analyse(self, result):
        return result

    def _get_fluxes(self, context, observing_run):

        # get the flux measurements from Source.xml for each MS
        xml_results = fluxes.get_setjy_results(observing_run.measurement_sets)
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

        # Flux service not used, return None by default
        # QA flux service messaging, return None by default
        return None, combined_results, None

    def _analyse_filenames(self, filenames, vis) -> (Set[str], Set[str]):
        to_import = set()
        to_convert = set()

        ms_dirs = self._ms_directories(filenames)
        if ms_dirs:
            LOG.debug('Adding measurement set(s) {0} from {1} to import queue'
                      ''.format(', '.join([os.path.basename(f) for f in ms_dirs]),
                                vis))
            cleaned_paths = list(map(os.path.normpath, ms_dirs))
            to_import.update(cleaned_paths)

        asdm_dirs = self._asdm_directories(filenames)
        if asdm_dirs:
            LOG.debug('Adding ASDMs {0} from {1} to conversion queue'
                      ''.format(', '.join(asdm_dirs), vis))
            to_convert.update(asdm_dirs)

        return to_import, to_convert

    def _asdm_to_vis_filename(self, asdm):
        return '{0}.ms'.format(os.path.join(self.inputs.output_dir,
                                            os.path.basename(asdm)))

    def _do_importasdm(self, asdm):
        inputs = self.inputs

        if inputs.save_flagonline:
            # Create the standard calibration flagging template file
            template_flagsfile = os.path.join(inputs.output_dir, os.path.basename(asdm) + '.flagtemplate.txt')
            self._make_template_flagfile(template_flagsfile, 'User flagging commands file for the calibration pipeline')

            # Create the standard Tsys calibration flagging template file.
            template_flagsfile = os.path.join(inputs.output_dir, os.path.basename(asdm) + '.flagtsystemplate.txt')
            self._make_template_flagfile(template_flagsfile,
                                         'User Tsys flagging commands file for the calibration pipeline')

            # Create the imaging targets file
            template_flagsfile = os.path.join(inputs.output_dir, os.path.basename(asdm) + '.flagtargetstemplate.txt')
            self._make_template_flagfile(template_flagsfile, 'User flagging commands file for the imaging pipeline')

        # PIPE-1200: if the output MS already exists on disk and overwrite is
        # set to False, then skip the remaining steps of calling CASA's
        # importasdm (to avoid Exception) and copying over XML files, and
        # return early.
        vis = self._asdm_to_vis_filename(asdm)
        if os.path.exists(vis) and not inputs.overwrite:
            LOG.info(f"Skipping call to CASA 'importasdm' for ASDM {asdm}"
                     f" because output MS {os.path.basename(vis)} already"
                     f" exists in output directory"
                     f" {os.path.abspath(inputs.output_dir)}, and the input"
                     f" parameter 'overwrite' is set to False. Will import the"
                     f" existing MS data into the pipeline instead.")
            return

        # Derive input parameters for importasdm.
        # Set filename for saving flag commands.
        outfile = os.path.join(inputs.output_dir, os.path.basename(asdm) + '.flagonline.txt')
        # Decide whether to create an MMS based on requested mode and whether
        # MPI is available.
        createmms = mpihelpers.parse_mpi_input_parameter(inputs.createmms)
        # Set choice of whether to use pointing correction; try to retrieve
        # from inputs (e.g. set by hsd pipeline), but otherwise default to
        # False.
        with_pointing_correction = getattr(inputs, 'with_pointing_correction', False)

        # Create importasdm task.
        task = casa_tasks.importasdm(asdm=asdm,
                                     vis=vis,
                                     savecmds=inputs.save_flagonline,
                                     outfile=outfile,
                                     process_caldevice=inputs.process_caldevice,
                                     asis=inputs.asis,
                                     overwrite=inputs.overwrite,
                                     bdfflags=inputs.bdfflags,
                                     lazy=inputs.lazy,
                                     with_pointing_correction=with_pointing_correction,
                                     ocorr_mode=inputs.ocorr_mode,
                                     createmms=createmms)
        try:
            self._executor.execute(task)
        except Exception as ee:
            LOG.warning(f"Caught importasdm exception: {ee}")

        # Copy across extra files from ASDM to MS.
        for xml_filename in ['Source.xml', 'SpectralWindow.xml', 'DataDescription.xml']:
            asdm_source = os.path.join(asdm, xml_filename)
            if os.path.exists(asdm_source):
                vis_source = os.path.join(vis, xml_filename)
                LOG.info(f'Copying {xml_filename} from ASDM to measurement set')
                LOG.trace(f'Copying {xml_filename}: {asdm_source} to {vis_source}')
                shutil.copyfile(asdm_source, vis_source)

    def _make_template_flagfile(self, outfile, titlestr):
        # Create a new file if overwrite is true and the file
        # does not already exist.
        inputs = self.inputs
        if inputs.overwrite or not os.path.exists(outfile):
            template_text = FLAGGING_TEMPLATE_HEADER.replace('___TITLESTR___', titlestr)
            with open(outfile, 'w') as f:
                f.writelines(template_text)

    @staticmethod
    def _rename_J2000_to_ICRS(ms_path):
        # PIPE-2006: rename J2000 FIELD and SOURCE table directions to ICRS (only for ALMA cycles 0-2);
        # adapted from a script by D.Petry (ESO), 2016-03-04
        with casa_tools.MSMDReader(ms_path) as msmd:
            if 'ALMA' not in msmd.observatorynames():
                return

        basename = os.path.basename(ms_path)
        changed_columns = set()
        with casa_tools.TableReader(ms_path + '/FIELD', nomodify=False) as tb:
            for colname in ['PhaseDir_Ref', 'DelayDir_Ref', 'RefDir_Ref']:
                a = tb.getcol(colname)
                for i in range(len(a)):
                    if a[i] == 0:  # J2000
                        a[i] = 21  # ICRS
                        changed_columns.add(colname)
                tb.putcol(colname, a)
        if changed_columns:
            LOG.info(basename + ': changing coords from J2000 to ICRS in the FIELD table, columns ' +
                     ', '.join(changed_columns))

        with casa_tools.TableReader(ms_path + '/SOURCE', nomodify=False) as tb:
            x = tb.getcolkeywords('DIRECTION')
            if x['MEASINFO']['Ref'] == 'J2000':
                x['MEASINFO']['Ref'] = 'ICRS'
                tb.putcolkeywords('DIRECTION', x)
                LOG.info(basename + ': changing coords from J2000 to ICRS in the SOURCE table')


def get_datacolumn_name(msname: str) -> Optional[str]:
    """
    Return a name of data column in MeasurementSet (MS).

    Args:
        msname: A path of MS

    Returns:
        Search for 'DATA' and 'FLOAT_DATA' columns in MS and returns the first
        matching column in MS. Returns None if no match is found.
    """
    return search_columns(msname, ['DATA', 'FLOAT_DATA'])


def get_correcteddatacolumn_name(msname: str) -> Optional[str]:
    """
    Return name of corrected data column in MeasurementSet (MS).

    Args:
        msname: A path of MS

    Returns:
        Search for 'CORRECTED_DATA' column in MS and return the name.
        Returns None if no match is found.
    """
    return search_columns(msname, ['CORRECTED_DATA'])


def search_columns(msname: str, search_cols: List[str]) -> Optional[str]:
    """
    Args:
        msname: MS directory name
        search_cols: List of column names to search for

    Returns:
        Search for columns in MS and return the first matching name.
        Returns None if no match is found.
    """
    with casa_tools.TableReader(msname) as tb:
        tb_cols = tb.colnames()
        for col in search_cols:
            if col in tb_cols:
                return col
    return None


FLAGGING_TEMPLATE_HEADER = '''#
# ___TITLESTR___
#
# Examples
# Note: Do not put spaces inside the reason string !
#
# mode='manual' antenna='DV02;DV03&DA51' spw='22,24:150~175' reason='QA2:applycal_amplitude_frequency'
# 
# mode='manual' spw='22' field='1' timerange='2018/02/10/00:01:01.0959~2018/02/10/00:01:01.0961' reason='QA2:timegaincal_phase_time'
# 
# TP flagging: The 'other' option is intended for bad TP pointing
# mode='manual' antenna='PM01&&PM01' reason='QA2:other_bad_pointing' 
#
# Tsys flagging: 
# mode='manual' antenna='DV02;DV03&DA51' spw='22,24' reason='QA2:tsysflag_tsys_frequency'
'''
