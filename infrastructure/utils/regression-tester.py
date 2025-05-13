"""
Pipeline Regression framework.

PipelineRegression class runs on pytest framework, so it needs to implement
test_* methods for testing.
"""

import ast
import shutil
import datetime
import glob
import os
import platform
import pytest
import re
from packaging import version
from typing import Tuple, Optional, List

import casatasks.private.tec_maps as tec_maps

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.recipereducer
from pipeline.infrastructure.renderer import regression
import pipeline.infrastructure.executeppr as almappr
import pipeline.infrastructure.executevlappr as vlappr
import pipeline.infrastructure.logging as logging
from pipeline.infrastructure.utils import shutdown_plotms, get_casa_session_details

LOG = infrastructure.get_logger(__name__)

class PipelineRegression(object):
    """Pipeline regression test class called from pytest."""

    def __init__(self, recipe: Optional[str] = None, input_dir: Optional[str] = None, visname: Optional[List[str]] = None,
                 expectedoutput_file: Optional[str] = None, output_dir: Optional[str] = None, project_id: Optional[str] = None,
                 expectedoutput_dir: Optional[str] = None):
        """Constructor of PilelineRegression.
        
        Args:
            recipe: recipe XML file name
            input_dir: path to directory contains input files
            visname: list of names of MeadurementSets
            expectedoutput_file: path to a file that defines expected output of a test. Will override expectedoutput_dir if that is 
                                 also specified.
            expectedoutput_dir: path to a directory which contains 1 or more expected output files. Not used if expectedoutput_file
                                is specified.
            output_dir: path to directory to output. If None, it sets visname
        """
        self.recipe = recipe
        self.input_dir = input_dir
        self.visname = visname
        self.project_id = project_id
        if expectedoutput_file: 
            self.expectedoutput_file = casa_tools.utils.resolve(expectedoutput_file)
        else:
            # Find the newest reference file in expectedoutput_dir and use that.
            if expectedoutput_dir:
                reference_data_files = glob.glob(casa_tools.utils.resolve(expectedoutput_dir)+'/*.results.txt')
                if reference_data_files:
                    # Pick the reference result file with the highest PL version number
                    def pipeline_version_from_refdata(file_name): 
                        regex_pattern = re.compile('.*pipeline-(.*\d).*results.txt')
                        match = regex_pattern.match(file_name)
                        if match:
                            version_string = match.group(1)
                            try:
                                return version.parse(version_string)
                            except:
                                LOG.warning("Couldn't determine pipeline version from reference file name. Skipping {}.".format(file_name))
                                return version.parse("0.0")
                        else: 
                            LOG.warning("Couldn't determine pipeline version from reference file name. Skipping {}.".format(file_name))
                            return version.parse("0.0")
                    self.expectedoutput_file = max(reference_data_files, key=pipeline_version_from_refdata)
                    LOG.info("Using {} for the reference value file.".format(self.expectedoutput_file))
                else: 
                    LOG.warning("No reference file found in {}. Test will fail.".format(expectedoutput_dir))

        self.testinput = [f'{input_dir}/{vis}' for vis in self.visname] 
        self.current_path = os.getcwd()
        if output_dir:
            self.output_dir = output_dir
        elif self.project_id: 
            self.output_dir = f'{self.project_id}__{self.visname[0]}_test_output'
        else: 
            self.output_dir = f'{self.visname[0]}_test_output'

        if hasattr(pytest, 'pytestconfig'):
            self.remove_workdir = pytest.pytestconfig.getoption('--remove-workdir')
            self.compare_only = pytest.pytestconfig.getoption('--compare-only')
        else:
            self.remove_workdir = False
            self.compare_only = False
        
        if not self.compare_only:
            self.__initialize_working_folder()


    def __initialize_working_folder(self):
        """Initialize a root folder for task execution."""
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.mkdir(self.output_dir)


    def __sanitize_regression_string(self, instring: str) -> Tuple:
        """Sanitize to get numeric values, remove newline chars and change to float.

        instring format: "[quantity_name]=[quantity value]:::tolerance"
        Example: 
        s2.hifv_statwt.13A-537.sb24066356.eb24324502.56514.05971091435.mean=1.6493377758284253:::0.000001
        
        Args:
            instring: input string
        Returns:
            tuple(key, value, optional tolerance)
        """
        # Get string with the quantity name and value (everything but the tolerance)
        # example: s2.hifv_statwt.13A-537.sb24066356.eb24324502.56514.05971091435.mean=1.6493377758284253
        keyval = instring.split(':::')[0]

        # Get just the quantity name
        # example: s2.hifv_statwt.13A-537.sb24066356.eb24324502.56514.05971091435.mean
        keystring = keyval.split('=')[0]

        # Try to convert the value in string to int, float, Boolean, or None
        try:
            value = ast.literal_eval(keyval.split("=")[-1])
        except ValueError:
            value = keyval.split("=")[-1]
        if not isinstance(value, (float, int, bool, type(None))):
            value = keyval.split("=")[-1]
            raise ValueError(
                "For the key: {}, value: {} cannot be converted to int/float/boolean/None.".format(keystring, value))

        try:
            tolerance = float(instring.split(':::')[-1].replace('\n', ''))
        except ValueError:
            tolerance = None

        return keystring, value, tolerance


    def run(self, ppr: Optional[str] = None, telescope: str = 'alma',
            default_relative_tolerance: float = 1e-7, omp_num_threads: Optional[int] = None):
        """
        Run test with PPR if supplied or recipereducer if no PPR and compared to expected results.

        The inputs and expectd output are usually found in the pipeline data repository.

        Args:
            ppr: PPR file name
            telescope: string 'alma' or 'vla'
            default_relative_tolerance: default relative tolerance of output value
            omp_num_threads: specify the number of OpenMP threads used for this regression test instance, regardless of 
                             the default value of the current CASA session. An explicit setting could mitigate potential small 
                             numerical difference from threading-sensitive CASA tasks (e.g. setjy/tclean, which relies 
                             on the FFTW library).            

        note: Because a PL execution depends on global CASA states, only one test can run under 
        a CASA process at the same. Therefore a parallelization based on process-forking might not work
        properly (e.g., xdist: --forked).  However, it's okay to group tests under several independent
        subprocesses (e.g., xdist: -n 4)

        """
        # Run the Pipeline using cal+imag ALMA IF recipe
        # set datapath in ~/.casa/config.py, e.g. datapath = ['/users/jmasters/pl-testdata.git']
        input_vis = [casa_tools.utils.resolve(testinput) for testinput in self.testinput]

        if not self.compare_only:
            # optionally set OpenMP nthreads to a specified value.
            if omp_num_threads is not None:
                # casa_tools.casalog.ompGetNumThreads() and get_casa_session_details()['omp_num_threads'] are equivalent.
                default_nthreads = get_casa_session_details()['omp_num_threads']
                casa_tools.casalog.ompSetNumThreads(omp_num_threads)

            last_casa_logfile = casa_tools.casalog.logfile()

        try:
            # run the pipeline for new results

            # switch from the pytest-call working directory to the root folder for this test.
            os.chdir(self.output_dir)

            if not(self.compare_only):
                # create the sub-directory structure for this test and switch to "working/"
                dd_list = ['products', 'working', 'rawdata'] if ppr else ['products', 'working']
                for dd in dd_list:
                    try:
                        os.mkdir(dd)
                    except FileExistsError:
                        pass
                os.chdir('working')

                # PIPE-1301: shut down the existing plotms process to avoid side-effects from changing CWD.
                # This is implemented as a workaround for CAS-13626
                shutdown_plotms()
                
                # PIPE-1432: reset casatasks/tec_maps.workDir as it's unaware of a CWD change.
                if hasattr(tec_maps, 'workDir'):
                    tec_maps.workDir = os.getcwd()+'/'            

                # switch to per-test casa/PL logfile paths and backup the default(initial) casa logfile name
                self.__reset_logfiles(prepend=True)

                if ppr:
                    self.__run_ppr(input_vis, ppr, telescope)
                else:
                    self.__run_reducer(input_vis)
            else: 
                os.chdir('working')

            # Get new results
            new_results = self.__get_results_of_from_current_context()

            # new results file path
            new_file = f'{self.visname[0]}.NEW.results.txt'

            # Store new results in a file
            self.__save_new_results_to(new_file, new_results)

            # Compare new results with expected results
            self.__compare_results(new_file, default_relative_tolerance)

        finally:
            if not self.compare_only:
                # restore the default logfile state
                self.__reset_logfiles(casa_logfile=last_casa_logfile)

            os.chdir(self.current_path)
            
            # clean up if requested
            if not self.compare_only and self.remove_workdir and os.path.isdir(self.output_dir):
                shutil.rmtree(self.output_dir)
        
        # restore the OpenMP nthreads to the value saved before the Pipeline ppr/reducer call.
        if not self.compare_only and omp_num_threads is not None:
            casa_tools.casalog.ompSetNumThreads(default_nthreads)

    def __compare_results(self, new_file: str, relative_tolerance: float): 
        """
        Compare results between new one loaded from file and old one.

        Args:
            new_file : file path of new results
            relative_tolerance : relative tolerance of output value
        """
        with open(self.expectedoutput_file) as expected_fd, open(new_file) as new_fd:
            expected_results = expected_fd.readlines()
            new_results = new_fd.readlines()
            errors = []
            worst_diff = (0, 0)
            worst_percent_diff = (0, 0)
            for old, new in zip(expected_results, new_results):
                try:
                    oldkey, oldval, tol = self.__sanitize_regression_string(old)
                    newkey, newval, _ = self.__sanitize_regression_string(new)
                except ValueError as e: 
                    errorstr = "The results: {0} could not be parsed. Error: {1}".format(new, str(e))
                    errors.append(errorstr)
                    continue

                assert oldkey == newkey
                tolerance = tol if tol else relative_tolerance
                if newval is not None: 
                    LOG.info(f'Comparing {oldval} to {newval} with a rel. tolerance of {tolerance}')
                    if oldval != pytest.approx(newval, rel=tolerance):
                        diff = oldval-newval
                        percent_diff = (oldval-newval)/oldval * 100 
                        if abs(diff) > abs(worst_diff[0]):
                            worst_diff = diff, oldkey
                        if abs(percent_diff) > abs(worst_percent_diff[0]):
                            worst_percent_diff = percent_diff, oldkey 
                        errorstr = f"{oldkey}\n\tvalues differ by > a relative difference of {tolerance}\n\texpected: {oldval}\n\tnew:      {newval}\n\tdiff: {diff}\n\tpercent_diff: {percent_diff}%"
                        errors.append(errorstr)
                elif oldval is not None:
                    # If only the new value is None, fail
                    errorstr = f"{oldkey}\n\tvalue is None\n\texpected: {oldval}\n\tnew:      {newval}"
                    errors.append(errorstr)
                else: 
                    # If old and new values are both None, this is expected, so pass
                    LOG.info(f'Comparing {oldval} and {newval}... both values are None.')

            [LOG.warning(x) for x in errors]
            n_errors = len(errors)
            if n_errors > 0:
                summary_str = f"Worst absolute diff, {worst_diff[1]}: {worst_diff[0]}\nWorst percentage diff, {worst_percent_diff[1]}: {worst_percent_diff[0]}%"
                errors.append(summary_str)
                pytest.fail("Failed to match {0} result value{1} within tolerance{1} :\n{2}".format(
                    n_errors, '' if n_errors == 1 else 's', '\n'.join(errors)), pytrace=True)

    def __save_new_results_to(self, new_file: str, new_results: List[str]):
        """
        Compare results between new one and old one, both results are loaded from specified files.

        Args:
            new_file : file path of new results to save
            new_results : List[str] of new results
        """
        with open(new_file, 'w') as fd:
            fd.writelines([str(x) + '\n' for x in new_results])

    def __get_results_of_from_current_context(self) -> List[str]:
        """
        Get results of current execution from context.

        Returns: List[str] of new results
        """
        context = pipeline.Pipeline(context='last').context
        new_results = sorted(regression.extract_regression_results(context))
        return new_results

    def __run_ppr(self, input_vis: List[str], ppr: str, telescope: str):
        """
        Execute the recipe defined by PPR.

        Args:
            input_vis : MS name
            ppr : PPR file name
            telescope : string 'alma' or 'vla'
        
        note: this private method is expected be called under "working/"
        """

        # executeppr expects the rawdata in ../rawdata
        for vis in input_vis: 
            os.symlink(vis, f'../rawdata/{os.path.basename(vis)}')

        # save a copy of PPR in the directory one level above "working/".
        ppr_path = casa_tools.utils.resolve(ppr)
        ppr_local = f'../{os.path.basename(ppr_path)}'
        shutil.copyfile(ppr_path, ppr_local)

        if telescope == 'alma':
            almappr.executeppr(ppr_local, importonly=False)
        elif telescope == 'vla':
            vlappr.executeppr(ppr_local, importonly=False)
        else:
            LOG.error("Telescope is not 'alma' or 'vla'.  Can't run executeppr.")

    def __run_reducer(self, input_vis: List[str]):
        """
        Execute the recipe by recipereducer.

        Args:
            input_vis : MS name

        note: this private method is expected be called under "working/"
        """
        LOG.warning("Running without Pipeline Processing Request (PPR).  Using recipereducer instead.")
        pipeline.recipereducer.reduce(vis=input_vis, procedure=self.recipe)

    def __reset_logfiles(self, casacalls_logfile=None, casa_logfile=None, prepend=False):
        """Put CASA/Pipeline logfiles into the test working directory."""

        # reset casacalls-*.txt
        if casacalls_logfile is None:
            casacalls_logfile = 'casacalls-{!s}.txt'.format(platform.node().split('.')[0])
        else:
            casacalls_logfile = casacalls_logfile
        _ = logging.get_logger('CASACALLS', stream=None, format='%(message)s', addToCasaLog=False,
                               filename=casacalls_logfile)
        # reset casa-*.log
        if casa_logfile is None:
            now_str = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            casa_logfile = os.path.abspath(f'casa-{now_str}.log')
        else:
            casa_logfile = os.path.abspath(casa_logfile)
        last_casa_logfile = casa_tools.casalog.logfile()
        casa_tools.casalog.setlogfile(casa_logfile)

        # prepend the content of the last CASA logfile in the new logfile.
        if prepend and os.path.exists(last_casa_logfile) and casa_logfile != last_casa_logfile:
            with open(last_casa_logfile, 'r') as infile:
                with open(casa_logfile, 'a') as outfile:
                    outfile.write(infile.read())

        return


# The methods below are test methods called from pytest.
@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__procedure_hifa_calimage__regression():
    """Run ALMA cal+image regression on a small test dataset.

    Recipe name:                procedure_hifa_calimage
    Dataset:                    uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
    """
    pr = PipelineRegression(recipe='procedure_hifa_calimage.xml',
                            input_dir='pl-unittest',
                            visname=['uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
                            expectedoutput_dir='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/')

    pr.run(ppr='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml')


@pytest.mark.fast
@pytest.mark.alma
def test_E2E6_1_00010_S__uid___A002_Xd0a588_X2239_regression():
    """Run ALMA cal+image regression on a 12m moderate-size test dataset in ASDM.

    Recipe name:                procedure_hifa_calimage
    Dataset:                    E2E6.1.00010.S: uid___A002_Xd0a588_X2239
    """

    input_dir = 'pl-regressiontest/E2E6.1.00010.S'
    ref_directory = 'pl-regressiontest/E2E6.1.00010.S'

    pr = PipelineRegression(recipe='procedure_hifa_calimage.xml',
                            input_dir=input_dir,
                            visname=['uid___A002_Xd0a588_X2239'],
                            expectedoutput_file=f'{ref_directory}/uid___A002_Xd0a588_X2239.casa-6.6.1-17-pipeline-2024.0.3.8.results.txt')

    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_Xc845c0_X7366_cycle5_restore_regression():
    """
    Restore from Cycle 5 (with current pipeline)

    Recipe name:                procedure_hifa_image
    Dataset:                    uid___A002_Xc845c0_X7366
    """

    input_dir = 'pl-regressiontest/uid___A002_Xc845c0_X7366'
    ref_directory = 'pl-regressiontest/uid___A002_Xc845c0_X7366'

    pr = PipelineRegression(recipe='procedure_hifa_image.xml',
                            input_dir=input_dir,
                            visname=['uid___A002_Xc845c0_X7366'],
                            expectedoutput_dir=ref_directory)

    # copy files for the restore into products folder
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_Xc46ab2_X15ae_selfcal_restore_regression():
    """Restore selfcal from Cycle 10 (with current pipeline)

    Recipe name:                procedure_hifa_image_selfcal
    Dataset:                    uid___A002_Xc46ab2_X15ae
    """

    input_dir = 'pl-regressiontest/uid___A002_Xc46ab2_X15ae_selfcal_restore'
    ref_directory = 'pl-regressiontest/uid___A002_Xc46ab2_X15ae_selfcal_restore'

    pr = PipelineRegression(recipe='procedure_hifa_image.xml',
                            input_dir=input_dir,
                            visname=['uid___A002_Xc46ab2_X15ae'],
                            expectedoutput_dir=ref_directory)

    # copy files into products folder for restore
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_Xef72bb_X9d29_renorm_restore_regression():
    """Restore renorm from Cycle 8 (with current pipeline)

    Recipe name:                procedure_hifa_image
    Dataset:                    uid___A002_Xef72bb_X9d29
    """

    input_dir = 'pl-regressiontest/uid___A002_Xef72bb_X9d29'
    ref_directory = 'pl-regressiontest/uid___A002_Xef72bb_X9d29'

    pr = PipelineRegression(recipe='procedure_hifa_image.xml',
                            input_dir=input_dir,
                            visname=['uid___A002_Xef72bb_X9d29'],
                            expectedoutput_dir=ref_directory)

    # copy files into products folder for restore
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_X85c183_X36f__procedure_hsd_calimage__regression():
    """Run ALMA single-dish cal+image regression on the obseration data of M100.

    Recipe name:                procedure_hsd_calimage
    Dataset:                    uid___A002_X85c183_X36f
    Expected results version:   casa-6.2.1-2-pipeline-2021.2.0.94
    """
    pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
                            input_dir='pl-regressiontest/uid___A002_X85c183_X36f',
                            visname=['uid___A002_X85c183_X36f'],
                            expectedoutput_dir=('pl-regressiontest/uid___A002_X85c183_X36f'))

    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___A002_X85c183_X36f_SPW15_23__PPR__regression():
    """Run ALMA single-dish restoredata regression on the observation data of M100.

    Dataset:                    uid___A002_X85c183_X36f_SPW15_23
    Expected results version:   casa-6.2.1-2-pipeline-2021.2.0.94
    """
    input_dir = 'pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23'
    pr = PipelineRegression(input_dir=input_dir,
                            visname=['uid___A002_X85c183_X36f_SPW15_23.ms'],
                            expectedoutput_dir=('pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23'))

    # copy files use restore task into products folder
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run(ppr=f'{input_dir}/PPR.xml')


@pytest.mark.fast
@pytest.mark.alma
def test_uid___mg2_20170525142607_180419__procedure_hsdn_calimage__regression():
    """Run ALMA single-dish cal+image regression for standard nobeyama recipe.

    Recipe name:                procedure_hsdn_calimage
    Dataset:                    mg2-20170525142607-180419
    Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
    """
    pr = PipelineRegression(
        recipe='procedure_hsdn_calimage.xml', input_dir='pl-regressiontest/mg2-20170525142607-180419',
        visname=['mg2-20170525142607-180419.ms'],
        expectedoutput_file=('pl-regressiontest/mg2-20170525142607-180419/' +
                             'mg2-20170525142607-180419.casa-6.6.1-15-pipeline-2024.0.0.60.results.txt'))
    pr.run()


@pytest.mark.fast
@pytest.mark.alma
def test_uid___mg2_20170525142607_180419__PPR__regression():
    """Run ALMA single-dish cal+image regression for restore nobeyama recipe.

    Dataset:                    mg2-20170525142607-180419
    Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
    """

    input_dir = 'pl-regressiontest/mg2-20170525142607-180419'

    pr = PipelineRegression(
        input_dir=input_dir, visname=['mg2-20170525142607-180419.ms'],
        expectedoutput_file=(f'{input_dir}/' +
                             'mg2-20170525142607-180419_PPR.casa-6.6.1-15-pipeline-2024.0.0.60.results.txt'),
        output_dir='mg2-20170525142607-180419_PPR')

    # copy files use restore task into products folder
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run(ppr=f'{input_dir}/PPR.xml')


@pytest.mark.skip(reason="Recent failure needs longer investigation")
@pytest.mark.alma
def test_uid___A002_Xee1eb6_Xc58d_pipeline__procedure_hifa_calsurvey__regression():
    """Run ALMA cal+survey regression on a calibration survey test dataset
 
    Recipe name:                procedure_hifa_calsurvey
    Dataset:                    uid___A002_Xee1eb6_Xc58d_original.ms
    Expected results version:   casa-6.3.0-48-pipeline-2021.3.0.5
    """
    input_directory = 'pl-regressiontest/uid___A002_Xee1eb6_Xc58d_calsurvey/'
    pr = PipelineRegression(recipe='procedure_hifa_calsurvey.xml',
                            input_dir=input_directory,
                            visname=['uid___A002_Xee1eb6_Xc58d_original.ms'],
                            expectedoutput_file=(input_directory +
                                            'uid___A002_Xee1eb6_Xc58d.casa-6.3.0-482-pipeline-2021.3.0.5.results.txt'),
                            output_dir='uid___A002_Xee1eb6_Xc58d_calsurvey_output')

    pr.run()


@pytest.mark.vla
@pytest.mark.fast
def test_13A_537__procedure_hifv__regression():
    """Run VLA calibration regression for standard procedure_hifv.xml recipe.

    Recipe name:                procedure_hifv
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    """

    input_dir = 'pl-regressiontest/13A-537'
    pr = PipelineRegression(recipe='procedure_hifv.xml',
                            input_dir=input_dir,
                            visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
                            expectedoutput_file=f'{input_dir}/13A-537.casa-6.6.1-17-pipeline-2024.0.3.8.results.txt',
                            output_dir='13A_537__procedure_hifv__regression')

    pr.run(telescope='vla', omp_num_threads=1)


@pytest.mark.vla
@pytest.mark.fast
def test_13A_537__calibration__PPR__regression():
    """Run VLA calibration regression with a PPR file.

    PPR name:                   PPR_13A-537.xml
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    """

    input_dir = 'pl-regressiontest/13A-537'

    pr = PipelineRegression(input_dir=input_dir,
                            visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
                            expectedoutput_file=f'{input_dir}/13A-537.casa-6.6.1-17-pipeline-2024.0.3.8.results.txt',
                            output_dir='13A_537__calibration__PPR__regression')

    pr.run(ppr=f'{input_dir}/PPR_13A-537.xml', telescope='vla', omp_num_threads=1)


@pytest.mark.vla
@pytest.mark.fast
def test_13A_537__restore__PPR__regression():
    """Run VLA calibration restoredata regression with a PPR file

    PPR name:                   PPR_13A-537_restore.xml
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    Expected results version:   casa-6.2.1.7-pipeline-2021.2.0.128
    """
    input_dir = 'pl-regressiontest/13A-537'
    pr = PipelineRegression(input_dir=input_dir,
                            visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
                            expectedoutput_file=(f'{input_dir}/' +
                                            '13A-537.casa-6.2.1.7-pipeline-2021.2.0.128.restore.results.txt'),
                            output_dir='13A_537__restore__PPR__regression')

    # copy files use restore task into products folder
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run(ppr=f'{input_dir}/PPR_13A-537_restore.xml', telescope='vla')


@pytest.mark.vlass
@pytest.mark.fast
def test_vlass_quicklook():
    """Run VLASS quicklook regression

    Recipe name: procedure_vlassQLIP.xml
    Dataset: TSKY0001.sb32295801.eb32296475.57549.31722762731_split_withcorrectdata.ms
    """

    input_dir = 'pl-regressiontest/vlass_quicklook'
    ref_directory = 'pl-regressiontest/vlass_quicklook'

    pr = PipelineRegression(recipe='procedure_vlassQLIP.xml',
                            input_dir=input_dir,
                            visname=['TSKY0001.sb32295801.eb32296475.57549.31722762731_split_withcorrectdata.ms'],
                            expectedoutput_dir=ref_directory)

    # Copy parameter list file into the working directory
    parameter_list_file = casa_tools.utils.resolve(
        f'{input_dir}/TSKY0001.sb32295801.eb32296475.57549.31722762731_split_QLIP_parameter.list')
    try:
        os.mkdir(f'{pr.output_dir}/working/')
    except FileExistsError:
        pass
    shutil.copyfile(parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/QLIP_parameter.list'))
    pr.run(telescope='vla')

# Section for longer-running tests

@pytest.fixture(autouse=True)
def data_directory(scope="module") -> str:
    if hasattr(pytest, 'pytestconfig'):
        big_data_dir = pytest.pytestconfig.getoption('--data-directory')
    else:
        big_data_dir = "/lustre/cv/projects/pipeline-test-data/regression-test-data/"

    if not os.path.exists(big_data_dir):
        print("Warning! The large dataset directory {} does not exist, so any long-running tests will fail.".format(big_data_dir))
    else:
        print("Using: {} for data directory".format(big_data_dir))
    return big_data_dir


def setup_flux_antennapos(test_directory, output_dir):
    # Copy flux.csv and antennapos.csv into the working directory
    flux_file = casa_tools.utils.resolve(f'{test_directory}/flux.csv')
    anteannapos_file = casa_tools.utils.resolve(f'{test_directory}/antennapos.csv')

    try:
        os.mkdir(f'{output_dir}/working/')
    except FileExistsError:
        pass

    shutil.copyfile(flux_file, casa_tools.utils.resolve(f'{output_dir}/working/flux.csv'))
    shutil.copyfile(anteannapos_file, casa_tools.utils.resolve(f'{output_dir}/working/antennapos.csv'))


@pytest.mark.slow
class TestSlowerRegression:

    # ALMA-section
    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2019_1_01094_S__uid___A002_Xecbc07_X6b0e_PPR__regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset
        
        Dataset: 2019.1.01094.S: uid___A002_Xecbc07_X6b0e, uid___A002_Xecf7c7_X1d83
        """
        test_directory = f'{data_directory}/alma_if/2019.1.01094.S/'
        ref_directory = 'pl-regressiontest/2019.1.01094.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                visname=['uid___A002_Xecbc07_X6b0e', 'uid___A002_Xecf7c7_X1d83'],
                                project_id="2019_1_01094_S",
                                expectedoutput_dir=ref_directory)

        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_E2E9_1_00061_S__uid___A002_Xfd764e_X5843_regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset
        
        Dataset: E2E9.1.00061.S: uid___A002_Xfd764e_X5843, uid___A002_Xfd764e_X60e2
        """
        test_directory = f'{data_directory}/alma_if/E2E9.1.00061.S/'
        ref_directory = 'pl-regressiontest/E2E9.1.00061.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                visname=['uid___A002_Xfd764e_X5843', 'uid___A002_Xfd764e_X60e2'],
                                project_id="E2E9_1_00061_S",
                                expectedoutput_dir=ref_directory)

        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2018_1_01255_S__uid___A002_Xe0e4ca_Xb18_regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset 
        
        Dataset: 2018.1.01255.S: uid___A002_Xe0e4ca_Xb18, uid___A002_Xeb9695_X2fe5
        """
        test_directory = f'{data_directory}/alma_if/2018.1.01255.S/'
        ref_directory = 'pl-regressiontest/2018.1.01255.S/'
        
        pr = PipelineRegression(input_dir = test_directory,
                                visname=['uid___A002_Xe0e4ca_Xb18', 'uid___A002_Xeb9695_X2fe5'],
                                project_id="2018_1_01255_S",
                                expectedoutput_dir=ref_directory)
        
        setup_flux_antennapos(test_directory, pr.output_dir)
        
        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2017_1_00912_S__uid___A002_Xc74b5b_X316a_regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset
        
        Dataset: 2017.1.00912.S: uid___A002_Xe6a684_X7c41
        """
        test_directory = f'{data_directory}/alma_if/2017.1.00912.S/'
        ref_directory = 'pl-regressiontest/2017.1.00912.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                visname=['uid___A002_Xc74b5b_X316a'],
                                project_id="2017_1_00912_S",
                                expectedoutput_dir=ref_directory)

        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2019_1_01184_S__uid___A002_Xe1d2cb_X12782_regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset 
        
        Dataset: 2019_1_01184_S: uid___A002_Xe1d2cb_X12782, uid___A002_Xe850fb_X4efc
        """
        test_directory = f'{data_directory}/alma_if/2019.1.01184.S/'
        ref_directory = 'pl-regressiontest/2019.1.01184.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                visname=['uid___A002_Xe1d2cb_X12782', 'uid___A002_Xe850fb_X4efc'],
                                project_id="2019_1_01184_S",
                                expectedoutput_dir=ref_directory)
        
        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2019_1_00678_S__uid___A002_Xe6a684_X7c41__PPR__regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset 
        
        Dataset: 2019.1.00678.S: uid___A002_Xe6a684_X7c41
        """
        test_directory = f'{data_directory}/alma_if/2019.1.00678.S/'
        ref_directory =  'pl-regressiontest/2019.1.00678.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                project_id="2019_1_00678_S",
                                visname=['uid___A002_Xe6a684_X7c41'],
                                expectedoutput_dir=ref_directory)

        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.twelve
    def test_2017_1_00670_S__uid___A002_Xca8fbf_X5733__PPR__regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset 
        
        Dataset: 2017.1.00670.S: uid___A002_Xca8fbf_X5733
        """
        test_directory = f'{data_directory}/alma_if/2017.1.00670.S/'
        ref_directory =  'pl-regressiontest/2017.1.00670.S/'

        pr = PipelineRegression(input_dir = test_directory,
                                project_id='2017_1_00670_S',
                                visname=['uid___A002_Xca8fbf_X5733'],
                                expectedoutput_dir=ref_directory)

        setup_flux_antennapos(test_directory, pr.output_dir)
        
        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.seven
    def test_2019_1_00847_S__uid___A002_Xe1f219_X1457_regression(self, data_directory):
        """Run longer regression test on this ALMA if dataset
        ALMA 7m

        Dataset: 2019.1.00847.S: uid___A002_Xe1f219_X1457, uid___A002_Xe1f219_X9dbf, uid___A002_Xe27761_X74f8
        """
        test_directory = f'{data_directory}/alma_if/2019.1.00847.S/'
        ref_directory = 'pl-regressiontest/2019.1.00847.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                project_id="2019_1_00847_S",
                                visname=['uid___A002_Xe1f219_X1457', 'uid___A002_Xe1f219_X9dbf', 'uid___A002_Xe27761_X74f8'],
                                expectedoutput_dir=ref_directory)
        
        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.seven
    def test_2019_1_00994_S__uid___A002_Xe44309_X7d94__PPR__regression(self, data_directory):
        """Run longer regression test on this ALMA IF dataset
        
        ALMA 7m

        Dataset: 2019.1.00994.S: uid___A002_Xe44309_X7d94, uid___A002_Xe45e29_X59ee, uid___A002_Xe45e29_X6666, uid___A002_Xe48598_X8697
        """
        test_directory = f'{data_directory}/alma_if/2019.1.00994.S/'
        ref_directory = 'pl-regressiontest/2019.1.00994.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                project_id="2019_1_00994_S",
                                visname=['uid___A002_Xe44309_X7d94', 'uid___A002_Xe45e29_X59ee', 'uid___A002_Xe45e29_X6666', 'uid___A002_Xe48598_X8697'],
                                expectedoutput_dir=ref_directory)
        
        setup_flux_antennapos(test_directory, pr.output_dir)

        pr.run(ppr=(test_directory + 'PPR.xml'))

    @pytest.mark.alma
    @pytest.mark.seven
    def test_2019_1_01056_S__uid___A002_Xe1f219_X6d0b__PPR__regression(self, data_directory):
        """Run longer regression test on this ALMA IF dataset
        
        ALMA 7m 

        Dataset: 2019.1.01056.S: uid___A002_Xe1f219_X6d0bm, uid___A002_Xe1f219_X7ee8
        """
        test_directory = f'{data_directory}/alma_if/2019.1.01056.S/'
        ref_directory = 'pl-regressiontest/2019.1.01056.S/'

        pr = PipelineRegression(input_dir=test_directory,
                                visname=['uid___A002_Xe1f219_X6d0b', 'uid___A002_Xe1f219_X7ee8'],
                                project_id="2019_1_01056_S",
                                expectedoutput_file=(f'{ref_directory}' +
                                                'uid___A002_Xe1f219_X6d0b.casa-6.5.4-2-pipeline-2023.0.0.17.results.txt'))
        
        setup_flux_antennapos(test_directory, pr.output_dir)
        
        pr.run(ppr=(test_directory + 'PPR.xml'))

    # SD Section
    @pytest.mark.alma
    @pytest.mark.sd
    def test_2019_2_00093_S__uid___A002_Xe850fb_X2df8_regression(self, data_directory):
        """Run longer regression test on this ALMA SD dataset
        
        Recipe name: procedure_hsd_calimage
        Dataset: 2019.2.00093.S: uid___A002_Xe850fb_X2df8, uid___A002_Xe850fb_X36e4, uid___A002_Xe850fb_X11e13
        """
        test_directory = f'{data_directory}/alma_sd/2019.2.00093.S/'
        ref_directory = 'pl-regressiontest/2019.2.00093.S/'

        pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
                                input_dir=test_directory,
                                visname=['uid___A002_Xe850fb_X2df8', 'uid___A002_Xe850fb_X36e4', 'uid___A002_Xe850fb_X11e13'], 
                                project_id="2019_2_00093_S",
                                expectedoutput_dir=ref_directory)
        pr.run()

    @pytest.mark.alma
    @pytest.mark.sd
    def test_2019_1_01056_S__uid___A002_Xe1d2cb_X110f1_regression(self, data_directory):
        """Run weekly regression test on this ALMA SD dataset
        
        Recipe name: procedure_hsd_calimage
        Dataset: 2019.1.01056.S: uid___A002_Xe1d2cb_X110f1, uid___A002_Xe1d2cb_X11d0a, uid___A002_Xe1f219_X6eeb
        """
        test_directory = f'{data_directory}/alma_sd/2019.1.01056.S/'
        ref_directory = 'pl-regressiontest/2019.1.01056.S/'

        pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
                                input_dir=test_directory,
                                visname=['uid___A002_Xe1d2cb_X110f1', 'uid___A002_Xe1d2cb_X11d0a', 'uid___A002_Xe1f219_X6eeb'], 
                                project_id="2019_1_01056_S",
                                expectedoutput_file=f'{ref_directory}uid___A002_Xe1d2cb_X110f1.casa-6.5.4-2-pipeline-2023.0.0.17.results.txt')

        setup_flux_antennapos(test_directory, pr.output_dir)
        pr.run()

    @pytest.mark.alma
    @pytest.mark.sd
    def test_2016_1_01489_T__uid___A002_Xbadc30_X43ee_regression(self, data_directory):
        """Run weekly regression test on this ALMA SD dataset
        
        Recipe name: procedure_hsd_calimage
        Dataset: 2016.1.01489.T: uid___A002_Xbadc30_X43ee, uid___A002_Xbaedce_X7694
        """
        test_directory = f'{data_directory}/alma_sd/2016.1.01489.T/'
        ref_directory = 'pl-regressiontest/2016.1.01489.T/'

        pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
                                input_dir=test_directory,
                                visname=['uid___A002_Xbadc30_X43ee', 'uid___A002_Xbaedce_X7694'], 
                                project_id="2016_1_01489_T",
                                expectedoutput_dir=ref_directory)
        pr.run()

    # VLA Section
    @pytest.mark.vla
    def test_13A_537__procedure_hifv_calimage__regression(self, data_directory):
        """Run VLA calibration regression for standard procedure_hifv_calimage_cont recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    13A-537.sb24066356.eb24324502.56514.05971091435
        """

        dataset_name = '13A-537.sb24066356.eb24324502.56514.05971091435'
        input_dir = 'pl-regressiontest/13A-537/'
        ref_directory = 'pl-regressiontest/13A-537/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        pr.run(telescope='vla', omp_num_threads=1)

    @pytest.mark.vla
    def test_15B_342__procedure_hifv__regression(self, data_directory):
        """Run VLA calibration regression for standard recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    15B-342.sb31041443.eb31041910.57246.076202627315
        """

        dataset_name = '15B-342.sb31041443.eb31041910.57246.076202627315'
        input_dir = f'{data_directory}/vla/15B-342/'
        ref_directory = 'pl-regressiontest/15B-342/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)
        pr.run(telescope='vla', omp_num_threads=1)

    @pytest.mark.vla
    def test_17B_188__procedure_hifv__regression(self, data_directory):
        """Run VLA calibration regression for standard recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    17B-188.sb35564398.eb35590549.58363.10481791667
        """

        dataset_name = '17B-188.sb35564398.eb35590549.58363.10481791667'
        input_dir = f'{data_directory}/vla/17B-188/'
        ref_directory = 'pl-regressiontest/17B-188/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        pr.run(telescope='vla', omp_num_threads=1)

    @pytest.mark.vla
    def test_18A_228__procedure_hifv__regression(self, data_directory):
        """Run VLA calibration regression for standard procedure_hifv_calimage_cont.xml recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    18A-228.sb35538192.eb35676319.58412.135923414358
        """
        input_dir = f'{data_directory}/vla/18A-228/'
        ref_directory = 'pl-regressiontest/18A-228/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=['18A-228.sb35538192.eb35676319.58412.13592341435'],
                                expectedoutput_dir=ref_directory)

        pr.run(telescope='vla', omp_num_threads=1)

    @pytest.mark.vla
    def test_18A_426__procedure_hifv__regression(self, data_directory):
        """Run VLA calibration regression for standard procedure_hifv_calimage_cont.xml recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    18A-426.sb35644955.eb35676220.58411.96917952546
        """
        input_dir = f'{data_directory}/vla/18A-426/'
        ref_directory = 'pl-regressiontest/18A-426/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=['18A-426.sb35644955.eb35676220.58411.96917952546'],
                                expectedoutput_dir=ref_directory)
        pr.run(telescope='vla', omp_num_threads=1)

    @pytest.mark.vla
    def test_21A_423__procedure_hifv__regression(self, data_directory):
        """Run VLA calibration regression for standard recipe.

        Recipe name:                procedure_hifv_calimage_cont
        Dataset:                    21A-423.sb39709588.eb40006153.59420.64362002315
        """

        dataset_name = '21A-423.sb39709588.eb40006153.59420.64362002315'
        input_dir = f'{data_directory}/vla/21A-423/'
        ref_directory = 'pl-regressiontest/21A-423/'

        pr = PipelineRegression(recipe='procedure_hifv_calimage_cont.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        pr.run(telescope='vla', omp_num_threads=1)

    ### VLASS section
    @pytest.mark.vlass
    def test_vlass_se_cont_mosaic(self, data_directory):
        """Run VLASS regression

        Recipe name: procedure_vlassSEIP_cv.xml
        Dataset: VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms                    
        """

        dataset_name = 'VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms'
        input_dir = f'{data_directory}/vlass/se_cont_mosaic/'
        ref_directory = 'pl-regressiontest/vlass_se_cont_mosaic/'

        pr = PipelineRegression(recipe=f'{input_dir}/procedure_vlassSEIP_cv.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        # Copy parameter list file into the working directory
        parameter_list_file = casa_tools.utils.resolve(f'{input_dir}/SEIP_parameter.list')
        try:
            os.mkdir(f'{pr.output_dir}/working/')
        except FileExistsError:
            pass
        shutil.copyfile(parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/SEIP_parameter.list'))

        pr.run(telescope='vla')

    @pytest.mark.vlass
    def test_vlass_se_cont_awp32(self, data_directory):    
        """Run VLASS regression

        Recipe name: procedure_vlassSEIP_cv.xml
        Dataset: VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms
        """

        dataset_name = 'VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms'
        input_dir = f'{data_directory}/vlass/se_cont_awp32/'
        ref_directory = 'pl-regressiontest/vlass_se_cont_awp32/'

        pr = PipelineRegression(recipe=f'{input_dir}/procedure_vlassSEIP_cv.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        # Copy parameter list file into the working directory
        parameter_list_file = casa_tools.utils.resolve(f'{input_dir}/SEIP_parameter_awp32.list')
        try:
            os.mkdir(f'{pr.output_dir}/working/')
        except FileExistsError:
            pass
        shutil.copyfile(parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/SEIP_parameter.list'))

        pr.run(telescope='vla')

    @pytest.mark.vlass
    def test_vlass_se_cube(self, data_directory):
        """Run VLASS regression

        Recipe name: procedure_vlassCCIP.xml
        Dataset: VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms
        """
        dataset_name = 'VLASS2.2.sb40889925.eb40967634.59536.14716583333_J232327.4+5024320_split.ms'
        input_dir = f'{data_directory}/vlass/se_cube/'
        ref_directory = 'pl-regressiontest/vlass_se_cube/'

        pr = PipelineRegression(recipe='procedure_vlassCCIP.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)

        # Copy parameter list files and reimaging resources into the working directory
        SEIP_parameter_list_file = casa_tools.utils.resolve(f'{input_dir}/SEIP_parameter.list')
        try:
            os.mkdir(f'{pr.output_dir}/working/')
        except FileExistsError:
            pass

        shutil.copyfile(SEIP_parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/SEIP_parameter.list'))

        CCIP_parameter_list_file = casa_tools.utils.resolve(f'{input_dir}/CCIP_parameter_sg16.list')
        shutil.copyfile(CCIP_parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/CCIP_parameter.list'))

        reimaging_resources_file = casa_tools.utils.resolve(f'{input_dir}/reimaging_resources.tgz')
        shutil.copyfile(reimaging_resources_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/reimaging_resources.tgz'))

        pr.run(telescope='vla')

    @pytest.mark.vlass
    def test_vlass_cal(self, data_directory):
        """Run VLASS regression

        Recipe name: procedure_hifvcalvlass.xml
        Dataset: VLASS2.1.sb39020033.eb39038648.59173.7629213426
        """
        dataset_name = 'VLASS2.1.sb39020033.eb39038648.59173.7629213426'
        input_dir = f'{data_directory}/vlass/cal/'
        ref_directory = 'pl-regressiontest/vlass_cal'

        pr = PipelineRegression(recipe='procedure_hifvcalvlass.xml',
                                input_dir=input_dir,
                                visname=[dataset_name],
                                expectedoutput_dir=ref_directory)
        pr.run(telescope='vla')
