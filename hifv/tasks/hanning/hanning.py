import os
import shutil
from typing import Type, Dict

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure.utils import conversion
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.utils import find_ranges
from pipeline.domain.measures import FrequencyUnits

LOG = infrastructure.get_logger(__name__)


class HanningInputs(vdp.StandardInputs):
    """Inputs class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    maser_detection = vdp.VisDependentProperty(default=True)
    spws_to_smooth = vdp.VisDependentProperty(default=None)

    def __init__(self, context, vis=None, maser_detection=None, spws_to_smooth=None):
        """
        Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set

        """
        super(HanningInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.maser_detection = maser_detection
        self.spws_to_smooth = spws_to_smooth

        if self.spws_to_smooth is not None:
            self.spws_to_smooth = conversion.range_to_list(self.spws_to_smooth)


class HanningResults(basetask.Results):
    """Results class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

    The class inherits from basetask.Results

    """
    def __init__(self, final=None, pool=None, preceding=None, smoothed_spws=None):
        """
        Args:
            final(list): final list of tables (not used in this task)
            pool(list): pool list (not used in this task)
            preceding(list): preceding list (not used in this task)

        """
        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if smoothed_spws is None:
            smoothed_spws = {}

        super(HanningResults, self).__init__()

        self.vis = None
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.smoothed_spws = smoothed_spws

    def merge_with_context(self, context):
        """
        Args:
            context(:obj:): Pipeline context object
        """
        m = context.observing_run.measurement_sets[0]


@task_registry.set_equivalent_casa_task('hifv_hanning')
class Hanning(basetask.StandardTaskTemplate):
    """Class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

        The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = HanningInputs

    def prepare(self) -> Type[HanningResults]:
        """Method where the hanning smoothing operation is executed.

        The MS SPECTRAL_WINDOW table is examined to see if the SDM_NUM_BIN value is greater than 1.
        If the value is great than 1, then hanning smoothing does not proceed.

        The CASA task hanningsmooth() is executed on the data, creating a temporary measurement set (MS).
        The original MS is removed from disk, and the temporary MS is renamed to the original MS.
        An exception in thrown if an error occurs.

        Return:
            HanningResults() type object
        """

        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            if 'OFFLINE_HANNING_SMOOTH' in table.colnames():
                LOG.warning("MS has already had offline hanning smoothing applied. Skipping this stage.")
                return HanningResults()

        spws = self.inputs.context.observing_run.get_ms(self.inputs.vis).get_spectral_windows(science_windows_only=True)
        smoothing_dict = {}

        # Smooth input spws only if applicable. Overrides everything else
        # At this time, only used by hifv_restoredata and not exposed to the user
        if self.inputs.spws_to_smooth is not None:
            for spw in spws:
                if spw.id in self.inputs.spws_to_smooth:
                    smoothing_dict[spw.id] = (True, "restored smoothing")
                else:
                    smoothing_dict[spw.id] = (False, "")
        else:
            # Retrieve SPWs information and determine which to smooth
            if not self.inputs.maser_detection:
                LOG.info("Maser detection turned off.")

            # If any spws had online smoothing applied, do not smooth any spws
            if any([spw.sdm_num_bin > 1 for spw in spws]):
                for spw in spws:
                    smoothing_dict[spw.id] = (False, "online smoothing applied")
            else:
                for spw in spws:
                    smoothing_dict[spw.id] = (False, "")
                    if spw.specline_window:
                        if self.inputs.maser_detection and self._checkmaserline(str(spw.id)):
                            smoothing_dict[spw.id] = (True, "spectral line, maser line")
                        else:
                            smoothing_dict[spw.id] = (False, "spectral line")
                    else:
                        smoothing_dict[spw.id] = (True, "continuum")

        hs_dict = {}
        for key, val in smoothing_dict.items():
            hs_dict[key] = val[0]

        if not any(hs_dict.values()):
            LOG.info("None of the science spectral windows were selected for smoothing.")
        elif all(hs_dict.values()):
            LOG.info("All science spectral windows were selected for hanning smoothing")
            try:
                self._do_hanningsmooth()
                LOG.info("Removing original VIS " + self.inputs.vis)
                shutil.rmtree(self.inputs.vis)
                LOG.info("Renaming temphanning.ms to " + self.inputs.vis)
                os.rename('temphanning.ms', self.inputs.vis)
            except Exception as ex:
                LOG.warning('Problem encountered with hanning smoothing. ' + str(ex))
        else:
            smoothing_windows = [str(x) for x, y in hs_dict.items() if y]
            message = find_ranges(smoothing_windows)
            LOG.info("Smoothing spectral window(s) {}.".format(message))
            try:
                with casa_tools.MSReader(self.inputs.vis, nomodify=False) as mset:
                    staql = {'spw': ",".join(smoothing_windows)}
                    mset.msselect(staql)
                    mset.hanningsmooth('data')
            except Exception as ex:
                LOG.warning('Problem encountered with hanning smoothing. ' + str(ex))

        # Adding column to SPECTRAL_WINDOW table to indicate whether the SPW was smoothed (True) or not (False)
        self._track_hsmooth(hs_dict)

        return HanningResults(smoothed_spws=smoothing_dict)

    def analyse(self, results):
        """Determine the best parameters by analysing the given jobs before returning any final jobs to execute.

        Override method of basetask.StandardTaskTemplate.analyze()

        Args:
            jobs (list of class: `~pipeline.infrastructure.jobrequest.JobRequest`):
                the job requests generated by :func:`~SimpleTask.prepare`

        Returns:
            class:`~pipeline.api.Result`
        """
        return results

    def _do_hanningsmooth(self):
        """Execute the CASA task hanningsmooth

        Return:
            Executor class
        """

        task = casa_tasks.hanningsmooth(vis=self.inputs.vis,
                                        datacolumn='data',
                                        outputvis='temphanning.ms')

        return self._executor.execute(task)

    def _checkmaserline(self, spw: str) -> bool:
        """Confirm if known maser line(s) appear in frequency range of spectral window

        Args:
            spw(str): spectral window number

        Return: Boolean
            True if maser line may exist in window; False otherwise
        """
        LOG.debug("Checking for maser line contamination in spw {}.".format(spw))

        def freq_to_vel(rest_freq, obs_freq):
            c_kms = 2.99792458e5
            return ((rest_freq - obs_freq) / rest_freq) * c_kms

        maser_dict = {
            'OH (1)': 1612231000,
            'OH (2)': 1665401800,
            'OH (3)': 1667359000,
            'OH (4)': 1720530000,
            'H2O': 22235080000,
            'CH3OH (1)': 6668519200,
            'CH3OH (2)': 1217859700,
            'SiOv0': 43423858000,
            'SiOv1': 43122079000,
            'SiOv2': 42820582000,
            'SiOv3': 42519373000,
            '29SiOv0': 42879916000,
            '30SiOv0': 42373359000,
            'SiS': 18154880000,
        }

        qaTool = casa_tools.quanta
        suTool = casa_tools.synthesisutils

        for ms in self.inputs.context.observing_run.measurement_sets:
            if ms.name == self.inputs.vis:
                ms_info = ms
        
        spw_info = ms_info.get_spectral_window(spw)
        freq_low = spw_info._min_frequency.convert_to(newUnits=FrequencyUnits.HERTZ).value
        freq_high = spw_info._max_frequency.convert_to(newUnits=FrequencyUnits.HERTZ).value
        if spw_info._ref_frequency_frame != 'TOPO':
            LOG.info("Spectral window reference frame not TOPO. Skipping maser detection.")
            return False

        to_lsrk = suTool.advisechansel(msname=ms_info.name, spwselection=spw, getfreqrange=True, freqframe='LSRK')
        freq_low = float(qaTool.getvalue(qaTool.convert(to_lsrk['freqstart'], 'Hz')))
        freq_high = float(qaTool.getvalue(qaTool.convert(to_lsrk['freqend'], 'Hz')))
        LOG.debug("Freq low: {}; Freq high: {}".format(freq_low, freq_high))

        for value in maser_dict.values():
            vel_low = freq_to_vel(value, freq_low)
            vel_high = freq_to_vel(value, freq_high)
            if (freq_low <= value <= freq_high) or abs(vel_low) <= 200 or abs(vel_high) <= 200:
                LOG.info("Maser line possible in spw {}. Hanning smoothing will be applied.".format(spw))
                return True
        return False

    def _track_hsmooth(self, hs_dict: Dict[int, bool]):
        """Modify SPECTRAL_WINDOW table to track hanning smoothing

        Args:
            hs_dict(dict): hanning smoothing dictionary to write in SPECTRAL_WINDOW table
        """

        LOG.info("Writing Hanning smoothing information to SPECTRAL_WINDOW table of MS {}.".format(self.inputs.vis))
        desc = {'OFFLINE_HANNING_SMOOTH': {'comment': 'Offline Hanning Smooth Flag',
                            'dataManagerGroup': 'StandardStMan',
                            'dataManagerType': 'StandardStMan',
                            'keywords': {},
                            'maxlen': 0,
                            'option': 0,
                            'valueType': 'boolean'}}
        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW', nomodify=False) as tb:
            tb.addcols(desc)
            for spw, value in hs_dict.items():
                tb.putcell('OFFLINE_HANNING_SMOOTH', spw, value)
