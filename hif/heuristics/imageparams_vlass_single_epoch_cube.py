from typing import Union, Tuple, Optional

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.domain.measures as measures
from .imageparams_vlass_single_epoch_continuum import ImageParamsHeuristicsVlassSeContMosaic

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVlassSeCube(ImageParamsHeuristicsVlassSeContMosaic):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None, linesfile=None, imaging_params={}):
        super().__init__(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CUBE'
        self.vlass_stage = 3

    def reffreq(self, deconvolver: Optional[str]=None, specmode: Optional[str]=None, spwsel: Optional[dict]=None) -> Optional[str]:
        """Tclean reffreq parameter heuristics.

        tclean(reffreq=None) will automatically calculate the referenece frequency using the mean frequency of the selected spws.
        For VLASS-SE-CONT, this is hardcoded to '3.0GHz'.
        For VLASS-SE-CUBE, PIPE-1401 requests this to be explicitly set as the central freq derived from individual SPW groups.
        None is returned here as a fallback and hif_editimlist() will set actual values.
        """
        return None

    def meanfreq_spwgroup(self, spw_selection):
        """Calculate the mean frequency of a spw group (specified by a selection string, e.g. '2,3,4')."""

        vis = self.vislist[0]
        ms = self.observing_run.get_ms(vis)
        spwid_list = [int(spw) for spw in spw_selection.split(',')]

        spwfreq_list = []
        for spwid in spwid_list:
            real_spwid = self.observing_run.virtual2real_spw_id(spwid, ms)
            spw = ms.get_spectral_window(real_spwid)
            spwfreq_list.append(float(spw.mean_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)))
        meanfreq_value = np.mean(spwfreq_list)

        return str(meanfreq_value)+'GHz'

    def flagpct_spwgroup(self, results_list: Union[list, None] = None, spw_selection=None):
        """Get the flag percentage of a spw group (specified by a selection string, e.g. '2,3,4').

        Note: this is a quick check using existing results from hifv_flagtargetsdata().
        More comprehensive (and expensive) check could be done using ImageParamsHeuristics.has_data() (also see PIPE-557)
        """

        flagpct = None

        # Catch exception as the success of result parsing is not guaranteed.
        try:
            if results_list and type(results_list) is list:
                for result in results_list:
                    result_meta = result.read()
                    if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                            'hifv_flagtargetsdata'):
                        flagtargets_summary = [r.summaries for r in result_meta][0][-1]

            n_flagged = n_total = 0.
            for spwid in [spw.strip() for spw in spw_selection.split(',')]:
                n_flagged += flagtargets_summary['spw'][str(spwid)]['flagged']
                n_total += flagtargets_summary['spw'][str(spwid)]['total']
            flagpct = n_flagged/n_total
        except Exception as e:
            pass

        return flagpct


    def mask(self, hm_masking=None, rootname=None, iteration=None, mask=None,
             results_list: Union[list, None] = None, clean_no_mask=None) -> Union[str, list]:
        """Tier-1 mask name to be used for computing Tier-1 and Tier-2 combined mask.

            Obtain the mask name from the latest MakeImagesResult object in context.results.
            If not found, then set empty string (as base heuristics)."""
        mask_list = ''
        if results_list and type(results_list) is list:
            for result in results_list:
                result_meta = result.read()
                if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                        'hifv_restorepims'):
                    mask_list = [r.mask_list for r in result_meta][0]

        # Add 'pb' string as a placeholder for cleaning without mask (pbmask only, see PIPE-977). This should
        # always stand at the last place in the mask list.
        # On request for first imaging stage (selfcal image) and automatically for the final imaging stage.
        if (clean_no_mask and self.vlass_stage == 1) or self.vlass_stage == 3:
            if clean_no_mask:
                LOG.info('Cleaning without user mask is performed in pre-self calibration imaging stage '
                         '(clean_no_mask_selfcal_image=True)')
            if type(mask_list) is list:
                mask_list.append('pb')
            elif mask_list != '':  # mask is non-empty string
                mask_list = [mask_list, 'pb']
            else:
                mask_list = 'pb'

        # In case hif_makeimages result was not found or results_list was not provided
        return mask_list

    def nterms(self, spwspec) -> Union[int, None]:
        """Tclean nterms parameter heuristics."""
        return 1

    def stokes(self, intent: str = '', joint_intents: str = '') -> str:
        """Tclean stokes parameter heuristics."""
        return 'IQUV'

    def psfcutoff(self) -> float:
        """Tclean psfcutoff parameter heuristics.

        PIPE-1466: use psfcutoff=0.5 to properly fit the PSF for all spws in the VLASS Coarse Cube pipeline,
        rather than the default value of 0.35 from CASA/tclean ver6.4.1.
        """
        return 0.5
