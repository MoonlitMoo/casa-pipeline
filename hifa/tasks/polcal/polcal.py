import operator
import os
from typing import List, Tuple, Union

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import measures
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import gaincal
from pipeline.hif.tasks import polcal
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import sessionutils
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'Polcal',
    'PolcalInputs',
    'PolcalResults',
]


class PolcalResults(basetask.Results):
    def __init__(self, vis=None):
        super().__init__()
        self.vis = vis
        self.session = {}

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        # Register results for each session into context.
        for session_results in self.session.values():
            session_results.merge_with_context(context)

    def __repr__(self):
        return 'PolcalResults'


class PolcalSessionResults(basetask.Results):
    def __init__(self, session=None, vis=None, final=None, pool=None, vislist=None, polcal_field_name=None, refant=None,
                 init_gcal_result=None, gain_ratio_rms_prior=None, uncal_pfg_result=None, best_scan_id=None,
                 kcross_result=None, polcal_phase_result=None, final_gcal_result=None, gain_ratio_rms_after=None,
                 cal_pfg_result=None, leak_polcal_result=None, xyratio_gcal_result=None, session_vs_result=None,
                 vis_vs_results=None, vs_diffs=None, polcal_amp_results=None):

        super().__init__()

        if final is None:
            final = []
        if pool is None:
            pool = []
        if vislist is None:
            vislist = []
        if uncal_pfg_result is None:
            uncal_pfg_result = {}
        if cal_pfg_result is None:
            cal_pfg_result = {}

        self.session = session
        self.vis = vis
        self.final = final
        self.pool = pool
        self.error = set()
        self.vislist = vislist
        self.polcal_field_name = polcal_field_name
        self.refant = refant
        self.init_gcal_result = init_gcal_result
        self.gain_ratio_rms_prior = gain_ratio_rms_prior
        self.uncal_pfg_result = uncal_pfg_result
        self.best_scan_id = best_scan_id
        self.kcross_result = kcross_result
        self.polcal_phase_result = polcal_phase_result
        self.final_gcal_result = final_gcal_result
        self.gain_ratio_rms_after = gain_ratio_rms_after
        self.cal_pfg_result = cal_pfg_result
        self.leak_polcal_result = leak_polcal_result
        self.xyratio_gcal_result = xyratio_gcal_result
        self.session_vs_result = session_vs_result
        self.vis_vs_results = vis_vs_results
        self.vs_diffs = vs_diffs
        self.polcal_amp_results = polcal_amp_results

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        # Register all CalApplications from each session.
        for calapp in self.final:
            LOG.debug(f'Adding calibration to callibrary:\n{calapp.calto}\n'
                      f'{calapp.calfrom}')
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        return 'PolcalSessionResults'


class PolcalInputs(vdp.StandardInputs):

    intent = vdp.VisDependentProperty(default='POLARIZATION,POLANGLE,POLLEAKAGE')
    minpacov = vdp.VisDependentProperty(default=30.0)
    solint_chavg = vdp.VisDependentProperty(default='5MHz')
    vs_stats = vdp.VisDependentProperty(default='min,max,mean')
    vs_thresh = vdp.VisDependentProperty(default=1e-3)

    def __init__(self, context, vis=None, intent=None, minpacov=None, solint_chavg=None, vs_stats=None, vs_thresh=None):
        self.context = context
        self.vis = vis
        self.intent = intent
        self.minpacov = minpacov
        self.solint_chavg = solint_chavg
        self.vs_stats = vs_stats
        self.vs_thresh = vs_thresh


@task_registry.set_equivalent_casa_task('hifa_polcal')
@task_registry.set_casa_commands_comment('Compute the polarization calibration.')
class Polcal(basetask.StandardTaskTemplate):
    Inputs = PolcalInputs

    # This is a multi-vis task that handles all MSes for one or more sessions
    # at once.
    is_multi_vis_task = True

    def prepare(self) -> PolcalResults:
        """
        Execute polarization calibration heuristics and return Results object
        that includes final caltables.

        Returns:
            PolcalResults instance.
        """
        # Initialize results.
        result = PolcalResults(vis=self.inputs.vis)

        # Inspect the vis list to identify sessions and corresponding MSes.
        vislist_for_session = sessionutils.group_vislist_into_sessions(self.inputs.context, self.inputs.vis)

        # Run polarization calibration for each session.
        for session_name, vislist in vislist_for_session.items():
            result.session[session_name] = self._polcal_for_session(session_name, vislist)

        return result

    def analyse(self, result: PolcalResults) -> PolcalResults:
        """
        Analyze the PolcalResults: check that all caltables from
        CalApplications exist on disk.

        Args:
            result: PolcalResults instance.

        Returns:
            PolcalResults instance.
        """
        # For each session, check that the caltables were all generated.
        for session_name, sresults in result.session.items():
            on_disk = [ca for ca in sresults.pool if ca.exists()]
            sresults.final[:] = on_disk

            missing = [ca for ca in sresults.pool if ca not in on_disk]
            sresults.error.clear()
            sresults.error.update(missing)
        return result

    def _polcal_for_session(self, session_name: str, vislist: List[str]) -> PolcalSessionResults:
        """
        Run polarization calibration heuristics for a single session and
        corresponding list of measurement sets.

        Args:
            session_name: name of session.
            vislist: list of measurement sets in session.

        Returns:
            PolcalSessionResults instance.
        """
        LOG.info(f"Deriving polarization calibration for session '{session_name}' with measurement set(s):"
                 f" {utils.commafy(vislist, quotes=False)}.")

        # Check that each MS in session shares the same single polarization
        # calibrator by field name; if not, then stop processing this session.
        polcal_field_name = self._check_matching_pol_field(session_name, vislist)
        if not polcal_field_name:
            return PolcalSessionResults(session=session_name)

        # Retrieve reference antenna for this session.
        refant = self._get_refant(session_name, vislist)

        # For each MS in session, run applycal to apply the registered total
        # intensity caltables to the polarization calibrator field.
        for vis in vislist:
            LOG.info(f"Session {session_name}: apply pre-existing caltables to polarization calibrator for MS {vis}.")
            self._run_applycal(vis)

        # Extract polarization data from each MS in session, concatenate into
        # a session MS, and register this session MS with local context.
        LOG.info(f"Creating polarization data MS for session '{session_name}'.")
        session_msname, spwmaps = self._create_session_ms(session_name, vislist)

        # Compute duration of polarization scans.
        scan_duration = self._compute_pol_scan_duration(session_msname)

        # Compute initial gain calibration for polarization calibrator, and
        # merge into local context.
        LOG.info(f"{session_msname}: compute initial gain calibration for polarization calibrator.")
        init_gcal_result = self._initial_gaincal(session_msname, refant)
        self._register_calapps_from_results([init_gcal_result])
        # Compute gain ratio RMS per scan (prior to polarization calibration).
        gain_ratio_rms_prior = self._compute_gain_ratio_rms(init_gcal_result)

        # Compute (uncalibrated) estimate of polarization of the polarization
        # calibrator.
        LOG.info(f"{session_msname}: compute estimate of polarization.")
        uncal_pfg_result = self._compute_polfromgain(session_msname, init_gcal_result, self.inputs.minpacov)
        # If polfromgain did not find any solutions, then log a warning and
        # return early (PIPE-2159).
        if not uncal_pfg_result or not list(uncal_pfg_result.values())[0]:
            LOG.warning(f"Unable to derive polarization calibration for session '{session_name}', no solutions returned"
                        f" by polfromgain.")
            return PolcalSessionResults(session=session_name)

        # Retrieve fractional Stokes results for averaged SpW for the
        # polarization calibrator field.
        smodel = list(uncal_pfg_result.values())[0]['SpwAve']

        # Identify scan with highest XY signal.
        best_scan_id = self._identify_scan_highest_xy(session_name, init_gcal_result)

        # Compute XY delay.
        LOG.info(f"{session_msname}: compute XY delay (Kcross) for polarization calibrator.")
        kcross_result, kcross_calapps = self._compute_xy_delay(session_msname, vislist, refant, best_scan_id, spwmaps)
        self._register_calapps_from_results([kcross_result])

        # Calibrate XY phase.
        LOG.info(f"{session_msname}: compute XY phase for polarization calibrator.")
        polcal_phase_result, pol_phase_calapps = self._calibrate_xy_phase(session_msname, vislist, smodel,
                                                                          scan_duration, spwmaps)

        # Retrieve fractional Stokes results for averaged SpW for the
        # polarization calibrator field.
        smodel = list(polcal_phase_result.polcal_returns[0].values())[0]['SpwAve']

        # Unregister caltables that have been created for the session MS so
        # far, prior to re-computing the gain calibration for polarization
        # calibrator.
        self._unregister_caltables(session_msname)

        # Final gain calibration for polarization calibrator, using the actual
        # polarization model.
        LOG.info(f"{session_msname}: compute final gain calibration for polarization calibrator.")
        final_gcal_result, final_gcal_calapps = self._final_gaincal(session_msname, vislist, refant, smodel, spwmaps)
        # Compute gain ratio RMS per scan (after polarization calibration).
        gain_ratio_rms_after = self._compute_gain_ratio_rms(final_gcal_result)

        # Recompute polarization of the polarization calibrator after
        # calibration.
        LOG.info(f"{session_msname}: recompute polarization of polarization calibrator after calibration.")
        cal_pfg_result = self._compute_polfromgain(session_msname, final_gcal_result, self.inputs.minpacov)
        if not cal_pfg_result:
            LOG.warning(f"Unable to derive polarization calibration for session '{session_name}', no solutions returned"
                        f" by polfromgain when recomputing polarization after calibration.")
            return PolcalSessionResults(session=session_name)

        # (Re-)register the final gain, XY delay, and XY phase caltables.
        self._register_calapps_from_results([final_gcal_result, kcross_result, polcal_phase_result])

        # Compute leakage terms.
        LOG.info(f"{session_msname}: estimate leakage terms for polarization calibrator.")
        leak_polcal_result, leak_pcal_calapps = self._compute_leakage_terms(session_msname, vislist, smodel,
                                                                            scan_duration, spwmaps)

        # Unregister caltables created so far, and re-register just the XY
        # delay, XY phase, and leakage term caltables, prior to computing the
        # X/Y ratio.
        self._unregister_caltables(session_msname)
        self._register_calapps_from_results([kcross_result, polcal_phase_result, leak_polcal_result])

        # Compute X/Y ratio.
        LOG.info(f"{session_msname}: compute X/Y ratio for polarization calibrator.")
        xyratio_gcal_result, xyratio_calapps = self._compute_xy_ratio(session_msname, vislist, refant, smodel, spwmaps)

        # Prior to applycal, re-register the final gain caltable.
        self._register_calapps_from_results([final_gcal_result])

        # Apply the polarization calibration to the polarization calibrator.
        LOG.info(f"{session_msname}: apply polarization calibrations to the polarization calibrator.")
        self._run_applycal(session_msname, parang=True)

        # Run visstat on session MS, once per obsid.
        LOG.info(f"{session_msname}: run visstat for session MS.")
        session_vs_result = {}
        for obsid in range(len(vislist)):
            session_vs_result[obsid] = self._run_visstat(session_msname, obsid=str(obsid))

        # Register the relevant CalApps for polarization calibrator in
        # each MS in this session.
        self._register_calapps(final_gcal_calapps + kcross_calapps + pol_phase_calapps + leak_pcal_calapps)

        # Run applycal to apply the newly derived polarization caltables to the
        # polarization calibrator in each MS in this session.
        for vis in vislist:
            LOG.info(f"Session {session_name}: apply polarization caltables to polarization calibrator for MS {vis}.")
            self._run_applycal(vis, parang=True)

        # Run visstat on each MS in this session.
        vis_vs_results = {}
        for vis in vislist:
            LOG.info(f"{session_msname}: run visstat for MS {vis}.")
            vis_vs_results[vis] = self._run_visstat(vis)

        # Compare results from visstat to log any differences exceeding the
        # threshold.
        LOG.info(f"{session_msname}: comparison of visstat results.")
        vs_diffs = self._compare_visstat_results(self.inputs.vs_stats, self.inputs.vs_thresh, session_vs_result,
                                                 vis_vs_results, spwmaps)

        # Set flux density for polarization calibrator in each MS in this
        # session.
        for vis in vislist:
            LOG.info(f"{session_msname}: run setjy for MS {vis}.")
            self._setjy_for_polcal(vis, smodel)

        # Compute amplitude calibration for polarization calibrator.
        polcal_amp_results, amp_calapps = self._compute_ampcal_for_polcal(vislist, refant)

        # Collect CalApplications.
        final_calapps = final_gcal_calapps + kcross_calapps + pol_phase_calapps + leak_pcal_calapps + \
            xyratio_calapps + amp_calapps

        # Collect results for session.
        result = PolcalSessionResults(
            session=session_name,
            vis=session_msname,
            pool=final_calapps,
            vislist=vislist,
            polcal_field_name=polcal_field_name,
            refant=refant,
            init_gcal_result=init_gcal_result,
            gain_ratio_rms_prior=gain_ratio_rms_prior,
            uncal_pfg_result=uncal_pfg_result,
            best_scan_id=best_scan_id,
            kcross_result=kcross_result,
            polcal_phase_result=polcal_phase_result,
            final_gcal_result=final_gcal_result,
            gain_ratio_rms_after=gain_ratio_rms_after,
            cal_pfg_result=cal_pfg_result,
            leak_polcal_result=leak_polcal_result,
            xyratio_gcal_result=xyratio_gcal_result,
            session_vs_result=session_vs_result,
            vis_vs_results=vis_vs_results,
            vs_diffs=vs_diffs,
            polcal_amp_results=polcal_amp_results,
        )

        return result

    def _get_refant(self, session_name: str, vislist: List[str]) -> str:
        # In the polarization recipes, the best reference antenna should have
        # been determined for the entire session by hifa_session_refant, and
        # stored in each MS of the session. Retrieve this refant from the first
        # MS in the MS list.
        ms = self.inputs.context.observing_run.get_ms(name=vislist[0])
        LOG.info(f"Session '{session_name}' is using reference antenna: {ms.reference_antenna}.")
        return ms.reference_antenna

    def _check_matching_pol_field(self, session_name: str, vislist: List[str]) -> str:
        # Retrieve polarization calibrator field name for each MS in session.
        pol_fields = {}
        for vis in vislist:
            ms = self.inputs.context.observing_run.get_ms(name=vis)
            pol_fields[vis] = [field.name for field in ms.get_fields(intent=self.inputs.intent)]

        # Check that each MS has one and only one polarization calibrator.
        pol_field_name = ''
        if not all(len(f) == 1 for f in pol_fields.values()):
            msg = f"Cannot process session '{session_name}': one or more measurement sets do not have exactly 1" \
                  f" polarization calibrator field."
            for vis, fields in pol_fields.items():
                msg += f"\n  {vis} has polarization calibrator field(s): {utils.commafy(fields)}"
            LOG.warning(msg)
        # Check that the polarization field for each MS matches by name.
        elif len({field for visfields in pol_fields.values() for field in visfields}) != 1:
            msg = f"Cannot process session '{session_name}': the measurement sets do not have the same polarization" \
                  f" calibrator (fields do not match by name)."
            for vis, fields in pol_fields.items():
                msg += f"\n  {vis} has polarization calibrator field: {fields}"
            LOG.warning(msg)
        # If no mismatch was found, then return the name of the single
        # polarization calibrator field.
        else:
            pol_field_name = pol_fields[vislist[0]][0]

        return pol_field_name

    def _run_applycal(self, vis: str, parang: bool = False):
        acinputs = applycal.IFApplycalInputs(context=self.inputs.context, vis=vis, intent=self.inputs.intent,
                                             parang=parang, flagsum=False, flagbackup=False, flagdetailedsum=False)
        actask = applycal.SerialIFApplycal(acinputs)
        self._executor.execute(actask)

    def _create_session_ms(self, session_name: str, vislist: List[str]) -> Tuple[str, dict]:
        """This method uses mstransform to create a new MS that contains only
        the polarization calibrator data."""
        # Extract polarization data for each vis, and capture name of new MS.
        pol_vislist = []
        for vis in vislist:
            LOG.info(f"Extracting corrected polarization data for MS {vis}")
            # Set name of output vis.
            outputvis = os.path.splitext(vis)[0] + '.polcalib.ms'

            # Retrieve science SpW(s) for current MS.
            ms = self.inputs.context.observing_run.get_ms(name=vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and execute mstransform job to split off the corrected
            # column for polarization intent.
            task_args = {
                'vis': vis,
                'intent': utils.to_CASA_intent(ms, self.inputs.intent),
                'outputvis': outputvis,
                'spw': sci_spws,
                'datacolumn': 'corrected',
                'reindex': False,
            }
            mstransform_job = casa_tasks.mstransform(**task_args)
            self._executor.execute(mstransform_job)

            pol_vislist.append(outputvis)

        # Concatenate the new polarization MSes into a single session MS.
        session_msname = session_name + '_polcalib.ms'
        LOG.info(f"Creating polarization session measurement set '{session_msname}' from input measurement set(s):"
                 f" {utils.commafy(pol_vislist, quotes=False)}.")
        concat_job = casa_tasks.concat(vis=pol_vislist, concatvis=session_msname)
        self._executor.execute(concat_job)

        # Initialize MS object and set its reference antenna to locked, to
        # enforce strict refantmode.
        session_ms = tablereader.MeasurementSetReader.get_measurement_set(session_msname)
        session_ms.reference_antenna_locked = True

        # Add session MS to local context.
        self.inputs.context.observing_run.add_measurement_set(session_ms)

        # Create SpW mapping from session MS to original input MSes.
        spwmaps = {}
        for vis in vislist:
            target_ms = self.inputs.context.observing_run.get_ms(name=vis)
            mapped = sessionutils.get_spwmap(session_ms, target_ms)
            spwmaps[vis] = list(range(max(mapped.values())+1))
            for k, v in mapped.items():
                spwmaps[vis][v] = k

        return session_msname, spwmaps

    def _compute_pol_scan_duration(self, vis: str) -> int:
        # Get polarization scans for session MS.
        ms = self.inputs.context.observing_run.get_ms(name=vis)
        pol_scans = ms.get_scans(scan_intent=self.inputs.intent)

        # Compute median duration of polarization scans.
        scan_duration = int(np.median([scan.time_on_source.total_seconds() for scan in pol_scans]))
        LOG.info(f"Session MS {vis}: median scan duration = {scan_duration} seconds.")
        return scan_duration

    def _initial_gaincal(self, vis: str, refant: str) -> gaincal.common.GaincalResults:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'solint': 'int',
            'refant': refant,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=self.inputs.intent, interp='linear')
        result.final = [new_calapp]

        return result

    def _register_calapps_from_results(self, results: List):
        """This method will register any "final" CalApplication present in any
        of the input Results to the callibrary in the local context (stored in
        inputs).

        Note: this is typically done through accepting the result into the
        relevant context. However, the framework does not allow merging a
        Results object into the same context multiple times. In this
        hifa_polcal task, the workflow requires unregistering / re-registering
        certain caltables, hence we use this worker method to do so.
        """
        # Collect CalApps to merge.
        calapps_to_merge = [calapp for result in results for calapp in result.final]
        self._register_calapps(calapps_to_merge)

    def _register_calapps(self, calapps: List):
        """This method will register a list of CalApplications to the
        callibrary in the local context (stored in inputs)."""
        for calapp in calapps:
            LOG.debug(f'Adding calibration to callibrary in task-specific context:\n{calapp.calto}\n{calapp.calfrom}')
            self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

    @staticmethod
    def _compute_gain_ratio_rms(result: gaincal.common.GaincalResults) -> Tuple[List, List]:
        # Get caltable to analyse.
        caltable = result.final[0].gaintable

        # Retrieve gains and scan from caltable.
        with casa_tools.TableReader(caltable) as table:
            scans = table.getcol('SCAN_NUMBER')
            gains = np.squeeze(table.getcol('CPARAM'))

        # Compute the gain ratio RMS for each scan.
        uniq_scans = sorted(set(scans))
        ratio_rms = np.zeros(len(uniq_scans))
        for ind, scanid in enumerate(uniq_scans):
            filt = np.where(scans == scanid)[0]
            ratio_rms[ind] = np.sqrt(np.average(np.power(np.abs(gains[0, filt]) / np.abs(gains[1, filt]) - 1.0, 2.)))

        return uniq_scans, list(ratio_rms)

    def _compute_polfromgain(self, vis: str, gcal_result: gaincal.common.GaincalResults, minpacov: float) -> dict:
        # Get caltable to analyse, and set name of output caltable.
        intable = gcal_result.final[0].gaintable
        caltable = os.path.splitext(intable)[0] + '_polfromgain.tbl'

        # Create and run polfromgain CASA task.
        pfg_job = casa_tasks.polfromgain(vis=vis, tablein=intable, caltable=caltable, minpacov=minpacov)
        pfg_result = self._executor.execute(pfg_job)

        return pfg_result

    @staticmethod
    def _identify_scan_highest_xy(session_name: str, gcal_result: gaincal.common.GaincalResults) -> int:
        # Get caltable to analyse.
        caltable = gcal_result.final[0].gaintable

        # Retrieve scan nr. and gains from initial polarization caltable.
        with casa_tools.TableReader(caltable) as table:
            scan_ids = table.getcol('SCAN_NUMBER')
            gains = np.squeeze(table.getcol('CPARAM'))

        # For each scan, derive the average XX/YY signal ratio.
        uniq_scan_ids = sorted(set(scan_ids))
        ratios = []
        for scan_id in uniq_scan_ids:
            scan_idx = scan_ids == scan_id
            ratios.append(
                np.sqrt(np.average(np.power(np.abs(gains[0, scan_idx]) / np.abs(gains[1, scan_idx]) - 1.0, 2.))))

        # Identify the scan with the best XY signal ratio as the scan where the
        # polarization signal is minimum in XX and YY.
        best_ratio_idx = np.argmin(ratios)
        best_scan_id = uniq_scan_ids[best_ratio_idx]
        LOG.info(f"Session {session_name} - scan with highest expected XY signal: {best_scan_id}.")

        return best_scan_id

    def _compute_xy_delay(self, vis: str, vislist: List[str], refant: str, best_scan: int, spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'scan': str(best_scan),
            'selectdata': True,  # needed when selecting on scan.
            'solint': ','.join(['inf', inputs.solint_chavg]),
            'smodel': [1, 0, 1, 0],
            'gaintype': 'KCROSS',
            'refant': refant,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _calibrate_xy_phase(self, vis: str, vislist: List[str], smodel: List[float], scan_duration: int,
                            spwmaps: dict) -> Tuple[polcal.polcalworker.PolcalWorkerResults, List]:
        inputs = self.inputs

        # Initialize polcal task inputs.
        task_args = {
            'vis': vis,
            'intent': inputs.intent,
            'solint': ','.join(['inf', inputs.solint_chavg]),
            'smodel': smodel,
            'combine': 'obs,scan',
            'poltype': 'Xfparang+QU',
            'preavg': scan_duration,
        }
        task_inputs = polcal.PolcalWorker.Inputs(inputs.context, **task_args)

        # Initialize and execute polcal task.
        task = polcal.PolcalWorker(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _unregister_caltables(self, vis: str):
        """
        This method will unregister from the callibrary in the local context
        (stored in inputs) any CalApplication that is registered for caltable
        produced so far during this hifa_polcal task.
        """
        # Define predicate function that matches the kind of caltable that
        # needs to be removed from the CalLibrary.
        def hifa_polcal_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            calto_vis = {os.path.basename(v) for v in calto.vis}
            do_delete = 'hifa_polcal' in calfrom.gaintable and vis in calto_vis
            if do_delete:
                LOG.debug(f'Unregistering caltable {calfrom.gaintable} from task-specific context.')
            return do_delete

        self.inputs.context.callibrary.unregister_calibrations(hifa_polcal_matcher)

    def _final_gaincal(self, vis: str, vislist: List[str], refant: str, smodel: List[float], spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'solint': 'int',
            'smodel': smodel,
            'refant': refant,
            'parang': True,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='linear')
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws,
                                                                intent=inputs.intent, interp='linear',
                                                                spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _compute_leakage_terms(self, vis: str, vislist: List[str], smodel: List[float], scan_duration: int,
                               spwmaps: dict) -> Tuple[polcal.polcalworker.PolcalWorkerResults, List]:
        inputs = self.inputs

        # Initialize polcal task inputs.
        task_args = {
            'vis': vis,
            'intent': inputs.intent,
            'solint': ','.join(['inf', inputs.solint_chavg]),
            'smodel': smodel,
            'combine': 'obs,scan',
            'poltype': 'Dflls',
            'preavg': scan_duration,
            'refant': '',
        }
        task_inputs = polcal.PolcalWorker.Inputs(inputs.context, **task_args)

        # Initialize and execute polcal task.
        task = polcal.PolcalWorker(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _compute_xy_ratio(self, vis: str, vislist: List[str], refant: str, smodel: List[float], spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'a',
            'intent': inputs.intent,
            'solint': 'inf',
            'smodel': smodel,
            'combine': 'obs,scan',
            'refant': refant,
            'solnorm': True,
            'parang': True,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against the non-polarization intents.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws,
                                                                intent='AMPLITUDE,BANDPASS,CHECK,PHASE,TARGET',
                                                                interp='nearest', spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _run_visstat(self, vis: str, obsid: Union[None, str] = None) -> dict:
        if obsid is None:
            obsid = ''

        # Retrieve science SpW(s) for vis.
        ms = self.inputs.context.observing_run.get_ms(name=vis)
        sci_spws = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        # Collect visstat results for each SpW in MS.
        vs_results = {}
        for sci_spw in sci_spws:
            # Create and run CASA visstat job.
            task_args = {
                'vis': vis,
                'intent': utils.to_CASA_intent(ms, self.inputs.intent),
                'spw': str(sci_spw),
                'datacolumn': 'corrected',
                'observation': obsid,
                'correlation': 'XY,YX',
            }
            visstat_job = casa_tasks.visstat(**task_args)
            visstat_result = self._executor.execute(visstat_job)
            vs_results[sci_spw] = list(visstat_result.values())[0]

        return vs_results

    @staticmethod
    def _compare_visstat_results(stats: str, threshold: float, session_vs_results: dict, vis_vs_results: dict,
                                 spwmaps: dict) -> dict:
        # Define function for determining the difference in given visstat
        # derived statistic between session MS and individual MS.
        def compute_diff(vres, sres, st):
            return abs(vres[st] - sres[st]) / sres[st]

        # Collect difference to put in task result.
        diffs = {}
        stats_to_compare = stats.split(",")

        # Compare visstat results for each observation id in the session MS
        # with the corresponding individual MS.
        for obsid, (vis, vis_vs_result) in enumerate(vis_vs_results.items()):
            session_vs_result = session_vs_results[obsid]
            diffs[vis] = {}
            for spw_id, visres in vis_vs_result.items():
                for stat in stats_to_compare:
                    diffs[vis][stat] = compute_diff(visres, session_vs_result[spwmaps[vis][spw_id]], stat)

                    # If the relative difference is above the threshold, then
                    # report the difference to the CASA log.
                    if diffs[vis][stat] > threshold:
                        LOG.info(f"Large relative difference found in comparison of visstat results for session MS and"
                                 f" {vis}, SpW {spw_id}, statistic '{stat}': {diffs[vis][stat]}.")

        return diffs

    def _setjy_for_polcal(self, vis: str, smodel: List[float]):
        # Get pol calibrator field ID and science SpWs from MS.
        ms = self.inputs.context.observing_run.get_ms(name=vis)
        sci_spws = ms.get_spectral_windows(science_windows_only=True)
        # This assumes that there is one and only one polcal field.
        polcal_field = ms.get_fields(intent=self.inputs.intent)[0]

        # Get imported flux density measurements (populated during importdata
        # stage).
        import_fluxes = sorted(polcal_field.flux_densities, key=operator.attrgetter('spw_id'))

        # Check for presence of fluxscale derived flux measurements in current
        # MS. These should have been populated by the hifa_gfluxscale stage. If
        # not present, log warning and return without running setjy.
        if ms.fluxscale_fluxes is None:
            LOG.warning(f"No fluxscale derived flux measurements registered for {vis}, cannot retrieve total"
                        f" intensity, skipping setjy step for polarization calibrator in this MS.")
            return

        # Get fluxscale derived flux measurements for the polarization
        # calibrator field in the current MS
        gfs_fluxes = sorted(ms.fluxscale_fluxes[str(polcal_field.id)], key=operator.attrgetter('spw_id'))

        # Create a separate job for each SpW, to use corresponding reffreq.
        for ind, sci_spw in enumerate(sci_spws):
            # Get the spectral index for current SpW from imported flux
            # density measurements.
            spix = float(import_fluxes[ind].spix)

            # Get fluxes for current SpW.
            iquv = gfs_fluxes[ind].casa_flux_density

            # For setjy command, use total intensity from hifa_gfluxscale stage,
            # and use QUV from earlier in this stage but scaled by total intensity.
            for i in range(3):
                iquv[i + 1] = smodel[i + 1] * iquv[0]

            # Set reference frequency to the central frequency of this SpW.
            reffreq = f"{sci_spw.centre_frequency.convert_to(measures.FrequencyUnits.GIGAHERTZ).value}GHz"

            # Initialize setjy task inputs.
            task_args = {
                'vis': vis,
                'spw': str(sci_spw.id),
                'selectdata': True,
                'intent': utils.to_CASA_intent(ms, self.inputs.intent),
                'scalebychan': True,
                'standard': 'manual',
                'fluxdensity': iquv,
                'spix': spix,
                'reffreq': reffreq,
                'usescratch': True,
            }
            job = casa_tasks.setjy(**task_args)
            self._executor.execute(job)

    def _compute_ampcal_for_polcal(self, vislist: List[str], refant: str) \
            -> Tuple[List[gaincal.common.GaincalResults], List]:
        inputs = self.inputs

        results = []
        final_calapps = []
        for vis in vislist:
            # Initialize gaincal task inputs.
            task_args = {
                'output_dir': inputs.output_dir,
                'vis': vis,
                'calmode': 'a',
                'intent': inputs.intent,
                'solint': 'inf',
                'gaintype': 'T',
                'refant': refant,
                'parang': True,
            }
            task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

            # Initialize and execute gaincal task.
            task = gaincal.GTypeGaincal(task_inputs)
            result = self._executor.execute(task)
            results.append(result)

            # Create a modified CalApplication to register this caltable
            # against the polarization intents.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=vis, intent=inputs.intent,
                                                                interp='nearest'))

        return results, final_calapps
