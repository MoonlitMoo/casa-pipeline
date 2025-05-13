import pipeline.h.tasks.restoredata.restoredata as restoredata
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.applycal import applycal
from pipeline.infrastructure import task_registry
from ..importdata import almaimportdata

LOG = infrastructure.get_logger(__name__)


class ALMARestoreDataInputs(restoredata.RestoreDataInputs):
    asis = vdp.VisDependentProperty(
        default='SBSummary ExecBlock Antenna Annotation Station Receiver Source CalAtmosphere CalWVR CalPointing')

    def __init__(self, context, copytoraw=None, products_dir=None, rawdata_dir=None, output_dir=None, session=None,
                 vis=None, bdfflags=None, lazy=None, asis=None, ocorr_mode=None):
        super(ALMARestoreDataInputs, self).__init__(context, copytoraw=copytoraw, products_dir=products_dir,
                                                    rawdata_dir=rawdata_dir, output_dir=output_dir, session=session,
                                                    vis=vis, bdfflags=bdfflags, lazy=lazy, asis=asis,
                                                    ocorr_mode=ocorr_mode)


@task_registry.set_equivalent_casa_task('hifa_restoredata')
class ALMARestoreData(restoredata.RestoreData):
    Inputs = ALMARestoreDataInputs

    # Override generic method and use an ALMA specific one. Not much difference
    # now but should simplify parameters in future
    def _do_importasdm(self, sessionlist, vislist):
        inputs = self.inputs

        container = vdp.InputsContainer(almaimportdata.ALMAImportData, inputs.context, vis=vislist, session=sessionlist,
                                        save_flagonline=False, lazy=inputs.lazy, bdfflags=inputs.bdfflags,
                                        dbservice=False, asis=inputs.asis, ocorr_mode=inputs.ocorr_mode)
        importdata_task = almaimportdata.ALMAImportData(container)
        return self._executor.execute(importdata_task, merge=True)

    # Override generic method for an ALMA specific one.
    def _do_applycal(self):
        # PIPE-1165: for hifa_applycal, include the polarization intents.
        container = vdp.InputsContainer(applycal.SerialApplycal, self.inputs.context,
                                        intent='TARGET,PHASE,BANDPASS,AMPLITUDE,CHECK,POLARIZATION,POLANGLE,POLLEAKAGE')

        # PIPE-1973: check whether any of the caltables-to-be-applied were
        # produced by hifa_polcal, and if so, ensure that applycal is called
        # with parang=True.
        try:
            hifa_polcal_found = self._check_for_hifa_polcal_tables(container)
            if hifa_polcal_found:
                LOG.info("Found hifa_polcal produced caltable(s) to be applied: will call applycal with parang=True.")
                container.parang = True
        except:
            LOG.info("Unable to determine if any hifa_polcal produced caltable(s) are to be applied; will not modify"
                     " value for 'parang'.")

        # Create task, and return result from executing task.
        applycal_task = applycal.SerialApplycal(container)
        return self._executor.execute(applycal_task, merge=True)

    def _check_for_hifa_polcal_tables(self, inputs_container):
        """
        Utility method to determine whether a hifa_polcal produced caltable
        is present in the context callibrary.
        """
        # Perform check for each set of inputs (i.e. per MS).
        hifa_polcal_found = False
        for inputs in inputs_container:
            # Get the target data selection for this task as a CalTo object and
            # retrieve corresponding CalState.
            calto = callibrary.get_calto_from_inputs(inputs)
            calstate = self.inputs.context.callibrary.get_calstate(calto)

            # Determine whether any caltable-to-be-applied was created by
            # hifa_polcal.
            if any('.hifa_polcal.' in calfrom.gaintable for _, calfroms in calstate.merged().items()
                   for calfrom in calfroms):
                hifa_polcal_found = True
                break

        return hifa_polcal_found
