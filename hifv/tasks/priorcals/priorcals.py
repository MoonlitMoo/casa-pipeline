"""
Example usage:

inputs = pipeline.vla.tasks.priorcals.Priorcals.Inputs(context)
task = pipeline.vla.tasks.priorcals.Priocals(inputs)
result = task.execute()
result.accept(context)

"""
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.tasks.gaincurves import GainCurves
from pipeline.hifv.tasks.opcal import Opcal
from pipeline.hifv.tasks.rqcal import Rqcal
from pipeline.hifv.tasks.swpowcal import Swpowcal
from pipeline.hifv.tasks.tecmaps import TecMaps
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from casatasks.private.correct_ant_posns_evla import correct_ant_posns_evla as correct_ant_posns
from . import resultobjects
from . import vlaantpos


LOG = infrastructure.get_logger(__name__)


class PriorcalsInputs(vdp.StandardInputs):
    """Inputs class for the hifv_priorcals pipeline task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    swpow_spw = vdp.VisDependentProperty(default='')
    show_tec_maps = vdp.VisDependentProperty(default=True)
    apply_tec_correction = vdp.VisDependentProperty(default=False)
    apply_gaincurves = vdp.VisDependentProperty(default=True)
    apply_opcal = vdp.VisDependentProperty(default=True)
    apply_rqcal = vdp.VisDependentProperty(default=True)
    apply_antpos = vdp.VisDependentProperty(default=True)
    apply_swpowcal = vdp.VisDependentProperty(default=False)
    ant_pos_time_limit = vdp.VisDependentProperty(default=150)

    def __init__(self, context, vis=None, show_tec_maps=None, apply_tec_correction=None, apply_gaincurves=None, apply_opcal=None,
                 apply_rqcal=None, apply_antpos=None, apply_swpowcal=None, swpow_spw=None, ant_pos_time_limit=None):
        """
        Args:
            context (:obj:): Pipeline context
            vis(str):  Measurement set
            show_tec_maps(bool):    Display the plot output from the CASA tec_maps recipe function
            apply_tec_correction:   CASA tec_maps recipe function is executed - this bool determines if gencal is
                                    executed and the resulting table applied
            apply_gaincurves(bool): Flag to apply gain curves correction. Default is True.
            apply_opcal(bool):      Flag to apply opacities correction. Default is True.
            apply_rqcal(bool):      Flag to apply requantizer gains correction. Default is True.
            apply_antpos(bool):     Flag to apply antenna position correction. Default is True.
            apply_swpowcal(bool):   Flag to apply switched power table. Default is False. If set True,
                                    apply_rqcal is ignored and no requantizer gain correction will be applied.
            swpow_spw(str):         spws for switched power
            ant_pos_time_limit(int): Antenna position time limit in days. Default is 150 days.
        """
        self.context = context
        self.vis = vis
        self.show_tec_maps = show_tec_maps
        self.apply_tec_correction = apply_tec_correction
        self.swpow_spw = swpow_spw
        self.ant_pos_time_limit = ant_pos_time_limit
        self.apply_gaincurves = apply_gaincurves
        self.apply_opcal = apply_opcal
        self.apply_rqcal = apply_rqcal
        self.apply_antpos = apply_antpos
        self.apply_swpowcal = apply_swpowcal
        # PIPE-1665: comment from the ticket
        # If I can added a requirement, there should also be the ability to apply the switched power table.
        # The default for the switched power table should be to not apply it. However, if a user sets application
        # of switched power to true, this should implicity turn off the requantizer table, because switched power
        # includes the requantizer gain information implicitly.
        if apply_swpowcal:
            self.apply_rqcal = False

    def to_casa_args(self):
        raise NotImplementedError


@task_registry.set_equivalent_casa_task('hifv_priorcals')
class Priorcals(basetask.StandardTaskTemplate):
    """Class for the Priorcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = PriorcalsInputs

    def prepare(self):

        callist = []

        gc_result = self._do_gaincurves()
        oc_result = self._do_opcal()
        rq_result = self._do_rqcal()
        sw_result = self._do_swpowcal()
        antpos_result, antcorrect = self._do_antpos()
        tecmaps_result = None
        if self.inputs.show_tec_maps or self.inputs.apply_tec_correction:
            tecmaps_result = self._do_tecmaps(show_tec_maps=self.inputs.show_tec_maps,
                                              apply_tec_correction=self.inputs.apply_tec_correction)

        # try:
        #    antpos_result.merge_withcontext(self.inputs.context)
        # except:
        #    LOG.error('No antenna position corrections.')

        return resultobjects.PriorcalsResults(pool=callist, gc_result=gc_result,
                                              oc_result=oc_result, rq_result=rq_result,
                                              antpos_result=antpos_result, antcorrect=antcorrect,
                                              tecmaps_result=tecmaps_result, sw_result=sw_result)

    def analyse(self, results):
        return results

    def _do_gaincurves(self):
        """Run gaincurves task"""

        inputs = GainCurves.Inputs(self.inputs.context, vis=self.inputs.vis)
        task = GainCurves(inputs)
        return self._executor.execute(task)

    def _do_opcal(self):
        """Run opcal task"""

        inputs = Opcal.Inputs(self.inputs.context, vis=self.inputs.vis)
        task = Opcal(inputs)
        return self._executor.execute(task)

    def _do_rqcal(self):
        """Run requantizer gains task"""

        inputs = Rqcal.Inputs(self.inputs.context, vis=self.inputs.vis)
        task = Rqcal(inputs)
        return self._executor.execute(task)

    def _do_swpowcal(self):
        """Run switched power task"""
        inputs = Swpowcal.Inputs(self.inputs.context, vis=self.inputs.vis, spw=self.inputs.swpow_spw)
        task = Swpowcal(inputs)
        return self._executor.execute(task)

    def _do_antpos(self):
        """Run hif_antpos to correct for antenna positions"""
        inputs = vlaantpos.VLAAntpos.Inputs(self.inputs.context, vis=self.inputs.vis, ant_pos_time_limit=self.inputs.ant_pos_time_limit)
        task = vlaantpos.VLAAntpos(inputs)
        result = self._executor.execute(task)

        antcorrect = {}

        try:
            antpos_caltable = result.final[0].gaintable
            if os.path.exists(antpos_caltable):
                LOG.info("Start antenna position corrections")
                antparamlist = correct_ant_posns(inputs.vis, print_offsets=False, time_limit=inputs.ant_pos_time_limit)
                LOG.info("End antenna position corrections")

                self._check_tropdelay(antpos_caltable)

                antList = antparamlist[1].split(',')
                N = 3
                subList = [antparamlist[2][n:n+N] for n in range(0, len(antparamlist[2]), N)]
                antcorrect = dict(zip(antList, subList))
        except Exception as ex:
            LOG.info("No offsets found. No caltable created.")
            LOG.debug(ex)

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        fracantcorrect = float(len(antcorrect)) / float(len(m.antennas))
        if fracantcorrect > 0.5:
            LOG.warning("{:5.2f} percent of antennas needed position corrections.".format(100.0 * fracantcorrect))

        return result, antcorrect

    def _do_tecmaps(self, show_tec_maps=True, apply_tec_correction=False):
        """Run tec_maps function"""

        inputs = TecMaps.Inputs(self.inputs.context, vis=self.inputs.vis, output_dir='', show_tec_maps=show_tec_maps,
                                apply_tec_correction=apply_tec_correction)
        task = TecMaps(inputs)
        return self._executor.execute(task)

    def _check_tropdelay(self, antpos_caltable):

        # Insert value if required for testing

        """
        #print "ADDED TEST TROP VALUE"
        trdelscale = 1.23
        tb = casa_tools.table()
        tb.open(antpos_caltable, nomodify=False)
        tb.putkeyword('VLATrDelCorr', trdelscale)
        tb.close()
        #print "END OF ADDING TEST TROP VALUE"
        """

        # Detect EVLA 16B Trop Del Corr
        # (Silent if required keyword absent, or has value=0.0)
        # antpostable = 'cal.antpos'
        trdelkw = 'VLATrDelCorr'
        with casa_tools.TableReader(antpos_caltable) as tb:
            if tb.keywordnames().count(trdelkw) == 1:
                trdelscale = tb.getkeyword(trdelkw)
                if trdelscale != 0.0:
                    warning_message = "NB: This EVLA dataset appears to fall within the period of semester 16B " \
                                      "during which the online tropospheric delay model was mis-applied. " \
                                      "A correction for the online tropospheric delay model error WILL BE APPLIED!  " \
                                      "Tropospheric delay error correction coefficient="+str(-trdelscale/1000.0)+ " (ps/m) "
                    LOG.debug("EVLA 16B Online Trop Del Corr is ON, scale=" + str(trdelscale))
                    LOG.warning(warning_message)
