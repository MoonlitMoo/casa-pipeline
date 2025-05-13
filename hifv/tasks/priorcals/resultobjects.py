import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask

LOG = infrastructure.get_logger(__name__)


class PriorcalsResults(basetask.Results):
    """Results class for the hifv_priorcals pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.Results.

    """
    def __init__(self, final=None, pool=None, preceding=None, gc_result=None, oc_result=None,
                 rq_result=None,  antpos_result=None, antcorrect=None, tecmaps_result=None, sw_result=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(PriorcalsResults, self).__init__()

        self.vis = None
        self.pool = pool
        self.final = final
        self.preceding = preceding
        self.error = set()

        self.gc_result = gc_result
        self.oc_result = oc_result
        self.rq_result = rq_result
        self.sw_result = sw_result
        self.antpos_result = antpos_result
        self.antcorrect = antcorrect
        self.tecmaps_result = tecmaps_result

    def merge_with_context(self, context):
        if self.gc_result and self.inputs["apply_gaincurves"]:
            try:
                self.gc_result.merge_with_context(context)
                LOG.info("Priorcals:  Merged gain curves cal")
            except Exception as ex:
                LOG.warning("No gain curves table written.")

        if self.oc_result and self.inputs["apply_opcal"]:
            try:
                self.oc_result.merge_with_context(context)
                LOG.info("Priorcals:  Merged Opac cal")
            except Exception as ex:
                LOG.warning("No opacities table written.")

        if self.rq_result and self.inputs["apply_rqcal"]:
            try:
                self.rq_result.merge_with_context(context)
                LOG.info("Priorcals:  Merged Requantizer gains")
            except Exception as ex:
                LOG.warning("No rq gains table written.")

        if self.antpos_result and self.inputs["apply_antpos"]:
            try:
                self.antpos_result.merge_with_context(context)
                LOG.info("Priorcals:   Merged Antenna positions corrections.")
            except Exception as ex:
                LOG.warning('No antenna position corrections.')

        if self.tecmaps_result:
            if self.tecmaps_result.final and self.tecmaps_result.pool:
                try:
                    self.tecmaps_result.merge_with_context(context)
                    LOG.info("Priorcals:  Merged TEC Maps.")
                except Exception as ex:
                    LOG.warning('No TEC Maps table written.')
            else:
                LOG.info('Priorcals:  TEC maps not applied.')

        if self.sw_result and self.inputs["apply_swpowcal"]:
            try:
                self.sw_result.merge_with_context(context)
                LOG.info("Priorcals: Merged Switched Power caltable")
            except Exception as ex:
                LOG.warning('No Switched Power table written.')

        return        
        # if not self.final:
        #     LOG.error('No results to merge')
        #     return

        # for calapp in self.final:
        #     LOG.debug('Adding calibration to callibrary:\n'
        #               '%s\n%s' % (calapp.calto, calapp.calfrom))
        #     context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):

        # Format the Priorcal results text output.
        s = 'Priorcal Results:\n'
        for calapplication in self.final:
            s += '\tBest caltable for spw #{spw} in {vis} is {name}\n'.format(
                spw=calapplication.spw, vis=os.path.basename(calapplication.vis),
                name=calapplication.gaintable)
        return s
