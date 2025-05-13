import os
import shutil

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hifv.heuristics import set_add_model_column_parameters
from pipeline.infrastructure import (casa_tasks, casa_tools, task_registry,
                                     utils)
from pipeline.infrastructure.contfilehandler import contfile_to_spwsel

LOG = infrastructure.get_logger(__name__)

# CALCULATE DATA WEIGHTS BASED ON ST. DEV. WITHIN EACH SPW
# use statwt


class StatwtInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    datacolumn = vdp.VisDependentProperty(default='corrected')
    overwrite_modelcol = vdp.VisDependentProperty(default=False)
    statwtmode = vdp.VisDependentProperty(default='VLA')

    @datacolumn.postprocess
    def datacolumn(self, unprocessed):
        if self.statwtmode == 'VLASS-SE' and unprocessed != 'residual_data':
            LOG.warning("Input datacolumn parameter is \'{}\', but the VLASS-SE default is \'residual_data\', "
                        "using default value.".format(unprocessed))
            return 'residual_data'
        else:
            return unprocessed

    def __init__(self, context, vis=None, datacolumn=None, overwrite_modelcol=None, statwtmode=None):
        super(StatwtInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.datacolumn = datacolumn
        self.overwrite_modelcol = overwrite_modelcol
        self.statwtmode = statwtmode


class StatwtResults(basetask.Results):
    def __init__(self, jobs=None, flag_summaries=[], wtables={}):

        if jobs is None:
            jobs = []

        super(StatwtResults, self).__init__()
        self.jobs = jobs
        self.summaries = flag_summaries
        self.wtables = wtables

    def __repr__(self):
        s = 'Statwt results:\n'
        for job in self.jobs:
            s += '%s performed. ' % str(job)
        return s


@task_registry.set_equivalent_casa_task('hifv_statwt')
class Statwt(basetask.StandardTaskTemplate):
    Inputs = StatwtInputs

    def prepare(self):

        if self.inputs.datacolumn == 'residual_data':
            LOG.info('Checking for model column')
            self._check_for_modelcolumn()

        if self.inputs.statwtmode not in ['VLA', 'VLASS-SE']:
            LOG.warning('Unkown mode \'%s\' was set. Known modes are [\'VLA\',\'VLASS-SE\']. '
                        'Continuing in \'VLA\' mode.' % self.inputs.statwtmode)
            self.inputs.statwtmode = 'VLA'

        fielddict = contfile_to_spwsel(self.inputs.vis, self.inputs.context)
        fields = ','.join(utils.fieldname_for_casa(x) for x in fielddict) if fielddict != {} else ''

        wtables = {}

        if self.inputs.statwtmode == 'VLASS-SE':
            wtables['before'] = self._make_weight_table(suffix='before')

        flag_summaries = []
        # flag statistics before task
        flag_summaries.append(self._do_flagsummary('before', field=fields))
        # actual statwt operation
        statwt_result = self._do_statwt(fielddict)
        # flag statistics after task
        flag_summaries.append(self._do_flagsummary('statwt', field=fields))

        wtables['after'] = self._make_weight_table(suffix='after')

        # Backup flag version after statwt was run
        job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='save', versionname='rfi_flagged_statwt', merge='replace', comment='flagversion after running hifv_statwt()')
        self._executor.execute(job)

        return StatwtResults(jobs=[statwt_result], flag_summaries=flag_summaries, wtables=wtables)

    def analyse(self, results):
        return results

    def _do_statwt(self, fielddict):

        if fielddict != {}:
            LOG.info('cont.dat file present.  Using VLA Spectral Line Heuristics for task statwt.')

        # VLA (default mode)
        # Note if default task_args changes, then 'vlass-se' case might need to be updated (PIPE-723)
        task_args = {'vis': self.inputs.vis,
                     'fitspw': '',
                     'fitcorr': '',
                     'combine': '',
                     'minsamp': 8,
                     'field': '',
                     'spw': '',
                     'datacolumn': self.inputs.datacolumn}
        # VLASS-SE
        if self.inputs.statwtmode == 'VLASS-SE':
            task_args['combine'] = 'field,scan,state,corr'
            task_args['minsamp'] = ''
            task_args['chanbin'] = 1
            task_args['timebin'] = '1yr'

        if fielddict == {}:
            job = casa_tasks.statwt(**task_args)
            return self._executor.execute(job)

        # cont.dat file present and need to execute by field and fitspw
        if fielddict != {}:
            for field in fielddict:
                task_args['fitspw'] = fielddict[field]
                task_args['field'] = field
                job = casa_tasks.statwt(**task_args)

                statwt_result = self._executor.execute(job)

            return statwt_result

    def _do_flagsummary(self, name, field=''):
        job = casa_tasks.flagdata(name=name, vis=self.inputs.vis, field=field, mode='summary')
        return self._executor.execute(job)

    def _check_for_modelcolumn(self):
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames() or self.inputs.overwrite_modelcol:
                LOG.info('Writing model data to {}'.format(ms.basename))
                imaging_parameters = set_add_model_column_parameters(self.inputs.context)
                job = casa_tasks.tclean(**imaging_parameters)
                tclean_result = self._executor.execute(job)
            else:
                LOG.info('Using existing MODEL_DATA column found in {}'.format(ms.basename))

    def _make_weight_table(self, suffix=''):

        stage_number = self.inputs.context.task_counter
        names = [os.path.basename(self.inputs.vis), 'hifv_statwt', 's'+str(stage_number), suffix, 'wts']
        outputvis = '.'.join(list(filter(None, names)))
        wtable = outputvis+'.tbl'

        isdir = os.path.isdir(outputvis)
        if isdir:
            shutil.rmtree(outputvis)

        if self.inputs.statwtmode == 'VLASS-SE':
            datacolumn = 'DATA'
        else:
            datacolumn = 'CORRECTED'

        task_args = {'vis': self.inputs.vis,
                     'outputvis': outputvis,
                     'spw': '*:0', # Channel 0 for all spwids
                     'datacolumn': datacolumn,
                     'keepflags': False}
        job = casa_tasks.split(**task_args)
        self._executor.execute(job)

        with casa_tools.MSMDReader(outputvis) as msmd:
            spws = msmd.spwfordatadesc(-1)

        with casa_tools.TableReader(outputvis, nomodify=False) as tb:
            for column in ['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM']:
                if column in tb.colnames():
                    tb.removecols(column)
            for spw in spws:
                stb = tb.query('DATA_DESC_ID=={0}'.format(spw))
                weights = stb.getcol('WEIGHT')
                weights_shape = weights.shape
                if weights.size > 0:
                    stb.putcol('DATA', np.reshape(weights, newshape=(weights_shape[0], 1, weights_shape[1])))
                    stb.putcol('WEIGHT', np.ones(weights_shape))
                    flag_row = stb.getcol('FLAG_ROW')
                    stb.putcol('FLAG', np.resize(flag_row, (weights_shape[0], 1, weights_shape[1])))
                stb.close()

        gaincal_spws = ','.join([str(s) for s in spws])

        job = casa_tasks.gaincal(vis=outputvis, caltable=wtable, solint='int',
                                 minsnr=0, calmode='ap', spw=gaincal_spws, append=False)
        self._executor.execute(job)
        return wtable
