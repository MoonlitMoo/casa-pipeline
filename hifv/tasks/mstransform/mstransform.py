import os
import shutil
import traceback

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
import pipeline.infrastructure.callibrary as callibrary
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.hif.tasks.mstransform import mstransform as mst

LOG = infrastructure.get_logger(__name__)


class VlaMstransformInputs(mst.MstransformInputs):

    @vdp.VisDependentProperty
    def outputvis(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_targets_cont.ms'

    @vdp.VisDependentProperty
    def outputvis_for_line(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_targets.ms'

    spw_line = vdp.VisDependentProperty(default='')
    omit_contline_ms = vdp.VisDependentProperty(default=False)

    def __init__(self, context, output_dir=None, vis=None, outputvis=None, field=None, intent=None, spw=None,
                 spw_line=None, chanbin=None, timebin=None, outputvis_for_line=None, omit_contline_ms=None):

        super().__init__(context, output_dir, vis, outputvis, field, intent, spw, chanbin, timebin)
        self.spw_line = spw_line
        self.outputvis = outputvis
        self.outputvis_for_line = outputvis_for_line
        self.omit_contline_ms = omit_contline_ms


@task_registry.set_equivalent_casa_task('hifv_mstransform')
class VlaMstransform(mst.Mstransform):
    Inputs = VlaMstransformInputs

    def prepare(self):
        inputs = self.inputs

        # Run CASA task to create the output MS for continuum data
        mstransform_args = inputs.to_casa_args()

        # Remove input member variables that don't belong as input to the mstransform task
        mstransform_args.pop('outputvis_for_line', None)
        mstransform_args.pop('spw_line', None)
        mstransform_args.pop('omit_contline_ms', None)
        mstransform_job = casa_tasks.mstransform(**mstransform_args)

        try:
            self._executor.execute(mstransform_job)
        except OSError as ee:
            LOG.warning(f"Caught mstransform exception: {ee}")

        # Copy across requisite XML files.
        mst.Mstransform._copy_xml_files(inputs.vis, inputs.outputvis)

        if not self.inputs.omit_contline_ms:
            # Create output MS for line data (_target.ms)
            self._create_targets_ms(inputs, mstransform_args)

        # Create the results structure
        result = VlaMstransformResults(vis=inputs.vis, outputvis=inputs.outputvis,
                                       outputvis_for_line=inputs.outputvis_for_line)

        return result

    def analyse(self, result):

        # Check for existence of the output vis.
        if not os.path.exists(result.outputvis):
            LOG.debug('Could not create science targets cont+line MS for continuum: %s' % (os.path.basename(result.outputvis)))
            return result

        # Check for existence of the output vis for line processing.
        if not os.path.exists(result.outputvis_for_line):
            LOG.info('Did not create science targets cont+line MS for line imaging: %s. Subsequent stages will not do line imaging.' % (os.path.basename(result.outputvis_for_line)))

        # Import the new measurement sets.
        try:
            to_import = os.path.relpath(result.outputvis)
            self._import_new_ms(result, to_import, datatype=DataType.REGCAL_CONT_SCIENCE)
            if os.path.exists(result.outputvis_for_line):
                to_import_for_line = os.path.relpath(result.outputvis_for_line)
                self._import_new_ms(result, to_import_for_line, datatype=DataType.REGCAL_CONTLINE_SCIENCE)
        except Exception:
            traceback.print_exc()
            msg = "Failed to import new measurement sets."
            raise Exception(msg)

        return result

    def _create_targets_ms(self, inputs, mstransform_args) -> bool:
        """
        Create _targets.ms for line imaging.

        This will be created if pre-RFI flags exist and there are spectral lines in 
        any spws.

        Returns True if targets.ms was created, else False.
        """
        produce_lines_ms = False

        # Split off non-RFI flagged target data
        # The main goal is to get an MS with the same shape as the _target.ms to
        # get the flags for non-RFI flagged data

        # Identify flags from before RFI flagging was applied
        pre_rfi_flagversion_name = None
        flags_list_task = casa_tasks.flagmanager(vis=inputs.vis, mode="list")
        flags_dict = self._executor.execute(flags_list_task)
        for value in flags_dict.values():
            if 'name' in value:
                if 'hifv_checkflag_target-vla' in value['name']:
                    pre_rfi_flagversion_name = value['name']

        if pre_rfi_flagversion_name is None:
            msg = "For {}: could not locate the pre-RFI flags to restore, so no targets.ms file will be created".format(inputs.vis)
            LOG.warning(msg)
            return False

        # Restore flags from before RFI flagging was applied
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=pre_rfi_flagversion_name)
        self._executor.execute(task)

        # Run CASA task to create the output MS for the line data
        mstransform_args['outputvis'] = inputs.outputvis_for_line

        if inputs.spw_line:
            mstransform_args['spw'] = inputs.spw_line
            produce_lines_ms = True
        else:  # check to see if any spws have been identified as spectral lines for this MS
            specline_spws = []
            for spw in inputs.ms.get_spectral_windows(science_windows_only=True):
                if spw.specline_window:
                    specline_spws.append(spw)
            if specline_spws:
                mstransform_args['spw'] = ','.join([str(spw.id) for spw in specline_spws])
                produce_lines_ms = True

        if produce_lines_ms:
            mstransform_job = casa_tasks.mstransform(**mstransform_args)

            try:
                self._executor.execute(mstransform_job)     
            except OSError as ee:
                LOG.warning(f"Caught mstransform exception: {ee}")

            # Save flags from line MS without rfi flagging
            task = casa_tasks.flagmanager(vis=inputs.outputvis_for_line, mode='save', versionname=pre_rfi_flagversion_name)
            self._executor.execute(task)

            # Copy across requisite XML files.
            mst.Mstransform._copy_xml_files(inputs.vis, inputs.outputvis_for_line)

        # Restore RFI flags to main MS
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname='rfi_flagged_statwt')
        self._executor.execute(task)

        return produce_lines_ms

    def _import_new_ms(self, result, to_import, datatype):
        observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

        # Adopt same session as source measurement set
        for ms in observing_run.measurement_sets:
            LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
            ms.session = self.inputs.ms.session
            LOG.debug('Setting data_column and origin_ms.')
            ms.origin_ms = self.inputs.ms.origin_ms
            ms.set_data_column(datatype, 'DATA')
            # Propagate spectral line spw designation from source MS
            for spw in ms.get_all_spectral_windows():
                spw.specline_window = self.inputs.ms.get_spectral_window(spw.id).specline_window
        result.mses.extend(observing_run.measurement_sets)


class VlaMstransformResults(mst.MstransformResults):
    def __init__(self, vis, outputvis, outputvis_for_line):
        super().__init__(vis, outputvis)
        self.outputvis_for_line = outputvis_for_line

    def merge_with_context(self, context):
        # Check for an output vis
        if not self.mses:
            LOG.error('No hif_mstransform results to merge')
            return

        target = context.observing_run

        # Adding mses to context
        for ms in self.mses:
            LOG.info('Adding {} to context'.format(ms.name))
            target.add_measurement_set(ms)

        # Create targets flagging template file if it does not already exist
        for ms in self.mses:
            template_flagsfile = os.path.join(
                self.inputs['output_dir'], os.path.splitext(os.path.basename(self.vis))[0] + '.flagtargetstemplate.txt')
            self._make_template_flagfile(template_flagsfile, 'User flagging commands file for the imaging pipeline')

        # Initialize callibrary
        for ms in self.mses:
            calto = callibrary.CalTo(vis=ms.name)
            LOG.info('Registering {} with callibrary'.format(ms.name))
            context.callibrary.add(calto, [])

    def __str__(self):
        # Format the Mstransform results.
        s = 'VlaMstransformResults:\n'
        s += '\tOriginal MS {vis} transformed to {outputvis} and {outputvis_for_line} \n'.format(
            vis=os.path.basename(self.vis),
            outputvis=os.path.basename(self.outputvis),
            outputvis_for_line=os.path.basename(self.outputvis_for_line))
        return s

    def __repr__(self):
        return 'VlaMstranformResults({}, {} + {})'.format(os.path.basename(self.vis), os.path.basename(self.outputvis),
                                                          os.path.basename(self.outputvis_for_line))


FLAGGING_TEMPLATE_HEADER = '''#
# ___TITLESTR___
#
# Examples
# Note: Do not put spaces inside the reason string !
#
# mode='manual' correlation='YY' antenna='DV01;DV08;DA43;DA48&DV23' spw='21:1920~2880' autocorr=False reason='bad_channels'
# mode='manual' spw='25:0~3;122~127' reason='stage8_2'
# mode='manual' antenna='DV07' timerange='2013/01/31/08:09:55.248~2013/01/31/08:10:01.296' reason='quack'
#
'''
