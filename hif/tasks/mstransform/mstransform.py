import operator
import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


# Define the minimum set of parameters required to split out
# the TARGET data from the complete and fully calibrated
# original MS. Other parameters will be added here as more
# capabilities are added to hif_mstransform.
class MstransformInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    @vdp.VisDependentProperty
    def outputvis(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_targets.ms'

    @outputvis.convert
    def outputvis(self, value):
        if isinstance(value, str):
            return list(value.replace('[', '').replace(']', '').replace("'", "").split(','))
        else:
            return value

    # By default find all the fields with TARGET intent
    @vdp.VisDependentProperty
    def field(self):

        # Find fields in the current ms that have been observed
        # with the desired intent
        fields = self.ms.get_fields(intent=self.intent)

        # When an observation is terminated the final scan can be a TARGET scan but may
        # not contain any TARGET data for the science spectral windows, e.g. there is square
        # law detector data but nothing else. The following code removes fields that do not contain
        # data for the requested spectral windows - the science spectral windows by default.

        if fields:
            fields_by_id = sorted(fields, key=operator.attrgetter('id'))
            last_field = fields_by_id[-1]

            # While we're here, remove any fields that are not linked with a source. This should
            # not occur since the CAS-9499 tablereader bug was fixed, but check anyway.
            if getattr(last_field, 'source', None) is None:
                fields.remove(last_field)
                LOG.info('Truncated observation detected (no source for field): '
                         'removing Field {!s}'.format(last_field.id))
            else:
                # .. and then any fields that do not contain the
                # requested spectral windows. This should prevent
                # aborted scans from being split into the TARGET
                # measurement set.
                requested_spws = set(self.ms.get_spectral_windows(self.spw))
                if last_field.valid_spws.isdisjoint(requested_spws):
                    LOG.info('Truncated observation detected (missing spws): '
                             'removing Field {!s}'.format(last_field.id))
                    fields.remove(last_field)

        unique_field_names = {f.name for f in fields}
        field_ids = {f.id for f in fields}

        # Fields with different intents may have the same name. Check for this
        # and return the ids rather than the names if necessary to resolve any
        # ambiguities
        if len(unique_field_names) == len(field_ids):
            return ','.join(unique_field_names)
        else:
            return ','.join([str(i) for i in field_ids])

    # Select TARGET data by default
    intent = vdp.VisDependentProperty(default='TARGET')

    # Find all the spws with TARGET intent. These may be a subset of the
    # science spws which include calibration spws.
    @vdp.VisDependentProperty
    def spw(self):
        science_target_intents = set(self.intent.split(','))
        science_target_spws = []

        science_spws = [spw for spw in self.ms.get_spectral_windows(science_windows_only=True)]
        for spw in science_spws:
            if spw.intents.intersection(science_target_intents):
                science_target_spws.append(spw)

        return ','.join([str(spw.id) for spw in science_target_spws])

    @spw.convert
    def spw(self, value):
        science_target_intents = set(self.intent.split(','))
        science_target_spws = []

        science_spws = [spw for spw in self.ms.get_spectral_windows(task_arg=value, science_windows_only=True)]
        for spw in science_spws:
            if spw.intents.intersection(science_target_intents):
                science_target_spws.append(spw)

        return ','.join([str(spw.id) for spw in science_target_spws])

    chanbin = vdp.VisDependentProperty(default=1)
    timebin = vdp.VisDependentProperty(default='0s')

    def __init__(self, context, output_dir=None, vis=None, outputvis=None, field=None, intent=None, spw=None,
                 chanbin=None, timebin=None):

        super(MstransformInputs, self).__init__()

        # set the properties to the values given as input arguments
        self.context = context
        self.vis = vis
        self.output_dir = output_dir
        self.outputvis = outputvis
        self.field = field
        self.intent = intent
        self.spw = spw
        self.chanbin = chanbin
        self.timebin = timebin

    def to_casa_args(self):

        # Get parameter dictionary.
        d = super(MstransformInputs, self).to_casa_args()

        # Force the data column to be 'corrected' and the
        # new (with casa 4.6) reindex parameter to be False 
        d['datacolumn'] = 'corrected'
        d['reindex'] = False

        if self.chanbin > 1:
            d['chanaverage'] = True
        else:
            d['chanaverage'] = False

        if self.timebin > '0s':
            d['timeaverage'] = True
        else:
            d['timeaverage'] = False

        return d


@task_registry.set_equivalent_casa_task('hif_mstransform')
class Mstransform(basetask.StandardTaskTemplate):
    Inputs = MstransformInputs

    def prepare(self):
        inputs = self.inputs

        # Create the results structure
        result = MstransformResults(vis=inputs.vis, outputvis=inputs.outputvis)

        # Run CASA task
        mstransform_args = inputs.to_casa_args()
        mstransform_job = casa_tasks.mstransform(**mstransform_args)
        try:
            self._executor.execute(mstransform_job)
        except OSError as ee:
            LOG.warning(f"Caught mstransform exception: {ee}")

        # Copy across requisite XML files.
        self._copy_xml_files(inputs.vis, inputs.outputvis)

        return result

    def analyse(self, result):

        # Check for existence of the output vis. 
        if not os.path.exists(result.outputvis):
            LOG.debug('Error creating science targets cont+line MS %s' % (os.path.basename(result.outputvis)))
            return result

        # Import the new measurement set.
        to_import = os.path.relpath(result.outputvis)
        observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

        # Adopt same session as source measurement set
        for ms in observing_run.measurement_sets:
            LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
            ms.session = self.inputs.ms.session
            LOG.debug('Setting data_column and origin_ms.')
            ms.origin_ms = self.inputs.ms.origin_ms
            ms.set_data_column(DataType.REGCAL_CONTLINE_SCIENCE, 'DATA')

        result.mses.extend(observing_run.measurement_sets)

        return result

    @staticmethod
    def _copy_xml_files(vis, outputvis):
        for xml_filename in ['SpectralWindow.xml', 'DataDescription.xml']:
            vis_source = os.path.join(vis, xml_filename)
            outputvis_targets_contline = os.path.join(outputvis, xml_filename)
            if os.path.exists(vis_source) and os.path.exists(outputvis):
                LOG.info('Copying %s from original MS to science targets cont+line MS', xml_filename)
                LOG.trace('Copying %s: %s to %s', xml_filename, vis_source, outputvis_targets_contline)
                shutil.copyfile(vis_source, outputvis_targets_contline)


class MstransformResults(basetask.Results):
    def __init__(self, vis, outputvis):
        super(MstransformResults, self).__init__()
        self.vis = vis
        self.outputvis = outputvis
        self.mses = []

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

    def _make_template_flagfile(self, outfile, titlestr):
        # Create a new file if overwrite is true and the file
        # does not already exist.
        if not os.path.exists(outfile):
            template_text = FLAGGING_TEMPLATE_HEADER.replace('___TITLESTR___', titlestr)
            with open(outfile, 'w') as f:
                f.writelines(template_text)

    def __str__(self):
        # Format the Mstransform results.
        s = 'MstransformResults:\n'
        s += '\tOriginal MS {vis} transformed to {outputvis}\n'.format(
            vis=os.path.basename(self.vis),
            outputvis=os.path.basename(self.outputvis))

        return s

    def __repr__(self):
        return 'MstranformResults({}, {})'.format(os.path.basename(self.vis), os.path.basename(self.outputvis))


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
