import os
import shutil
import tarfile
from collections import namedtuple

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.tasks.makeimlist import makeimlist
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure.utils import nested_dict

LOG = infrastructure.get_logger(__name__)


class UVcontSubInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    fitorder = vdp.VisDependentProperty(default={})
    intent = vdp.VisDependentProperty(default='TARGET')
    field = vdp.VisDependentProperty(default='')
    spw = vdp.VisDependentProperty(default='')
    parallel = sessionutils.parallel_inputs_impl(default=False)

    def __init__(self, context, output_dir=None, vis=None, field=None,
                 spw=None, intent=None, fitorder=None, parallel=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.field = field
        self.spw = spw
        self.intent = intent
        self.fitorder = fitorder
        self.parallel = parallel


class SerialUVcontSub(basetask.StandardTaskTemplate):
    Inputs = UVcontSubInputs

    def prepare(self):
        inputs = self.inputs

        # Define minimal entity object to be able to use some
        # of the imaging heuristics methods to convert
        # frequency ranges to TOPO.
        MinimalTcleanHeuristicsInputsGenerator = namedtuple('MinimalTcleanHeuristicsInputs', 'vis field intent phasecenter spw spwsel_lsrk specmode')

        # Check if this stage has been disabled for vla (never set for ALMA)
        if inputs.context.vla_skip_mfs_and_cube_imaging:
            result = UVcontSubResults()
            result.skip_stage = True
            return result

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result = UVcontSubResults()
                result.mitigation_error = True
                return result

        # If no field and no spw is specified, use all TARGET intents except
        # mitigated sources.
        # If field or spw is specified, work on that selection

        # Determine intent(s) to work on
        allowed_intents = ('TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE')
        if inputs.intent not in (None, ''):
            if all(i.strip() in allowed_intents for i in inputs.intent.split(',')):
                intent = inputs.intent.replace(' ', '')
            else:
                result = UVcontSubResults()
                result.error = True
                result.error_msg = f'"intent" must be in {allowed_intents}'
                LOG.error(result.error_msg)
                return result
        else:
            intent = 'TARGET'

        # Determine field IDs to work on
        if inputs.field not in (None, ''):
            field = inputs.field
        else:
            field = ''

        # Determine spw IDs to work on
        if inputs.spw not in (None, ''):
            spw = inputs.spw
        else:
            spw = ''

        # Set fitorder lookup dictionary
        if inputs.fitorder not in (None, ''):
            fitorder = inputs.fitorder
        else:
            fitorder = nested_dict()

        # Optionally recover cont.dat
        self._precheck_contdat()

        known_synthesized_beams = inputs.context.synthesized_beams

        datatype = None
        for possible_datatype in inputs.processing_data_type:
            if possible_datatype in inputs.ms.data_column:
                datatype = possible_datatype.name
                break

        # Get list of fields and spw to work on from makeimlist call
        # which automatically handles mitigated sources. spw mitigation
        # shall not be considered, hence the specmode is mfs.

        # Create makeimlist inputs
        makeimlist_inputs = makeimlist.MakeImListInputs(inputs.context, vis=[inputs.vis])
        makeimlist_inputs.datatype = datatype
        makeimlist_inputs.field = field
        makeimlist_inputs.intent = intent
        makeimlist_inputs.spw = spw
        makeimlist_inputs.specmode = 'mfs'
        makeimlist_inputs.clearlist = True
        makeimlist_inputs.known_synthesized_beams = known_synthesized_beams

        # Create imlist
        makeimlist_task = makeimlist.MakeImList(makeimlist_inputs)
        makeimlist_result = makeimlist_task.prepare()
        imlist = makeimlist_result.targets

        # Collect datacolumn, fields, spws and fit specifications
        fields = dict()
        real_spws = dict()
        fitspec = nested_dict()
        # Keep list of actual field/intent/spw combinations in hif_makeimlist
        # order for weblog. Avoid saving the full list in the results object
        # because of the large heuristics objects which would bloat the context.
        field_intent_spw_list = []
        topo_freq_fitorder_dict = nested_dict()
        for imaging_target in imlist:
            datacolumn = imaging_target['datacolumn']

            # Using virtual spws for task parameter and tclean heuristics calls.
            # Need to specify real spws for uvcontsub2021.
            real_spw = str(inputs.context.observing_run.virtual2real_spw_id(imaging_target['spw'], inputs.ms))

            minimal_tclean_inputs = MinimalTcleanHeuristicsInputsGenerator(imaging_target['vis'],
                                                                           imaging_target['field'],
                                                                           imaging_target['intent'],
                                                                           imaging_target['phasecenter'],
                                                                           imaging_target['spw'],
                                                                           imaging_target['spwsel_lsrk'],
                                                                           imaging_target['specmode'])

            fields[minimal_tclean_inputs.field] = True
            real_spws[real_spw] = True
            field_intent_spw_list.append({'field': imaging_target['field'],
                                          'intent': imaging_target['intent'],
                                          'spw': real_spw})

            # Convert the cont.dat frequency ranges to TOPO
            (_, _, spw_topo_freq_param_dict, _, _, _, _) = imaging_target['heuristics'].calc_topo_ranges(minimal_tclean_inputs)

            field_ids = imaging_target['heuristics'].field(minimal_tclean_inputs.intent, minimal_tclean_inputs.field)[0]

            fitspec[field_ids][real_spw]['chan'] = spw_topo_freq_param_dict[minimal_tclean_inputs.vis[0]][minimal_tclean_inputs.spw]

            # Collect frequency ranges for weblog
            topo_freq_fitorder_dict[minimal_tclean_inputs.field][real_spw]['freq'] = spw_topo_freq_param_dict[minimal_tclean_inputs.vis[0]][minimal_tclean_inputs.spw]

            # Default fit order
            fitspec[field_ids][real_spw]['fitorder'] = 1

            # Check for any user specified fit order.
            user_fitorder = False
            if minimal_tclean_inputs.field in fitorder:
                if minimal_tclean_inputs.spw in fitorder[minimal_tclean_inputs.field]:
                    fitspec[field_ids][real_spw]['fitorder'] = fitorder[minimal_tclean_inputs.field][minimal_tclean_inputs.spw]
                    user_fitorder = True

            # If there was no user defined fit order, check for hif_findcont flags.
            if not user_fitorder:
                if imaging_target['spwsel_low_bandwidth'] or imaging_target['spwsel_low_spread']:
                    fitspec[field_ids][real_spw]['fitorder'] = 0

            # Collect fit order for weblog
            topo_freq_fitorder_dict[minimal_tclean_inputs.field][real_spw]['fitorder'] = fitspec[field_ids][real_spw]['fitorder']

        result = UVcontSubResults()
        result.field_intent_spw_list = field_intent_spw_list
        result.topo_freq_fitorder_dict = topo_freq_fitorder_dict

        if '_targets' in inputs.vis:
            outputvis = inputs.vis.replace('_targets', '_targets_line')
        else:
            outputvis = f"{inputs.vis.split('.ms')[0]}_line.ms"
        # Check if it already exists and remove it
        if os.path.exists(outputvis):
            LOG.info('Removing {} from disk'.format(outputvis))
            shutil.rmtree(outputvis)

        # Run uvcontsub task
        uvcontsub_args = {'vis': inputs.vis,
                          'datacolumn': datacolumn,
                          'outputvis': outputvis,
                          'intent': utils.to_CASA_intent(inputs.ms, intent),
                          'fitspec': fitspec.as_plain_dict(),
                          'field': ','.join(fields.keys()),
                          'spw': ','.join(real_spws.keys())}
        uvcontsub_job = casa_tasks.uvcontsub(**uvcontsub_args)
        try:
            casa_uvcontsub_result = self._executor.execute(uvcontsub_job)
        except OSError as e:
            LOG.warning(f'Caught uvcontsub exception: {e}')
            casa_uvcontsub_result = {'error': str(e)}

        # Copy across requisite XML files.
        self._copy_xml_files(inputs.vis, outputvis)

        result.vis = inputs.vis
        result.outputvis = outputvis
        result.casa_uvcontsub_result = casa_uvcontsub_result

        return result

    def analyse(self, result):

        if not result.mitigation_error and not result.skip_stage:
            # Check for existence of the output vis.
            if not os.path.exists(result.outputvis):
                LOG.debug('Error creating science targets line MS %s' % (os.path.basename(result.outputvis)))
                return result

            # Import the new measurement set.
            to_import = os.path.relpath(result.outputvis)
            observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

            # Adopt same session as source measurement set
            for ms in observing_run.measurement_sets:
                LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
                ms.session = self.inputs.ms.session
                ms.set_data_column(DataType.REGCAL_LINE_SCIENCE, 'DATA')
            result.line_mses.extend(observing_run.measurement_sets)

        return result

    @staticmethod
    def _copy_xml_files(vis, outputvis):
        for xml_filename in ['SpectralWindow.xml', 'DataDescription.xml']:
            vis_source = os.path.join(vis, xml_filename)
            outputvis_target_line = os.path.join(outputvis, xml_filename)
            if os.path.exists(vis_source) and os.path.exists(outputvis):
                LOG.info('Copying %s from original MS to science targets line MS', xml_filename)
                LOG.trace('Copying %s: %s to %s', xml_filename, vis_source, outputvis_target_line)
                shutil.copyfile(vis_source, outputvis_target_line)

    @staticmethod
    def _precheck_contdat():
        """Attempt to recover `cont.dat` if missing.

        This method checks if `cont.dat` is present in the current directory.
        If not, it searches for a compressed auxiliary product (`*.auxproducts.tgz`) 
        or a direct `cont.dat` file in the `../products/` directory. If a valid 
        tar archive is found containing `cont.dat`, the method extracts it.
        """
        contdatfile = 'cont.dat'
        if os.path.exists(contdatfile):
            LOG.debug("File %s already exists. No recovery needed.", contdatfile)
            return

        search_patterns = ["../products/*.auxproducts.tgz", "../products/"+contdatfile]
        for pattern in search_patterns:
            for file_path in utils.glob_ordered(pattern):
                if file_path.endswith(".auxproducts.tgz") and tarfile.is_tarfile(file_path):
                    try:
                        with tarfile.open(file_path, "r:gz") as tar:
                            members = [m for m in tar.getmembers() if m.name == contdatfile]
                            if members:
                                LOG.info("Extracting %s from %s", contdatfile, file_path)
                                tar.extractall(members=members)
                                LOG.info("Extraction complete.")
                                return
                    except tarfile.TarError as e:
                        LOG.error("Failed to extract %s from %s: %s", contdatfile, file_path, str(e))
                        return

                elif file_path.endswith("cont.dat"):
                    LOG.info("Found %s at %s. Copying to current directory.", contdatfile, file_path)
                    shutil.copyfile(file_path, contdatfile)
                    return

        LOG.debug("No valid archive or 'cont.dat' file found in '../products/'. Recovery failed.")


@task_registry.set_equivalent_casa_task('hif_uvcontsub')
class UVcontSub(sessionutils.ParallelTemplate):
    """UVcontSub class for parallelization."""

    Inputs = UVcontSubInputs
    Task = SerialUVcontSub

class UVcontSubResults(basetask.Results):
    """
    UVcontSubResults is the results class for the pipeline UVcontSub task.
    """

    def __init__(self):
        super().__init__()
        self.mitigation_error = False
        self.skip_stage = False
        self.vis = None
        self.outputvis = None
        self.field_intent_spw_list = []
        self.topo_freq_fitorder_dict = None
        self.line_mses = []
        self.casa_uvcontsub_result = None
        self.error = False
        self.error_msg = ''

    def merge_with_context(self, context):
        # Check to see if this stage was delibrately skipped
        if self.skip_stage:
            LOG.info("Skipping hif_uvcontsub due to VLA spectral lines MS not existing.")
            return

        # Check for an output vis
        if not self.line_mses:
            LOG.error('No hif_uvcontsub results to merge')
            return

        target = context.observing_run

        # Adding line mses to context
        for ms in self.line_mses:
            # Check if MS with the same name had already been registered and remove it
            try:
                index = [existing_ms.basename for existing_ms in target.measurement_sets].index(ms.basename)
                LOG.info('Removing {} from context'.format(ms.name))
                target.measurement_sets.pop(index)
            except:
                # Exception happens if name is not found. No special handling needed.
                pass
            LOG.info('Adding {} to context'.format(ms.name))
            target.add_measurement_set(ms)

        # Create targets flagging template file if it does not already exist
        for ms in self.line_mses:
            template_flagsfile = os.path.join(
                self.inputs['output_dir'], os.path.splitext(os.path.basename(self.vis))[0] + '.flagtargetstemplate.txt')
            self._make_template_flagfile(template_flagsfile, 'User flagging commands file for the imaging pipeline')

        # Initialize callibrary
        for ms in self.line_mses:
            # TODO: Check for existing entries for the line MS and remove them.
            #       This is probably only the case for future selfcal use cases.
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

    def __repr__(self):
        s = 'UVcontSubResults:\n'
        s += f'\tContinuum subtracted for {self.vis}. Line data stored in {self.outputvis}'
        return s

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
