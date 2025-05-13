import copy
import os.path

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.utils as utils


class MakeImagesResult(basetask.Results):
    def __init__(self):
        super(MakeImagesResult, self).__init__()
        self.targets = []
        self.results = []
        self.plot_path = None
        self.mitigation_error = False
        self.sensitivities_for_aqua = []
        self.logrecords = []
        self.overwrite = True

    def add_result(self, result, target, outcome):
        target['outcome'] = outcome
        # Remove heuristics object to avoid accumulating large amounts of unnecessary memory
        del target['heuristics']
        self.targets.append(target)
        self.results.append(result)

        # pull the log records from the worker results, potentially executing
        # on MPI servers, into this object
        self.logrecords.extend(getattr(result, 'logrecords', []))

    def set_info(self, info):
        self.clean_list_info = info

    def merge_with_context(self, context):
        """Add the cleaned targets to the context."""

        for result in self.results:
            try:
                img_params = {kk: vv['imaging_params'] for (kk, vv) in result.iterations.items()}
                imageitem = imagelibrary.ImageItem(
                    imagename=result.image, sourcename=result.sourcename,
                    spwlist=result.spw, specmode=result.specmode,
                    sourcetype=result.intent,
                    multiterm=result.multiterm,
                    metadata=result.imaging_metadata,
                    imaging_params=img_params,  # imaging parameters for each iteration
                    imageplot=result.imageplot)
                if 'TARGET' in result.intent:
                    context.sciimlist.add_item(imageitem, self.overwrite)
                else:
                    context.calimlist.add_item(imageitem, self.overwrite)
            except:
                pass

        # Calculated sensitivities for later stages
        skip_recalc = False
        for result in self.results:
            # Calculated beams for later stages
            if result.synthesized_beams is not None:
                if 'recalc' in result.synthesized_beams:
                    context.synthesized_beams = copy.deepcopy(result.synthesized_beams)
                    del context.synthesized_beams['recalc']
                else:
                    utils.update_beams_dict(context.synthesized_beams, result.synthesized_beams)

            # Calculated sensitivities for later stages
            if result.per_spw_cont_sensitivities_all_chan is not None:
                if 'recalc' in result.per_spw_cont_sensitivities_all_chan and not skip_recalc:
                    context.per_spw_cont_sensitivities_all_chan = copy.deepcopy(result.per_spw_cont_sensitivities_all_chan)
                    del context.per_spw_cont_sensitivities_all_chan['recalc']
                    # Copy only the first recalculated dictionary
                    skip_recalc = True
                else:
                    utils.update_sens_dict(context.per_spw_cont_sensitivities_all_chan, result.per_spw_cont_sensitivities_all_chan)

        # empty the pending list and message
        context.clean_list_pending = []
        context.clean_list_info = {}

        # Remove heuristics objects to avoid accumulating large amounts of unnecessary memory
        for target in self.inputs['target_list']:
            try:
                del target['heuristics']
            except:
                pass

        for result in self.results:
            try:
                del result.inputs['image_heuristics']
            except:
                pass

    def __repr__(self):
        repr = 'MakeImages:'

        field_width = len('field')
        intent_width = len('intent')
        spw_width = len('spw')
        phasecenter_width = len('phasecenter')
        cell_width = len('cell')
        imsize_width = len('imsize')
        imagename_width = len('imagename')
        outcome_width = len('outcome')
        for target in self.targets:
            field_width = max(field_width, len(target['field']))
            intent_width = max(intent_width, len(target['intent']))
            spw_width = max(spw_width, len(target['spw']))
            phasecenter_width = max(phasecenter_width,
             len(target['phasecenter']))
            cell_width = max(cell_width, len(str(target['cell'])))
            imsize_width = max(imsize_width, len(str(target['imsize'])))
            imagename = os.path.basename(target['imagename'])
            imagename_width = max(imagename_width, len(imagename))
            outcome = target['outcome']
            outcome_width = max(outcome_width, len(outcome))

        field_width += 1
        intent_width += 1
        spw_width += 1
        phasecenter_width += 1
        cell_width += 1
        imsize_width += 1
        imagename_width += 1
        outcome_width += 1

        repr += '\n'
        repr += '{0:{1}}'.format('field', field_width)
        repr += '{0:{1}}'.format('intent', intent_width)
        repr += '{0:{1}}'.format('spw', spw_width)
        repr += '{0:{1}}'.format('phasecenter', phasecenter_width)
        repr += '{0:{1}}'.format('cell', cell_width)
        repr += '{0:{1}}'.format('imsize', imsize_width)
        repr += '{0:{1}}'.format('imagename', imagename_width)
        repr += '{0:{1}}'.format('outcome', outcome_width)

        for target in self.targets:
            repr += '\n'
            repr += '{0:{1}}'.format(target['field'], field_width)
            repr += '{0:{1}}'.format(target['intent'], intent_width)
            repr += '{0:{1}}'.format(target['spw'], spw_width)
            repr += '{0:{1}}'.format(target['phasecenter'], phasecenter_width)
            repr += '{0:{1}}'.format(str(target['cell']), cell_width)
            repr += '{0:{1}}'.format(str(target['imsize']), imsize_width)
            repr += '{0:{1}}'.format(os.path.basename(target['imagename']),
              imagename_width)
            repr += '{0:{1}}'.format(target['outcome'], outcome_width)

        return repr
