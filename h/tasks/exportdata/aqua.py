"""
Prototype pipeline AQUA report generator


Definitions
    Metrics are physical quantities, e.g. phase rms improvement resulting from
    WVR calibration., % data flagged, etc

    Scores are numbers between 0.0 and 1.0 derived from metrics.  Not all
    metrics currently derived in the pipeline are scored.

Structure
    The report contains
        A project structure section.
        A QA summary section.
        A per stage QA section.
        A per topic QA section.

Issues with the Original Schema / Current Pipeline Design
    The per ASDM dimension was ignored.

    The multiple metrics / scores per stage and / or per ASDM
    dimension was ignored.

    For stages with single scores / metrics and multiple ASDMs the
    current report generator selects the MS with the worst metric and
    reports that value. This metric by definition corresponds to
    the lowest score.

    Stages which generate multiple scores / metrics and multiple
    ASDMs are currently dealt with on an ad hoc basis.

    The scores and metrics are noew stored with the stage results.

    Metrics may have units information. They may be encoded as
    CASA quanta strings if appropriate.

Future Technical Solutions
    Suggestions
    Add a toAqua method to the base results class which returns a
    list of metrics for export. Pass these to the QA classes
    for scoring.

    Add the euivalent of a  toAqua registration method similar to what is
    done with QA handlers already
"""
import datetime
import itertools
import operator
import os
import copy
from typing import List, Optional
import xml.etree.cElementTree as ElementTree
from xml.dom import minidom

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.pipelineqa as pqa
from pipeline import environment
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.pipelineqa import QAScore

LOG = logging.get_logger(__name__)

# constant for an undefined value
UNDEFINED = 'Undefined'

# this holds all QA-metric-to-XML export functions
_AQUA_REGISTRY = set()

# Maps task name to function that gets sensitivity dict from result
TASK_NAME_TO_SENSITIVITY_EXPORTER = {}


def register_aqua_metric(fn):
    """
    Register a 'QA metric to XML' conversion function.

    This function can also be used as a decorator.

    :param fn: function to register
    :return:
    """
    _AQUA_REGISTRY.add(fn)
    return fn


class AquaXmlGenerator(object):
    """
    Class to create the XML for an AQUA pipeline report.
    """

    def get_report_xml(self, context):
        """
        Create and return the AQUA report XML for the results stored in a
        context.

        :param context: pipeline context to parse
        :return: root XML Element of AQUA report
        :rtype: xml.etree.cElementTree.Element
        """
        # read in all results in the context
        all_results = [r.read() for r in context.results]
        # the imaging tasks don't wrap themselves in a ResultsList. Until they
        # do, we have to fake that here.
        for idx, r in enumerate(all_results):
            try:
                iter(r)
            except TypeError:
                # temporary import for expedience
                # TODO make this a utility function
                import pipeline.infrastructure.renderer.htmlrenderer as htmlrenderer
                all_results[idx] = htmlrenderer.wrap_in_resultslist(r)

        # Initialize
        root = ElementTree.Element('PipelineAquaReport')

        # Construct the project structure element
        root.append(self.get_project_structure(context))

        # Construct the QA summary element
        root.append(self.get_qa_summary(context))

        # Construct the per pipeline stage elements
        root.append(self.get_per_stage_qa(context, all_results))

        # Construct the topics elements.
        root.append(self.get_topics_qa(context, all_results))

        return root

    def get_project_structure(self, context):
        """
        Get the project structure element.

        Given the current data flow it is unclear how the report generator
        generator will acquire the entity id of the original processing
        request.

        The processing procedure name is known but not yet passed to the
        pipeline processing request.

        :param context: pipeline context
        :return: XML for project structure
        :rtype: xml.etree.cElementTree.Element
        """
        root = ElementTree.Element('ProjectStructure')

        ElementTree.SubElement(root, 'ProposalCode').text = context.project_summary.proposal_code
        ElementTree.SubElement(root, 'ProcessingRequestEntityId').text = UNDEFINED
        if context.project_structure.recipe_name == 'Undefined':
            ElementTree.SubElement(root, 'ProcessingProcedure').text = UNDEFINED
        else:
            ElementTree.SubElement(root, 'ProcessingProcedure').text = context.project_structure.recipe_name

        return root

    def get_qa_summary(self, context):
        """
        Get the AQUA summary XML element.

        :param context: pipeline context
        :return: XML summarising execution
        :rtype: xml.etree.cElementTree.Element
        """
        root = ElementTree.Element('QaSummary')

        # Generate the report date
        now = datetime.datetime.utcnow()
        ElementTree.SubElement(root, 'ReportDate').text = now.strftime('%Y-%m-%d %H:%M:%S')

        # Processing time
        exec_start = context.results[0].read().timestamps.start
        exec_end = context.results[-1].read().timestamps.end
        # remove unnecessary precision for execution duration
        dt = exec_end - exec_start
        exec_duration = datetime.timedelta(days=dt.days, seconds=dt.seconds)
        ElementTree.SubElement(root, 'ProcessingTime').text = str(exec_duration)

        # Software versions
        ElementTree.SubElement(root, 'CasaVersion').text = environment.casa_version_string
        ElementTree.SubElement(root, 'PipelineVersion').text = environment.pipeline_revision

        # Score for the complete pipeline run
        # NB. the final pipeline score is not yet available.
        ElementTree.SubElement(root, 'FinalScore').text = UNDEFINED

        return root

    def get_per_stage_qa(self, context, all_results):
        """
        Get the XML for all stages.

        :param context: pipeline context
        :param all_results: all Results for this pipeline run
        :return: XML for all stages
        :rtype: xml.etree.cElementTree.Element
        """
        # Get the stage summary element.
        xml_root = ElementTree.Element('QaPerStage')

        ordered_results = sorted(all_results, key=operator.attrgetter('stage_number'))
        for stage_result in ordered_results:
            # Create the generic stage element
            stage_name, representative_score, subscores = _get_pipeline_stage_and_scores(stage_result)
            stage_element = ElementTree.Element('Stage',
                                                Number=str(stage_result.stage_number),
                                                Name=stage_name)

            # add representative score element
            score_element = ElementTree.Element('RepresentativeScore', Score=str(representative_score.score), Reason=representative_score.longmsg)

            # add corresponding metric element
            score_element.extend(self._get_xml_for_qa_metric(representative_score))

            # add corresponding data selection element
            score_element.extend(self._get_xml_for_qa_data_selection(representative_score))

            # add score element to stage
            stage_element.append(score_element)

            # add subscore elements
            for qa_score in subscores:
                score_element = ElementTree.Element('SubScore', Score=str(qa_score.score), Reason=qa_score.longmsg)

                # add corresponding metric element
                score_element.extend(self._get_xml_for_qa_metric(qa_score))

                # add corresponding data selection element
                score_element.extend(self._get_xml_for_qa_data_selection(qa_score))

                # add score element to stage
                stage_element.append(score_element)

            xml_root.append(stage_element)

        return xml_root

    def _get_xml_for_qa_metric(self, qa_score):
        """
        Generate XML element for QA metric

        :param qa_score: QAScore
        :return: XML element
        """

        # create a pseudo registry for the generic XML generator
        generic_registry = {GenericMetricXmlGenerator()}

        if any(fn.handles(qa_score.origin.metric_name) for fn in _AQUA_REGISTRY):
            return self._get_xml_for_qa_scores([qa_score], _AQUA_REGISTRY)
        else:
            return self._get_xml_for_qa_scores([qa_score], generic_registry)

    def _get_xml_for_qa_data_selection(self, qa_score):
        """
        Generate XML element for QA data selection

        :param qa_score: QAScore
        :return: XML element
        """
        target_asdms = qa_score.applies_to.vis
        if target_asdms:
            Asdm = ','.join([vis_to_asdm(a) for a in target_asdms])
        else:
            Asdm = 'N/A'

        target_session = qa_score.applies_to.session
        if target_session:
            Session = ','.join([str(s) for s in target_session])
        else:
            Session = 'N/A'

        Spw = ','.join(sorted(map(str, qa_score.applies_to.spw)))
        if Spw == '':
            Spw = 'N/A'

        Intent = ','.join(sorted(qa_score.applies_to.intent))
        if Intent == '':
            Intent = 'N/A'

        extra_attributes = {}

        Field = ','.join(sorted(map(str, qa_score.applies_to.field)))
        if len(Field) > 0:
            extra_attributes['Field'] = Field

        Antenna = ','.join(sorted(map(str, qa_score.applies_to.ant)))
        if len(Antenna) > 0:
            extra_attributes['Antenna'] = Antenna

        return [ElementTree.Element('DataSelection', Asdm=Asdm, Session=Session, Spw=Spw, Intent=Intent, **extra_attributes)]

    def _get_xml_for_qa_scores(self, items, registry) -> List[ElementTree.Element]:
        """
        Generate the XML elements for a list of QA scores.

        :param items: list of QAScores
        :param registry: list of XML generator functions
        :return: list of XML elements
        :rtype: list of xml.etree.ElementTree
        """
        # group scores into a {<metric name>: [<QAScore, ...>]} dict
        metric_to_scores = {}
        keyfunc = operator.attrgetter('origin.metric_name')
        s = sorted(list(items), key=keyfunc)
        for k, g in itertools.groupby(s, keyfunc):
            metric_to_scores[k] = list(g)

        # let each generator process the QA scores it can handle, accumulating
        # the XML as we go
        elements = []
        for metric_name, scores in metric_to_scores.items():
            xml = [fn(scores) for fn in registry if fn.handles(metric_name)]
            elements.extend(utils.flatten(xml))

        return elements

    def get_topics_qa(self, context, all_results):
        """
        Get the XML for all results, divided into sections by topic.

        :param context: pipeline context
        :param all_results: all Results for this pipeline run
        :return: XML for topics
        :rtype: xml.etree.cElementTree.Element
        """
        # Set the top level topics element.
        root = ElementTree.Element('QaPerTopic')

        # Add the data topic
        topic = qaadapter.registry.get_dataset_topic()
        dataset_results = [r for r in all_results if topic.handles_result(r)]
        root.append(self.get_dataset_topic(context, dataset_results))

        # Add the flagging topic
        topic = qaadapter.registry.get_flagging_topic()
        flagging_results = [r for r in all_results if topic.handles_result(r)]
        root.append(self.get_flagging_topic(context, flagging_results))

        # Add the calibration topic
        topic = qaadapter.registry.get_calibration_topic()
        calibration_results = [r for r in all_results if topic.handles_result(r)]
        root.append(self.get_calibration_topic(context, calibration_results))

        # Add the imaging topic
        topic = qaadapter.registry.get_imaging_topic()
        imaging_results = [r for r in all_results if topic.handles_result(r)]
        root.append(self.get_imaging_topic(context, imaging_results))

        return root

    def get_calibration_topic(self, context, topic_results):
        """
        Get the XML for the calibration topic.

        :param context: pipeline context
        :param topic_results: list of Results for this topic
        :return: XML for calibration topic
        :rtype: xml.etree.cElementTree.Element
        """
        return self._xml_for_topic('Calibration', context, topic_results)

    def get_dataset_topic(self, context, topic_results):
        """
        Get the XML for the dataset topic.

        :param context: pipeline context
        :param topic_results: list of Results for this topic
        :return: XML for dataset topic
        :rtype: xml.etree.cElementTree.Element
        """
        return self._xml_for_topic('Dataset', context, topic_results)

    def get_flagging_topic(self, context, topic_results):
        """
        Get the XML for the flagging topic.

        :param context: pipeline context
        :param topic_results: list of Results for this topic
        :return: XML for flagging topic
        :rtype: xml.etree.cElementTree.Element
        """
        return self._xml_for_topic('Flagging', context, topic_results)

    def get_imaging_topic(self, context, topic_results):
        """
        Get the XML for the imaging topic.

        :param context: pipeline context
        :param topic_results: list of Results for this topic
        :return: XML for imaging topic
        :rtype: xml.etree.cElementTree.Element
        """
        return self._xml_for_topic('Imaging', context, topic_results)

    def _xml_for_topic(self, topic_name, context, topic_results):
        # the overall topic score is defined as the minimum score of all
        # representative scores for each task in that topic, which themselves
        # are (often) the minimum of the scores for that task
        try:
            min_score = min([r.qa.representative for r in topic_results if r.qa.representative.score is not None], key=operator.attrgetter('score'))
            score = str(min_score.score)
        except ValueError:
            # empty list
            score = UNDEFINED

        xml_root = ElementTree.Element(topic_name, Score=score)
        topic_xml = self.get_per_stage_qa(context, topic_results)
        xml_root.extend(topic_xml)

        return xml_root


def export_to_disk(report, filename):
    """
    Convert an XML document to a nicely formatted XML string and save it in a
    file.
    """
    xmlstr = ElementTree.tostring(report, 'utf-8')

    # Reformat it to prettyprint style
    reparsed = minidom.parseString(xmlstr)
    reparsed_xmlstr = reparsed.toprettyxml(indent='  ')

    # Save it to a file.
    with open(filename, 'w') as aquafile:
        aquafile.write(reparsed_xmlstr)


def vis_to_asdm(vispath):
    """
    Get the expected ASDM name from the path of a measurement set.

    :param vispath: path to convert
    :return: expected name of ASDM for MS
    """
    return os.path.splitext(os.path.basename(vispath))[0]


def xml_generator_for_metric(qa_label, value_spec):
    """
    Return a function that converts a matching QAScore to XML.

    :param qa_label: QA metric label to match
    :param value_spec: string format spec for how to format metric value
    :return: function
    """
    # We don't (yet) allow % in the output XML, even when it represents a
    # percentage
    if value_spec.endswith('%}'):
        value_formatter = _create_trimmed_formatter(value_spec, 1)
    else:
        value_formatter = _create_value_formatter(value_spec)

    # return LowestScoreMetricXmlGenerator(qa_label, formatters={'Value': value_formatter})
    return MetricXmlGenerator(qa_label, formatters={'Value': value_formatter})


class MetricXmlGenerator(object):
    """
    Creates a AQUA report XML element for QA scores.
    """

    def __init__(self, metric_name, formatters=None):
        """
        The constructor accepts an optional dict of string formatters: functions
        that accept a string and return a formatted string. If this argument is
        not supplied, the default formatter keys and formatter functions applied
        will be:

            'Name': convert to string
            'Value': convert to string
            'Units': convert to string

        :param metric_name: metric to match
        :param formatters: (optional) dict string formatters
        """
        self.metric_name = metric_name

        # set default attribute formatters before updating with user overrides
        self.attr_formatters = {
            'Name': str,
            'Value': str,
            'Units': str,
        }
        if formatters:
            self.attr_formatters.update(formatters)

    def __call__(self, qa_scores: List[QAScore]) -> List[Optional[ElementTree.Element]]:
        scores_to_process = self.filter(qa_scores)
        return [self.to_xml(score) for score in scores_to_process]

    def handles(self, metric_name: str) -> bool:
        """
        Returns True if this class can generate XML for the given metric.

        :param metric_name: name of metric
        :return: True if metric handled by this class
        """
        return metric_name == self.metric_name

    def filter(self, qa_scores: List[QAScore]) -> List[QAScore]:
        """
        Reduce a list of entries to those entries that require XML to be generated.

        :param qa_scores: list of QAScores
        :return: list of QAScores
        """
        return qa_scores

    def to_xml(self, qa_score: QAScore) -> Optional[ElementTree.Element]:
        """
        Return the XML representation of a QA score and associated metric.

        :param qa_score: QA score to convert
        :return: XML element
        :rtype: xml.etree.ElementTree.Element
        """
        if not qa_score:
            return None

        origin = qa_score.origin
        score_value = str(qa_score.score)
        score_message = str(qa_score.longmsg)

        init_args = dict(
            Name=self.attr_formatters['Name'](origin.metric_name),
            Value=self.attr_formatters['Value'](origin.metric_score),
            Units=self.attr_formatters['Units'](origin.metric_units),
        )

        return ElementTree.Element('Metric', **init_args)


class LowestScoreMetricXmlGenerator(MetricXmlGenerator):
    """
    Metric XML Generator that only returns XML for the lowest QA score that it
    handles.
    """

    def __init__(self, metric_name, formatters=None):
        super(LowestScoreMetricXmlGenerator, self).__init__(metric_name, formatters)

    def filter(self, qa_scores):
        handled = [(vis, qa_score) for vis, qa_score in qa_scores
                   if self.handles(qa_score.origin.metric_name)]

        if not handled:
            return []

        lowest = min(handled, key=lambda vis_qa_score: vis_qa_score[1].score)
        return [lowest]


class GenericMetricXmlGenerator(MetricXmlGenerator):
    """
    Metric XML Generator that processes any score it is given, formatting the
    metric value to 3dp.
    """

    def __init__(self):
        # format all processed entries to 3dp
        formatters = {'Value': _create_value_formatter('{:0.3f}')}
        super(GenericMetricXmlGenerator, self).__init__('Generic metric', formatters)

    def handles(self, _):
        return True


def _create_trimmed_formatter(format_spec, trim=0):
    """
    Create a function that formats values as a percent.

    :param format_spec: string format specification to apply
    :param trim: number of characters to trim
    :return: function
    """
    g = _create_value_formatter(format_spec)

    def f(val):
        val = g(val)
        if val == UNDEFINED:
            return UNDEFINED
        else:
            return val[:-trim]

    return f


def _create_value_formatter(format_spec):
    """
    Create a function that applies a string format spec.

    This function return a function that accepts one argument and returns the
    string formatted according to the given string format specification. If
    the argument cannot be formatted, the default 'undefined' string will be
    returned.

    This is used internally to create a set of formatting functions that all
    exhibit the same behaviour, whereby 'Undefined' is returned on errors.

    :param format_spec: string format specification to apply
    :return: function
    """
    def f(val):
        try:
            return format_spec.format(val)
        except (ValueError, TypeError):
            # Handle lists of metrics and other possible flavors with a string
            # representation.
            return str(val)
        except:
            return UNDEFINED

    return f


def _get_pipeline_stage_and_scores(result, include_hidden_scores=False):
    """
    Get the CASA equivalent task name which is stored by the infrastructure
    as  <task_name> (<arg1> = <value1>, ...). Also get the representative
    scores and the subscores. Optionally also include hidden scores.
    """
    stage_name = result.pipeline_casa_task.split('(')[0]
    if include_hidden_scores:
        subscores = copy.deepcopy(result.qa.pool)
    else:
        subscores = [score for score in result.qa.pool if score.weblog_location != pqa.WebLogLocation.HIDDEN]
    representative_score = result.qa.representative
    return stage_name, representative_score, subscores


def sensitivity_xml_for_stages(context, results, name=''):
    """
    Get the XML for all sensitivities reported by all tasks.

    :param context: pipeline context
    :param results: all results for the imaging topic
    :param name: the name of per stage tag (optional)
    :return: XML for sensitivities
    :rtype: xml.etree.cElementTree.Element
    """
    xml_root = ElementTree.Element('ImageSensitivity')

    ordered_results = sorted(results, key=operator.attrgetter('stage_number'))
    for stage_result in ordered_results:
        # Create the generic stage element
        stage_name, _, _ = _get_pipeline_stage_and_scores(stage_result)

        for task_name, exporter in TASK_NAME_TO_SENSITIVITY_EXPORTER.items():
            if stage_name == task_name:
                stage_xml = xml_for_sensitivity_stage(context, stage_result, exporter, name)
                xml_root.append(stage_xml)

    return xml_root


def xml_for_sensitivity_stage(context, stage_results, exporter, name):
    """
    Translate the sensitivity dictionaries contained in a task result to XML.

    :param context: pipeline context
    :param stage_results: hifa_preimagecheck result
    :param exporter: function that returns a list of sensitivity dicts from the result
    :param name: the name of per stage tag (optional)
    :return: XML for all sensitivities reported by the result stage
    :rtype: xml.etree.cElementTree.Element
    """
    stage_name, representative_score, _ = _get_pipeline_stage_and_scores(stage_results)

    tagname = name if name != '' else 'SensitivityEstimates'

    xml_root = ElementTree.Element(tagname,
                                   Origin=stage_name,
                                   StageNumber=str(stage_results.stage_number),
                                   Score=str(representative_score.score))

    sensitivity_dicts = exporter(stage_results)

    for d in sensitivity_dicts:
        ms_xml = xml_for_sensitivity(d)
        xml_root.append(ms_xml)

    return xml_root


def xml_for_sensitivity(d):
    """
    Return the XML representation for a sensitivity dictionary.

    :param d: sensitivity dict
    :return: XML element
    :rtype: xml.etree.cElementTree.Element
    """
    qa = casa_tools.quanta

    def value(quanta):
        return str(qa.getvalue(quanta)[0])

    try:
        if d['is_representative'] is None:
            is_representative = 'N/A'
        else:
            is_representative = str(d['is_representative'])
    except:
        is_representative = 'N/A'

    try:
        if d['bandwidth'] is None:
            bandwidth_hz = 'N/A'
        else:
            bandwidth = qa.quantity(d['bandwidth'])
            bandwidth_hz = value(qa.convert(bandwidth, 'Hz'))
    except:
        bandwidth_hz = 'N/A'

    try:
        effective_bw = qa.quantity(d['effective_bw'])
        effective_bw_hz = value(qa.convert(effective_bw, 'Hz'))
        if effective_bw_hz == '0.0':
            effective_bw_hz = 'N/A'
    except:
        effective_bw_hz = 'N/A'

    try:
        if d['beam']['major'] is None:
            major_arcsec = 'N/A'
        else:
            major = qa.quantity(d['beam']['major'])
            major_arcsec = value(qa.convert(major, 'arcsec'))
    except:
        major_arcsec = 'N/A'

    try:
        if d['beam']['minor'] is None:
            minor_arcsec = 'N/A'
        else:
            minor = qa.quantity(d['beam']['minor'])
            minor_arcsec = value(qa.convert(minor, 'arcsec'))
    except:
        minor_arcsec = 'N/A'

    try:
        if d['cell'][0] is None:
            cell_x_arcsec = 'N/A'
        else:
            cell_x = qa.quantity(d['cell'][0])
            cell_x_arcsec = value(qa.convert(cell_x, 'arcsec'))
    except:
        cell_x_arcsec = 'N/A'

    try:
        if d['cell'][1] is None:
            cell_y_arcsec = 'N/A'
        else:
            cell_y = qa.quantity(d['cell'][1])
            cell_y_arcsec = value(qa.convert(cell_y, 'arcsec'))
    except:
        cell_y_arcsec = 'N/A'

    try:
        if d['beam']['positionangle'] is None:
            positionangle_deg = 'N/A'
        else:
            positionangle = qa.quantity(d['beam']['positionangle'])
            positionangle_deg = value(qa.convert(positionangle, 'deg'))
    except:
        positionangle_deg = 'N/A'

    try:
        if d['sensitivity'] is None:
            sensitivity_jy_per_beam  = 'N/A'
        else:
            sensitivity = qa.quantity(d['sensitivity'])
            sensitivity_jy_per_beam = value(qa.convert(sensitivity, 'Jy/beam'))
    except:
        sensitivity_jy_per_beam  = 'N/A'

    try:
        if d['pbcor_image_min'] is None:
            pbcor_image_min_jy_per_beam = 'N/A'
        else:
            pbcor_image_min = qa.quantity(d['pbcor_image_min'])
            pbcor_image_min_jy_per_beam = value(qa.convert(pbcor_image_min, 'Jy/beam'))
    except:
        pbcor_image_min_jy_per_beam = 'N/A'

    try:
        if d['pbcor_image_max'] is None:
            pbcor_image_max_jy_per_beam = 'N/A'
        else:
            pbcor_image_max = qa.quantity(d['pbcor_image_max'])
            pbcor_image_max_jy_per_beam = value(qa.convert(pbcor_image_max, 'Jy/beam'))
    except:
        pbcor_image_max_jy_per_beam = 'N/A'

    try:
        if d['imagename'] is None:
            imagename = 'N/A'
        else:
            imagename = d['imagename']
    except:
        imagename = 'N/A'

    try:
        if d['datatype'] is None:
            datatype = 'N/A'
        else:
            datatype = d['datatype']
    except:
        datatype = 'N/A'

    xml = ElementTree.Element('Sensitivity',
        Array=d['array'],
        BandwidthHz=bandwidth_hz,
        EffectiveBandwidthHz=effective_bw_hz,
        BeamMajArcsec=major_arcsec,
        BeamMinArcsec=minor_arcsec,
        BeamPosAngDeg=positionangle_deg,
        BwMode=d['bwmode'],
        CellXArcsec=cell_x_arcsec,
        CellYArcsec=cell_y_arcsec,
        Intent=d['intent'],
        Field=d['field'],
        Robust=str(d.get('robust', '')),
        UVTaper=str(d.get('uvtaper', '')),
        SensitivityJyPerBeam=sensitivity_jy_per_beam,
        MsSpwId=d['spw'],
        IsRepresentative=is_representative,
        PbcorImageMinJyPerBeam=pbcor_image_min_jy_per_beam,
        PbcorImageMaxJyPerBeam=pbcor_image_max_jy_per_beam,
        ImageName=imagename,
        DataType=datatype
      )

    return xml
