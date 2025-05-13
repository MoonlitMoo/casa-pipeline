"""AQUA pipeline report generator for Single Dish."""

from typing import List
from xml.etree.ElementTree import Element
import xml.etree.cElementTree as ElementTree

from pipeline.h.tasks.common.sensitivity import Sensitivity
import pipeline.h.tasks.exportdata.aqua as aqua
from pipeline.infrastructure.basetask import Results, ResultsList
from pipeline.infrastructure.launcher import Context


class AlmaAquaXmlGenerator(aqua.AquaXmlGenerator):
    """Class for creating the AQUA pipeline report.

    Note __init__ and get_project_structure are copies of the counterpart
    in almaifaqua
    """

    def __init__(self):
        """Initialize AlmaAquaXmlGenerator instance."""
        super(AlmaAquaXmlGenerator, self).__init__()

    def get_project_structure(self, context: Context) -> Element:
        """Get the project structure element.

        Args:
            context : pipeline context

        Returns:
            XML Element object for project structure
        """
        # get base XML from base class
        root = super(AlmaAquaXmlGenerator, self).get_project_structure(context)

        # add our ALMA-specific elements
        ElementTree.SubElement(root, 'OusEntityId').text = \
            context.project_structure.ous_entity_id
        ElementTree.SubElement(root, 'OusPartId').text = \
            context.project_structure.ous_part_id
        ElementTree.SubElement(root, 'OusStatusEntityId').text = \
            context.project_structure.ousstatus_entity_id

        return root

    def get_imaging_topic(self, context: Context,
                          topic_results: List[Results]) -> Element:
        """Get the XML for the imaging topic.

        Args:
            context : pipeline context
            topic_results : list of Results for this topic

        Returns:
            XML for imaging topic
        """
        # get base XML from base class
        xml_root = super(AlmaAquaXmlGenerator, self).get_imaging_topic(
            context, topic_results)

        # add sensitivities
        sensitivity_xml = aqua.sensitivity_xml_for_stages(context,
                                                          topic_results)
        # omit containing element if no measurements were found
        if len(list(sensitivity_xml)) > 0:
            xml_root.extend(sensitivity_xml)

        return xml_root


def _hsd_imaging_sensitivity_exporter(stage_results: ResultsList) \
        -> List[Sensitivity]:
    """XML exporter expects this function to return a list of dictionaries.

    This function is used only once for now, with no arguments
    and returns an empty list.

    Args:
        stage_results: ResultsList of stages

    Returns:
        list of Sensitivity
    """
    # XML exporter expects this function to return a list of dictionaries
    sensitivities = []
    for result in stage_results:
        if result.sensitivity_info is not None and \
                result.sensitivity_info.to_export:
            sensitivities.append(result.sensitivity_info.sensitivity)
    return sensitivities


aqua.TASK_NAME_TO_SENSITIVITY_EXPORTER['hsd_imaging'] = \
    _hsd_imaging_sensitivity_exporter
