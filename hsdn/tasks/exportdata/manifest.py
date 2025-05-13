"""The NRO pipeline data product manifest class."""

from xml.etree.ElementTree import Element
import xml.etree.cElementTree as eltree

import pipeline.h.tasks.common.manifest as manifest


class NROPipelineManifest(manifest.PipelineManifest):
    """Class for creating the NRO pipeline data product manifest."""

    @staticmethod
    def add_reduction_script(ous: Element, script: str):
        """Add the template reduction script for restoredata workflow.

        Args:
            ous : XML Element
            script : reduction script file name
        """
        eltree.SubElement(ous, "reduction_script", name=script)

    @staticmethod
    def add_scalefile(ous: Element, filename: str):
        """Add the template scale file for restoredata workflow.

        Args:
            ous : XML Element
            filename : scale file name
        """
        eltree.SubElement(ous, "scale_file", name=filename)
