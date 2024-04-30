import os
import pyblish.api

from openpype.lib import (
    get_oiio_tools_path,
    run_subprocess,
)
from openpype.pipeline import publish


class ExtractOTIOFile(publish.Extractor):
    """
    Extractor export transcoded clip from Hiero
    """
    
    order = pyblish.api.ExtractorOrder -0.1
    label = "Extract Transcoded Clips"
    hosts = ["hiero"]
    families = ["transcode"]

    def process(self, instance):
        oiio_tool_path = get_oiio_tools_path()
        staging_dir = self.staging_dir(instance)
        output_template = os.path.join(staging_dir, instance.data["name"])
        sequence = instance.context.data["activeTimeline"]

        self.log.debug(staging_dir)
        self.log.debug(output_template)
        self.log.debug(sequence)
