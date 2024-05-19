import os
import json
import pyblish.api
from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path
)


class CollectDeliveryData(pyblish.api.InstancePlugin):
    """
    Fixes missing or incomplete data for Delivery/Gather feature
    """
    label = "Collect for Delivery/Gather data"
    order = pyblish.api.CollectorOrder + 0.4999
    families = [
        "delivery"
    ]

    def process(self, instance):

        context = instance.context
        publ_settings = context.data["project_settings"]["global"]["publish"]
        version_padding = context.data["anatomy"]["templates"]["defaults"]\
            ["version_padding"]

        self.log.debug(json.dumps(instance.data, indent=4, default=str))

        # raise