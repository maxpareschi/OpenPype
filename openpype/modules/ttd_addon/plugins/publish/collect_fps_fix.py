import pyblish.api

from openpype.modules.ttd_addon.lib.editorial import (
    truncate
)


class CollectFpsFix(pyblish.api.InstancePlugin):
    """
    Truncate FPS to 3 digits to catch strange edge cases with
    libs and dccs (like maya and otio)
    """
    
    label = "Collect Fps Fix"
    order = pyblish.api.CollectorOrder


    def process(self, instance):
        
        truncated_fps = truncate(float(instance.data.get("fps", 24.0)), 3)
        instance.data["fps"] = truncated_fps
        instance.context.data["fps"] = truncated_fps
        self.log.info(f"FPS truncated to: {truncated_fps} in instances and context data.")