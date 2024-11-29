import os

import pyblish.api
from openpype.pipeline import publish

from openpype.modules.ttd_addon.lib.pipeline import (
    find_in_project_settings
)
from openpype.modules.ttd_addon.lib.editorial import (
    truncate,
    get_image_data
)


class ExtractTimecode(publish.Extractor):
    """
    Extracts timecode from any supported extension.
    Uses OIIO primarily and FFprobe as fallback
    for movie filetypes.

    Inherits from pyblish.Plugin
    """


    label = "Extract Timecode - TTD"
    order = order = pyblish.api.ExtractorOrder + 0.01899

    families = ["render", "review", "preview", "gather"]
    
    settings = find_in_project_settings("extract_timecode")
    optional = True
    active = True


    def process(self, instance):

        default_tc = self.settings.get("default_timecode", "01:00:00:01")
        self.log.debug(f"Default tc is: {default_tc}") #type: ignore
        self.log.debug(f"Found FPS in instance: {instance.data.get('fps')}") #type: ignore

        instance.data["default_timecode"] = default_tc

        instance_fps = truncate(float(instance.data.get("fps", 24.0)), 3)
        self.log.debug(f"FPS truncated to: {instance_fps}") #type: ignore

        tc_list = []
        for repre in instance.data.get("representations", []):
            if repre["name"] != "thumbnail":
                tc = default_tc
                file = os.path.join(
                    repre["stagingDir"],
                    repre["files"][0] if isinstance(repre["files"], list) else repre["files"]
                )
                self.log.debug("Extracting timecode on file: '{}'".format(file)) #type: ignore
                try:
                    image_data = get_image_data(file, backend="iinfo")
                    self.log.debug(f"Image Data found: {image_data}") #type: ignore
                    if image_data:
                        tc = image_data.timecode
                except Exception as e:
                    self.log.warning(f"get_image_data on backend 'iinfo' failed: {e}") #type: ignore
                tc_list.append(tc)
                try:
                    image_data = get_image_data(file, backend="ffprobe")
                    self.log.debug(f"Image Data found: {image_data}") #type: ignore
                    if image_data:
                        tc = image_data.timecode
                except Exception as e:
                    self.log.warning(f"get_image_data on backend 'ffprobe' failed: {e}") #type: ignore
                tc_list.append(tc)

        final_tc = None
        final_tc_list = list(set(tc_list))
        self.log.debug(f"Timecodes found: '{final_tc_list}'") #type: ignore
        for tc in final_tc_list:
            if tc and tc != default_tc:
                self.log.debug(f"New timecode found: '{tc}'") #type: ignore
                final_tc = tc
        if not final_tc:
            final_tc = default_tc

        instance.data["timecode"] = final_tc
        self.log.info(f"Extracted Timecode data: {instance.data['timecode']}") #type: ignore


    