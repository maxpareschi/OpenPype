import os
import json
import subprocess
import math

import pyblish.api
import opentimelineio as otio

from openpype.pipeline import publish
from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path,
    run_subprocess
)
from openpype.settings import get_project_settings, get_current_project_settings

from openpype.pipeline.editorial import shift_timecode

def truncate(number, digits) -> float:
    # Improve accuracy with floating point operations, to avoid truncate(16.4, 2) = 16.39 or truncate(-1.13, 2) = -1.12
    try:
        nbDecimals = len(str(number).split('.')[1])
    except:
        nbDecimals = 0
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

def get_frame_from_timecode(tc, fps=24.0):
    rationaltime = otio.opentime.from_timecode(tc, fps)
    frames = rationaltime.to_frames(fps)
    return frames

class ExtractTimecode(publish.Extractor):
    """
    Extractor for general timecode
    """

    label = "Extract Timecode"
    order = order = pyblish.api.ExtractorOrder + 0.01899
    families = ["render", "review", "preview"]
    allowed_extensions = ["mov", "mp4", "dpx", "cin", "exr"]

    optional = True
    active = True

    def _finditems(self, search_dict, field):
        """
        Takes a dict with nested lists and dicts,
        and searches all dicts for a key of the field
        provided.
        """
        fields_found = []

        for key, value in search_dict.items():

            if key == field:
                fields_found.append(value)

            elif isinstance(value, dict):
                results = self._finditems(value, field)
                for result in results:
                    fields_found.append(result)

            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        more_results = self._finditems(item, field)
                        for another_result in more_results:
                            fields_found.append(another_result)

        return fields_found
    

    def get_timecode_oiio(self, in_file):
        cmd = [
            get_oiio_tools_path("iinfo"),
            "-v",
            in_file.replace("\\", "/")
        ]
        # res = subprocess.run(
        #     cmd,
        #     check=True,
        #     capture_output=True
        # )
        # lines = res.stdout.decode("utf-8", errors="ignore").replace(" ", "").splitlines()
        res = run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW)
        lines = res.replace(" ", "").splitlines()
        found_timecodes = []
        tc = None
        
        for l in lines:
            if l.lower().find("timecode") >= 0: # or l.lower().find("tc") >= 0:
                found_timecodes.append(l)

        for tcode in found_timecodes:
            if tcode.find("smpte") >= 0:
                tc = ":".join(tcode.split(":")[-4:])

        return tc


    def get_timecode_ffprobe(self, in_file):
        cmd = [
            get_ffmpeg_tool_path("ffprobe"),
            "-v",
            "error",
            "-hide_banner",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            in_file.replace("\\", "/")
        ]
        # res = json.loads(
        #     subprocess.run(
        #         cmd,
        #         check=True,
        #         capture_output=True,
        #         text=True
        #     ).stdout
        # )
        # lines = res.replace(" ", "").splitlines()
        res = json.loads(run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW))
        tc = list(set(self._finditems(res, "timecode")))[0]
        return tc


    def process(self, instance):
        settings = get_current_project_settings()["global"]["publish"]["ExtractTimecode"]
        default_tc = settings.get("default_tc", "01:00:00:01")
        self.log.debug(f"Default tc is: {default_tc}")
        self.log.debug(f"Found FPS in instance: {instance.data.get('fps')}")
        instance_fps = truncate(float(instance.data.get("fps", 24.0)), 3)
        self.log.debug(f"FPS truncated to: {instance_fps}")
        tc_list = []
        if instance.data.get("representations", None):
            for repre in instance.data["representations"]:
                if repre["ext"] in self.allowed_extensions:
                    tc = default_tc
                    file = os.path.join(
                        repre["stagingDir"],
                        repre["files"][0] if isinstance(repre["files"], list) else repre["files"]
                    )
                    self.log.debug("Extracting timecode on file: '{}'".format(file))
                    try:
                        tc = self.get_timecode_oiio(file)
                    except:
                        self.log.debug("No timecode found using iinfo, trying ffprobe...")
                        try:
                            tc = self.get_timecode_ffprobe(file)
                        except:
                            self.log.debug("No timecode found using ffprobe...")
                    tc_list.append(tc)
        
        final_tc = None
        self.log.debug("Default timecode set to: '{}'".format(default_tc))
        final_tc_list = list(set(tc_list))
        self.log.debug("Timecodes : '{}'".format(final_tc_list))
        for tc in final_tc_list:
            if tc and tc != default_tc:
                self.log.debug("New timecode found: '{}'".format(tc))
                final_tc = tc
        if not final_tc:
            final_tc = default_tc

        final_tc_no_handles = instance.data["timecode_no_handles"] = shift_timecode(
            final_tc, instance.data.get("handleStart", 0), instance_fps)

        frame_start_tc = get_frame_from_timecode(final_tc, instance_fps)
        frame_start_tc_no_handles = get_frame_from_timecode(final_tc_no_handles, instance_fps)

        tc_data = {
            "timecode": final_tc,
            "timecode_no_handles": final_tc_no_handles,
            "frame_start_tc": frame_start_tc,
            "frame_start_tc_no_handles": frame_start_tc_no_handles
        }

        instance.data.update(tc_data)
        
        for repre in instance.data["representations"]:
            if repre["ext"] in self.allowed_extensions:
                repre.update(tc_data)

        self.log.debug(f"Extracted timecode data: {json.dumps(tc_data, indent=4, default=str)}")


                    
                    