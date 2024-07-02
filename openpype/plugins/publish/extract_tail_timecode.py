import os
import json
import subprocess
import pyblish.api
import opentimelineio as otio


from openpype.pipeline import publish
from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path
)


class ExtractTailTimecode(publish.Extractor):
    """
    Extractor for Tail timecode
    """

    label = "Extract Tail Timecode"
    order = order = pyblish.api.ExtractorOrder + 0.0189999
    families = ["render", "review", "preview", "gather"]
    allowed_extensions = ["mov", "mp4", "dpx", "cin", "exr", "jpg", "jpeg", "png"]

    optional = True
    active = True

    def timecode_to_frames(self, timecode, framerate):
        rt = otio.opentime.from_timecode(timecode, framerate)
        return int(otio.opentime.to_frames(rt))

    def frames_to_timecode(self, frames, framerate):
        rt = otio.opentime.from_frames(frames, framerate)
        return otio.opentime.to_timecode(rt)

    def frames_to_seconds(self, frames, framerate):
        rt = otio.opentime.from_frames(frames, framerate)
        return otio.opentime.to_seconds(rt)

    def offset_timecode(self, tc, framerate, offset=-1):
        tc_frames = self.timecode_to_frames(tc, framerate)
        tc_frames += offset
        tc = self.frames_to_timecode(tc_frames, framerate)
        return tc

    def get_length_ffprobe(self, input):
        length = subprocess.run([
            get_ffmpeg_tool_path("ffprobe"),
                "-v",
                "error",
                "-select_streams", "v:0",
                "-count_frames",
                "-show_entries",
                "stream=nb_read_frames",
                "-of", "csv=p=0",
                input.replace("\\", "/")
            ],
            check=True,
                capture_output=True,
                text=True
            ).stdout.strip("\n")
        return length

    def get_timecode_oiio(self, input):
        res = subprocess.run(
            [
                get_oiio_tools_path("iinfo"),
                "-v",
                input.replace("\\", "/")
            ],
            check=True,
            capture_output=True
        )
        lines = res.stdout.decode("utf-8").replace(" ", "").splitlines()
        for line in lines:
            if line.lower().find("timecode") > 0:
                vals = line.split(":")
                vals.reverse()
                nums = []
                for i in range(0, 4):
                    nums.append(vals[i])
                nums.reverse()
                tc = ":".join(nums)
                break
        tc = tc.replace("\"", "")
        return tc

    def get_timecode_ffprobe(self, input):
        tc = subprocess.run(
            [
                get_ffmpeg_tool_path("ffprobe"),
                "-v",
                "error",
                "-show_entries",
                "format_tags=timecode",
                "-of",
                "compact=print_section=0:nokey=1",
                input.replace("\\", "/")
            ],
            check=True,
            capture_output=True,
            text=True
        ).stdout.strip("\n")
        return tc

    def process(self, instance):
        timecode = instance.data.get("timecode", None)
        if not timecode:
            self.log.warning("No start timecode detected!!!")
            raise ValueError

        default_length = int(instance.data.get(
            "frameEndHandle", int(instance.data.get("ftrameEnd", 0)) + int(instance.data.get("handleEnd", 0))
        )) - int(instance.data.get(
            "frameStartHandle", int(instance.data.get("frameStart", 0)) - int(instance.data.get("handleStart", 0))
        )) + 1

        self.log.debug("Expected instance length is: {}".format(default_length))

        length_list = []
        for repre in instance.data["representations"]:
            self.log.debug("Processing repre '{}'".format(repre["name"]))
            if repre["ext"] in self.allowed_extensions and repre["name"] is not "thumbnail":
                length = None
                if isinstance(repre["files"], list):
                    length = len(repre["files"])
                    self.log.debug("Repre is a sequence, file list length is: '{}'".format(length))
                else:
                    length = self.get_length_ffprobe(repre["files"][0])
                    self.log.debug("FFprobe detected length is: '{}'".format(length))
                if length:
                    if length != default_length:
                        self.log.debug("Frame range of repre is different from asset length! setting to: '{}'".format(length))
                        length_list.append(length)
                else:
                    self.log.debug("could not infer length, tail timecode will not be set!")
                    raise ValueError
            else:
                self.log.debug("Repre not valid for timecode extraction, skipping...")
        
        final_length_list = sorted(list(set(length_list)))
        final_length = None

        if final_length_list:
            final_length = final_length_list[-1]
        else:
            final_length = default_length
        
        self.log.debug("Final length is: {}".format(final_length))

        tail_tc = self.offset_timecode(timecode, instance.data.get("fps", instance.context.data.get("fps", 24.0)), offset=final_length)

        for repre in instance.data["representations"]:
            if repre["name"] is not "thumbnail":
                repre["tail_timecode"] = tail_tc

        instance.data["tail_timecode"] = tail_tc
        self.log.debug("Final tail timecode is: {}".format(tail_tc))
                


                    
                    