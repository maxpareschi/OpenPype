import os
import subprocess
import pyblish.api

from openpype.pipeline import publish
from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path
)


class ExtractTimecode(publish.Extractor):
    """
    Extractor for general timecode
    """

    label = "Extract Timecode"
    order = order = pyblish.api.ExtractorOrder + 0.01899
    families = ["render", "review", "preview"]
    allowed_extensions = ["mov", "mp4", "dpx", "cin", "exr"]

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
        default_tc = "01:00:00:00"
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

        for repre in instance.data["representations"]:
            if repre["ext"] in self.allowed_extensions:
                repre["timecode"] = final_tc
        instance.data["timecode"] = final_tc

        self.log.debug("Final timecode for instance set to: '{}'".format(final_tc))


                    
                    