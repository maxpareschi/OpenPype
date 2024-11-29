from typing import Union, Optional, Any
import sys
import os
import math
import json
import subprocess
import logging

import opentimelineio as otio
import clique

try:
    from openpype.lib import (
        get_oiio_tools_path,
        get_ffmpeg_tool_path,
        run_subprocess
    )
    from openpype.modules.ttd_addon.lib.pipeline import (
        find_all_keys_recursive,
        find_key_recursive
    )
except:
    from ....lib import (
        get_oiio_tools_path,
        get_ffmpeg_tool_path,
        run_subprocess
    )
    from pipeline import (
        find_all_keys_recursive,
        find_key_recursive
    )


SEQUENCE_FRAME_PATTERN = fr"[._]{clique.DIGITS_PATTERN}\.\D+\d?$"


class ImageVideoInfo:
    def __init__(self,
                 path: 'Union[str, None]' = None,
                 keys: 'list[str]' = list(),
                 logger: 'Union[logging.Logger, None]' = None) -> None:
        self.path = path
        self.keys = keys
        self._video_extensions = [
            ".mov", ".mkv", ".mp4", ".m4v",
            ".m4p", ".mpeg", ".mpg", ".m2v",
            ".mp2", ".mpv", ".avi", ".webm",
            ".qt", ".yuv", ".asf"
        ]
        self.initialize()
        if path:
            self.read(path)

    def initialize(self):
        self.width: int = 0
        self.height: int = 0
        self.data_width: int = 0
        self.data_height: int = 0
        self.pixel_aspect = 1.0
        self.origin_x: int = 0
        self.origin_y: int = 0
        self.channels: int = 3
        self.fps: float = 24.0
        self.length: int = 1
        self.timecode: str = "01:00:00:01"
        self.colorspace = "scene_linear"
        self.bit_depth: str = ""
        self.file_type: str = ""
        self.metadata = dict()
    
    def __repr__(self) -> str:
        params: 'list[str]' = []
        for key in vars(self):
            if key == "_video_extensions" or key == "keys":
                continue
            elif key == "metadata":
                params.append("\"metadata\": \"{0} entries\"".format(len(getattr(self, key).keys())))
            else:
                params.append("\"{0}\": \"{1}\"".format(key, getattr(self, key)))
        return (f"<{self.__class__.__qualname__} object at at {id(self)}: {{ {', '.join(params)} }} >")

    def read(self, path: str = "", keys: 'list[str]' = list()) -> None:
        if keys:
            self.keys = keys
        if path:
            self.path = path
        self.initialize()
        if os.path.splitext(self.path)[-1] in self._video_extensions: #type: ignore
            res = get_info_ffprobe(self.path, keys=self.keys) #type: ignore
        else:
            res = get_info_iinfo(self.path, keys=self.keys) #type: ignore
        if isinstance(res, dict):
            for key, value in res.items():
                setattr(self, key, value)


class SequenceInfo:
    """
    Compose a sequence object with useful properties
    for further processing. Uses clique to assemble frames.
    """
    def __init__(self,
                 frames: 'Optional[list[str]]' = None,
                 logger: 'Optional[logging.Logger]' = None) -> None:

        self.root: 'Optional[str]' = None
        self.frames: 'Optional[list[str]]' = frames
        self.frame_start: Optional[int] = None
        self.frame_end: Optional[int] = None
        self.length: Optional[int] = None
        self.head: Optional[str] = None
        self.tail: Optional[str] = None
        self.padding: Optional[int] = None
        self.indexes: 'Optional[list[int]]' = None
        self.frame_digits: 'Optional[tuple[int, int]]' = None
        self.frame_divider: str = "."
        self.log: logging.Logger = logger if logger else (
            logging.getLogger(self.__class__.__qualname__)
        )
        self._frame_pattern: str = SEQUENCE_FRAME_PATTERN
        self.assemble()

    def __repr__(self) -> str:
        return (f"<{self.__class__.__qualname__} "
                f"object at {id(self)}> {{ {self.head}%0{self.padding}d{self.tail}, "
                f"length: {self.length}, start: {self.frame_start}, "
                f"digits: {self.frame_digits} }}")

    def assemble(self, frames: 'Optional[list[str]]' = None) -> None:
        if not frames:
            frames = self.frames
        if frames:
            collections, _ = clique.assemble(frames,
                                             patterns=[self._frame_pattern],
                                             assume_padded_when_ambiguous=False)
            self.root = os.path.dirname(frames[0]).replace("\\", "/")
            self.frame_start = list(collections[0].indexes)[0]
            self.frame_end = list(collections[0].indexes)[-1]
            self.indexes = list(collections[0].indexes)
            self.length = len(collections[0].indexes)
            self.head = str(collections[0].head)
            self.tail = str(collections[0].tail)
            self.padding = len(str(self.frame_end))
            self.frame_divider = str(self.head)[-1:]
            self.frame_digits = (len(str(self.frame_start)), len(str(self.frame_end)))

    def digits_check(self):
        if self.frame_digits and (self.frame_digits[0] == self.frame_digits[1]):
            return True
        return False

    def resample(self,
                 root: Optional[str] = None,
                 head: Optional[str] = None,
                 tail: Optional[str] = None,
                 frame_start: Optional[int] = None,
                 length: Optional[int] = None,
                 padding: Optional[int] = None,
                 suffix: Optional[str] = None,
                 frame_divider: Optional[str] = None) -> 'list[str]':
        """
        Resample a file list based on keywords provided.
        This method will use class properties that
        were assembled upon creation as fallback for
        any not provided keyword.
        """
        frames: 'list[str]' = []
        if not root:
            root = self.root
        if not head:
            head = self.head
        if not tail:
            tail = self.tail
        if not frame_start:
            frame_start = self.frame_start
        if not length:
            length = self.length
        if not padding:
            padding = self.padding
        if not frame_divider:
            frame_divider = self.frame_divider
        if frame_start and padding and length and head and tail:
            for frame in range(length):
                new_frame: str = root if root else ""
                new_frame += head[:-1]
                if suffix:
                    new_frame = new_frame + '_' + suffix
                new_frame += frame_divider
                new_frame += str(frame + frame_start).zfill(padding)
                new_frame += tail # type: ignore
                frames.append(new_frame)
        return frames


def find_sequences(path: str) -> 'list[SequenceInfo]':
    files: 'list[str]' = []
    sequences: 'list[SequenceInfo]' = []
    path_contents = os.scandir(path)
    for entry in path_contents:
        if entry.is_file():
            files.append(entry.name)
    collections, remainders = clique.assemble(files,
                                              patterns=[SEQUENCE_FRAME_PATTERN],
                                              assume_padded_when_ambiguous=True)
    for collection in collections:
        collected_files = [
            (
                f"{collection.head}"
                f"{str(frame).zfill(collection.padding)}"
                f"{collection.tail}".replace("\\", "/")
            )
            for frame in collection.indexes
        ]
        sequence = SequenceInfo(collected_files)
        sequences.append(sequence)
    return sequences


def truncate(number: float, digits: int) -> float:
    """
    Returns a float number with appropriate truncation
    Useful to reason with framerates in DCCs    

    Args:
        number (float): arbitrary float number
        digits (int): number of digits to truncate after dot

    Returns:
        float: truncated float value
    """
    try:
        nbDecimals = len(str(number).split('.')[1])
    except:
        nbDecimals = 0
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return float(math.trunc(stepper * number) / stepper)


def frames_to_timecode(frames: int, framerate: float) -> str:
    """
    Returning timecode from frames.

    Args:
        frames (int): frame
        framerate (float): frame rate

    Returns:
        str: timecode string
    """
    rt = otio.opentime.from_frames(frames, framerate)
    return str(otio.opentime.to_timecode(rt, framerate))


def timecode_to_frames(timecode: str, framerate: float) -> int:
    """
    Returning frames from timecode.

    Args:
        timecode (str): timecode string
        framerate (float): frame rate

    Returns:
        int: frames
    """
    rt = otio.opentime.from_timecode(timecode, framerate)
    return int(otio.opentime.to_frames(rt, framerate))


def shift_timecode(timecode: str, offset: int, framerate: float) -> str:
    """
    Returning timecode shifted by 'offset' frames.
    'offset' can be negative (useful for slates)

    Args:
        timecode (str): timecode string
        offset (int): frame
        framerate (float): frame rate

    Returns:
        str: offsetted timecode string
    """
    rt = otio.opentime.from_timecode(timecode, framerate)
    fr = int(otio.opentime.to_frames(rt, framerate))
    fr += offset
    new_rt = otio.opentime.from_frames(fr, framerate)
    return str(otio.opentime.to_timecode(new_rt, framerate))


def get_timecode_oiio(in_file: str) -> Union[str, None]:
    """
    Returns timecode found using OIIO.
    This works best for dpx and exr file sequences.

    Args:
        in_file (str): file to run oiio against.

    Returns:
        str | None: Timecode string or None if not found.
    """
    cmd = [
        get_oiio_tools_path("iinfo"),
        "-v",
        in_file.replace("\\", "/")
    ]
    res = run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW)
    lines = res.replace(" ", "").splitlines()
    found_timecodes = []
    tc = None
    for l in lines:
        if l.lower().find("timecode") >= 0:
            found_timecodes.append(l)
    for tcode in found_timecodes:
        if tcode.find("smpte") >= 0:
            if tcode.find(";") >= 0:
                tc = ":".join(tcode.split(":")[-3:])
            else:
                tc = ":".join(tcode.split(":")[-4:])
    return tc


def get_info_iinfo(in_file: str,
                   keys: 'list[str]' = list()) -> 'dict[str, Any]':
    """
    Returns a dict of image fields found using IINFO.
    This works best for image (especially exr).
    if keys argument has a "*" entry then all metadata
    will be fetched, otherwise only requested ones.

    Args:
        in_file (str): file to run iinfo against.
        keys (list[str]): keys to return

    Returns:
        (dict) a dictionary with searched key value pairs
    """
    returned_keys = {
        "width": 0,
        "height": 0,
        "data_width": 0,
        "data_height": 0,
        "origin_x": 0,
        "origin_y": 0,
        "pixel_aspect": 1.0,
        "fps": 24.0,
        "length": 1,
        "timecode": "01:00:00:01",
        "colorspace": "scene_linear",
        "channels": 3,
        "bit_depth": "",
        "file_type": "",
        "metadata": {}
    }
    colorspace_mapping = {
        "Linear": "scene_linear",
        "rec709": "color_picking"
    }
    cmd = [
        get_oiio_tools_path("iinfo"),
        "-v",
        in_file.replace("\\", "/")
    ]
    res: str = run_subprocess(cmd)

    for _idx, line in enumerate(res.splitlines()):
        line = line.strip()
        line_key, line_value = line.split(": ", maxsplit=1)
        line_key = line_key.strip("\"").strip("\'")
        line_value = line_value.strip("\"").strip("\'")
        if _idx == 0:
            resolution, channels, image_type = (data.strip() for data in line_value.split(","))
            data_width, data_height = (int(pixels.strip()) for pixels in resolution.split("x"))
            channels = int(channels.split(" ")[0])
            bit_depth = image_type.split(" ")[0].strip()
            file_type = image_type.split(" ")[-1].strip()
            returned_keys.update({
                "data_width": data_width,
                "data_height": data_height,
                "channels": channels,
                "bit_depth": bit_depth,
                "file_type": file_type
            })
        else:
            if "pixel data origin" in line_key:
                origin_x, origin_y = (int(val.strip()) for val in line_value.replace("x=", "").replace("y=", "").split(",")) 
                returned_keys.update({
                    "origin_x": origin_x,
                    "origin_y": origin_y
                })
            elif "full/display size" in line_key:
                width, height = (int(pixels.strip()) for pixels in line_value.split("x"))
                returned_keys.update({
                    "width": width,
                    "height": height
                })
            elif "framesPerSecond" in line_key or "FramesPerSecond" in line_key:
                fps = truncate(eval(line_value.split(" ")[0].strip()), 3)
                returned_keys.update({
                    "fps": fps
                })
            elif "smpte:TimeCode" in line_key:
                timecode = line_value
                returned_keys.update({
                    "timecode": timecode
                })
            elif "PixelAspectRatio" in line_key:
                pixel_aspect = float(line_value)
                returned_keys.update({
                    "pixel_aspect": pixel_aspect
                })
            elif "oiio:ColorSpace" in line_key:
                returned_keys.update({
                    "colorspace": colorspace_mapping.get(line_value, line_value)
                })
            if "*" in keys:
                returned_keys["metadata"].update({
                    line_key: line_value
                })
            else:
                for key in keys:
                    if line_key.lower().find(key.lower()) >= 0:
                        returned_keys["metadata"].update({
                            line_key: line_value
                        })
    
    if returned_keys["width"] == 0:
        returned_keys["width"] = returned_keys["data_width"]
    if returned_keys["height"] == 0:
        returned_keys["height"] = returned_keys["data_height"]

    return returned_keys


def get_info_ffprobe(in_file: str,
                     keys: 'list[str]' = list()) -> 'dict[str, Any]':
    """
    Returns a dict of image fields found using FFPROBE.
    This works best for movies.
    if keys argument has a "*" entry then all metadata
    will be fetched, otherwise only requested ones.

    Args:
        in_file (str): file to run iinfo against.
        keys (list[str]): keys to return

    Returns:
        (dict) a dictionary with searched key value pairs
    """
    colorspace_mapping = {
        "sRGB": "color_picking",
        "bt709": "color_picking"
    }
    cmd = [
        get_ffmpeg_tool_path("ffprobe"),
        "-v",
        "error",
        "-hide_banner",
        "-print_format",
        "json",
        "-show_streams",
        "-select_streams", "v:0",
        "-show_format",
        in_file.replace("\\", "/")
    ]
    res = json.loads(run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW))

    returned_keys = {
        "data_width": find_key_recursive(res, "width") or 0,
        "data_height": find_key_recursive(res, "height") or 0,
        "pixel_aspect": truncate(eval(find_key_recursive(res, "sample_aspect_ratio").replace(":", "/")), 3) or 1.0,
        "fps": truncate(eval(find_key_recursive(res, "avg_frame_rate")), 3) or "24.0",
        "length": int(find_key_recursive(res, "nb_frames")) or 1,
        "timecode": find_key_recursive(res, "timecode") or "01:00:00:01",
        "bit_depth": find_key_recursive(res, "pix_fmt") or "yuv422p",
        "file_type": find_key_recursive(res, "codec_type") or "video",
        "metadata": {}
    }

    colorspace = find_key_recursive(res, "color_space")
    colorspace = colorspace_mapping.get(colorspace, colorspace)
    channels = 3
    if returned_keys["bit_depth"] and returned_keys["bit_depth"].find("4444") >= 0:
        channels = 4

    returned_keys.update({
        "width": returned_keys["data_width"] or 0,
        "height": returned_keys["data_height"] or 0,
        "colorspace": colorspace or "color_picking",
        "channels": channels,
    })

    if "*" in keys:
        returned_keys["metadata"] = res
    else:
        for key in keys:
            value = find_key_recursive(res, key)
            if value:
                returned_keys.update({ key: value })

    return returned_keys


def get_info_ffprobe_basic(in_file: str,
                           keys: 'list[str]' = list()) -> 'dict[str, Any]':
    """
    Returns a dict of requested fields found using FFPROBE.
    This works best for movie files.

    Args:
        in_file (str): file to run ffprobe against.
        keys (list[str]): keys to return

    Returns:
        (dict) a dictionary with searched key value pairs
    """
    returned_keys = {}
    cmd = [
        get_ffmpeg_tool_path("ffprobe"),
        "-v",
        "error",
        "-hide_banner",
        "-print_format",
        "json",
        "-show_streams",
        "-select_streams", "v:0",
        "-show_format",
        in_file.replace("\\", "/")
    ]
    res = json.loads(run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW))
    res["streams"] = [res["streams"][0]]
    for key in keys:
        found_value = list(set(find_all_keys_recursive(res, key)))[0]
        if found_value:
            returned_keys.update({ key: found_value })
    
    return returned_keys


def get_timecode_ffprobe(in_file: str) -> str:
    """
    Returns a timecode found using FFPROBE.
    This works best for movie files.

    Args:
        in_file (str): file to run ffprobe against.

    Returns:
        (str) Timecode
    """
    return str(get_info_ffprobe_basic(in_file, ["timecode"]).get("timecode", "01:00:00:01"))


def get_fps_ffprobe(in_file: str) -> float:
    """
    Returns a list of timecode found using FFPROBE.
    This works best for movie files.

    Args:
        in_file (str): file to run ffprobe against.

    Returns:
        (float) Frames per second
    """
    fps = get_info_ffprobe_basic(in_file, ["avg_frame_rate"]).get("avg_frame_rate", 0.0)
    if fps:
        fps = truncate(
            eval(
                get_info_ffprobe_basic(in_file, ["avg_frame_rate"])["avg_frame_rate"]
            ),
            3
        )
    return fps


def get_length_ffprobe(in_file: str) -> int:
    """
    Returns a length found using FFPROBE.
    This works best for movie files.

    Args:
        in_file (str): file to run ffprobe against.

    Returns:
        (int) Length in frames
    """
    frames = get_info_ffprobe_basic(in_file, ["nb_frames"]).get("nb_frames", 0)
    return int(frames)