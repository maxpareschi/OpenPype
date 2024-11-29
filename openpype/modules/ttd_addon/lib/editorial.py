from typing import Union, Any
from dataclasses import dataclass, field

import os
import re
import math
import json
import copy
import subprocess

import opentimelineio as otio

try:
    from openpype.modules.ttd_addon.lib.pipeline import (
        find_key_recursive,
        get_oiio,
        get_ffmpeg,
        execute_subprocess
    )
except:
    from pipeline import (
        find_key_recursive,
        get_oiio,
        get_ffmpeg,
        execute_subprocess
    )


SEQUENCE_FRAME_PATTERN = r"\.(?P<index>(?P<padding>0*)\d+)\.\D+\d?$"

VIDEO_EXTENSIONS = [
    ".mov", ".mkv", ".mp4", ".m4v",
    ".m4p", ".mpeg", ".mpg", ".m2v",
    ".mp2", ".mpv", ".avi", ".webm",
    ".qt", ".yuv", ".asf"
]

IMAGE_EXTENSIONS = [
    ".exr", ".dpx", ".cin", ".hdr",
    ".hdri", ".tif", ".tif", ".psd",
    ".psb", ".pix", ".bmp", ".tga",
    ".sgi",".png", ".jpg", ".jpeg",
    ".webp"
]

COLORSPACE_MAPPINGS = {
    "Linear": "ACES - ACEScg",
    "rec709": "Output - Rec.709",
    "sRGB": "Output - Rec.709",
    "bt709": "Output - Rec.709"
}


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


@dataclass
class ImageData:
    """
    Simple data holding class for Image
    and Video files properties.
    Metadata is never printed but always
    present. It may be empty though.
    """
    path: str = ""
    width: int = 0
    height: int = 0
    data_width: int = 0
    data_height: int = 0
    pixel_aspect: float = 1.0
    origin_x: int = 0
    origin_y: int = 0
    channels: int = 0
    fps: float = 0.0
    length: int = 0
    timecode: str = ""
    colorspace: str = ""
    bit_depth: str = ""
    file_type: str = ""
    metadata: 'dict[str, Any]' = field(repr = False,
                                       default_factory=lambda: dict())
    

def get_image_data_iinfo(path: str,
                   keys: 'list[str]' = list()) -> ImageData:
    """
    Returns a dict of image fields found using IINFO.
    This works best for image (especially exr).
    if keys argument has a "*" entry then all metadata
    will be fetched, otherwise only requested ones.

    Args:
        path (str): file to run iinfo against.
        keys (list[str]): keys to return

    Returns:
        (ImageData) an ImageData object
    """

    image_data = ImageData(path = path)

    cmd = [
        get_oiio("iinfo"),
        "-v",
        path.replace("\\", "/")
    ]

    res: str = execute_subprocess(cmd)

    for _idx, line in enumerate(res.splitlines()):
        line = line.strip()
        line_key, line_value = line.split(": ", maxsplit=1)
        line_key = line_key.strip("\"").strip("\'")
        line_value = line_value.strip("\"").strip("\'")
        if _idx == 0:
            resolution, channels, image_type = (data.strip() for data in line_value.split(","))
            image_data.data_width, image_data.data_height = (int(pixels.strip()) for pixels in resolution.split("x"))
            image_data.channels = int(channels.split(" ")[0])
            image_data.bit_depth = image_type.split(" ")[0].strip()
            image_data.file_type = image_type.split(" ")[-1].strip()
        else:
            if "pixel data origin" in line_key:
                image_data.origin_x, image_data.origin_y = (int(val.strip()) for val in line_value.replace("x=", "").replace("y=", "").split(",")) 
            elif "full/display size" in line_key:
                image_data.width, image_data.height = (int(pixels.strip()) for pixels in line_value.split("x"))
            elif "framesPerSecond" in line_key or "FramesPerSecond" in line_key:
                image_data.fps = truncate(eval(line_value.split(" ")[0].strip()), 3)
            elif "smpte:TimeCode" in line_key:
                image_data.timecode = line_value
            elif "PixelAspectRatio" in line_key:
                image_data.pixel_aspect = float(line_value)
            elif "oiio:ColorSpace" in line_key:
                image_data.colorspace = COLORSPACE_MAPPINGS.get(line_value, line_value)
            if "*" in keys:
                image_data.metadata.update({
                    line_key: line_value
                })
            else:
                for key in keys:
                    if line_key.lower().find(key.lower()) >= 0:
                        image_data.metadata.update({
                            line_key: line_value
                        })
    
    if image_data.width == 0:
        image_data.width = image_data.data_width
    if image_data.height == 0:
        image_data.height = image_data.data_height

    return image_data


def get_image_data_ffprobe(path: str,
                     keys: 'list[str]' = list()) -> ImageData:
    """
    Returns a dict of image fields found using FFPROBE.
    This works best for movies.
    if keys argument has a "*" entry then all metadata
    will be fetched, otherwise only requested ones.

    Args:
        path (str): file to run ffprobe against.
        keys (list[str]): keys to return

    Returns:
        (ImageData) an ImageData object
    """

    image_data = ImageData(path = path)

    cmd = [
        get_ffmpeg("ffprobe"),
        "-v",
        "error",
        "-hide_banner",
        "-print_format",
        "json",
        "-show_streams",
        "-select_streams", "v:0",
        "-show_format",
        path.replace("\\", "/")
    ]
    res = json.loads(execute_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW))

    image_data.data_width = find_key_recursive(res, "width") or 0
    image_data.data_height = find_key_recursive(res, "height") or 0
    image_data.width = image_data.data_width or 0
    image_data.height = image_data.data_height or 0
    image_data.pixel_aspect = truncate(eval(find_key_recursive(res, "sample_aspect_ratio").replace(":", "/")), 3) or 1.0
    image_data.fps = truncate(eval(find_key_recursive(res, "avg_frame_rate")), 3) or 24.0
    image_data.length = int(find_key_recursive(res, "nb_frames")) or 1
    image_data.timecode = find_key_recursive(res, "timecode") or "01:00:00:01"
    image_data.bit_depth = find_key_recursive(res, "pix_fmt") or "yuv422p"
    image_data.file_type = find_key_recursive(res, "codec_type") or "video"
    image_data.metadata = dict()

    colorspace = find_key_recursive(res, "color_space")
    colorspace = COLORSPACE_MAPPINGS.get(colorspace, colorspace)
    channels = 3
    if image_data.bit_depth and image_data.bit_depth.find("4444") >= 0:
        channels = 4
    image_data.colorspace = colorspace or "color_picking"
    image_data.channels = channels

    if "*" in keys:
        image_data.metadata = res
    else:
        for key in keys:
            value = find_key_recursive(res, key)
            if value:
                image_data.metadata.update({ key: value })

    return image_data


def get_image_data(path: str,
                   keys: 'list[str]' = list(),
                   backend: str = "") -> Union[ImageData, None]:
    """
    Gets image data from either video or image files.
    Also reads and stores any metadata.

    Args:
        path (str): file to run ffprobe against.
        keys (list[str]): keys to return
        backend (str): choose either 'iinfo', 'ffprobe'
            to specify backend to use, otherwise auto
            mode will be enabled (video with ffprobe,
            images with iinfo)

    Returns:
        (ImageData, None) an ImageData object or None if
        source file type (extension) is not supported.
    """
    res = None
    try:
        if backend == "ffprobe":
            res = get_image_data_ffprobe(path=path, keys=keys)
        elif backend == "iinfo":
            res = get_image_data_iinfo(path=path, keys=keys)
        else:
            if os.path.splitext(path)[1] in VIDEO_EXTENSIONS:
                res = get_image_data_ffprobe(path=path, keys=keys)
            elif os.path.splitext(path)[1] in IMAGE_EXTENSIONS:
                res = get_image_data_iinfo(path=path, keys=keys)
    except:
        pass
    return res


@dataclass
class SequenceData:
    """
    Simple data holding class for Image
    and video naming to sequence.
    """
    path: str = ""
    head: str = ""
    tail: str = ""
    padding: int = 0
    frame_start: int = 0
    length: int = 0
    frame_list: 'list[str]' = field(default_factory=lambda: list())
    image_data: Union[ImageData, None] = None


def get_sequence_data(file_list: 'list[str]' = list(),
                      root_path: str = "",
                      search_string: str = "") -> 'Union[None, SequenceData, list[SequenceData]]':
    """
    Gets a list of SequenceData. Also scans
    the files and stores an ImageData instance.
    
    Args:
        file_list (list[str]): a list of valid files. Can be
            basenames or full paths. if basename then root_path
            also needs to be provided.
        root_path (str): the dirname of the files you want to
            assemble. Can be provided alone to scan a directory,
            in that case multiple sequences or none will be returned.
        search_string (str): can be specified to limit the scan
            results, can also accept regex syntax.

    Returns:
        (list[SequenceData]) a list of sequence objects
    """
    
    files = file_list

    if not file_list and not root_path:
        raise ValueError("Please pass either a 'file_list', a 'root_path' or both!")
    elif file_list and root_path:
        if not os.path.isfile(os.path.join(root_path, files[0]).replace("\\", "/")):
            raise ValueError("Cannot read files in 'root_path/file_list'!")
    elif file_list and not root_path:
        if os.path.isfile(files[0]):
            root_path = os.path.dirname(files[0])
            files = [os.path.basename(file) for file in files]
        else:
            raise ValueError("Cannot read files in 'file_list'!")
    elif root_path and not file_list:
        files = [
            file for file
            in os.listdir(root_path)
            if os.path.isfile(os.path.join(root_path, file))
        ]
    else:
        raise ValueError("Unhandled exception! Fuck this! Probably a bug...")
    
    sequence_list = []
    sequence_data = {}

    pattern = re.compile(r"(.*{0}.*)(?<=[\.\_])(\d+)(?=[\.]|$)(.*)".format(search_string))
    
    for file in files:
        frame_match = pattern.match(file)
        
        if not frame_match:
            file_head, file_tail = os.path.splitext(file)
            sequence = SequenceData()
            sequence.path = root_path
            sequence.head = file_head
            sequence.tail = file_tail
            sequence.padding = 0
            sequence.length = 1
            sequence.frame_start = 0
            sequence.frame_list = [file]
            sequence.image_data = get_image_data(f"{root_path}/{file}")
            sequence_list.append(sequence)
            continue

        head, frame_number, tail = frame_match.groups()
        if head in sequence_data:
            if tail in sequence_data[head]:
                sequence_data[head][tail].append(frame_number)
            else:
                sequence_data[head][tail] = [frame_number]
        else:
            sequence_data[head] = {tail: [frame_number]}

    for head, tails in sequence_data.items():
        for tail, frames in tails.items():
            sequence = SequenceData()
            sequence.path = root_path
            sequence.length = len(frames)
            sequence.head = head if sequence.length > 1 else f"{head}{frames[0]}"
            sequence.tail = tail
            sequence.frame_start = int(sorted(frames)[0])
            sequence.padding = len(str(sorted(frames)[-1]))
            sequence.frame_list = sorted([f"{head}{frame}{tail}" for frame in frames])
            sequence.image_data = get_image_data(f"{root_path}/{head}{sorted(frames)[0]}{tail}")
            sequence_list.append(sequence)
    
    if sequence_list:
        if len(sequence_list) < 2:
            sequence_list = sequence_list[0]
    else:
        sequence_list = None

    return sequence_list


def resample_sequence(sequence: SequenceData,
                      suffix: str = "",
                      frame_divider: str = ".") -> SequenceData:
    
    sequence.head = sequence.head[:-2] + (f"_{suffix}" if suffix else "") + sequence.head[-1:]
    sequence.frame_list = []
    if sequence.length == 1:
        sequence.frame_list.append(f"{sequence.head}{sequence.tail}")
    else:
        for frame in range(sequence.length):
            frame_path = f"{sequence.head[:-1]}"
            frame_path += f"{frame_divider}{str(frame + sequence.frame_start).zfill(sequence.padding)}"
            frame_path += sequence.tail
            sequence.frame_list.append(frame_path)
    
    sequence.image_data = ImageData()

    return sequence
