import math
import json
import subprocess
from typing import Union

import opentimelineio as otio

from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path,
    run_subprocess
)
from openpype.modules.ttd_addon.lib.pipeline import (
    find_all_keys_recursive
)


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


def get_timecode_ffprobe(in_file: str) -> 'list[str]':
    """
    Returns a list of timecode found using FFPROBE.
    This works best for movie files.

    Args:
        in_file (str): file to run oiio against.

    Returns:
        list[str] Timecode list of strings
    """
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
    res = json.loads(run_subprocess(cmd, creationflags=subprocess.CREATE_NO_WINDOW))
    tc = list(set(find_all_keys_recursive(res, "timecode")))[0]
    return tc