import math
import opentimelineio as otio


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