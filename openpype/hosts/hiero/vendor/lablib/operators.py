from __future__ import annotations
from dataclasses import dataclass, field

import os
import re


@dataclass
class ImageInfo:
    filename: str = None
    origin_x: int = 0
    origin_y: int = 0
    width: int = 1920
    height: int = 1080
    display_width: int = 1920
    display_height: int = 1080
    channels: int = 3
    fps: float = 24.0
    par: float = 1.0
    timecode: str = "01:00:00:01"


@dataclass
class SequenceInfo:
    path: str = None
    frames: list[str] = field(default_factory = list)
    frame_start: int = None
    frame_end: int = None
    head: str = None
    tail: str = None
    padding: int = 0
    hash_string: str = None
    format_string: str = None

    def _get_file_splits(self, file_name: str) -> None:
        head, ext = os.path.splitext(file_name)
        frame = int(re.findall(r'\d+$', head)[0])
        return head.replace(str(frame), ""), frame, ext
    
    def _get_length(self) -> int:
        result = int(self.frame_end) - int(self.frame_start) + 1
        return result

    def compute_all(self,
                scan_dir: str,
                return_only_longer: bool = True) -> list:
        files = os.listdir(scan_dir)
        sequenced_files = []
        matched_files = []
        for f in files:
            head, tail = os.path.splitext(f)
            matches = re.findall(r'\d+$', head)
            if matches:
                sequenced_files.append(f)
                matched_files.append(head.replace(matches[0], ""))
        matched_files = list(set(matched_files))

        results = []
        for m in matched_files:
            seq = SequenceInfo()
            for sf in sequenced_files:
                if m in sf:
                    seq.frames.append(
                        os.path.join(
                            scan_dir,
                            sf
                        ).replace("\\", "/")
                    )

            head, frame, ext = self._get_file_splits(seq.frames[0])
            seq.path = os.path.abspath(scan_dir).replace("\\", "/")
            seq.frame_start = frame
            seq.frame_end = self._get_file_splits(seq.frames[-1])[1]
            seq.head = os.path.basename(head)
            seq.tail = ext
            seq.padding = len(str(frame))
            seq.hash_string = "{}#{}".format(os.path.basename(head), ext)
            seq.format_string = "{}%0{}d{}".format(os.path.basename(head), len(str(frame)), ext)
            results.append(seq)

        return results
    
    def compute_longest(self, scan_dir: str) -> SequenceInfo:
        return self.compute_all(scan_dir = scan_dir)[0]


@dataclass
class RepoTransform:
    translate: list[float] = field(default_factory = lambda: list([0.0, 0.0]))
    rotate: float = 0.0
    scale: list[float] = field(default_factory = lambda: list([0.0, 0.0]))
    center: list[float] = field(default_factory = lambda: list([0.0, 0.0]))


@dataclass
class FileTransform:
    src: str = ""
    cccId: str = "0"
    direction: int = 0


@dataclass
class DisplayViewTransform:
    src: str = "ACES - ACEScg"
    display: str = "ACES"
    view: str = "Rec.709"
    direction: int = 0


@dataclass
class ColorSpaceTransform:
    src: str = "ACES - ACEScg"
    dst: str = "ACES - ACEScg"


@dataclass
class CDLTransform:
    # src: str = "" # NOT NEEDED, USE FILETRANSFORM FOR CDL FILES
    offset: list[float] = field(default_factory = lambda: list([0.0, 0.0, 0.0]))
    power: list[float] = field(default_factory = lambda: list([1.0, 1.0, 1.0]))
    slope: list[float] = field(default_factory = lambda: list([0.0, 0.0, 0.0]))
    sat: float = 1.0
    description: str = ""
    id: str = ""
    direction: int = 0


