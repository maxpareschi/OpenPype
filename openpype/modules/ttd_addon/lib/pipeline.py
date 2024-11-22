from typing import Union, Optional, Any

import os
import re
import logging
import clique

try:
    from openpype.settings import get_current_project_settings
except:
    logging.root.setLevel(logging.NOTSET)
    logging.basicConfig()
    logging.debug("Testing 'ttd_addon/lib/pipeline.py' as standalone script.")


SEQUENCE_FRAME_PATTERN = fr"[._]{clique.DIGITS_PATTERN}\.\D+\d?$"


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
        self.frame_divider: str = "KKK"
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
        if frame_start and padding and length and self.head and self.tail:
            for frame in range(length):
                new_frame: str = root if root else ""
                new_frame += self.head[:-2]
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


def search_paths_recursive(path: str) -> 'list[str]':
    """
    Scans recursively in directory for nested
    directories and return those paths.

    Useful for adding plugins in arbitrary trees
    and adding them to the search paths.

    Returns a list of paths in string format.
    """
    plugin_paths = []
    for dir in os.scandir(path):
        if dir.is_dir():
            plugin_paths.append(
                os.path.join(path).replace("\\", "/")
            )
            for root, dirs, files in os.walk(dir, topdown=False):
                for d in dirs:
                    plugin_paths.append(
                        os.path.normpath(os.path.join(root, d)).replace("\\", "/")
                    )
    return plugin_paths


def find_key_recursive(search_dict: dict,
                       search_key: str) -> 'dict[str, Any]':
    """
    Takes a dict with nested lists and dicts, searches all dicts
    for a key of the field provided.
    
    Returns value of first matched key or an empty dict if no match is found.
    """
    result = dict()
    for key in search_dict.keys():
        if key == search_key:
            result = search_dict[key]
            break
        elif isinstance(search_dict[key], dict):
            result = find_key_recursive(search_dict[key], search_key)
            if result:
                break
        elif isinstance(search_dict[key], (list, tuple)):
            for item in search_dict[key]:
                if isinstance(item, dict):
                    result = find_key_recursive(item, search_key)
                    if result:
                        break
    return result


def find_all_keys_recursive(search_dict: dict,
                            search_key: str) -> list:
        """
        Takes a dict with nested lists and dicts,
        and searches all dicts for a key of the field
        provided.

        Returns a list of matching values found .
        """
        fields_found = []
        for key, value in search_dict.items():
            if key == search_key:
                fields_found.append(value)
            elif isinstance(value, dict):
                results = find_all_keys_recursive(value, search_key)
                for result in results:
                    fields_found.append(result)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    if isinstance(item, dict):
                        more_results = find_all_keys_recursive(item, search_key)
                        for another_result in more_results:
                            fields_found.append(another_result)
        return fields_found


def find_in_project_settings(search_key: str) -> 'dict[str, Any]':
    """
    Find current project settings matching a provided key.
    Useful to find plugin settings in case auto discovery
    does not work.
    
    Returns a settings dict or None if no settings are found.
    """
    project_settings = get_current_project_settings() #type: ignore
    return find_key_recursive(project_settings, search_key)
    

def get_profile(profiles: 'Union[list[dict[str, list[str]]], dict[str, dict[str, list[str]]]]',
                match: 'dict[str, str]',
                logger: 'Union[logging.Logger, None]' = None) -> 'dict[str, Any]':
    """
    Selects profile from a list of profiles.
    Works by assigning points depending on match:
    +1: key is present and value matches
    0: key is present and value is empty [*]
    -1 key is present and value does not match

    IMPORTANT:
    Match profile should be a dict with matching keys.
    Every matching key should be a single string value.
    This reflects the status of the match (which is always single).
    ---
    
    Returns selected profile (the one with the most points)
    for current process.
    """

    if not logger:
        logger = logging.getLogger(__name__)

    profile_list = []
    matched_profiles = []

    # format a profile object into a list to standardize looping
    if isinstance(profiles, dict):
        for profile in profiles.values():
            profile_list.append(profile)
    elif isinstance(profiles, list):
        profile_list = profiles
    else:
        raise TypeError("Supplied profile data if neither a list or a dict!")
    
    logger.info(f"Matching profiles with data: {match}")

    for profile in profile_list:
        logger.info(f"Scanning Profile: {profile}")
        
        points = 0

        for match_key, match_value in match.items():
            source_match = profile.get(match_key, None)
            if not match_value or not source_match:
                continue
            if match_value in source_match:
                points += 1
            else:
                points = -1
                break
        if points >= 0:
            matched_profiles.append((points, profile))
    
    matched_profiles.sort(key = lambda x: x[0], reverse=True)
    selected_profile = [p[1] for p in matched_profiles if p[0] == matched_profiles[0][0]]
    

    if len(selected_profile) > 1:
        raise ValueError(("\nWrong 'selected_profile' list lenght: "
                          f"{json.dumps(selected_profile, indent=4, default=str)}"
                          "\nThis means that multiple profiles have been found. "
                          "Please review your profile definitions!"))
    
    elif not selected_profile:
        logger.info(("No matching profile found! If this was not expected "
                     "please review your settings."))
        selected_profile = {}
    else:
        logger.info(f"Selected Profile: {selected_profile}")
        selected_profile, = selected_profile

    return selected_profile


##########################
##         TESTS        ##
##########################

if __name__ == "__main__":

    import json

    TEST_PROFILES = False
    TEST_SEQUENCES = True

    if TEST_PROFILES:
        profiles_path = "C:/Users/max.pareschi/Desktop/project_settings.json"
        with open(profiles_path) as f:
            profiles = json.loads(f.read())["project_settings/ttd_addon"]["publish_plugins"]["extract_transcode"]["profiles"]
        test_data = {
            "hosts": "Nuke",
            "families": "rgsdfg",
            "assets": "SHOT0010",
            "task_names": "compositing",
            "task_types": "Compositing",
            "subsets": "renderCompositingMain"
        }
        profile_points = get_profile(profiles, test_data)

    if TEST_SEQUENCES:
        dirs = [
            "X:/prj/DEMETER/editorial/edit_resources/plates/20241024/PKG_DEMT401_20241023/PKG_DEMT401_20241023/DMR401_060_340",
            "X:/prj/DEMETER/editorial/edit_resources/plates/20241007_2/PKG-DEMT402_VFX Pull_22Dogs_2024.10.04/DMR402_Sc006_FXPULL_241003-plates_22dogs/DMR402_006_060_FG1",
            "X:/prj/OBX/editorial/plates/rain_elements_exr/Generic_Rain_Lens_01/RainOnLens",
            "C:/Users/max.pareschi/Desktop/ihjsd"
        ]
        for dir in dirs:
            sequences = find_sequences(dir)
            for sequence in sequences:
                print(sequence)
                # new_files = sequence.resample(frame_start=0, padding=7)
                # print(new_files)