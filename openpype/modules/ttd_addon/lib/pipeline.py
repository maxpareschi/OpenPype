from typing import Union, Any

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


class SequenceInfo:
    
    def __init__(self, frames: 'list[str]' = list()) -> None:
        self.frames = frames,
        self.frame_start: int = 0
        self.frame_end: int = 0
        self.length: int = 0
        self.head: str = ""
        self.tail: str = ""
        self.padding: int = 0
        self.frame_digits: 'tuple[int, int]' = (0, 0)
        self.frame_divider = "."
        self.tail_divider = "."
        self._frame_pattern: str = r'[._]{0}\.\D+\d?$'.format(clique.DIGITS_PATTERN)

    def assemble(self) -> None:
        collections, remainders = clique.assemble(self.frames,
                                                  patterns=[self._frame_pattern],
                                                  assume_padded_when_ambiguous=True)
        self.frame_start = collections[0].indexes[0]
        self.frame_end = collections[0].indexes[-1]
        self.length = len(collections[0].indexes)
        self.head = collections[0].head.replace(".", "")
        self.tail = collections[0].tail.replace(".", "")
        self.padding = collections[0].padding
        self.frame_digits = (len(str(self.frame_start)), len(str(self.frame_end)))

    def find(self, path: str) -> None:
        files = os.listdir(path)
        self.frames = files
        self.assemble()

    def resample(self,
                 start_frame: int = 1001,
                 length: int = 0,
                 prefix: str = "",
                 suffix: str = "",
                 padding: int = 4) -> 'list[str]':
        
        frames = []

        if length == 0:
            length = self.length

        for f in range(length):
            frames.append((
                f"{self.head}{self.frame_divider}"
                f"{str(f+start_frame).zfill(padding)}"
                f"{self.tail_divider}{self.tail}" 
            ))

        return frames
        

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


def assemble_file_sequence(files: 'list[str]') -> None:
    """
    Find file sequences
    
    Returns a settings dict or None if no settings are found.
    """

    frame_pattern = r'[._]{0}\.\D+\d?$'.format(clique.DIGITS_PATTERN)

    sequence_data = {

    }

    collections, remainders = clique.assemble(files, patterns=[frame_pattern], assume_padded_when_ambiguous=True)

    for coll in collections:
        print(coll.format())
        print(f"{coll.head} - {coll.padding} - {coll.tail}")
        

    print(remainders)
    print("---")



##########################
##         TESTS        ##
##########################

if __name__ == "__main__":

    import json

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

    dirs = [
        "X:/prj/DEMETER/editorial/edit_resources/plates/20241024/PKG_DEMT401_20241023/PKG_DEMT401_20241023/DMR401_060_340",
        "X:/prj/DEMETER/editorial/edit_resources/plates/20241007_2/PKG-DEMT402_VFX Pull_22Dogs_2024.10.04/DMR402_Sc006_FXPULL_241003-plates_22dogs/DMR402_006_060_FG1",
        "X:/prj/OBX/editorial/plates/rain_elements_exr/Generic_Rain_Lens_01/RainOnLens",
        "C:/Users/max.pareschi/Desktop/ihjsd"
    ]
    for d in dirs:
        assemble_file_sequence(os.listdir(d))