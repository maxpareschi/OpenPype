from typing import Union, Any

import os
import logging

try:
    from openpype.settings import get_current_project_settings
except:
    logging.root.setLevel(logging.NOTSET)
    logging.basicConfig()
    logging.debug("Testing 'ttd_addon/lib/pipeline.py' as standalone script.")


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