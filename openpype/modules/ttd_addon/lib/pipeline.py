import os
from typing import Union
from openpype.settings import get_project_settings


def search_paths_recursive(path: str) -> dict:
    """
    Scans recursively in directory for nested
    directories and return those paths.

    Useful for adding plugins in arbitrary trees
    and adding them to the search paths.

    Returns a lits of paths in string format.
    """
    plugin_paths = {}
    for dir in os.scandir(path):
        if dir.is_dir():
            key = os.path.basename(dir)
            plugin_paths.update(
                {
                    key: [
                        os.path.join(path, key).replace("\\", "/")
                    ]
                }
            )
            for root, dirs, files in os.walk(dir, topdown=False):
                for d in dirs:
                    plugin_paths[key].append(
                        os.path.normpath(os.path.join(root, d)).replace("\\", "/")
                    )
    return plugin_paths


def find_key_recursive(search_dict: dict, search_key: str) -> Union[dict, None]:
    """
    Takes a dict with nested lists and dicts, searches all dicts
    for a key of the field provided.
    
    Returns value of first matched key or None if no match is found.
    """
    result = None
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


def find_all_keys_recursive(search_dict: dict, search_key: str) -> list:
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


def find_in_project_settings(search_key: str) -> Union[dict, None]:
    """
    Find current project settings matching a provided key.
    Useful to find plugin settings in case auto discovery
    does not work.
    
    Returns a settings dict or None if no settings are found.
    """
    project_settings = get_project_settings(os.environ["AVALON_PROJECT"])
    return find_key_recursive(project_settings, search_key)

    