import os
from typing import Union
from openpype.settings import get_project_settings



def search_paths_recursive(path: str) -> dict:
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


def find_key_recursive(settings: dict, search_key: str) -> Union[dict, None]:
    """
    Takes a dict with nested lists and dicts, searches all dicts
    for a key of the field provided.
    
    Returns value of first matched key or None if no match is found.
    """
    result = None
    for key in settings.keys():
        if key == search_key:
            result = settings[key]
            break
        elif isinstance(settings[key], dict):
            result = find_key_recursive(settings[key], search_key)
            if result:
                break
        elif isinstance(settings[key], (list, tuple)):
            for item in settings[key]:
                if isinstance(item, dict):
                    result = find_key_recursive(item, search_key)
                    if result:
                        break
    return result


def find_in_project_settings(search_key: str) -> Union[dict, None]:
    """
    Find current project settings matching a provided key.
    Useful to find plugin settings in case auto discovery
    does not work.
    
    Returns a settings dict or None if no settings are found.
    """
    project_settings = get_project_settings(os.environ["AVALON_PROJECT"])
    return find_key_recursive(project_settings, search_key)

    