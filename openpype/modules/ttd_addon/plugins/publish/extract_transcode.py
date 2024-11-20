from typing import Union, Any

import os
import re

import pyblish.api
from openpype.pipeline import publish
from openpype.lib.profiles_filtering import filter_profiles

# Absolute import, replace with new dir structure if changing location
from openpype.modules.ttd_addon.lib.pipeline import (
    find_in_project_settings,
    get_profile
)


class ExtractTranscode(publish.Extractor):
    """
    Extracts transcodes from presets filtered by profiles.
    Uses nuke terminal to process, needs a Nuke Render license.
    Ideal for farm or multi output. Works with any video or image
    
    Inherits from pyblish.Plugin
    """

    label = "Extract Transcodes - TTD"
    order = pyblish.api.ExtractorOrder

    # optional = True

    # Supported extensions
    linear_image_exts = ["exr", "hdr", "hdri"]
    log_image_exts = ["dpx", "cin"]
    gamma_image_exts = ["jpg", "jpeg", "png", "tif", "tiff", "psd", "bmp", "sgi"]
    movie_exts = ["mov", "mxf", "mp4", "avi"]
    supported_exts = linear_image_exts + log_image_exts + gamma_image_exts + movie_exts

    # Find settings and cache them in the plugin for faster access
    settings = find_in_project_settings("extract_transcode")
    presets = settings["presets"]
    profiles = settings["profiles"]


    def process(self, instance: pyblish.api.Instance) -> None:

        self.log.info(f"Welcome to '{self.label}' plugin!")
        self.log.info(f"Found {len(self.profiles.keys())} Profiles: {tuple(self.profiles.keys())}")
        self.log.info(f"Found {len(self.presets.keys())} Presets: {tuple(self.presets.keys())}")

        if not self.validate_instance(instance):
            return

        instance_profile = self.get_instance_profile(instance)
        profile = get_profile(self.profiles, instance_profile, logger=self.log)
        if not profile:
            return
        
        presets = profile["linked_presets"]
        
        new_representations = []
        
        for repre in instance.data.get("representations", []):
            if not self.validate_representation(repre):
                continue

        instance.data["representations"] = new_representations

    
    def validate_instance(self, instance: pyblish.api.Instance) -> bool:
        """
        Validate if instance should be processed.
        Useful for skipping processing early in non compatible cases.

        Args:
            instance (pyblish.api.Instance): Instance to be checked.

        Returns:
            bool: True if valid else False.
        """

        if instance.data.get("farm", None):
            self.log.warning("Instance is tagged for farm processing, skipping...")
            return False

        if not self.profiles:
            self.log.warning("No profiles specified in settings, skipping...")
            return False
        
        if not instance.data.get("representations", None):
            self.log.warning("No Representations found, skipping...")
            return False

        return True

    def validate_representation(self, representation: 'dict[str, Any]') -> bool:
        """
        Validate if representation should be processed.
        Needed to skip or enable processing depending on features.

        Args:
            representation (dict): Representation to be checked.

        Returns:
            bool: True if valid else False.
        """
        if (
            representation.get("thumbnail", False) or
            representation["name"] == "thumbnail" or
            "thumbnail" in representation.get("tags", [])
        ):
            self.log.info(f"Representation '{representation['name']}' is a thumbnail, skipping...")
            return False
        
        if (
            "review" in representation.get("tags", []) and
            representation["name"].find("otio") >= 0
        ):
            self.log.info((f"Representation '{representation['name']}' is already "
                            "processed as review item and comes from hiero as an otio "
                            "extracted sequence, skipping..."))
            return False
        
        if representation.get("ext") not in self.supported_exts:
            self.log.info((f"Representation '{representation['name']}' has "
                           f"unsupported extension: '{representation.get('ext', None)}', "
                            "skipping..."))
            return False
        
        if not representation.get("files", None):
            self.log.info((f"Representation '{representation['name']}' does not "
                            "have any file, skipping..."))
            return False
    
        return True
        
    def process_representation_colorspace(self,
                                          instance: pyblish.api.Instance,
                                          representation: dict,
                                          default_linear: str = "scene_linear",
                                          default_gamma: str = "color_picking",
                                          default_log: str = "compositing_log") -> str:
        """
        Process colorspace, adding it to repre if found in
        versionData or defaulting to filetype.
        Needed to skip or enable processing depending on features.

        Args:
            instance (dict): Parent instance
            representation (dict): Representation to be checked.
            default_linear (str): default linear colorspace
            default_gamma (str): default gamma colorspace
            default_log (str): default log colorspace

        Returns:
            str: Colorspace computed, fallback to scene_linear
        """
        if representation.get("ext") in self.linear_image_exts:        
            colorspace = default_linear
        elif representation.get("ext") in self.log_image_exts:
            colorspace = default_log
        else:
            colorspace = default_gamma

        version_colorspace = instance.data.get("versionData", {}).get("colorspace", None)
        if version_colorspace:
            colorspace = version_colorspace
        representation["colorspace"] = colorspace
        
        return representation["colorspace"]
    
    def get_instance_profile(self, instance: pyblish.api.Instance) -> 'dict[str, str]':
        profile = {
            "hosts": instance.context.data["hostName"],
            "families": instance.data["family"],
            "assets": instance.data["asset"],
            "task_names": instance.data["anatomyData"].get("task", {}).get("name", ""),
            "task_types": instance.data["anatomyData"].get("task", {}).get("type", ""),
            "subsets": instance.data["subset"]
        }
        return profile

    def update_representation_metadata(self,
                                       representation: dict,
                                       metadata: dict) -> None:
        """
        Updates metadata per Representation using settings overrides.

        Args:
            representation (dict): Representation to be checked.
            metadata (dict): profile metadata overrides as dict

        Returns:
            Nothing
        """
        pass