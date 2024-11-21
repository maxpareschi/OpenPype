from typing import Union, Any

import os
import re
import json
import copy

import pyblish.api
from openpype.pipeline import publish

from ...lib.pipeline import (
    find_in_project_settings,
    get_profile
)
from ...lib.editorial import (
    truncate
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

    optional = True

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

        # Welcome and settings logging.
        self.log.info(f"Welcome to '{self.label}' plugin!") #type: ignore
        self.log.info((f"Found {len(self.profiles.keys())} " #type: ignore
                       f"Profiles in settings: {tuple(self.profiles.keys())}"))
        self.log.info((f"Found {len(self.presets.keys())} " #type: ignore
                       f"Presets in settings: {tuple(self.presets.keys())}"))

        # Validate instance so we process only what is
        # needed/valid for the plugin.
        if not self.validate_instance(instance):
            return
        
        # Backup original representation list and create
        # the empty final representation list
        original_representations = copy.deepcopy(instance.data.get("representations", []))
        final_representations = []

        # Get a instance data and build the current
        # profile to be matched against settings.
        instance_profile = self.get_instance_profile(instance)
        
        # Find the profile in settings using the
        # ttd_addon lib function. can be subbed by OpenPype
        # filter profiles, but it had some problems and we don't
        # want to mess with their code as much as we can.
        # If there's no match we return early.
        active_profile = get_profile(self.profiles, instance_profile, logger=self.log) #type: ignore
        if not active_profile:
            return
        
        # Raise if no presets are specified
        # since it's a configuration error.
        if not active_profile["linked_presets"]:
            raise ValueError("No linked presets in profiles! Please check your settings.")
        
        # Finds the active presets linked to the selected
        # profile we found earlier.
        active_presets = {}
        for key in active_profile["linked_presets"]:
            active_presets.update({key: self.presets[key]})

        # Log initial representation list.
        self.log.info((f"Initial representation list for instance " #type: ignore
                       f"{instance.data['name']}': \n"
                       f"{json.dumps(original_representations, indent=4, default=str)}"))

        # loop through representations and transcode if needed.
        for _idx, representation in enumerate(original_representations):

            # Log current representation.
            self.log.debug(f"Processing representation ({_idx + 1}): " #type: ignore
                           f"'{representation['name']}')")

            # Validate if the representation is valid for
            # transcoding purposes. If not valid append it
            # to the final representation list and skip.
            if not self.validate_representation(representation):
                final_representations.append(representation)
                continue

            # Make an independent copy of current repesentation
            # so we can change it without screwing up anything.
            working_repre = copy.deepcopy(representation)

            # Sanitize representation, refer to method docstring
            # to understand which element gets removed/added/changed.
            self.sanitize_representation(instance, working_repre, active_presets)

            # Start processing presets for current repre
            for preset_name, preset in active_presets.items():

                # Log current processing preset.
                self.log.debug(f"Processing preset '{preset_name}'") #type: ignore

                # Optionally override repre data based on settings
                self.override_representation_data(working_repre, preset)

                

        
        instance.data["representations"] = final_representations

    
    def validate_instance(self,
                          instance: pyblish.api.Instance) -> bool:
        """
        Validate if instance should be processed.
        Useful for skipping processing early in non compatible cases.

        Args:
            instance (pyblish.api.Instance): Instance to be checked.

        Returns:
            bool: True if valid else False.
        """

        # Check if instance is tagged to be processed on farm
        if instance.data.get("farm", None):
            self.log.warning("Instance is tagged for farm processing, skipping...") #type: ignore
            return False

        # Check if there are profiles in settings
        if not self.profiles:
            self.log.warning("No profiles specified in settings, skipping...") #type: ignore
            return False
        
        # Check if instance has representations
        if not instance.data.get("representations", None):
            self.log.warning("No Representations found, skipping...") #type: ignore
            return False

        return True

    def validate_representation(self,
                                representation: 'dict[str, Any]') -> bool:
        """
        Validate if representation should be processed.
        Needed to skip or enable processing depending on features.

        Args:
            representation (dict): Representation to be checked.

        Returns:
            bool: True if valid else False.
        """

        # If repre is a thumbnail then skip
        if (
            representation.get("thumbnail", False) or
            representation["name"] == "thumbnail" or
            "thumbnail" in representation.get("tags", [])
        ):
            self.log.info(f"Representation '{representation['name']}' is a thumbnail, skipping...") #type: ignore
            return False
        
        # If repre is not a supported image or movie format then skip
        if representation.get("ext") not in self.supported_exts:
            self.log.info((f"Representation '{representation['name']}' has " #type: ignore
                           f"unsupported extension: '{representation.get('ext', None)}', "
                            "skipping..."))
            return False
        
        # If repre does not have files then skip since
        # there's no transcoding to be done.
        if not representation.get("files", None):
            self.log.info((f"Representation '{representation['name']}' does not " #type: ignore
                            "have any file, skipping..."))
            return False
    
        # Catch edge case for review made from hiero through
        # otio, so we skip it regularly since it would apply
        # double transforms on everything.
        if (
            "review" in representation.get("tags", []) and
            representation["name"].find("otio") >= 0
        ):
                self.log.info((f"Representation '{representation['name']}' is already " #type: ignore
                                "processed as review item possibly from hiero as an otio "
                                "extracted sequence, skipping..."))
                return False

        self.log.info(f"Representation '{representation['name']}' is valid.") #type: ignore

        return True

    def get_instance_profile(self,
                             instance: pyblish.api.Instance) -> 'dict[str, str]':
        # Build matching profile from instance data
        profile = {
            "hosts": instance.context.data["hostName"],
            "families": instance.data["family"],
            "assets": instance.data["asset"],
            "task_names": instance.data["anatomyData"].get("task", {}).get("name", ""),
            "task_types": instance.data["anatomyData"].get("task", {}).get("type", ""),
            "subsets": instance.data["subset"]
        }
        return profile

    def sanitize_representation(self,
                                instance: pyblish.api.Instance,
                                representation: 'dict[str, Any]',
                                presets: 'dict[str, Any]') -> None:
        """
        Fixes representation data to be compatible with transcoding.
        - Remove the review tag from representation so it does
          not get into conflicts with transcoding presets
          which may have a review in them.
        - Sets Timecode in representation
        - Sets Colorspace in representation
        """

        # Search for review tags in passed profiles.
        presets_have_review_tags = False
        for preset in presets.values():
            if "review" in preset.get("tags", []):
                presets_have_review_tags = True
                break
        
        # Remove review tag from repre only if profiles
        # have a review tag in them. Otherwise keep it.
        if (
            "review" in representation.get("tags", []) and
            presets_have_review_tags
        ):
            representation["tags"].remove("review")
            self.log.info((f"Removed 'review' tag " #type: ignore
                           f"from representation '{representation['name']}' "
                           "to ensure no conflicts with extractors running afterwards."))
        
        # Add timecode to representation
        default_tc = instance.data.get("default_timecode", "01:00:00:01")
        representation["timecode"] = instance.data.get("timecode", default_tc)
        self.log.info((f"Added timecode '{representation['timecode']}' " #type: ignore
                      f"to representation '{representation['name']}'"))

        # Add colorspace to representation
        self.process_representation_colorspace(instance, representation)

    def process_representation_colorspace(self,
                                          instance: pyblish.api.Instance,
                                          representation: dict,
                                          default_linear: str = "scene_linear",
                                          default_gamma: str = "color_picking",
                                          default_log: str = "compositing_log") -> None:
        """
        Process colorspace, adding it to repre if found in
        versionData or defaulting to presets in method
        signature.

        Args:
            instance (object): Parent instance
            representation (dict): Representation to be checked.
            default_linear (str): default linear colorspace
            default_gamma (str): default gamma colorspace
            default_log (str): default log colorspace

        Returns:
            None
        """
        # Set default colorspaces based on extension
        # This helps to process extensions
        if representation.get("ext") in self.linear_image_exts:        
            colorspace = default_linear
        elif representation.get("ext") in self.log_image_exts:
            colorspace = default_log
        else:
            colorspace = default_gamma

        # Checks if there's a versionData colorspace already
        # computed and assign that if found. otherwise
        # stick to the default value.
        version_colorspace = instance.data.get("versionData", {}).get("colorspace", None)
        if version_colorspace:
            colorspace = version_colorspace
        
        # Assign colorspace to representation
        representation["colorspace"] = colorspace
        self.log.info((f"Added colorspace '{representation['colorspace']}' " #type: ignore
                       f"to representation '{representation['name']}'"))

    def override_representation_data(self,
                                     representation: dict,
                                     preset: dict) -> None:
        """
        Updates metadata per Representation using settings overrides.

        Args:
            representation (dict): Representation to be overridden.
            preset (dict): preset with overrides

        Returns:
            Nothing
        """

        # Store input overrides
        overrides = preset["input_overrides"]

        # skip if override block is disabled
        if not overrides["enabled"]:
            self.log.info("Input overrides are disabled for this preset. Moving on.") #type: ignore
            return
        
        # Override colorspace
        if overrides["colorspace"]:
            representation["colorspace"] = overrides["colorspace"]
        
        # Override timecode
        if overrides["timecode"]:
            representation["timecode"] = overrides["timecode"]

        # Override fps
        if float(overrides["fps"]) > float(0):
            representation["fps"] = truncate(overrides["fps"], 3)

        # Store metadata overrides on representation
        representation["metadata"] = {}
        for key, value in overrides["metadata"]:
            representation["metadata"].update({
                key: value
            })

        
