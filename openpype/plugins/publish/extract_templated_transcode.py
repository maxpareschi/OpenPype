import os
import sys
import copy
import json
import re
import shutil
import clique
import subprocess
import pyblish.api

from openpype.pipeline import (
    anatomy,
    publish
)

from openpype.pipeline.template_data import get_template_data_with_names

from openpype.lib.applications import (
    ApplicationManager,
)

from openpype.lib.profiles_filtering import filter_profiles


class ExtractTemplatedTranscode(publish.Extractor):
    """
        Extracts transcodes following profiles, using nuke terminal as a base.
        Ideal for farm or multi output, should also work for hiero.
    """

    label = "Extract Templated Transcodes"
    order = pyblish.api.ExtractorOrder + 0.019

    optional = True

    # Supported extensions
    supported_exts = ["exr", "dpx", "mov", "mxf"]

    # Configurable by Settings
    profiles = None

    def process(self, instance):

        if not self.profiles:
            self.log.debug("No profiles present for color transcode")
            return

        if "representations" not in instance.data:
            self.log.debug("No representations, skipping.")
            return

        profile = self._get_profile(instance)
        if not profile:
            return

        template_format_data = self._get_template_data_format()

        new_representations = []
        repres = instance.data["representations"]

        self.log.debug("Initial representation list: {}".format(
            json.dumps(repres, indent=4, default=str)
        ))

        added_representations = False
        added_review = False

        for idx, repre in enumerate(list(repres)):

            self.log.debug("repre ({}): '{}':\n{}".format(
                idx + 1, repre["name"], json.dumps(repre, indent=4, default=str)))

            if not self._repre_is_valid(repre, instance):
                continue

            for profile_name, profile_def in profile.get("outputs", {}).items():
                self.log.debug("Processing profile '{}'".format(profile_name))

                new_repre = copy.deepcopy(repre)

                repre_name_override = profile_def["representation_name_override"].strip()

                if profile_name == "passthrough":
                    if repre_name_override:
                        new_repre["name"] = repre["name"] + "_" + repre_name_override
                        new_repre["outputName"] = repre_name_override
                    instance.data["representations"].append(new_repre)
                    self.log.debug("profile is in passthrough mode, skipping transcode and adding as representation: {}".format(
                        json.dumps(new_repre, indent=4, default=str)))
                    continue

                new_ext = profile_def["extension"].strip()
                if new_ext == "":
                    new_ext = new_repre["ext"].replace('.', '')
                new_repre["ext"] = new_ext

                repre_name_suffix = ""
                if repre_name_override != "":
                    repre_name_suffix = "_" + repre_name_override
                    new_repre["outputName"] = repre_name_override
                    self.log.debug("Representation outputName is set as '{}'".format(new_repre["outputName"]))

                new_repre["name"] = new_repre["ext"] + repre_name_suffix
                self.log.debug("Representation name is set as '{}'".format(new_repre["name"]))

                transcoding_type = profile_def["transcoding_type"]
                template_original_path = profile_def["template_path"]["template"].strip()
                subset_chain = profile_def["subset_chain"]
                input_colorspace = profile_def["color_conversion"]["input_colorspace"].strip()
                if input_colorspace == "":
                    input_colorspace = new_repre["colorspaceData"]["colorspace"]
                output_colorspace = profile_def["color_conversion"]["output_colorspace"].strip()
                color_config = new_repre["colorspaceData"]["config"]["path"].strip()

                if transcoding_type == "template":
                    if template_original_path == "":
                        self.log.debug("Skipping Representation: missing template path!")
                        continue
                elif transcoding_type == "chain_subsets":
                    if len(subset_chain) == 0:
                        self.log.debug("Skipping Representation: missing subset chain!")
                        continue
                elif transcoding_type == "color_conversion":
                    if output_colorspace == "":
                        self.log.debug("Skipping Representation: missing output color profile!")
                        continue

                new_staging_dir = self._get_transcode_temp_dir(
                    self.staging_dir(instance),
                    profile_name)
                new_repre["stagingDir"] = new_staging_dir

                orig_file_list = list(set(copy.deepcopy(new_repre["files"])))

                renamed_files = []
                for file_name in orig_file_list:
                    head, _ = os.path.splitext(file_name)
                    frame = re.findall(r'\d+$', head)[0]
                    frame_index = head.find(frame) -1
                    new_head = head[:frame_index]
                    if new_repre.get("outputName"):
                        new_head = new_head + '_{}'.format(new_repre["outputName"])
                    new_head = new_head + head[frame_index:]
                    new_file_name = "{}.{}".format(new_head, new_repre["ext"])
                    renamed_files.append(new_file_name)
                new_repre["files"] = renamed_files

                repre_in = self._translate_to_sequence(repre)
                repre_out = self._translate_to_sequence(new_repre)

                nuke_script_save_path = os.path.join(
                    new_repre["stagingDir"],
                    "{}nk".format(repre_out[1])
                ).replace("\\", "/")

                self.log.debug(json.dumps(profile_def, indent=4, default=str))

                processed_data = {
                    "mode": transcoding_type,
                    "input_path": repre_in[0],
                    "output_path": repre_out[0],
                    "save_path": nuke_script_save_path,
                    "frameStart": instance.data["frameStart"]-instance.data["handleStart"],
                    "frameEnd": instance.data["frameEnd"]+instance.data["handleEnd"],
                    "fps": instance.data["fps"],
                    "project": instance.data["anatomyData"]["project"],
                    "asset": instance.data["asset"],
                    "task": instance.data.get("task", ""),
                    "input_colorspace": input_colorspace,
                    "output_colorspace": output_colorspace,
                    "color_config": color_config,
                    "reformat": profile_def["reformat_options"]["enabled"],
                    "reformat_type": profile_def["reformat_options"]["reformat_type"],
                    "reformat_width": profile_def["reformat_options"]["reformat_width"],
                    "reformat_height": profile_def["reformat_options"]["reformat_height"],
                    "override_thumbnail": profile_def["override_thumbnail"],
                    "thumbnail_path": os.path.join(
                        os.path.dirname(repre_out[0]),
                        repre_out[1] + "thumbnail.jpg"
                    ).replace("\\", "/")
                }

                if transcoding_type == "template":
                    processed_data["template"] = template_original_path.format(**template_format_data)
                    self.log.debug("will process template '{}' for rendering in context '{}'".format(
                        processed_data["template"],
                        instance.data["asset"]
                    ))

                elif transcoding_type == "chain_subsets":
                    subset_list = []
                    for subset in subset_chain:
                        subset_list.append(subset)
                    processed_data["subset_chain"] = subset_list
                    self.log.debug("Will chain subsets {} for rendering in context '{}'".format(
                        processed_data["subset_chain"],
                        instance.data["asset"]
                    ))

                elif transcoding_type == "color_conversion":
                    new_repre["colorspaceData"]["colorspace"] = output_colorspace
                    self.log.debug("Will convert representation from '{}' to '{}' for rendering in context '{}'".format(
                        processed_data["input_colorspace"],
                        processed_data["output_colorspace"],
                        instance.data["asset"]
                    ))

                nuke_process = self.run_transcode_script(processed_data)
                
                # baking_data = {
                #     "bakeRenderPath": new_repre["stagingDir"],
                #     "bakeScriptPath": nuke_script_save_path,
                #     "bakeWriteNodeName": "WRITE_TRANSCODE"
                # }
                #  
                # if not instance.data.get("bakingNukeScripts", False):
                #     instance.data["bakingNukeScripts"] = []
                # instance.data["bakingNukeScripts"].append(baking_data)

                if int(nuke_process) != 0:
                    self.log.debug("Transcode process returned a non zero code, skipping...")
                    continue

                # cleanup temporary transcoded files
                instance.context.data["cleanupFullPaths"].append(
                    new_staging_dir)

                custom_tags = profile_def.get("custom_tags")
                if custom_tags:
                    if new_repre.get("custom_tags") is None:
                        new_repre["custom_tags"] = []
                    new_repre["custom_tags"].extend(custom_tags)

                if new_repre.get("tags") is None:
                    new_repre["tags"] = []
                for tag in profile_def["tags"]:
                    if tag not in new_repre["tags"]:
                        new_repre["tags"].append(tag)

                    if tag == "review":
                        added_review = True

                # If there is only 1 file outputted then convert list to
                # string, cause that'll indicate that its not a sequence.
                if len(new_repre["files"]) == 1:
                    new_repre["files"] = new_repre["files"][0]

                # If the source representation has "review" tag, but its not
                # part of the output defintion tags, then both the
                # representations will be transcoded in ExtractReview and
                # their outputs will clash in integration.
                if "review" in repre.get("tags", []):
                    added_review = True

                self.log.debug("Adding new representation: {}".format(
                    json.dumps(new_repre, indent=4, default=str)))

                new_representations.append(new_repre)
                added_representations = True

                if profile_def["override_thumbnail"] and os.path.exists(processed_data["thumbnail_path"]):
                    self.log.debug("Starting thumbnail override...")
                    thumb_missing = True
                    thumb_repre = {
                        "name": "thumbnail",
                        "outputName": "thumb",
                        "ext": os.path.splitext(
                            os.path.basename(processed_data["thumbnail_path"]))[1].replace(".", ""),
                        "tags": [
                            "thumbnail",
                            "publish_on_farm"
                        ],
                        "stagingDir": os.path.dirname(processed_data["thumbnail_path"]).replace("\\", "/"),
                        "files": os.path.basename(processed_data["thumbnail_path"])
                    }

                    for repre_id, repre_search in enumerate(instance.data["representations"]):
                        if repre_search.get("name", "") == "thumbnail" or "thumbnail" in repre_search.get("tags", []):
                            instance.data["representations"][repre_id] = thumb_repre
                            thumb_missing = False
                    
                    if thumb_missing:
                        instance.data["representations"].append(thumb_repre)
                    
                    self.log.debug("Thumbnail set as representation: {}".format(
                        json.dumps(thumb_repre, indent=4, default=str)))

            if added_representations:
                self._mark_original_repre_for_deletion(repre, profile,
                                                       added_review) 

        for repre in tuple(instance.data["representations"]):
            tags = repre.get("tags") or []
            if "delete_original" in tags:
                instance.data["representations"].remove(repre)     
        
        instance.data["representations"].extend(new_representations)
        self.log.debug("Final Representations list: \n{}\nFull Representations dump: {}\n".format(
            "\n".join([
                "name: '{}'\t| ext: '{}'\t| tags: '{}'\t| outputName: '{}'\t| colorspace: '{}'".format(
                    repre["name"], repre["ext"], repre.get("tags", None),
                    repre.get("outputName", None), repre.get("colorspaceData",{}).get("colorspace", None)
                ) for repre in instance.data["representations"]
            ]),
            json.dumps(instance.data["representations"], indent=4, default=str)
        ))
        self.log.debug(json.dumps(instance.data["representations"], indent=4, default=str))
                    
    def _translate_to_sequence(self, repre):
        pattern = [clique.PATTERNS["frames"]]
        collections, remainder = clique.assemble(
            repre["files"], patterns=pattern,
            assume_padded_when_ambiguous=True)

        if collections:
            if len(collections) > 1:
                raise ValueError(
                    "Too many collections {}".format(collections))

            collection = collections[0]
            frames = list(collection.indexes)
            padding = len(str(frames[0]))
            frame_str = "".join(["#" for i in range(padding)])
            file_name = os.path.join(
                repre["stagingDir"],
                "{}{}{}".format(
                    collection.head,
                    frame_str,
                    collection.tail
                )
            ).replace("\\", "/")

        return file_name, collection.head, frame_str, collection.tail, frames

    def _get_profile(self, instance, log=None):
        """Returns profile if and how repre should be color transcoded."""
        host_name = instance.context.data["hostName"]
        family = instance.data["family"]
        task_data = instance.data["anatomyData"].get("task", {})
        task_name = task_data.get("name")
        task_type = task_data.get("type")
        subset = instance.data["subset"]
        filtering_criteria = {
            "hosts": host_name,
            "families": family,
            "task_names": task_name,
            "task_types": task_type,
            "subsets": subset
        }

        profile = filter_profiles(self.profiles, filtering_criteria,
                                  logger=log)

        if not profile:
            self.log.debug((
              "Skipped instance. None of profiles in presets are for"
              " Host: \"{}\" | Families: \"{}\" | Task \"{}\""
              " | Task type \"{}\" | Subset \"{}\" "
            ).format(host_name, family, task_name, task_type, subset))

        return profile

    def _repre_is_valid(self, repre, instance):
        """Validation if representation should be processed.

        Args:
            repre (dict): Representation which should be checked.

        Returns:
            bool: False if can't be processed else True.
        """

        if repre.get("thumbnail", False) or repre["name"] == "thumbnail" or "thumbnail" in repre.get("tags", []):
            self.log.debug((
                "Representation '{}' is a thumbnail. Skipped."
            ).format(repre["name"], repre.get("ext")))
            return False

        if "review" in repre.get("tags", []):
            self.log.debug((
                "Representation '{}' is already processed as review item, skipping"
            ).format(repre["name"], repre.get("ext")))
            return False

        if repre.get("ext") not in self.supported_exts:
            self.log.debug((
                "Representation '{}' has unsupported extension: '{}'. Skipped."
            ).format(repre["name"], repre.get("ext")))
            return False

        if not repre.get("files"):
            self.log.debug((
                "Representation '{}' has empty files. Skipped."
            ).format(repre["name"]))
            return False

        if not repre.get("colorspaceData"):
            cdata = {
                "colorspace": "scene_linear",
                "config": {
                    "path": os.environ.get("OCIO", os.environ["OPENPYPE_OCIO_CONFIG"])
                }
            }
            version_data_cspace = instance.data.get("versionData", {}).get("colorspace", None)
            if version_data_cspace:
                cdata["colorspace"] = version_data_cspace
            repre["colorspaceData"] = cdata

        if not repre.get("colorspaceData"):
            self.log.debug("Representation '{}' has no colorspace data. "
                           "Skipped.".format(repre["name"]))
            return False

        return True

    def _get_transcode_temp_dir(self, basedir, name):
        new_dir = os.path.join(
            basedir,
            "temp_transcode_{}".format(name)
        ).replace("\\", "/")
        shutil.rmtree(new_dir, ignore_errors=True)
        os.makedirs(new_dir, exist_ok=True)
        return new_dir

    def _get_template_data_format(self):
        ana = anatomy.Anatomy(os.environ["AVALON_PROJECT"])
        roots = anatomy.Roots(ana).roots
        template_data = get_template_data_with_names(
            project_name=os.environ["AVALON_PROJECT"],
            asset_name=os.environ["AVALON_ASSET"]
        )
        template_data.update({"root": roots})
        template_data.update(os.environ)
        return template_data

    def run_transcode_script(self, data):
        self.log.debug("TRANSCODE >>> START (Nuke terminal mode)\n")
        return_code = 1

        app_manager = ApplicationManager()
        nuke_app = app_manager.find_latest_available_variant_for_group("nuke")
        if not nuke_app:
            self.log.warning("Nuke path not found, no transcoding possible.")
            return
        
        if sys.platform == "win32":
            ext = ".exe"

        nukepy = os.path.join(
            os.path.dirname(str(nuke_app.find_executable())),
            "python" + ext
        ).replace("\\", "/")

        nukeexe = str(nuke_app.find_executable())

        script_path = os.path.join(
            os.path.dirname(__file__),
            "extract_templated_transcode_script.py"
        ).replace("\\", "/")        
        
        env = copy.deepcopy(os.environ)
        env["PYTHONPATH"]= os.pathsep.join(sorted(list(set(env["PYTHONPATH"].split(";")))))
        env.update({
            "project_name": data["project"]["name"],
            "asset_name": data["asset"],
            "task_name": data["task"]
        })

        json_args = os.path.join(
            os.path.dirname(data["save_path"]),
            os.path.splitext(os.path.basename(data["save_path"]))[0] + ".json"
        ).replace("\\", "/")
        with open(json_args, "w") as data_json:
            data_json.write(json.dumps(data, indent=4, default=str))

        cmd = [
            os.path.abspath(nukepy),
            os.path.abspath(script_path),
            os.path.abspath(json_args)
        ]
        
        self.log.debug("Json exchange file written to: {}".format(json_args))
        self.log.debug("Launcing suprocess: {}".format(" ".join(cmd)))

        process = subprocess.Popen(
            cmd,
            universal_newlines=True,
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            bufsize=1,
            start_new_session=True,
            env=env
        )

        return_code = process.wait()

        self.log.debug("TRANSCODE >>> END (return code: {})\n".format(return_code))

        return return_code

    def _mark_original_repre_for_deletion(self, repre, profile, added_review):
        """If new transcoded representation created, delete old."""
        if not repre.get("tags"):
            repre["tags"] = []

        delete_original = profile["delete_original"]
        keep_original_review = profile["keep_original_review"]

        if delete_original:
            repre["tags"].append("delete_original")

        if added_review and "review" in repre["tags"] and not keep_original_review:
            repre["tags"].remove("review")