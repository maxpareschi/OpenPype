import os
import sys
import copy
import json
import re
import shutil
import clique
import subprocess
import uuid
import platform
if platform.system().lower() == "windows":
    from ctypes import create_unicode_buffer, windll

import pyblish.api

from openpype.pipeline import (
    anatomy,
    publish
)

from openpype.pipeline.template_data import get_template_data_with_names
from openpype.lib.applications import ApplicationManager
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
    supported_exts = ["exr", "dpx", "jpg", "png", "cin", "mov", "mxf", "mp4", "tiff", "tif"]
    movie_exts = ["mov", "mxf", "mp4"]

    # Configurable by Settings
    profiles = None

    def process(self, instance):

        if instance.data.get("farm", None):
            self.log.debug("Farm mode enabled, skipping")
            return

        if not self.profiles:
            self.log.debug("No profiles present for color transcode")
            return

        if "representations" not in instance.data:
            self.log.debug("No representations, skipping.")
            return

        extensions = []
        for repre in instance.data.get("representations"):
            if self._repre_is_valid(repre, instance):
                extensions.append(repre["ext"])

        if extensions:
            if not instance.data.get("gather_representation_ext"):
                instance.data["gather_representation_ext"] = extensions[-1]
        
        self.log.debug("Computed extension for profile matching: '{}'".format(
            instance.data.get("gather_representation_ext"))
        )

        profile = self._get_profile(instance)
        if not profile:
            self.log.debug("No Profile found, skipping.")
            return

        template_format_data = self._get_template_data_format()

        force_tc = profile.get("force_tc", None).strip()
        force_fps = profile.get("force_fps", None)
        if force_fps:
            instance.data["fps"] = float(force_fps)
            try:
                instance.data["anatomyData"]["fps"] = force_fps
            except:
                pass
            try:
                instance.data["assetEntity"]["data"]["fps"] = force_fps
            except:
                pass
            try:
                instance.data["slateGlobal"]["slate_common_data"]["fps"] = force_fps
            except:
                pass
        if force_tc:
            instance.data["timecode"] = force_tc

        new_representations = []
        repres = instance.data["representations"]

        self.log.debug("Initial representation list: {}".format(
            json.dumps(repres, indent=4, default=str)
        ))

        for idx, repre in enumerate(list(repres)):

            self.log.debug("repre ({}): '{}':\n{}".format(
                idx + 1, repre["name"], json.dumps(repre, indent=4, default=str)))

            if not self._repre_is_valid(repre, instance):
                continue

            for profile_name, profile_def in profile.get("outputs", {}).items():
                self.log.debug("Processing profile '{}'".format(profile_name))

                new_repre = copy.deepcopy(repre)

                if not new_repre.get("data"):
                    new_repre["data"] = dict()

                repre_name_override = profile_def["representation_name_override"].strip()
                
                new_repre["tags"] = profile_def["tags"]
                new_repre["custom_tags"] = profile_def["custom_tags"]

                if profile_name == "passthrough":
                    if repre_name_override:
                        new_repre["name"] = repre["name"] + "_" + repre_name_override
                        new_repre["outputName"] = repre_name_override
                    new_repre_files = self._translate_to_sequence(new_repre)
                    if not new_repre.get("frameStart", None):
                        new_repre["frameStart"] = new_repre_files[4][0]
                    if not new_repre.get("frameEnd", None):
                        new_repre["frameEnd"] = new_repre_files[4][-1]
                    instance.data["representations"].append(new_repre)
                    self.log.debug("profile is in passthrough mode, skipping transcode, adding tags and and pushing as representation: {}".format(
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
                    profile_def["color_conversion"]["input_colorspace"] = input_colorspace
                output_colorspace = profile_def["color_conversion"]["output_colorspace"].strip()
                color_config = new_repre["colorspaceData"]["config"]["path"].strip()

                if transcoding_type == "template":
                    if template_original_path == "":
                        raise ValueError("Error on Representation: missing template path!")
                elif transcoding_type == "chain_subsets":
                    if len(subset_chain) == 0:
                        raise ValueError("Error on Representation: missing subset chain!")
                elif transcoding_type == "color_conversion":
                    if output_colorspace == "":
                        raise ValueError("Error on Representation: missing output color profile!")
                    
                self.log.debug("Source staging dir is set as '{}'".format(repre["stagingDir"]))

                temp_staging_dir = self._get_temp_staging_dir()
                self.log.debug("Temporary staging is set as '{}'".format(temp_staging_dir))

                new_staging_dir = self._get_transcode_temp_dir(
                    temp_staging_dir,
                    profile_name)
                new_repre["stagingDir"] = self._sanitize_path(new_staging_dir)

                self.log.debug("Destination staging dir is set as '{}'".format(new_repre["stagingDir"]))

                orig_file_list = list(set(copy.deepcopy(new_repre["files"])))

                frame_start = instance.data["frameStart"]-instance.data["handleStart"]
                frame_end = instance.data["frameEnd"]+instance.data["handleEnd"]

                if not new_repre["ext"] in self.movie_exts and not isinstance(new_repre["files"], list):
                    frame_start = 1
                    frame_end = 1

                input_is_sequence = True

                if isinstance(new_repre["files"], list):
                    renamed_files = []
                    for file_name in orig_file_list:
                        head, _ = os.path.splitext(file_name)
                        frame = re.findall(r"(\d+)", head)[-1]
                        frame_index = str(head).rindex(frame) - 1
                        new_head = head[:frame_index]
                        if new_repre.get("outputName"):
                            new_head = new_head + '_{}'.format(new_repre["outputName"])
                        new_head = new_head + head[frame_index:]
                        new_file_name = "{}.{}".format(new_head, new_repre["ext"])
                        renamed_files.append(new_file_name)
                    new_repre["files"] = renamed_files
                else:
                    expected_files = []
                    source_head, source_tail = os.path.splitext(new_repre["files"])
                    for f in range(frame_start, frame_end+1):
                        new_head = source_head
                        if new_repre.get("outputName"):
                            new_head += '_{}'.format(new_repre["outputName"])
                        expected_files.append("{}.{}.{}".format(
                            new_head,
                            f,
                            new_repre["ext"]
                        ))
                    new_repre["files"] = expected_files
                    input_is_sequence = False

                repre_in = self._translate_to_sequence(repre)
                repre_out = self._translate_to_sequence(new_repre, new_repre["stagingDir"])

                nuke_script_save_path = os.path.join(
                    new_repre["stagingDir"],
                    "{}nk".format(repre_out[1])
                ).replace("\\", "/")

                try:
                    frame_start = repre_in[4][0]
                    self.log.debug("Start frame detected: ({}) updated, script data...".format(frame_start))
                except:
                    self.log.debug("Using frame start data ({}) from generated Instance...".format(frame_start))
                
                try:
                    frame_end = repre_in[4][-1]
                    self.log.debug("End frame detected: ({}) updated, script data...".format(frame_end))
                except:
                    self.log.debug("Using frame end data ({}) from generated Instance...".format(frame_end))

                processed_data = {
                    "mode": transcoding_type,
                    "input_path": repre_in[0],
                    "output_path": repre_out[0],
                    "save_path": nuke_script_save_path,
                    "thumbnail_path": os.path.join(
                        os.path.dirname(repre_out[0]),
                        repre_out[1] + "thumbnail.jpg"
                    ).replace("\\", "/"),
                    "input_is_sequence": input_is_sequence,
                    "frameStart": frame_start,
                    "frameEnd": frame_end,
                    "fps": instance.data["fps"],
                    "project": instance.data["anatomyData"]["project"],
                    "asset": instance.data["asset"],
                    "task": instance.data.get("task", ""),
                    "color_config": color_config,
                    "profile_data": profile_def
                }
                if instance.data.get("timecode", None):
                    processed_data.update({
                        "timecode": instance.data["timecode"]
                    })

                if transcoding_type == "template":
                    processed_data["profile_data"]["template_path"]["template"] = template_original_path.format(**template_format_data)
                    new_repre["data"]["colorspace"] = "data"
                    self.log.debug("will process template '{}' for rendering in context '{}'".format(
                        processed_data["profile_data"]["template_path"]["template"],
                        instance.data["asset"]
                    ))

                elif transcoding_type == "chain_subsets":
                    new_repre["data"]["colorspace"] = "data"
                    self.log.debug("Will chain subsets {} for rendering in context '{}'".format(
                        subset_chain,
                        instance.data["asset"]
                    ))

                elif transcoding_type == "color_conversion":
                    new_repre["colorspaceData"]["colorspace"] = output_colorspace
                    new_repre["data"]["colorspace"] = output_colorspace
                    self.log.debug("Will convert representation from '{}' to '{}' for rendering in context '{}'".format(
                        input_colorspace,
                        output_colorspace,
                        instance.data["asset"]
                    ))

                nuke_process = self.run_transcode_script(processed_data)

                if int(nuke_process) != 0:
                    raise RuntimeError("Error in Transcode process!! (return code != 0)")

                # cleanup temporary transcoded files
                instance.context.data["cleanupFullPaths"].append(
                    new_repre["stagingDir"])

                # ensure that ["files"] is a list even if it has just one element
                if not isinstance(new_repre["files"], list):
                    new_repre["files"] = [new_repre["files"]]

                # sort files
                new_repre["files"] = sorted(new_repre["files"])

                # If there is only 1 file outputted then convert list to
                # string, cause that'll indicate that its not a sequence.
                # else set frameStart and frameEnd property which traypublished does not
                # fill for some reason
                if len(new_repre["files"]) == 1:
                    new_repre["files"] = new_repre["files"][0]
                else:
                    new_repre["frameStart"] = repre_out[4][0]
                    new_repre["frameEnd"] = repre_out[4][-1]

                self.log.debug("Adding new representation: {}".format(
                    json.dumps(new_repre, indent=4, default=str)))

                new_representations.append(new_repre)

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

            self._mark_original_repre_for_deletion(repre, profile)

        for repre in tuple(instance.data["representations"]):
            tags = repre.get("tags", [])
            if "delete_original" in tags:
                instance.data["representations"].remove(repre)
        
        instance.data["representations"].extend(new_representations)
        self.log.debug("Final Representations list: \n{}\nFull Representations dump: {}\n".format(
            "\n".join([
                "name: '{}' | ext: '{}' | tags: '{}' | outputName: '{}' | colorspace: '{}'".format(
                    repre["name"], repre["ext"], repre.get("tags", None),
                    repre.get("outputName", None), repre.get("colorspaceData",{}).get("colorspace", None)
                ) for repre in instance.data["representations"]
            ]),
            json.dumps(instance.data["representations"], indent=4, default=str)
        ))
                    
    def _translate_to_sequence(self, repre, staging_dir=None):
        if not staging_dir:
            staging_dir = repre["stagingDir"]
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
            clique_padding = int(collection.padding)
            last_padding = len(str(frames[-1]))
            padding = max(clique_padding, last_padding)
            frame_str = "".join(["#" for i in range(padding)])
            file_name = os.path.join(
                staging_dir,
                "{}{}{}".format(
                    collection.head,
                    frame_str,
                    collection.tail
                )
            ).replace("\\", "/")
        else:
            self.log.debug("Repre is not a sequence, single name output: '{}'".format(repre["files"]))
            if isinstance(repre["files"], list):
                repre["files"] = repre["files"][-1]
            head, tail = os.path.splitext(repre["files"])
            return os.path.join(
                staging_dir, repre["files"]).replace("\\", "/"), head, "", tail, None

        return file_name, collection.head, frame_str, collection.tail, frames

    def _get_profile(self, instance, log=None):
        """Returns profile if and how repre should be transcoded."""
        host_name = instance.context.data["hostName"]
        family = instance.data["family"]
        asset = instance.data["asset"]
        task_data = instance.data["anatomyData"].get("task", {})
        task_name = task_data.get("name")
        task_type = task_data.get("type")
        subset = instance.data["subset"]
        extension = instance.data.get("gather_representation_ext", "")
        filtering_order = (
            "families",
            "assets",
            "extensions",
            "subsets",
            "task_names",
            "task_types",
            "hosts"
        )
        filtering_criteria = {
            "hosts": host_name,
            "families": family,
            "assets": asset,
            "task_names": task_name,
            "task_types": task_type,
            "subsets": subset,
            "extensions": extension
        }

        profile = filter_profiles(self.profiles, filtering_criteria, filtering_order,
                                  logger=log)
        
        self.log.debug(profile)

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

        if "review" in repre.get("tags", []) and repre["name"].find("otio") >= 0:
            self.log.debug((
                "Representation '{}' is already processed as review item and comes from hiero as an otio extracted sequence, skipping"
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
            repre["colorspace"] = cdata["colorspace"]

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
    
    def _sanitize_path(self, path):
        final_path = path.replace("\\", "/")
        if platform.system().lower() == "windows":
            BUFFER_SIZE = 512
            windows_path = final_path.replace("/", "\\")
            buffer = create_unicode_buffer(BUFFER_SIZE)
            GetLongPathName = windll.kernel32.GetLongPathNameW
            GetLongPathName(windows_path, buffer, BUFFER_SIZE)
            final_path = buffer.value
            return final_path.replace("\\", "/")
    
    def _get_temp_staging_dir(self, subdir=None):
        temp_dir = os.environ.get(
            "TMP",
            os.environ.get(
                "TEMP",
                os.environ.get(
                    "TMPDIR",
                    None
                )
            )
        )
        if temp_dir:
            temp_dir = os.path.join(
                temp_dir,
                "pyblish_tmp_" + str(uuid.uuid4()).replace("-", "")[0:15]
            ).replace("\\", "/")
            if subdir:
                temp_dir = os.path.join(
                    temp_dir,
                    subdir
                ).replace("\\", "/")
            os.makedirs(temp_dir, exist_ok=True)
            return self._sanitize_path(temp_dir)
        else:
            raise AttributeError("staging dir was not created, wrong parameters!")

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
        env["PYTHONPATH"] = os.pathsep.join(sorted(list(set(env["PYTHONPATH"].split(";")))))
        env["PYTHONPATH"] = os.path.dirname(str(nuke_app.find_executable())).replace("\\", "/") + "/plugins" + os.pathsep + env["PYTHONPATH"]
        env["PYTHONHOME"] = os.path.dirname(nukepy)
        env["PATH"] = os.path.dirname(str(nuke_app.find_executable())).replace("\\", "/") + "/plugins" + os.pathsep + env["PATH"]
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

        save_log_path = os.path.splitext(data["save_path"])[0] + ".log"
        self.log.debug("Log file for Nuke python process written to: {}".format(save_log_path))
        with open(save_log_path, "w") as log:
            log.write("")
        self.log.debug("Json exchange file written to: {}".format(json_args))
        self.log.debug("Launcing suprocess: {}".format(" ".join(cmd)))

        process_kwargs = {
            "universal_newlines": True,
            "start_new_session": True,
            "env": env,
            "bufsize": 1,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT
        }

        process = subprocess.Popen(
            cmd,
            **process_kwargs
        )
        while process.poll() is None:
            line = process.stdout.readline().strip("\n").strip()
            if line and line[0] != ".":
                self.log.debug(line)
        
        self.log.debug("TRANSCODE >>> END (return code: {})\n".format(process.returncode))

        return process.returncode

    def _mark_original_repre_for_deletion(self, repre, profile):
        """If new transcoded representation created, delete old."""
        if not repre.get("tags"):
            repre["tags"] = []

        delete_original = profile["delete_original"]

        if delete_original:
            repre["tags"].append("delete_original")
