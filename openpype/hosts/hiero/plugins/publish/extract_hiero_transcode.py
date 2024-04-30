import os
import json
import clique
import sys
import subprocess
from time import sleep

import pyblish.api

import hiero.core.nuke as nuke

from openpype.lib import (
    get_oiio_tools_path,
    run_subprocess
)
from openpype.pipeline import publish


class ExtractHieroTranscode(publish.Extractor):
    """
    Extractor that exports transcoded clips from Hiero
    It takes the source clip, reformats as sequence resolution
    and sets colorspace to output \"scene linear\" exrs and
    \"color_picking\" jpgs to be generic enough to work on various
    OCIO configs using roles.
    """

    order = pyblish.api.ExtractorOrder -0.0001
    label = "Extract Transcoded Clips"
    hosts = ["hiero"]
    families = ["transcode"]

    def process(self, instance):
        oiiotool_path = get_oiio_tools_path()
        staging_dir = self.staging_dir(instance)
        sequence = instance.context.data["activeTimeline"]
        project = sequence.project()
        project_ocio = project.ocioConfigPath()
        if not project_ocio:
            project_ocio = os.environ.get("OCIO", "")
        native_colorspace = instance.data["versionData"]["colorspace"]

        for repre in instance.data["representations"]:
            if "review" in repre.get("tags", []):
                continue
            
            repre["name"] = repre["name"] + "_" + instance.data["subsetSourceName"].lower()
            repre["outputName"] = instance.data["subsetSourceName"].lower()

            transcode_staging_dir = os.path.join(
                staging_dir, "{}_transcodes".format(instance.data["name"]))
            
            if not os.path.isdir(transcode_staging_dir):
                os.mkdir(transcode_staging_dir)

            base_name = os.path.join(transcode_staging_dir, "sourceClip_transcoded")
            
            source_path = os.path.join(
                repre["stagingDir"],instance.data["originalBasename"]) + "#." + repre["ext"]
            
            transcoded_path = base_name + ".#." + repre["ext"]
            
            cmd = [
                oiiotool_path.replace("\\", "/"), "-v",
                "-i", source_path.replace("\\", "/"),
                "--colorconfig", project_ocio,
                "--colorconvert", native_colorspace, "scene_linear",
                "--fit:fillmode=width:exact=1:pad=1", "{}x{}".format(
                    sequence.format().width(),
                    sequence.format().height()
                ),
                "--ch", "R,G,B", "--label", "sc_lin",
                "-o", transcoded_path.replace("\\", "/"),
                "-i", "sc_lin",
                "--colorconvert", "scene_linear", "color_picking",
                "-o", transcoded_path.replace("\\", "/").replace(
                    repre["ext"],
                    "jpg"
                )
            ]
            
            self.log.debug("Running Transcode >>> {}".format(" ".join(cmd)))
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            with process.stdout:
                try:
                    for line in iter(process.stdout.readline, b''):
                        self.log.debug(line.decode("utf-8").strip())
            
                except subprocess.CalledProcessError as e:
                    self.log.error("{}".format(str(e)))
            
        collections, remainders = clique.assemble(os.listdir(transcode_staging_dir))
        self.log.debug("Transcoded Collections: {}".format(collections))
        self.log.debug("Transcoded Remainders: {}".format(remainders))

        for collection in collections:
            representation_data = {
                "frameStart": instance.data["sourceStartH"],
                "frameEnd": instance.data["sourceEndH"],
                "stagingDir": transcode_staging_dir,
                "name": collection.tail.replace(".", ""),
                "ext": collection.tail.replace(".", ""),
                "files": [files for files in collection]
            }
            if representation_data["ext"] == "exr":
                instance.data["representations"].insert(0, representation_data)
            else:
                instance.data["representations"].append(representation_data)
        
        self.log.debug(json.dumps(instance.data, indent=4, default=str))
