import os
import json

import pyblish.api

from openpype.pipeline import publish
from openpype.hosts.houdini.api.lib import render_rop

import hou


class ExtractHoudiniReview(publish.Extractor):

    order = pyblish.api.ExtractorOrder - 0.01
    label = "Extract Houdini review"
    families = ["review"]
    hosts = ["houdini"]

    def process(self, instance):

        ropnode = hou.node(instance.data.get("instance_node"))

        fps = instance.data.get("fps")

        start = ropnode.parm("f1").eval()
        end = ropnode.parm("f2").eval()

        camera_path = ropnode.parm("camera").eval()
        camera_name = camera_path.split('/')[-1]

        if start is None or end is None:
            frame_range = hou.playbar.frameRange()

            start = frame_range[0] if start is None else start
            end = frame_range[1] if end is None else end

        output = ropnode.evalParm("picture")
        staging_dir = os.path.normpath(os.path.dirname(output))
        instance.data["stagingDir"] = staging_dir
        
        
        file_name = os.path.basename(output)
        file_name_no_ext , ext = os.path.splitext(file_name)
        file_name_clean = file_name_no_ext.rsplit('.', 1)[0]

        file_names = [f'{file_name_clean}.{i:04d}.{instance.data["image_format"]}' for i in range(int(start), int(end) + 1)]
        # instance.data["image_format"] = ext

        instance.data["frameStartHandle"] = start
        instance.data["frameEndHandle"] = end
        
        render_rop(ropnode)

        tags = ["review"]
        if not instance.data.get("keep_images"):
            tags.append("delete")

        representation = {
            "name": instance.data["image_format"],
            "ext": instance.data["image_format"],
            "files": file_names,
            "stagingDir": staging_dir,
            "frameStart": instance.data["frameStartHandle"],
            "frameEnd": instance.data["frameEndHandle"],
            "tags": tags,
            "preview": True,
            "fps": fps,
            "camera_name": camera_name
            # TODO check if is getting camera, maybe get by calling the path of camera and the name
        }

        #if "representations" not in instance.data:
            #instance.data["representations"] = []
        instance.data["representations"].append(representation)

        thumb_repre = {
            "name": "thumbnail",
            "ext": instance.data["image_format"],
            "files": file_names[len(file_names)//2],
            "stagingDir": staging_dir,
            "tags": ["thumbnail"],
            "thumbnail": True
        }
        instance.data["representations"].append(thumb_repre)

        self.log.debug(json.dumps(instance.data, indent=4, default=str))