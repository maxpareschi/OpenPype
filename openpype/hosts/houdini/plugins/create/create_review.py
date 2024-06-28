# -*- coding: utf-8 -*-
"""Creator plugin for creating USD renders."""
import hou  # noqa

from openpype.hosts.houdini.api import plugin
from openpype.hosts.houdini.api.lib import get_template_from_value
from openpype.pipeline import CreatedInstance
from openpype.lib.attribute_definitions import (
    BoolDef,
    NumberDef,
    TextDef,
    UISeparatorDef
)


class CreateHoudiniReview(plugin.HoudiniCreator):

    identifier = "io.openpype.creators.houdini.review"
    label = "Review"
    family = "review"
    icon = "video-camera"

    def get_pre_create_attr_defs(self):
        attrs = super(CreateHoudiniReview, self).get_pre_create_attr_defs()
        attrs.extend([
            UISeparatorDef(),
            TextDef(
                "image_format",
                default="png",
                label="Image Format"
            ),
            BoolDef(
                "keep_images",
                default=True,
                label="Keep Images"
            ),
            BoolDef(
                "override_resolution",
                default=True,
                label="Override Resolution"
            ),
            NumberDef(
                "resx",
                default=1920,
                label="Width"
            ),
            NumberDef(
                "resy",
                default=1080,
                label="Height"
            ),
            NumberDef(
                "aspect",
                default=1.0,
                label="Aspect Ratio"
            )
        ])
        return attrs

    def create (self, subset_name, instance_data, pre_create_data):

        instance_data.pop("active", None)
        if not instance.data.get("families", None):
            instance.data["families"] = []
        instance.data["families"].append("review")
        instance_data.update({"node_type": "opengl"})
        instance_data["image_format"] = pre_create_data.get("image_format")
        instance_data["keep_images"] = pre_create_data.get("keep_images")

        instance = super(CreateHoudiniReview, self).create(
            subset_name,
            instance_data,
            pre_create_data)
        
        instance_node = hou.node(instance.get("instance_node"))

        frame_range = hou.playbar.frameRange()

        filepath = "{root}/{subset}/{subset}.$F4.{ext}".format(
            root = hou.text.expandString("$HIP/pyblish"),
            subset="`chs(\"subset\")`",  # keep dynamic link to subset
            ext=pre_create_data.get("image_format") or "png"
        )

        parms = {
            "picture": filepath,
            "trange": 1,
            "f1": frame_range[0],
            "f2": frame_range[1],
        }

        override_resolution = pre_create_data.get("override_resolution")
        if override_resolution:
            parms.update({
                "tres": override_resolution,
                "res1": pre_create_data.get("resx"),
                "res2": pre_create_data.get("resy"),
                "aspect": pre_create_data.get("aspect"),
            })

        # determine if the context is obj or stage

        context_node = instance_node.parent()
        context_name = context_node.name()

        camera = None
        force_objects = []

        if self.selected_nodes:
            # The first camera found in selection we will use as camera
            # if no node is selected, it will choose a camera node
            # if there are more than 1 camera node, it will choose one of them

            for node in self.selected_nodes:
                path = node.path()
                node_type = node.type().name()
                
                if "/stage" in path:
                    hou.ui.displayMessage("No Camera detected, please input camera path into review node before publishing...")
                
                if node_type == "cam":
                    if not camera:
                        camera = path
                else:
                    force_objects.append(path)

            if not camera:
                self.log.warning("No camera found in selection.")
                all_nodes = hou.node("/obj").children()

                for node in all_nodes:
                    if node.type() == "cam":
                        camera = node.path()
                        break

            if not camera:
                print("No camera found in selection or in scene.")
            else:
                print(f"using camera {camera}")

            parms.update({
                "scenepath": "/obj",
                #"forceobjects": " ".join(force_objects),
                "camera": camera or "",
                "vobjects": "*"  # clear candidate objects from '*' value,
            })

        instance_node.setParms(parms)

        to_lock = ["id", "family"]

        self.lock_parameters(instance_node, to_lock)


