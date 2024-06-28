import json

import hou
import pyblish.api


class CollectHoudiniReviewData(pyblish.api.InstancePlugin):
    """Collect Review Data."""

    label = "Collect Houdini Review Data"
    # This specific order value is used so that
    # this plugin runs after CollectRopFrameRange
    order = pyblish.api.CollectorOrder + 0.1
    hosts = ["houdini"]
    families = ["review"]

    def process(self, instance):

        # This fixes the burnin having the incorrect start/end timestamps
        # because without this it would take it from the context instead
        # which isn't the actual frame range that this instance renders.
        instance.data["handleStart"] = 0
        instance.data["handleEnd"] = 0
        instance.data["fps"] = instance.context.data["fps"]

        # Enable ftrack functionality
        instance.data.setdefault("families", []).append('ftrack')

        # Get the camera from the rop node to collect the focal length
        ropnode_path = instance.data["instance_node"]
        ropnode = hou.node(ropnode_path)

        camera_path = ropnode.parm("camera").eval()
        camera_node = hou.node(camera_path)

        start = ropnode.parm("f1").eval()
        end = ropnode.parm("f2").eval()

        instance.data["frameStart"] = start
        instance.data["frameEnd"] = end

        if not camera_node:
            msg = "No valid camera node found on review node: {}".format(camera_path)
            self.log.warning(msg)
            raise ValueError(msg)

        # Collect focal length.
        focal_length_parm = camera_node.parm("focal")
        if not focal_length_parm:
            msg = "No 'focal' (focal length) parameter found on camera: {}".format(camera_path)
            self.log.warning(msg)
            raise ValueError(msg)

        if focal_length_parm.isTimeDependent():
            start = instance.data["frameStartHandle"]
            end = instance.data["frameEndHandle"] + 1
            focal_length = [
                focal_length_parm.evalAsFloatAtFrame(t)
                for t in range(int(start), int(end))
            ]
        else:
            focal_length = focal_length_parm.evalAsFloat()

        # Store focal length in `burninDataMembers`
        burnin_members = instance.data.setdefault("burninDataMembers", {})
        burnin_members["focalLength"] = focal_length

        self.log.debug(json.dumps(instance.data, indent=4, default=str))
        import os
        self.log.debug(json.dumps({k: v for k, v in os.environ.items()}, indent=4, default=str))

