# -*- coding: utf-8 -*-
from maya import cmds  # noqa
import pyblish.api


class CollectFbxExport(pyblish.api.InstancePlugin):
    """Collect for FBX export."""

    order = pyblish.api.CollectorOrder + 0.2
    label = "Collect for FBX export"
    families = ["exportfbx"]

    def process(self, instance):
        if not instance.data.get("families"):
            instance.data["families"] = []

        if "fbx" not in instance.data["families"]:
            instance.data["families"].append("fbx")
