# -*- coding: utf-8 -*-
"""Creator plugin for creating USD renders."""

import hou  # type: ignore
import re
from os import environ

from openpype.settings import get_current_project_settings
from openpype.hosts.houdini.api import plugin
from openpype.hosts.houdini.api.lib import get_template_from_value
from openpype.pipeline import CreatedInstance
from openpype.lib.attribute_definitions import (
    BoolDef,
    NumberDef,
    TextDef,
    UISeparatorDef
)


class CreateUSDRender(plugin.HoudiniCreator):
    """USD Render ROP in /stage"""
    identifier = "io.openpype.creators.houdini.usdrender"
    label = "USD Render (experimental)"
    family = "usdrender"
    icon = "magic"

    def get_instance_attr_defs(self):
        settings = get_current_project_settings()
        pools = settings["deadline"]["publish"]["CollectDeadlinePools"]
        hou_cfg = settings["deadline"]["publish"]["HoudiniSubmitDeadline"]

        primpool = hou_cfg["jobInfo"].get("primary_pool", pools["primary_pool"])
        sec_pool = hou_cfg["jobInfo"].get("secondary_pool", pools["secondary_pool"])

        instance_parms = {
            "usd_intermediate_on_farm": True,
            "flush_data_after_each_frame": False,
            "separator": "separator",
            "suspendPublishJob": False,
            "review": True,
            "multipartExr": True,
            "priority": hou_cfg["priority"],
            "chunk_size": 1,
            "concurrent_tasks": 1,
            "group": hou_cfg["group"],
            "department": environ.get("AVALON_TASK", ""),
            # "machine_list": "",
            "primary_pool": primpool,
            "secondary_pool": sec_pool
        }
        attrs = []
        for k, v in instance_parms.items():
            parts = re.split(r'_|(?=[A-Z])', k)
            label = " ".join([part.capitalize() for part in parts if part])
            if v == "separator":
                attrs.append(UISeparatorDef())
            elif isinstance(v, bool):
                attrs.append(BoolDef(k, default=v, label=label))
            elif isinstance(v, int):
                attrs.append(NumberDef(k, default=v, label=label))
            elif isinstance(v, float):
                attrs.append(NumberDef(k, default=v, label=label, decimals=3))
            elif isinstance(v, str):
                attrs.append(TextDef(k, default=v, label=label))
        return attrs

    def create(self, subset_name, instance_data, pre_create_data):
        instance_data["parent"] = hou.node("/stage").path()

        # Remove the active, we are checking the bypass flag of the nodes
        instance_data.pop("active", None)
        instance_data.update({"node_type": "usdrender"})

        instance = super(CreateUSDRender, self).create(
            subset_name,
            instance_data,
            pre_create_data)  # type: CreatedInstance

        instance_node = hou.node(instance.get("instance_node"))

        parms = {
            # Render frame range
            "trange": 1
        }
        if self.selected_nodes:
            parms["loppath"] = self.selected_nodes[0].path()
        instance_node.setParms(parms)

        main_group = instance_node.parmTemplateGroup()
        instance_node.setParmTemplateGroup(main_group)

        # Lock some Avalon attributes
        to_lock = ["family", "id"]
        self.lock_parameters(instance_node, to_lock)
