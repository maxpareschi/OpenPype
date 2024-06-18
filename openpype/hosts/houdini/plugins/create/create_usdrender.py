# -*- coding: utf-8 -*-
"""Creator plugin for creating USD renders."""

import hou  # noqa

from openpype.hosts.houdini.api import plugin
from openpype.hosts.houdini.api.lib import get_template_from_value
from openpype.pipeline import CreatedInstance


class CreateUSDRender(plugin.HoudiniCreator):
    """USD Render ROP in /stage"""
    identifier = "io.openpype.creators.houdini.usdrender"
    label = "USD Render (experimental)"
    family = "usdrender"
    icon = "magic"

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

        # Add Deadline Parameters to Create USD Render ROP
        extra_parms = {
            "intermediate_to_farm": False,
            "suspendPublishJob": False,
            "review": True,
            "multipartExr": True,
            "priority": 50,
            "chunk_size": 1,
            "concurrent_tasks": 1,
            "group": "",
            "department": "",
            "machine_list": "",
            "primary_pool": "",
            "secondary_pool": ""
        }

        extra_parm_templates = []
        
        divider_parm = hou.LabelParmTemplate("render_divider", "")
        divider_parm.setLabelParmType(hou.labelParmType.Heading)
        extra_parm_templates.append(divider_parm)
        title_parm = hou.LabelParmTemplate("render_title", "RENDER OPTIONS")
        extra_parm_templates.append(title_parm)
        
        for key, value in extra_parms.items():
            extra_parm_templates.append(
                get_template_from_value(key, value)
            )
        
        main_group = instance_node.parmTemplateGroup()
        extra_group = main_group.findFolder("Extra")

        for t in extra_parm_templates:
            main_group.appendToFolder(extra_group, t)
            extra_group = main_group.findFolder("Extra")

        instance_node.setParmTemplateGroup(main_group)

        # Lock some Avalon attributes
        to_lock = ["family", "id"]
        self.lock_parameters(instance_node, to_lock)
