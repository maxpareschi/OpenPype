import os
import json
import pyblish.api
from openpype.lib import (
    get_oiio_tools_path,
    get_ffmpeg_tool_path
)


class CollectSlateGlobal(pyblish.api.InstancePlugin):
    """
    Check if slate global is active and enable slate workflow for
    selected families
    """
    label = "Collect Slate Global data"
    order = pyblish.api.CollectorOrder + 0.499
    # families = [
    #     "review",
    #     # "render",
    #     "gather"
    # ]

    _slate_settings_name = "ExtractSlateGlobal"

    def process(self, instance):

        context = instance.context
        publ_settings = context.data["project_settings"]["global"]["publish"]
        version_padding = context.data["anatomy"]["templates"]["defaults"]\
            ["version_padding"]

        if self._slate_settings_name in publ_settings:

            settings = publ_settings[self._slate_settings_name]

            if not settings["enabled"]:
                self.log.info("ExtractSlateGlobal is not active. Skipping...")
                return
            
            if (
                "render.farm" in instance.data.get("families")
                ) or (
                    "gather.farm" in instance.data.get("families")
                ):
                self.log.info("Skipping Slate Global Collect, "
                    "farm mode is on - defer to deadline...")
                return

            self.log.info("ExtractSlateGlobal is active.")

            tpl_path = settings["slate_template_path"].format(**os.environ)
            res_path = settings["slate_template_res_path"].format(**os.environ)
            _env = {
                "PATH": "{0};{1}".format(
                    os.path.dirname(get_oiio_tools_path()),
                    os.path.dirname(get_ffmpeg_tool_path())
                )
            }

            if "slateGlobal" not in instance.data:
                slate_global = instance.data["slateGlobal"] = dict()

            slate_global.update({
                "slate_template_path": tpl_path,
                "slate_template_res_path": res_path,
                "slate_profiles": settings["profiles"],
                "slate_common_data": {},
                "slate_env": _env,
                "slate_thumbnail": "",
                "slate_repre_data": {}
            })

            slate_data = slate_global["slate_common_data"]
            slate_data.update(instance.data["anatomyData"])
            slate_data["@version"] = str(
                instance.data["version"]
            ).zfill(
                version_padding
            )
            slate_data["frame_padding"] = version_padding
            slate_data["intent"] = {
                "label": "",
                "value": ""
            }
            slate_data["comment"] = ""
            slate_data["scope"] = ""

            task = instance.data["anatomyData"].get("task",{}).get("type", None)
            if not task:
                self.log.debug("No task found in instance, trying to inject from gather data...")
                if instance.data.get("gather_task_injection"):
                    slate_data["task"] = instance.data["gather_task_injection"]
                else:
                    raise ValueError("No gather data to inject from, task will remain blank...")
            else:
                anatomy_task = instance.data.get("anatomyData", {}).get("task", None)
                if not anatomy_task["name"]:
                    task_name = instance.data.get("task", task.lower())
                    slate_data["task"] = {
                        "name": task_name,
                        "type": task,
                        "short": instance.context.data["projectEntity"]["config"]\
                            ["tasks"][task]["short_name"]
                    }
                else:
                    slate_data["task"] = slate_data["task"] = {
                        "name": anatomy_task["name"],
                        "type": anatomy_task["type"],
                        "short": anatomy_task["short"]
                    }
            
            self.log.debug("Task '{}' was set for slate templating, proceeding...".format(slate_data["task"]))


            if "customData" in instance.data:
                slate_data.update(instance.data["customData"])
            
            # if "families" not in instance.data:
            #     instance.data["families"] = list()
            # 
            # if not "versionData" in instance.data:
            #     instance.data["versionData"] = dict()
            # 
            # if "families" not in instance.data["versionData"]:
            #     instance.data["versionData"]["families"] = list()
            # if not task:
            #     default_task = {
            #         "name": settings["missing_task_type"][0].lower(),
            #         "type": settings["missing_task_type"][0],
            #         "short": instance.context.data["projectEntity"]["config"]\
            #             ["tasks"][settings["missing_task_type"][0]]["short_name"]
            #     }
            #     instance.data["anatomyData"]["task"] = default_task
            #     slate_data["task"] = default_task
            # self.log.debug("Task: {} is enabled for Extract "
            #     "Slate Global workflow, tagging for slate "
            #     "extraction on review families...".format(
            #         task
            # ))
            # instance.data["slate"] = True
            # instance.data["families"].append("slate")
            # instance.data["versionData"]["families"].append("slate")

            self.log.debug(
                "SlateGlobal Data: {}".format(
                    json.dumps(
                        instance.data["slateGlobal"],
                        indent=4,
                        default=str
                    )
                )
            )