from copy import deepcopy

import pyblish.api

from openpype.client import (
    get_asset_by_name,
    get_last_version_by_subset_name
)

class CollectGatherData(pyblish.api.InstancePlugin):
    """
    Fixes missing or incomplete data for Delivery/Gather feature
    """
    label = "Collect for Delivery/Gather data"
    order = pyblish.api.CollectorOrder + 0.489999
    families = [
        "gather"
    ]

    def get_formatted_task(self, instance):
        self.log.debug("inferring task from publisher options...")
        task_name = instance.data.get("task", None)
        if not task_name:
            self.log.debug("No task name found, defaulting to gathered data...")
            return None
        project_name = instance.data["project"]
        asset_name = instance.data["asset"]
        asset_doc = get_asset_by_name(project_name, asset_name, fields=["data"])
        task_type = asset_doc["data"]["tasks"].get(task_name, {}).get("type")
        if task_type:
            instance.context.data["anatomy"]["tasks"]
            task_short = instance.context.data["anatomy"]["tasks"].get(task_type, {}).get("short_name", "")
        else:
            task_type = ""
            task_short = ""

        return {
            "name": task_name,
            "type": task_type,
            "short": task_short
        }

    def get_last_version(self, instance):
        self.log.debug("Querying latest versions for instances.")
        project_name = instance.data["project"]
        asset_name = instance.data["asset"]
        subset_name = instance.data["subset"]
        asset_doc = get_asset_by_name(project_name, asset_name, fields=["_id"])

        last_version = get_last_version_by_subset_name(
            project_name,
            subset_name,
            asset_doc["_id"],
            asset_name,
            fields=["name"]
        )

        if last_version:
            return last_version["name"]
        else:
            return None

    def process(self, instance):
        import json
        context = instance.context

        publish_options = context.data["publish_attributes"]        
        gather_options = publish_options["CollectGatherOptions"]
        self.log.debug("Gather publish options: '{}'".format(
            json.dumps(gather_options, indent=4, default=str)))
        
        if gather_options["gather_on_farm"]:
            instance.data["families"].extend([
                "gather.farm",
                "publish_on_farm"
            ])


            instance.data["farm"] = False # Juan: if we set this to true, the integrate.py
            # will not add keys to the context such as the expected gather files
            # which are needed to create the publish process so we keep
            # this as False, and set it up to True in "submit_gather_deadline.py"


            self.log.debug("Instance 'families': '{}'".format(
                instance.data["families"]
            ))
            self.log.debug("Instance 'farm': '{}'".format(
                instance.data["farm"]
            ))

        gather_settings = context.data["project_settings"]["ftrack"]["user_handlers"]["gather_action"]
        
        min_version = gather_settings["min_gather_version"]
        missing_task_version = gather_settings["missing_task_gather_version"]

        task = instance.data.get("task")
        fallback_task = gather_settings.get("missing_task_override", [])[0]

        self.log.debug("Gather info: {}".format(json.dumps(
            {
                "gather_asset_name": instance.data["gather_asset_name"],
                "gather_assetversion_name": instance.data["gather_assetversion_name"],
                "gather_task_injection": instance.data["gather_task_injection"]
            },
            default=str,
            indent=4
        )))

        task_info = self.get_formatted_task(instance)

        if task_info["short"].lower() not in instance.data["gather_assetversion_name"].lower():
            self.log.debug("Found new task for gather, updating gather data.")
            instance.data["gather_task_injection"] = task_info
            template_data = deepcopy(instance.context.data["anatomyData"])
            template_data.update({
                "asset": instance.data["asset"],
                "variant": instance.data["variant"],
                "task": task_info
            })
            instance.data["gather_assetversion_name"] = gather_settings["ftrack_name_template"].format(**template_data)
            self.log.debug("Updated gather info: {}".format(json.dumps(
                {
                    "gather_asset_name": instance.data["gather_asset_name"],
                    "gather_assetversion_name": instance.data["gather_assetversion_name"],
                    "gather_task_injection": instance.data["gather_task_injection"]
                },
                default=str,
                indent=4
            )))

        self.log.debug("Minimum version for normal gathers is '{}'".format(min_version))
        self.log.debug("Minimum version for taskless gathers is '{}'".format(missing_task_version))

        self.log.debug("Fallback task type is '{}'".format(fallback_task))
        self.log.debug("Detected task name is '{}'".format(task_info["name"]))
        self.log.debug("Detected task type is '{}'".format(task_info["type"]))

        version_number = instance.data.get("version", None)
        self.log.debug("Current Version is set to '{}'".format(version_number))
        latest_version = self.get_last_version(instance)
        self.log.debug("Latest Version is set to '{}'".format(latest_version))

        if version_number is None:
            if not task or task_info["type"] == fallback_task:
                version_number = missing_task_version
            else:
                version_number = min_version
            if latest_version is not None:
                version_number += int(latest_version)
        
        instance.data["version"] = version_number

        self.log.debug("Computed version for gathering '{}' is '{}'".format(
            instance.data["name"], instance.data["version"]))