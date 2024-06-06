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
            instance.data["families"].append("gather.farm")
            instance.data["publish_on_farm"] = True
            self.log.debug("Instance 'families': '{}'".format(
                instance.data["families"]
            ))
            self.log.debug("Instance 'publish_on_farm': '{}'".format(
                instance.data["publish_on_farm"]
            ))

        gather_settings = context.data["project_settings"]["ftrack"]["user_handlers"]["gather_action"]
        min_version = gather_settings["min_gather_version"]
        missing_task_version = gather_settings["missing_task_gather_version"]
        task = instance.data.get("task")

        version_number = instance.data.get("version", None)
        self.log.debug("Current Version is set to '{}'".format(version_number))
        latest_version = self.get_last_version(instance)
        self.log.debug("Latest Version is set to '{}'".format(latest_version))

        if version_number is None:
            if task is None or task == "":
                version_number = missing_task_version
            else:
                version_number = min_version
            if latest_version is not None:
                version_number += int(latest_version)
        
        instance.data["version"] = version_number

        self.log.debug("Computed version for gathering '{}' is '{}'".format(
            instance.data["name"], instance.data["version"]))