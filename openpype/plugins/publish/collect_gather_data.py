from copy import deepcopy
import json
import os

import pyblish.api

from openpype.client import (
    get_asset_by_name,
    get_last_version_by_subset_name,
    get_representations,
    get_subsets
)


def yield_elegible_gather_subset_names(prj: str, gather_asset_name: str, target_task: str):
    """Yield eligible subset names based on project, asset names and task.

    This function identifies subsets starting with 'gather' that have
    representations matching the target task. It yields each valid subset name
    as it finds them.

    Parameters
    ----------
    prj : str
        The name of the project.
    gather_asset_name : str
        The name of the asset to gather data from.
    target_task : str
        The name of the task to filter representations by.

    Yields
    ------
    subset_name : str
        Names of eligible subsets that meet the criteria.

    Notes
    -----
    - The function stops checking further representations once a match is found,
      improving efficiency.
    """
    asset = get_asset_by_name(prj, gather_asset_name)

    gather_subsets = [
        s
        for s in get_subsets(prj, asset_ids=[asset["_id"]])
        if s["name"].startswith("gather")
    ]

    for subset in gather_subsets:
        latest_version = get_last_version_by_subset_name(
            prj, subset["name"], asset_name=gather_asset_name
        )
        repres = get_representations(
            prj, version_ids=[latest_version["_id"]], fields=["context"]
        )
        for repre in repres:
            if repre["context"]["task"]["name"] == target_task:
                print(f"Found elegible subset name: {subset['name']}")
                yield subset["name"]
                break


class CollectGatherData(pyblish.api.InstancePlugin):
    """
    Fixes missing or incomplete data for Delivery/Gather feature
    """
    label = "Collect for Delivery/Gather data"
    order = pyblish.api.CollectorOrder + 0.489999
    families = ["gather"]

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

    def get_last_version(self, instance: pyblish.api.Instance):
        """Get the latest version number from eligible gather subsets.

        This function retrieves the highest version number by querying
        eligible gather subsets based on project, asset, and task. It
        iterates through each subset to find the latest candidate version.

        Parameters
        ----------
        instance : pyblish.api.Instance
            An instance containing metadata about the current operation,
            including 'project', 'asset', and 'task'.

        Returns
        -------
        int or None
            The highest version number found, or None if no valid versions
            are available.
        """
        self.log.debug("Querying latest versions for instances.")
        prj = instance.data["project"]
        asset_name = instance.data["asset"]
        self.log.info(instance.data)
        task = instance.data["task"]
        # subset_name = instance.data["subset"]
        asset_doc = get_asset_by_name(prj, asset_name, fields=["_id"])

        last_version = 0
        for subset in yield_elegible_gather_subset_names(prj, asset_name, task):
            candidate_version = get_last_version_by_subset_name(
                prj,
                subset,
                asset_doc["_id"],
                asset_name,
                fields=["name"]
            )
            if not candidate_version:
                continue
            last_version = max(last_version, int(candidate_version["name"]))

        if last_version:
            return last_version
        else:
            return None

    def process(self, instance):
        context = instance.context
        instance.data["farm"] = False

        publish_options = context.data.get("publish_attributes", None)
        if publish_options:
            gather_options = publish_options["CollectGatherOptions"]
            self.log.debug("Gather publish options: '{}'".format(
                json.dumps(gather_options, indent=4, default=str)))
            
            if gather_options["gather_on_farm"]:
                instance.data["gather_review"] = True
                instance.data["farm"] = True
                instance.data["families"].extend([
                    "gather.farm",
                ])
                instance.data["priority"] = gather_options["gather_deadline_priority"]
                instance.data["primaryPool"] = gather_options["gather_deadline_pool"]
                instance.data["gather_deadline_group"] = gather_options["gather_deadline_group"]
            else:
                if "gather.farm" in instance.data.get("families", []):
                    instance.data["families"].remove("gather.farm")
            
            self.log.debug("Instance 'families': '{}'".format(
                instance.data["families"]
            ))
            self.log.debug("Instance 'farm': '{}'".format(
                instance.data["farm"]
            ))

        if instance.data.get("source", None):
            instance.data["gather_json_location"] = os.path.normpath(
                os.path.dirname(
                    instance.data["source"]
                )
            ).replace("\\", "/")
        elif instance.data.get("gather_representation_files"):
            instance.data["gather_json_location"] =  os.path.normpath(
                os.path.dirname(
                    instance.data.get("gather_representation_files")[0]
                )
            ).replace("\\", "/")
        else:
            instance.data["gather_json_location"] = ""
        
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
        instance.context.data["version"] = version_number

        self.log.debug("Computed version for gathering '{}' is '{}'".format(
            instance.data["name"], instance.data["version"]))
        
        if os.environ.get("OPENPYPE_FARM_JSON_PATH", None):
            instance.context.data["cleanupFullPaths"].append(
                os.environ["OPENPYPE_FARM_JSON_PATH"]
            )
            self.log.debug("This publish comes from a gather on farm job, tagging '{}' for deletion".format(
                os.environ["OPENPYPE_FARM_JSON_PATH"]
            ))