import os

from openpype.lib import PreLaunchHook
from openpype.pipeline import Anatomy
from openpype.settings import get_system_settings
from openpype.pipeline.template_data import get_template_data_with_names
from openpype.client import (
    get_asset_by_name,
    get_last_version_by_subset_name,
    get_representation_by_name
)


class GlobalOCIOHook(PreLaunchHook):

    order = 100

    def execute(self):
        self.log.debug("Hierarchy search is not yet implemented.")
        project_name = self.launch_context.data["project_name"]
        settings = Anatomy(project_name).get("ociosettings")
        template_data = self.get_template_data()
        if settings["enabled"]:
            config_path = None
            self.log.debug("Searching for suitable OCIO configs in subsets...")
            for effect in settings["subsets_group"]["subsets"]:
                self.log.debug(f"scanning for subset '{effect['subset']}'")
                config_path = self.get_ocio_config_from_effect(
                    effect["subset"],
                    data = template_data,
                    repre_name = effect["representation"]
                )
                if config_path and os.path.isfile(config_path):
                    break

            if not config_path:
                self.log.debug("No OCIO config from subsets found, falling back to explicit paths.")
                for path in settings["configs"]["paths"]:
                    config_path = str(path).strip().format(**template_data)
                    if os.path.isfile(config_path):
                        self.log.debug(f"Discovered OCIO config from explicit path: '{config_path}'")
                        break

            if config_path:
                self.launch_context.env.update({
                    "OCIO": config_path.replace("\\", "/")
                })
                os.environ["OCIO"] = config_path.replace("\\", "/")
                self.log.debug(f"Final OCIO config is: '{config_path}'")
            else:
                self.log.debug("No valid OCIO configs found, falling back to app imageio settings...")
        else:
            self.log.debug("Global OCIO search is disabled, deferring to app imageio settings...")

    def get_template_data(self):
        project_name = self.launch_context.data["project_name"]
        asset_name = self.launch_context.data["asset_name"]
        task_name = self.launch_context.data["task_name"]
        app = self.launch_context.data["app"]
        template_data = get_template_data_with_names(
            project_name,
            asset_name,
            task_name,
            app,
            get_system_settings()
        )
        template_data.update(self.launch_context.env)
        template_data.update({
            "root": Anatomy(project_name).roots
        })
        return template_data

    def get_ocio_config_from_effect(self, effect_name, data=None, repre_name="effectOcio"):
        repre_file = None
        if not data:
            data = self.get_template_data()
        try:
            asset_doc = get_asset_by_name(
                project_name=data["project"]["name"],
                asset_name=data["asset"],
                fields=["_id"]
            )
            version_doc = get_last_version_by_subset_name(
                project_name=data["project"]["name"],
                subset_name=effect_name,
                asset_id=asset_doc["_id"],
                fields=["_id"]
            )
            repre_doc = get_representation_by_name(
                project_name=data["project"]["name"],
                representation_name=repre_name,
                version_id=version_doc["_id"]
            )
            repre_file = repre_doc["files"][0]["path"]
            repre_file = str(repre_file).format(**data)
            self.log.debug(f"Discovered OCIO config from '{effect_name}' in asset context '{data['asset']}': '{repre_file}'")
        except:
            self.log.debug(f"Could not resolve '{effect_name}' in '{data['asset']}'")

        return repre_file