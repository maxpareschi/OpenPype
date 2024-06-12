import os
import platform

from openpype.lib import PreLaunchHook
from openpype.pipeline import Anatomy
from openpype.settings import (
    get_system_settings,
    get_project_settings
)
from openpype.pipeline.template_data import get_template_data_with_names
from openpype.client import (
    get_asset_by_name,
    get_last_version_by_subset_name,
    get_representation_by_name
)


class GlobalOCIOHook(PreLaunchHook):

    order = 19

    def execute(self):
        self.log.debug("Hierarchy search is not yet implemented.")
        project_name = self.launch_context.data.get("project_name", None)
        if not project_name:
            return
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
                if config_path:
                    break

            if not config_path:
                self.log.debug("No valid OCIO config from subsets found, falling back to explicit paths...")
                for path in settings["configs"]["paths"]:
                    config_path = str(path).strip().format(**template_data)
                    if os.path.isfile(config_path):
                        self.log.debug(f"Discovered OCIO config from explicit path: '{config_path}'")
                        break

            if not config_path:
                self.log.debug("No valid OCIO config from global paths found, falling back to app imageio settings...")
                config_path = self.get_ocio_config_from_app_settings(data=template_data)

            if config_path:
                self.launch_context.env.update({
                    "OCIO": config_path.replace("\\", "/")
                })
                os.environ["OCIO"] = config_path.replace("\\", "/")
                self.log.debug(f"Final OCIO config is: '{config_path}'")

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
    
    def get_ocio_config_from_app_settings(self, data=None):
        if not data:
            data = self.get_template_data()
        app_group = self.launch_context.data["app"].group.host_name
        imageio_keys = [
            "colorManagementPreference_v2",
            "workfile"
        ]
        colorspace_keys = [
            "customOCIOConfigPath",
            "configFilePath"
        ]
        imageio_settings = get_project_settings(
            self.launch_context.data["project_name"]).get(app_group, {}).get("imageio", None)
        
        ocio_config = None
        
        if imageio_settings:
            ocio_section = None
            for io_key in imageio_keys:
                ocio_section = imageio_settings.get(io_key, None)
                if ocio_section:
                    break
            
            if ocio_section:
                ocio_paths = None
                for csp_key in colorspace_keys:
                    ocio_paths = ocio_section.get(csp_key, None)
                    if ocio_paths:
                        break
            if ocio_paths:
                for path in ocio_paths[platform.system().lower()]:
                    check_config = str(path).format(**data)
                    if os.path.isfile(check_config):
                        self.log.debug(f"Found config in '{app_group}' imageio settings: '{check_config}'")
                        ocio_config = check_config

            if not ocio_config:
                self.log.debug(f"No valid config in '{app_group}' imageio settings. Check your paths.")
            
        else:
            self.log.debug(f"No imageio settings found for app '{app_group}'")

        return ocio_config    
        
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
            found_repre = str(repre_doc["files"][0]["path"]).format(**data)
            if found_repre and os.path.isfile(found_repre):
                repre_file = found_repre
                self.log.debug(f"Discovered OCIO config from '{effect_name}' in asset context '{data['asset']}': '{repre_file}'")
        except:
            self.log.debug(f"Could not resolve '{effect_name}' in '{data['asset']}'")

        return repre_file