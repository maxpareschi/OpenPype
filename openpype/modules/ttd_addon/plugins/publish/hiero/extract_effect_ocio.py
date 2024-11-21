import os
import json

import pyblish.api
from openpype.pipeline import publish

from openpype.modules.ttd_addon.vendor.lablib import (
    EffectsFileProcessor,
    ColorProcessor
)
from openpype.modules.ttd_addon.lib.pipeline import (
    find_in_project_settings
)


class ExtractEffectOCIO(publish.Extractor):
    """
    Extracts OCIO config files from effect stack.
    Upon extraction it also updates the representation
    to also feature an effectOcio entry to be published
    """

    label = "Extract Effect OCIO - TTD"
    order = pyblish.api.ExtractorOrder

    hosts = ["hiero"]
    families = ["effect"]

    settings = find_in_project_settings("color_management_ocio")
    profiles = settings["profiles"]
    optional = True
    active = True

    def process(self, instance):

        current_ocio_config = os.environ.get("OCIO", False)

        for 

        if current_ocio_config and self.settings.get("enabled"):
            self.log.debug(f"Computing OCIO with base config: '{current_ocio_config}'")
            epr = EffectsFileProcessor(effect_file)
            if epr.color_operators:
                self.log.debug("Found color operators: '{}'".format(
                    json.dumps(epr.color_operators, indent=4, default=str)))
                
                cpr = ColorProcessor(
                    operators=epr.color_operators,
                    config_path = current_ocio_config,
                    staging_dir = staging_dir,
                    context = instance.data["asset"],
                    family = instance.data["anatomyData"]["project"]["code"],
                    log = self.log
                )
                if self.settings["active_views"]:
                    cpr.set_views(self.settings["active_views"])

                self.log.debug("Colorprocessor with data: {}".format(cpr))
            
                config_path = os.path.join(
                    staging_dir,
                    os.path.splitext(file)[0] + ".ocio"
                ).replace("\\", "/")
                
                self.log.debug("Writing ocio config at: '{}'".format(config_path))
                cpr.create_config(dest=config_path)
            
                ocio_representation = {
                    "files": os.path.basename(config_path),
                    "stagingDir": staging_dir,
                    "name": family + "Ocio",
                    "ext": "ocio"
                }
                instance.data["representations"].append(ocio_representation)
            else:
                self.log.debug("No color operators found, skipping effect stack...")