# from openpype import plugins
import os
import json
import pyblish.api

from openpype.settings import get_current_project_settings
from openpype.pipeline import publish
from lablib import (
    EffectsFileProcessor,
    ColorProcessor,
)

class ExtractClipEffects(publish.Extractor):
    """Extract clip effects instances."""

    order = pyblish.api.ExtractorOrder
    label = "Export Clip Effects"
    families = ["effect"]

    def process(self, instance):

        settings = get_current_project_settings()[
            "hiero"]["publish"]["ExtractOCIOEffects"]
        item = instance.data["item"]
        effects = instance.data.get("effects")

        # skip any without effects
        if not effects:
            return

        subset = instance.data.get("subset")
        family = instance.data["family"]

        self.log.debug("creating staging dir")
        staging_dir = self.staging_dir(instance)

        transfers = list()
        if "transfers" not in instance.data:
            instance.data["transfers"] = list()

        ext = "json"
        file = subset + "." + ext

        # when instance is created during collection part
        resources_dir = instance.data["resourcesDir"]

        # change paths in effects to files
        for k, effect in effects.items():
            if "assignTo" in k:
                continue
            trn = self.copy_linked_files(effect, resources_dir)
            if trn:
                transfers.append((trn[0], trn[1]))

        instance.data["transfers"].extend(transfers)
        self.log.debug("_ transfers: `{}`".format(
            instance.data["transfers"]))

        # create representations
        instance.data["representations"] = list()

        transfer_data = [
            "handleStart", "handleEnd",
            "sourceStart", "sourceStartH", "sourceEnd", "sourceEndH",
            "frameStart", "frameEnd",
            "clipIn", "clipOut", "clipInH", "clipOutH",
            "asset", "version"
        ]

        # pass data to version
        version_data = dict()
        version_data.update({k: instance.data[k] for k in transfer_data})

        # add to data of representation
        version_data.update({
            "colorspace": item.sourceMediaColourTransform(),
            "colorspaceScript": instance.context.data["colorspace"],
            "families": [family, "plate"],
            "subset": subset,
            "fps": instance.context.data["fps"]
        })
        instance.data["versionData"] = version_data

        representation = {
            "files": file,
            "stagingDir": staging_dir,
            "name": family + ext.title(),
            "ext": ext
        }
        instance.data["representations"].append(representation)
        
        effect_file = os.path.join(staging_dir, file).replace("\\", "/")

        with open(effect_file, "w") as outfile:
            outfile.write(json.dumps(effects, indent=4, sort_keys=True))

        self.log.debug("Processing effect stack file: '{}'".format(
            effect_file
        ))

        current_ocio_config = instance.data["versionData"]["colorspaceScript"]["ocioConfigPath"]
        if os.environ.get("OCIO", False):
            current_ocio_config = os.environ["OCIO"]

        if current_ocio_config and settings["enabled"]:
            self.log.debug("Computing OCIO with base config: '{}'".format(
                current_ocio_config
            ))
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
                if settings["active_views"]:
                    cpr.set_views(settings["active_views"])

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

        self.log.debug("_ representations: `{}`".format(
            instance.data["representations"]))

        self.log.debug("_ version_data: `{}`".format(
            instance.data["versionData"]))

    def copy_linked_files(self, effect, dst_dir):
        for k, v in effect["node"].items():
            if k in "file" and v != '':
                base_name = os.path.basename(v)
                dst = os.path.join(dst_dir, base_name).replace("\\", "/")

                # add it to the json
                effect["node"][k] = dst
                return (v, dst)
