import json
import os

from openpype.pipeline.create import (
    CreateContext
)

from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_last_version_by_subset_id,
    get_project
)


class DeliveryProcess:

    def __init__(self, data, host):
        self.data = data
        self.host = host

    def start_context(self):
        create_context = CreateContext(self.host,
                                       headless=True,
                                       discover_publish_plugins=True,
                                       reset=True)

        for instance in list(create_context.instances):
            create_plugin = create_context.creators.get(
                instance.creator_identifier
            )
            create_plugin.remove_instances([instance])

    def get_files_from_repre(self, repre, version):
        files = []
        for file in repre["files"]:
            files.append(file["path"].format(**repre["context"]))
        repre_start = int(re.findall(r'\d+$', os.path.splitext(files[0])[0])[0])
        version_start = int(version["data"]["frameStart"]) - int(version["data"]["handleStart"])
        self.log.debug("FRAMES DETECTED: repre_start:{} <-> version_start:{}".format(repre_start, version_start))
        if repre_start < version_start:
            files.pop(0)
        return files

    def process_data(self):
        asset_doc = get_asset_by_name(
            self.data["project"],
            self.data["asset"]
        )
        subset_doc = get_subset_by_name(
            self.data["project"],
            self.data["subset"],
            asset_doc["_id"]
        )
        version_doc = get_last_version_by_subset_id(
            self.data["project"],
            subset_doc["_id"]
        )
        repre_doc = get_representation_by_name(
            self.data["project"],
            self.data["delivery_representation_name"],
            version_doc["_id"]
        )

        repre_files = self.get_files_from_repre(repre_doc, version_doc)

        computed_asset = repre_doc["context"]["asset"]
        computed_task = repre_doc["context"]["task"]["name"]
        computed_variant = "{}_{}".format(repre_doc["context"]["subset"].replace(repre_doc["context"]["family"], ""), computed_asset)
        computed_subset = "delivery{}".format(computed_variant)
        computed_name = computed_subset