import os

from copy import deepcopy

import openpype.hosts.hiero.api as phiero

from openpype.hosts.hiero.api.lib import imprint
from openpype.settings.lib import get_anatomy_settings
from openpype.client.entities import get_asset_by_name


class CreateRender(phiero.Creator):

    label = "Create Publishable Render"
    family = "clip"
    icon = "film"
    defaults = ["Main"]

    gui_tracks = [track.name()
                  for track in phiero.get_current_sequence().videoTracks()]
    gui_name = "Render attributes creator"
    gui_info = "Define attributes for 'Render' family publishing"
    gui_inputs = {
        "farmHierarchy": {
            "type": "section",
            "label": "<span style=\"color: #FFAA22\"><b>FARM SETTINGS</b></span>",
            "target": "ui",
            "order": 0,
            "value": {
                "ingestOnFarm": {
                    "value": False,
                    "type": "QCheckBox",
                    "label": "<b>Launch Ingest on farm</b>",
                    "target": "tag",
                    "toolTip": "Launch Ingest on farm",  # noqa
                    "order": 0
                },
                "ingestPool": {
                    "value": "ingest",
                    "type": "QLineEdit",
                    "label": "Deadline Pool",
                    "target": "tag",
                    "toolTip": "Pool to launch the publishing on",  # noqa
                    "order": 1
                },
                "ingestGroup": {
                    "value": "ingest",
                    "type": "QLineEdit",
                    "label": "Deadline Group",
                    "target": "tag",
                    "toolTip": "Group to launch the publishing on",  # noqa
                    "order": 2
                },
                "ingestPriority": {
                    "value": 50,
                    "type": "QSpinBox",
                    "label": "Priority",
                    "target": "tag",
                    "toolTip": "Priority to launch the publishing on",  # noqa
                    "order": 3
                }
            }
        },
        "hierarchyData": {
            "type": "section",
            "label": "<span style=\"color: #22AAFF\"><b>Hierarchy Names</b></span>",
            "target": "ui",
            "order": 1,
            "value": {
                "baseFolder": {
                    "value": "shots",
                    "type": "QLineEdit",
                    "label": "Folder",
                    "target": "tag",
                    "toolTip": "Name of folder used for root of generated shots.\nUsable tokens:\n\t{_clip_}: name of used clip\n\t{_track_}: name of parent track layer\n\t{_sequence_}: name of parent sequence (timeline)",  # noqa
                    "order": 0
                },
                "baseEpisode": {
                    "value": "",
                    "type": "QLineEdit",
                    "label": "Episode",
                    "target": "tag",
                    "toolTip": "Name of episode.\nUsable tokens:\n\t{_clip_}: name of used clip\n\t{_track_}: name of parent track layer\n\t{_sequence_}: name of parent sequence (timeline)",  # noqa
                    "order": 1
                },
                "baseSequence": {
                    "value": "",
                    "type": "QLineEdit",
                    "label": "Sequence",
                    "target": "tag",
                    "toolTip": "Name of sequence of shots.\nUsable tokens:\n\t{_clip_}: name of used clip\n\t{_track_}: name of parent track layer\n\t{_sequence_}: name of parent sequence (timeline)",  # noqa
                    "order": 2
                },
            }
        },
        "frameRangeAttr": {
            "type": "section",
            "label": "<span style=\"color: #66FF99\"><b>Frame Attributes</b></span>",
            "target": "ui",
            "order": 2,
            "value": {
                "generateReview": {
                    "value": True,
                    "type": "QCheckBox",
                    "label": "<b>Generate Review</b>",
                    "target": "tag",
                    "toolTip": "Generate Review",  # noqa
                    "order": 0
                },
                "workfileFrameStart": {
                    "value": 1009,
                    "type": "QSpinBox",
                    "label": "Start Frame",
                    "target": "tag",
                    "toolTip": "Set starting frame number",  # noqa
                    "order": 1
                },
                "handleStart": {
                    "value": 8,
                    "type": "QSpinBox",
                    "label": "Handle Start",
                    "target": "tag",
                    "toolTip": "Handle at start of clip",  # noqa
                    "order": 2
                },
                "handleEnd": {
                    "value": 8,
                    "type": "QSpinBox",
                    "label": "Handle End",
                    "target": "tag",
                    "toolTip": "Handle at end of clip",  # noqa
                    "order": 3
                }
            }
        }
    }


    def process(self):
        gui_inputs = deepcopy(self.gui_inputs)

        # open widget for plugins inputs
        widget = self.widget(self.gui_name, self.gui_info, gui_inputs)
        widget.exec_()

        if len(self.selected) < 1:
            return

        if not widget.result:
            print("Operation aborted")
            return

        for track_item in self.selected:
            item_name = track_item.currentVersion().name()

            name_splits = item_name.split("_")
            incoming_version = int(str(name_splits[-1]).replace("v", ""))
            incoming_task = str(name_splits[-2])
            incoming_asset = "_".join(name_splits[:-2])

            family = "render"
            variant = str(track_item.parent().name())
            asset = incoming_asset
            version = incoming_version
            task = incoming_task.replace(variant, "").replace(family, "")

            anatomy_tasks = get_anatomy_settings(os.environ["AVALON_PROJECT"]).get("tasks", {})
            asset_doc = get_asset_by_name(os.environ["AVALON_PROJECT"], asset)
            asset_tasks = asset_doc["data"].get("tasks", {}) #type: ignore
            
            if not asset_tasks:
                raise ValueError("Found asset has no tasks available!")

            for k, v in deepcopy(asset_tasks).items():
                asset_tasks[k].update({"short_name": anatomy_tasks[v["type"]]["short_name"]})

            task_found = False
            
            for k, v in asset_tasks.items():
                if (
                    k.lower() == task.lower() or
                    v["short_name"].lower() == task.lower() or
                    v["type"].lower == task.lower()
                ):
                    task = k
                    task_found = True
                    break
            
            if not task_found:
                raise ValueError("Could not find suitable task, please check input filename!")
            
            subset = f"render{task.capitalize()}{variant.capitalize()}"

            folder = widget.result.get("baseFolder", {}).get("value", "")
            episode = widget.result.get("baseEpisode", {}).get("value", "")
            sequence = widget.result.get("baseSequence", {}).get("value", "")

            hierarchy = []
            if folder:
                hierarchy.append({"entity_type": "folder", "entity_name": folder})
            if episode:
                hierarchy.append({"entity_type": "episode", "entity_name": episode})
            if sequence:
                hierarchy.append({"entity_type": "sequence", "entity_name": sequence})

            families = ["clip"]
            if widget.result.get("generateReview", {}).get("value", False):
                families.append("review")

            item_data = {
                "id": "pyblish.avalon.instance",
                "active": True,
                "publish": True,
                "family": family,
                "asset": asset,
                "subset": subset,
                "variant": variant,
                "task": task,
                "version": version,
                "ingestOnFarm": widget.result.get("ingestOnFarm", {}).get("value", False),
                "ingestPool": widget.result.get("ingestPool", {}).get("value", ""),
                "ingestGroup": widget.result.get("ingestGroup", {}).get("value", ""),
                "ingestPriority": widget.result.get("ingestPriority", {}).get("value", 50),
                "hierarchyData": {
                    "folder": folder,
                    "episode": episode,
                    "sequence": sequence,
                    "track": variant,
                    "shot": asset
                },
                "hierarchy": "/".join([entity["entity_name"] for entity in hierarchy]),
                "audio": False,
                "sourceResolution": True,
                "workfileFrameStart": widget.result.get("workfileFrameStart", {}).get("value", 1009),
                "handleStart": widget.result.get("handleStart", {}).get("value", 8),
                "handleEnd": widget.result.get("handleEnd", {}).get("value", 8),
                "parents": hierarchy,
                "families": families,
                "heroTrack": True,
                "reviewTrack": None,
            }

            imprint(track_item, item_data)