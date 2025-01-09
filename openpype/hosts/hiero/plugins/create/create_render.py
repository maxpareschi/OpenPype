from multiprocessing import Value
import os
import json
import inspect

from copy import deepcopy

from qtpy import QtWidgets

import openpype.hosts.hiero.api as phiero

from openpype.hosts.hiero.api.lib import imprint
from openpype.settings.lib import get_anatomy_settings
from openpype.client.entities import get_asset_by_name
from openpype.client import get_last_version_by_subset_name


def show_message(window_title, title, message, icon=None):
    if isinstance(message, list):
        message = ''.join([f"<li style='margin-bottom: 10px;'>{item}</li>" for item in message])
        message = f"<ul style='color: #FFFFFF;'>{message}</ul>"
    
    msg_box = QtWidgets.QMessageBox()
    if icon:
        msg_box.setIcon(icon)
    msg_box.setWindowTitle(window_title)
    msg_box.setText(f"<b style='color: #FFCC99;'>{title}</b>")
    msg_box.setInformativeText(message)
    msg_box.exec()


class CreateRender(phiero.Creator):

    label = "Create Publishable Render"
    family = "clip"
    icon = "film"
    defaults = ["Main"]

    gui_name = "Render attributes creator"
    gui_info = "Define attributes for 'Render' family publishing"
    gui_inputs = {
        "farmData": {
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
            "label": "<span style=\"color: #22AAFF\"><b>Hierarchy Settings</b></span>",
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
                }
            }
        },
        "settingsData": {
            "type": "section",
            "label": "<span style=\"color: #99FF66\"><b>Publish Settings</b></span>",
            "target": "ui",
            "order": 2,
            "value": {
                "ingestFrameStart": {
                    "value": 1001,
                    "type": "QSpinBox",
                    "label": "Start Frame",
                    "target": "tag",
                    "toolTip": "Set starting frame number",  # noqa
                    "order": 0
                },
                "generateReview": {
                    "value": True,
                    "type": "QCheckBox",
                    "label": "<b>Generate Review</b>",
                    "target": "tag",
                    "toolTip": "Generate Review",  # noqa
                    "order": 1
                },
                "versionZero": {
                    "value": False,
                    "type": "QCheckBox",
                    "label": "<b>Publish as v0</b>",
                    "target": "tag",
                    "toolTip": "Check if render is a v0",  # noqa
                    "order": 2
                },
            }
        }
    }


    def process(self):
        
        gui_inputs = deepcopy(self.gui_inputs)

        # open widget for plugins inputs
        widget = self.widget(self.gui_name, self.gui_info, gui_inputs)
        widget.exec_()

        if len(self.selected) < 1:
            show_message(
                "Render Ingest - Warnings",
                "No selection to work on!",
                "Please select at least one clip to proceed. Skipping.",
                icon = QtWidgets.QMessageBox.Warning
            )
            return

        if not widget.result:
            print("Operation aborted")
            return
        
        if not isinstance(widget.result, dict):
            print("Operation aborted")
            return
        
        errored_items = []
        warning_items = []

        for track_item in self.selected:

            project_name = os.environ["AVALON_PROJECT"]

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

            anatomy_tasks = get_anatomy_settings(project_name).get("tasks", {})
            asset_doc = get_asset_by_name(project_name, asset)

            asset_frame_start = asset_doc["data"]["frameStart"] - asset_doc["data"]["handleStart"] #type: ignore
            asset_frame_end = asset_doc["data"]["frameEnd"] + asset_doc["data"]["handleEnd"] #type: ignore
            asset_duration = asset_frame_end - asset_frame_start + 1

            clip_frame_start = track_item.source().sourceIn()
            clip_frame_end = track_item.source().sourceOut()
            clip_duration = clip_frame_end - clip_frame_start + 1

            if clip_duration != asset_duration:
                warning_items.append(f"{item_name} - Duration of clip is '{clip_duration}', duration of asset is '{asset_duration}'. <b>Please Doublecheck your plates!</b> ")
                track_item.source().binItem().setColor("#FFAA44")

            asset_tasks = asset_doc["data"].get("tasks", {}) #type: ignore

            if not asset_tasks:
                errored_items.append(f"{asset} - No tasks assigned to asset!")
                track_item.source().binItem().setColor("#CC55CC")
                continue

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
                errored_items.append(f"{item_name} - No suitable tasks found, please check input filename!")
                track_item.source().binItem().setColor("#CC5555")
                continue

            subset = f"render{task.capitalize()}{variant.capitalize()}"

            latest_version_doc = get_last_version_by_subset_name(
                project_name,
                subset,
                asset_id = asset_doc["_id"] #type: ignore
            )

            accepted_version = 1

            if widget.result.get("versionZero", {}).get("value", False):
                accepted_version = 0

            if latest_version_doc:
                accepted_version = latest_version_doc.get("name", accepted_version) + 1 #type: ignore
                if not (accepted_version == version):
                    errored_items.append(f"{item_name} - Version is not valid! Scanned version is '{version}', <b>Should be '{accepted_version}'!</b>")
                    track_item.source().binItem().setColor("#CC5555")
                    continue
            else:
                show_message(
                    "Render Ingest - Warning",
                    f"Couldn't find latest version for '{asset}'!",
                    f"Please check your track name, <b>subset is set at '{subset}' and track is '{variant}'!</b><br>Defaulting at version '{accepted_version}'.",
                    icon = QtWidgets.QMessageBox.Warning
                )
                version = accepted_version

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
                "frameStart": widget.result.get("ingestFrameStart", {}).get("value", 1001),
                "workfileFrameStart": widget.result.get("ingestFrameStart", {}).get("value", 1001),
                "handleStart": 0,
                "handleEnd": 0,
                "parents": hierarchy,
                "families": families,
                "heroTrack": True,
                "reviewTrack": None,
            }

            imprint(track_item, item_data)

        if warning_items:
            show_message(
                "Render Ingest - Warnings",
                "Found mismatches while creating ingest elements",
                warning_items,
                icon = QtWidgets.QMessageBox.Warning
            )

        if errored_items:
            show_message(
                "Render Ingest - Errors",
                "Found errors while creating ingest elements",
                errored_items,
                icon = QtWidgets.QMessageBox.Critical
            )