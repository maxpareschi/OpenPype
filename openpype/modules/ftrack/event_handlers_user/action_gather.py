import json
import os
import re
import sys
import subprocess
import copy
import platform

import openpype.lib
import pyblish.api

import openpype.hosts
import openpype.hosts.traypublisher
import openpype.hosts.traypublisher.api
from openpype.lib import ApplicationManager
from qtpy import QtWidgets, QtCore

import openpype.modules
from openpype.pipeline import install_host
from openpype.modules.ftrack import FTRACK_MODULE_DIR
from openpype.modules.ftrack.lib import BaseAction, statics_icon

from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_last_version_by_subset_id,
    get_project
)

import openpype.pipeline
from openpype.settings import (
    get_system_settings,
    get_project_settings
)

import openpype
import openpype.tools
import openpype.tools.traypublisher


class GatherAction(BaseAction):
    """Gather selected Assetversions for publish into delivery family."""

    identifier = "gather.versions"
    label = "Gather"
    description = "Gather version"
    icon = statics_icon("ftrack", "action_icons", "Gather.png")

    type = "Application"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def discover(self, session, entities, event):
        etype = entities[0].entity_type

        if etype == "AssetVersionList" or etype == "AssetVersion":
            return True
        else:
            return False

    def launch(self, session, entities, event):

        assetversions = self.get_all_assetversions(session, entities)
        
        project_name = assetversions[0]["project"]["full_name"]

        host = openpype.hosts.traypublisher.api.TrayPublisherHost()
        host.set_project_name(project_name)
        install_host(host)
        
        self.log.debug(json.dumps(host.get_context_data(), indent=4, default=str))
        create_context = openpype.pipeline.create.CreateContext(host,
                                                                headless=True,
                                                                discover_publish_plugins=True,
                                                                reset=True)

        for instance in list(create_context.instances):
            create_plugin = create_context.creators.get(
                instance.creator_identifier
            )
            create_plugin.remove_instances([instance])

        for version in assetversions:
            self.target_asset_name = "{}_delivery".format(version["asset"]["name"])
            self.publisher_start(version, session, create_context)

        app_instance = QtWidgets.QApplication.instance()
        if app_instance is None:
            app_instance = QtWidgets.QApplication([])
        if platform.system().lower() == "windows":
            import ctypes
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                u"traypublisher"
            )
        
        window = openpype.tools.traypublisher.window.TrayPublishWindow()
        window._overlay_widget._set_project(project_name)
        # window.set_context_label("{} - GATHER DELIVERIES".format(project_name))
        window.show()
        app_instance.exec_()

        return True

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

    def get_all_assetversions(self, session, entities):

        result = []

        for entity in entities:
            etype = entity.entity_type

            if etype == "FileComponent":
                query = "select id, asset_id, version from AssetVersion where components any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersion":
                query = "select id, asset_id, version from AssetVersion where id is '{0}'".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersionList":
                query = "select id, asset_id, version from AssetVersion where lists any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            else:
                message = "\"{}\" entity type is not implemented yet.".format(entity.entity_type)
                self.log.error(message)
        
        return result

    def publisher_start(self, entity, session, create_context):

        project_name = entity["project"]["full_name"]
        subset_name = entity["asset"]["name"]
        asset_name = entity["asset"]["parent"]["name"]
        repre_name = "exr"
        for component in entity["components"]:
            if component["name"].find(repre_name) >= 0:
                repre_name = component["name"]
                break

        asset_doc = get_asset_by_name(
            project_name,
            asset_name
        )
        subset_doc = get_subset_by_name(
            project_name,
            subset_name,
            asset_doc["_id"]
        )
        version_doc = get_last_version_by_subset_id(
            project_name,
            subset_doc["_id"]
        )
        repre_doc = get_representation_by_name(
            project_name,
            repre_name,
            version_doc["_id"]
        )

        repre_files = self.get_files_from_repre(repre_doc, version_doc)

        computed_asset = repre_doc["context"]["asset"]
        computed_task = repre_doc["context"]["task"]["name"]
        computed_variant = repre_doc["context"]["subset"].replace(repre_doc["context"]["family"], "")
        computed_subset = "delivery{}".format(computed_variant)
        computed_name = "{}_{}".format(computed_asset, computed_subset)

        delivery_instance = {
            "project": project_name,
            "family": "delivery",
            "families": ["delivery"],
            "subset": computed_subset,
            "variant": computed_variant,
            "asset": repre_doc["context"]["asset"],
            "task": computed_task,
            "name": computed_name,
            "label": computed_name,
            "delivery_root_name": get_project_settings(project_name)["ftrack"]["publish"]["IntegrateFtrackApi"]["delivery_root"],
            "delivery_representation_name": repre_doc["name"],
            "delivery_representation_files": repre_files,
            "delivery_asset_name": "{}_delivery".format(asset_name),
            "delivery_task_id": entity["task_id"],
            "delivery_ftrack_source_id": entity["id"]
        }

        note = self.get_comment_from_notes(session, entity)
        if note:
            delivery_instance.update(note)

        self.log.debug("Instance data to be created: {}".format(json.dumps(delivery_instance, indent=4, default=str)))

        publish_file_list = [item.to_dict() for item in openpype.lib.FileDefItem.from_paths(
            repre_files, allow_sequences=True)]
        
        create_context.create(
            "settings_delivery",
            computed_subset,
            delivery_instance,
            pre_create_data={
                "representation_files": publish_file_list,
                "reviewable": publish_file_list[0],
            }
        )

    def get_comment_from_notes(self, session, entity):
        client_tag = "For Client"
        notes = []
        query = "select content, date, note_label_links.label.name from Note where parent_id is '{0}' and note_label_links.label.name is '{1}'".format(entity["id"],
                                                                                                                                           client_tag)
        for note in session.query(query).all():
            notes.append(note)

        if not notes:
            return None
        
        notes_sorted = list(sorted(notes, key=lambda d: d["date"]))
        intent_value = None
        result = {
            "comment": "",
            "intent": {
                "label": "",
                "value": ""
            }
        }
        for label in notes_sorted[-1]["note_label_links"]:
            if label["label"]["name"] != client_tag:
                intent_value = label["label"]["name"]
        
        if intent_value:
            system_settings = get_system_settings()
            intent_settings = system_settings["modules"]["ftrack"]["intent"]["items"]
            for key, value in intent_settings.items():
                if key == intent_value:
                    result["intent"]["label"] = value
                    result["intent"]["value"] = key

        result["comment"] = notes_sorted[-1]["content"]

        return result

    
def register(session):
    '''Register plugin. Called when used as an plugin.'''

    GatherAction(session).register()
