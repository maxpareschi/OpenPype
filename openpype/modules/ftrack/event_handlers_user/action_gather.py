import json
import os
import re
import tempfile
import traceback
import copy

from openpype.modules.ftrack.lib import BaseAction, statics_icon

from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_version_by_name
)

from openpype.lib import (
    get_openpype_execute_args,
    run_detached_process,
)

from openpype.settings.lib import (
    get_system_settings,
    get_project_settings,
    get_anatomy_settings
)


class GatherAction(BaseAction):
    """Gather selected Assetversions for publish into gather family."""

    identifier = "gather.versions"
    label = "Gather"
    description = "Gather version"
    icon = statics_icon("ftrack", "action_icons", "Gather.png")

    type = "Application"

    exclude_component_list = [ "review", "thumbnail" ]

    def __init__(self, *args, **kwargs):
        self.assetversions = list()
        self.project_name = None
        super().__init__(*args, **kwargs)

    def discover(self, session, entities, event):
        etype = entities[0].entity_type

        if etype == "AssetVersionList" or etype == "AssetVersion":
            return True
        else:
            return False

    def interface(self, session, entities, event):
        if event['data'].get('values', {}):
            return
        
        self.assetversions = self.get_all_assetversions(session, entities)
        self.project_name = self.assetversions[0]["project"]["full_name"]

        items = [{
            "type": "label",
            "value": "<h1><b>Select Representations to Gather:</b></h1>"
        }]

        try:
            for assetversion in self.assetversions:
                enum_data = []
                components = self.get_all_available_components_for_assetversion(session, assetversion)
                for component in components:
                    enum_data.append({
                        "label": component,
                        "value": component
                    })
                enum_data = sorted(enum_data, key = lambda d: not "exr"==d["label"])
                if not enum_data:
                    raise IndexError("Failed to fetch any components")
                item_name = "{} - {} v{}".format(
                    assetversion["asset"]["parent"]["name"],
                    assetversion["asset"]["name"],
                    str(assetversion["version"]).zfill(3)
                )
                items.extend(
                    [
                        {
                            "type": "label",
                            "value": "<b>{}</b>".format(item_name)
                        },
                        {
                            "label": "<span style=\"font-size: 7pt;\">Representation</span>",
                            "type": "enumerator",
                            "name": assetversion["id"],
                            "data": enum_data,
                            "value": enum_data[0]["value"]
                        }
                    ]
                )
            
            return {
                "type": "form",
                "title": "Gather Action",
                "items": items,
                "submit_button_label": "Gather",
                "width": 500,
                "height": 600
            }
    
        except:
            self.log.error(traceback.format_exc())
            return {"success": False, "message": traceback.format_exc().splitlines()[-1]}

    def launch(self, session, entities, event):

        user_values = event["data"].get("values", None)

        if user_values is None:
            return
        
        self.log.info("Sumbitted choices: {}".format(user_values))

        self.project_name = self.assetversions[0]["project"]["full_name"]

        forwarding_data = []

        for version in self.assetversions:
            current_links = len(version["outgoing_links"])
            if current_links > 0:
                self.log.debug("This asset has already linked gather versions attached, skipping gather for now...")
                continue
            instance_data = self.publisher_start(session, version, user_values)
            forwarding_data.append(instance_data)

        exchange_file = tempfile.mktemp(prefix="traypublisher_gather_", suffix=".json")
        with open(exchange_file, "w") as exf:
            exf.write(json.dumps(forwarding_data, indent=4, default=str))

        args = get_openpype_execute_args(
            "module",
            "traypublisher",
            "gather",
            exchange_file
        )
        run_detached_process(args)
                
        return {
            "success": True,
            "message": "Saved intermediate data, opening publisher..."
        }

    def get_all_available_components_for_assetversion(self, session, assetversion):
        component_list = []
        components = session.query("select name from Component where version_id is '{}'".format(assetversion["id"])).all()
        for comp in components:
            valid = True
            for excl in self.exclude_component_list:
                if comp["name"].find(excl) >= 0:
                    valid = False
                    break
            if valid:
                component_list.append(comp["name"])
            
        return list(set(component_list))

    def get_files_from_repre(self, repre, version):
        files = []
        for file in repre["files"]:
            files.append(file["path"].format(**repre["context"]))
        version_start = int(version["data"]["frameStart"]) - int(version["data"]["handleStart"])
        detected_startframe = re.findall(r'\d+$', os.path.splitext(files[0])[0])
        if detected_startframe:
            repre_start = int(detected_startframe[0])
        else:
            repre_start = version_start
        self.log.debug("Detected frames: repre_start:{} <-> version_start:{}".format(repre_start, version_start))
        if repre_start < version_start:
            files.pop(0)
        return files

    def get_all_assetversions(self, session, entities):

        result = []

        for entity in entities:
            etype = entity.entity_type

            if etype == "FileComponent":
                query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where components any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersion":
                query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where id is '{0}'".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersionList":
                query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where lists any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            else:
                message = "\"{}\" entity type is not implemented yet.".format(entity.entity_type)
                self.log.error(message)
        
        return result

    def get_comment_from_notes(self, session, entity, label_name):
        notes = []
        query = "select content, date, note_label_links.label.name from Note where parent_id is '{0}' and note_label_links.label.name is '{1}'".format(entity["id"],
                                                                                                                                           label_name)
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
            if label["label"]["name"] != label_name:
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

    def get_all_available_tasks(self, session, version):
        tasks = {}
        query = "select id, name, type.name from Task where parent_id is '{}'".format(version["asset"]["parent"]["id"])
        for task in session.query(query).all():
            tasks.update({ task["name"]: task["type"]["name"] })
        return tasks
     
    def publisher_start(self, session, version, user_values):
        family = "gather"
        project_name = self.project_name
        project_id = version["project_id"]
        version_name = int(version["version"])
        subset_name = version["asset"]["name"]
        asset_name = version["asset"]["parent"]["name"]
        repre_name = user_values[version["id"]]
        settings = get_project_settings(project_name)["ftrack"]["user_handlers"]["gather_action"]
        anatomy = get_anatomy_settings(project_name)

        self.log.debug("Asset Name for subset '{}' is '{}'".format(subset_name, asset_name))

        asset_doc = get_asset_by_name(
            project_name,
            asset_name
        )
        subset_doc = get_subset_by_name(
            project_name,
            subset_name,
            asset_doc["_id"]
        )
        version_doc = get_version_by_name(
            project_name,
            version_name,
            subset_doc["_id"]
        )
        repre_doc = get_representation_by_name(
            project_name,
            repre_name,
            version_doc["_id"]
        )

        repre_files = self.get_files_from_repre(repre_doc, version_doc)
        computed_asset = repre_doc["context"]["asset"]
        ftrack_tasks = self.get_all_available_tasks(session, version)
        avail_tasks = copy.deepcopy(ftrack_tasks)

        if len(settings["missing_task_override"]) > 0:
            task_override = settings["missing_task_override"][0]
        else:
            raise ValueError("Missing Task override if empty task from settings!")
        
        if not avail_tasks.get(task_override, None):
            avail_tasks.update({
                task_override.lower(): task_override
            })
        self.log.debug("Available tasks for current asset:\n{}".format(json.dumps(avail_tasks, indent=4, default=str)))

        try:
            detected_task_name = repre_doc["context"]["task"]["name"]
            if detected_task_name not in avail_tasks.keys():
                self.log.debug("Task '{}' not found in available tasks. Override name will be used.".format(detected_task_name))
                detected_task_name = task_override.lower()
        except:
            self.log.debug("Failed to fetch task for asset!")
            detected_task_name = next((key for key in avail_tasks if avail_tasks[key] == task_override), "")
            
        self.log.debug("Detected task name: '{}'".format(detected_task_name))

        try:
            type_id = session.query("select id from Type where name is '{}'".format(task_override)).one()["id"]
            self.log.debug("Creating task named '{}' of type '{}' in asset '{}'".format(
                detected_task_name, task_override, version["asset"]["parent"]["name"]
            ))
            session.create("Task", {
                "name": detected_task_name,
                "type_id": type_id,
                "parent_id": version["asset"]["parent"]["id"]
            })
            session.commit()
            self.log.debug("Created task '{}' of type '{}'".format(detected_task_name, task_override))
        except:
            if detected_task_name:
                self.log.debug("Task '{}'  of type '{}' is already present, no need to create one. skipping task generation...".format(detected_task_name, task_override))
            else:
                raise ValueError("Failed creating task '{}' of type '{}', gathering might fail or be malformed!!".format(detected_task_name, task_override))

        task_info = {
            "type": avail_tasks[detected_task_name],
            "name": detected_task_name,
            "short": anatomy["tasks"][avail_tasks[detected_task_name]]["short_name"]
        }
        self.log.debug("Computed task is: {}".format(json.dumps(task_info, indent=4, default=str)))

        computed_variant = repre_doc["context"]["subset"].replace(
            repre_doc["context"]["family"],
            ""
        ).replace(
            detected_task_name.capitalize(),
            ""
        )
        self.log.debug("Computed variant is '{}'".format(computed_variant))

        subset_format_data = {
            "asset": computed_asset,
            "family": family,
            "task": task_info,
            "variant": computed_variant,
        }

        computed_subset = settings["subset_name_template"].format_map(subset_format_data)
        computed_assetversion_name = settings["ftrack_name_template"].format_map(subset_format_data)
        self.log.debug("Computed subset is '{}'".format(computed_subset))

        gather_root = settings["gather_root"].strip()
        gather_suffix = settings["gather_asset_suffix"].strip()

        if gather_root:
            if gather_suffix:
                gather_suffix = "_" + gather_suffix
            else:
                gather_suffix = "_"
        else:
            gather_root = asset_doc["data"]["parents"][-1]
            gather_suffix = ""

        if task_info["name"] == "":
            task_info["name"] = task_info["type"].lower()

        gather_instance = {
            "project": project_name,
            "family": family,
            "subset": computed_subset,
            "variant": computed_variant,
            "asset": repre_doc["context"]["asset"],
            "task": detected_task_name,
            "gather_root_name": gather_root,
            "gather_project_name": project_name,
            "gather_project_id": project_id,
            "gather_assetversion_name": computed_assetversion_name,
            "gather_representation_name": repre_doc["name"],
            "gather_representation_files": repre_files,
            "gather_representation_ext": os.path.splitext(repre_files[0])[-1].replace(".", ""),
            "gather_asset_name": asset_name + gather_suffix,
            "gather_task_id": str(version["task_id"]) if str(version["task_id"]) != "NOT_SET" else None,
            "gather_ftrack_source_id": version["id"],
            "gather_task_injection": task_info
        }

        note = self.get_comment_from_notes(
            session, version, settings["gather_note_label_name"].strip())
        if note:
            gather_instance.update(note)

        self.log.debug("Instance data to be created: {}".format(
            json.dumps(gather_instance, indent=4, default=str)))

        return gather_instance

    
def register(session):
    '''Register plugin. Called when used as an plugin.'''

    GatherAction(session).register()
