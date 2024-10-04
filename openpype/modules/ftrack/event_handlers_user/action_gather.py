from __future__ import annotations
from typing import List, Dict
import json
from re import compile as recomp
from tempfile import mktemp
import traceback
from pathlib import Path
from pprint import pformat
from logging import getLogger

logger = getLogger(__name__)

from ftrack_api import Session
from ftrack_api.entity.asset_version import AssetVersion
from ftrack_api.event.base import Event
from ftrack_api.entity.base import Entity
from ftrack_api.exception import ServerError
from openpype.modules.ftrack.lib import BaseAction, statics_icon
from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_version_by_name,
)
from openpype.lib import get_openpype_execute_args, run_detached_process
from openpype.settings.lib import (
    get_system_settings,
    get_project_settings,
    get_anatomy_settings,
)

# match any 4 or more digit sequence surrounded by either . or _
FRAME_REGEX = recomp("(?<=[\.\_])\d{4,}(?=[\.\_]|$)")


def yield_resolved_files_from_repre(repre: dict):
    """Yields the file paths from a repre resolved with the repre context."""

    for file in repre["files"]:
        yield file["path"].format(**repre["context"])


def has_slate(version: dict, files: List[str, Path]):
    """Compares version data and its resolved files to check if files have slate.

    If the files are videos, it will asume they don't have slate.
    """

    data = version["data"]
    version_start = int(data["frameStart"]) - int(data["handleStart"])

    matches = FRAME_REGEX.findall(Path(files[0]).stem)[-1]
    if not matches:
        return False
    repre_start = int(matches[0])
    logger.debug(f"repre_start:{repre_start} <-> version_start:{version_start}")
    if repre_start < version_start:
        return True
    return False


def get_files_from_repre(repre: dict, version: dict, keep_slate: bool = False):
    """Returns the repre files with option to remove the slate frame from them."""

    files = list(yield_resolved_files_from_repre(repre))
    if has_slate(version, files) and not keep_slate:
        files.pop(0)
    return files


def get_all_available_tasks_from_version(session: Session, version: AssetVersion):
    """Returns a dict of task names and its type for a given version's parent.

    The dict is similar to the one below:
    {
        "animation": "Animation",
        "compositing": "Compositing",
        "editing": "Editing",
        "fx": "FX",
        "layout": "Layout",
        "lookdev": "Lookdev",
        "modeling": "Modeling",
        "roundtrip": "Roundtrip"
    }
    """

    tasks = {}
    id_ = version["asset"]["parent"]["id"]
    query = f"select id, name, type.name from Task where parent_id is '{id_}'"
    for task in session.query(query).all():
        tasks.update({task["name"]: task["type"]["name"]})
    return tasks


def create_task_in_ftrack(session: Session, task_type: str, task_name: str, id_: str):
    """Creates an Ftrack task of type `task_type` and name `task_name`.

    The task is attached to the asset with id `id_`.
    """
    type_id = session.query(f"Type where name is {task_type}").one()["id"]
    data = {"name": task_name, "type_id": type_id, "parent_id": id_}
    session.create("Task", data)
    try:
        session.commit()
    except ServerError as e:
        if not "DuplicateEntryError" in e.message:
            logger.critical(f"Error creating task from data {pformat(data)}")
            raise e
        logger.debug("Task not created as it already exists")
    else:
        logger.debug(f"Created task '{task_name}' of type '{task_type}'")


def get_gather_task_name(tasks: Dict[str, str], repre: dict, fallback: str):
    """Returns the gather task name for `repre`. Fallsback to `fallback` task."""

    try:
        asset_task_name: str = repre["context"]["task"]["name"]
    except KeyError as e:
        logger.debug(f"Failed to fetch task for asset. Falling back to {fallback}")
        task_name = fallback.lower()
    else:
        if asset_task_name not in tasks.keys():
            logger.warning(f"Task '{asset_task_name}' not in available tasks.")
            logger.warning(f"Task {fallback} will be used.")
            task_name = fallback.lower()
        else:
            task_name = asset_task_name

    logger.debug(f"Detected gather task name: '{task_name}'")
    return task_name


def get_comment_from_notes(session: Session, entity: Entity, label_name: str):
    notes = []
    select = "select content, date, note_label_links.label.name"
    where = (
        f"where parent_id is \"{entity['id']}\" "
        f'and note_label_links.label.name is "{label_name}"'
    )
    query = f"{select} from Note {where}"
    for note in session.query(query).all():
        notes.append(note)

    if not notes:
        return None

    notes_sorted = list(sorted(notes, key=lambda d: d["date"]))
    intent_value = None
    result = {"comment": "", "intent": {"label": "", "value": ""}}

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


def ensure_gather_fallback_exists(
    session: Session, version: AssetVersion, tasks: Dict[str, str]
):
    """Ensures there is an existing gather fallback task in Ftrack for `version`."""

    prj = version["project"]["full_name"]
    cfg = get_project_settings(prj)["ftrack"]["user_handlers"]["gather_action"]

    if len(cfg["missing_task_override"]) > 0:  # this is a list for UI reasons
        fallback: str = cfg["missing_task_override"][0]
    else:
        raise ValueError("Failed to find 'missing_task_override' in settings.")

    if not tasks.get(fallback):  # create fallback if doesn't exist
        id_ = version["asset"]["parent"]["id"]
        create_task_in_ftrack(session, fallback, fallback.lower(), id_)
        tasks.update({fallback.lower(): fallback})
    return fallback


def create_temp_json(data: dict):
    tmp = mktemp(prefix="traypublisher_gather_", suffix=".json")
    with open(tmp, "w") as ftw:
        ftw.write(json.dumps(data, indent=4, default=str))
    return tmp


def yield_gather_data_from_versions(
    session: Session, versions: List[AssetVersion], user_values: dict
):
    """Yields gather data from asset versions that are not yet gathered."""

    for v in versions:
        current_links = len(v["outgoing_links"])
        if current_links > 0:
            logger.debug(f"Skipping gather of {v} as it already has one linked.")
            continue
        yield get_gather_intance(session, v, user_values)


def get_all_assetversions(session: Session, entities: List[Entity]):

    result: List[AssetVersion] = []

    for entity in entities:
        etype = entity.entity_type

        if etype == "FileComponent":
            query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where components any (id='{0}')".format(
                entity["id"]
            )
            for assetversion in session.query(query).all():
                result.append(assetversion)

        elif etype == "AssetVersion":
            query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where id is '{0}'".format(
                entity["id"]
            )
            for assetversion in session.query(query).all():
                result.append(assetversion)

        elif etype == "AssetVersionList":
            query = "select id, asset_id, task.name, task_id, version, asset.name, asset.parent.name, outgoing_links from AssetVersion where lists any (id='{0}')".format(
                entity["id"]
            )
            for assetversion in session.query(query).all():
                result.append(assetversion)

        else:
            message = '"{}" entity type is not implemented yet.'.format(
                entity.entity_type
            )
            logger.error(message)

    return result


def get_all_available_components_for_assetversion(
    session: Session, assetversion: AssetVersion, exclude_component_list: List[str]
):

    component_list = []
    components = session.query(
        "select name from Component where version_id is '{}'".format(assetversion["id"])
    ).all()
    for comp in components:
        valid = True
        for excl in exclude_component_list:
            if comp["name"].find(excl) >= 0:
                valid = False
                break
        if valid:
            component_list.append(comp["name"])

    return list(set(component_list))


def get_gather_intance(session: Session, version: AssetVersion, user_values: dict):

    # SET UP LOCAL VARIABLES
    family = "gather"
    prj = version["project"]["full_name"]
    project_id = version["project_id"]
    version_name = int(version["version"])
    subset_name = version["asset"]["name"]
    asset_name = version["asset"]["parent"]["name"]
    repre_name = user_values[version["id"]]
    settings = get_project_settings(prj)["ftrack"]["user_handlers"]["gather_action"]
    anatomy = get_anatomy_settings(prj)
    asset_doc = get_asset_by_name(prj, asset_name)
    subset_doc = get_subset_by_name(prj, subset_name, asset_doc["_id"])
    version_doc = get_version_by_name(prj, version_name, subset_doc["_id"])
    repre_doc = get_representation_by_name(prj, repre_name, version_doc["_id"])
    repre_files = get_files_from_repre(repre_doc, version_doc)
    computed_asset = repre_doc["context"]["asset"]
    repre_family: str = repre_doc["context"]["family"]
    repre_subset: str = repre_doc["context"]["subset"]
    task_id = str(version["task_id"])

    logger.debug(f"Asset name for subset '{subset_name}' is '{asset_name}'")

    # if assetversion is attached to a task, that task will use
    # if there is not task attached to the asset version, the task_override
    # will be assigned
    available_tasks = get_all_available_tasks_from_version(session, version)
    logger.debug(f"Available tasks @ current asset:\n{pformat(available_tasks)}")
    fallback = ensure_gather_fallback_exists(session, version, available_tasks)
    task_name = get_gather_task_name(available_tasks, repre_doc, fallback)

    task_info = {
        "type": available_tasks[task_name],
        "name": task_name,
        "short": anatomy["tasks"][available_tasks[task_name]]["short_name"],
    }

    logger.debug(f"Computed task is: {pformat(task_info)}")
    task_name_cap = task_name.capitalize()
    computed_variant = repre_subset.replace(repre_family, "").replace(task_name_cap, "")
    logger.debug(f"Computed variant is '{computed_variant}'")

    subset_format_data = {
        "asset": computed_asset,
        "family": family,
        "task": task_info,
        "variant": computed_variant,
    }

    computed_subset = settings["subset_name_template"].format_map(subset_format_data)
    computed_assetversion_name = settings["ftrack_name_template"].format_map(
        subset_format_data
    )
    logger.debug("Computed subset is '{}'".format(computed_subset))

    gather_root = settings["gather_root"].strip()
    gather_suffix = settings["gather_asset_suffix"].strip()  # TODO: why strip here?

    gather_suffix = "_" + gather_suffix if gather_suffix else "_"
    if not gather_root:
        gather_root = asset_doc["data"]["parents"][-1]
        gather_suffix = ""

    repre_file = repre_files[0] if isinstance(repre_files, list) else repre_files
    ext = Path(repre_file).suffix[1:]
    gather_task_id = task_id if task_id != "NOT_SET" else None

    gather_instance = {
        "project": prj,
        "family": family,
        "subset": computed_subset,
        "variant": computed_variant,
        "asset": computed_asset,
        "task": task_name,
        "gather_root_name": gather_root,
        "gather_project_name": prj,
        "gather_project_id": project_id,
        "gather_assetversion_name": computed_assetversion_name,
        "gather_representation_name": repre_doc["name"],
        "gather_representation_files": repre_files,
        "gather_representation_ext": ext,
        "gather_asset_name": asset_name + gather_suffix,
        "gather_task_id": gather_task_id,
        "gather_ftrack_source_id": version["id"],
        "gather_task_injection": task_info,
    }

    label_name = settings["gather_note_label_name"].strip()
    note = get_comment_from_notes(session, version, label_name)
    if note:
        gather_instance.update(note)

    logger.debug(f"Instance data to be created: {pformat(gather_instance)}")

    return gather_instance


class GatherAction(BaseAction):
    """Gather selected Assetversions for publish into gather family."""

    identifier = "gather.versions"
    label = "Gather"
    description = "Gather version"
    icon = statics_icon("ftrack", "action_icons", "Gather.png")

    type = "Application"

    exclude_component_list = ["review", "thumbnail"]

    def __init__(self, *args, **kwargs):
        global logger
        self.assetversions: List[AssetVersion] = list()
        self.project_name = None
        super().__init__(*args, **kwargs)
        logger = self.log

    def discover(self, session, entities, event):
        etype = entities[0].entity_type

        if etype == "AssetVersionList" or etype == "AssetVersion":
            return True
        else:
            return False

    def interface(self, session, entities, event):
        if event["data"].get("values", {}):
            return

        self.assetversions = get_all_assetversions(session, entities)
        self.project_name = self.assetversions[0]["project"]["full_name"]

        items = [
            {
                "type": "label",
                "value": "<h1><b>Select Representations to Gather:</b></h1>",
            }
        ]

        try:
            for assetversion in self.assetversions:
                enum_data = []
                components = get_all_available_components_for_assetversion(
                    session, assetversion, self.exclude_component_list
                )
                for component in components:
                    enum_data.append({"label": component, "value": component})
                enum_data = sorted(enum_data, key=lambda d: not "exr" == d["label"])
                if not enum_data:
                    raise IndexError("Failed to fetch any components")
                item_name = "{} - {} v{}".format(
                    assetversion["asset"]["parent"]["name"],
                    assetversion["asset"]["name"],
                    str(assetversion["version"]).zfill(3),
                )
                default_enum_value = None
                for en in enum_data:
                    if "exr" in en["value"]:
                        default_enum_value = en["value"]
                        break
                if not default_enum_value:
                    for en in enum_data:
                        if "jpg" in en["value"] or "png" in en["value"]:
                            default_enum_value = en["value"]
                            break
                if not default_enum_value:
                    default_enum_value = enum_data[0]["value"]
                items.extend(
                    [
                        {"type": "label", "value": "<b>{}</b>".format(item_name)},
                        {
                            "label": '<span style="font-size: 7pt;">Representation</span>',
                            "type": "enumerator",
                            "name": assetversion["id"],
                            "data": enum_data,
                            "value": default_enum_value,
                        },
                    ]
                )

            return {
                "type": "form",
                "title": "Gather Action",
                "items": items,
                "submit_button_label": "Gather",
                "width": 500,
                "height": 600,
            }

        except:
            self.log.error(traceback.format_exc())
            return {
                "success": False,
                "message": traceback.format_exc().splitlines()[-1],
            }

    def launch(self, session: Session, entities: List[Entity], event: Event):

        session.reset()
        session.auto_populating(True)

        user_values = event["data"].get("values", None)

        if user_values is None:
            return

        self.log.info(f"Sumbitted choices: {user_values}")

        json_data = list(
            yield_gather_data_from_versions(session, self.assetversions, user_values)
        )
        if not json_data:
            return {
                "success": True,
                "message": "All AssetVersions already gathered. Skipping.",
            }

        # Create temp json and launch the traypublisher locally
        temp = create_temp_json(json_data)
        args = get_openpype_execute_args("module", "traypublisher", "gather", temp)
        self.log.info(temp)
        run_detached_process(args)

        return {
            "success": True,
            "message": "Saved intermediate data, opening traypublisher...",
        }


def register(session):
    """Register plugin. Called when used as an plugin."""

    GatherAction(session).register()
