"""These methods were extracted from the OP delete versions loader action."""

from logging import getLogger
import collections
import os
import uuid

logger = getLogger(__name__)




import clique
from pymongo import UpdateOne


from openpype import style
from openpype.client import get_versions, get_representations
from openpype.modules import ModulesManager
from openpype.lib import format_file_size
from openpype.pipeline import AvalonMongoDB, Anatomy
from openpype.pipeline.load import (
    get_representation_path_with_anatomy,
    InvalidRepresentationContext,
)



sequence_splitter = "__sequence_splitter__"



def _ftrack_delete_versions(data):
    """Delete version on ftrack.

    Handling of ftrack logic in this plugin is not ideal. But in OP3 it is
    almost impossible to solve the issue other way.

    Note:
        Asset versions on ftrack are not deleted but marked as
            "not published" which cause that they're invisible.

    Args:
        data (dict): Data sent to subset loader with full context.
    """

    # First check for ftrack id on asset document
    #   - skip if ther is none
    asset_ftrack_id = data["asset"]["data"].get("ftrackId")
    if not asset_ftrack_id:
        logger.info((
            "Asset does not have filled ftrack id. Skipped delete"
            " of ftrack version."
        ))
        return

    # Check if ftrack module is enabled
    modules_manager = ModulesManager()
    ftrack_module = modules_manager.modules_by_name.get("ftrack")
    if not ftrack_module or not ftrack_module.enabled:
        return

    import ftrack_api

    session = ftrack_api.Session()
    subset_name = data["subset"]["name"]
    versions = {
        '"{}"'.format(version_doc["name"])
        for version_doc in data["versions"]
    }
    asset_versions = session.query(
        (
            "select id, is_published from AssetVersion where"
            " asset.parent.id is \"{}\""
            " and asset.name is \"{}\""
            " and version in ({})"
        ).format(
            asset_ftrack_id,
            subset_name,
            ",".join(versions)
        )
    ).all()

    # Set attribute `is_published` to `False` on ftrack AssetVersions
    for asset_version in asset_versions:
        asset_version["is_published"] = False

    try:
        session.commit()

    except Exception:
        msg = (
            "Could not set `is_published` attribute to `False`"
            " for selected AssetVersions."
        )
        logger.error(msg)


def path_from_representation(representation, anatomy):
    try:
        context = representation["context"]
    except KeyError:
        return (None, None)

    try:
        path = get_representation_path_with_anatomy(
            representation, anatomy
        )
    except InvalidRepresentationContext:
        return (None, None)

    sequence_path = None
    if "frame" in context:
        context["frame"] = sequence_splitter
        sequence_path = get_representation_path_with_anatomy(
            representation, anatomy
        )

    if sequence_path:
        sequence_path = sequence_path.normalized()

    return (path.normalized(), sequence_path)



def get_data(context, versions_count, version_ids = None):
    subset = context["subset"]
    asset = context["asset"]
    project_name = context["project"]["name"]
    anatomy = Anatomy(project_name)

    versions = list(get_versions(project_name, subset_ids=[subset["_id"]], version_ids = version_ids))

    versions_by_parent = collections.defaultdict(list)
    for ent in versions:
        versions_by_parent[ent["parent"]].append(ent)

    def sort_func(ent):
        return int(ent["name"])

    all_last_versions = []
    for _parent_id, _versions in versions_by_parent.items():
        for idx, version in enumerate(
            sorted(_versions, key=sort_func, reverse=True)
        ):
            if idx >= versions_count:
                break
            all_last_versions.append(version)

    logger.debug("Collected versions ({})".format(len(versions)))

    # Filter latest versions
    # for version in all_last_versions:
    #     versions.remove(version)

    # Update versions_by_parent without filtered versions
    versions_by_parent = collections.defaultdict(list)
    for ent in versions:
        versions_by_parent[ent["parent"]].append(ent)

    # Filter already deleted versions
    versions_to_pop = []
    for version in versions:
        version_tags = version["data"].get("tags")
        if version_tags and "deleted" in version_tags:
            versions_to_pop.append(version)

    for version in versions_to_pop:
        msg = "Asset: \"{}\" | Subset: \"{}\" | Version: \"{}\"".format(
            asset["name"], subset["name"], version["name"]
        )
        logger.debug((
            "Skipping version. Already tagged as `deleted`. < {} >"
        ).format(msg))
        versions.remove(version)

    version_ids = [ent["_id"] for ent in versions]

    logger.debug(
        "Filtered versions to delete ({})".format(len(version_ids))
    )

    if not version_ids:
        msg = "Skipping processing. Nothing to delete on {}/{}".format(
            asset["name"], subset["name"]
        )
        logger.info(msg)
        print(msg)
        return

    repres = list(get_representations(
        project_name, version_ids=version_ids
    ))

    logger.debug(
        "Collected representations to remove ({})".format(len(repres))
    )

    dir_paths = {}
    file_paths_by_dir = collections.defaultdict(list)
    for repre in repres:
        file_path, seq_path = path_from_representation(repre, anatomy)
        if file_path is None:
            logger.debug((
                "Could not format path for represenation \"{}\""
            ).format(str(repre)))
            continue

        dir_path = os.path.dirname(file_path)
        dir_id = None
        for _dir_id, _dir_path in dir_paths.items():
            if _dir_path == dir_path:
                dir_id = _dir_id
                break

        if dir_id is None:
            dir_id = uuid.uuid4()
            dir_paths[dir_id] = dir_path

        file_paths_by_dir[dir_id].append([file_path, seq_path])

    dir_ids_to_pop = []
    for dir_id, dir_path in dir_paths.items():
        if os.path.exists(dir_path):
            continue

        dir_ids_to_pop.append(dir_id)

    # Pop dirs from both dictionaries
    for dir_id in dir_ids_to_pop:
        dir_paths.pop(dir_id)
        paths = file_paths_by_dir.pop(dir_id)
        # TODO report of missing directories?
        paths_msg = ", ".join([
            "'{}'".format(path[0].replace("\\", "/")) for path in paths
        ])
        logger.debug((
            "Folder does not exist. Deleting it's files skipped: {}"
        ).format(paths_msg))

    data = {
        "dir_paths": dir_paths,
        "file_paths_by_dir": file_paths_by_dir,
        "versions": versions,
        "asset": asset,
        "subset": subset,
        "archive_subset": versions_count == 0
    }

    return data


def delete_whole_dir_paths(dir_paths, delete=True):
    size = 0

    for dir_path in dir_paths:
        # Delete all files and fodlers in dir path
        for root, dirs, files in os.walk(dir_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                size += os.path.getsize(file_path)
                if delete:
                    os.remove(file_path)
                    logger.debug("Removed file: {}".format(file_path))

            for name in dirs:
                if delete:
                    os.rmdir(os.path.join(root, name))

        if not delete:
            continue

        # Delete even the folder and it's parents folders if they are empty
        while True:
            if not os.path.exists(dir_path):
                dir_path = os.path.dirname(dir_path)
                continue

            if len(os.listdir(dir_path)) != 0:
                break

            os.rmdir(os.path.join(dir_path))

    return size

def delete_only_repre_files(dir_paths, file_paths, delete=True):
    size = 0

    for dir_id, dir_path in dir_paths.items():
        dir_files = os.listdir(dir_path)
        collections, remainders = clique.assemble(dir_files)
        for file_path, seq_path in file_paths[dir_id]:
            file_path_base = os.path.split(file_path)[1]
            # Just remove file if `frame` key was not in context or
            # filled path is in remainders (single file sequence)
            if not seq_path or file_path_base in remainders:
                if not os.path.exists(file_path):
                    logger.debug(
                        "File was not found: {}".format(file_path)
                    )
                    continue

                size += os.path.getsize(file_path)

                if delete:
                    os.remove(file_path)
                    logger.debug("Removed file: {}".format(file_path))

                if file_path_base in remainders:
                    remainders.remove(file_path_base)
                continue

            seq_path_base = os.path.split(seq_path)[1]
            head, tail = seq_path_base.split(sequence_splitter)

            final_col = None
            for collection in collections:
                if head != collection.head or tail != collection.tail:
                    continue
                final_col = collection
                break

            if final_col is not None:
                # Fill full path to head
                final_col.head = os.path.join(dir_path, final_col.head)
                for _file_path in final_col:
                    if os.path.exists(_file_path):

                        size += os.path.getsize(_file_path)

                        if delete:
                            os.remove(_file_path)
                            logger.debug(
                                "Removed file: {}".format(_file_path)
                            )

                _seq_path = final_col.format("{head}{padding}{tail}")
                logger.debug("Removed files: {}".format(_seq_path))
                collections.remove(final_col)

            elif os.path.exists(file_path):
                size += os.path.getsize(file_path)

                if delete:
                    os.remove(file_path)
                    logger.debug("Removed file: {}".format(file_path))
            else:
                logger.debug(
                    "File was not found: {}".format(file_path)
                )

    # Delete as much as possible parent folders
    if not delete:
        return size

    for dir_path in dir_paths.values():
        while True:
            if not os.path.exists(dir_path):
                dir_path = os.path.dirname(dir_path)
                continue

            if len(os.listdir(dir_path)) != 0:
                break

            logger.debug("Removed folder: {}".format(dir_path))
            os.rmdir(dir_path)

    return size



def main(project_name, data, remove_publish_folder):
    # Size of files.
    size = 0
    if not data:
        return size

    if remove_publish_folder:
        size = delete_whole_dir_paths(data["dir_paths"].values())
    else:
        size = delete_only_repre_files(
            data["dir_paths"], data["file_paths_by_dir"]
        )

    mongo_changes_bulk = []
    for version in data["versions"]:
        orig_version_tags = version["data"].get("tags") or []
        version_tags = [tag for tag in orig_version_tags]
        if "deleted" not in version_tags:
            version_tags.append("deleted")

        if version_tags == orig_version_tags:
            continue

        update_query = {"_id": version["_id"]}
        update_data = {"$set": {"data.tags": version_tags}}
        mongo_changes_bulk.append(UpdateOne(update_query, update_data))

    if data["archive_subset"]:
        mongo_changes_bulk.append(UpdateOne(
            {
                "_id": data["subset"]["_id"],
                "type": "subset"
            },
            {"$set": {"type": "archived_subset"}}
        ))

    if mongo_changes_bulk:
        dbcon = AvalonMongoDB()
        dbcon.Session["AVALON_PROJECT"] = project_name
        dbcon.install()
        dbcon.bulk_write(mongo_changes_bulk)
        dbcon.uninstall()

    # self._ftrack_delete_versions(data)

    return size



def load(contexts, versions_to_keep = 0, remove_publish_folder = False, version_ids = None):
    versions_to_keep = 2
    remove_publish_folder = False
    try:
        size = 0
        for count, context in enumerate(contexts):

            data = get_data(context, versions_to_keep, version_ids)
            if not data:
                continue

            project_name = context["project"]["name"]
            size += main(project_name, data, remove_publish_folder)
            print("Progressing {}/{}".format(count + 1, len(contexts)))

        msg = f"Total size of files: {format_file_size(size)}"
        logger.info(msg)

    except Exception:
        logger.error("Failed to delete versions.", exc_info=True)