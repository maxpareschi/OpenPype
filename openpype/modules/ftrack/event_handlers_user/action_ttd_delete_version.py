from typing import List, Set, Callable
from pathlib import Path
from logging import getLogger, basicConfig, DEBUG, INFO
from os import environ, remove, rmdir

from logging import getLogger

logger = getLogger(__name__)

from openpype_modules.ftrack.lib import BaseAction, statics_icon # type: ignore
from openpype.client import get_subsets, get_versions, get_representations
from openpype.pipeline.anatomy import Anatomy
from openpype.client.operations import OperationsSession
from ftrack_api.event.base import Event
from ftrack_api import Session
from ftrack_api.entity.asset_version import AssetVersion
from ftrack_api.entity.base import Entity




def get_op_version_from_ftrack_assetversion(
    prj_name: str, asset_mongo_id: str, subset_name: str, version_number: str
):
    """
    version = session.query("AssetVersion where project.name is TEST_JUAN").first()
    asset_mongo_id = version["asset"]["parent"]["custom_attributes"]["avalon_mongo_id"]
    subset_name = version["asset"]["name"]
    version_number = version["version"]
    """
    try:
        subset = list(get_subsets(
            prj_name, asset_ids=[asset_mongo_id], subset_names=[subset_name]
        ))[0]
        return list(get_versions(
            prj_name, subset_ids=[subset["_id"]], versions=[version_number]
        ))[0]
    except Exception as e:
        logger.warning("Failed to find an OP version with:\n"
            f"prj_name = {prj_name}\n"
            f"version_number = {version_number}\n"
            f"subset_name = {subset_name} and\n"
            f"parent mongo_ id = {asset_mongo_id}"
        )

def ttd_remove_op_versions(prj_name: str, versions: List[dict], test_run: bool = False):

    # GATHER INSTANCES
    anatomy = Anatomy(prj_name)
    version_ids = [v["_id"] for v in versions]
    representations = list(get_representations(prj_name, version_ids=version_ids))
    files = [f for r in representations for f in r["files"]]
    file_paths = [f["path"] for f in files]

    # START DELETION
    # 1.- delete files in drive
    for f in file_paths:
        try:
            f = anatomy.fill_root(f)
            logger.info(f"Removing file {f}")
            if test_run: continue
            remove(f)  # remove file
        except Exception as e:
            logger.warning(f"Failed to remove file {f} because of: {str(e)}")
    else:
        try:
            rmdir(Path(f).parent.as_posix())  # remove parent (folder called v###)
        except Exception as e:
            ...

    session = OperationsSession()

    # 2. delete repres
    for repre in representations:
        repre_id = repre["_id"]
        logger.info(f"Removing representation (id={repre_id}, name={repre['name']})")
        if test_run: continue
        session.delete_entity(prj_name, "representation", repre_id)

    # 3. delete subset if all varsions will be removed
    for subset in get_subsets(prj_name, subset_ids=[v["parent"] for v in versions]):
        # pprint(subset)
        subset_ver = get_versions(prj_name, subset_ids=[subset["_id"]])
        subset_versions = list(v["_id"] for v in subset_ver)
        # check if all versions in subset are marked for deletion
        if all(v in version_ids for v in subset_versions):
            logger.info(f"Removing subset (id={subset['_id']}, name={subset['name']})")
            if test_run: continue
            session.delete_entity(prj_name, "subset", subset["_id"])

    # 4. remove versions
    for version_id in version_ids:
        logger.info(f"Removing version (id={version_id})")
        if test_run: continue
        session.delete_entity(prj_name, "version", version_id)

    # TODO: verions has "source" which is a work source that we can explicitly remove

    session.commit()

def ttd_remove_ayon_versions(prj_name: str, version: List[dict]):
    raise NotImplementedError("This function has not yet been implemented in Ayon.")

def ttd_remove_versions(prj_name: str, versions: List[dict]):
    try:
        get_products
        logger.debug(f"Ayon detected.")
    except NameError as e:
        logger.debug(f"OpenPype detected.")
        ttd_remove_op_versions(prj_name, versions)
    else:
        ttd_remove_ayon_versions(prj_name, versions)

def delete_versions(versions: List[AssetVersion]):
    op_versions = list()
    for version in versions:
        prj = version["project"]["full_name"]
        version_parent = version["asset"]["parent"]
        asset_mongo_id = version_parent["custom_attributes"]["avalon_mongo_id"]
        subset_name = version["asset"]["name"]
        version_number = version["version"]

        in_links = list(version["incoming_links"])
        if in_links:
            version_parent = in_links[0]["from"]["asset"]["parent"]
            asset_mongo_id = version_parent["custom_attributes"]["avalon_mongo_id"]
        op_version = get_op_version_from_ftrack_assetversion(
            prj, asset_mongo_id, subset_name, version_number
        )
        if op_version is None:
            continue
        op_versions.append(op_version)

    ttd_remove_versions(prj, op_versions)


class DeleteVersionAction(BaseAction):

    identifier = 'ttd.delete_version.action'
    label = 'Delete Version'
    description = 'Delete version action'
    priority = 10000
    # role_list = ['Pypeclub']
    icon = statics_icon("ftrack", "action_icons", "DeleteVersion.png")
    query_items = "asset.parent, asset.parent.custom_attributes, asset.name, version"


    def __init__(self, *args, **kwargs):
        i = super().__init__(*args, **kwargs)
        global logger
        logger = self.log
        return i


    def discover(self, session: Session, entities: List[Entity], event: Event):
        if len(entities) != 1:
            return False
        elif not isinstance(entities[0], AssetVersion):
            return False
        return True

    def launch(self, session: Session, entities: List[Entity], event: Event):
        # from pprint import pprint
        entity = entities[0]
        self.log.info(f"Working on version {entity['asset']['name']}")
        in_links = list(entity["incoming_links"])
        out_links = list(entity["outgoing_links"])

        if out_links and in_links:
            msg = f"Incoming and outgoing links should not happen simultaneously."
            raise NotImplementedError(msg)

        version_ids = [entity["id"]]

        if in_links:  # is delivery version
            self.log.info("Working on delivery version")

            # clear source attributes
            in_links[0]["from"]["custom_attributes"]["client_version_string"] = ""

        elif out_links:  # has delivery version
            self.log.info("Working on normal version with delivery attached")

            # fetch delivery version and mark it for deletion
            delivery_version = out_links[0]["to"]
            version_ids.append(delivery_version["id"])

        else:  # just remove this one, don't do anything else
            self.log.info("Working on normal version")

        ids = ", ".join(version_ids)
        query = f"select {self.query_items} from AssetVersion where id in ({ids})"
        versions_to_delete = self.session.query(query).all()
        delete_versions(versions_to_delete)
        for version in versions_to_delete:
            session.delete(version)
        session.commit()
        msg = f"Removing versions {[v['id'] for v in versions_to_delete]}"
        return { "success" : True, "message" : msg}



def register(session):
    # if session.server_url == "https://testing-22dogs.ftrackapp.com":
    #     DeleteVersionAction(session).register()
    DeleteVersionAction(session).register()
