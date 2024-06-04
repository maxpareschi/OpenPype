from __future__ import annotations
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
            logger.info("Removing file {}".format(f))
            if test_run: continue
            remove(f)  # remove file
        except Exception as e:
            logger.warning("Failed to remove file {} because of: {}".format(f, str(e)))
    else:
        try:
            rmdir(Path(f).parent.as_posix())  # remove parent (folder called v###)
        except Exception as e:
            ...

    session = OperationsSession()

    # 2. delete repres
    for repre in representations:
        repre_id = repre["_id"]
        logger.info("Removing representation (id={}, name={})".format(
            repre_id,
            repre["name"]
        ))

        if test_run: continue
        session.delete_entity(prj_name, "representation", repre_id)

    # 3. delete subset if all varsions will be removed
    for subset in get_subsets(prj_name, subset_ids=[v["parent"] for v in versions]):
        # pprint(subset)
        subset_ver = get_versions(prj_name, subset_ids=[subset["_id"]])
        subset_versions = list(v["_id"] for v in subset_ver)
        # check if all versions in subset are marked for deletion
        if all(v in version_ids for v in subset_versions):
            logger.info("Removing subset (id={}, name={})".format(
                subset["_id"],
                subset["name"]
            ))
            if test_run: continue
            session.delete_entity(prj_name, "subset", subset["_id"])

    # 4. remove versions
    for version_id in version_ids:
        logger.info("Removing version (id={})".format(version_id))
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
        subset_name = version["custom_attributes"]["subset"]
        if not subset_name:
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
    settings_key = "delivery_action"


    def __init__(self, *args, **kwargs):
        i = super().__init__(*args, **kwargs)
        global logger
        logger = self.log
        return i

    def discover(self, session: Session, entities: List[Entity], event: Event):
        is_valid = False
        for entity in entities:
            if entity.entity_type in (
                "AssetVersion",
                "ReviewSession",
                "AssetVersionList"
            ):
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        self.versions = None
        return is_valid

    def get_interface(self):
        gui = [
            {
                "type":"label",
                "value": "<h2><b>WARNING</b>: The following versions will be removed from Ftrack and OpenPype.<br>"
                "If you want to cancel, press the <b>X</b> button in the upper right corner.</h2>"
            },
            {
                "type": "hidden",
                "name":"hidden",
                "value": True
            }
        ]
        vlist = "<ul>"
        for v in self.versions:
            name = "<h3>{} - {} - {}</h3>".format(
                v['asset']['parent']['name'],
                v['asset']['name'],
                v['version']
            )
            vlist += "<li>{}</i>".format(name)
        gui.append({
            "type":"label",
            "value": vlist + "</ul>"
        })

        return gui

    def launch(self, session: Session, entities: List[Entity], event: Event):
        # from pprint import pprint
        self.versions = self.versions or self._extract_asset_versions(session, entities)

        if not event["data"].get("values", {}):
            return {
                "type": "form",
                "items": self.get_interface(),
                "title": "Confirm",
                "submit_button_label": "I know what I'm doing. <b>Remove permanently.</b>"
            }

        for entity in self.versions:
            self.log.info(f"Working on version {entity['asset']['name']}")
            in_links = list(entity["incoming_links"])
            out_links = list(entity["outgoing_links"])

            if out_links and in_links:
                msg = "Incoming and outgoing links should not happen simultaneously."
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
            query = "select {} from AssetVersion where id in ({})".format(
                self.query_items,
                ids
            )
            versions_to_delete = self.session.query(query).all()
            if not versions_to_delete:
                continue
            delete_versions(versions_to_delete)
            for version in versions_to_delete:
                session.delete(version)
            session.commit()
            # msg = f"Removing versions {[v['id'] for v in versions_to_delete]}"
        return { "success" : True, "message" : "Versions removed correctly."}

    def _extract_asset_versions(self, session: Session, entities: List[Entity]):
        asset_version_ids = set()
        review_session_ids = set()
        asset_version_list_ids = set()

        for entity in entities:
            entity_type_low = entity.entity_type.lower()

            if entity_type_low == "assetversion":
                asset_version_ids.add(entity["id"])

            elif entity_type_low == "reviewsession":
                review_session_ids.add(entity["id"])

            elif entity_type_low == "assetversionlist":
                asset_version_list_ids.add(entity["id"])

        for version_id in self._get_asset_version_ids_from_asset_ver_list(
            session, asset_version_list_ids
        ):
            asset_version_ids.add(version_id)

        for version_id in self._get_asset_version_ids_from_review_sessions(
            session, review_session_ids
        ):
            asset_version_ids.add(version_id)

        qkeys = self.join_query_keys(asset_version_ids)
        query = "select id, version, asset_id, incoming_links, outgoing_links"
        query += " from AssetVersion where id in ({})".format(qkeys)

        asset_versions = session.query(query).all()
        for version in [*asset_versions]:
            if version["outgoing_links"] and not version["incoming_links"]:
                delivery = version["outgoing_links"][0]["to"]
                if delivery in asset_versions:
                    continue
                asset_versions.append(delivery)
                self.log.info("Appending version source {}".format(delivery))

        return asset_versions

    def _get_asset_version_ids_from_review_sessions(
        self, session, review_session_ids
    ):
        if not review_session_ids:
            return set()
        review_session_objects = session.query((
            "select version_id from ReviewSessionObject"
            " where review_session_id in ({})"
        ).format(self.join_query_keys(review_session_ids))).all()

        return {
            review_session_object["version_id"]
            for review_session_object in review_session_objects
        }

    def _get_asset_version_ids_from_asset_ver_list( self, session, asset_ver_list_ids):
        # this can be static method..
        if not asset_ver_list_ids:
            return set()

        ids = ", ".join(asset_ver_list_ids)
        query_str = "select id from AssetVersion where lists any (id in ({}}))".format(ids)
        asset_versions = session.query(query_str).all()

        return {asset_version["id"] for asset_version in asset_versions}


def register(session):
    # if session.server_url == "https://testing-22dogs.ftrackapp.com":
    #     DeleteVersionAction(session).register()
    DeleteVersionAction(session).register()
