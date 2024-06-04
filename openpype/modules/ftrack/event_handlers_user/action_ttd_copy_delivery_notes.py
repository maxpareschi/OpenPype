from __future__ import annotations
from typing import List
from logging import getLogger

logger = getLogger(__name__)

from openpype_modules.ftrack.lib import BaseAction, statics_icon, create_notes # type: ignore
from ftrack_api import Session
from ftrack_api.entity.base import Entity
from ftrack_api.entity.asset_version import AssetVersion
from ftrack_api.event.base import Event

class CopyDeliveryNotes(BaseAction):
    """Action for forwarding client notes from delivery version into source."""

    identifier = 'ttd.copy.notes.action'
    label = 'Forward Delivery Notes'
    description = 'Forward client notes from delivery to source.'
    icon = statics_icon("ftrack", "action_icons", "ForwardNotes.png")
    settings_key = "delivery_action"
    note_tag = "Client Feedback"

    def discover(self, session: Session, entities: List[Entity], event: Event):
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() in ("assetversion", "reviewsession", "assetversionlist"):
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def copy_client_notes(self, session: Session, versions: List[AssetVersion]):
        
        versions_ids = f"({', '.join([v['id'] for v in versions])})"
        versions_by_id = {v["id"]: v for v in versions}

        select = "select author, category, content, in_reply_to, category.name, parent_id"
        where = f"where parent_id in {versions_ids} and in_reply_to is None and category.name is \"{self.note_tag}\""

        notes = session.query(f"{select} from Note {where}").all()

        msg = ""

        for note in notes:
            # if note["category"]["name"] != "For Client":
            #     print(f"Warning, note is marked as something else appart from 'For Client'")

            version = versions_by_id[note["parent_id"]]
            in_links = list(version["incoming_links"])
            out_links = list(version["outgoing_links"])
        
            if not out_links and not in_links:
                self.log.info(f"Skipping version {version}. No links found.")
                continue
        
            elif in_links and out_links:
                self.log.info(f"Skipping version {version}. Too many links found.")
                continue
        
            elif out_links and not in_links:
                self.log.info(f"Skipping version {version}. This note is already copied.")
                continue
            
            # target is the source version and source is the delivery version
            target = list(version["incoming_links"])[0]["from"]
            source = version
            self.log.info(f"Working on note {note} from version {source}")
    
            notes_in_target = [n for n in notes if n["parent_id"] == target["id"]]
    
            self.log.info(f"Target notes are: {notes_in_target}")

            msg += create_notes(session, note, target, notes_in_target)

        session.commit()
        return msg


    def launch(self, session: Session, entities: List[Entity], event: Event):
        session = Session(plugin_paths=[])
        self.log.info(event)
        versions = self._extract_asset_versions(session, entities)
        for version in versions:
            if list(version["incoming_links"]) or list(version["outgoing_links"]):
                break
        else:
            return {"success": False, "message": "No deliveries found in selection."}
        msg = self.copy_client_notes(session, versions)
        return {"success" : True, "message": msg or "No new notes where created."}

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
        query += f" from AssetVersion where id in ({qkeys})"
        asset_versions = session.query(query).all()

        for version in [*asset_versions]:
            if version["outgoing_links"] and not version["incoming_links"]:
                delivery = version["outgoing_links"][0]["to"]
                if delivery in asset_versions:
                    continue
                asset_versions.append(delivery)
                self.log.info(f"Appending version source {delivery}")
            elif version["incoming_links"]:
                source = version["incoming_links"][0]["from"]
                if source in asset_versions:
                    continue
                asset_versions.append(source)
                self.log.info(f"Appending version source {source}")

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
        query_str = f"select id from AssetVersion where lists any (id in ({ids}))"
        asset_versions = session.query(query_str).all()

        return {asset_version["id"] for asset_version in asset_versions}

def register(session: Session):
    CopyDeliveryNotes(session).register()
