from __future__ import annotations
from typing import List
import os
import copy
import json
import collections
from pathlib import Path
from logging import getLogger
from datetime import datetime

logger = getLogger(__name__)

from openpype.client import (
    get_project,
    get_assets,
    get_subsets,
    get_versions,
    get_representations
)
# from openpype.settings import get_project_settings
from openpype.settings import get_project_settings
from openpype_modules.ftrack.lib import BaseAction, statics_icon, create_list # type: ignore
from openpype_modules.ftrack.lib.avalon_sync import CUST_ATTR_ID_KEY # type: ignore
from openpype_modules.ftrack.lib.custom_attributes import ( # type: ignore
    query_custom_attributes
)
from openpype.lib.dateutils import get_datetime_data
from openpype.pipeline import Anatomy
from openpype.settings import get_anatomy_settings
from openpype.pipeline.load import get_representation_path_with_anatomy
from openpype.pipeline.delivery import (
    get_format_dict,
    check_destination_path,
    deliver_single_file,
    deliver_sequence,
)
from ftrack_api import Session
from ftrack_api.entity.base import Entity

from openpype.modules.ftrack.event_handlers_user.action_ttd_delete_version import get_op_version_from_ftrack_assetversion


class Delivery(BaseAction):
    identifier = "delivery.action"
    label = "Delivery"
    description = "Deliver data to client"
    role_list = ["Pypeclub", "Administrator", "Project manager"]
    icon = statics_icon("ftrack", "action_icons", "Delivery.png")
    settings_key = "delivery_action"

    def discover(self, session, entities, event):
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() in ("assetversion", "reviewsession", "assetversionlist"):
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def interface(self, session, entities, event):
        if event["data"].get("values", {}):
            return
        
        self.action_settings = self.get_ftrack_settings(
            session, event, entities)["user_handlers"]["delivery_action"]

        title = "Delivery data to Client"

        items = []
        item_splitter = {"type": "label", "value": "---"}

        project_entity = self.get_project_from_entity(entities[0])
        project_name = project_entity["full_name"]
        project_doc = get_project(project_name, fields=["name"])
        if not project_doc:
            return {
                "success": False,
                "message": (
                    "Didn't found project \"{}\" in avalon."
                ).format(project_name)
            }

        repre_names = self._get_repre_names(project_name, session, entities)

        items.append({
            "type": "hidden",
            "name": "__project_name__",
            "value": project_name
        })

        # Prepare anatomy data
        anatomy = Anatomy(project_name)
        new_anatomies = []
        first = None
        # get delivery templates from delivery settings
        for key, template in (self.action_settings.get("delivery_templates") or {}).items():
            # Use only keys with `{root}` or `{root[*]}` in value
            if isinstance(template, str) and "{root" in template:
                new_anatomies.append(key)
                if first is None:
                    first = key
        # get delivery templates from anatomy
        for key, template in (anatomy.templates.get("delivery") or {}).items():
            # Use only keys with `{root}` or `{root[*]}` in value
            if isinstance(template, str) and "{root" in template:
                new_anatomies.append(key)
                if first is None:
                    first = key

        # prune duplicate names
        new_anatomies = list(sorted(set(new_anatomies)))
        # convert list items into dicts for interface use 
        for idx, key in enumerate(copy.deepcopy(new_anatomies)):
            new_anatomies[idx] = {
                "label": key,
                "value": key
            }

        skipped = False
        # Add message if there are any common components
        if not repre_names or not new_anatomies:
            skipped = True
            items.append({
                "type": "label",
                "value": "<h1>Something went wrong:</h1>"
            })

        items.append({
            "type": "hidden",
            "name": "__skipped__",
            "value": skipped
        })

        if not repre_names:
            if len(entities) == 1:
                items.append({
                    "type": "label",
                    "value": (
                        "- Selected entity doesn't have components to deliver."
                    )
                })
            else:
                items.append({
                    "type": "label",
                    "value": (
                        "- Selected entities don't have common components."
                    )
                })

        # Add message if delivery anatomies are not set
        if not new_anatomies:
            items.append({
                "type": "label",
                "value": (
                    "- `\"delivery\"` anatomy key is not set in config."
                )
            })

        # Skip if there are any data shortcomings
        if skipped:
            return {
                "items": items,
                "title": title
            }

        items.append({
            "value": "<h1>Choose Delivery Template</h1>",
            "type": "label"
        })

        items.append({
            "type": "enumerator",
            "name": "__new_anatomies__",
            "data": new_anatomies,
            "value": first
        })

        items.append({
            "value": "<h1>Choose Components</h1>",
            "type": "label"
        })

        for repre_name in repre_names:
            items.append({
                "type": "boolean",
                "value": False,
                "label": repre_name,
                "name": repre_name
            })

        items.append({
            "value": "<br><h2><i>Override root location</i></h2>",
            "type": "label"
        })

        items.append({
            "type": "text",
            "name": "__location_path__",
            "empty_text": "Type root location path here...(Optional)"
        })

        if entities[0].entity_type.lower() == "assetversionlist":
            items.append({
                "value": "<br><h2><i>Create Optional Review Session</i></h2>",
                "type": "label"
            })
            items.append({
                "type": "boolean",
                "value": False,
                "label": "Create ReviewSession",
                "name": "create_review_session"
            })

        return {
            "items": items,
            "title": title,
            "type": "form",
            "submit_button_label": "Deliver",
            "width": 500,
            "height": 750
        }

    def _get_repre_names(self, project_name, session, entities):
        version_ids = self._get_interest_version_ids(
            project_name, session, entities
        )
        if not version_ids:
            return []
        repre_docs = get_representations(
            project_name,
            version_ids=version_ids,
            fields=["name"]
        )
        repre_names = {repre_doc["name"] for repre_doc in repre_docs}
        return list(sorted(repre_names))

    def _get_interest_version_ids(self, project_name, session, entities):
        # Extract AssetVersion entities
        asset_versions = self._extract_asset_versions(session, entities)
        # Prepare Asset ids
        asset_ids = [
            asset_version["asset_id"]
            for asset_version in asset_versions if not asset_version["incoming_links"]
        ]
        asset_ids.extend([
            asset_version["incoming_links"][0]["from"]["asset_id"]
            for asset_version in asset_versions if asset_version["incoming_links"]
        ])
        asset_ids = set(asset_ids)
        if not asset_ids:
            raise ValueError(f"Failed to find asset_ids for versions {[e['id'] for e in entities]}")
        # Query Asset entities
        assets = session.query((
            "select id, name, context_id from Asset where id in ({})"
        ).format(self.join_query_keys(asset_ids))).all()
        assets_by_id = {
            asset["id"]: asset
            for asset in assets
        }
        parent_ids = set()
        subset_names = set()
        version_nums = set()
        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            if asset_version["incoming_links"]:
                asset_id = asset_version["incoming_links"][0]["from"]["asset_id"]
            asset = assets_by_id[asset_id]
            subset_realname = asset_version["custom_attributes"].get("subset")
            if not subset_realname:
                subset_realname = asset["name"]
            parent_ids.add(asset["context_id"])
            subset_names.add(subset_realname)
            version_nums.add(asset_version["version"])

        asset_docs_by_ftrack_id = self._get_asset_docs(
            project_name, session, parent_ids
        )
        subset_docs = self._get_subset_docs(
            project_name,
            asset_docs_by_ftrack_id,
            subset_names,
            asset_versions,
            assets_by_id
        )
        version_docs = self._get_version_docs(
            project_name,
            asset_docs_by_ftrack_id,
            subset_docs,
            version_nums,
            asset_versions,
            assets_by_id
        )

        return [version_doc["_id"] for version_doc in version_docs]

    def _extract_asset_versions(self, session, entities):
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

        return asset_versions

        # filtered_ver = list()
        # for version in asset_versions:
        #     if version["outgoing_links"]:
        #         version_ = version["outgoing_links"][0]["to"]
        #         self.log.info(f"Using delivery version {version_} instead of {version}")
        #         version = version_
        #     filtered_ver.append(version)

        # return filtered_ver


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


    def _get_version_docs(
        self,
        project_name,
        asset_docs_by_ftrack_id,
        subset_docs,
        version_nums,
        asset_versions,
        assets_by_id
    ):
        subset_docs_by_id = {
            subset_doc["_id"]: subset_doc
            for subset_doc in subset_docs
        }
        version_docs = list(get_versions(
            project_name,
            subset_ids=subset_docs_by_id.keys(),
            versions=version_nums
        ))
        version_docs_by_parent_id = collections.defaultdict(dict)
        for version_doc in version_docs:
            subset_doc = subset_docs_by_id[version_doc["parent"]]

            asset_id = subset_doc["parent"]
            subset_name = subset_doc["name"]
            version = version_doc["name"]
            if version_docs_by_parent_id[asset_id].get(subset_name) is None:
                version_docs_by_parent_id[asset_id][subset_name] = {}

            version_docs_by_parent_id[asset_id][subset_name][version] = (
                version_doc
            )

        filtered_versions = []
        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            if asset_version["incoming_links"]:
                asset_id = asset_version["incoming_links"][0]["from"]["asset_id"]
            asset = assets_by_id[asset_id]
            parent_id = asset["context_id"]
            asset_doc = asset_docs_by_ftrack_id.get(parent_id)
            if not asset_doc:
                continue

            subsets_by_name = version_docs_by_parent_id.get(asset_doc["_id"])
            if not subsets_by_name:
                continue

            subset_realname = asset_version["custom_attributes"].get("subset")
            if not subset_realname:
                subset_realname = asset["name"]
            version_docs_by_version = subsets_by_name.get(subset_realname)
            if not version_docs_by_version:
                continue

            version = asset_version["version"]
            version_doc = version_docs_by_version.get(version)
            if version_doc:
                filtered_versions.append(version_doc)
        return filtered_versions

    def _get_subset_docs(
        self,
        project_name,
        asset_docs_by_ftrack_id,
        subset_names,
        asset_versions,
        assets_by_id
    ):
        asset_doc_ids = [
            asset_doc["_id"]
            for asset_doc in asset_docs_by_ftrack_id.values()
        ]
        subset_docs = list(get_subsets(
            project_name,
            asset_ids=asset_doc_ids,
            subset_names=subset_names
        ))
        subset_docs_by_parent_id = collections.defaultdict(dict)
        for subset_doc in subset_docs:
            asset_id = subset_doc["parent"]
            subset_name = subset_doc["name"]
            subset_docs_by_parent_id[asset_id][subset_name] = subset_doc

        filtered_subsets = []
        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            if asset_version["incoming_links"]:
                asset_id = asset_version["incoming_links"][0]["from"]["asset_id"]
            asset = assets_by_id[asset_id]

            parent_id = asset["context_id"]
            asset_doc = asset_docs_by_ftrack_id.get(parent_id)
            if not asset_doc:
                continue

            subsets_by_name = subset_docs_by_parent_id.get(asset_doc["_id"])
            if not subsets_by_name:
                continue

            subset_realname = asset_version["custom_attributes"].get("subset")
            if not subset_realname:
                subset_realname = asset["name"]
            subset_doc = subsets_by_name.get(subset_realname)
            if subset_doc:
                filtered_subsets.append(subset_doc)
        return filtered_subsets

    def _get_asset_docs(self, project_name, session, parent_ids):
        asset_docs = list(get_assets(
            project_name, fields=["_id", "name", "data.ftrackId"]
        ))

        asset_docs_by_id = {}
        asset_docs_by_name = {}
        asset_docs_by_ftrack_id = {}
        for asset_doc in asset_docs:
            asset_id = str(asset_doc["_id"])
            asset_name = asset_doc["name"]
            ftrack_id = asset_doc["data"].get("ftrackId")

            asset_docs_by_id[asset_id] = asset_doc
            asset_docs_by_name[asset_name] = asset_doc
            if ftrack_id:
                asset_docs_by_ftrack_id[ftrack_id] = asset_doc

        attr_def = session.query((
            "select id from CustomAttributeConfiguration where key is \"{}\""
        ).format(CUST_ATTR_ID_KEY)).first()
        if attr_def is None:
            return asset_docs_by_ftrack_id

        avalon_mongo_id_values = query_custom_attributes(
            session, [attr_def["id"]], parent_ids, True
        )
        missing_ids = set(parent_ids)
        for item in avalon_mongo_id_values:
            if not item["value"]:
                continue
            asset_id = item["value"]
            entity_id = item["entity_id"]
            asset_doc = asset_docs_by_id.get(asset_id)
            if asset_doc:
                asset_docs_by_ftrack_id[entity_id] = asset_doc
                missing_ids.remove(entity_id)

        entity_ids_by_name = {}
        if missing_ids:
            not_found_entities = session.query((
                "select id, name from TypedContext where id in ({})"
            ).format(self.join_query_keys(missing_ids))).all()
            entity_ids_by_name = {
                entity["name"]: entity["id"]
                for entity in not_found_entities
            }

        for asset_name, entity_id in entity_ids_by_name.items():
            asset_doc = asset_docs_by_name.get(asset_name)
            if asset_doc:
                asset_docs_by_ftrack_id[entity_id] = asset_doc

        return asset_docs_by_ftrack_id

    def launch(self, session, entities, event):
        if "values" not in event["data"]:
            return {
                "success": True,
                "message": "Nothing to do"
            }

        values = event["data"]["values"]
        skipped = values.pop("__skipped__")
        if skipped:
            return {
                "success": False,
                "message": "Action skipped"
            }

        user_id = event["source"]["user"]["id"]
        user_entity = session.query(
            "User where id is {}".format(user_id)
        ).one()

        job = session.create("Job", {
            "user": user_entity,
            "status": "running",
            "data": json.dumps({
                "description": "Delivery processing."
            })
        })
        session.commit()

        try:
            report = self.real_launch(session, entities, event)

        except Exception as exc:
            report = {
                "success": False,
                "title": "Delivery failed",
                "items": [{
                    "type": "label",
                    "value": (
                        "Error during delivery action process:<br>{}"
                        "<br><br>Check logs for more information."
                    ).format(str(exc))
                }]
            }
            self.log.warning(
                "Failed during processing delivery action.",
                exc_info=True
            )

        finally:
            if report["success"]:
                job["status"] = "done"
            else:
                job["status"] = "failed"
            session.commit()

        if not report["success"]:
            self.show_interface(
                items=report["items"],
                title=report["title"],
                event=event
            )
            return {
                "success": False,
                "message": "Errors during delivery process. See report."
            }

        return report

    def generate_version_by_repre_id_dict(self, session: Session, entities: List[Entity]):
        # reset "AssetVersion" custom attribute "disk_file_location"
        version_by_repre_id = dict()
        ftrack_asset_versions = self._extract_asset_versions(session, entities)
        for v in ftrack_asset_versions:
            print(f"Looking for repre mapping for version {v}")
            project_name = v["project"]["full_name"]
            asset_mongo_id = v["asset"]["parent"]["custom_attributes"]["avalon_mongo_id"]
            in_links = list(v["incoming_links"])

            if in_links:
                # v = in_links[0]
                version_parent = in_links[0]["from"]["asset"]["parent"]
                asset_mongo_id = version_parent["custom_attributes"]["avalon_mongo_id"]

            subset_name = v["asset"]["name"]
            version_number = v["version"]
            op_v = get_op_version_from_ftrack_assetversion(
                project_name, asset_mongo_id, subset_name, version_number)
            if not op_v:
                print(f"Failed to find OP version for v {v}")
                continue

            version_id = op_v["_id"]
            representations = list(get_representations(
                project_name, version_ids=[version_id]))
    
            for r in representations:
                print(f"Adding {r['_id']}{v} for repre-version dict")
                version_by_repre_id[r["_id"]] = v
        
        return version_by_repre_id

    def real_launch(self, session, entities, event):
        self.log.info("Delivery action just started.")
        report_items = collections.defaultdict(list)

        values: dict = event["data"]["values"]

        location_path = values.pop("__location_path__")
        anatomy_name = values.pop("__new_anatomies__")
        project_name = values.pop("__project_name__")

        # if launched from list retrieve the list name to fill
        # up template values
        ftrack_list_name = None
        if entities[0].entity_type == "AssetVersionList":
            ftrack_list_name = entities[0]["name"]

        collected_paths = []
        collected_repres = []

        repre_names = []
        for key, value in values.items():
            if value is True:
                repre_names.append(key)

        if not repre_names:
            return {
                "success": True,
                "message": "No selected components to deliver."
            }

        location_path = location_path.strip()
        if location_path:
            location_path = os.path.normpath(location_path)
            if not os.path.exists(location_path):
                os.makedirs(location_path)

        self.log.debug("Collecting representations to process.")
        version_ids = self._get_interest_version_ids(
            project_name, session, entities
        )
        repres_to_deliver = list(get_representations(
            project_name,
            representation_names=repre_names,
            version_ids=version_ids
        ))

        version_by_repre_id = self.generate_version_by_repre_id_dict(session, entities)

        for v in set(version_by_repre_id.values()):
            print(f"Reseting custom attributes values for version {v}")
            v["custom_attributes"]["delivery_name"] = ""

        assert version_by_repre_id != dict()

        anatomy = Anatomy(project_name)


        # Add and/or override Anatomy Delivery templates based on
        # Delivery action settings. These settings can also feature more
        # templating patterns coming from ftrack such as '{ftrack[listname]}'
        template_override = self.action_settings["delivery_templates"].get(anatomy_name, None)
        if template_override:
            raw_anatomy_templates = get_anatomy_settings(project_name)["templates"]
            raw_anatomy_templates["delivery"][anatomy_name] = template_override
            anatomy.templates["delivery"][anatomy_name] = template_override
            anatomy.templates_obj.set_templates(raw_anatomy_templates)
        
        # add ftrack custom template keys for path resolving
        # TODO: put more data and expand templating items
        ftrack_template_data = {
            "ftrack": {
                "listname": ftrack_list_name,
                "username": session.api_user,
                # "first_name": event["user"]["first_name"],
                # "last_name": event["user"]["last_name"]
            }
        }
        if entities[0].entity_type == "AssetVersionList":
            ftrack_template_data["category"] = entities[0]["category"]["name"]

        format_dict = get_format_dict(anatomy, location_path)
        datetime_data = get_datetime_data()

        attr_by_version = dict()

        for repre in repres_to_deliver:
            source_path = repre.get("data", {}).get("path")
            debug_msg = "Processing representation {}".format(repre["_id"])
            if source_path:
                debug_msg += " with published path {}.".format(source_path)
            self.log.debug(debug_msg)

            anatomy_data = copy.deepcopy(repre["context"])

            # update anatomy_data with ftrack template data
            if ftrack_list_name:
                anatomy_data.update(ftrack_template_data)
            
            repre_report_items = check_destination_path(repre["_id"],
                                                        anatomy,
                                                        anatomy_data,
                                                        datetime_data,
                                                        anatomy_name)

            if repre_report_items:
                report_items.update(repre_report_items)
                continue

            # Get source repre path
            frame = repre['context'].get('frame')

            if frame:
                repre["context"]["frame"] = len(str(frame)) * "#"

            repre_path = get_representation_path_with_anatomy(repre, anatomy)
            # TODO add backup solution where root of path from component
            # is replaced with root
            args = (
                repre_path,
                repre,
                anatomy,
                anatomy_name,
                anatomy_data,
                format_dict,
                report_items,
                self.log
            )
            if not frame:
                r, success = deliver_single_file(*args)
            else:
                r, success = deliver_sequence(*args)

            # print(r, success)

            if not success:
                # print(f"Not success")
                continue


            anatomy_filled = anatomy.format_all(anatomy_data)
            dest_path = anatomy_filled["delivery"][anatomy_name]
            
            collected_repres.append(repre["name"])
            collected_paths.append(dest_path)

            try:
                version = version_by_repre_id[repre["_id"]]
                if version["id"] not in attr_by_version:
                    attr_by_version[version["id"]] = {"attr":"", "entity": version}

                files = "\n".join([Path(f).name for f in r["created_files"][-1:]])
                # c_attr = version["custom_attributes"]["disk_file_location"]
                # print(f"Updating custom attr from {c_attr} to {files}")
                attr_by_version[version["id"]]["attr"] += files +"\n\n"
                print(f"Adding files {files}")
            except Exception as e:
                print(f"Failed to update version for representation {repre['_id']} due to {e}")
                print(f"valid ids are: {version_by_repre_id.keys()}")


        from pprint import pprint

        pprint(attr_by_version.items())

        for id_, value in attr_by_version.items():
            value["entity"]["custom_attributes"]["delivery_name"] = value["attr"]


        report_items.pop("created_files") # removes false positive
        # get final path of repre to be used for attributes
        # and fill custom attributes on list

            
        if entities[0].entity_type.lower() == "assetversionlist":
            entities[0]["custom_attributes"]["delivery_package_name"] = ftrack_list_name
            entities[0]["custom_attributes"]["delivery_type"] = ", ".join(list(set(collected_repres)))
            entities[0]["custom_attributes"]["delivery_package_path"] = os.path.commonpath(collected_paths)
            create_list(
                session,
                entities,
                event,
                client_review = values["create_review_session"],
                list_name = ftrack_list_name,
                list_category_name = entities[0]["category"]["name"],
                log = self.log
            )

        session.commit()
        return self.report(report_items)

    def report(self, report_items):
        """Returns dict with final status of delivery (succes, fail etc.)."""
        items = []

        for msg, _items in report_items.items():
            if not _items:
                continue

            if items:
                items.append({"type": "label", "value": "---"})

            items.append({
                "type": "label",
                "value": "# {}".format(msg)
            })
            if not isinstance(_items, (list, tuple)):
                _items = [_items]
            __items = []
            for item in _items:
                __items.append(str(item))

            items.append({
                "type": "label",
                "value": '<p>{}</p>'.format("<br>".join(__items))
            })

        if not items:
            return {
                "success": True,
                "message": "Delivery Finished"
            }

        return {
            "items": items,
            "title": "Delivery report",
            "success": False
        }


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    Delivery(session).register()
