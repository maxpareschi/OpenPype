import os
import copy
import json
import collections

from openpype.client import (
    get_project,
    get_assets,
    get_subsets,
    get_versions,
    get_representations
)
from openpype_modules.ftrack.lib import BaseAction, statics_icon # type: ignore
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
            if entity.entity_type in (
                "AssetVersion", "ReviewSession", "AssetVersionList"
            ):
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

        title = "Deliver data to Client"

        items = []

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
            "value": "<br><h3><i>Collect Gathers instead of selected versions if they exist:</i></h3>",
            "type": "label"
        })

        items.append({
            "type": "boolean",
            "value": True,
            "label": "Prioritize Gathers",
            "name": "prioritize_gathers"
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
        gather_ids = self._get_interest_version_ids(
            project_name, session, entities, prioritize_gathers = True
        )
        if not version_ids and gather_ids:
            return []
        version_ids.extend(gather_ids)
        repre_docs = get_representations(
            project_name,
            version_ids=version_ids,
            fields=["name"]
        )
        repre_names = [repre_doc["name"] for repre_doc in repre_docs]
        return list(sorted(set(repre_names)))

    def _get_interest_version_ids(self, project_name, session, entities, prioritize_gathers = False):
        # Extract AssetVersion entities
        asset_versions = self._extract_asset_versions(session,
                                                      entities,
                                                      prioritize_gathers = prioritize_gathers)
        # Prepare Asset ids, prioritizing gathers if specified
        if prioritize_gathers:
            asset_ids = [
                asset_version["asset_id"]
                for asset_version in asset_versions if not asset_version["incoming_links"]
            ]
            asset_ids.extend([
                asset_version["incoming_links"][0]["from"]["asset_id"]
                for asset_version in asset_versions if asset_version["incoming_links"]
            ])
        else:
            asset_ids = [asset_version["asset_id"] for asset_version in asset_versions]

        asset_ids = set(asset_ids)
        if not asset_ids:
            raise ValueError("Failed to find asset_ids for versions {}".format([e['id'] for e in entities]))
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

    def _extract_asset_versions(self, session, entities, prioritize_gathers = False):
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

        filtered_ver = list()
        for version in asset_versions:
            if prioritize_gathers:
                if version["outgoing_links"]:
                    version_ = version["outgoing_links"][0]["to"]
                    self.log.info("Using gathered version '{}'".format(version_))
                    version = version_
            else:
                self.log.info("Using version '{}'".format(version))
            filtered_ver.append(version)

        return filtered_ver

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
        query_str = "select id from AssetVersion where lists any (id in ({}))".format(ids)
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
                "message": "Delivery skipped..."
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

    def real_launch(self, session, entities, event):
        self.log.info("Delivery action just started.")
        report_items = collections.defaultdict(list)

        values = event["data"]["values"]

        location_path = values.pop("__location_path__")
        anatomy_name = values.pop("__new_anatomies__")
        project_name = values.pop("__project_name__")

        # Get anatomy settings to fill up extra info (task["short"] if
        # no task is present and override is specified)
        anatomy_settings = get_anatomy_settings(project_name)

        # if launched from list retrieve the list name to fill
        # up template values
        ftrack_list_name = None
        if entities[0].entity_type == "AssetVersionList":
            ftrack_list_name = entities[0]["name"]

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
            project_name, session, entities, prioritize_gathers = values["prioritize_gathers"]
        )
        repres_to_deliver = list(get_representations(
            project_name,
            representation_names=repre_names,
            version_ids=version_ids
        ))
        anatomy = Anatomy(project_name)

        format_dict = get_format_dict(anatomy, location_path)

        datetime_data = get_datetime_data()
        for repre in repres_to_deliver:
            source_path = repre.get("data", {}).get("path")
            debug_msg = "Processing representation {}".format(repre["_id"])
            if source_path:
                debug_msg += " with published path {}.".format(source_path)
            self.log.debug(debug_msg)

            anatomy_data = copy.deepcopy(repre["context"])

            if ftrack_list_name:
                anatomy_data.update({
                    "ftrack": {
                        "listname": ftrack_list_name
                    }
                })
            
            # Add and/or override Anatomy Delivery templates based on
            # Delivery action settings. These settings can also feature more
            # templating patterns coming from ftrack such as '{ftrack[listname]}'
            template_override = self.action_settings["delivery_templates"].get(anatomy_name, None)
            if template_override:
                raw_anatomy_templates = get_anatomy_settings(project_name)["templates"]
                raw_anatomy_templates["delivery"][anatomy_name] = template_override
                anatomy.templates["delivery"][anatomy_name] = template_override
                anatomy.templates_obj.set_templates(raw_anatomy_templates)
            
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

            # Log if the task is not presend to debug templates
            repre_task = repre["context"]["task"]
            if not repre_task.get("type"):
                self.log.debug("No task found in representation, template may fail to resolve!")

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
                deliver_single_file(*args)
            else:
                deliver_sequence(*args)

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
