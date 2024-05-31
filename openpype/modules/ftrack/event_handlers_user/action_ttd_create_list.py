import threading
import datetime
import copy
import collections
import ftrack_api

from openpype.lib import get_datetime_data
from datetime import date
from openpype.settings.lib import (
    get_project_settings,
    get_default_project_settings
)
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
from openpype.pipeline.load import get_representation_path_with_anatomy
from openpype.pipeline.delivery import (
    get_format_dict,
    check_destination_path,
    deliver_single_file,
    deliver_sequence,
)


class CreateListAction(BaseAction):
    """Create daily review session object per project.
    """
    identifier = "ttd.create.list"
    label = "Create List"
    description = "Manually create a list from other lists"
    role_list = ["Pypeclub", "Administrator", "Project manager"]
    icon = statics_icon("ftrack", "action_icons", "CreateList.png")

    def discover(self, session, entities, event):
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() in (
                #"assetversion",
                "assetversionlist"
            ):
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid
    
    def interface(self, session, entities, event):
        if event['data'].get('values', {}):
            return
        
        entity_type = entities[0].entity_type

        self.list_types = []
        for listcat in session.query("select name, id from ListCategory").all():
            self.list_types.append({"id": listcat["id"], "name": listcat["name"]})

        today = date.today()
        formatted_date = today.strftime("%Y%m%d")

        items = [{
            "type": "label",
            "value": "<h1><b>Lists Options:</b></h1>"
        }]

        enum_data = []
        for list_type in self.list_types:
            enum_data.append({
                "label": list_type["name"],
                "value": list_type["name"]
            })

        category_name = enum_data[0]["value"]
        list_name = formatted_date + "_delivery_submission_list"
        for lt in self.list_types:
            if lt["name"] == "Delivery":
                category_name = "Delivery"
        

        if entity_type == "AssetVersion":
            entity_list_name = entities[0]["lists"][-1]["name"]
            entity_list_category = entities[0]["lists"][-1]["category"]["name"]
        elif entity_type == "AssetVersionList":
            entity_list_name = entities[0]["name"]
            entity_list_category = entities[0]["category"]["name"]

        if entity_list_name:
            list_name = entity_list_name

        if entity_list_category:
            category_name = entity_list_category
        
        items.extend(
            [   
                {
                    "type": "label",
                    "value": "<b>List Name (can be changed here)</b>"
                },
                {
                    "label": "List Name",
                    "type": "text",
                    "name": "list_name",
                    "value": list_name
                },
                {
                    "type": "label",
                    "value": "---"
                },
                {
                    "type": "label",
                    "value": "Create list based on Gathered versions"
                },
                {
                    "type": "boolean",
                    "value": True,
                    "label": "Prioritize Gathers",
                    "name": "prioritize_gathers"
                },
                {
                    "type": "label",
                    "value": "---"
                },
                {
                    "type": "label",
                    "value": "Create Review List instead of classic list"
                },
                {
                    "type": "boolean",
                    "value": False,
                    "label": "Client Review",
                    "name": "client_review"
                },
                {
                    "type": "label",
                    "value": "---"
                },
                {
                    "type": "label",
                    "value": "'Category' for classic lists or 'Folder' for client review lists."
                },
                {
                    "label": "List Category/Folder",
                    "type": "enumerator",
                    "name": "list_category",
                    "data": enum_data,
                    "value": category_name
                },
            ]
        )
            
        return {
            "type": "form",
            "title": "Create Lists",
            "items": items,
            "submit_button_label": "Create",
            "width": 500,
            "height": 600
        }

    def launch(self, session, entities, event):

        user_values = event["data"].get("values", None)

        if user_values is None:
            return
        
        self.log.info("Sumbitted choices: {}".format(user_values))

        if user_values["client_review"]:
            target_exists = True if session.query("ReviewSession where name is '{}'".format(
                user_values["list_name"]
            )).first() else False
            if target_exists:
                return {"success": False, "message": "Review Session is already present (duplicate name)!"}
        else:
            target_exists = True if session.query("AssetVersionList where name is '{}'".format(
                user_values["list_name"]
            )).first() else False
            if target_exists:
                return {"success": False, "message": "List is already present (duplicate name)!"}

        list_category = session.query("ListCategory where name is '{}'".format(
            user_values["list_category"]
        )).first() or None

        review_session_folder = session.query("ReviewSessionFolder where name is '{}'".format(
            user_values["list_category"]
        )).first() or None

        list_owner = session.query("User where id is '{}'".format(
            event["source"]["user"]["id"]
        )).first() or None

        assetversions = session.query("AssetVersion where lists.id is '{}'".format(
            entities[0]["id"]
        )).all()


        final_assetversions = []
        for av in assetversions:
            self.log.debug("Processing AssetVersion '{} v{}' in source_list".format(
                av["asset"]["name"],
                av["version"]
            ))
            if user_values["prioritize_gathers"]:
                if av["incoming_links"]:
                    self.log.debug("This is already a gathered version, collecting.")
                    final_assetversions.append(av)
                elif av["outgoing_links"]:
                    self.log.debug("This is a source version, collecting '{} v{}' as linked gather.".format(
                        av["outgoing_links"][0]["to"]["asset"]["name"],
                        av["outgoing_links"][0]["to"]["version"]
                    ))
                    final_assetversions.append(
                        av["outgoing_links"][0]["to"]
                    )
                else:
                    self.log.debug("This version has no gather, skipping collection...")
            else:
                self.log.debug("Collecting direct version, skipping gathers if any.")
                final_assetversions.append(av)
 
        if user_values["client_review"]:
            if not review_session_folder:
                review_session_folder = session.create("ReviewSessionFolder", {
                    "project": entities[0]["project"],
                    "name": list_category["name"]
                })
            review_session_data = {
                "project": entities[0]["project"],
                "category": list_category,
                "name": user_values["list_name"]
            }
            if list_owner:
                review_session_data.update({"created_by": list_owner})

            review_session = session.create("ReviewSession", review_session_data)
            review_session_folder["review_sessions"].append(review_session)
            self.log.debug("Created Review Session '{}/{}'".format(
                list_category["name"], user_values["list_name"]))

            for fav in final_assetversions:
                created_review_object = session.create("ReviewSessionObject", {
                    "asset_version": fav,
                    "review_session": review_session,
                    "name": fav["asset"]["name"],
                    "description": fav["comment"],
                    "version": "Version {}".format(str(fav["version"]).zfill(3))
                })
                created_review_object["notes"].extend(
                    [n for n in fav["notes"]]
                )
                self.log.debug("Appended version '{} v{}' to created review session: {}".format(
                    fav["asset"]["name"],
                    fav["version"],
                    created_review_object["name"]
                ))
        
        else:
            list_data = {
                "project": entities[0]["project"],
                "category": list_category,
                "name": user_values["list_name"]
            }
            if list_owner:
                list_data.update({"owner": list_owner})

            created_list = session.create("AssetVersionList", list_data)
            self.log.debug("Created List '{}/{}'".format(
                list_category["name"], user_values["list_name"]))

            created_list["custom_attributes"]["source_list_name"] = entities[0]["name"]
            self.log.debug("Source List is set as '{}'".format(
                created_list["custom_attributes"]["source_list_name"]))

            for fav in final_assetversions:
                created_list["items"].append(fav)
                self.log.debug("Appended version '{} v{}' to created list: {}".format(
                    fav["asset"]["name"],
                    fav["version"],
                    created_list["name"]
                ))

        session.commit()

        return {"success": True, "message": "Creation successful!"}



def register(session):
    '''Register plugin. Called when used as an plugin.'''

    CreateListAction(session).register()
