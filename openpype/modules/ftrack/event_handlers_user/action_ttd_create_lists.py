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


class CreateListsAction(BaseAction):
    """Create daily review session object per project.
    """
    identifier = "ttd.create.lists"
    label = "Create Lists"
    description = "Manually create lists from other lists or selected assetversions"
    role_list = ["Pypeclub", "Administrator", "Project manager"]
    icon = statics_icon("ftrack", "action_icons", "CreateLists.png")
    settings_key = "create_lists_action"

    def discover(self, session, entities, event):
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() in ("assetversion", "assetversionlist"):
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

        list_name = formatted_date + "_delivery_submission_list"
        category_name = enum_data[0]["value"]

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
        self.project_name = self.assetversions[0]["project"]["full_name"]


        return True


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    CreateListsAction(session).register()
