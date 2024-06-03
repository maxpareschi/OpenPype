import ftrack_api
from datetime import date

from openpype_modules.ftrack.lib import BaseAction, statics_icon, create_list # type: ignore


class CreateDerivedListAction(BaseAction):
    """Create daily review session object per project.
    """
    identifier = "ttd.create.derived.list"
    label = "Create Derived List"
    description = "Manually create a derived list from other lists"
    role_list = ["Pypeclub", "Administrator", "Project manager"]
    icon = statics_icon("ftrack", "action_icons", "CreateList.png")
    settings_key = "create_derived_list_action"

    def discover(self, session, entities, event):
        is_valid = False
        if entities[0].entity_type == "AssetVersionList":
            is_valid = True

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

        list_name = None
        category_name = None

        category_name = enum_data[0]["value"]
        for lt in self.list_types:
            if lt["name"] == "Delivery":
                category_name = "Delivery"
        
        if entity_type == "AssetVersionList":
            entity_list_name = entities[0]["name"]
            entity_list_category = entities[0]["category"]["name"]

        if entity_list_name:
            list_name = entity_list_name

        if entity_list_category:
            category_name = entity_list_category

        if not list_name or not category_name:
            return {"success": False, "message": "No list name or category found!"}
        
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

        created_list = create_list(
            session,
            entities,
            event,
            client_review = user_values["client_review"],
            list_name = user_values["list_name"],
            list_category_name = user_values["list_category"],
            prioritize_gathers = user_values["prioritize_gathers"],
            log = self.log
        )

        self.log.debug("Created '{}' named '{}'".format(
            created_list.entity_type,
            created_list["name"]
        ))

        return {"success": True, "message": "Creation successful!"}


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    CreateDerivedListAction(session).register()
