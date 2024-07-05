import ftrack_api
from datetime import date, datetime

from openpype_modules.ftrack.lib import BaseAction, statics_icon, create_list # type: ignore
from ftrack_api import Session
from ftrack_api.event.base import Event

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

        settings = self.get_ftrack_settings(session, event, entities)["user_handlers"]
        template_name = settings["create_derived_list_action"]["review_session_template_name"]

        if event['data'].get('values', {}):
            return

        items = [{
            "type": "label",
            "value": "<h1><b>Lists Options:</b></h1>"
        }]

        enum_data = []
        for listcat in session.query("select name from ListCategory").all():
            enum_data.append({
                "label": listcat["name"],
                "value": listcat["name"]
            })

        list_name = entities[0]["name"]
        category_name = entities[0]["category"]["name"]

        clean_list_name = list_name.replace(
            category_name, ""
        ).replace(
            category_name.lower(), ""
        ).replace(
            category_name.capitalize(), ""
        ).replace(
            category_name.upper(), ""
        )

        category_name = enum_data[0]["value"]
        for lt in enum_data:
            if lt["value"] == "Delivery":
                category_name = "Delivery"
                break
        
        list_name = clean_list_name + category_name.lower()

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
                    "value": "'Category' for classic lists or 'Folder' for client review lists."
                },
                {
                    "label": "List Category/Folder",
                    "type": "enumerator",
                    "name": "list_category",
                    "data": enum_data,
                    "value": category_name
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
                    "value": "<b>Review Template</b><br>"
                    "This is only applicable if 'Client Review' is turned on. "
                    "Must be the name of an existting Client Review in the project."
                },
                {
                    "label": "Review Template Name",
                    "type": "text",
                    "name": "template_name",
                    "value": template_name,
                }
            ]
        )
            
        return {
            "type": "form",
            "title": "Create Lists",
            "items": items,
            "submit_button_label": "Create",
            "width": 500,
            "height": 650
        }

    def launch(self, session, entities, event):

        session.reset()
        session.auto_populating(True)

        user_values = event["data"].get("values", None)

        if user_values is None:
            return
        
        self.log.info("Sumbitted choices: {}".format(user_values))
        list_name = user_values["list_name"]
        all_asset_version_lists = session.query(
            "select name from AssetVersionList "
            f"where project.id is {entities[0]['project']['id']}").all()
        all_review_sessions = session.query(
            "select name from ReviewSession "
            f"where project.id is {entities[0]['project']['id']}").all()



        existing_names = [l["name"] for l in all_asset_version_lists]

        if user_values["client_review"]:
            existing_names = [l["name"] for l in all_review_sessions]

        if list_name in existing_names:
            return {
                "success": False,
                "message": f"Error: List name '{list_name}' exists already."}

        created_list = create_list(
            session,
            entities,
            event,
            client_review = user_values["client_review"],
            template_name=user_values["template_name"],
            list_name = user_values["list_name"],
            list_category_name = user_values["list_category"],
            prioritize_gathers = user_values["prioritize_gathers"],
            log = self.log
        )

        if create_list:
            self.log.debug("Created '{}' named '{}'".format(
                created_list.entity_type,
                created_list["name"]
            ))

        return {"success": True, "message": "Creation successful!"}


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    CreateDerivedListAction(session).register()
