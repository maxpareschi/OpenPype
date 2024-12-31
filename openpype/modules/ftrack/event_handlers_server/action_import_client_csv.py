import os

from openpype.modules.ftrack.lib import ServerAction

class ImportClientCSV(ServerAction):
    """Action that imports notes from a CSV
    """

    identifier = "import.client.csv"
    label = "Import Client CSV (Server)"
    description = "Import Client Notes from a CSV"

    webserver_host = os.environ.get("OPENPYPE_FTRACK_WEBSERVER_URL", "localhost")

    def discover(self, session, entities, event):
        if not entities:
            return False

        for entity in entities:
            if entity.entity_type.lower() == "project":
                return True
        return False

    def interface(self, session, entities, event):

        event_source = event["source"]   
        user_info = event_source.get("user") or {}
        user_id = user_info.get("id")

        if not user_id:
            return None

        values = event["data"].get("values")
        if values:
            return None

        widget = {
            "type": "widget",
            "url": (f"https://{self.webserver_host}:8079/upload_csv?&"
                    f"client_token={event_source['clientToken']}&"
                    f"client_id={event_source['id']}&"
                    f"user_name={user_info['username']}&"
                    f"user_id={user_id}&"
                    f"entity_id={event['data']['selection'][0]['entityId']}&"
                    f"entity_type={event['data']['selection'][0]['entityType']}"),
            "title": "Import Client CSV",
            "width": 450,
            "height": 450
        }

        return widget

    def launch(self, session, entities, event):
        return

def register(session):
    '''Register plugin. Called when used as an plugin.'''

    ImportClientCSV(session).register()
