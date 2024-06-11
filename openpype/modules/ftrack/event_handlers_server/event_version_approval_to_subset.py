from openpype_modules.ftrack.lib import BaseEvent
from openpype.client.operations import OperationsSession
from openpype.client import get_subset_by_name


class VersionApprovalToSubset(BaseEvent):
    """
    Sets approved_version field in db when status on assetversion
    is set to any status that is mapped as done
    """
    def launch(self, session, event):
        filtered_entities_info = self.filter_entity_info(event)
        if not filtered_entities_info:
            return

        self.process_by_project(session, event, filtered_entities_info)

    def filter_entity_info(self, event):
        entities = []
        for entity_info in event["data"].get("entities", []):
            # Filter AssetVersions
            if entity_info["entityType"] != "assetversion":
                continue

            # Skip if statusid not in keys (in changes)
            keys = entity_info.get("keys")
            if not keys or "statusid" not in keys:
                continue

            # Get version status change id
            version_status_id = (
                entity_info
                .get("changes", {})
                .get("statusid", {})
                .get("new", {})
            )

            # Just check that 'new' is set to any value
            if not version_status_id:
                continue

            # Get project id from entity info
            entity_info["project_id"] = entity_info["parents"][-1]["entityId"]
            entity_info["asset_id"] = entity_info["parents"][1]["entityId"]

            entities.append(entity_info)
            
        return entities

    def process_by_project(self, session, event, entities):
        project_id = entities[0]["parents"][-1]["entityId"]
        project_name = self.get_project_name_from_event(
            session, event, project_id
        )
        project_settings = self.get_project_settings_from_event(
            event, project_name
        )
        event_settings = project_settings["ftrack"]["events"]["version_to_approved_subset"]
        # Skip if event is not enabled or status mapping is not set
        if not event_settings["enabled"]:
            self.log.debug("Project \"{}\" has disabled {}".format(
                project_name, self.__class__.__name__
            ))
            return

        updates = []
        for entity in entities:
            version = session.query(
                "select project.full_name, version, custom_attributes, asset.parent.name from AssetVersion where id is {}".format(
                    entity["entityId"]
                )
            ).one()

            status = session.query(
                "select id, name, state from Status where id is {}".format(
                    entity["changes"]["statusid"]["new"]
                )
            ).one()
            
            version_data = {
                "project_name": version["project"]["full_name"],
                "version": version["version"],
                "version_id": entity["entityId"],
                "asset_name": version["asset"]["parent"]["name"],
                "asset_id": entity["parentId"],
                "avalon_mongo_id": version["asset"]["parent"]["custom_attributes"]["avalon_mongo_id"],
                "subset": version["custom_attributes"]["subset"],
                "status_id": status["id"],
                "status_name": status["name"],
                "status_state": status["state"]["name"]
            }
            updates.append(version_data)

        for up in updates:
            subset = get_subset_by_name(
                up["project_name"],
                up["subset"],
                up["avalon_mongo_id"]
            )
            if subset:
                self.log.debug("Found status change on asset version in '{}/{}'".format(
                    up["asset_name"],
                    up["subset"]
                ))

                current_approved_version = subset["data"].get("approved_version", None)
                self.log.debug("Current approved version is '{}'.".format(
                    current_approved_version
                ))

                if up["status_state"] == "Done":
                    v = str(up["version"])
                    update_session = OperationsSession()
                    update_session.update_entity(
                        up["project_name"],
                        "subset",
                        subset["_id"],
                        {
                            "data.approved_version": v
                        }
                    )
                    update_session.commit()
                    self.log.debug("Subset was set to approved version '{}'.".format(v))
                else:
                    if current_approved_version:
                        if int(up["version"]) == int(current_approved_version):
                            update_session = OperationsSession()
                            update_session.update_entity(
                                up["project_name"],
                                "subset",
                                subset["_id"],
                                {
                                    "data.approved_version": ""
                                }
                            )
                            update_session.commit()
                            self.log.debug("Approved version for this subses was reset to ''.")
                        else:
                            self.log.debug("Approved version was not changed for subset.")
                    else:
                        self.log.debug("Approved version was not changed for subset.")


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    VersionApprovalToSubset(session).register()
