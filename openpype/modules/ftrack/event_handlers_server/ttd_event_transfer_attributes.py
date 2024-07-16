from pprint import pformat


from ftrack_api import Session
from ftrack_api.entity.base import Entity
from ftrack_api.event.base import Event
from openpype_modules.ftrack.lib import BaseEvent
from openpype.client.operations import OperationsSession
from openpype.client import get_subset_by_name

"""

[ [{'action': 'update',
  'changes': {'frameEnd': {'new': '1022', 'old': '1020'}},
  'entityId': '36a60d41-b4d1-49a6-8ff8-36b3d696df06',
  'entityType': 'task',
  'entity_type': 'Shot',
  'keys': [ 'frameEnd' ],
  'objectTypeId': 'bad911de-3bd6-47b9-8b46-3476e237cb36',
  'parentId': '1b88317b-631f-4126-8b60-977d19c85e14',
  'parents': [{'entityId': '36a60d41-b4d1-49a6-8ff8-36b3d696df06',
               'entityType': 'task',
               'entity_type': 'Shot',
               'parentId': '1b88317b-631f-4126-8b60-977d19c85e14'},
              {'entityId': '1b88317b-631f-4126-8b60-977d19c85e14',
               'entityType': 'task',
               'entity_type': 'Sequence',
               'parentId': '6b3dc3f3-f781-489e-8837-c0d12ed3aff8'},
              {'entityId': '6b3dc3f3-f781-489e-8837-c0d12ed3aff8',
               'entityType': 'show',
               'entity_type': 'Project',
               'parentId': None}]}] ]
"""


def transfer_hierarchical_frame_data_in_shot(logger, shot: Entity):
    """Copy hierarchical frame data into fstart and fend attributes in a Ftrack shot
    
    As in TTD we update the OpenPype frameStart and frameEnd, and those are
    hierarchical and thus cannot be displayed in columns, this function copies
    the data from these attributes into the native Ftrack's fstart and fend,
    so that they can be displayed in Ftrack as columns.
    This function creates a local Ftrack session to not mess up with the commits.
    """    

    name = shot["name"]

    if "delivery" in name or "gather" in name:
        logger.info(f"Ignoring shot {name}")
        return

    old_start = shot["custom_attributes"]["fstart"]
    old_end = shot["custom_attributes"]["fend"]
    old_duration = old_end - old_start + 1

    handle_start = shot["custom_attributes"]["handleStart"]
    handle_end = shot["custom_attributes"]["handleEnd"]

    try:
        new_start = shot["custom_attributes"]["frameStart"] - handle_start
        new_end = shot["custom_attributes"]["frameEnd"] + handle_end
    except TypeError as e:
        logger.info(f"Failed to collect OpenPype frame data {handle_end} in shot {name}")
        return

    new_duration = new_end - new_start + 1
    if (
        old_start == new_start
        and old_end == new_end
        and old_duration == new_duration
    ):
        logger.info(f"Nothing to update for shot {name}")
        return

    logger.info(f"Updating {name} 'fstart' to {new_start} and 'fend' to {new_end}")

    old_start = shot["custom_attributes"]["fstart"] = new_start
    old_end = shot["custom_attributes"]["fend"] = new_end


def transfer_hierarchical_frame_data_in_project(project: str):
    """Copy hierarchical frame data into fstart and fend attrs in all project shots"""

    session = Session(auto_populate=True)
    where = f"where project.name is {project}"
    select = "select " + ",".join(["id", "custom_attributes", "name"])
    shots = session.query(f"{select} from Shot {where}").all()
    for shot in shots:
        transfer_hierarchical_frame_data_in_shot(shot)
    session.commit()



class TTDTransferAttributes(BaseEvent):
    """Intercept attributes updates and transfer them based on mappings.
    
    For now there are no mappings as we only need to transfer frame data
    but in the future this can be extended to work with a settings mapping.
    """

    priority = 0
    target_attrs__ = ["frameStart", "frameEnd", "handleStart", "handleEnd"]

    def launch(self, session: Session, event: Event):

        filtered_entities_info = self.filter_entity_info(event)
        if not filtered_entities_info:
            return

        ids_ = [e["entityId"] for e in filtered_entities_info]
        where = f"where id in ({','.join(ids_)})"
        select = "select " + ",".join(["id", "custom_attributes", "name"])
        session = Session()
        shots = session.query(f"{select} from Shot {where}").all()
        for shot in shots:
            self.log.info(f"Updating frame data in shot {shot['name']}")
            transfer_hierarchical_frame_data_in_shot(self.log, shot)
        session.commit()

    def filter_entity_info(self, event):
        entities = []

        for entity_info in event["data"].get("entities", []):
            if entity_info["entity_type"] != "Shot":
                continue
            if entity_info["entityType"] != "task":
                continue
            if all(tag not in entity_info["changes"] for tag in self.target_attrs__):
                continue
            entities.append(entity_info)

        return entities

def register(session):
    '''Register plugin. Called when used as an plugin.'''

    TTDTransferAttributes(session).register()
