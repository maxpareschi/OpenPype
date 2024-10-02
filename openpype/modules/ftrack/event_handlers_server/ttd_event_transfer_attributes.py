from pprint import pformat
from logging import getLogger

import json

logger = getLogger(__name__)

from ftrack_api import Session
from ftrack_api.entity.base import Entity
from ftrack_api.event.base import Event
from openpype_modules.ftrack.lib import BaseEvent # type: ignore

def look_for_hierarchical_attrs(entity: Entity, attr: str):
    """Look for hierarchical attributes in parent's hierarchy.
    
    This is done because the ftrack_api may be bugged as stated in the oficial
    documentation: 
    https://ftrack-python-api.rtd.ftrack.com/en/latest/example/custom_attribute.html#limitations
    """

    asset = entity.get("asset") or entity
    ancestors = asset.get("ancestors")

    if ancestors is None:
        raise NotImplementedError(f"The entity type {entity.entity_type} is not implemented.")

    for e in [entity] + ancestors:
        value = e["custom_attributes"][attr]
        if value is not None:
            logger.debug(f"Found attr {attr} at entity {e} with value {value}")
            return value
    logger.debug(f"Failed to find attr {attr}")


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

    handle_start = look_for_hierarchical_attrs(shot, "handleStart")
    handle_end = look_for_hierarchical_attrs(shot, "handleEnd")

    try:
        new_start = look_for_hierarchical_attrs(shot, "frameStart") - handle_start
        new_end = look_for_hierarchical_attrs(shot, "frameEnd") + handle_end
    except TypeError as e:
        logger.warning(e)
        logger.info(f"Failed to collect OpenPype frame data {handle_end}, {handle_start} in shot {name}")
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

    session = Session()
    with session.auto_populating(True):
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

    target_attrs__ = ["frameStart", "frameEnd", "handleStart", "handleEnd"]

    def launch(self, session: Session, event: Event):

        filtered_entities_info = self.filter_entity_info(event)
        if not filtered_entities_info:
            return

        # self.log.info(pformat(filtered_entities_info))

        ids_ = [e["entityId"] for e in filtered_entities_info]
        where = f"where id in ({','.join(ids_)})"
        select = "select " + ",".join(["id", "custom_attributes", "name"])
        # session = Session()

        with session.auto_populating(True):
            shots = session.query(f"{select} from Shot {where}").all()
            for shot in shots:
                self.log.info(f"Updating frame data in shot {shot['name']}")
                transfer_hierarchical_frame_data_in_shot(self.log, shot)
    
        session.commit()

    def filter_entity_info(self, event):
        entities = []

        for entity_info in event["data"].get("entities", []):
            self.log.debug(json.dumps(entity_info, indent=4, default=str, sort_keys=True))
            if entity_info.get("entity_type", None) != "Shot":
                continue
            if entity_info.get("entityType", None) != "task":
                continue
            if all(tag not in entity_info.get("changes", []) for tag in self.target_attrs__):
                continue
            entities.append(entity_info)

        return entities

def register(session):
    '''Register plugin. Called when used as an plugin.'''

    TTDTransferAttributes(session).register()
