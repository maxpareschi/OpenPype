from __future__ import annotations
from logging import getLogger, Logger
from typing import Optional, List
from tempfile import gettempdir
from urllib.request import urlopen
from shutil import copyfileobj

logger = getLogger(__name__)

from ftrack_api.entity.component import Component
from ftrack_api.entity.note import Note
from ftrack_api.entity.location import Location
from ftrack_api import Session
from ftrack_api.entity.base import Entity
from ftrack_api.event.base import Event

matching_fields = [
    "content",
    "project_id",
    # "parent_type",
    "category_id",
    "user_id",
    ]

def check_review_template(session: Session, target_name: str, project_id: str):
    review_session = session.query(
        f"select name from ReviewSession where project.id is {project_id}"
    ).all()
    return next((rs for rs in review_session if rs["name"] == target_name), None)


def copy_review_invitees(session: Session, source: Entity, target: Entity):
    for invitee in source["review_session_invitees"]:
        # Make sure email is not None but string
        email = invitee["email"] or ""
        session.create(
            "ReviewSessionInvitee",
            {
                "name": invitee["name"],
                "email": email,
                "review_session": target
            }
        )
    session.commit()


def duplicate_ftrack_server_component(comp: Component, session: Session):

    # server_location: Location = Session().get('Location', symbol.SERVER_LOCATION_ID)

    for cloc in comp["component_locations"]:

        if cloc["location"]["name"] == "ftrack.server":
            server_location: Location = cloc['location']
            logger.info(f"Fetching component from location {cloc['location']}")
            try:
                url = server_location.get_url(comp)
            except Exception as e:
                logger.info(f"Failed to retrieve url for {comp} in {server_location} due to {e}")
                raise (e)
            name = comp["name"] + comp["file_type"]
            f = gettempdir() + "/" + name
            logger.info(f"Creating component from temp file: {f}")

            with urlopen(url) as response, open(f, 'wb') as out_file:
                copyfileobj(response, out_file)
        else:
            logger.info(f"Ignoring location {cloc['location']['name']}")
            # it is unmanaged, maybe the source is locally and thus inaccesible
            continue
        return session.create_component(f, {"name":name}, location=cloc["location"])


def transfer_note_components(note_src: Note, note_target: Note, session: Session):
    for note_comp in note_src["note_components"]:
        logger.info(f"Working on component {note_comp}")
        comp = note_comp["component"]
        if any(comp["name"] != c["component"] for c in note_target["note_components"]):
            logger.info(f"This component was already copied {comp['name']}")
            continue
        try:
            new_component = duplicate_ftrack_server_component(comp, session)
        except Exception as e:
            session.rollback()
            logger.info(f"Failed to copy component {comp} due to {e}")
            continue
        if new_component is None:
            logger.info(f"No new component was returned")
            continue
        session.create("NoteComponent", {"component_id":new_component["id"], "note_id": note_target["id"]})


def create_notes(
    session: Session, src_note: Note, target: Entity, notes_in_target: List[Note]
    ):
    msg = ""
    # REMOVE ALREADY MATCHING NOTES IN TARGET
    for target_note in notes_in_target:
        if  all(src_note[f] == target_note[f] for f in matching_fields):

            logger.info(
                f"Note already copied. {src_note} -> {target_note}, "
                f"removing it. Content is: {src_note['content']}"
            )
            session.delete(target_note)
    else:
        replies = [
            session.create(
                "Note",
                {tag: reply[tag] for tag in matching_fields}
                ) for reply in src_note["replies"]
            ]

        logger.info(f"Note components are: {list(src_note['note_components'])}")

        r = session.create("Note", {
            **{tag: src_note[tag] for tag in matching_fields},
            "parent_id": target["id"],
            "parent_type": target.entity_type,
            "replies": replies,
            # "note_label_links" : note_label_links,
            })
        
        transfer_note_components(src_note, r, session)

        for i, reply in enumerate(src_note["replies"]):
            transfer_note_components(reply, r["replies"][i], session)

        logger.info(f"Updated client notes for version_id {target['id']}")
        msg += f"Updated client notes for version {target['id']}"
        session.commit()
    return msg


def copy_client_notes(session: Session, review_items: List[Entity]):
    # session = Session(plugin_paths=[])
    sources = [rev["asset_version"] for rev in review_items]
    logger.info(f"Source versions are: {sources}")
    logger.info(f"Review versions are: {review_items}")
    versions_ids = f"({', '.join([v['id'] for v in sources])})"
    versions_by_id = {v["id"]: v for v in sources}

    logger.info(f"versions_by_id dict is {versions_by_id}")

    select = "select author, category, content, in_reply_to, category.name, parent_id"
    where = f"where parent_id in {versions_ids} and in_reply_to is None and category.name is \"For Client\""

    notes = session.query(f"{select} from Note {where}").all()

    logger.info(f"Notes are {notes}")

    msg = ""
    for note in notes:
        logger.info(f"Working on note {note}")
        source = versions_by_id[note["parent_id"]]
        target = next(r for r in review_items if r["asset_version"] == source)

        logger.info(f"Working on note {note} from version {source}")
        notes_in_target = [n for n in notes if n["parent_id"] == target["id"]]

        logger.info(f"Target notes are: {notes_in_target}")
        msg += create_notes(session, note, target, notes_in_target)

        session.commit()

    return msg

def create_list(session: Session,
                entities: List[Entity],
                event: Event,
                client_review: bool = False,
                template_name: str = "",
                list_name: Optional[str] = None,
                list_category_name = None,
                prioritize_gathers: bool = False,
                log: Logger = None
                ):

    
    global logger
    if log:
        logger=log
    else:
        log = logger

    review_session = None
    created_list = None

    if client_review:
        target_exists = True if session.query("ReviewSession where name is '{}'".format(
            list_name
        )).first() else False
        if target_exists:
            return {"success": False, "message": "Review Session is already present (duplicate name)!"}
    else:
        target_exists = True if session.query("AssetVersionList where name is '{}'".format(
            list_name
        )).first() else False
        if target_exists:
            return {"success": False, "message": "List is already present (duplicate name)!"}

    list_category = session.query("ListCategory where name is '{}'".format(
        list_category_name
    )).first() or None

    review_session_folder = session.query("ReviewSessionFolder where name is '{}'".format(
        list_category_name
    )).first() or None

    list_owner = session.query("User where id is '{}'".format(
        event["source"]["user"]["id"]
    )).first() or None

    assetversions = session.query("AssetVersion where lists.id is '{}'".format(
        entities[0]["id"]
    )).all()


    final_assetversions = []
    for av in assetversions:
        log.debug("Processing AssetVersion '{} v{}' in source_list".format(
            av["asset"]["name"],
            av["version"]
        ))
        if prioritize_gathers:
            if av["incoming_links"]:
                log.debug("This is already a gathered version, collecting.")
                final_assetversions.append(av)
            elif av["outgoing_links"]:
                log.debug("This is a source version, collecting '{} v{}' as linked gather.".format(
                    av["outgoing_links"][0]["to"]["asset"]["name"],
                    av["outgoing_links"][0]["to"]["version"]
                ))
                final_assetversions.append(
                    av["outgoing_links"][0]["to"]
                )
            else:
                log.debug("This version has no gather, skipping collection...")
        else:
            log.debug("Collecting direct version, skipping gathers if any.")
            final_assetversions.append(av)

    if client_review:
        review_template = check_review_template(
            session, template_name, entities[0]["project"]["id"])

        if not review_session_folder:
            review_session_folder = session.create("ReviewSessionFolder", {
                "project": entities[0]["project"],
                "name": list_category["name"]
            })
        review_session_data = {
            "project": entities[0]["project"],
            "category": list_category,
            "name": list_name
        }
        if list_owner:
            review_session_data.update({"created_by": list_owner})

        review_session = session.create("ReviewSession", review_session_data)

        if review_template is not None:

            copy_review_invitees(session, review_template, review_session)


        review_session_folder["review_sessions"].append(review_session)
        log.debug("Created Review Session '{}/{}'".format(
            list_category["name"], list_name))
        review_items = list()
        for fav in final_assetversions:
            created_review_object = session.create("ReviewSessionObject", {
                "asset_version": fav,
                "review_session": review_session,
                "name": fav["asset"]["name"],
                "description": fav["comment"],
                "version": "Version {}".format(str(fav["version"]).zfill(3))
            })
            review_items.append(created_review_object)
            # created_review_object["notes"].extend(
            #     [n for n in fav["notes"]]
            # )
            log.debug("Appended version '{} v{}' to created review session: {}".format(
                fav["asset"]["name"],
                fav["version"],
                created_review_object["name"]
            ))
        session.commit()
        copy_client_notes(Session(plugin_paths=[]), review_items)

    else:
        list_data = {
            "project": entities[0]["project"],
            "category": list_category,
            "name": list_name
        }
        if list_owner:
            list_data.update({"owner": list_owner})

        created_list = session.create("AssetVersionList", list_data)
        log.debug("Created List '{}/{}'".format(
            list_category["name"], list_name))

        created_list["custom_attributes"]["source_list_name"] = entities[0]["name"]
        log.debug("Source List is set as '{}'".format(
            created_list["custom_attributes"]["source_list_name"]))

        for fav in final_assetversions:
            created_list["items"].append(fav)
            log.debug("Appended version '{} v{}' to created list: {}".format(
                fav["asset"]["name"],
                fav["version"],
                created_list["name"]
            ))

    session.commit()

    if review_session:
        return review_session
    
    if created_list:
        return created_list