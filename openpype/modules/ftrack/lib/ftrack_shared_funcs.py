from logging import getLogger
import ftrack_api


def create_list(session,
                entities,
                event,
                client_review = False,
                list_name = None,
                list_category_name = None,
                prioritize_gathers = False,
                log = None
                ):
    
    if not log:
        log = getLogger("Create List")

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
        review_session_folder["review_sessions"].append(review_session)
        log.debug("Created Review Session '{}/{}'".format(
            list_category["name"], list_name))

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
            log.debug("Appended version '{} v{}' to created review session: {}".format(
                fav["asset"]["name"],
                fav["version"],
                created_review_object["name"]
            ))

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