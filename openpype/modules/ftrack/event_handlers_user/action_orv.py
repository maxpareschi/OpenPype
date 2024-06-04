import os
import subprocess
import re
import traceback
from typing import Callable, List, Tuple
from pathlib import Path

import ftrack_api
from ftrack_api import Session
from ftrack_api.entity.asset_version import AssetVersion
from openpype_modules.ftrack.lib import BaseAction, statics_icon # type: ignore



class ORVAction(BaseAction):
    """ Launch ORV action """
    identifier = "orv.play.action"
    label = "ORV Play"
    description = "ORV Play"
    icon = statics_icon("ftrack", "action_icons", "ORV.png")
    RV_ENV_KEY = "RV_HOME"

    type = "Application"

    allowed_types = ["img", "mov", "exr", "mxf", "dpx",
                     "jpg", "jpeg", "png", "tif",
                     "tga", "prores", "dnx"]
    disallowed_types = [ "%mp4%", "%thumbnail%", "hip", "usd" ]

    not_implemented = ["Project", "ReviewSession",
                       "ReviewSessionFolder", "Folder"]

    def __init__(self, *args, **kwargs):
        """ boostrap the class """
        super().__init__(*args, **kwargs)

        orv_path = None
        
        # search for rv.exe in env
        if os.environ.get(self.RV_ENV_KEY):
            orv_path = os.path.join(os.environ.get(self.RV_ENV_KEY),
                                   "bin",
                                   "rv.exe" if os.name == "nt" else "rv")
            if not os.path.exists(orv_path):
                orv_path = None
        
        # handle failure
        if not orv_path:
            self.log.warning("ORV path was not found, wrong or " +
                             "missing {} env var!".format(self.RV_ENV_KEY))
            # return {
            #     "success": False,
            #     "message": "ORV path was not found."
            # }
        
        self.orv_path = orv_path
        self.orvpush_path = re.sub(r"rv(?=$|\.exe)", "rvpush", self.orv_path)

    def get_all_assetversions(self, session, entities):

        result = []

        for entity in entities:
            etype = entity.entity_type
            query_str = "select id, asset_id, version, version.asset.parent.name"
            if etype == "FileComponent":
                query = "{0} from AssetVersion where components any (id='{1}')".format(query_str, entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersion":
                query = "{0} from AssetVersion where id is '{1}'".format(query_str, entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersionList":
                query = "{0} from AssetVersion where lists any (id='{1}')".format(query_str, entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            elif etype == "Task":
                query = "{0} from AssetVersion where task_id is '{1}'".format(query_str, entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "Shot":
                query = "{0} from AssetVersion where asset.parent.id is '{1}'".format(query_str, entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            else:
                message = "\"{}\" entity type is not implemented yet.".format(entity.entity_type)
                self.log.error(message)
                raise NotImplementedError(message)
        
        return result

    def get_all_available_components(
            self, session: Session, assetversions: List[AssetVersion], include: List[str]
        ):
        """returns all the components that are available for the selected versions.
        
        This allows to have a list of the different component names that exist
        for the selected asset versions.
        The main purpose is to fill up a dropdown later on in the ftrack GUI.
        """
        id_matches = ",".join([av["id"] for av in assetversions])
        name_matches = " or ".join(["name like '%{}%'".format(allow) for allow in include])
        
        query = [
            "select name from Component where version.id in",
            f"({id_matches}) and ({name_matches})"
        ]
        return list(set([c["name"] for c in session.query(" ".join(query)).all()]))
    
    def get_all_component_paths(
            self,
            session: Session,
            assetversions: List[AssetVersion],
            component_name: str,
            only_latest: bool = False,
        ):
    
        assetversion_ids = "','".join([av["id"] for av in assetversions])
        asset_ids = "','".join([av["asset_id"] for av in assetversions])
        name_match = " or ".join([f"component.name like '%{tag}%'" for tag in self.allowed_types])
        
        query = [
            "select",
            "resource_identifier,",
            "component,",
            "component_id,",
            "component.version,",
            "component.version_id,",
            "component.version.version,",
            "component.version.custom_attributes,"
            "component.version.asset,",
            "component.version.asset_id,",
            "component.version.asset.name,",
            "component.version.asset.parent.name,",
            "component.version.asset.versions,",
            "component.version.asset.latest_version",
            "from ComponentLocation",
            # f" where component.name is {component_name}",
            f"where (component.name is {component_name} or {name_match})",
            
        ]
        if only_latest:
            query.extend([
                "and component.version.asset_id in ('{}')".format(asset_ids),
                "and component.version.is_latest_version is true"
            ])
        else:
            query.extend([
                "and component.version_id in ('{}')".format(assetversion_ids)
            ])

        return session.query(" ".join(query)).all()

    def get_previous_component_paths(self, session, comp_locs, component_name):
        prev_assetversions = []

        for cpath in comp_locs:
            assetversion = cpath["component"]["version"]
            assetversions = cpath["component"]["version"]["asset"]["versions"]
            index = assetversions.index(assetversion)
            if index > 0:
                prev_assetversions.append(assetversions[index-1])
            else:
                prev_assetversions.append(None)
        
        self.log.debug(prev_assetversions)
        
        prev_comp_locs = self.get_all_component_paths(
            session,
            [p for p in prev_assetversions if p is not None],
            component_name
        )
        
        for i, pav in enumerate(prev_assetversions):
            if pav is None:
                prev_comp_locs.insert(i, None)
        
        return prev_comp_locs

    def get_paths_list(self, comp_locations: list):
        seen = list()
        for i, cpath in enumerate(comp_locations):
            path = Path(cpath.get("resource_identifier"))
            if path is None or not path.exists():
                self.log.warning(f"File {path} from {cpath['component']['name']} failed to be found. Ignoring it.")
                continue
            if cpath['component']["version_id"] not in seen:
                seen.append(cpath['component']["version_id"])
                yield path.as_posix(), cpath["component"]["version"]["asset"]["parent"]["name"]

    def get_interface(self, available_components, is_manual_selection = False):
        """ Returns correctly formed interface elements """
        enum_data = []
        for component in available_components:
            enum_data.append({
                "label": component,
                "value": component
            })
        enum_data = sorted(enum_data, key = lambda d: not "exr"==d["label"])
        if not enum_data:
            raise IndexError("Failed to fetch any components")
        items = []
        items.extend(
            [
                {
                    "type": "label",
                    "value": "<h1><b>Select Components to play:</b></h1>"
                },
                {
                    "type": "label",
                    "value": "NOTE: If no selected component is available it will attempt to fallback to other component."
                },
                {
                    "label": "<b>Component</b>",
                    "type": "enumerator",
                    "name": "selected_component",
                    "data": enum_data,
                    "value": enum_data[0]["value"]
                },
                {
                    "type": "label",
                    "value": "---"
                },
                {
                    "type": "label",
                    "value": "<h1>Options</h1>"
                },
            ]
        )
        if not is_manual_selection:
            items.extend(
                [
                    {
                        "label": "<b>Load latest versions only</b>",
                        "type": "boolean",
                        "name": "only_latest",
                        "value": False
                    },
                ],
            )
        items.extend(
            [
                {
                    "label": "<div><b>Remove slate</b></div><div style=\"font-size: 8pt;\">(Only works for sequences)</div>",
                    "type": "boolean",
                    "name": "no_slate",
                    "value": True
                },
                {
                    "label": "<div><b>Load previous version</b></div><div style=\"font-size: 8pt;\">(All components)</div>",
                    "type": "boolean",
                    "name": "load_previous_version",
                    "value": False
                },
            ],
        )
        return items

    def discover(self, session, entities, event):
        """ enable action only for implemented types """
        etype = entities[0].entity_type

        if etype in self.not_implemented or not self.orv_path:
            return False
        else:
            return True

    def interface(self, session, entities, event):
        """ Preprosces data and fetches interface elements
            Creates a job to let user know it's processing """
        
        if event["data"].get("values", {}):
            return

        etype = entities[0].entity_type

        is_manual_selection = True if (
            etype == "AssetVersionList" or
            etype == "AssetVersion" or
            etype == "FileComponent"
        ) else False
        
        try:
            # retrieve all available components
            assetversions = self.get_all_assetversions(session, entities)
            available_components = self.get_all_available_components(
                session, assetversions, self.allowed_types)
            items = self.get_interface(
                available_components, is_manual_selection)
            return {
                "items": items,
                "width": 500,
                "height": 570
            }
        
        except:
            self.log.error(traceback.format_exc())
            return {"success": False, "message": traceback.format_exc().splitlines()[-1]}

    def launch(self, session, entities, event):
        """ Launch application loops through all components
            and assembles the subprocess command """
        
        # user values is a dict like:
        # {'selected_component': 'exr', 'load_previous_version': False, 'no_slate': True}
        user_values = event["data"].get("values", None)

        if user_values is None:
            return
        
        self.log.info("Sumbitted choices: {}".format(user_values))

        # Get custom attributes and interface 
        selected_component: str = user_values.get("selected_component", None) # exr, mov...
        load_previous_version: bool = user_values.get("load_previous_version", False)
        no_slate: bool = user_values.get("no_slate", False)
        fps: float = entities[0].get("custom_attributes", {}).get("fps") or 24.0
        
        # TODO: this key seem to be missing in the GUI
        only_latest: bool = user_values.get("only_latest", False)
    
        assetversions: List[AssetVersion] = self.get_all_assetversions(session, entities)

        comp_locs = self.get_all_component_paths(
            session, assetversions, selected_component, only_latest)

        prev_comp_locs = None
        if load_previous_version:
            prev_comp_locs = self.get_previous_component_paths(
                session, comp_locs, selected_component)

        # NOTE: get_path_list2 is a generator, it needs to be turned into a list
        comp_locations = [c for c in comp_locs + (prev_comp_locs or []) if c is not None]

        def return_ttd_envs():
            return {**os.environ,
                "TTD_STUDIO_RESOURCES":"R:",
                "TTD_STUDIO_LOCAL_SOFTWARE":'"C:/Program Files"',
                "TTD_STUDIO_COMMON_SOFTWARE":"R:/shared_software/common",
                "TTD_STUDIO_SHARED_SOFTWARE":"R:/shared_software/windows",
                "OCIO":"R:/ocioconfigs/aces_1.2/config.ocio",
                "solidangle_LICENSE":"5053@appserver",
                "peregrinel_LICENSE":"5080@appserver",
                "MAYA_VERSION":"2022",
                "MTOA_VERSION":"5.0.0.1",
                "YETI_VERSION":"4.1.0",
                "MTOA":"%TTD_STUDIO_SHARED_SOFTWARE%/MtoA/%MTOA_VERSION%",
                "YETI":"%TTD_STUDIO_SHARED_SOFTWARE%/Yeti/Yeti-v%YETI_VERSION%_Maya%MAYA_VERSION%-windows",
                "MAYA_MODULE_PATH":"%MAYA_MODULE_PATH%;%MTOA%;%YETI%",
            }

        def order_lambda(comp_loc):
            from difflib import SequenceMatcher
            # order the list of all components so the selected one goes first
            # and then by order of preference
            comp_names_priorities = ["exr", "exr_main", "exr_source", "dnxhd_exr", "dnxhd_mov", "mov", "jpeg"]
            name = comp_loc["component"]["name"]
            
            if name == selected_component:
                return 0
            elif name in comp_names_priorities:
                return comp_names_priorities.index(name) + 1
            else:
                return 1.0 - SequenceMatcher(None, name, selected_component).ratio()
            
        comp_locations.sort(key = order_lambda)

        paths: List[Tuple[str]] = list(self.get_paths_list( comp_locations))
        if not paths:
            return {"success": True, "message": "No valid components where found in the server."}

        # START OF OPENRVPUSH PROC
        src = "from openrv_tools_22dogs import orvpush_inputs_callback\n"
        signature = f"({', '.join([str(i) for i in [paths, no_slate, fps]])})"
        src += "orvpush_inputs_callback" + signature
        prj = entities[0]["project"]["full_name"]

        cmd = [self.orvpush_path, "-tag", prj, "py-exec", src]
        self.log.debug(f"Running ORVPUSH: {cmd}")
        rv_push_process = subprocess.Popen(cmd, env=return_ttd_envs())
        msg = f"ORV Launching: {fps} FPS with {'no' if no_slate else ''} slate."
        return {"success": True, "message": msg}


def register(session):
    """Register hooks."""
    if not isinstance(session, ftrack_api.Session):
        return
    action = ORVAction(session)
    action.register()
