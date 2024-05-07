import os
import subprocess
import re
import traceback
from typing import Callable, List

import ftrack_api
from openpype_modules.ftrack.lib import BaseAction, statics_icon # type: ignore



def return_pyexec_command(f: Callable, *args, **kwargs):
    """Parse a function source code as a string.
    
    f is expected to be a python function.
    This is a utility function that allows to have syntax highlighting and
    intelisense while developing code that is going to be sent as a string
    to be executed somewhere else.
    """
    # get source code of function
    from inspect import getsource
    src = getsource(f)

    # remove top level indentation in case the funciton is not globally defined
    tab = re.match("[\t\ ]*(?=def\ )", src).group()
    if tab:
        src = "\n".join([l[len(tab):] for l in src.split("\n")])
    
    # return the source code as top level function and add execution line
    parsed_kw = [f"{k}={v}" for k, v in kwargs.items()]
    signature = f"({', '.join([str(i) for i in [*args, *parsed_kw]])})"
    return src + f.__name__ + signature + "\n"

def monkey_patch_openrv_gui():
    """Run inside OpenRV session. Adds combobox for RVSwitch nodes."""
    from logging import getLogger, basicConfig, INFO, DEBUG
    from os import environ
    from PySide2.QtWidgets import QComboBox, QApplication # type: ignore
    from rv import qtutils as rvq, extra_commands as rvec, commands as rvc # type: ignore
    # environ["RV_MULTI_MEDIA_REP_DEBUG"] = 1
    level=DEBUG if "RV_MULTI_MEDIA_REP_DEBUG" in environ else INFO
    getLogger().setLevel(DEBUG)
    logger = getLogger("gui_proc")

    app = QApplication.instance() or QApplication()
    wids = [w for w in app.allWidgets() if isinstance(w, QComboBox)]
    gen = (w for w in wids if w.objectName() == "22_version_dropdown")
    try:
        version_dropdown = next(gen)
    except StopIteration as e:
        version_dropdown = None


    if version_dropdown is not None:
        logger.debug(f"Dropdown found.")
        return

    logger.debug(f"Monkey patching OpenRV GUI as no version dropdown was found.")
    bottom_toolbar = rvq.sessionBottomToolBar()
    version_dropdown = QComboBox()
    version_dropdown.setObjectName("22_version_dropdown")
    # version_dropdown.addItems(["v021", "v022", "v023"])
    bottom_toolbar.addWidget(version_dropdown)
    def update_combobox (event):
        version_dropdown.clear()
        version_dropdown.addItems(rvc.nodesOfType("RVSwitchGroup"))

    rvc.bind("default", "global", "new-node", update_combobox, "___doc___")
    rvc.bind("default", "global", "after-node-deleted", update_combobox, "__doc__")
    logger.debug(f"All done.")
    
    def on_item_clicked(text):
        info = rvec.sourceMetaInfoAtFrame(rvc.frame())
        # logger.debug(info)
        src_node = rvc.sourceMediaRepSourceNode(info["node"])
        # logger.debug(f"Current source node is {src_node}")
        switch_node = rvc.sourceMediaRepSwitchNode(src_node)
        # logger.debug(f"Current switch node is {switch_node}.")
        rvc.setViewNode(text)

    try:
        version_dropdown.textActivated.disconnect()
    except Exception as e:
        ...

    version_dropdown.textActivated.connect(on_item_clicked)

def orvpush_proc(items: List[List[str]]):
    """Main function to be run inside OpenRV.
    
    This function must be parsed with the 'return_pyexec_command' before
    being sent over.
    Because this function is executed in another interpreter, global
    variables won't be inherited in this scope, which means that
    all imports must happen in the local scope.
    """

    from logging import getLogger
    from pathlib import Path
    from re import compile as recomp

    import rv.commands as rvc # type: ignore

    logger = getLogger("orvpush_proc")
    SHOT_REGEX = recomp(r"(?<=\_)\d{3}\_\d{3}(?=\_)")



    def flatten_input_list(items: List[List[str]]):
        """Adapt paths so it matches rvpush needs.
        TODO: when rvpush is  approved, refactor the function that
        generates the list of items so that it matches rvpush
        instead of rv so that we dont need the lines down below
        """
        inputs = list()
        for group in items:
            for string in group:
                for substring in string.split(" "):
                    if len(substring) < 5:
                        logger.debug(f"Ignoring substring {substring}")
                        continue
                    elif Path(substring.replace(".#.", ".1001.")).exists():
                        logger.debug(f"Adding file {substring}")
                        inputs.append(Path(substring))
                    else:
                        logger.debug(f"Ignoring file {substring} as it doesnt exists")
        return inputs

    inputs = flatten_input_list(items)

    def is_source_in_rv_session(f: Path):
        """Checks whether a path file is already imported.
        TODO: if file was imported outside of pipeline, it will return true,
        but the file won't appear in any switch node.
        """
        for src in rvc.sources():
            if src is None:
                continue
            p = Path(src[0])
            if p.parent == f.parent and p.suffix == f.suffix:
                return True
        return False


    rvc.addSourceBegin() # halt new sources connections

    # iterate over the component paths [[], [], []]
    for f in inputs:
        logger.debug(f"working on {f}")

        if is_source_in_rv_session(f):
            continue
                      
        tag = f.name
        shot = SHOT_REGEX.findall(f.stem)[0]

        # iterate over existing sources and look for shot groups
        for switch_node in rvc.nodesOfType("RVSwitch"):
            try:
                src_node = rvc.sourceMediaRepSourceNode(switch_node)
                src = Path(rvc.sourceMedia(src_node)[0])
            except Exception as e:
                continue
            
            sh = SHOT_REGEX.findall(Path(src).stem)[0]
            
            logger.debug(f"Match found for incoming file and existing switch node.")
            if sh == shot:
                args = [f.as_posix()]
                if f.suffix in [".exr", ".jpg", ".jpeg"]:
                    args += ["+in", "1001"]
                try:
                    rvc.addSourceMediaRep(src_node, tag, args)
                    logger.debug(f"Reused switch node {switch_node} @ {shot}:")
                except Exception as e:
                    logger.warning(f"Error {e} found... Check with dev team.")
                    pass
                break
        else:
            logger.debug(f"Creating new switch node.")
            args = [f.as_posix(), "+mediaRepName", tag]
            if f.suffix in [".exr", ".jpg", ".jpeg"]:
                args += ["+in", "1001"]
            src = rvc.addSourceVerbose(args)
            logger.debug(f"New switch node created {src} @ {shot}:")

    rvc.addSourceEnd() # start connecting all new sources
    logger.debug('ERROR: This message will not be color coded.')

class ORVAction(BaseAction):
    """ Launch ORV action """
    identifier = "orv.play.action"
    label = "ORV Play"
    description = "ORV Play"
    icon = statics_icon("ftrack", "action_icons", "ORV.png")
    RV_ENV_KEY = "RV_HOME"

    type = "Application"

    allowed_types = ["img", "mov", "exr", "mxf", "dpx",
                     "jpg", "jpeg", "png", "tif", "tiff",
                     "tga", "dnxhd", "prores", "dnx"]

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

            if etype == "FileComponent":
                query = "select id, asset_id, version from AssetVersion where components any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersion":
                query = "select id, asset_id, version from AssetVersion where id is '{0}'".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "AssetVersionList":
                query = "select id, asset_id, version from AssetVersion where lists any (id='{0}')".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            elif etype == "Task":
                query = "select id, asset_id, version from AssetVersion where task_id is '{0}'".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)

            elif etype == "Shot":
                query = "select id, asset_id, version from AssetVersion where asset.parent.id is '{0}'".format(entity["id"])
                for assetversion in session.query(query).all():
                    result.append(assetversion)
            
            else:
                message = "\"{}\" entity type is not implemented yet.".format(entity.entity_type)
                self.log.error(message)
        
        return result

    def get_all_available_components(self, session, assetversions, allow_list):
        id_matches = ",".join([av["id"] for av in assetversions])
        name_matches = " or ".join(["name like '%{}'".format(allow) for allow in allow_list])
        
        query = [
            "select name from Component where version.id in",
            "({}) and ({})".format(id_matches, name_matches)
        ]
        return list(set([c["name"] for c in session.query(" ".join(query)).all()]))
    
    def get_all_component_paths(self, session, assetversions, component_name, only_latest = False):
        assetversion_ids = "','".join([av["id"] for av in assetversions])
        asset_ids = "','".join([av["asset_id"] for av in assetversions])
        
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
            "component.version.asset.versions,",
            "component.version.asset.latest_version",
            "from ComponentLocation where",
            "component.name is {}".format(component_name),
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

    def get_previous_component_paths(self, session, component_paths, component_name):
        prev_assetversions = []

        for cpath in component_paths:
            assetversion = cpath["component"]["version"]
            assetversions = cpath["component"]["version"]["asset"]["versions"]
            index = assetversions.index(assetversion)
            if index > 0:
                prev_assetversions.append(assetversions[index-1])
            else:
                prev_assetversions.append(None)
        
        self.log.debug(prev_assetversions)
        
        prev_component_paths = self.get_all_component_paths(
            session,
            [p for p in prev_assetversions if p is not None],
            component_name
        )
        
        for i, pav in enumerate(prev_assetversions):
            if pav is None:
                prev_component_paths.insert(i, None)
        
        return prev_component_paths

    def get_pathlist(self, cur_paths, prev_paths = None, no_slate = False):
        path_list = []
        for i, cpath in enumerate(cur_paths):
            path_group = [self.parse_file(cpath, no_slate)]
            if prev_paths is not None:
                if len(prev_paths) >= i and prev_paths[i] is not None:
                    path_group.append(
                        self.parse_file(prev_paths[i], no_slate))
            path_list.append(path_group)
        
        return path_list

    def parse_file(self, component_location, no_slate = False):

        path = os.path.abspath(
            component_location["resource_identifier"]
        ).replace("\\", "/")

        head, ext = os.path.splitext(path)

        try:
            match = re.findall(r'\d+$', head)[0]
            head = head.replace(match, "")

            if no_slate:
                frame_in = "-in {} ".format(int(match) + 1)
            else:
                frame_in = ""
            path = "{}{}#{}".format(
                frame_in,
                head,
                ext
            )
        except:
            pass

        return path

    def get_interface(self, available_components, is_manual_selection = False):
        """ Returns correctly formed interface elements """
        enum_data = []
        for component in available_components:
            enum_data.append({
                "label": component,
                "value": component
            })
        enum_data = sorted(enum_data, key = lambda d: d["value"], reverse = True)
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
                    "value": "NOTE: If no selected component is available it will not appended in the viewer."
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
        """ Preprocess data and fetches interface elements
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
        
        user_values = event["data"].get("values", None)

        if user_values is None:
            return
        
        self.log.info("Sumbitted choices: {}".format(user_values))

        # Get custom attributes and interface values
        fps = entities[0].get("custom_attributes", {}).get("fps", 24.0)
        selected_component = user_values.get("selected_component", None)
        load_previous_version = user_values.get("load_previous_version", False)
        no_slate = user_values.get("no_slate", False)
        only_latest = user_values.get("only_latest", False)

        args = []
    
        assetversions = self.get_all_assetversions(session, entities)
        component_paths = self.get_all_component_paths(
            session, assetversions, selected_component, only_latest)

        if load_previous_version:
            prev_component_paths = self.get_previous_component_paths(
                session, component_paths, selected_component)
        else:
            prev_component_paths = None
        
        paths = self.get_pathlist(
            component_paths, prev_component_paths, no_slate)

        # START OF OPENRVPUSH PROC
        # generate dropdown on the fly
        src += "from typing import Callable, List\n"
        src += return_pyexec_command(monkey_patch_openrv_gui)

        # leverage multimedia sources feature as version switcher
        src += return_pyexec_command(orvpush_proc, paths)
        cmd = [self.orvpush_path, "py-exec", src]
        self.log.debug(f"Running ORVPUSH: {cmd}")
        rv_push_process = subprocess.Popen(cmd)
        # END OF OPENRVPUSH PROC

        # args.append(self.orv_path)

        # for path in paths:
        #     args.append("[")
        #     for sub in path:
        #         args.extend(sub.split(" "))
        #     args.append("]")
        
        # # force session fps
        # if fps is not None:
        #     args.extend(["-fps", str(fps)])
        
        # self.log.info("Running ORV: {}".format(args))
        # subprocess.Popen(args)
            
        return {"success": True, "message": "ORV Launching!"}


def register(session):
    """Register hooks."""
    if not isinstance(session, ftrack_api.Session):
        return
    action = ORVAction(session)
    action.register()
