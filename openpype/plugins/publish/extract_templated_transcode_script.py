from __future__ import annotations
from os import remove
from typing import Union, List
from logging import getLogger
from pprint import pformat
from pathlib import Path
from sys import argv


logger = getLogger(__name__)

import nuke  # type: ignore
from nuke import Node  # type: ignore
from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_last_version_by_subset_id,
)
from openpype.pipeline import register_loader_plugin_path, register_creator_plugin_path
from openpype.pipeline.load import (
    loaders_from_representation,
    load_with_repre_context,
    get_loaders_by_name,
)
import openpype.hosts.nuke as OPnuke


PLUGINS_DIR = Path(OPnuke.__file__).parent / "plugins"
LOAD_PATH = (PLUGINS_DIR / "load").as_posix()
CREATE_PATH = (PLUGINS_DIR / "create").as_posix()


def remove_file_if_exists(file: Union[str, Path]):
    """Remove a file if file exists."""

    if Path(file).exists():
        logger.debug(f"Removing existing file: {file}")
        remove(file)


def load_node(loader: str, subset_name: str, repre_name: str, prj: str, asset: str):
    asset_doc = get_asset_by_name(prj["name"], asset)
    subset_doc = get_subset_by_name(prj["name"], subset_name, asset_doc["_id"])
    version_doc = get_last_version_by_subset_id(prj["name"], subset_doc["_id"])
    repre_doc = get_representation_by_name(prj["name"], repre_name, version_doc["_id"])

    repre_context = {
        "project": {"name": prj["name"], "code": prj["code"]},
        "asset": asset_doc,
        "subset": subset_doc,
        "version": version_doc,
        "representation": repre_doc,
    }

    all_loaders = get_loaders_by_name()
    if loader:
        loader = all_loaders[loader]
    else:
        loader = loaders_from_representation(all_loaders.values(), repre_doc)[0]
    return load_with_repre_context(loader, repre_context)


def create_read_node(data: dict):
    """Creates read node based on data in `data`."""

    read: Node = nuke.nodes.Read()

    if data["input_is_sequence"]:
        read["first"].setValue(data["frameStart"])
        read["last"].setValue(data["frameEnd"])
        read["origfirst"].setValue(data["frameStart"])
        read["origlast"].setValue(data["frameEnd"])
    else:
        read["first"].setValue(1)
        read["last"].setValue(data["frameEnd"] - data["frameStart"] + 1)
        read["origfirst"].setValue(1)
        read["origlast"].setValue(data["frameEnd"] - data["frameStart"] + 1)
    read["frame_mode"].setValue("start at")
    read["frame"].setValue(str(data["frameStart"]))
    read["file"].setValue(data["input_path"])

    logger.info(f"Created read node '{read.name()}'")

    return read


def create_reformat_node(data: dict):
    """Creates a reformat node based on data in `data`."""

    reformat_options = data["profile_data"]["reformat_options"]

    reformat: Node = nuke.nodes.Reformat()
    reformat["type"].setValue("to box")
    reformat["box_fixed"].setValue(True)
    reformat["box_width"].setValue(reformat_options["reformat_width"])
    reformat["box_height"].setValue(reformat_options["reformat_height"])
    reformat["black_outside"].setValue(reformat_options["reformat_black_outside"])
    reformat["resize"].setValue(reformat_options["reformat_type"])
    reformat["filter"].setValue("Lanczos6")
    reformat["clamp"].setValue(True)
    reformat["center"].setValue(True)

    logger.info(f"Created reformat node '{reformat.name()}'")

    return reformat


def create_timecode_node(data: dict):
    """Creates a timecode node based on data in `data`."""

    timecode_node: Node = nuke.nodes.AddTimeCode()
    timecode_node["startcode"].setValue(data["timecode"])
    timecode_node["metafps"].setValue(False)
    timecode_node["useFrame"].setValue(True)
    timecode_node["fps"].setValue(data["fps"])
    timecode_node["frame"].setValue(data["frameStart"])

    logger.info(f"Created timecode node '{timecode_node.name()}'")

    return timecode_node


def create_write_node(data: dict) -> Node:
    """Creates a write node based on data in `data`."""

    write: Node = nuke.nodes.Write(file=data["output_path"])

    write["name"].setValue("WRITE_TRANSCODE")
    write["create_directories"].setValue(True)
    write["use_limit"].setValue(True)
    write["first"].setValue(data["frameStart"])
    write["last"].setValue(data["frameEnd"])
    write["file_type"].setValue(Path(data["output_path"]).suffix[1:])

    # adding knob data from options
    if data["profile_data"]["write_options"].get("knobs"):
        for option in data["profile_data"]["write_options"]["knobs"]:
            write[option["name"]].setValue(option["value"])

    logger.info(f"Created write node '{write.name()}'")

    return write


def create_subset_chain(data: dict):
    """Creates a chain of node based on data in `data`."""

    result: List[Node] = list()
    for subset in data["profile_data"]["subset_chain"]:
        loader_name = subset["loader"]
        subset_name = subset["subset"]
        repre_name = subset["representation"]
        subset_node: Node = load_node(
            loader_name, subset_name, repre_name, data["project"], data["asset"]
        )
        result.append(subset_node)

        logger.info(f"'{subset_node.name()}' node created.")

    return result


def connect_nodes_in_list(node_list: List[Node]):
    """Connect an ordered series of Nuke nodes."""

    for i, node in enumerate(node_list):

        if i == 0:
            continue

        prev_node = node_list[i - 1]
        node.setInput(0, prev_node)
        logger.info(f"'{node.name()}' node input connected to '{prev_node.name()}'.")


def transcode_template(data: dict):
    """Procedure Template from the Extract Templated Transcode settings."""

    remove_file_if_exists(data["save_path"])

    logger.info("\nStart Template:\n")

    nuke.nodePaste(data["profile_data"]["template_path"]["template"])

    input_node = None
    output_node = None
    placeholders = []

    for n in nuke.allNodes():
        if n.knob("is_placeholder"):
            placeholders.append(n)
        if n.Class() == "Input":
            input_node = n
        if n.Class() == "Output":
            output_node = n

    if output_node is None:
        raise ValueError("FATAL: Output Node not detected in template!")

    for p in placeholders:

        if p["plugin_identifier"].getValue() == "nuke.create":
            logger.warning(
                f"Placeholder {p['name'].getValue()} is a a nuke.create identifier,"
                "creators not supported!"
            )
            continue

        new_node = load_node(
            p["loader"].getValue(),
            p["subset"].getValue(),
            p["representation"].getValue(),
            data["project"],
            data["asset"],
        )

        for i in range(p.inputs()):
            if p.input(i):
                new_node.setInput(i, p.input(i))

        for dep in p.dependent(nuke.INPUTS):
            for i in range(dep.inputs()):
                if dep.input(i) == p:
                    dep.setInput(i, new_node)

    if input_node:
        read_node = create_read_node(data)
        read_node["raw"].setValue(True)

        for dep in input_node.dependent(nuke.INPUTS):
            for i in range(dep.inputs()):
                if dep.input(i) == input_node:
                    dep.setInput(i, read_node)

    write_node = create_write_node(data)
    write_node["raw"].setValue(True)

    reformat_node = None
    if data["profile_data"]["reformat_options"]["enabled"]:
        reformat_node = create_reformat_node(data)
    reformat_node.setInput(0, output_node.input(0))

    timecode_node = None
    if data.get("timecode", None):
        timecode_node = create_timecode_node(data)
        if reformat_node:
            timecode_node.setInput(0, reformat_node)
        else:
            timecode_node.setInput(0, output_node.input(0))

    if timecode_node:
        write_node.setInput(0, timecode_node)
    elif reformat_node:
        write_node.setInput(0, reformat_node)
    else:
        write_node.setInput(0, output_node.input(0))

    for p in placeholders:
        nuke.delete(p)
    nuke.delete(input_node)
    nuke.delete(output_node)

    logger.info("\nEnd Template.\n")

    return write_node


def transcode_subsetchain(data):
    """Procedure Subset Chain from the Extract Templated Transcode settings."""

    logger.info("\nStart Subset Chain process:\n")

    remove_file_if_exists(data["save_path"])

    node_list: List[Node] = list()

    read_node = create_read_node(data)
    read_node["raw"].setValue(True)
    node_list = [read_node]

    node_list.extend(create_subset_chain(data))

    if data["profile_data"]["reformat_options"]["enabled"]:  # handle reformat options
        node_list.append(create_reformat_node(data))

    if data.get("timecode", None):  # handle timecode options
        node_list.append(create_timecode_node(data))

    write_node = create_write_node(data)
    write_node["raw"].setValue(True)
    node_list.append(write_node)

    connect_nodes_in_list(node_list)

    logger.info("\nEnd Subset Chain.\n")

    return write_node


def transcode_color_conversion(data: dict):
    """Procedure Color Conversion from the Extract Templated Transcode settings."""

    logger.info("\nStart Color Conversion Procedure:\n")

    remove_file_if_exists(data["save_path"])

    input_color_space = data["profile_data"]["color_conversion"]["input_colorspace"]
    output_color_space = data["profile_data"]["color_conversion"]["output_colorspace"]

    read_node = create_read_node(data)
    read_node["colorspace"].setValue(input_color_space)
    node_list = [read_node]

    if data["profile_data"]["reformat_options"]["enabled"]:  # handle reformat options
        node_list.append(create_reformat_node(data))

    if data.get("timecode", None):  # handle timecode options
        node_list.append(create_timecode_node(data))

    # create write node and connect nodes
    write_node = create_write_node(data)
    write_node["colorspace"].setValue(output_color_space)
    node_list.append(write_node)

    connect_nodes_in_list(node_list)

    logger.info("\nEnd Color Conversion.\n")

    return write_node


def install_nukepy():
    """Install OpenPype's plugins for this Nuke session."""

    register_loader_plugin_path(LOAD_PATH)
    register_creator_plugin_path(CREATE_PATH)
    logger.info("Registered Nuke plug-ins..")


def set_root_node_attributes(data: dict):
    """Transfer base instance values from the input data to Nuke's root node."""

    root_node = nuke.root()
    root_node["first_frame"].setValue(data["frameStart"])
    root_node["last_frame"].setValue(data["frameEnd"])
    root_node["fps"].setValue(data["fps"])
    root_node["colorManagement"].setValue("OCIO")
    root_node["OCIO_config"].setValue("custom")
    root_node["customOCIOConfigPath"].setValue(data["color_config"])


def process_thumb(node: Node, data: dict):

    logger.info("\nStart Thumbnail:\n")

    frame_number = int(data["frameEnd"] - data["frameStart"] // 2) + data["frameStart"]

    thumb_read: Node = nuke.nodes.Read(file=node["file"].getValue())
    thumb_read["name"].setValue("READ_THUMB")
    thumb_read["first"].setValue(data["frameStart"])
    thumb_read["last"].setValue(data["frameEnd"])
    thumb_read["raw"].setValue(True)

    thumb_write: Node = nuke.nodes.Write(file=data["thumbnail_path"])
    thumb_write["name"].setValue("WRITE_THUMB")
    thumb_write["create_directories"].setValue(True)
    thumb_write["use_limit"].setValue(True)
    thumb_write["first"].setValue(frame_number)
    thumb_write["last"].setValue(frame_number)
    thumb_write["raw"].setValue(True)
    thumb_write["file_type"].setValue("jpg")
    thumb_write.setInput(0, thumb_read)

    if thumb_write:
        nuke.execute(thumb_write, start=frame_number, end=frame_number)
    else:
        raise ValueError("Can't render, invalid write node!")

    logger.info("\nEnd Thumbnail.\n")


def main(data: dict):
    """Main procedure for this Nuke script."""

    logger.info(f"Transcode with arguments: {pformat(data)}")

    set_root_node_attributes(data)

    if data["mode"] == "template":
        write_node = transcode_template(data)
    elif data["mode"] == "chain_subsets":
        write_node = transcode_subsetchain(data)
    elif data["mode"] == "color_conversion":
        write_node = transcode_color_conversion(data)
    else:
        raise ValueError("Data not valid!")

    saved = nuke.scriptSave(data["save_path"])
    if not saved:
        raise OSError("Could not save script file!")

    if write_node:
        if write_node.knob("metadata"):
            write_node["metadata"].setValue("all metadata")
        if write_node.knob("noprefix"):
            write_node["noprefix"].setValue(True)
        nuke.execute(write_node)

    else:
        raise ValueError("Can't render, invalid write node!")

    return write_node


if __name__ == "__main__":
    import json
    from logging import basicConfig, DEBUG, INFO

    basicConfig(level=DEBUG)

    logger.debug(f"Args are: {argv}")

    with open(argv[-1], "r") as ftr:
        json_data = json.loads(ftr.read())

    install_nukepy()

    main_write_node = main(json_data)

    if json_data["profile_data"]["override_thumbnail"]:
        process_thumb(main_write_node, json_data)

    nuke.scriptExit()

    quit()
