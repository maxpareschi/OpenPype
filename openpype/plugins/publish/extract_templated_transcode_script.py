import sys

ARGS = sys.argv

import os
import json
import nuke

from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_representation_by_name,
    get_last_version_by_subset_id,
)

from openpype.pipeline import (
    register_loader_plugin_path,
    register_creator_plugin_path
)

from openpype.pipeline.load import (
    loaders_from_representation,
    load_with_repre_context,
    get_loaders_by_name
)

import openpype.hosts.nuke as OPnuke


HOST_DIR = os.path.dirname(os.path.abspath(OPnuke.__file__))
PLUGINS_DIR = os.path.join(HOST_DIR, "plugins")
PUBLISH_PATH = os.path.join(PLUGINS_DIR, "publish")
LOAD_PATH = os.path.join(PLUGINS_DIR, "load")
CREATE_PATH = os.path.join(PLUGINS_DIR, "create")


def load_node(loader_name, subset_name, repre_name, project, asset):
    asset_doc = get_asset_by_name(
        project["name"],
        asset
    )
    subset_doc = get_subset_by_name(
        project["name"],
        subset_name,
        asset_doc["_id"]
    )
    version_doc = get_last_version_by_subset_id(
        project["name"],
        subset_doc["_id"]
    )
    repre_doc = get_representation_by_name(
        project["name"],
        repre_name,
        version_doc["_id"]
    )
    repre_context = {
        "project": {
            "name": project["name"],
            "code": project["code"]
        },
        "asset": asset_doc,
        "subset": subset_doc,
        "version": version_doc,
        "representation": repre_doc,
    }
    all_loaders = get_loaders_by_name()
    if loader_name:
        loader = all_loaders[loader_name]
    else:
        loader = loaders_from_representation(all_loaders.values(), repre_doc)[0]
    return load_with_repre_context(loader, repre_context)


def transcode_template(data):

    if os.path.isfile(data["save_path"]):
        os.remove(data["save_path"])

    nuke.nodePaste(data["template"])

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

    if not output_node:
        raise ValueError("FATAL: Output Node detected in template!")

    for p in placeholders:
        if p["plugin_identifier"].getValue() == "nuke.create":
            print("Placeholder {} is a a nuke.create identifier, creators not supported!".format(
                p["name"].getValue()
            ))
            continue
        new_node = load_node(p["loader"].getValue(),
                        p["subset"].getValue(),
                        p["representation"].getValue(),
                        data["project"],
                        data["asset"])

        for i in range(p.inputs()):
            if p.input(i):
                new_node.setInput(i, p.input(i))

        for dep in p.dependent(nuke.INPUTS):
            for i in range(dep.inputs()):
                if dep.input(i) == p:
                    dep.setInput(i, new_node)

    if input_node:
        read = nuke.nodes.Read()
        if data["input_is_sequence"]:
            read["first"].setValue(data["frameStart"])
            read["last"].setValue(data["frameEnd"])
            read["origfirst"].setValue(data["frameStart"])
            read["origlast"].setValue(data["frameEnd"])
        else:
            read["first"].setValue(1)
            read["last"].setValue(data["frameEnd"]-data["frameStart"]+1)
            read["origfirst"].setValue(1)
            read["origlast"].setValue(data["frameEnd"]-data["frameStart"]+1)
        read["frame_mode"].setValue("start at")
        read["frame"].setValue(str(data["frameStart"]))
        read["raw"].setValue(True)
        read["file"].setValue(data["input_path"])
        for dep in input_node.dependent(nuke.INPUTS):
            for i in range(dep.inputs()):
                if dep.input(i) == input_node:
                    dep.setInput(i, read)

    write = nuke.nodes.Write(file = data["output_path"])
    write["name"].setValue("WRITE_TRANSCODE")
    write["create_directories"].setValue(True)
    write["use_limit"].setValue(True)
    write["first"].setValue(data["frameStart"])
    write["last"].setValue(data["frameEnd"])
    write["raw"].setValue(True)
    write["file_type"].setValue(
        os.path.splitext(data["output_path"])[1].replace(".", "")
    )
    try:
        write["metadata"].setValue("all metadata")
    except:
        print("\nNo metadata type to be set.")
        
    if data["reformat"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["reformat_width"])
        reformat["box_height"].setValue(data["reformat_height"])
        reformat["resize"].setValue(data["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        reformat.setInput(0, output_node.input(0))
        write.setInput(0, reformat)
    else:
        write.setInput(0, output_node.input(0))

    for p in placeholders:
        nuke.delete(p)
    nuke.delete(input_node)
    nuke.delete(output_node)

    saved = nuke.scriptSave(data["save_path"])

    if not saved:
        raise OSError("Could not save script file!")

    return write


def transcode_subsetchain(data):
    if os.path.isfile(data["save_path"]):
        os.remove(data["save_path"])

    read = nuke.nodes.Read()
    if data["input_is_sequence"]:
        read["first"].setValue(data["frameStart"])
        read["last"].setValue(data["frameEnd"])
        read["origfirst"].setValue(data["frameStart"])
        read["origlast"].setValue(data["frameEnd"])
    else:
        read["first"].setValue(1)
        read["last"].setValue(data["frameEnd"]-data["frameStart"]+1)
        read["origfirst"].setValue(1)
        read["origlast"].setValue(data["frameEnd"]-data["frameStart"]+1)
    read["frame_mode"].setValue("start at")
    read["frame"].setValue(str(data["frameStart"]))
    read["raw"].setValue(True)
    read["file"].setValue(data["input_path"])

    node_list = [
        read
    ]
    print("'{}' is the last tagged node.".format(node_list[-1]["name"].getValue()))

    for subset in data["subset_chain"]:
        loader_name = subset["loader"]
        subset_name = subset["subset"]
        repre_name = subset["representation"]
        
        subset_node = load_node(loader_name,
                                subset_name,
                                repre_name,
                                data["project"],
                                data["asset"])
        
        print("Created subset chain node: {}".format(subset_node["name"].getValue()))
        print("Connecting input 0 of node '{}' to last known node: '{}'.".format(
                subset_node["name"].getValue(),
                node_list[-1]["name"].getValue()
            )
        )
        print(nuke.toNode(node_list[-1]["name"].getValue())["name"].getValue())
        subset_node.setInput(0, nuke.toNode(node_list[-1]["name"].getValue()))
        print("input 0 was set to node {}".format(subset_node.input(0)["name"].getValue()))
        node_list.append(subset_node)
        print("'{}' is the last tagged node.".format(node_list[-1]["name"].getValue()))
        print("Node list until now: {}".format(node_list))

    if data["reformat"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["reformat_width"])
        reformat["box_height"].setValue(data["reformat_height"])
        reformat["resize"].setValue(data["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        print("Connecting input 0 of node '{}' to last known node: '{}'.".format(
                reformat["name"].getValue(),
                node_list[-1]["name"].getValue()
            )
        )
        reformat.setInput(0, node_list[-1])
        print("input 0 was set to node {}".format(reformat.input(0)["name"].getValue()))
        node_list.append(reformat)
        print("'{}' is the last tagged node.".format(node_list[-1]["name"].getValue()))
        print("Node list until now: {}".format(node_list))

    write = nuke.nodes.Write(file = data["output_path"])
    write["name"].setValue("WRITE_TRANSCODE")
    write["create_directories"].setValue(True)
    write["use_limit"].setValue(True)
    write["first"].setValue(data["frameStart"])
    write["last"].setValue(data["frameEnd"])
    write["raw"].setValue(True)
    write["file_type"].setValue(
        os.path.splitext(data["output_path"])[1].replace(".", "")
    )
    print("Connecting input 0 of node '{}' to last known node: '{}'.".format(
            write["name"].getValue(),
            node_list[-1]["name"].getValue()
        )
    )
    try:
        write["metadata"].setValue("all metadata")
    except:
        print("\nNo metadata type to be set.")
    write.setInput(0, node_list[-1])
    print("input 0 was set to node {}".format(write.input(0)["name"].getValue()))
    node_list.append(write)
    print("'{}' is the last tagged node.".format(node_list[-1]["name"].getValue()))
    print("Node list until now: {}".format(node_list))
    

    print("Created nodes layout:\n NAME | CLASS | DEPENDECIES")
    for n in nuke.allNodes():
        print("{} | {} | {}".format(
                n["name"].getValue(),
                n.Class(),
                n.dependent(nuke.INPUTS)
            )
        )

    saved = nuke.scriptSave(data["save_path"])
    if not saved:
        raise OSError("Could not save script file!")

    return write


def transcode_color_conversion(data):
    if os.path.isfile(data["save_path"]):
        os.remove(data["save_path"])

    read = nuke.nodes.Read()
    if data["input_is_sequence"]:
        read["first"].setValue(data["frameStart"])
        read["last"].setValue(data["frameEnd"])
        read["origfirst"].setValue(data["frameStart"])
        read["origlast"].setValue(data["frameEnd"])
    else:
        read["first"].setValue(1)
        read["last"].setValue(data["frameEnd"]-data["frameStart"]+1)
        read["origfirst"].setValue(1)
        read["origlast"].setValue(data["frameEnd"]-data["frameStart"]+1)
    read["frame_mode"].setValue("start at")
    read["frame"].setValue(str(data["frameStart"]))
    read["colorspace"].setValue(data["input_colorspace"])
    read["file"].setValue(data["input_path"])
    last_node = read

    if data["reformat"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["reformat_width"])
        reformat["box_height"].setValue(data["reformat_height"])
        reformat["resize"].setValue(data["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        reformat.setInput(0, read)
        last_node = reformat

    write = nuke.nodes.Write(file = data["output_path"])
    write["name"].setValue("WRITE_TRANSCODE")
    write["create_directories"].setValue(True)
    write["use_limit"].setValue(True)
    write["first"].setValue(data["frameStart"])
    write["last"].setValue(data["frameEnd"])
    write["colorspace"].setValue(data["output_colorspace"])
    write["file_type"].setValue(
        os.path.splitext(data["output_path"])[1].replace(".", "")
    )
    try:
        write["metadata"].setValue("all metadata")
    except:
        print("\nTranscode: No metadata type to be set.")
    write.setInput(0, last_node)

    saved = nuke.scriptSave(data["save_path"])
    if not saved:
        raise OSError("Could not save script file!")

    return write


def install_nukepy():
    # pyblish.api.register_host("nuke")
    print("Registering Nuke for plug-ins..")
    # pyblish.api.register_plugin_path(PUBLISH_PATH)
    register_loader_plugin_path(LOAD_PATH)
    register_creator_plugin_path(CREATE_PATH)


def install_all():
    print("installing loader_plugins...")
    install_nukepy()
    all_loaders = get_loaders_by_name()
    print(all_loaders)


def process_all(data):
    print("\nExecuting Transcode with arguments:")
    print("\n{}".format(json.dumps(data, indent=4, default=str)))

    write_node = None

    root_node = nuke.root()
    root_node["first_frame"].setValue(data["frameStart"])
    root_node["last_frame"].setValue(data["frameEnd"])
    root_node["fps"].setValue(data["fps"])
    root_node["colorManagement"].setValue("OCIO")
    root_node["OCIO_config"].setValue("custom")
    root_node["customOCIOConfigPath"].setValue(data["color_config"])

    if data["mode"] == "template":
        write_node = transcode_template(data)
    elif data["mode"] == "chain_subsets":
        write_node = transcode_subsetchain(data)
    elif data["mode"] == "color_conversion":
        write_node = transcode_color_conversion(data)
    else:
        raise ValueError("Data not valid!")

    if write_node:
        nuke.execute(write_node)
    else:
        raise ValueError("Can't render, invalid write node!")
    
    return write_node
    

def process_thumb(node, data):
    frame_number = int(data["frameEnd"] - data["frameStart"] / 2) + data["frameStart"]
    thumb_write = nuke.nodes.Write(file = data["thumbnail_path"])
    thumb_write["name"].setValue("WRITE_THUMB")
    thumb_write["create_directories"].setValue(True)
    thumb_write["use_limit"].setValue(True)
    thumb_write["first"].setValue(frame_number)
    thumb_write["last"].setValue(frame_number)
    thumb_write["raw"].setValue(True)
    thumb_write["file_type"].setValue(
        os.path.splitext(data["output_path"])[1].replace(".", "")
    )
    try:
        thumb_write["metadata"].setValue("all metadata")
    except:
        print("\nThumbnail: No metadata type to be set.")
    thumb_write.setInput(0, node)
    
    if thumb_write:
        nuke.execute(thumb_write, start=frame_number, end=frame_number)
    else:
        raise ValueError("Can't render, invalid write node!")


if __name__ == "__main__":

    json_path = ARGS[1]

    with open(json_path,"r") as data_json:
        data = json.loads(data_json.read())

    log = open(os.path.splitext(data["save_path"])[0] + ".log", "w")
    old_stdout = sys.stdout
    sys.stdout = log
    
    install_all()
    main_write_node = process_all(data)
    if data["override_thumbnail"]:
        process_thumb(main_write_node, data)

    nuke.scriptExit()

    sys.stdout = old_stdout
    log.close()
    
    quit()
