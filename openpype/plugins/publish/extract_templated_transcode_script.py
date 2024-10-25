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

    print("Start Template:")

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

    if not output_node:
        raise ValueError("FATAL: Output Node not detected in template!")

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
        read["raw"].setValue(True)
        read["file"].setValue(data["input_path"])
        read["frame_mode"].setValue("start at")
        read["frame"].setValue(str(data["frameStart"]))
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

    if data["profile_data"]["write_options"].get("knobs"):
        for option in data["profile_data"]["write_options"]["knobs"]:
            write[option["name"]].setValue(option["value"])
    
    reformat = None
    if data["profile_data"]["reformat_options"]["enabled"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["profile_data"]["reformat_options"]["reformat_width"])
        reformat["box_height"].setValue(data["profile_data"]["reformat_options"]["reformat_height"])
        reformat["black_outside"].setValue(data["profile_data"]["reformat_options"]["reformat_black_outside"])
        reformat["resize"].setValue(data["profile_data"]["reformat_options"]["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        reformat.setInput(0, output_node.input(0))
    
    tcnode = None
    if data.get("timecode", None):
        tcnode = nuke.nodes.AddTimeCode()
        tcnode["startcode"].setValue(data["timecode"])
        tcnode["metafps"].setValue(False)
        tcnode["useFrame"].setValue(True)
        tcnode["fps"].setValue(data["fps"])
        tcnode["frame"].setValue(data["frameStart"])
        if reformat:
            tcnode.setInput(0, reformat)
        else:
            tcnode.setInput(0, output_node.input(0))
    
    if tcnode:
        write.setInput(0, tcnode)
    elif reformat:
        write.setInput(0, reformat)
    else:
        write.setInput(0, output_node.input(0))

    for p in placeholders:
        nuke.delete(p)
    nuke.delete(input_node)
    nuke.delete(output_node)

    print("End Template.")

    return write


def transcode_subsetchain(data):
    if os.path.isfile(data["save_path"]):
        os.remove(data["save_path"])

    print("Start Subset Chain:")

    node_list = []

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
    read["raw"].setValue(True)
    read["file"].setValue(data["input_path"])
    read["frame_mode"].setValue("start at")
    read["frame"].setValue(str(data["frameStart"]))
    node_list.append(read)
    print("'{}' node created.".format(node_list[-1].name()))

    for subset in data["profile_data"]["subset_chain"]:
        loader_name = subset["loader"]
        subset_name = subset["subset"]
        repre_name = subset["representation"]
        subset_node = load_node(loader_name,
                                subset_name,
                                repre_name,
                                data["project"],
                                data["asset"])
        node_list.append(subset_node)
        print("'{}' node created.".format(node_list[-1].name()))

    if data["profile_data"]["reformat_options"]["enabled"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["profile_data"]["reformat_options"]["reformat_width"])
        reformat["box_height"].setValue(data["profile_data"]["reformat_options"]["reformat_height"])
        reformat["black_outside"].setValue(data["profile_data"]["reformat_options"]["reformat_black_outside"])
        reformat["resize"].setValue(data["profile_data"]["reformat_options"]["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        node_list.append(reformat)
        print("'{}' node created.".format(node_list[-1].name()))
    
    if data.get("timecode", None):
        tcnode = nuke.nodes.AddTimeCode()
        tcnode["startcode"].setValue(data["timecode"])
        tcnode["metafps"].setValue(False)
        tcnode["useFrame"].setValue(True)
        tcnode["fps"].setValue(data["fps"])
        tcnode["frame"].setValue(data["frameStart"])
        node_list.append(tcnode)
    
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
    if data["profile_data"]["write_options"].get("knobs"):
        for option in data["profile_data"]["write_options"]["knobs"]:
            write[option["name"]].setValue(option["value"])

    node_list.append(write)
    print("'{}' node created.".format(node_list[-1].name()))

    for node_id, node in enumerate(node_list):
        if node_id == 0:
            continue
        else:
            node.setInput(0, node_list[node_id-1])
            print("'{}' node input connected to '{}' node.".format(
                node.name(), node_list[node_id-1].name()
            ))
    
    print("End Subset Chain.")

    return write


def transcode_color_conversion(data):
    if os.path.isfile(data["save_path"]):
        os.remove(data["save_path"])

    print("Start Color Conversion:")

    node_list = []

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
    read["colorspace"].setValue(data["profile_data"]["color_conversion"]["input_colorspace"])
    read["file"].setValue(data["input_path"])
    read["frame_mode"].setValue("start at")
    read["frame"].setValue(str(data["frameStart"]))
    node_list.append(read)
    print("'{}' node created.".format(node_list[-1].name()))

    if data["profile_data"]["reformat_options"]["enabled"]:
        reformat = nuke.nodes.Reformat()
        reformat["type"].setValue("to box")
        reformat["box_fixed"].setValue(True)
        reformat["box_width"].setValue(data["profile_data"]["reformat_options"]["reformat_width"])
        reformat["box_height"].setValue(data["profile_data"]["reformat_options"]["reformat_height"])
        reformat["black_outside"].setValue(data["profile_data"]["reformat_options"]["reformat_black_outside"])
        reformat["resize"].setValue(data["profile_data"]["reformat_options"]["reformat_type"])
        reformat["filter"].setValue("Lanczos6")
        reformat["clamp"].setValue(True)
        reformat["center"].setValue(True)
        node_list.append(reformat)
        print("'{}' node created.".format(node_list[-1].name()))

    if data.get("timecode", None):
        tcnode = nuke.nodes.AddTimeCode()
        tcnode["startcode"].setValue(data["timecode"])
        tcnode["metafps"].setValue(False)
        tcnode["useFrame"].setValue(True)
        tcnode["fps"].setValue(data["fps"])
        tcnode["frame"].setValue(data["frameStart"])
        node_list.append(tcnode)

    write = nuke.nodes.Write(file = data["output_path"])
    write["name"].setValue("WRITE_TRANSCODE")
    write["create_directories"].setValue(True)
    write["use_limit"].setValue(True)
    write["first"].setValue(data["frameStart"])
    write["last"].setValue(data["frameEnd"])
    write["colorspace"].setValue(data["profile_data"]["color_conversion"]["output_colorspace"])
    write["file_type"].setValue(
        os.path.splitext(data["output_path"])[1].replace(".", "")
    )
    if data["profile_data"]["write_options"].get("knobs"):
        for option in data["profile_data"]["write_options"]["knobs"]:
            write[option["name"]].setValue(option["value"])

    node_list.append(write)
    print("'{}' node created.".format(node_list[-1].name()))

    for node_id, node in enumerate(node_list):
        if node_id == 0:
            continue
        else:
            node.setInput(0, node_list[node_id-1])
            print("'{}' node input connected to '{}' node.".format(
                node.name(), node_list[node_id-1].name()
            ))

    print("End Color Conversion.")

    return write


def install_nukepy():
    print("Registered Nuke plug-ins..")
    register_loader_plugin_path(LOAD_PATH)
    register_creator_plugin_path(CREATE_PATH)


def install_all():
    print("installed loader_plugins...")
    install_nukepy()
    all_loaders = get_loaders_by_name()


def process_all(data):
    print("Transcode with arguments:")
    print(data)
    print("Start Main.")

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
        if write_node.knob("metadata"):
            write_node["metadata"].setValue("all metadata")
        if write_node.knob("noprefix"):
            write_node["noprefix"].setValue(True)
        write_node["render_order"].setValue(1)
        print("End Main.\n")
        return write_node
    else:
        raise ValueError("Write node was malformed, aborting...")
    

def process_thumb(node, data):

    print("Start Thumbnail:")

    frame_number = int((int(data["frameEnd"]) - int(data["frameStart"])) / 2) + int(data["frameStart"])

    thumb_read = nuke.nodes.Read(file = node["file"].getValue())
    thumb_read["name"].setValue("READ_THUMB")
    thumb_read["first"].setValue(data["frameStart"])
    thumb_read["last"].setValue(data["frameEnd"])
    thumb_read["raw"].setValue(True)
    thumb_read["on_error"].setValue("black")
    print("'{}' node created.".format(thumb_read.name()))
    
    thumb_write = nuke.nodes.Write(file = data["thumbnail_path"])
    thumb_write["name"].setValue("WRITE_THUMB")
    thumb_write["create_directories"].setValue(True)
    thumb_write["use_limit"].setValue(True)
    thumb_write["first"].setValue(frame_number)
    thumb_write["last"].setValue(frame_number)
    thumb_write["raw"].setValue(True)
    thumb_write["file_type"].setValue("jpg")
    thumb_write["render_order"].setValue(2)
    print("'{}' node created.".format(thumb_write.name()))
    thumb_write.setInput(0, thumb_read)
    print("'{}' node input connected to '{}' node.".format(
        thumb_write.name(), thumb_read.name()
    ))
    
    if thumb_write:
        print("End Thumbnail.\n")
        return thumb_write
    else:
        raise ValueError("Write node was malformed, aborting...")

    


if __name__ == "__main__":

    json_path = ARGS[-1]

    with open(json_path,"r") as data_json:
        data = json.loads(data_json.read())

    install_all()

    main_write_node = process_all(data)
    
    if data["profile_data"]["override_thumbnail"]:
        thumb_write_node = process_thumb(main_write_node, data)

    saved = nuke.scriptSave(data["save_path"])
    if not saved:
        raise OSError("Could not save script file!")

    nuke.scriptExit()
    
    quit()
