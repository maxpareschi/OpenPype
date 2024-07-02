from sys import path as syspath, argv

import os
import json
import traceback

import hou


HOUDINI_USD_SUBMISSION_SCRIPT = os.path.abspath(__file__)


def create_intermediate_usd(ropnode, file_abs_path, fileperframe=False):
    # Get instance node and upstream LOP node
    loppath = ropnode.parm("loppath").eval()
    staging_dir, file_name = os.path.split(file_abs_path)

    # create the render intermediate node
    rend = hou.node("/out").createNode("usd", node_name="render_intermediate")
    rend.parm("trange").set(ropnode.parm("trange").eval())
    rend.parm("f1").set(ropnode.parm("f1").eval())
    rend.parm("f2").set(ropnode.parm("f2").eval())
    rend.parm("f3").set(ropnode.parm("f3").eval())
    rend.parm("loppath").set(loppath)
    rend.parm("lopoutput").set(file_abs_path)
    rend.parm("fileperframe").set(fileperframe)
    print(f"Writing intermediate render USD '{file_name}' to '{staging_dir}'")

    verbose = not hou.isUIAvailable()
    try:
        rend.render(verbose=verbose, output_progress=verbose)
    except hou.Error as exc:
        traceback.print_exc()
        raise RuntimeError("Render failed: {0}".format(exc))
    assert os.path.exists(file_abs_path), f"Output does not exist: {file_abs_path}"

    rend.destroy() # delete the render intermediate node


if __name__ == "__main__":
    hip_file, rop_path, target, flush = argv[-4:]
    if flush == "--flush":
        flush = True
    else:
        flush = False
    hou.hipFile.load(hip_file)
    rop = hou.node(rop_path)
    create_intermediate_usd(rop, target, fileperframe=flush)