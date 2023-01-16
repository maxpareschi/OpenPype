import re
import os

import hou
import pxr.UsdRender

import pyblish.api


def get_var_changed(variable=None):
    """Return changed variables and operators that use it.

    Note: `varchange` hscript states that it forces a recook of the nodes
          that use Variables. That was tested in Houdini
          18.0.391.

    Args:
        variable (str, Optional): A specific variable to query the operators
            for. When None is provided it will return all variables that have
            had recent changes and require a recook. Defaults to None.

    Returns:
        dict: Variable that changed with the operators that use it.

    """
    cmd = "varchange -V"
    if variable:
        cmd += " {0}".format(variable)
    output, _ = hou.hscript(cmd)

    changed = {}
    for line in output.split("Variable: "):
        if not line.strip():
            continue

        split = line.split()
        var = split[0]
        operators = split[1:]
        changed[var] = operators

    return changed


class CollectRenderProducts(pyblish.api.InstancePlugin):
    """Collect USD Render Products."""

    label = "Collect Render Products"
    order = pyblish.api.CollectorOrder + 0.4
    hosts = ["houdini"]
    families = ["usdrender"]

    def process(self, instance):

        current_file = instance.context.data["currentFile"]

        # get padding based on last frame to be rendered
        # or project frame padding, max value.
        padding = max(
            len(str(int(
                hou.node(
                    instance.data["instance_node"]
                ).parm("f2").eval()
            ))),
            instance.context.data["projectEntity"]["config"]\
                ["templates"]["defaults"]["frame_padding"]
        )

        node = instance.data.get("output_node")
        if not node:
            rop_path = instance.data["instance_node"].path()
            raise RuntimeError(
                "No output node found. Make sure to connect an "
                "input to the USD ROP: %s" % rop_path
            )

        # Workaround Houdini 18.0.391 bug where $HIPNAME doesn't automatically
        # update after scene save.
        if hou.applicationVersion() == (18, 0, 391):
            self.log.debug(
                "Checking for recook to workaround " "$HIPNAME refresh bug..."
            )
            changed = get_var_changed("HIPNAME").get("HIPNAME")
            if changed:
                self.log.debug("Recooking for $HIPNAME refresh bug...")
                for operator in changed:
                    hou.node(operator).cook(force=True)

                # Make sure to recook any 'cache' nodes in the history chain
                chain = [node]
                chain.extend(node.inputAncestors())
                for input_node in chain:
                    if input_node.type().name() == "cache":
                        input_node.cook(force=True)

        stage = node.stage()

        filenames = []
        for prim in stage.Traverse():

            if not prim.IsA(pxr.UsdRender.Product):
                continue

            # Get Render Product Name
            product = pxr.UsdRender.Product(prim)

            prim_path = str(prim.GetPath())

            # We force taking it from any random time sample as opposed to
            # "default" that the USD Api falls back to since that won't return
            # time sampled values if they were set per time sample.
            name = product.GetProductNameAttr().Get(time=0)

            # if name is not set create a name using default data from instance
            # set the temp render dir in
            # workdir/renders/houdini/workfile/variant/variant
            if name == "":
                name = os.path.join(
                    os.path.dirname(current_file),
                    instance.context.data.get('project_settings')\
                        .get('houdini')\
                        .get('RenderSettings')\
                        .get('default_render_image_folder'),
                    os.path.splitext(
                        os.path.basename(current_file)
                    )[0],
                    instance.data["variant"],
                    "{0}.{1}.exr".format(
                        instance.data["variant"],
                        "#" * padding
                    )
                ).replace("\\", "/")

            dirname = os.path.dirname(name)
            basename = os.path.basename(name)

            dollarf_regex = r"(\$F([0-9]?))"
            frame_regex = r"^(.+\.)([0-9]+)(\.[a-zA-Z]+)$"
            if re.match(dollarf_regex, basename):
                # TODO: Confirm this actually is allowed USD stages and HUSK
                # Substitute $F
                def replace(match):
                    """Replace $F4 with padded #."""
                    padding = int(match.group(2)) if match.group(2) else 1
                    return "#" * padding

                filename_base = re.sub(dollarf_regex, replace, basename)
                filename = os.path.join(dirname, filename_base)
            else:
                # Substitute basename.0001.ext
                def replace(match):
                    prefix, frame, ext = match.groups()
                    padding = "#" * len(frame)
                    return prefix + padding + ext

                filename_base = re.sub(frame_regex, replace, basename)
                filename = os.path.join(dirname, filename_base)
                filename = filename.replace("\\", "/")

            assert "#" in filename, (
                "Couldn't resolve render product name "
                "with frame number: %s" % name
            )

            filenames.append(filename)

            self.log.info("Collected %s name: %s" % (prim_path, filename))

        # Enforce just one render product per instance.
        # TODO: Investigate a smarter way to not duplicate render
        # settings for all render products. Sometimes it's desiderable,
        # sometimes it's just a hassle.
        # TODO 02: Investigate on how to allow multiple products on
        # one instance? Probably not worth the effort.
        if len(filenames) > 1:
            raise RuntimeError("Only one render product per instance is allowed!")
        elif len(filenames) == 0:
            raise RuntimeError(
                "No render product found, one is needed for "
                "every render instance"
            )

        # Filenames for Deadline
        instance.data["files"] = filenames
