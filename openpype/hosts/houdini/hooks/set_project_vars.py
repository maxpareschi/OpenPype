import os
from openpype.lib import PostLaunchHook
import hou


class SetProjectVars(PostLaunchHook):
    """Set current dir to workdir.

    Hook `GlobalHostDataHook` must be executed before this hook.
    """
    app_groups = ["houdini"]

    def execute(self):
        pass
        # hou.setEnvVar("$PRJ", os.environ["AVALON_PROJECT"])
        # hou.setEnvVar("$PRJ", self.launch_context.data)