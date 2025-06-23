import os
import copy
from openpype.lib import PreLaunchHook
# import json

class AddPythonOptionals(PreLaunchHook):
    """Set current dir to workdir.

    Hook `GlobalHostDataHook` must be executed before this hook.
    """
    app_groups = ["houdini"]

    def execute(self):

        env = copy.deepcopy(self.launch_context.env)

        python_env_bin_folder = None
        pythonpath = env.get("PYTHONPATH", [])
        openpype_root = os.getenv("OPENPYPE_REPOS_ROOT")

        if self.application.name.find("20.5") == 0 or self.application.name.find("20-5") == 0:
            python_env_folder = "python_311"
            python_env_bin_folder = os.path.join(openpype_root, "openpype", "vendor", "bin", "otio_311")
        elif self.application.name.find("20") == 0:
            python_env_folder = "python_310"
        elif self.application.name.find("19") == 0:
            python_env_folder = "python_39"
        else:
            python_env_folder = "python_3"
        
        python_optional_dir = os.path.normpath(
            os.path.join(
                openpype_root,
                "openpype",
                "vendor",
                "python",
                python_env_folder
            )
        )
        if pythonpath:
            pythonpath = [path for path in pythonpath.split(os.pathsep) if path]
        pythonpath = list(set(pythonpath))
        if python_env_bin_folder is not None:
            pythonpath.insert(0, python_env_bin_folder)
        pythonpath.insert(0, python_optional_dir)

        env["PYTHONPATH"] = os.pathsep.join(pythonpath)
        self.launch_context.env = env
        self.log.debug("Added '{}' to launch context PYTHONPATH".format(python_optional_dir))
        self.log.debug("launch context full PYTHONPATH: \n\t- {}\n".format("\n\t- ".join(self.launch_context.env["PYTHONPATH"].split(os.pathsep))))
        # self.log.debug("Full env vars dump:{}".format(
        #     json.dumps(
        #         {k: v for k, v in env.items()}, default=str, indent=4
        #     )
        # ))