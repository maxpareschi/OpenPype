import os
import json
import getpass
import re

import requests
import pyblish.api

import hou #noqa

from openpype.pipeline import legacy_io
from openpype.hosts.houdini.api.lib import render_rop


class houdiniSubmitUSDRenderDeadline(pyblish.api.InstancePlugin):
    """Submit Solaris USD Render ROPs to Deadline.

    Renders are submitted to a Deadline Web Service as
    registered in deadline module settings.

    Target "local":
        Even though this does *not* render locally this is seen as
        a 'local' submission as it is the regular way of submitting
        a Houdini render locally.

    """

    label = "Submit USD Render to Deadline"
    order = pyblish.api.IntegratorOrder
    hosts = ["houdini"]
    families = ["usdrender"]
    targets = ["local"]

    # presets
    priority = 50
    chunk_size = 1
    concurrent_tasks = 1
    group = ""
    department = ""
    primary_pool = ""
    secondary_pool = ""

    # TODO Implement other deadline stuff
    # limit_groups = {}
    # env_allowed_keys = []
    # env_search_replace_values = {}

    def process(self, instance):

        instance.data["toBeRenderedOn"] = "deadline"

        node = hou.node(instance.data["instance_node"])
        context = instance.context

        # IMPORTANT FOR REVIEW
        instance.data["review"] = node.parm("review").eval()
        instance.data["multipartExr"] = node.parm("multipartExr").eval()
        instance.data["useSequenceForReview"] = True

        # get default deadline webservice url from deadline module
        deadline_url = instance.context.data["defaultDeadline"]
        # if custom one is set in instance, use that
        if instance.data.get("deadlineUrl"):
            deadline_url = instance.data.get("deadlineUrl")
        assert deadline_url, "Requires Deadline Webservice URL"

        self.deadline_url = "{}/api/jobs".format(deadline_url)
        self._comment = context.data.get("comment", "")
        self._deadline_user = context.data.get(
            "deadlineUser", getpass.getuser())
        submit_frame_start = int(
            instance.context.data["frameStartHandle"])
        submit_frame_end = int(
            instance.context.data["frameEndHandle"])
        submit_frame_step = int(
            instance.context.data.get("byFrameStep", 1))

        # get output path
        hou_output_dir = os.path.join(
            os.path.dirname(hou.hipFile.path()),
            instance.context.data["project_settings"]\
                ["houdini"]["RenderSettings"]\
                ["default_render_image_folder"],
            os.path.splitext(hou.hipFile.basename())[0],
            instance.data["variant"]
        ).replace("\\", "/")
        hou_output_file = "{}.<STARTFRAME>.exr".format(
            instance.data["variant"]
        )
        hou_output_abspath = os.path.join(
            hou_output_dir,
            hou_output_file
        ).replace("\\", "/")

        hou_usd_path = self.create_intermediate_usd(instance)

        hou_renderer = node.parm("renderer").eval()

        # check for workfile publishable state
        workfile = self.get_workfile(context)
        if workfile is not None:
            template_data = workfile.data.get("anatomyData")
            rep = workfile.data.get("representations")[0].get("name")
            self.log.debug(rep)
            template_data["representation"] = rep
            template_data["ext"] = rep
            template_data["comment"] = None
            anatomy_filled = context.data["anatomy"].format(template_data)
            template_filled = anatomy_filled["publish"]["path"]
            self.log.debug(template_filled)
            script_path = os.path.normpath(template_filled)
            self.log.info(
                "Using published scene for render {}".format(script_path)
            )
        else:
            raise AttributeError("Workfile (scene) must be published along")

        render_dir = os.path.normpath(os.path.dirname(hou_output_abspath))
        usd_name = os.path.basename(hou_usd_path)
        batchname = "%s - %s" % (
            instance.data["anatomyData"]["project"]["code"],
            usd_name
        )
        jobname = "%s - %s - %s" % (
            instance.data["anatomyData"]["project"]["code"],
            usd_name,
            instance.name
        )

        output_filename_0 = self.preview_fname(hou_output_abspath)

        try:
            # Ensure render folder exists
            os.makedirs(render_dir)
        except OSError:
            pass

        # define vars for deadline
        self.priority = node.parm(
            "priority").eval() or self.priority
        self.chunk_size = node.parm(
            "chunk_size").eval() or self.chunk_size
        self.concurrent_tasks = node.parm(
            "concurrent_tasks").eval() or self.concurrent_tasks
        self.group = node.parm("group").eval() or self.group
        self.department = node.parm(
            "department").eval() or self.department
        self.primary_pool = node.parm(
            "primary_pool").eval() or self.primary_pool
        self.secondary_pool = node.parm(
            "secondary_pool").eval() or self.secondary_pool

        # NEED TO GET LIMITS GROUP FUNCTIONALITY IN AGAIN
        # resolve any limit groups
        # self.log.info("Limit groups: `{}`".format(limit_groups))

        payload = {
            "JobInfo": {
                "BatchName": batchname,
                "Name": jobname,
                "UserName": self._deadline_user,
                "Priority": self.priority,
                "ChunkSize": self.chunk_size,
                "ConcurrentTasks": self.concurrent_tasks,
                "Department": self.department,
                "Pool": self.primary_pool,
                "SecondaryPool": self.secondary_pool,
                "Group": self.group,
                "Plugin": "CommandLine",
                "Frames": "{start}-{end}x{step}".format(
                    start=submit_frame_start,
                    end=submit_frame_end,
                    step=submit_frame_step
                ),
                "Comment": self._comment,
                "OutputFilename0": output_filename_0.replace(
                    "\\", "/").replace("<STARTFRAME>", "####"),
            },
            "PluginInfo": {
                "Arguments": (
                    "-o {0} "
                    "--make-output-path "
                    "--frame <STARTFRAME> "
                    "--renderer {1} "
                    "--verbose Ca2 "
                    "{2}"
                ).format(
                    hou_output_abspath,
                    hou_renderer,
                    hou_usd_path
                ),
                "Executable": "husk.exe",
            },
            "AuxFiles": []
        }

        # Include critical environment variables with submission
        keys = [
            "PYTHONPATH",
            "PATH",
            "AVALON_PROJECT",
            "AVALON_ASSET",
            "AVALON_TASK",
            "AVALON_TOOLS"
            "FTRACK_API_KEY",
            "FTRACK_API_USER",
            "FTRACK_SERVER",
            "PYBLISHPLUGINPATH",
            "HOUDINI_PATH",
            "TOOL_ENV",
            "OPENPYPE_VERSION"
        ]

        # Add mongo url if it's enabled
        if instance.context.data.get("deadlinePassMongoUrl"):
            keys.append("OPENPYPE_MONGO")

        environment = dict({key: os.environ[key] for key in keys
                            if key in os.environ}, **legacy_io.Session)

        environment["AVALON_APP_NAME"] = instance.context.data["appName"]

        # to recognize job from PYPE for turning Event On/Off
        environment["OPENPYPE_RENDER_JOB"] = "1"

        payload["JobInfo"].update({
            "EnvironmentKeyValue%d" % index: "{key}={value}".format(
                key=key,
                value=environment[key].replace("\\", "/")
            ) for index, key in enumerate(environment)
        })

        # adding expectied files to instance.data
        self.expected_files(
            instance,
            hou_output_abspath,
            submit_frame_start,
            submit_frame_end,
            submit_frame_step
        )

        response = self.submit(instance, payload)

        # Store output dir for unified publisher (filesequence)
        instance.data["deadlineSubmissionJob"] = response.json()
        instance.data["outputDir"] = os.path.dirname(
            hou_output_dir).replace("\\", "/")

        # check if tags key is in repre, this is to avoid errors when
        # publishing from deadline
        for repre in instance.data["representations"]:
            if "tags" not in repre:
                repre["tags"] = []


    def submit(self, instance, payload):

        plugin = payload["JobInfo"]["Plugin"]
        self.log.info("using render plugin : {}".format(plugin))

        self.log.info("Submitting..")
        self.log.info(json.dumps(payload, indent=4, sort_keys=True))

        self.log.debug("__ expectedFiles: `{}`".format(
            json.dumps(instance.data["expectedFiles"])))
        response = requests.post(
            self.deadline_url, json=payload, timeout=10)

        if not response.ok:
            raise Exception(response.text)

        return response

    def preflight_check(self, instance):
        """Ensure the startFrame, endFrame and byFrameStep are integers"""

        for key in ("frameStart", "frameEnd"):
            value = instance.data[key]

            if int(value) == value:
                continue

            self.log.warning(
                "%f=%d was rounded off to nearest integer"
                % (value, int(value))
            )

    def preview_fname(self, path):
        """Return output file path with #### for padding.

        Deadline requires the path to be formatted with # in place of numbers.
        For example `/path/to/render.####.png`

        Args:
            path (str): path to rendered images

        Returns:
            str

        """
        self.log.debug("_ path: `{}`".format(path))
        if "%" in path:
            search_results = re.search(r"(%0)(\d)(d.)", path).groups()
            self.log.debug("_ search_results: `{}`".format(search_results))
            return int(search_results[1])
        if "#" in path:
            self.log.debug("_ path: `{}`".format(path))
        return path

    # Fill the expected file list, and also check
    # for replaces with <STARTFRAME> string (needed in
    # commandline deadline plugin)
    def expected_files(
        self,
        instance,
        path,
        start_frame,
        end_frame,
        step_frame
    ):
        """ Create expected files in instance data
        """
        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        dirname = os.path.dirname(path)
        file = os.path.basename(path)

        if "#" in file:
            pparts = file.split("#")
            padding = "%0{}d".format(len(pparts) - 1)
            file = pparts[0] + padding + pparts[-1]

        if "<STARTFRAME>" in file:
            padding = "%0{}d".format(
                len(str(int(
                    hou.node(
                        instance.data["instance_node"]
                    ).parm("f2").eval()
                )))
            )
            file = file.replace("<STARTFRAME>", padding)

        if "%" not in file:
            instance.data["expectedFiles"].append(path)
            return

        for i in range(start_frame, (end_frame + 1), step_frame):
            instance.data["expectedFiles"].append(
                os.path.join(dirname, (file % i)).replace("\\", "/"))

    # this creates a usd intermediate file to be used as a
    # rendering source. Assumes save to be in the same dir as
    # staging_dir/HOU-RENDER-SETTINGS/.
    def create_intermediate_usd(self, instance):

        # get a temp path
        cur_file = hou.hipFile.path().replace("\\", "/")
        file_name = "{}.{}".format(os.path.splitext(
            os.path.basename(cur_file))[0], "usd")
        staging_dir = os.path.join(
            os.path.dirname(cur_file),
            instance.context.data["project_settings"]\
                ["houdini"]["RenderSettings"]\
                ["default_render_image_folder"]).replace("\\", "/")
        file_abs_path = os.path.join(staging_dir,
            file_name).replace("\\", "/")
        
        # create the render intermediate node
        ropnode = hou.node(instance.data.get("instance_node"))
        loppath = ropnode.parm("loppath").eval()
        rend = hou.node("/out").createNode("usd",
            node_name="render_intermediate")
        rend.parm("loppath").set(loppath)
        rend.parm("lopoutput").set(file_abs_path)

        self.log.info("Writing intermediate render USD '%s' to '%s'" % (
            file_name, staging_dir))

        # save the usd file and check for actual file creation
        render_rop(rend)
        assert os.path.exists(file_abs_path),\
            "Output does not exist: %s" % file_abs_path

        # delete the render intermediate node
        rend.destroy()

        if "representations" not in instance.data:
            instance.data["representations"] = []

        representation = {
            'name': 'usd',
            'ext': 'usd',
            'files': file_name,
            "stagingDir": staging_dir,
            # "tags": ["delete"]
            'tags': []
        }
        
        instance.data["representations"].append(representation)
         
        return file_abs_path

    # Checks for workfile integration alongside usdrender
    def get_workfile(self, context):
        for instance in context:
            if "workfile" == instance.data.get("family") or \
               "workfile" in instance.data.get("families", []):
                return instance
        return None