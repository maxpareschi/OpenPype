import os
import re
import json
import getpass
from subprocess import run
from re import findall
from pathlib import Path

import requests
import pyblish.api

import nuke # type: ignore
from openpype.pipeline import legacy_io


class GatherSubmitDeadline(pyblish.api.InstancePlugin):
    """Submit gather to deadline."""

    label = "Submit gather to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["traypublisher", "nuke"]
    # families = ["render.farm", "prerender.farm"]
    families = ["gather.farm"]
    # families = []
    optional = True
    targets = ["local"]

    # presets
    priority = 50
    chunk_size = 1
    concurrent_tasks = 1
    group = ""
    department = ""
    use_gpu = False
    env_allowed_keys = []
    env_search_replace_values = {}

    def process(self, instance):
        instance.data["toBeRenderedOn"] = "deadline"
        families = instance.data["families"]

        node = instance[0]
        context = instance.context

        deadline_url = instance.context.data["defaultDeadline"]
        deadline_url = instance.data.get("deadlineUrl", deadline_url)
        assert deadline_url, "Requires Deadline Webservice URL"

        self.deadline_url = f"{deadline_url}/api/jobs"
        self._comment = context.data.get("comment", "")
        self._ver = re.search(r"\d+\.\d+", context.data.get("hostVersion"))
        self._deadline_user = context.data.get( "deadlineUser", getpass.getuser())
        submit_frame_start = int(instance.data["frameStartHandle"])
        submit_frame_end = int(instance.data["frameEndHandle"])

        # get output path
        render_path = instance.data['path']
        self.log.info(f">>>> Render path is {render_path}")
        script_path = context.data["currentFile"]


        # self.expected_files(
        #     instance,
        #     render_path,
        #     submit_frame_start,
        #     submit_frame_end
        # )

        for item in context:
            if "workfile" in item.data["families"]:
                msg = "Workfile (scene) must be published along"
                assert item.data["publish"] is True, msg

                template_data = item.data.get("anatomyData")
                rep = item.data["representations"][0].get("name")
                template_data["representation"] = rep
                template_data["ext"] = rep
                template_data["comment"] = None
                anatomy_filled = context.data["anatomy"].format(template_data)
                template_filled = anatomy_filled["publish"]["path"]
                script_path = os.path.normpath(template_filled)

                self.log.info(f"Using published scene for render {script_path}")

        # response = self.payload_submit(
        #     instance,
        #     script_path,
        #     render_path,
        #     node.name(),
        #     submit_frame_start,
        #     submit_frame_end
        # )

        response = self.create_fake_deadline_job(instance, render_path)

        instance.data["deadlineSubmissionJob"] = response.json()
        instance.data["outputDir"] = os.path.dirname(
            render_path).replace("\\", "/")
        instance.data["publishJobState"] = "Suspended"

        self.redefine_families(instance, families)

    def create_fake_deadline_job( self, instance, render_path):
        """Creates a Deadline job that creates an image at $home/deleteme_image.bmp''"""
        args = [
            "Add-Type -AssemblyName System.Drawing",
            "$filename = \\<QUOTE><PLACEHOLDER_FILE>\\<QUOTE>",
            "$bmp = new-object System.Drawing.Bitmap 250,61",
            "$font = new-object System.Drawing.Font Consolas,24",
            "$brushBg = [System.Drawing.Brushes]::Yellow, [System.Drawing.Brushes]::Blue, [System.Drawing.Brushes]::Orange, [System.Drawing.Brushes]::Green, [System.Drawing.Brushes]::Red | Get-Random",
            "$brushFg = [System.Drawing.Brushes]::Black",
            "$graphics = [System.Drawing.Graphics]::FromImage($bmp)",
            "$graphics.FillRectangle($brushBg,0,0,$bmp.Width,$bmp.Height)",
            "$graphics.DrawString('22Dogs',$font,$brushFg,10,10)",
            "$graphics.Dispose()",
            "$bmp.Save($filename)",
            # "Invoke-Item $filename",
            "Write-Host \\<QUOTE>Image created at $filename\\<QUOTE>",
            "if (![System.IO.File]::Exists(\\<QUOTE><PLACEHOLDER_FILE>\\<QUOTE>)){Write-Warning \\<QUOTE>File does not exist\\<QUOTE>}"
        ]

        render_dir = os.path.normpath(os.path.dirname(render_path))

        output_filename_0 = self.preview_fname(render_dir)

        # render_dir = Path.home().as_posix()
        # output_filename_0 = render_dir + "/foo.bmp"

        chunk_size = instance.data["deadlineChunkSize"] or self.chunk_size
        concurrent_tasks = instance.data["deadlineConcurrentTasks"] or self.concurrent_tasks
        priority = instance.data["deadlinePriority"] or self.priority
        arguments = "<QUOTE>" + ";".join(args) + "<QUOTE>"
        expected_file = output_filename_0 + ".1001.bmp"
        arguments = arguments.replace("<PLACEHOLDER_FILE>", expected_file)
        payload = {
            "JobInfo": {
                "BatchName": "OP Test for gather and publish in Deadline",
                "Name": "Test publish for OP",
                "UserName": self._deadline_user,
                "Priority": priority,
                "ChunkSize": chunk_size,
                "ConcurrentTasks": concurrent_tasks,
                "Department": self.department,
                "Pool": instance.data.get("primaryPool"),
                "SecondaryPool": instance.data.get("secondaryPool"),
                "Group": self.group,
                "Plugin": "CommandLine",
                "Comment": self._comment,
                "OutputFilename0": output_filename_0,
                "Frames":"1001"

            },
            "PluginInfo": {
                "Executable": "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
                "Arguments": arguments,
            },

            "AuxFiles": [] # Mandatory for Deadline, may be empty
        }

        # Include critical environment variables with submission
        keys = [
            "PYTHONPATH",
            "PATH",
            "AVALON_PROJECT",
            "AVALON_ASSET",
            "AVALON_TASK",
            "AVALON_APP_NAME",
            "FTRACK_API_KEY",
            "FTRACK_API_USER",
            "FTRACK_SERVER",
            "PYBLISHPLUGINPATH",
            "NUKE_PATH",
            "TOOL_ENV",
            "FOUNDRY_LICENSE",
            "OPENPYPE_VERSION"
        ]
        # Add mongo url if it's enabled
        if instance.context.data.get("deadlinePassMongoUrl"):
            keys.append("OPENPYPE_MONGO")

        # add allowed keys from preset if any
        if self.env_allowed_keys:
            keys += self.env_allowed_keys

        environment = dict({key: os.environ[key] for key in keys
                            if key in os.environ}, **legacy_io.Session)

        for _path in os.environ:
            if _path.lower().startswith('openpype_'):
                environment[_path] = os.environ[_path]

        # to recognize job from PYPE for turning Event On/Off
        environment["OPENPYPE_RENDER_JOB"] = "1"

        # finally search replace in values of any key
        if self.env_search_replace_values:
            for key, value in environment.items():
                for _k, _v in self.env_search_replace_values.items():
                    environment[key] = value.replace(_k, _v)

        payload["JobInfo"].update({
            f"EnvironmentKeyValue{index}": f"{key}={environment[key]}"
            for index, key in enumerate(environment)
        })

        plugin = payload["JobInfo"]["Plugin"]
        self.log.info("using render plugin : {}".format(plugin))

        self.log.info("Submitting..")
        self.log.info(json.dumps(payload, indent=4, sort_keys=True))

        instance.data["expectedFiles"] = [Path(expected_file).name]
        self.log.debug(f"__ expectedFiles: `{instance.data['expectedFiles']}`")
        response = requests.post(self.deadline_url, json=payload, timeout=10)

        if not response.ok:
            raise Exception(response.text)

        return response


    def payload_submit(
        self,
        instance,
        script_path,
        render_path,
        exe_node_name,
        start_frame,
        end_frame,
        response_data=None
    ):
        render_dir = os.path.normpath(os.path.dirname(render_path))
        script_name = os.path.basename(script_path)
        jobname = "%s - %s" % (script_name, instance.name)

        output_filename_0 = self.preview_fname(render_path)

        if not response_data:
            response_data = {}

        try:
            # Ensure render folder exists
            os.makedirs(render_dir)
        except OSError:
            pass

        chunk_size = instance.data["deadlineChunkSize"] or self.chunk_size
        concurrent_tasks = instance.data["deadlineConcurrentTasks"] or self.concurrent_tasks
        priority = instance.data["deadlinePriority"] or self.priority

        payload = {
            "JobInfo": {
                "BatchName": script_name, # Top-level group name

                # Asset dependency to wait for at least the scene file to sync.
                # "AssetDependency0": script_path,

                "Name": jobname, # Job name, as seen in Monitor
                "UserName": self._deadline_user,
                "Priority": priority,
                "ChunkSize": chunk_size,
                "ConcurrentTasks": concurrent_tasks,
                "Department": self.department,
                "Pool": instance.data.get("primaryPool"),
                "SecondaryPool": instance.data.get("secondaryPool"),
                "Group": self.group,
                "Plugin": "Nuke",
                "Frames": f"{start_frame}-{end_frame}",
                "Comment": self._comment,

                # Optional, enable double-click to preview rendered
                # frames from Deadline Monitor
                "OutputFilename0": output_filename_0.replace("\\", "/"),

            },
            "PluginInfo": {
                "SceneFile": script_path, # input

                # Output directory and filename
                "OutputFilePath": render_dir.replace("\\", "/"),
                # "OutputFilePrefix": render_variables["filename_prefix"],

                # Mandatory for Deadline
                "Version": self._ver.group(),

                # Resolve relative references
                "ProjectPath": script_path,
                "AWSAssetFile0": render_path,

                "UseGpu": self.use_gpu, # using GPU by default
                "WriteNode": exe_node_name # Only the specific write node is rendered.
            },

            "AuxFiles": [] # Mandatory for Deadline, may be empty
        }

        if response_data.get("_id"):
            payload["JobInfo"].update({
                "JobType": "Normal",
                "BatchName": response_data["Props"]["Batch"],
                "JobDependency0": response_data["_id"],
                "ChunkSize": 99999999
            })

        # Include critical environment variables with submission
        keys = [
            "PYTHONPATH",
            "PATH",
            "AVALON_PROJECT",
            "AVALON_ASSET",
            "AVALON_TASK",
            "AVALON_APP_NAME",
            "FTRACK_API_KEY",
            "FTRACK_API_USER",
            "FTRACK_SERVER",
            "PYBLISHPLUGINPATH",
            "NUKE_PATH",
            "TOOL_ENV",
            "FOUNDRY_LICENSE",
            "OPENPYPE_VERSION"
        ]
        # Add mongo url if it's enabled
        if instance.context.data.get("deadlinePassMongoUrl"):
            keys.append("OPENPYPE_MONGO")

        # add allowed keys from preset if any
        if self.env_allowed_keys:
            keys += self.env_allowed_keys

        environment = dict({key: os.environ[key] for key in keys
                            if key in os.environ}, **legacy_io.Session)

        for _path in os.environ:
            if _path.lower().startswith('openpype_'):
                environment[_path] = os.environ[_path]

        # to recognize job from PYPE for turning Event On/Off
        environment["OPENPYPE_RENDER_JOB"] = "1"

        # finally search replace in values of any key
        if self.env_search_replace_values:
            for key, value in environment.items():
                for _k, _v in self.env_search_replace_values.items():
                    environment[key] = value.replace(_k, _v)

        payload["JobInfo"].update({
            "EnvironmentKeyValue%d" % index: "{key}={value}".format(
                key=key,
                value=environment[key]
            ) for index, key in enumerate(environment)
        })

        plugin = payload["JobInfo"]["Plugin"]
        self.log.info("using render plugin : {}".format(plugin))

        self.log.info("Submitting..")
        self.log.info(json.dumps(payload, indent=4, sort_keys=True))

        # adding expectied files to instance.data
        self.expected_files(
            instance,
            render_path,
            start_frame,
            end_frame
        )

        self.log.debug("__ expectedFiles: `{}`".format(
            instance.data["expectedFiles"]))
        response = requests.post(self.deadline_url, json=payload, timeout=10)

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

    def expected_files(
        self,
        instance,
        path,
        start_frame,
        end_frame
    ):
        """ Create expected files in instance data """

        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        dirname = os.path.dirname(path)
        file = os.path.basename(path)

        if "#" in file:
            pparts = file.split("#")
            padding = "%0{}d".format(len(pparts) - 1)
            file = pparts[0] + padding + pparts[-1]

        if "%" not in file:
            instance.data["expectedFiles"].append(path)
            return

        if instance.data.get("slate"):
            start_frame -= 1

        for i in range(start_frame, (end_frame + 1)):
            instance.data["expectedFiles"].append(
                os.path.join(dirname, (file % i)).replace("\\", "/"))

    @staticmethod
    def redefine_families(instance, families):
        """This method changes the family into 'write' for nuke so it is that
        it is not recogniced by following plugins and thus they are turned off."""


        if "render.farm" in families:
            instance.data['family'] = 'write'
            families.insert(0, "render2d")
        elif "prerender.farm" in families:
            instance.data['family'] = 'write'
            families.insert(0, "prerender")
        elif "gather.farm" in families:
            instance.data['family'] = 'write'
            families.insert(0, "gather")

        instance.data["families"] = families