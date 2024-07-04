import os
import getpass

import pyblish.api

from openpype.pipeline import legacy_io
import copy

class GatherSubmitDeadline(pyblish.api.InstancePlugin):
    """Submit gather to deadline."""

    label = "Submit gather to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    families = ["gather.farm"]
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

        context = instance.context

        deadline_url = instance.context.data["defaultDeadline"]
        deadline_url = instance.data.get("deadlineUrl", deadline_url)
        assert deadline_url, "Requires Deadline Webservice URL"

        self.deadline_url = f"{deadline_url}/api/jobs"
        self._comment = context.data.get("comment", None)
        
        if not str(self._comment).strip():
            self._comment = " "
        
        instance.context.data["comment"] = self._comment
        self._deadline_user = context.data.get( "deadlineUser", getpass.getuser())

        # get output path
        render_path = os.path.dirname(instance.data.get("source"))
        self.log.info(f">>>> Render path is {render_path}")
        context.data["currentFile"] = ""

        legacy_io.Session["AVALON_TASK"] = instance.data['task']
        legacy_io.Session["AVALON_ASSET"] = instance.data['asset']

        batch = f"{instance.data['project']} - {instance.data['asset']} - "
        batch += f"{instance.data['task']} - {instance.data['subset']} - "
        batch += f"v{instance.data['version']:03}"

        instance.data["deadlineSubmissionJob"] = {
            "Props" : {
                "Batch": batch,
                "User": context.data.get("deadlineUser", getpass.getuser()),
            }
        }
        
        instance.data["outputDir"] = os.path.normpath(render_path)
        instance.data["gather_json_location"] = os.path.normpath(render_path)
        instance.data["publishJobState"] = "Suspended"
        instance.context.data["version"] = instance.data["version"]

        files = copy.deepcopy(instance.data["gather_representation_files"])

        if isinstance(files, str):
            files = [files]

        for file in files:
            self.expected_files(
                instance,
                file,
                instance.data["frameStart"],
                instance.data["frameEnd"]
            )

        instance.data["families"] = list(set(instance.data["families"]))
    
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

        # if instance.data.get("slate"):
        #     start_frame -= 1

        for i in range(start_frame, (end_frame + 1)):
            instance.data["expectedFiles"].append(
                os.path.join(dirname, (file % i)).replace("\\", "/"))


        

