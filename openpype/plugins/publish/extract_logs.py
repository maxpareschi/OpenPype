# -*- coding: utf-8 -*-
"""Cleanup leftover files from publish."""
import os
import json
import uuid
import pyblish.api


class ExtractLogs(pyblish.api.InstancePlugin):
    """
    Extract logs before integration.

    """

    order = pyblish.api.ExtractorOrder + 0.07
    label = "Extract Logs"
    optional = True
    active = True

    def process(self, instance):
        #if not instance.data.get("publishDir", None):
        #    self.log.warning("No publish dir was set in instance, cannot write log.")
        #    return

        # log_name = instance.data.get('name', 'publish')
        # log_file = f"{instance.data['publishDir']}/{log_name}_log.json"
        # 
        # with open(log_file, "w") as f:
        #     f.write(json.dumps(instance.context.data, indent=4, default=str))
        # 
        # self.log.info(f"Written '{os.path.basename(log_file)}' log file at {log_file}")

        log_file = f"{os.environ.get('TEMP', os.environ.get('TMP'))}/{uuid.uuid4()}.json"

        contents = []

        for result in instance.context.data["results"]:
            contents.append(f">>> --- {result['plugin'].label} ---")
            for record in result["records"]:
                msg = record['msg'].replace('\n', '\n\t')
                contents.append(f"\t{record['name']} - {record['levelname']} - {msg}")
            if result["success"]:
                contents.append(f"<<< --- Plugin completed in: {result['duration']} ms. ---\n")
            elif result["error"]:
                contents.append(f"<<< --- Plugin error! : {result['error']} ms\n")
            

        with open(log_file, "w") as f:
            f.write("\n".join(contents))

        instance.data["representations"].append({
            "name": "log",
            "ext": "log",
            "stagingDir": os.path.dirname(log_file),
            "files": os.path.basename(log_file),
            "tags": ["delete_original"],
        })

        self.log.info(f"Written '{os.path.basename(log_file)}' log file at {log_file}")