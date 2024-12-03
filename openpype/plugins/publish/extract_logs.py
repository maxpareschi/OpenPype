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

        log_file = f"{os.environ.get('TEMP', os.environ.get('TMP'))}/{uuid.uuid4()}.log"

        with open(log_file, "w") as f:
            for result in instance.context.data["results"]:
                f.write(f">>> --- {result['plugin'].label} ---\n")
                for record in result["records"]:
                    msg = record.get('msg', '!!').replace('\n', '\n\t')
                    f.write(f"\t{record.get('name', '!!')} - {record.get('levelname', '!!')} - {msg}\n")
                if result["success"]:
                    f.write(f"<<< --- Plugin completed in: {result['duration']} ms. ---\n\n")
                elif result["error"]:
                    f.write(f"<<< --- Plugin error! : {result['error']} ms\n\n")

        instance.data["representations"].append({
            "name": "log",
            "ext": "log",
            "stagingDir": os.path.dirname(log_file),
            "files": os.path.basename(log_file),
            "tags": ["delete_original"],
        })

        self.log.info(f"Written '{os.path.basename(log_file)}' log file at {log_file}")