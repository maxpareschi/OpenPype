# -*- coding: utf-8 -*-
"""Cleanup leftover files from publish."""
from copy import Error
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
        try:
            log_file = f"{os.environ.get('TEMP', os.environ.get('TMP'))}/{uuid.uuid4()}.log"
            with open(log_file, "w") as f:
                for result in instance.context.data["results"]:
                    f.write(f">>> --- {result['plugin'].label} ---\n")
                    for record in result["records"]:
                        try:
                            msg = record.getMessage().replace('\n', '\n\t') or '!!'
                            name = record.name or '!!'
                            levelname = record.levelname or '!!'
                        except:
                            msg = record.get("msg", "!!").replace('\n', '\n\t')
                            name = record.get("name", "!!")
                            levelname = record.get("levelname", "!!")
                        f.write(f"\t{name} - {levelname} - {msg}\n")
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
        
        except Exception as e:
            self.log.warning(f"Could not write log file: {e}")
