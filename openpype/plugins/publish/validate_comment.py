import os
import pyblish.api

from openpype.lib import filter_profiles
from openpype.pipeline import PublishValidationError


class ValidateComment(pyblish.api.ContextPlugin):
    """Validate comment of publish.

    Comment needs to be set for all publishes.
    """

    order = pyblish.api.ValidatorOrder -0.1

    label = "Validate Comment"
    enabled = False

    # Can be modified by settings
    profiles = [{
        "hosts": [],
        "task_types": [],
        "tasks": [],
        "validate": False
    }]

    def process(self, context):
        # Skip if there are no profiles
        validate = True
        if self.profiles:
            # Collect data from context
            task_name = context.data.get("task")
            task_type = context.data.get("taskType")
            host_name = context.data.get("hostName")

            filter_data = {
                "hosts": host_name,
                "task_types": task_type,
                "tasks": task_name
            }
            matching_profile = filter_profiles(
                self.profiles, filter_data, logger=self.log
            )
            if matching_profile:
                validate = matching_profile["validate"]

        if not validate:
            self.log.debug((
                "Validation of intent was skipped."
                " Matching profile for current context disabled validation."
            ))
            return

        msg = "Please make sure to input a comment. NO COMMENT, NO PARTY!"

        comment = context.data.get("comment") or {}
        self.log.debug("Comment is set to: '{}'".format(comment))
        if not comment:
            raise PublishValidationError(
                title="Missing Comment",
                description=msg,
                detail="",
                message="missing_comment"
            )
