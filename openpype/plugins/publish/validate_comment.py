import os
import pyblish.api

from openpype.lib import filter_profiles
from openpype.pipeline import PublishValidationError


class ValidateComment(pyblish.api.ContextPlugin):
    """Validate comment of publish.

    Comment needs to be set for all publishes.
    """

    order = pyblish.api.ValidatorOrder - 0.1

    label = "Validate Comment"
    enabled = True

    def process(self, context):
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
