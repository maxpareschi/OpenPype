from pprint import pformat

import pyblish.api

from openpype.client import (
    get_asset_by_name,
    get_subset_by_name,
    get_last_version_by_subset_id,
)

class CollectSourceColorspace(pyblish.api.InstancePlugin):
    """
    Collects source colorspace from plateMain if present,
    otherwise defaults to None
    """
    label = "Collect source colorspace"
    order = pyblish.api.CollectorOrder + 0.49925
    families = [
        "review",
        "render",
        "gather"
    ]

    def process(self, instance):

        if instance.data.get("farm", None):
            self.log.info("Farm mode is on, skipping.")
            return

        context = instance.context

        asset_doc = None
        subset_doc = None
        version_doc = None
  
        asset_doc = get_asset_by_name(context.data["projectName"],
                                      instance.data["asset"],
                                      fields=["_id"])
        if asset_doc:
            subset_doc = get_subset_by_name(context.data["projectName"],
                                            "plateMain",
                                            asset_doc["_id"],
                                            fields=["_id"])
        if subset_doc:
            version_doc = get_last_version_by_subset_id(context.data["projectName"],
                                                        subset_doc["_id"],
                                                        fields=["_id", "data"])
        
        if version_doc:
            try:
                instance.data["colorspace"] = version_doc["data"]["colorspace"]
            except Exception as e:
                self.log.info(pformat(version_doc["data"]))
                self.log.warning("Missing colorspace key in version doc. Setting colorspace to None")
                instance.data["colorspace"] = "Output - Rec.709"
        else:
            instance.data["colorspace"] = None
