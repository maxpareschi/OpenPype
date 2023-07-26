"""
Requires:
    context > hostName
    context > appName
    context > appLabel
    context > comment
    context > ftrackSession
    instance > ftrackIntegratedAssetVersionsData
"""

import sys
import copy

import six
import pyblish.api
from openpype.lib import StringTemplate


class IntegrateFtrackSubsetIntent(pyblish.api.InstancePlugin):
    """Create comments in Ftrack."""

    # Must be after integrate asset new
    order = pyblish.api.IntegratorOrder + 0.49999
    label = "Integrate Ftrack Subset and Intent"
    families = ["ftrack"]
    optional = True

    def process(self, instance):
        asset_versions_key = "ftrackIntegratedAssetVersionsData"
        asset_versions_data_by_id = instance.data.get(asset_versions_key)
        if not asset_versions_data_by_id:
            self.log.info("There are any integrated AssetVersions")
            return

        context = instance.context
        subset = instance.data["subset"]

        session = context.data["ftrackSession"]

        intent = instance.context.data.get("intent")


        intent_label = None
        if intent:
            value = intent["value"]
            if value:
                intent_label = intent["label"] or value

        if intent_label:
            self.log.debug(
                "Intent label is set to `{}`.".format(intent_label)
            )
        else:
            self.log.debug("Intent is not set.")

        for asset_version_data in asset_versions_data_by_id.values():
            asset_version = asset_version_data["asset_version"]

            try:
                if asset_version["custom_attributes"]["subset"]:
                    asset_version["custom_attributes"]["subset"] = subset
                if asset_version["custom_attributes"]["intent"]:
                    asset_version["custom_attributes"]["intent"] = value
                session.commit()
                self.log.debug("subset and intent added to AssetVersion \"{}\"".format(
                    str(asset_version)
                ))
            except Exception:
                tp, value, tb = sys.exc_info()
                session.rollback()
                session._configure_locations()
                self.log.warning("Error encountered: {}, {}, {}".format(tp, value, tb))
