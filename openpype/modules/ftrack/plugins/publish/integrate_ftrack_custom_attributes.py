"""
Requires:
    context > intent
    instance > subset
    context > ftrackSession
    instance > ftrackIntegratedAssetVersionsData
"""

import sys
import six
import pyblish.api


class IntegrateFtrackIntentSubset(pyblish.api.InstancePlugin):
    """Add description to AssetVersions in Ftrack."""

    # Must be after integrate asset new
    order = pyblish.api.IntegratorOrder + 0.4999
    label = "Integrate Ftrack Intent and Subset"
    families = ["ftrack"]
    optional = True

    def process(self, instance):

        # Check if there are any integrated AssetVersion entities
        asset_versions_key = "ftrackIntegratedAssetVersionsData"
        asset_versions_data_by_id = instance.data.get(asset_versions_key)
        if not asset_versions_data_by_id:
            self.log.info("There are any integrated AssetVersions")
            return

        intent = instance.context.data.get("intent", "Work in Progress")
        if intent:
            value = intent.get("value")
            if value:
                intent = intent.get("label") or value

        subset = instance.data.get("subset", "renderCompositingMain")

        session = instance.context.data["ftrackSession"]
        for asset_version_data in asset_versions_data_by_id.values():
            
            asset_version = asset_version_data["asset_version"]

            asset_version["custom_attributes"]["subset"] = subset
            asset_version["custom_attributes"]["intent"] = intent

            try:
                session.commit()
                self.log.debug("intent {} and Subset {} added to AssetVersion \"{}\"".format(
                    str(intent, subset, asset_version)
                ))
            except Exception:
                tp, value, tb = sys.exc_info()
                session.rollback()
                session._configure_locations()
                six.reraise(tp, value, tb)
