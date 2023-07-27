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

        settings_intent = instance.context.data["system_settings"]["modules"]["ftrack"]["intent"]
        default_intent_key = settings_intent.get("default")
        default_intent_label = settings_intent["items"].get(default_intent_key)
        intent = instance.context.data.get("intent")

        if not intent:
            intent = {
                "value": default_intent_key,
                "label": default_intent_label
            }

        self.log.debug(settings_intent)

        self.log.debug(intent)


        subset = instance.data.get("subset", "renderCompositingMain")

        session = instance.context.data["ftrackSession"]
        for asset_version_data in asset_versions_data_by_id.values():
            
            asset_version = asset_version_data["asset_version"]

            asset_version["custom_attributes"]["subset"] = subset
            asset_version["custom_attributes"]["intent"] = intent["value"]

            try:
                session.commit()
                self.log.debug("intent {} and Subset {} added to AssetVersion \"{}\"".format(
                    intent, subset, str(asset_version)
                ))
            except:
                tp, value, tb = sys.exc_info()
                session.rollback()
                session._configure_locations()
                self.log.warning("Errors detected: {}, {}, {}: ".format(tp, value, tb))
