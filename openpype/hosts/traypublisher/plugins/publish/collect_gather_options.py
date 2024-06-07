import pyblish.api
import json

from openpype.lib import BoolDef
from openpype.pipeline import OpenPypePyblishPluginMixin


class CollectGatherOptions(
    pyblish.api.ContextPlugin,
    OpenPypePyblishPluginMixin
):
    label = "Gather context options"
    hosts = ["traypublisher"]
    families = ["gather"]

    def process(self, context):
        attrs_value = self.get_attr_values_from_data(context.data)
        self.log.info("Gather context options: '{}'".format(
            json.dumps(attrs_value, indent=4, default=str))
        )

    @classmethod
    def get_attribute_defs(cls):
        return [
            BoolDef("gather_on_farm", default=False, label="Gather instances on farm")
        ]