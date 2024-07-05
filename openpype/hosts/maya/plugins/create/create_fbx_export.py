from openpype.hosts.maya.api import (
    lib,
    plugin
)


class CreateFbxExport(plugin.Creator):
    """FBX with animations"""

    name = "exportFbxMain"
    label = "Fbx export"
    family = "exportfbx"
    icon = "male"

    def __init__(self, *args, **kwargs):
        super(CreateFbxExport, self).__init__(*args, **kwargs)

        # get basic animation data : start / end / handles / steps
        animation_data = lib.collect_animation_data()
        for key, value in animation_data.items():
            self.data[key] = value

        # Bake to world space by default, when this is False it will also
        # include the parent hierarchy in the baked results
        self.data['bakeToWorldSpace'] = True
