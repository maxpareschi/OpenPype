"""
TTD Addon v0.1.0
"""

import os
import click

from openpype.modules import (
    JsonFilesSettingsDef,
    OpenPypeAddOn,
    ModulesManager,
    IPluginPaths,
    ITrayAction
)

from .lib import pipeline


class TTDAddonSettingsDef(JsonFilesSettingsDef):
    schema_prefix = "ttd_addon"
    def get_settings_root_path(self):
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "settings"
        )


class TTDAddon(OpenPypeAddOn, IPluginPaths, ITrayAction):

    label = "TTD Addon"
    name = "ttd_addon"

    def initialize(self, settings):
        module_settings = settings[self.name]
        self._connected_modules = None
        self._dialog = None

    def tray_init(self):
        self._create_dialog()

    def _create_dialog(self):
        if self._dialog is not None:
            return
        from .ttd_widgets import MyExampleDialog
        self._dialog = MyExampleDialog()

    def show_dialog(self):
        self._create_dialog()
        self._dialog.open()

    def get_connected_modules(self):
        names = set()
        if self._connected_modules is not None:
            for module in self._connected_modules:
                names.add(module.name)
        return names

    def on_action_trigger(self):
        self.show_dialog()

    def get_plugin_paths(self):
        plugins_dir = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)
            ),
            "plugins"
        )
        return pipeline.search_paths_recursive(plugins_dir)

    def cli(self, click_group):
        click_group.add_command(cli_main)


@click.group(TTDAddon.name, help="Example addon dynamic cli commands.")
def cli_main():
    pass


@cli_main.command()
def nothing():
    """Does nothing but print a message."""
    print("You've triggered \"nothing\" command.")


@cli_main.command()
def show_dialog():
    """Show TTDAddon dialog.

    We don't have access to addon directly through cli so we have to create
    it again.
    """
    from openpype.tools.utils.lib import qt_app_context

    manager = ModulesManager()
    ttd_addon = manager.modules_by_name[TTDAddon.name]
    with qt_app_context():
        ttd_addon.show_dialog()