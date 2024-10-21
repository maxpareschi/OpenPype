import os

import click

from openpype.lib import get_openpype_execute_args
from openpype.lib.execute import run_detached_process
from openpype.modules import OpenPypeModule, ITrayAction, IHostAddon

TRAYPUBLISH_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class TrayPublishAddon(OpenPypeModule, IHostAddon, ITrayAction):
    label = "New Publish"
    name = "traypublisher"
    host_name = "traypublisher"

    def initialize(self, modules_settings):
        self.enabled = True
        self.publish_paths = [
            os.path.join(TRAYPUBLISH_ROOT_DIR, "plugins", "publish")
        ]
        self._experimental_tools = None

    def tray_init(self):
        from openpype.tools.experimental_tools import ExperimentalTools

        self._experimental_tools = ExperimentalTools()

    def tray_menu(self, *args, **kwargs):
        super(TrayPublishAddon, self).tray_menu(*args, **kwargs)
        traypublisher = self._experimental_tools.get("traypublisher")
        traypublisher.enabled = True
        visible = False
        if traypublisher and traypublisher.enabled:
            visible = True
        self._action_item.setVisible(visible)

    def on_action_trigger(self):
        self.run_traypublisher()

    def connect_with_modules(self, enabled_modules):
        """Collect publish paths from other modules."""
        publish_paths = self.manager.collect_plugin_paths()["publish"]
        self.publish_paths.extend(publish_paths)

    def run_traypublisher(self):
        args = get_openpype_execute_args(
            "module", self.name, "launch"
        )
        run_detached_process(args)

    def cli(self, click_group):
        click_group.add_command(cli_main)

@click.group(TrayPublishAddon.name, help="TrayPublisher related commands.")
def cli_main():
    pass


@cli_main.command()
def launch():
    """Launch TrayPublish tool UI."""
    from openpype.tools import traypublisher
    traypublisher.main()


@click.command()
@click.argument('input', type=str)
def gather(input):
    import json
    import platform
    from openpype.tools.traypublisher.window import TrayPublishWindow
    from openpype.pipeline.create import CreateContext
    from openpype.tools import traypublisher
    from openpype.lib import FileDefItem
    from qtpy import QtWidgets

    data = None
    with open(input, "r") as djf:
        data = json.loads(djf.read())
    
    host = traypublisher.init_host()
    project_name = data[0]["project"]
    host.set_project_name(project_name)
    print("Project Name: was set: '{}'".format(project_name))

    create_context = CreateContext(host,
                                   headless=True,
                                   discover_publish_plugins=True,
                                   reset=True)
    
    for instance in list(create_context.instances):
        create_plugin = create_context.creators.get(
            instance.creator_identifier
        )
        create_plugin.remove_instances([instance])

    for instance in data:
        publish_file_list = [item.to_dict() for item in FileDefItem.from_paths(
            instance["gather_representation_files"], allow_sequences=True)]
        
        create_context.create(
            "settings_{}".format(instance["family"]),
            instance["subset"],
            instance,
            pre_create_data={
                "representation_files": publish_file_list,
                "reviewable": publish_file_list[0],
            }
        )
    
    if not create_context.instances:
        msg = "No valid instances could be gathered, aborting..."
        print(msg)
        return False
    
    app_instance = QtWidgets.QApplication.instance()
    if app_instance is None:
        app_instance = QtWidgets.QApplication([])
    if platform.system().lower() == "windows":
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            u"traypublisher"
        )
    
    window = TrayPublishWindow()
    window._overlay_widget._set_project(project_name)
    window.set_context_label("{} - GATHER VERSIONS".format(project_name))
    window.show()
    app_instance.exec_()

    return True


cli_main.add_command(gather)