from __future__ import annotations
from openpype.lib import PreLaunchHook
from openpype.client import get_asset_by_name


def update_custom_environ_vars(env: dict):
    ctx = get_asset_by_name(env["AVALON_PROJECT"], env["AVALON_ASSET"])
    env["SFSTART"] = str(ctx['data']['frameStart'] - ctx['data']['handleStart'])
    env["SFEND"] = str(ctx['data']['frameEnd'] + ctx['data']['handleEnd'])

class SetCustomEnvVars(PreLaunchHook):

    app_groups = ["houdini"]

    def execute(self):
        env = self.launch_context.env
        update_custom_environ_vars(env)
        print(f"Adding env var SFSTART: {env['SFSTART']} and SFEND: {env['SFEND']}")
