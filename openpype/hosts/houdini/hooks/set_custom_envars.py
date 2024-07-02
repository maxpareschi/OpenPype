from __future__ import annotations

from openpype.lib import PreLaunchHook
from openpype.client import get_asset_by_name
from openpype.lib.applications import ApplicationLaunchContext


def update_application_launch_ctx(launch_ctx: ApplicationLaunchContext):
    """Adds SFSTART and SFEND to the given OpenPype app launch context."""

    env = launch_ctx.env
    ctx = get_asset_by_name(env["AVALON_PROJECT"], env["AVALON_ASSET"])
    env["SFSTART"] = str(ctx['data']['frameStart'] - ctx['data']['handleStart'])
    env["SFEND"] = str(ctx['data']['frameEnd'] + ctx['data']['handleEnd'])

class SetCustomEnvVars(PreLaunchHook):

    app_groups = ["houdini"]

    def execute(self):
        update_application_launch_ctx(self.launch_context)
        start = self.launch_context.env['SFSTART']
        end = self.launch_context.env['SFEND']
        self.log.debug(f"Adding env var SFSTART: {start} and SFEND: {end}")
