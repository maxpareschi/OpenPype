""" Addon class definition and Settings definition must be imported here.

If addon class or settings definition won't be here their definition won't
be found by OpenPype discovery.
"""

from .addon import (
    TTDAddonSettingsDef,
    TTDAddon
)

__all__ = (
    "TTDAddonSettingsDef",
    "TTDAddon"
)