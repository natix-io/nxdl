"""Moved to ``nxdl_core.safety_critical_events``; this path stays for the shipped client's imports."""

from nxdl_core.safety_critical_events import (
    PUBLIC_EVENT_NAME_TO_INTERNAL_NAMES,
    internal_names_for_public_names,
    public_name_for_internal,
)

__all__ = [
    "PUBLIC_EVENT_NAME_TO_INTERNAL_NAMES",
    "internal_names_for_public_names",
    "public_name_for_internal",
]
