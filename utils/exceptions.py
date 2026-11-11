"""Moved to ``nxdl_core.exceptions``; this path stays for the shipped client's imports."""

from nxdl_core.exceptions import (
    NotSupportedFootageFormat,
    NotSupportedTripFormat,
    UserExceptions,
)

__all__ = [
    "NotSupportedFootageFormat",
    "NotSupportedTripFormat",
    "UserExceptions",
]
