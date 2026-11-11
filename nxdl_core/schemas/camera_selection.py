from enum import Enum


class CAMERA_SELECTION(str, Enum):
    """Which cameras of a minute the client keeps.

    The server's trip manifest is authoritative about which cameras a minute
    actually has (that varies per trip and per health grade). This selection is
    a purely client-side narrowing of that set — the default keeps everything
    the manifest returns.

    Lives in its own module (not next to CAMERA_TYPE in schemas/trip_metadata.py)
    so that schemas/app_settings.py can import it without pulling in the mcap /
    protobuf / calibration import chain.
    """

    FRONT = "front"  # front camera only
    FRONT_REAR = "front_rear"  # front + rear (back) only
    ALL = "all"  # every camera present in the server manifest

    def __str__(self):
        return self.value
