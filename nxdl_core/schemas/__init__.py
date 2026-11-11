from .dataset_request import (
    DatasetFilters,
    DatasetOutput,
    DatasetRequest,
    FixedMetadataOutput,
    FootageOutput,
    MinuteOutput,
    SavedDatasetRequest,
    TripInsightOutput,
)
from .frame_data import FrameData
from .trip_manifest import (
    MinuteManifest,
    TripManifest,
    TripSegment,
)

__all__ = [
    "DatasetFilters",
    "DatasetOutput",
    "DatasetRequest",
    "FixedMetadataOutput",
    "FootageOutput",
    "FrameData",
    "MinuteManifest",
    "MinuteOutput",
    "SavedDatasetRequest",
    "TripInsightOutput",
    "TripManifest",
    "TripMetadata",
    "TripSegment",
]


def __getattr__(name: str):
    # TripMetadata is loaded on first use: trip_metadata imports nxdl_core.csv_helpers,
    # which imports nxdl_core.schemas.frame_data, so an eager import here made
    # `import nxdl_core.csv_helpers` (as the first import) fail on a cycle.
    if name == "TripMetadata":
        from .trip_metadata import TripMetadata

        return TripMetadata
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
