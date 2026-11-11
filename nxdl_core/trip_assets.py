"""Which files a trip needs, independent of how they are fetched.

Pure enumeration over a ``TripManifest`` and the dataset's ``DatasetOutput``:
no HTTP, no filesystem. ``rel_path`` is relative to the trip directory.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from nxdl_core.schemas import DatasetOutput, TripManifest
from nxdl_core.schemas.camera_selection import CAMERA_SELECTION
from nxdl_core.schemas.trip_manifest import SignedFile
from nxdl_core.paths import (
    build_camera_directory_path,
    build_camera_file_path,
    build_camera_video_path,
    build_feature_file_path,
    build_fused_sei_path,
    build_minute_directory_path,
    is_camera_allowed,
)


@dataclass
class PendingRestore:
    """Files the manifest withholds because they sit in S3 Deep Archive.

    Two categories: ``files`` are restoring (link comes after the restore
    completes) and ``archived`` files will never come — the dataset was created
    without ``settings.request_restore``.
    """

    files: int = 0
    max_available_at: int | None = None  # epoch ms; None when no ETA is known
    archived: int = 0

    def add(self, available_at: int | None) -> None:
        self.files += 1
        if available_at is not None and (
            self.max_available_at is None or available_at > self.max_available_at
        ):
            self.max_available_at = available_at

    def add_archived(self) -> None:
        self.archived += 1


@dataclass(frozen=True, slots=True)
class TripAsset:
    kind: Literal["video", "telemetry", "sei", "fused_sei", "feature"]
    link: SignedFile
    rel_path: Path  # relative to the trip directory


def enumerate_trip_assets(
    manifest: TripManifest,
    output: DatasetOutput,
    camera_selection: CAMERA_SELECTION | None = None,
) -> tuple[list[TripAsset], PendingRestore]:
    """List the files a trip needs, mirroring the manifest structure.

    Files still restoring from Deep Archive have no link yet — they yield no asset
    and are tallied in the returned ``PendingRestore`` instead.
    """
    trip_dir = Path()
    timezone = manifest.timezone
    assets: list[TripAsset] = []
    pending = PendingRestore()

    for minute in manifest.minutes:
        minute_dir = build_minute_directory_path(
            trip_dir, minute.minute_epoch, timezone
        )
        if output.footage.fused_sei is not None:
            if minute.fused_sei_status == "archived":
                pending.add_archived()
            elif minute.fused_sei_status == "restoring":
                pending.add(minute.fused_sei_available_at)
            elif getattr(minute, "fused_sei", None) is not None:
                fused_sei_path = build_fused_sei_path(
                    minute_dir, minute.minute_epoch, timezone
                )
                assets.append(TripAsset("fused_sei", minute.fused_sei, fused_sei_path))
        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type, camera_selection):
                continue
            camera_dir = build_camera_directory_path(
                minute_dir=minute_dir, camera_type=camera.camera_type
            )
            camera_path = build_camera_video_path(
                camera_dir, camera.camera_type, minute.minute_epoch, timezone
            )
            if camera.video_status == "archived":
                pending.add_archived()
            elif camera.video_status == "restoring" or camera.camera is None:
                pending.add(camera.video_available_at)
            else:
                assets.append(TripAsset("video", camera.camera, camera_path))

            if (
                manifest.health_grade != 0
                and camera.telemetry is not None
                and output.footage.needs_telemetry()
            ):
                telemetry_path = build_camera_file_path(
                    camera_dir, camera.telemetry.presigned_url
                )
                assets.append(TripAsset("telemetry", camera.telemetry, telemetry_path))

            if output.footage.sei is not None:
                if camera.sei_status == "archived":
                    pending.add_archived()
                elif camera.sei_status == "restoring":
                    pending.add(camera.sei_available_at)
                elif camera.sei is not None:
                    sei_path = camera_path.with_suffix(".sei")
                    sei_path = sei_path.with_name(sei_path.name + ".csv")
                    assets.append(TripAsset("sei", camera.sei, sei_path))

    for feature_name in (
        "fixed_metadata",
        "base_insights",
        "safety_critical",
        "ego_actions",
    ):
        feature = getattr(manifest, feature_name, None)
        if feature is None:
            continue
        feature_path = build_feature_file_path(trip_dir, feature_name)
        assets.append(TripAsset("feature", feature, feature_path))

    return assets, pending
