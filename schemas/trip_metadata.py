import json
import logging
import os
import csv
from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from schemas.trip_manifest import FixedMetadata
from utils.mcap.trip_metadata_pb2 import Metadata
from utils.mcap_tools import generate_mcap_file
from utils.csv_helpers import split_csv_sections
from utils.calibration import get_camera_and_device_calibrations
from utils.exceptions import NotSupportedTripFormat, NotSupportedFootageFormat
from schemas.dataset_request import TripJSONFormat, TripMCAPFormat

logger = logging.getLogger(__name__)


class CAMERA_TYPE(Enum):
    # ! IMPORTANT: we should NOT change the name of 'REAR' camera,
    #       because navinfo anonymizer is using explicitly the 'rear' name for specific anonymization on rear camera,
    #       in any case, if we want to change it, we should also notify NAV INFO to change their side as well
    FRONT = "front"
    REAR = "rear"
    RIGHT_REPEATER = "right_repeater"
    LEFT_REPEATER = "left_repeater"
    RIGHT_PILLAR = "right_pillar"
    LEFT_PILLAR = "left_pillar"

    def __str__(self):
        return self.value

@dataclass
class TripMetadata:
    trip_id: str
    vehicle_make: str
    vehicle_model: str
    phone_make: str
    phone_model: str
    platform_type_video: str
    platform_type_data: str
    frame_width: int
    frame_height: int
    frame_mp: float
    camera_sensor_width: float = 6.4
    camera_sensor_height: float = 3.6
    platform_type_gps: str = "vx360"
    reference_frame: str = "ground_nominal"
    version: int = 1
    camera_calibrations: list = field(default_factory=list)
    device_calibrations: list = field(default_factory=list)

    def __post_init__(self):
        if self.frame_width == 1280:
            self.camera_sensor_width = 6.4
        elif self.frame_width == 2560:
            self.camera_sensor_width = 8.8

        if self.frame_height == 960:
            self.camera_sensor_height = 3.6
        elif self.frame_height == 1920:
            self.camera_sensor_height = 6.6

    @classmethod
    def from_fixed_metadata(
        cls,
        trip_id: str,
        trip_version: str,
        fixed_metadata: FixedMetadata,
        camera_calibrations: list[dict],
        device_calibrations: list[dict],
    ) -> "TripMetadata":
        return cls(
            trip_id=trip_id,
            version=trip_version,
            camera_calibrations=camera_calibrations,
            device_calibrations=device_calibrations,
            **fixed_metadata.model_dump(),
        )

    @classmethod
    def from_manifest(
        cls,
        trip_id: str,
        trip_version: str,
        fixed_metadata: FixedMetadata,
        vehicle_hw_version: str | None,
    ) -> "TripMetadata":
        if not vehicle_hw_version:
            raise ValueError("Trip manifest is missing vehicle_hw_version.")

        vehicle_model = str(getattr(fixed_metadata, "vehicle_model", ""))
        camera_calibrations, device_calibrations = get_camera_and_device_calibrations(
            hw_version=vehicle_hw_version,
            model=vehicle_model,
        )
        return cls.from_fixed_metadata(
            trip_id=trip_id,
            trip_version=trip_version,
            fixed_metadata=fixed_metadata,
            camera_calibrations=camera_calibrations,
            device_calibrations=device_calibrations,
        )

    @classmethod
    def from_csv(cls, csv_path: Path) -> "TripMetadata":
        """Reconstruct a TripMetadata instance from a fixed_metadata CSV."""
        sections = split_csv_sections(csv_path.read_text())

        static: dict = {}
        camera_calibrations: list[dict] = []
        device_calibrations: list[dict] = []

        for section_name, rows in sections:
            lower = section_name.lower()
            if "static metadata" in lower:
                if rows:
                    static = rows[0]
            elif "camera extrinsics" in lower:
                camera_calibrations = rows
            elif "device extrinsics" in lower:
                device_calibrations = rows

        return cls(
            trip_id=str(static.get("trip_identifier", "")),
            vehicle_make=str(static.get("vehicle_make", "")),
            vehicle_model=str(static.get("vehicle_model", "")),
            phone_make=str(static.get("phone_make", "")),
            phone_model=str(static.get("phone_model", "")),
            platform_type_video=str(static.get("platform_type_video", "")),
            platform_type_data=str(static.get("platform_type_data", "")),
            frame_width=int(static.get("frame_width", 0)),
            frame_height=int(static.get("frame_height", 0)),
            frame_mp=float(static.get("frame_mp", 0.0)),
            platform_type_gps=str(static.get("platform_type_gps", "vx360")),
            reference_frame=str(static.get("reference_frame", "ground_nominal")),
            version=int(static.get("version", 1)),
            camera_calibrations=camera_calibrations,
            device_calibrations=device_calibrations,
        )

    def save_to_csv_file(self, csv_path: Path | str) -> None:
        path = Path(csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)

            csv_file.write("# Static metadata\n")
            static_metadata = {
                "frame_width": self.frame_width,
                "frame_height": self.frame_height,
                "frame_mp": self.frame_mp,
                "vehicle_make": self.vehicle_make,
                "vehicle_model": self.vehicle_model,
                "platform_type_video": self.platform_type_video,
                "platform_type_gps": self.platform_type_gps,
                "reference_frame": self.reference_frame,
            }
            writer.writerow(["version", "trip_identifier", *static_metadata.keys()])
            writer.writerow([self.version, self.trip_id, *static_metadata.values()])

            csv_file.write("\n# Camera extrinsics and intrinsics\n")
            if self.camera_calibrations:
                writer.writerow(self.camera_calibrations[0].keys())
                for camera_calibration in self.camera_calibrations:
                    writer.writerow(camera_calibration.values())

            csv_file.write("\n# Device extrinsics \n")
            if self.device_calibrations:
                writer.writerow(self.device_calibrations[0].keys())
                for device_calibration in self.device_calibrations:
                    writer.writerow(device_calibration.values())

        logger.info("Trip metadata saved to CSV: %s", path)

    def get_camera_calibration(self, camera_type: CAMERA_TYPE):
        camera_name = [f"camera_{camera_type.value}"] 
        camera_ex = [
            camera for camera in self.camera_calibrations if camera["device_name"] in camera_name
        ]
        return camera_ex[0] if camera_ex else None

    def get_device_calibration(self, device_name: str):
        if device_name not in ["phone", "vx360"]:
            raise ValueError(f"Invalid device name: {device_name}. Must be 'phone' or 'vx360'.")
        
        device_ex = [
            device for device in self.device_calibrations if device["device_name"] in device_name
        ]
        return device_ex[0] if device_ex else None

    def _as_json(self, format: TripJSONFormat = TripJSONFormat.default) -> dict:
        if format == TripJSONFormat.default:
            return {
                "version": self.version,
                # "footage_filename": video_filename,
                "trip_identifier": self.trip_id,
                "frame_width": self.frame_width,
                "frame_height": self.frame_height,
                "frame_mp": self.frame_mp,
                "vehicle_make": self.vehicle_make,
                "vehicle_model": self.vehicle_model,
                "platform_type_video": self.platform_type_video,
                "platform_type_gps": self.platform_type_gps,
                "reference_frame": self.reference_frame,
            }
        else:
            raise NotSupportedTripFormat(format, "JSON")

    def save_to_json_file(self, json_path: str, format: TripJSONFormat = TripJSONFormat.default) -> None:
        """Save metadata + device_extrinsics to a JSON file."""
        if os.path.exists(json_path):
            return
        all_calibrations = self.camera_calibrations + self.device_calibrations
        data = self._as_json(format)
        data["device_extrinsics"] = all_calibrations
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)
        logger.info("Trip metadata saved to JSON: %s", json_path)

    @staticmethod
    def _version_to_mcap_int(version: int | str) -> int:
        """Convert supported manifest versions to proto-safe integers."""
        if isinstance(version, int):
            return version

        major, _, minor = str(version).strip().partition(".")
        if major in {"1", "2"} and minor == "0":
            return int(major)

        raise ValueError(f"Unsupported version for MCAP metadata: {version!r}")

    def _convert_to_mcap(self, format: TripMCAPFormat = TripMCAPFormat.default) -> dict:
        if format != TripMCAPFormat.default.value:
            raise NotSupportedFootageFormat(format, "MCAP")
        return {
            "version": self._version_to_mcap_int(self.version),
            "gps": {
                "latitude_ref": "N",
                "longitude_ref": "E",
            },
            "image": {
                "image_width": self.frame_width,
                "image_height": self.frame_height,
                "megapixels": str(self.frame_mp),
            },
            "context": {
                "trip_identifier": self.trip_id,
                "vehicle_make": self.vehicle_make,
                "vehicle_model": self.vehicle_model,
                "phone_make": self.phone_make,
                "phone_model": self.phone_model,
                "platform_type_video": self.platform_type_video,
                "platform_type_data": self.platform_type_data,
                "platform_type_gps": self.platform_type_gps,
                "reference_frame": self.reference_frame,
            },
            "calibration": {
                "devices": [
                    {
                        "device_name": cam.get("device_name"),
                        "extrinsics": {
                            "r11": float(cam.get("r11", 0.0)),
                            "r12": float(cam.get("r12", 0.0)),
                            "r13": float(cam.get("r13", 0.0)),
                            "tx": float(cam.get("tx", 0.0)),
                            "r21": float(cam.get("r21", 0.0)),
                            "r22": float(cam.get("r22", 0.0)),
                            "r23": float(cam.get("r23", 0.0)),
                            "ty": float(cam.get("ty", 0.0)),
                            "r31": float(cam.get("r31", 0.0)),
                            "r32": float(cam.get("r32", 0.0)),
                            "r33": float(cam.get("r33", 0.0)),
                            "tz": float(cam.get("tz", 0.0)),
                            "field_of_view_hor_deg": int(cam.get("field_of_view_hor_deg", 0)),
                            "field_of_view_ver_deg": int(cam.get("field_of_view_ver_deg", 0)),
                        },
                        "intrinsics": {
                            "fx": float(cam.get("fx", 0.0)),
                            "cx": float(cam.get("cx", 0.0)),
                            "fy": float(cam.get("fy", 0.0)),
                            "cy": float(cam.get("cy", 0.0)),
                            "k1": float(cam.get("k1", 0.0)),
                            "k2": float(cam.get("k2", 0.0)),
                            "k3": float(cam.get("k3", 0.0)),
                            "p1": float(cam.get("p1", 0.0)),
                            "p2": float(cam.get("p2", 0.0)),
                        },
                    }
                    for cam in self.camera_calibrations
                ] + [
                    {
                        "device_name": dev.get("device_name"),
                        "extrinsics": {
                            "tx": float(dev.get("tx", 0.0)),
                            "ty": float(dev.get("ty", 0.0)),
                            "tz": float(dev.get("tz", 0.0)),
                        },
                    }
                    for dev in self.device_calibrations
                ],
            },
        }

    def save_to_mcap_file(self, mcap_path: str, format: TripMCAPFormat = TripMCAPFormat.default) -> None:
        try:
            
            mcap_metadata = self._convert_to_mcap(format)
            proto_schema = Metadata()
            schema_name = "Metadata"
            topic_name = "Metadata"
            
            generate_mcap_file(mcap_path, mcap_metadata, proto_schema, schema_name, topic_name)
            
        except Exception as e:
            logger.error(f"save_trip_to_mcap_file({mcap_path}): {e}")
            raise e