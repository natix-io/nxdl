from schemas.frame_data import FrameData
from schemas.dataset_request import FootageMCAPFormat
from utils.exceptions import NotSupportedFootageFormat
from datetime import tzinfo, UTC
from zoneinfo import ZoneInfo
from utils.utils import safe_float
from utils.mcap import minute_metadata_pb2
from utils.mcap_tools import generate_mcap_file
import logging

logger = logging.getLogger(__name__)


def _frame_timestamp_ms(sd: FrameData, tz: tzinfo) -> int:
    """Epoch-ms for a frame's trip-local wall-clock.

    The CSV timestamp is a naive wall-clock in the trip's timezone. A naive
    ``.timestamp()`` would resolve it against the host's local timezone (e.g.
    Europe/Berlin on the build box), shifting every non-local trip by the host
    offset. Attach the trip timezone explicitly so the instant is correct.
    """
    dt = sd.local_timestamp
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return int(dt.timestamp() * 1000)


def save_to_mcap_file(mcap_path: str, frame_data_list: list[FrameData], timezone: str | None, format: FootageMCAPFormat = FootageMCAPFormat.default):
    try:
        mcap_metadata = _convert_to_mcap(frame_data_list, 1, timezone, format)
        proto_schema = minute_metadata_pb2.TelemetryData()
        schema_name = "TelemetryData"
        topic_name = "TelemetryData"

        generate_mcap_file(mcap_path, mcap_metadata, proto_schema, schema_name, topic_name)
        
    except Exception as e:
        logger.error(f"save_trip_to_mcap_file({mcap_path}): {e}")
        raise e


def _convert_to_mcap(frame_data_list: list[FrameData], version: int, timezone: str | None, format: FootageMCAPFormat = FootageMCAPFormat.default):
    if format is not FootageMCAPFormat.default:
        raise NotSupportedFootageFormat(format, "MCAP")
    if timezone is None:
        logger.warning("No trip timezone provided for MCAP; falling back to UTC for frame timestamps")
        tz: tzinfo = UTC
    else:
        tz = ZoneInfo(timezone)
    return {
        "version": version,
        "frames": [
        {
            "timestamp_ms": _frame_timestamp_ms(sd, tz),
            "frame_number": sd.frame_number if sd.frame_number is not None else "na",
            **({"gnss": {
                **({"latitude_deg": sd.GPS_latitude_deg} if sd.GPS_latitude_deg is not None else {}),
                **({"longitude_deg": sd.GPS_longitude_deg} if sd.GPS_longitude_deg is not None else {}),
                # "altitude_m": sd.altitude_m if sd.altitude_m is not None else "na",
                **({"latitude_uncertainty_deg": sd.horizontal_accuracy_m} if sd.horizontal_accuracy_m is not None else {}),
                **({"longitude_uncertainty_deg": sd.horizontal_accuracy_m} if sd.horizontal_accuracy_m is not None else {}),
                # "altitude_uncertainty_m": sd.vertical_accuracy_m if sd.vertical_accuracy_m is not None else "na",
                # "velocity_mps": {"north_mps": safe_float(sd.velocity_north_mps), "east_mps": safe_float(sd.velocity_east_mps), "down_mps": safe_float(sd.velocity_down_mps)},
                "velocity_mps": {"north_mps": safe_float(sd.velocity_north_mps), "east_mps": safe_float(sd.velocity_east_mps)},
                **({"heading_deg": sd.heading_deg} if sd.heading_deg is not None else {}),
            }} if any([
                    sd.GPS_latitude_deg, sd.GPS_longitude_deg, sd.horizontal_accuracy_m, sd.heading_deg
                ]) else {}),
            **({"imu": {
                "reference_frame": "IMU",
                **({"x_angular_velocity_rad_per_s": sd.angular_velocity_x_rad_per_s} if sd.angular_velocity_x_rad_per_s is not None else {}),
                **({"y_angular_velocity_rad_per_s": sd.angular_velocity_y_rad_per_s} if sd.angular_velocity_y_rad_per_s is not None else {}),
                **({"z_angular_velocity_rad_per_s": sd.angular_velocity_z_rad_per_s} if sd.angular_velocity_z_rad_per_s is not None else {}),
                # "x_linear_acceleration_mps2": sd.acceleration_x_mps2 if sd.acceleration_x_mps2 is not None else "na",
                # "y_linear_acceleration_mps2": sd.acceleration_y_mps2 if sd.acceleration_y_mps2 is not None else "na",
                # "z_linear_acceleration_mps2": sd.acceleration_z_mps2 if sd.acceleration_z_mps2 is not None else "na",
                **({"x_linear_acceleration_mps2": sd.acceleration_x_mps2} if sd.acceleration_x_mps2 is not None else {}),
                **({"y_linear_acceleration_mps2": sd.acceleration_y_mps2} if sd.acceleration_y_mps2 is not None else {}),
                **({"z_linear_acceleration_mps2": sd.acceleration_z_mps2} if sd.acceleration_z_mps2 is not None else {}),
                # "angular_velocity_accuracy_rad_per_s": sd.angular_velocity_accuracy_rad_per_s if sd.angular_velocity_accuracy_rad_per_s is not None else "na",
                # "linear_acceleration_accuracy_mps2": sd.acceleration_accuracy_mps2 if sd.acceleration_accuracy_mps2 is not None else "na"
            }} if any ([
                sd.angular_velocity_x_rad_per_s, sd.angular_velocity_y_rad_per_s, sd.angular_velocity_z_rad_per_s, 
                sd.acceleration_x_mps2, sd.acceleration_y_mps2, sd.acceleration_z_mps2
                ]) else {}),
            **({"odometry": {
                **({"unsigned_speed_mps": sd.speed_mps} if sd.speed_mps is not None else {})
            }} if any ([
                sd.speed_mps
            ]) else {})
        } for sd in frame_data_list
        ]
    }
