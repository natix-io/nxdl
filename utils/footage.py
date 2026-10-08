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


def _earliest_frame_time_ns(frame_data_list: list[FrameData], timezone: str | None) -> int:
    """The first moment this telemetry describes, in nanoseconds.

    Used as the MCAP message time so the file is a function of its data alone:
    the same telemetry always produces the same bytes, and the timestamp means
    what a reader expects it to.
    """
    tz: tzinfo = UTC if timezone is None else ZoneInfo(timezone)
    stamps = [_frame_timestamp_ms(frame, tz) for frame in frame_data_list]
    return min(stamps) * 1_000_000 if stamps else 0


def delivered_time_ns(
    frames: list[FrameData],
    timezone: str | None,
    csv_format: "FootageCSVFormat | None",
    mcap_enabled: bool,
) -> int | None:
    """The earliest time the files DELIVERED for one minute encode, or None.

    Not the earliest frame parsed: what the delivered files can give back. A
    footage MCAP or a default-format CSV encodes every frame's time; a
    ``here_tech`` CSV encodes only the frames with a GPS fix, in its own epoch-ms
    ``time`` column, as written. `app.regen` reads those same files, so the two
    agree by construction — including for a trip that mixes a converted minute
    with one the format left untouched.
    """
    from schemas.dataset_request import FootageCSVFormat  # noqa: PLC0415

    if not frames:
        return None
    if mcap_enabled or csv_format is FootageCSVFormat.default:
        return _earliest_frame_time_ns(frames, timezone)
    if csv_format is FootageCSVFormat.here_tech:
        times = [
            row["time"]
            for row in (f.to_csv_row(fmt=csv_format) for f in frames)
            if row is not None
        ]
        if times:
            return min(times) * 1_000_000
        # No frame had a GPS fix, so the conversion wrote nothing and the
        # formatter kept the raw default-format CSV instead — which encodes
        # every frame, and is what regeneration will read back.
        return _earliest_frame_time_ns(frames, timezone)
    return None


def trip_start_time_ns(trip_dir, timezone: str | None) -> int:
    """The earliest time any delivered telemetry file in the trip encodes.

    For the trip-level metadata record, which carries no time of its own; the
    download path computes the same value from the frames it is about to write
    (`delivered_time_ns`). Per camera folder the footage MCAP is read first (an
    absolute time, every frame), then a default-format CSV (needs the trip's
    timezone), then a ``here_tech`` CSV (absolute epoch ms, GPS frames only).
    Files that yield nothing are skipped; 0 comes back only when none did.
    """
    import csv  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    from utils.csv_helpers import parse_telemetry_csv  # noqa: PLC0415

    def from_mcap(path: Path) -> int | None:
        from mcap.reader import make_reader  # noqa: PLC0415

        try:
            with path.open("rb") as handle:
                times = [m.log_time for _, _, m in make_reader(handle).iter_messages()]
        except Exception:
            return None
        times = [t for t in times if t > 0]
        return min(times) if times else None

    def from_csv(path: Path) -> int | None:
        try:
            with path.open(newline="") as handle:
                header = next(csv.reader(handle), [])
            if "timestamp" in header:
                frames = parse_telemetry_csv(path)
                return _earliest_frame_time_ns(frames, timezone) if frames else None
            if "time" in header:
                with path.open(newline="") as handle:
                    times = [int(row["time"]) for row in csv.DictReader(handle) if row.get("time")]
                return min(times) * 1_000_000 if times else None
        except Exception:
            logger.debug("No trip start time readable from %s", path)
        return None

    stamps: list[int] = []
    for folder in sorted({p.parent for p in Path(trip_dir).rglob("*") if p.parent.name.endswith("_FOLDER")}):
        found = None
        for mcap_path in sorted(folder.glob("*.mcap")):
            found = from_mcap(mcap_path)
            if found is not None:
                break
        if found is None:
            for csv_path in sorted(folder.glob("*.csv")):
                if csv_path.name.endswith(".sei.csv"):
                    continue
                found = from_csv(csv_path)
                if found is not None:
                    break
        if found is not None:
            stamps.append(found)
    return min(stamps) if stamps else 0


def save_to_mcap_file(mcap_path: str, frame_data_list: list[FrameData], timezone: str | None, format: FootageMCAPFormat = FootageMCAPFormat.default):
    try:
        mcap_metadata = _convert_to_mcap(frame_data_list, 1, timezone, format)
        proto_schema = minute_metadata_pb2.TelemetryData()
        schema_name = "TelemetryData"
        topic_name = "TelemetryData"

        generate_mcap_file(
            mcap_path,
            mcap_metadata,
            proto_schema,
            schema_name,
            topic_name,
            log_time_ns=_earliest_frame_time_ns(frame_data_list, timezone),
        )
        
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
