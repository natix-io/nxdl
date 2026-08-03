from dataclasses import dataclass, fields
from typing import Optional
from datetime import datetime

from schemas.dataset_request import FootageCSVFormat

_NA = "na"

# The delivered telemetry CSV columns, in order, for FootageCSVFormat.default. Narrower
# than FrameData itself: the service writes every field it models, and the client keeps
# only these. Single source of truth for both to_csv_row and csv_fieldnames.
_DEFAULT_CSV_FIELDS = (
    "timestamp",
    "frame_number",
    "GPS_latitude_deg",
    "GPS_longitude_deg",
    "horizontal_accuracy_m",
    "speed_mps",
    "velocity_north_mps",
    "velocity_east_mps",
    "heading_deg",
    "heading_accuracy_deg",
    "image_direction",
)


@dataclass
class FrameData:
    local_timestamp: datetime
    frame_number: Optional[str]
    GPS_latitude_deg: Optional[str]
    GPS_longitude_deg: Optional[str]
    horizontal_accuracy_m: Optional[str]
    angular_velocity_x_rad_per_s: Optional[str]
    angular_velocity_y_rad_per_s: Optional[str]
    angular_velocity_z_rad_per_s: Optional[str]
    acceleration_x_mps2: Optional[str]
    acceleration_y_mps2: Optional[str]
    acceleration_z_mps2: Optional[str]
    speed_mps: Optional[str]
    velocity_north_mps: Optional[str]
    velocity_east_mps: Optional[str]
    heading_deg: Optional[str]
    heading_accuracy_deg: Optional[str]
    image_direction: Optional[float]
    GPS_latitude_deg_phone: Optional[str]
    GPS_longitude_deg_phone: Optional[str]
    heading_deg_phone: Optional[str]

    def __post_init__(self):
        self.timestamp = self.get_timestamp()

    def get_timestamp(self, format: str = None, utc: bool = False) -> str:
        dt = self.local_timestamp
        if utc:
            dt = datetime.fromtimestamp(dt.timestamp())
        if not format:
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        return dt.strftime(format)

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "FrameData":
        """Build a FrameData from a single CSV DictReader row."""
        return cls(
            local_timestamp=datetime.fromisoformat(row["timestamp"]),
            frame_number=_opt_str(row.get("frame_number")),
            GPS_latitude_deg=_opt_str(row.get("GPS_latitude_deg")),
            GPS_longitude_deg=_opt_str(row.get("GPS_longitude_deg")),
            horizontal_accuracy_m=_opt_str(row.get("horizontal_accuracy_m")),
            angular_velocity_x_rad_per_s=_opt_str(row.get("angular_velocity_x_rad_per_s")),
            angular_velocity_y_rad_per_s=_opt_str(row.get("angular_velocity_y_rad_per_s")),
            angular_velocity_z_rad_per_s=_opt_str(row.get("angular_velocity_z_rad_per_s")),
            acceleration_x_mps2=_opt_str(row.get("acceleration_x_mps2")),
            acceleration_y_mps2=_opt_str(row.get("acceleration_y_mps2")),
            acceleration_z_mps2=_opt_str(row.get("acceleration_z_mps2")),
            speed_mps=_opt_str(row.get("speed_mps")),
            velocity_north_mps=_opt_str(row.get("velocity_north_mps")),
            velocity_east_mps=_opt_str(row.get("velocity_east_mps")),
            heading_deg=_opt_str(row.get("heading_deg")),
            heading_accuracy_deg=_opt_str(row.get("heading_accuracy_deg")),
            image_direction=_opt_float(row.get("image_direction")),
            GPS_latitude_deg_phone=_opt_str(row.get("GPS_latitude_deg_phone")),
            GPS_longitude_deg_phone=_opt_str(row.get("GPS_longitude_deg_phone")),
            heading_deg_phone=_opt_str(row.get("heading_deg_phone")),
        )

    @classmethod
    def csv_fieldnames(cls, fmt: FootageCSVFormat = FootageCSVFormat.default) -> list[str]:
        """The CSV column names for a format, without needing a row.

        Shares `_DEFAULT_CSV_FIELDS` with `to_csv_row` so the two cannot drift. Needed to
        write a header for a minute that has no telemetry rows at all: the service emits
        those CSVs with headers and no data (a minute whose device/phone logs are absent),
        and the delivered file must carry the same columns as its populated siblings.
        """
        if fmt is FootageCSVFormat.default:
            return list(_DEFAULT_CSV_FIELDS)
        if fmt is FootageCSVFormat.here_tech:
            return ["time", "latitude", "longitude"]
        raise ValueError(f"Unsupported footage CSV format: {fmt}")

    def to_csv_row(
        self,
        fmt: FootageCSVFormat = FootageCSVFormat.default,
        default_value_for_none: str = _NA,
    ) -> dict | None:
        """Convert to a dict suitable for csv.DictWriter.

        Returns None when the format dictates this row should be skipped
        (e.g. here_tech rows without GPS).
        """
        if fmt is FootageCSVFormat.default:
            row: dict = {"timestamp": self.timestamp}
            for field in _DEFAULT_CSV_FIELDS:
                value = getattr(self, field)
                row[field] = value if value is not None else default_value_for_none
            return row

        if fmt is FootageCSVFormat.here_tech:
            if self.GPS_latitude_deg and self.GPS_longitude_deg:
                return {
                    "time": int(self.local_timestamp.timestamp() * 1000),
                    "latitude": self.GPS_latitude_deg,
                    "longitude": self.GPS_longitude_deg,
                }
            return None

        raise ValueError(f"Unsupported footage CSV format: {fmt}")


def _opt_str(value: str | None) -> str | None:
    if value is None or value == _NA:
        return None
    return value


def _opt_float(value: str | None) -> float | None:
    if value is None or value == _NA:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
