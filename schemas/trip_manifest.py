from datetime import datetime
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

class SignedFile(BaseModel):
    presigned_url: str = Field(..., description="Temporary URL to download the object")
    expires_at: datetime = Field(..., description="UTC timestamp when URL expires")


class FixedMetadata(BaseModel):
    frame_width: int
    frame_height: int
    frame_mp: float
    vehicle_make: str
    vehicle_model: str
    phone_make: str
    phone_model: str
    platform_type_video: str
    platform_type_data: str
    platform_type_gps: str = "vx360"
    reference_frame: str = "ground_nominal"


class CameraItem(BaseModel):
    metadata_file_name: str | None = None
    camera_type: str | None = None
    camera: SignedFile
    telemetry: SignedFile | None = None
    sei: SignedFile | None = None


class MinuteManifest(BaseModel):
    minute_id: int
    minute_epoch: int | None = None
    minute_epoch_local: int | None = None
    fused_sei: SignedFile | None = None
    cameras: list[CameraItem] = Field(default_factory=list)

    def get_minute_key(self) -> str | None:
        if self.minute_epoch_local is None:
            return None
        dt = datetime.fromtimestamp(self.minute_epoch_local / 1000, tz=ZoneInfo("UTC"))
        return dt.strftime("%Y-%m-%d_%H-%M")


class TripManifest(BaseModel):
    trip_id: str
    sub_trip: int
    health_grade: int = 500
    version: str | None = None
    country: str | None = None
    state: str | None = None
    vehicle_hw_version: str | None = None
    timezone: str | None = None
    minutes: list[MinuteManifest] = Field(default_factory=list)
    fixed_metadata: SignedFile | None = None
    base_insights: SignedFile | None = None
    safety_critical: SignedFile | None = None
    ego_actions: SignedFile | None = None
    manifest_generated_at: datetime
